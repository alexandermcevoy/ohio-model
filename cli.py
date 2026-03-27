"""
cli.py — Typer CLI for the Ohio House Election Model.
"""

from __future__ import annotations

from pathlib import Path

import typer

app = typer.Typer(help="Ohio House Election Model — partisan lean analysis tool.")

# Default paths (relative to project root)
DEFAULT_PRECINCT_SHP = "data/shapefiles/oh_2020/oh_2020.shp"
DEFAULT_DISTRICT_SHP = "data/shapefiles/Corrected Sept 29 2023 Unified Bipartisan Redistricting Plan HD SHP.shp"
DEFAULT_OUTPUT_CSV = "reports/session1/oh_house_partisan_lean_2020.csv"


@app.command()
def run(
    precincts: str = typer.Option(DEFAULT_PRECINCT_SHP, help="Path to VEST 2020 Ohio precinct shapefile."),
    districts: str = typer.Option(DEFAULT_DISTRICT_SHP, help="Path to Census TIGER/Line SLDL shapefile."),
    output: str = typer.Option(DEFAULT_OUTPUT_CSV, help="Output CSV path."),
):
    """
    Run the full pipeline: ingest → crosswalk → partisan lean → reports.
    """
    from src.ingest import load_precincts, load_districts, get_vest_races
    from src.crosswalk import build_crosswalk, validate_crosswalk
    from src.partisan import compute_lean, build_output, validate_statewide_result
    from src.validate import write_validation_summary

    typer.echo("=== Ohio House Model — Proof of Concept ===\n")

    # Step 1: Ingest
    typer.echo("Step 1: Ingesting shapefiles …")
    precinct_gdf = load_precincts(precincts)
    district_gdf = load_districts(districts)

    vest_races = get_vest_races(precinct_gdf)
    vote_cols = [col for cols in vest_races.values() for col in cols]

    if not vote_cols:
        typer.echo("ERROR: No VEST vote columns detected. Verify the precinct shapefile.", err=True)
        raise typer.Exit(code=1)

    # Step 2: Crosswalk
    typer.echo("\nStep 2: Building precinct-to-district crosswalk …")
    fragments, district_votes = build_crosswalk(precinct_gdf, district_gdf, vote_cols)
    crosswalk_issues = validate_crosswalk(precinct_gdf, fragments, district_votes, vote_cols)

    # Step 3: Partisan lean
    typer.echo("\nStep 3: Computing partisan lean …")
    statewide_totals = {col: float(precinct_gdf[col].sum()) for col in vote_cols}
    district_votes_with_lean = compute_lean(district_votes, vote_cols, statewide_totals)
    partisan_issues = validate_statewide_result(statewide_totals, vote_cols)

    # Step 4: Output
    typer.echo("\nStep 4: Writing outputs …")
    output_df = build_output(district_votes_with_lean)

    output_path = Path(output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_df.to_csv(output_path, index=False, float_format="%.6f")
    typer.echo(f"Lean CSV written to: {output_path}")

    write_validation_summary(
        precincts=precinct_gdf,
        districts=district_gdf,
        fragments=fragments,
        district_votes=district_votes,
        output_df=output_df,
        crosswalk_issues=crosswalk_issues,
        partisan_issues=partisan_issues,
        vote_cols=vote_cols,
    )

    typer.echo("\nDone.")


SOS_FILES = {
    "2024": "data/raw/statewide-races-precint-level.xlsx",
    "2022": "data/raw/statewide-races-by-precinct.xlsx",
    "2020": "data/raw/statewideresultsbyprecinct.xlsx",
    "2018": "data/raw/2018-11-06_statewideprecinct_miami.xlsx",
}


@app.command("composite")
def run_composite(
    precincts: str = typer.Option(DEFAULT_PRECINCT_SHP, help="VEST 2020 precinct shapefile."),
    districts: str = typer.Option(DEFAULT_DISTRICT_SHP, help="Ohio House district shapefile."),
):
    """
    Session 2: Build multi-race composite partisan lean index.

    Ingests all available SOS xlsx files, joins statewide races to VEST 2020
    geometry, runs the spatial crosswalk, and builds the weighted composite.
    """
    import pandas as pd
    from src.ingest import load_precincts, load_districts, get_vest_races
    from src.crosswalk import build_crosswalk, validate_crosswalk
    from src.partisan import compute_lean, validate_statewide_result
    from src.ingest_sos import load_sos_file, get_race_df
    from src.join_sos_vest import build_county_lookup, join_sos_to_vest, crosscheck_vest_sos_2020
    from src.ingest_house_results import parse_house_results, combine_house_results
    from src.composite import build_composite, merge_composite_with_house_results

    typer.echo("=== Ohio House Model — Session 2: Composite Lean ===\n")

    # ── Load VEST + district shapefiles ──────────────────────────────────────
    typer.echo("Loading shapefiles …")
    vest_gdf = load_precincts(precincts)
    district_gdf = load_districts(districts)

    # ── Load all SOS files ────────────────────────────────────────────────────
    typer.echo("\nLoading SOS xlsx files …")
    sos_files: dict[str, object] = {}
    for year, path in SOS_FILES.items():
        p = Path(path)
        if p.exists():
            sos_files[year] = load_sos_file(p)
        else:
            typer.echo(f"  {year}: file not found at {path} — skipping.")

    if not sos_files:
        typer.echo("ERROR: No SOS files found.", err=True)
        raise typer.Exit(1)

    # ── Build county FIPS → name lookup from 2020 SOS (highest quality match) ─
    typer.echo("\nBuilding county FIPS lookup …")
    sos_2020 = sos_files.get("2020")
    if sos_2020:
        county_lookup = build_county_lookup(vest_gdf, sos_2020.precinct_statewide)
        crosscheck_vest_sos_2020(vest_gdf, sos_2020, county_lookup)
    else:
        # Fall back to any available SOS file
        first_sos = next(iter(sos_files.values()))
        county_lookup = build_county_lookup(vest_gdf, first_sos.precinct_statewide)

    # ── Run statewide races through VEST crosswalk ────────────────────────────
    typer.echo("\nJoining SOS statewide races to VEST geometry + running crosswalk …")

    district_leans: dict[tuple[str, str], "pd.Series"] = {}
    statewide_validation: list[str] = []

    # VEST 2020 presidential is already in the shapefile — use it directly
    vest_races = get_vest_races(vest_gdf)
    vest_vote_cols = [col for cols in vest_races.values() for col in cols]
    if vest_vote_cols:
        typer.echo("\n  Running VEST 2020 presidential crosswalk …")
        frags, dist_votes = build_crosswalk(vest_gdf, district_gdf, vest_vote_cols)
        sw_totals = {c: float(vest_gdf[c].sum()) for c in vest_vote_cols}
        lean_df = compute_lean(dist_votes, vest_vote_cols, sw_totals)
        issues = validate_statewide_result(sw_totals, vest_vote_cols)
        statewide_validation += issues
        if "PRE_lean" in lean_df.columns:
            district_leans[("2020", "pre")] = lean_df.set_index("district_num")["PRE_lean"]

    # For each SOS year+race: join to VEST, crosswalk, compute lean
    RACES_BY_YEAR: dict[str, list[str]] = {
        "2024": ["pre", "uss"],
        "2022": ["gov", "uss", "atg", "aud", "sos_off", "tre"],
        "2018": ["gov", "uss", "atg", "aud", "sos_off", "tre"],
        # 2020 SOS has only presidential (already handled via VEST above)
    }

    for year, races in RACES_BY_YEAR.items():
        sos = sos_files.get(year)
        if sos is None:
            continue
        for race_label in races:
            if race_label not in sos.statewide:
                continue
            spec = sos.statewide[race_label]
            if not spec.has_contest():
                continue

            typer.echo(f"\n  {year} {race_label} …")
            race_df = get_race_df(sos, race_label)
            d_col = f"{year}_{race_label}_d"
            r_col = f"{year}_{race_label}_r"

            try:
                enriched = join_sos_to_vest(
                    vest_gdf, race_df, d_col, r_col, county_lookup, year, race_label
                )
            except ValueError as e:
                typer.echo(f"    SKIPPED: {e}", err=True)
                continue

            frags, dist_votes = build_crosswalk(enriched, district_gdf, [d_col, r_col])
            sw_totals = {d_col: float(race_df["d_votes"].sum()),
                         r_col: float(race_df["r_votes"].sum())}
            lean_df = _compute_two_party_lean(dist_votes, d_col, r_col, sw_totals)
            district_leans[(year, race_label)] = lean_df.set_index("district_num")["lean"]
            statewide_validation.append(
                f"{year} {race_label}: D={sw_totals[d_col]:,.0f} R={sw_totals[r_col]:,.0f} "
                f"D-2P={sw_totals[d_col]/(sw_totals[d_col]+sw_totals[r_col]):.4f}"
            )

    # ── House actual results ──────────────────────────────────────────────────
    typer.echo("\nAggregating actual house race results …")
    house_results_list = []
    for year, sos in sos_files.items():
        hr = parse_house_results(sos)
        if not hr.empty:
            house_results_list.append(hr)

    house_long = pd.concat(house_results_list, ignore_index=True) if house_results_list else pd.DataFrame()
    house_wide = combine_house_results(house_results_list)

    # Save long-format house results
    Path("reports").mkdir(exist_ok=True)
    if not house_long.empty:
        house_long.to_csv("reports/session2/oh_house_actual_results.csv", index=False, float_format="%.6f")
        typer.echo(f"  House results written to reports/session2/oh_house_actual_results.csv")

    # ── Build composite lean ──────────────────────────────────────────────────
    typer.echo("\nBuilding composite lean index …")
    composite_df = build_composite(district_leans)

    # Year baselines for candidate effect: statewide D 2-party share from the
    # primary reference race each cycle (presidential 2020/2024, gov 2022/2018).
    # 2020 presidential comes from VEST (already validated exact match with SOS).
    _BASELINE_RACE = {"2024": "pre", "2022": "gov", "2018": "gov"}
    year_baselines: dict[str, float] = {}
    for _yr, _race in _BASELINE_RACE.items():
        _sos = sos_files.get(_yr)
        if _sos and _race in _sos.statewide:
            _rd = get_race_df(_sos, _race)
            _d, _r = _rd["d_votes"].sum(), _rd["r_votes"].sum()
            if _d + _r > 0:
                year_baselines[_yr] = float(_d / (_d + _r))
    if vest_vote_cols:
        _d20 = float(vest_gdf["G20PREDBID"].sum())
        _r20 = float(vest_gdf["G20PRERTRU"].sum())
        if _d20 + _r20 > 0:
            year_baselines["2020"] = _d20 / (_d20 + _r20)

    typer.echo("\n  Year baselines for candidate effect:")
    for _yr in sorted(year_baselines):
        typer.echo(f"    {_yr}: {year_baselines[_yr]:.4f}")

    available_years = [y for y in SOS_FILES if y in sos_files]
    final_df = merge_composite_with_house_results(
        composite_df, house_wide, available_years, year_baselines
    )

    final_df.to_csv("reports/session2/oh_house_composite_lean.csv", index=False, float_format="%.6f")
    typer.echo("\nComposite lean written to reports/session2/oh_house_composite_lean.csv")

    # ── Store year baselines for downstream validation ────────────────────────
    import json
    baselines_path = Path("data/processed/year_baselines.json")
    baselines_path.parent.mkdir(parents=True, exist_ok=True)
    baselines_path.write_text(json.dumps(year_baselines, indent=2), encoding="utf-8")
    typer.echo(f"Year baselines stored to {baselines_path}")

    # ── Drop-one composite sensitivity ───────────────────────────────────────
    typer.echo("\n--- Drop-one composite sensitivity ---")
    from src.composite import drop_one_sensitivity
    drop_one_sensitivity(district_leans)

    # ── Validation summary ────────────────────────────────────────────────────
    val_path = Path("reports/session2/validation_summary_session2.txt")
    _write_session2_validation(val_path, district_leans, statewide_validation, county_lookup)
    typer.echo(f"Validation summary written to {val_path}")
    typer.echo("\nDone.")


def _compute_two_party_lean(
    dist_votes,
    d_col: str,
    r_col: str,
    sw_totals: dict,
) -> "pd.DataFrame":
    """Thin wrapper: compute lean for a single D/R column pair."""
    import pandas as pd
    df = dist_votes.copy()
    df["two_party"] = df[d_col] + df[r_col]
    df["dem_share"] = df[d_col] / df["two_party"].replace(0, float("nan"))
    sw_d = sw_totals[d_col]
    sw_r = sw_totals[r_col]
    sw_share = sw_d / (sw_d + sw_r) if (sw_d + sw_r) > 0 else float("nan")
    df["lean"] = df["dem_share"] - sw_share
    return df[["district_num", d_col, r_col, "two_party", "dem_share", "lean"]]


def _write_session2_validation(path, district_leans, sw_issues, county_lookup):
    from datetime import datetime
    lines = [
        "Ohio House Model — Session 2 Validation Summary",
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "=" * 60,
        "  Races included in composite",
        "=" * 60,
    ]
    for (year, race), series in sorted(district_leans.items()):
        lines.append(f"  {year} {race:15s}  n_districts={series.notna().sum()}")
    lines += ["", "=" * 60, "  Statewide benchmarks", "=" * 60]
    lines += [f"  {s}" for s in sw_issues]
    lines += ["", "=" * 60, f"  County FIPS mapped: {len(county_lookup)}/88", "=" * 60]
    path.write_text("\n".join(lines), encoding="utf-8")


@app.command("classify")
def run_classify(
    composite: str = typer.Option(
        "reports/session2/oh_house_composite_lean.csv",
        help="Composite lean CSV from the 'composite' command.",
    ),
    house_results: str = typer.Option(
        "reports/session2/oh_house_actual_results.csv",
        help="Long-format house results CSV from the 'composite' command.",
    ),
):
    """
    Session 3: District classification, targeting framework, and path-to-majority.

    Reads the composite lean CSV and house results CSV produced by the
    'composite' command. Outputs targeting CSV, scenario table, and pickup ladder.
    """
    import pandas as pd
    from src.ingest_sos import load_sos_file
    from src.ingest_house_results import extract_candidate_names
    from src.classify import build_targeting_df
    from src.scenarios import (
        run_scenario_table,
        build_pickup_ladder,
        build_defensive_list,
        format_pickup_ladder,
        print_scenario_summary,
    )

    typer.echo("=== Ohio House Model — Session 3: Classification & Targeting ===\n")

    # ── Load inputs ───────────────────────────────────────────────────────────
    composite_path = Path(composite)
    house_path = Path(house_results)

    if not composite_path.exists():
        typer.echo(f"ERROR: {composite_path} not found. Run 'composite' first.", err=True)
        raise typer.Exit(1)
    if not house_path.exists():
        typer.echo(f"ERROR: {house_path} not found. Run 'composite' first.", err=True)
        raise typer.Exit(1)

    composite_df = pd.read_csv(composite_path)
    house_long = pd.read_csv(house_path)
    typer.echo(f"Loaded {len(composite_df)} districts from {composite_path.name}")
    typer.echo(f"Loaded {len(house_long)} house result rows from {house_path.name}")

    # ── Extract 2024 candidate names ──────────────────────────────────────────
    candidate_names_2024 = None
    sos_2024_path = Path(SOS_FILES["2024"])
    if sos_2024_path.exists():
        typer.echo("\nExtracting 2024 candidate names from SOS file …")
        sos_2024 = load_sos_file(sos_2024_path)
        candidate_names_2024 = extract_candidate_names(sos_2024)
        n_names = candidate_names_2024["dem_candidate_2024"].notna().sum()
        typer.echo(f"  {n_names} districts with D candidate names, "
                   f"{candidate_names_2024['rep_candidate_2024'].notna().sum()} with R")
    else:
        typer.echo(f"  NOTE: 2024 SOS file not found at {sos_2024_path}; candidate names unavailable.")

    # ── Compute district uncertainty (sigma) for WP-based tiers ──────────────
    typer.echo("\nComputing district uncertainty for WP-based tiers …")
    from src.simulate import compute_sigma_prior, estimate_district_sigma
    from src.classify import compute_swing_metrics as _csm

    sigma_prior = compute_sigma_prior(composite_df)
    typer.echo(f"  Sigma prior (pooled candidate effect std): {sigma_prior:.4f}")

    # Compute swing metrics on a scratch copy so we can estimate sigma before
    # the full classify pass.  compute_swing_metrics needs district + composite_lean.
    _swing_tmp = _csm(composite_df.copy(), house_long)
    sigma_df = estimate_district_sigma(_swing_tmp, sigma_prior)
    typer.echo(f"  Sigma range: {sigma_df['sigma_i'].min():.4f} – {sigma_df['sigma_i'].max():.4f}")

    # ── Build targeting DataFrame ─────────────────────────────────────────────
    typer.echo("\nClassifying districts and computing swing metrics …")
    targeting_df = build_targeting_df(composite_df, house_long, candidate_names_2024, sigma_df=sigma_df)

    # ── Tier summary ──────────────────────────────────────────────────────────
    typer.echo("\n--- Tier distribution (at 48% statewide D — neutral midterm) ---")
    from src.classify import TIER_ORDER, REFERENCE_ENVIRONMENTS
    tier_counts = targeting_df["tier"].value_counts()
    for tier in TIER_ORDER:
        n = tier_counts.get(tier, 0)
        held = targeting_df[targeting_df["tier"] == tier]["current_holder"].value_counts().to_dict()
        held_str = "  ".join(f"{p}:{c}" for p, c in sorted(held.items()))
        typer.echo(f"  {tier:12s} {n:3d} districts   [{held_str}]")

    # Show tier counts at all reference environments
    for env in REFERENCE_ENVIRONMENTS:
        env_label = f"{int(env * 100)}"
        tier_col = f"tier_{env_label}"
        if tier_col in targeting_df.columns:
            counts = targeting_df[tier_col].value_counts()
            tossup_n = counts.get("tossup", 0)
            lean_d_n = counts.get("lean_d", 0)
            lean_r_n = counts.get("lean_r", 0)
            typer.echo(f"  At {env_label}%: {tossup_n} tossup, {lean_d_n} lean_d, {lean_r_n} lean_r")

    n_pickup = targeting_df["pickup_opportunity"].sum()
    n_defense = targeting_df["defensive_priority"].sum()
    n_open = targeting_df["open_seat_2026"].sum()
    typer.echo(f"\n  Pickup opportunities (R-held, competitive tier): {n_pickup}")
    typer.echo(f"  Defensive priorities (D-held, at risk):          {n_defense}")
    typer.echo(f"  Known 2026 open seats:                           {n_open}")

    # ── Apply redistricting filter to house_long ──────────────────────────────
    redistricting_path_for_filter = Path("reports/redistricting_overlap.csv")
    if redistricting_path_for_filter.exists():
        import pandas as _pd_rd
        from src.ingest_house_results import apply_redistricting_filter as _apply_rd_filter
        overlap_for_filter = _pd_rd.read_csv(redistricting_path_for_filter)
        house_long, rd_filter_summary = _apply_rd_filter(house_long, overlap_for_filter)
        typer.echo(
            f"\n  Redistricting filter: {rd_filter_summary['n_dropped']} observations dropped "
            f"({rd_filter_summary.get('n_interim_final_relocated', 0)} districts also lost 2022 data)."
        )
    else:
        typer.echo("\n  NOTE: redistricting_overlap.csv not found — run 'redistricting-fix' first.")

    # ── Drop-one sensitivity: merge into targeting ────────────────────────────
    drop_one_path = Path("data/processed/drop_one_sensitivity.csv")
    if drop_one_path.exists():
        import pandas as _pd
        drop_one_df = _pd.read_csv(drop_one_path)
        targeting_df = targeting_df.merge(
            drop_one_df[["district", "max_change", "most_sensitive_to"]],
            on="district", how="left",
        )
        targeting_df = targeting_df.rename(columns={
            "max_change": "composite_sensitivity",
            "most_sensitive_to": "most_sensitive_race",
        })
        n_sens = (targeting_df["composite_sensitivity"] > 0.02).sum()
        typer.echo(f"\n  Composite sensitivity merged: {n_sens} districts with >2pt sensitivity")
    else:
        typer.echo("\n  NOTE: drop_one_sensitivity.csv not found — run 'composite' to generate it.")

    # ── Anomaly detection ─────────────────────────────────────────────────────
    typer.echo("\n--- Residual anomaly detection ---")
    import json as _json
    baselines_path = Path("data/processed/year_baselines.json")
    redistricting_path = Path("reports/redistricting_overlap.csv")

    if baselines_path.exists():
        year_baselines = _json.loads(baselines_path.read_text(encoding="utf-8"))
        import pandas as _pd2
        redistricting_df = _pd2.read_csv(redistricting_path) if redistricting_path.exists() else None
        from src.validate import detect_anomalies
        detect_anomalies(
            composite_df[["district", "composite_lean"]],
            house_long,
            year_baselines,
            redistricting_df=redistricting_df,
            output_path="reports/anomaly_flags.csv",
        )
    else:
        typer.echo("  NOTE: year_baselines.json not found — run 'composite' to generate it.")

    # ── Scenario table ────────────────────────────────────────────────────────
    typer.echo("\n--- Scenario table (uniform swing model — fundamentals only) ---")
    scenario_df = run_scenario_table(targeting_df)
    print_scenario_summary(scenario_df)

    # ── Pickup ladder & defensive list ───────────────────────────────────────
    ladder_df = build_pickup_ladder(targeting_df)
    defensive_df = build_defensive_list(targeting_df)

    report_text = format_pickup_ladder(ladder_df, scenario_df, defensive_df)
    typer.echo("\n" + report_text)

    # ── Write outputs ─────────────────────────────────────────────────────────
    Path("reports").mkdir(exist_ok=True)

    targeting_path = Path("reports/session3/oh_house_targeting.csv")
    targeting_df.to_csv(targeting_path, index=False, float_format="%.6f")
    typer.echo(f"\nTargeting CSV written to {targeting_path}")

    scenario_path = Path("reports/session3/oh_house_scenario_table.csv")
    scenario_df.drop(columns=["cumulative_d_districts"]).to_csv(
        scenario_path, index=False
    )
    typer.echo(f"Scenario table written to {scenario_path}")

    ladder_path = Path("reports/session3/oh_house_pickup_ladder.txt")
    ladder_path.write_text(report_text, encoding="utf-8")
    typer.echo(f"Pickup ladder written to {ladder_path}")

    typer.echo("\nDone.")


@app.command("session4")
def run_session4(
    precincts: str = typer.Option(DEFAULT_PRECINCT_SHP, help="VEST 2020 precinct shapefile."),
    districts: str = typer.Option(DEFAULT_DISTRICT_SHP, help="Ohio House district shapefile."),
    pop_weights_cache: str = typer.Option(
        "data/processed/pop_weight_table.parquet",
        help="Cache path for population weight table.",
    ),
    acs_cache: str = typer.Option(
        "data/processed/acs_raw_blockgroups.parquet",
        help="Cache path for raw ACS block-group data.",
    ),
):
    """
    Session 4: ACS demographics, pop-weighted crosswalk, OLS regression, classification update.

    Sub-task A: Pull ACS 5-year estimates → district demographic table.
    Sub-task B: Build Census 2020 block population weights → re-run composite with
                pop-weighted crosswalk, compare before/after district leans.
    Sub-task C: OLS regression of house outcomes on composite lean + demographics + incumbency.
    Sub-task D: Re-run district classification with data-quality flags on target mode.
    """
    import pandas as pd
    from datetime import datetime
    from src.ingest import load_precincts, load_districts, get_vest_races
    from src.crosswalk import (
        build_crosswalk,
        validate_crosswalk,
        build_pop_weight_table,
        build_crosswalk_pop_weighted,
    )
    from src.partisan import compute_lean, validate_statewide_result
    from src.ingest_sos import load_sos_file, get_race_df
    from src.join_sos_vest import build_county_lookup, join_sos_to_vest, crosscheck_vest_sos_2020
    from src.ingest_house_results import parse_house_results, combine_house_results
    from src.composite import build_composite, merge_composite_with_house_results
    from src.demographics import build_district_demographics
    from src.model import run_regression, format_regression_summary
    from src.classify import build_targeting_df

    typer.echo("=== Ohio House Model — Session 4: Demographics, Crosswalk Upgrade & Regression ===\n")

    # ── Load shapefiles ───────────────────────────────────────────────────────
    typer.echo("Loading shapefiles …")
    vest_gdf = load_precincts(precincts)
    district_gdf = load_districts(districts)

    # ── Load all SOS files ────────────────────────────────────────────────────
    typer.echo("\nLoading SOS xlsx files …")
    sos_files: dict[str, object] = {}
    for year, path in SOS_FILES.items():
        p = Path(path)
        if p.exists():
            sos_files[year] = load_sos_file(p)
        else:
            typer.echo(f"  {year}: file not found at {path} — skipping.")

    if not sos_files:
        typer.echo("ERROR: No SOS files found.", err=True)
        raise typer.Exit(1)

    # ── County lookup ─────────────────────────────────────────────────────────
    typer.echo("\nBuilding county FIPS lookup …")
    sos_2020 = sos_files.get("2020")
    if sos_2020:
        county_lookup = build_county_lookup(vest_gdf, sos_2020.precinct_statewide)
        crosscheck_vest_sos_2020(vest_gdf, sos_2020, county_lookup)
    else:
        first_sos = next(iter(sos_files.values()))
        county_lookup = build_county_lookup(vest_gdf, first_sos.precinct_statewide)

    # =========================================================================
    # Sub-Task A: ACS Demographics
    # =========================================================================
    typer.echo("\n" + "=" * 60)
    typer.echo("Sub-Task A: ACS 5-year demographics")
    typer.echo("=" * 60)

    Path("data/processed").mkdir(parents=True, exist_ok=True)
    demographics_df = build_district_demographics(
        district_gdf,
        cache_path=acs_cache,
    )

    demo_out = Path("data/processed/oh_house_demographics.csv")
    demographics_df.to_csv(demo_out, float_format="%.6f")
    typer.echo(f"\n  Demographics written to {demo_out}  ({len(demographics_df)} districts)")

    # =========================================================================
    # Sub-Task B: Population-Weighted Crosswalk + Updated Composite
    # =========================================================================
    typer.echo("\n" + "=" * 60)
    typer.echo("Sub-Task B: Census 2020 block population weights + composite re-run")
    typer.echo("=" * 60)

    # Load or build pop weight table
    pw_path = Path(pop_weights_cache)
    if pw_path.exists():
        typer.echo(f"\nLoading cached pop weight table from {pw_path} …")
        pop_weights = pd.read_parquet(pw_path)
        typer.echo(f"  {len(pop_weights):,} precinct × district rows loaded.")
    else:
        typer.echo("\nBuilding Census 2020 block population weight table …")
        typer.echo("  (This takes ~5–10 minutes; will cache for future runs.)")
        pop_weights = build_pop_weight_table(vest_gdf, district_gdf)
        pw_path.parent.mkdir(parents=True, exist_ok=True)
        pop_weights.to_parquet(pw_path, index=False)
        typer.echo(f"  Saved to {pw_path}")

    # Load old composite for before/after comparison
    old_composite_path = Path("reports/session2/oh_house_composite_lean.csv")
    old_composite_df: pd.DataFrame | None = None
    if old_composite_path.exists():
        old_composite_df = pd.read_csv(old_composite_path)

    # Re-run composite pipeline with pop-weighted crosswalk
    typer.echo("\nRe-running composite pipeline with pop-weighted crosswalk …")

    district_leans: dict[tuple[str, str], "pd.Series"] = {}
    statewide_validation: list[str] = []

    # VEST 2020 presidential — pop-weighted
    vest_races = get_vest_races(vest_gdf)
    vest_vote_cols = [col for cols in vest_races.values() for col in cols]
    if vest_vote_cols:
        typer.echo("\n  Running VEST 2020 presidential crosswalk (pop-weighted) …")
        frags, dist_votes = build_crosswalk_pop_weighted(
            vest_gdf, district_gdf, vest_vote_cols, pop_weights
        )
        sw_totals = {c: float(vest_gdf[c].sum()) for c in vest_vote_cols}
        lean_df = compute_lean(dist_votes, vest_vote_cols, sw_totals)
        issues = validate_statewide_result(sw_totals, vest_vote_cols)
        statewide_validation += issues
        if "PRE_lean" in lean_df.columns:
            district_leans[("2020", "pre")] = lean_df.set_index("district_num")["PRE_lean"]

    RACES_BY_YEAR: dict[str, list[str]] = {
        "2024": ["pre", "uss"],
        "2022": ["gov", "uss", "atg", "aud", "sos_off", "tre"],
        "2018": ["gov", "uss", "atg", "aud", "sos_off", "tre"],
    }

    for year, races in RACES_BY_YEAR.items():
        sos = sos_files.get(year)
        if sos is None:
            continue
        for race_label in races:
            if race_label not in sos.statewide:
                continue
            spec = sos.statewide[race_label]
            if not spec.has_contest():
                continue

            typer.echo(f"\n  {year} {race_label} …")
            race_df = get_race_df(sos, race_label)
            d_col = f"{year}_{race_label}_d"
            r_col = f"{year}_{race_label}_r"

            try:
                enriched = join_sos_to_vest(
                    vest_gdf, race_df, d_col, r_col, county_lookup, year, race_label
                )
            except ValueError as e:
                typer.echo(f"    SKIPPED: {e}", err=True)
                continue

            frags, dist_votes = build_crosswalk_pop_weighted(
                enriched, district_gdf, [d_col, r_col], pop_weights
            )
            sw_totals = {d_col: float(race_df["d_votes"].sum()),
                         r_col: float(race_df["r_votes"].sum())}
            lean_df = _compute_two_party_lean(dist_votes, d_col, r_col, sw_totals)
            district_leans[(year, race_label)] = lean_df.set_index("district_num")["lean"]
            statewide_validation.append(
                f"{year} {race_label}: D={sw_totals[d_col]:,.0f} R={sw_totals[r_col]:,.0f} "
                f"D-2P={sw_totals[d_col]/(sw_totals[d_col]+sw_totals[r_col]):.4f}"
            )

    # House actual results
    typer.echo("\nAggregating actual house race results …")
    house_results_list = []
    for year, sos in sos_files.items():
        hr = parse_house_results(sos)
        if not hr.empty:
            house_results_list.append(hr)
    house_long = pd.concat(house_results_list, ignore_index=True) if house_results_list else pd.DataFrame()
    house_wide = combine_house_results(house_results_list)

    if not house_long.empty:
        house_long.to_csv("reports/session2/oh_house_actual_results.csv", index=False, float_format="%.6f")

    # Build composite
    typer.echo("\nBuilding composite lean index (pop-weighted) …")
    composite_df = build_composite(district_leans)

    _BASELINE_RACE = {"2024": "pre", "2022": "gov", "2018": "gov"}
    year_baselines: dict[str, float] = {}
    for _yr, _race in _BASELINE_RACE.items():
        _sos = sos_files.get(_yr)
        if _sos and _race in _sos.statewide:
            _rd = get_race_df(_sos, _race)
            _d, _r = _rd["d_votes"].sum(), _rd["r_votes"].sum()
            if _d + _r > 0:
                year_baselines[_yr] = float(_d / (_d + _r))
    if vest_vote_cols:
        _d20 = float(vest_gdf["G20PREDBID"].sum())
        _r20 = float(vest_gdf["G20PRERTRU"].sum())
        if _d20 + _r20 > 0:
            year_baselines["2020"] = _d20 / (_d20 + _r20)

    available_years = [y for y in SOS_FILES if y in sos_files]
    new_composite_df = merge_composite_with_house_results(
        composite_df, house_wide, available_years, year_baselines
    )

    # Before/after comparison
    lean_changes: list[dict] = []
    if old_composite_df is not None:
        typer.echo("\n  Before/after composite lean comparison (pop-weighted vs area-weighted):")
        merged_compare = old_composite_df[["district", "composite_lean"]].rename(
            columns={"composite_lean": "lean_area"}
        ).merge(
            new_composite_df[["district", "composite_lean"]].rename(
                columns={"composite_lean": "lean_pop"}
            ),
            on="district",
        )
        merged_compare["lean_delta"] = merged_compare["lean_pop"] - merged_compare["lean_area"]
        merged_compare = merged_compare.reindex(
            merged_compare["lean_delta"].abs().sort_values(ascending=False).index
        )
        typer.echo(f"  {'District':>10}  {'Area lean':>10}  {'Pop lean':>10}  {'Delta':>8}")
        for _, row in merged_compare.head(10).iterrows():
            typer.echo(
                f"  {int(row['district']):>10}  {row['lean_area']:>+.4f}    {row['lean_pop']:>+.4f}    {row['lean_delta']:>+.4f}"
            )
        lean_changes = merged_compare.to_dict("records")

    new_composite_df.to_csv("reports/session2/oh_house_composite_lean.csv", index=False, float_format="%.6f")
    typer.echo("\n  Updated composite lean written to reports/session2/oh_house_composite_lean.csv")

    # =========================================================================
    # Sub-Task C: OLS Regression
    # =========================================================================
    typer.echo("\n" + "=" * 60)
    typer.echo("Sub-Task C: GLM (binomial/logit) regression — house outcomes ~ lean + demographics + incumbency")
    typer.echo("=" * 60)

    ols_results, glm_results, reg_df = run_regression(new_composite_df, demographics_df)
    regression_summary = format_regression_summary(ols_results, glm_results, reg_df)
    typer.echo("\n" + regression_summary)

    reg_summary_path = Path("reports/session4/regression_summary.txt")
    reg_summary_path.write_text(regression_summary, encoding="utf-8")
    typer.echo(f"\n  Regression summary written to {reg_summary_path}")

    # =========================================================================
    # Sub-Task D: Updated Classification with Data Quality Flags + GLM effects
    # =========================================================================
    typer.echo("\n" + "=" * 60)
    typer.echo("Sub-Task D: Updated district classification (data quality flags + GLM candidate effects)")
    typer.echo("=" * 60)

    # Load updated house_long for targeting
    updated_house_long = pd.read_csv("reports/session2/oh_house_actual_results.csv")

    # Load old targeting for comparison
    old_targeting_path = Path("reports/session3/oh_house_targeting.csv")
    old_target_modes: pd.Series | None = None
    if old_targeting_path.exists():
        old_targeting_df = pd.read_csv(old_targeting_path)
        if "target_mode" in old_targeting_df.columns:
            old_target_modes = old_targeting_df.set_index("district")["target_mode"]

    new_targeting_df = build_targeting_df(new_composite_df, updated_house_long)

    # Merge GLM candidate effects: for each year where the district was contested,
    # GLM residual = actual dem_share − GLM predicted dem_share.
    # Pivot to wide and attach to targeting_df.
    glm_effects_wide = (
        reg_df[reg_df["glm_residual"].notna()][["district", "year", "glm_residual"]]
        .pivot(index="district", columns="year", values="glm_residual")
        .rename(columns={yr: f"glm_candidate_effect_{yr}" for yr in reg_df["year"].unique()})
        .reset_index()
    )
    new_targeting_df = new_targeting_df.merge(glm_effects_wide, on="district", how="left")

    # Report mode changes
    if old_target_modes is not None:
        new_modes = new_targeting_df.set_index("district")["target_mode"]
        changed = []
        for dist in new_modes.index:
            if dist in old_target_modes.index and old_target_modes[dist] != new_modes[dist]:
                changed.append({
                    "district": dist,
                    "old_mode": old_target_modes[dist],
                    "new_mode": new_modes[dist],
                })
        if changed:
            typer.echo(f"\n  Target mode changes: {len(changed)} districts")
            for c in changed[:20]:
                typer.echo(f"    District {c['district']:3d}: {c['old_mode']} → {c['new_mode']}")
        else:
            typer.echo("\n  No target mode changes.")

    new_targeting_df.to_csv(old_targeting_path, index=False, float_format="%.6f")
    typer.echo(f"\n  Updated targeting CSV written to {old_targeting_path}")

    # =========================================================================
    # Write validation summary
    # =========================================================================
    val_lines = [
        "Ohio House Model — Session 4 Validation Summary",
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "=" * 60,
        "  Sub-Task A: ACS Demographics",
        "=" * 60,
        f"  Districts with demographics: {len(demographics_df)}",
        f"  college_pct range: {demographics_df['college_pct'].min():.3f} – {demographics_df['college_pct'].max():.3f}",
        f"  median_income range: ${demographics_df['median_income'].min():,.0f} – ${demographics_df['median_income'].max():,.0f}",
        f"  pop_density range: {demographics_df['pop_density'].min():.1f} – {demographics_df['pop_density'].max():.1f} ppl/sqmi",
        "",
        "=" * 60,
        "  Sub-Task B: Pop-Weighted Crosswalk",
        "=" * 60,
    ]
    if lean_changes:
        val_lines.append("  Top 10 composite lean changes (area → pop-weighted):")
        val_lines.append(f"  {'District':>10}  {'Area lean':>10}  {'Pop lean':>10}  {'Delta':>8}")
        for r in lean_changes[:10]:
            val_lines.append(
                f"  {int(r['district']):>10}  {r['lean_area']:>+.4f}    {r['lean_pop']:>+.4f}    {r['lean_delta']:>+.4f}"
            )
    val_lines += [
        "",
        "=" * 60,
        "  Sub-Task C: GLM (Binomial/Logit) Regression",
        "=" * 60,
        regression_summary,
        "",
        "=" * 60,
        "  Sub-Task D: Classification Update",
        "=" * 60,
    ]
    mode_counts = new_targeting_df["target_mode"].value_counts()
    for mode, cnt in mode_counts.items():
        val_lines.append(f"  {mode:25s}: {cnt}")

    n_contested_dist = new_targeting_df["n_contested"].value_counts().sort_index()
    val_lines.append("")
    val_lines.append("  n_contested distribution:")
    for nc, cnt in n_contested_dist.items():
        val_lines.append(f"    n_contested={nc}: {cnt} districts")

    val_path = Path("reports/session4/validation_summary_session4.txt")
    val_path.write_text("\n".join(val_lines), encoding="utf-8")
    typer.echo(f"\nValidation summary written to {val_path}")
    typer.echo("\nDone.")


# ---------------------------------------------------------------------------
# Session 5 CLI commands
# ---------------------------------------------------------------------------

_DEFAULT_COMPOSITE = "reports/session2/oh_house_composite_lean.csv"
_DEFAULT_TARGETING = "reports/session3/oh_house_targeting.csv"
_DEFAULT_DEMOGRAPHICS = "data/processed/oh_house_demographics.csv"
_DEFAULT_SCENARIOS = "reports/session3/oh_house_scenario_table.csv"


def _load_standard_dfs(
    composite: str = _DEFAULT_COMPOSITE,
    targeting: str = _DEFAULT_TARGETING,
    demographics: str = _DEFAULT_DEMOGRAPHICS,
) -> "tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]":
    """Load the three standard DataFrames used by most Session 5 commands."""
    import pandas as pd
    composite_df = pd.read_csv(composite)
    targeting_df = pd.read_csv(targeting)
    demo_df = pd.read_csv(demographics) if Path(demographics).exists() else pd.DataFrame()
    return composite_df, targeting_df, demo_df


_DEFAULT_REDISTRICTING = "reports/redistricting_overlap.csv"
_DEFAULT_ANOMALIES = "reports/anomaly_flags.csv"


@app.command("report")
def run_report(
    district: int = typer.Option(None, help="Single district number (1–99)."),
    all: bool = typer.Option(False, "--all", help="Generate profiles for all 99 districts."),
    output_dir: str = typer.Option("reports/district_profiles", help="Output directory for PDFs."),
    composite: str = typer.Option(_DEFAULT_COMPOSITE, help="Composite lean CSV."),
    targeting: str = typer.Option(_DEFAULT_TARGETING, help="Targeting CSV."),
    demographics: str = typer.Option(_DEFAULT_DEMOGRAPHICS, help="Demographics CSV."),
    redistricting: str = typer.Option(_DEFAULT_REDISTRICTING, help="Redistricting overlap CSV."),
    anomalies: str = typer.Option(_DEFAULT_ANOMALIES, help="Anomaly flags CSV."),
):
    """
    Generate PDF district profile(s) with full v2.0 layout.

    Use --district N for a single profile, or --all for all 99.
    Outputs to reports/district_profiles/ by default.
    """
    import pandas as pd
    from src.export import generate_district_profile, generate_all_profiles

    composite_df, targeting_df, demo_df = _load_standard_dfs(composite, targeting, demographics)
    redistricting_df = pd.read_csv(redistricting) if Path(redistricting).exists() else None
    anomaly_df = pd.read_csv(anomalies) if Path(anomalies).exists() else None

    if all:
        typer.echo(f"Generating all 99 district profiles → {output_dir}/")
        generate_all_profiles(
            targeting_df, composite_df, demo_df, output_dir,
            redistricting_df=redistricting_df, anomaly_df=anomaly_df,
        )
        typer.echo("Done.")
    elif district is not None:
        if not (1 <= district <= 99):
            typer.echo("ERROR: District must be between 1 and 99.", err=True)
            raise typer.Exit(1)
        out = Path(output_dir) / f"district_{district:02d}.pdf"
        typer.echo(f"Generating profile for District {district} → {out}")
        generate_district_profile(
            district, targeting_df, composite_df, demo_df, out,
            redistricting_df=redistricting_df, anomaly_df=anomaly_df,
        )
        typer.echo(f"Done: {out}")
    else:
        typer.echo("Specify --district N or --all.", err=True)
        raise typer.Exit(1)


@app.command("targets")
def run_targets(
    tier: str = typer.Option(None, help="Filter by tier (e.g. tossup, lean_r, lean_d)."),
    mode: str = typer.Option(None, help="Filter by target mode (e.g. persuasion, mobilization)."),
    pickup_only: bool = typer.Option(False, "--pickup", help="Show only pickup opportunities (R-held competitive)."),
    targeting: str = typer.Option(_DEFAULT_TARGETING, help="Targeting CSV."),
):
    """
    Show pickup targets filtered by tier and/or targeting mode.
    """
    import pandas as pd
    from src.classify import TIER_ORDER

    df = pd.read_csv(targeting)

    if pickup_only:
        df = df[df["pickup_opportunity"] == True]
    if tier:
        df = df[df["tier"] == tier.lower()]
    if mode:
        df = df[df["target_mode"] == mode.lower()]

    if df.empty:
        typer.echo("No districts match the specified filters.")
        raise typer.Exit(0)

    df = df.sort_values("composite_lean", ascending=False)
    typer.echo(
        f"\n{'Dist':>5}  {'Lean':>7}  {'Tier':12}  {'Holder':6}  {'Mode':20}  "
        f"{'Swing SD':8}  {'Flip@':8}  {'n_cont':6}"
    )
    typer.echo("-" * 85)
    for _, row in df.iterrows():
        flip = row.get("flip_threshold")
        flip_str = f"{flip*100:.1f}%" if flip is not None and not (isinstance(flip, float) and pd.isna(flip)) else "n/a"
        sd = row.get("swing_sd")
        sd_str = f"{sd:.3f}" if sd is not None and not (isinstance(sd, float) and pd.isna(sd)) else "n/a"
        typer.echo(
            f"  {int(row['district']):>3}  {row['composite_lean']:>+7.4f}  "
            f"{row['tier']:12}  {str(row['current_holder']):6}  "
            f"{str(row['target_mode']):20}  {sd_str:>8}  {flip_str:>8}  "
            f"{int(row['n_contested']):>6}"
        )
    typer.echo(f"\n{len(df)} district(s) shown.")


@app.command("scenario")
def run_scenario(
    statewide_d: float = typer.Option(None, "--statewide-d", help="Statewide D two-party share (e.g. 48.5 for 48.5%)."),
    targeting: str = typer.Option(_DEFAULT_TARGETING, help="Targeting CSV."),
    scenarios: str = typer.Option(_DEFAULT_SCENARIOS, help="Scenario table CSV."),
):
    """
    Show scenario analysis at a given statewide D% or print the full table.
    """
    import pandas as pd

    scen_df = pd.read_csv(scenarios)
    targ_df = pd.read_csv(targeting)

    if statewide_d is not None:
        # Find closest row
        pct = statewide_d / 100 if statewide_d > 1 else statewide_d
        closest = (scen_df["statewide_d_pct"] - pct * 100).abs().idxmin()
        row = scen_df.iloc[closest]
        d_seats = int(row["d_seats"])
        typer.echo(f"\nAt statewide D = {row['statewide_d_pct']:.1f}%:")
        typer.echo(f"  Projected D seats: {d_seats}/99")
        typer.echo(f"  R seats:           {99 - d_seats}/99")
        typer.echo(f"  Newly flipped:     {row.get('newly_flipped', 'none')}")
        typer.echo()
        typer.echo("Reference thresholds:")
        typer.echo("  Hold 34 (current):          ≥ 45.5% statewide D")
        typer.echo("  Reach 40 (veto threshold):  ≥ 48.5% statewide D")
        typer.echo("  Majority (50 seats):        ≥ 53.5% statewide D")
    else:
        typer.echo(f"\n{'Statewide D':>12}  {'D Seats':>9}  {'Newly Flipped'}")
        typer.echo("-" * 60)
        for _, row in scen_df.iterrows():
            flipped = str(row.get("newly_flipped", ""))
            flipped = flipped if flipped and flipped != "nan" else "—"
            typer.echo(f"  {row['statewide_d_pct']:>10.1f}%  {int(row['d_seats']):>9}  {flipped}")


@app.command("defense")
def run_defense(
    targeting: str = typer.Option(_DEFAULT_TARGETING, help="Targeting CSV."),
):
    """
    Show D-held seats at risk (defensive priorities).
    """
    import pandas as pd

    df = pd.read_csv(targeting)
    defense_df = df[df["defensive_priority"] == True].sort_values("composite_lean")

    if defense_df.empty:
        typer.echo("No defensive priorities identified.")
        raise typer.Exit(0)

    typer.echo(f"\nDefensive priorities ({len(defense_df)} seats):\n")
    typer.echo(
        f"{'Dist':>5}  {'Lean':>7}  {'Tier':12}  {'Mode':20}  "
        f"{'Margin 2024':12}  {'Cand Eff 2024':13}"
    )
    typer.echo("-" * 80)
    for _, row in defense_df.iterrows():
        margin = row.get("margin_2024")
        m_str = f"{margin*100:+.1f}%" if margin is not None and not (isinstance(margin, float) and pd.isna(margin)) else "n/a"
        ce = row.get("candidate_effect_2024")
        ce_str = f"{ce:+.3f}" if ce is not None and not (isinstance(ce, float) and pd.isna(ce)) else "n/a"
        typer.echo(
            f"  {int(row['district']):>3}  {row['composite_lean']:>+7.4f}  "
            f"{row['tier']:12}  {str(row['target_mode']):20}  "
            f"{m_str:>12}  {ce_str:>13}"
        )


@app.command("export")
def run_export(
    output_dir: str = typer.Option("exports", help="Output directory for CSV exports."),
    composite: str = typer.Option(_DEFAULT_COMPOSITE),
    targeting: str = typer.Option(_DEFAULT_TARGETING),
    demographics: str = typer.Option(_DEFAULT_DEMOGRAPHICS),
):
    """
    Export all current CSVs to a single directory.
    """
    import shutil
    import pandas as pd

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    files = [
        composite,
        targeting,
        demographics,
        "reports/session2/oh_house_actual_results.csv",
        "reports/session3/oh_house_scenario_table.csv",
        "reports/session1/oh_house_partisan_lean_2020.csv",
    ]

    copied = 0
    for f in files:
        src = Path(f)
        if src.exists():
            shutil.copy(src, out / src.name)
            typer.echo(f"  Copied {src.name}")
            copied += 1
        else:
            typer.echo(f"  SKIPPED (not found): {f}", err=True)

    typer.echo(f"\n{copied} file(s) exported to {out}/")


@app.command("methodology")
def run_methodology(
    pdf: bool = typer.Option(False, "--pdf", help="Also generate methodology.pdf."),
    md_path: str = typer.Option("reports/session5/methodology.md", help="Methodology markdown file."),
):
    """
    Print methodology summary or generate methodology PDF.
    """
    import pandas as pd
    from src.classify import TIER_ORDER

    md = Path(md_path)
    if not md.exists():
        typer.echo(f"ERROR: {md_path} not found.", err=True)
        raise typer.Exit(1)

    # Print header + section list
    typer.echo("\n=== Ohio House Election Model — Methodology Summary ===\n")
    typer.echo(f"Full document: {md_path}  ({md.stat().st_size // 1024} KB)")
    typer.echo()
    text = md.read_text(encoding="utf-8")
    for line in text.split("\n"):
        if line.startswith("## "):
            typer.echo(f"  {line[3:]}")
    typer.echo()
    typer.echo("Key parameters:")
    typer.echo("  Composite races:     9 (2018–2024)")
    typer.echo("  Weighting basis:     Governor-year heavy (2022 gov = 22.7%)")
    typer.echo("  Tier system:         Win-probability at 46/48/50% statewide D")
    typer.echo("  Tier thresholds:     Cook-style (>95% Safe, 75-95% Likely, 55-75% Lean, 45-55% Tossup)")
    typer.echo("  Regression:          GLM binomial/logit, clustered SEs by district")
    typer.echo("  D incumbency AME:    +6.7 pts")
    typer.echo("  R incumbency AME:    -6.1 pts")
    typer.echo("  Deviance explained:  62.7%")

    if pdf:
        from src.export import generate_methodology_pdf
        out = Path("reports/session5/methodology.pdf")
        typer.echo(f"\nGenerating {out} …")
        generate_methodology_pdf(md_path, out)
        typer.echo(f"Done: {out}")


@app.command("session5")
def run_session5(
    all_profiles: bool = typer.Option(True, "--all-profiles/--no-profiles", help="Generate all 99 district PDFs."),
    methodology_pdf: bool = typer.Option(True, "--methodology-pdf/--no-methodology-pdf", help="Generate methodology PDF."),
    external_csv: str = typer.Option(None, help="Path to external validation CSV (optional)."),
    composite: str = typer.Option(_DEFAULT_COMPOSITE),
    targeting: str = typer.Option(_DEFAULT_TARGETING),
    demographics: str = typer.Option(_DEFAULT_DEMOGRAPHICS),
):
    """
    Session 5: Methodology document, external validation, and reporting layer.

    Sub-task A: methodology.md already generated in reports/.
    Sub-task B: External validation against DRA (manual if no CSV provided).
    Sub-task C: Generate all district PDF profiles and methodology PDF.
    """
    import pandas as pd
    from src.export import generate_all_profiles, generate_methodology_pdf
    from src.validate_external import run_external_validation

    typer.echo("=== Ohio House Model — Session 5: Reporting Layer ===\n")

    composite_df, targeting_df, demo_df = _load_standard_dfs(composite, targeting, demographics)

    # ── Sub-Task A: Methodology doc ───────────────────────────────────────────
    md_path = Path("reports/session5/methodology.md")
    if md_path.exists():
        word_count = len(md_path.read_text().split())
        typer.echo(f"Sub-Task A: methodology.md exists ({word_count:,} words) ✓")
    else:
        typer.echo("Sub-Task A: WARNING — reports/session5/methodology.md not found.", err=True)

    # ── Sub-Task B: External validation ──────────────────────────────────────
    typer.echo("\nSub-Task B: External validation")
    ext_result = run_external_validation(composite_df, external_csv)
    typer.echo(ext_result if "MANUAL" in ext_result else "  Complete.")

    # ── Sub-Task C: PDFs ──────────────────────────────────────────────────────
    if methodology_pdf:
        typer.echo("\nSub-Task C: Generating methodology PDF …")
        try:
            generate_methodology_pdf("reports/session5/methodology.md", "reports/session5/methodology.pdf")
        except Exception as e:
            typer.echo(f"  WARNING: methodology PDF failed: {e}", err=True)

    if all_profiles:
        typer.echo("\nSub-Task C: Generating district profiles …")
        generate_all_profiles(targeting_df, composite_df, demo_df, "reports/session5/district_profiles")

    # ── Summary ───────────────────────────────────────────────────────────────
    profile_dir = Path("reports/session5/district_profiles")
    n_profiles = len(list(profile_dir.glob("*.pdf"))) if profile_dir.exists() else 0
    typer.echo(f"\n=== Session 5 complete ===")
    typer.echo(f"  Methodology doc:    reports/session5/methodology.md")
    typer.echo(f"  Methodology PDF:    reports/session5/methodology.pdf")
    typer.echo(f"  District profiles:  {n_profiles}/99 in reports/session5/district_profiles/")
    typer.echo(f"  External valid.:    {'reports/session5/external_validation.csv' if Path('reports/session5/external_validation.csv').exists() else 'manual required'}")
    typer.echo("\nDone.")


@app.command("redistricting-fix")
def run_redistricting_fix(
    composite: str = typer.Option(
        "reports/session2/oh_house_composite_lean.csv",
        help="Composite lean CSV from the 'composite' command.",
    ),
    house_results: str = typer.Option(
        "reports/session2/oh_house_actual_results.csv",
        help="Long-format house results CSV from the 'composite' command.",
    ),
    demographics: str = typer.Option(
        "data/processed/oh_house_demographics.csv",
        help="ACS demographics CSV from session4.",
    ),
    targeting_out: str = typer.Option(
        "reports/session3/oh_house_targeting.csv",
        help="Targeting CSV to update.",
    ),
    regression_out: str = typer.Option(
        "reports/session4/regression_summary.txt",
        help="Regression summary to update.",
    ),
):
    """
    Redistricting fix: identify districts whose pre-2022 house race results are
    from a different electoral geography, drop contaminated observations, and
    rerun swing metrics + regression.

    Steps:
      1. Compare 2020 vs 2022 SOS precinct-to-district assignments (Jaccard overlap).
      2. Drop 2018/2020 house results for relocated/redrawn districts.
      3. Rerun district classification and swing metrics.
      4. Rerun GLM/OLS regression on the cleaned dataset.
      5. Write updated targeting CSV, regression summary, and validation report.
    """
    import pandas as pd
    from datetime import datetime
    from src.ingest_sos import load_sos_file
    from src.validate import check_precinct_redistricting_overlap
    from src.ingest_house_results import (
        apply_redistricting_filter,
        apply_redistricting_filter_to_composite,
    )
    from src.classify import build_targeting_df, TIER_ORDER
    from src.model import run_regression, format_regression_summary

    typer.echo("=== Ohio House Model — Redistricting Contamination Fix ===\n")

    # ── Load SOS files (need pre and post for overlap check) ─────────────────
    typer.echo("Loading SOS files for precinct overlap check …")
    sos_files: dict = {}
    for year, path in SOS_FILES.items():
        p = Path(path)
        if p.exists():
            sos_files[year] = load_sos_file(p)
        else:
            typer.echo(f"  {year}: not found at {path} — skipping.")
    if not sos_files:
        typer.echo("ERROR: No SOS files found.", err=True)
        raise typer.Exit(1)

    # ── Step 1: Precinct overlap check ────────────────────────────────────────
    typer.echo("\nStep 1: Computing precinct membership overlap (2020→2022 and 2022→2024) …")
    overlap_df = check_precinct_redistricting_overlap(
        sos_files,
        output_path="reports/redistricting_overlap.csv",
    )

    n_relocated = int((overlap_df["overlap_category"] == "relocated").sum())
    n_redrawn = int((overlap_df["overlap_category"] == "redrawn").sum())
    n_contaminated = n_relocated + n_redrawn

    # ── Step 2: Load and filter house results ─────────────────────────────────
    typer.echo("\nStep 2: Applying redistricting filter …")
    house_long_path = Path(house_results)
    composite_path = Path(composite)

    if not house_long_path.exists():
        typer.echo(f"ERROR: {house_long_path} not found.", err=True)
        raise typer.Exit(1)
    if not composite_path.exists():
        typer.echo(f"ERROR: {composite_path} not found.", err=True)
        raise typer.Exit(1)

    house_long_raw = pd.read_csv(house_long_path)
    composite_raw = pd.read_csv(composite_path)
    demographics_df = pd.read_csv(demographics)

    house_long_clean, filter_summary = apply_redistricting_filter(house_long_raw, overlap_df)
    composite_clean = apply_redistricting_filter_to_composite(composite_raw, overlap_df)

    # ── Step 3: Rerun targeting + swing metrics ───────────────────────────────
    typer.echo("\nStep 3: Rerunning district classification with cleaned data …")

    # Before-state snapshot
    targeting_old = pd.read_csv(targeting_out) if Path(targeting_out).exists() else None
    old_mode_counts = targeting_old["target_mode"].value_counts().to_dict() if targeting_old is not None else {}

    targeting_new = build_targeting_df(composite_clean, house_long_clean)

    # Before/after comparison
    typer.echo("\n--- Targeting mode: before vs after redistricting fix ---")
    typer.echo(f"  {'Mode':<22} {'Before':>8}  {'After':>8}  {'Delta':>8}")
    typer.echo(f"  {'-'*52}")
    all_modes = sorted(set(list(old_mode_counts.keys()) + list(targeting_new["target_mode"].value_counts().keys())))
    for mode in all_modes:
        before = old_mode_counts.get(mode, 0)
        after = int((targeting_new["target_mode"] == mode).sum())
        delta = after - before
        typer.echo(f"  {mode:<22} {before:>8}  {after:>8}  {delta:>+8}")

    # Districts whose mode changed
    if targeting_old is not None:
        merged_modes = targeting_old[["district", "target_mode"]].merge(
            targeting_new[["district", "target_mode"]],
            on="district",
            suffixes=("_old", "_new"),
        )
        changed = merged_modes[merged_modes["target_mode_old"] != merged_modes["target_mode_new"]]
        if len(changed):
            typer.echo(f"\n  Districts with changed targeting mode ({len(changed)}):")
            typer.echo(f"  {'Dist':>6}  {'Old mode':<22}  {'New mode':<22}")
            for _, row in changed.iterrows():
                typer.echo(f"  {int(row['district']):>6}  {row['target_mode_old']:<22}  {row['target_mode_new']:<22}")
        else:
            typer.echo("\n  No targeting modes changed.")

    # District 52 sanity check
    d52_new = targeting_new[targeting_new["district"] == 52]
    if not d52_new.empty:
        d52_row = d52_new.iloc[0]
        typer.echo(f"\n  District 52 sanity check:")
        typer.echo(f"    swing_sd:     {d52_row.get('swing_sd', 'N/A')}")
        typer.echo(f"    n_contested:  {d52_row.get('n_contested', 'N/A')}")
        typer.echo(f"    target_mode:  {d52_row.get('target_mode', 'N/A')}")

    # ── Step 4: Rerun regression ──────────────────────────────────────────────
    typer.echo("\nStep 4: Rerunning regression on cleaned dataset …")
    ols_old_r2 = None
    ols_old_inc_r = None
    if Path(regression_out).exists():
        old_txt = Path(regression_out).read_text()
        import re as _re
        m = _re.search(r"R²:\s+([\d.]+)", old_txt)
        if m:
            ols_old_r2 = float(m.group(1))
        m2 = _re.search(r"incumbent_party.*?T\.R_inc.*?([-\d.]+)", old_txt)
        if m2:
            ols_old_inc_r = float(m2.group(1))

    ols_new, glm_new, reg_df_new = run_regression(composite_clean, demographics_df)
    regression_text = format_regression_summary(ols_new, glm_new, reg_df_new)
    Path(regression_out).write_text(regression_text, encoding="utf-8")
    typer.echo(f"  Regression summary written to {regression_out}")

    # Print stability check
    d_inc_key = "C(incumbent_party, Treatment('open'))[T.D_inc]"
    r_inc_key = "C(incumbent_party, Treatment('open'))[T.R_inc]"
    new_r_inc = float(ols_new.params.get(r_inc_key, float("nan")))
    typer.echo(f"\n  Regression stability:")
    typer.echo(f"    Observations: {len(reg_df_new)} (after filter)")
    if ols_old_r2 is not None:
        typer.echo(f"    OLS R²: {ols_old_r2:.4f} → {ols_new.rsquared:.4f}  (delta {ols_new.rsquared - ols_old_r2:+.4f})")
    else:
        typer.echo(f"    OLS R²: {ols_new.rsquared:.4f}")
    if ols_old_inc_r is not None:
        delta_inc = new_r_inc - ols_old_inc_r
        typer.echo(f"    R incumbency OLS coef: {ols_old_inc_r:.4f} → {new_r_inc:.4f}  (delta {delta_inc:+.4f})")
        if abs(delta_inc) < 0.01:
            typer.echo("    PASS: Incumbency estimate stable (delta < 1 pt) — contamination impact was minor.")
        elif abs(delta_inc) < 0.02:
            typer.echo("    NOTE: Incumbency estimate shifted 1–2 pts — minor bias from contamination.")
        else:
            typer.echo("    WARNING: Incumbency estimate shifted > 2 pts — contamination was materially biasing results.")

    # ── Step 5: Write updated outputs ─────────────────────────────────────────
    typer.echo("\nStep 5: Writing updated outputs …")

    Path(targeting_out).parent.mkdir(parents=True, exist_ok=True)
    targeting_new.to_csv(targeting_out, index=False, float_format="%.6f")
    typer.echo(f"  Targeting CSV updated:     {targeting_out}")

    composite_out = "reports/session2/oh_house_composite_lean.csv"
    composite_clean.to_csv(composite_out, index=False, float_format="%.6f")
    typer.echo(f"  Composite CSV updated:     {composite_out}")

    house_out = "reports/session2/oh_house_actual_results.csv"
    house_long_clean.to_csv(house_out, index=False, float_format="%.6f")
    typer.echo(f"  House results CSV updated: {house_out}")

    # ── Validation summary ────────────────────────────────────────────────────
    val_lines = [
        "Ohio House Model — Redistricting Fix Validation",
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "=" * 60,
        "  Step 1: Precinct Overlap Check",
        "=" * 60,
        f"  Comparing 2020 vs 2022 SOS precinct-to-district assignments.",
        f"  Same (Jaccard ≥ 0.70):        {int((overlap_df['overlap_category']=='same').sum()):3d} districts",
        f"  Redrawn (0.30–0.70):           {n_redrawn:3d} districts",
        f"  Relocated (Jaccard < 0.30):    {n_relocated:3d} districts",
        f"  Total contaminated:            {n_contaminated:3d} districts",
        "",
    ]
    if n_relocated > 0:
        relocated_list = overlap_df[overlap_df["overlap_category"]=="relocated"].sort_values("jaccard_similarity")
        val_lines.append("  Relocated districts:")
        for _, row in relocated_list.iterrows():
            val_lines.append(f"    District {int(row['district']):3d}  Jaccard={row['jaccard_similarity']:.4f}")
    if n_redrawn > 0:
        redrawn_list = overlap_df[overlap_df["overlap_category"]=="redrawn"].sort_values("jaccard_similarity")
        val_lines.append("  Redrawn districts:")
        for _, row in redrawn_list.iterrows():
            val_lines.append(f"    District {int(row['district']):3d}  Jaccard={row['jaccard_similarity']:.4f}")
    val_lines += [
        "",
        "=" * 60,
        "  Step 2: Filter Applied",
        "=" * 60,
        f"  Observations before filter: {filter_summary['n_obs_before']}",
        f"  Observations dropped:       {filter_summary['n_dropped']}",
        f"  Observations after filter:  {filter_summary['n_obs_after']}",
        "",
        "=" * 60,
        "  Step 3: Targeting Mode Changes",
        "=" * 60,
    ]
    if targeting_old is not None and len(changed):
        val_lines.append(f"  {len(changed)} districts changed targeting mode:")
        for _, row in changed.iterrows():
            val_lines.append(f"    District {int(row['district']):3d}: {row['target_mode_old']} → {row['target_mode_new']}")
    else:
        val_lines.append("  No targeting modes changed.")
    val_lines += [
        "",
        "=" * 60,
        "  Step 4: Regression Stability",
        "=" * 60,
        f"  Observations: {len(reg_df_new)} (after filter)",
        f"  OLS R²: {ols_new.rsquared:.4f}",
        f"  R incumbency OLS coef: {new_r_inc:.4f}",
        "",
        "  District 52 sanity check:",
    ]
    if not d52_new.empty:
        val_lines.append(f"    swing_sd:    {d52_row.get('swing_sd', 'N/A')}")
        val_lines.append(f"    n_contested: {d52_row.get('n_contested', 'N/A')}")
        val_lines.append(f"    target_mode: {d52_row.get('target_mode', 'N/A')}")
    val_lines += [
        "",
        "=" * 60,
        "  Composite lean: UNCHANGED (built from statewide races only).",
        "  Tier assignments: UNCHANGED.",
        "  Scenario table: UNCHANGED.",
        "  External validation: UNCHANGED.",
        "=" * 60,
    ]

    val_path = Path("reports/validation_summary_redistricting_fix.txt")
    val_path.write_text("\n".join(val_lines), encoding="utf-8")
    typer.echo(f"  Validation summary:        {val_path}")
    typer.echo("\nDone.")


@app.command("open-seats")
def run_open_seats(
    targeting: str = typer.Option(_DEFAULT_TARGETING, help="Targeting CSV."),
    output: str = typer.Option(
        "reports/oh_house_2026_opportunities.txt",
        help="Output path for the 2026 opportunities report.",
    ),
    ladder_output: str = typer.Option(
        "reports/session3/oh_house_pickup_ladder.txt",
        help="Path to write updated pickup ladder.",
    ),
    composite: str = typer.Option(_DEFAULT_COMPOSITE, help="Composite lean CSV."),
):
    """
    Session 6: 2026 open seat intelligence and updated pickup ladder.

    Reads the current targeting CSV, applies OPEN_SEATS_2026 intelligence,
    and generates:
      - reports/oh_house_2026_opportunities.txt
      - Updated reports/session3/oh_house_pickup_ladder.txt

    The headline: how much easier is 40 seats in 2026 vs 2024?
    """
    import pandas as pd
    from src.scenarios import (
        run_scenario_table,
        build_pickup_ladder,
        build_defensive_list,
        format_pickup_ladder,
        build_2026_opportunities_report,
    )

    typer.echo("=== Ohio House Model — Session 6: 2026 Open Seat Intelligence ===\n")

    targ_df = pd.read_csv(targeting)
    typer.echo(f"Loaded {len(targ_df)} districts from {Path(targeting).name}")

    # ── 2026 open seat summary ────────────────────────────────────────────────
    from src.classify import OPEN_SEATS_2026
    n_open_r = sum(
        1 for k in OPEN_SEATS_2026
        if targ_df[targ_df["district"] == k]["current_holder"].squeeze() == "R"
    )
    n_open_d = sum(
        1 for k in OPEN_SEATS_2026
        if targ_df[targ_df["district"] == k]["current_holder"].squeeze() == "D"
    )
    typer.echo(f"Known 2026 open seats: {len(OPEN_SEATS_2026)} total — {n_open_r} R-held, {n_open_d} D-held")

    # ── Generate opportunities report ─────────────────────────────────────────
    typer.echo("\nBuilding 2026 opportunities report …")
    report_text = build_2026_opportunities_report(targ_df)
    typer.echo("\n" + report_text)

    out_path = Path(output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(report_text, encoding="utf-8")
    typer.echo(f"\n2026 opportunities report written to {out_path}")

    # ── Regenerate pickup ladder with 2026 annotations ────────────────────────
    typer.echo("\nRegenerating pickup ladder with 2026 open seat annotations …")
    scenario_df  = run_scenario_table(targ_df)
    ladder_df    = build_pickup_ladder(targ_df)
    defensive_df = build_defensive_list(targ_df)

    ladder_text = format_pickup_ladder(ladder_df, scenario_df, defensive_df)
    typer.echo("\n" + ladder_text)

    ladder_path = Path(ladder_output)
    ladder_path.parent.mkdir(parents=True, exist_ok=True)
    ladder_path.write_text(ladder_text, encoding="utf-8")
    typer.echo(f"\nPickup ladder written to {ladder_path}")

    typer.echo("\nDone.")


@app.command("ask")
def run_ask(
    question: str = typer.Argument(..., help="Question to ask about the Ohio House model data."),
    model: str = typer.Option("claude-opus-4-6", help="Claude model ID."),
):
    """
    Ask a single question about the Ohio House model data.

    Uses the Claude API with live data as context.
    Requires ANTHROPIC_API_KEY to be set in the environment.

    Examples:
      python cli.py ask "What are the top 5 pickup targets?"
      python cli.py ask "How many seats do Democrats win at 48.5% statewide?"
      python cli.py ask "Which districts have unreliable 2022 house data?"
    """
    from src.query import ask, build_system_prompt

    typer.echo(f"Building context from live data files …", err=True)
    system_prompt = build_system_prompt()
    typer.echo(f"Querying {model} …\n", err=True)

    answer = ask(question, model=model, system_prompt=system_prompt)
    typer.echo(answer)


@app.command("chat")
def run_chat(
    model: str = typer.Option("claude-opus-4-6", help="Claude model ID."),
):
    """
    Start an interactive multi-turn chat about the Ohio House model data.

    Maintains conversation history within the session.
    Requires ANTHROPIC_API_KEY to be set in the environment.
    Type 'quit' or Ctrl-C to end.
    """
    from src.query import chat, build_system_prompt

    typer.echo("Building context from live data files …", err=True)
    system_prompt = build_system_prompt()

    chat(model=model, system_prompt=system_prompt)


_DEFAULT_VOTER_UNIVERSE = "data/processed/oh_house_voter_universe.csv"
_DEFAULT_CLEAN_PARQUET  = "data/processed/voter_file_clean.parquet"
_DEFAULT_VOTERFILE_DIR  = "data/voterfiles"


@app.command("voters")
def run_voters(
    district: int  = typer.Option(None, "--district",  help="Show voter universe for a specific district."),
    target:   str  = typer.Option(None, "--target",    help="Filter type for --export: mobilization / persuasion / all_targets."),
    export:   str  = typer.Option(None, "--export",    help="Export contact list to this CSV path (requires --district and --target)."),
    targets_only: bool = typer.Option(False, "--targets-only", help="Print mobilization/persuasion summary for all target districts."),
    build:    bool = typer.Option(False, "--build",    help="Build voter file parquet + universe CSV from raw SOS files."),
    force:    bool = typer.Option(False, "--force",    help="Force rebuild of parquet even if it already exists."),
    voterfile_dir: str = typer.Option(_DEFAULT_VOTERFILE_DIR, "--voterfile-dir", help="Directory containing SWVF_*.txt files."),
    parquet:  str  = typer.Option(_DEFAULT_CLEAN_PARQUET,     "--parquet",       help="Path to clean voter file parquet."),
    universe: str  = typer.Option(_DEFAULT_VOTER_UNIVERSE,    "--universe",      help="Path to voter universe CSV."),
):
    """
    Voter file analysis, targeting universes, and contact list export.

    Build pipeline:
      python cli.py voters --build              # ingest + score + aggregate (~10 min)
      python cli.py voters --build --force      # rebuild even if parquet exists

    District queries:
      python cli.py voters --district 52                                  # voter universe summary
      python cli.py voters --district 52 --target mobilization --export contacts_d52.csv

    Statewide summaries:
      python cli.py voters --targets-only       # mob/persuasion counts for all realistic targets

    PII policy: --export writes voter_id + classification scores only.
    Operatives join on voter_id to their own SOS/VAN copy for contact details.
    """
    import pandas as pd
    from pathlib import Path
    from src.voterfile import (
        load_voter_file,
        build_voter_universe,
        export_contact_universe,
        format_district_voter_summary,
    )

    # ── Build mode ────────────────────────────────────────────────────────────
    if build:
        typer.echo("=== Ohio House Model — Voter File Build ===\n")
        typer.echo("Step 1: Ingesting and scoring voter file …")
        typer.echo(f"  Source: {voterfile_dir}/SWVF_*.txt")
        typer.echo(f"  Output: {parquet}")
        load_voter_file(
            voterfile_dir=voterfile_dir,
            output_parquet=parquet,
            force=force,
        )

        typer.echo("\nStep 2: Building district voter universe …")
        typer.echo(f"  Output: {universe}")
        universe_df = build_voter_universe(
            parquet_path=parquet,
            output_csv=universe,
        )

        # Merge voter universe into targeting CSV if it exists
        targeting_path = Path(_DEFAULT_TARGETING)
        if targeting_path.exists():
            typer.echo("\nStep 3: Merging voter universe into targeting CSV …")
            from src.voterfile import merge_voter_universe_into_targeting
            targeting_df = pd.read_csv(targeting_path)
            updated_targeting = merge_voter_universe_into_targeting(targeting_df, universe_df)
            updated_targeting.to_csv(targeting_path, index=False, float_format="%.6f")
            typer.echo(f"  Targeting CSV updated: {targeting_path}")

            n_mob  = updated_targeting.get("n_mobilization_targets", pd.Series(dtype=float)).sum()
            n_pers = updated_targeting.get("n_persuasion_targets", pd.Series(dtype=float)).sum()
            typer.echo(f"\n  Statewide contact universes (across all 99 districts):")
            typer.echo(f"    D-leaning low-propensity voters:    {int(n_mob):>10,}")
            typer.echo(f"    Unaffiliated/crossover regular voters: {int(n_pers):>10,}")

        typer.echo("\nDone.")
        return

    # ── Ensure universe CSV exists ────────────────────────────────────────────
    universe_path = Path(universe)
    if not universe_path.exists():
        typer.echo(
            f"Voter universe not found at {universe_path}. "
            "Run: python cli.py voters --build",
            err=True,
        )
        raise typer.Exit(1)

    universe_df = pd.read_csv(universe_path)

    # ── Export contact list ───────────────────────────────────────────────────
    if export:
        if district is None:
            typer.echo("ERROR: --export requires --district.", err=True)
            raise typer.Exit(1)
        if not target:
            typer.echo("ERROR: --export requires --target (mobilization / persuasion / all_targets).", err=True)
            raise typer.Exit(1)
        n = export_contact_universe(
            district=district,
            target_type=target,
            output_path=export,
            parquet_path=parquet,
        )
        typer.echo(f"Exported {n:,} {target} contacts for District {district} → {export}")
        typer.echo(f"(voter_id + scores only — join to SOS/VAN for contact details)")
        return

    # ── Single district summary ───────────────────────────────────────────────
    if district is not None:
        typer.echo(format_district_voter_summary(district, universe_df))
        return

    # ── All targets summary ───────────────────────────────────────────────────
    if targets_only:
        targeting_path = Path(_DEFAULT_TARGETING)
        if not targeting_path.exists():
            typer.echo(f"Targeting CSV not found at {targeting_path}.", err=True)
            raise typer.Exit(1)
        targeting_df = pd.read_csv(targeting_path)

        # Join voter universe to targeting
        pickup_col = "pickup_opportunity"
        open_col   = "open_seat_2026"
        if pickup_col not in targeting_df.columns:
            typer.echo("Targeting CSV missing 'pickup_opportunity' column.", err=True)
            raise typer.Exit(1)

        targets = targeting_df[targeting_df[pickup_col] == True].merge(
            universe_df[["district", "total_active_voters", "n_mobilization_targets",
                         "pct_mobilization_targets", "n_persuasion_targets",
                         "pct_persuasion_targets", "partisan_advantage"]],
            on="district",
            how="left",
        ).sort_values("composite_lean", ascending=False)

        typer.echo(f"\nVoter universe — all pickup opportunities ({len(targets)} districts)\n")
        header = (
            f"  {'HD':>4}  {'Tier':<9}  {'Open':>4}  {'Active Reg':>10}  "
            f"{'D-lean Low-prop':>15}  {'%':>5}  {'UA/Cross Regular':>16}  {'%':>5}  {'Adv':>6}"
        )
        typer.echo(header)
        typer.echo("  " + "-" * (len(header) - 2))

        for _, row in targets.iterrows():
            is_open = "Y" if row.get(open_col) else " "
            active  = int(row.get("total_active_voters") or 0)
            mob_n   = int(row.get("n_mobilization_targets") or 0)
            mob_p   = row.get("pct_mobilization_targets") or 0
            pers_n  = int(row.get("n_persuasion_targets") or 0)
            pers_p  = row.get("pct_persuasion_targets") or 0
            adv     = row.get("partisan_advantage") or 0
            typer.echo(
                f"  {int(row['district']):>4}  {row['tier']:<9}  {is_open:>4}  "
                f"{active:>10,}  {mob_n:>15,}  {mob_p:>5.1%}  "
                f"{pers_n:>16,}  {pers_p:>5.1%}  {adv:>+6.3f}"
            )
        typer.echo(f"\n  Totals across {len(targets)} pickup opportunities:")
        typer.echo(f"    D-leaning low-propensity voters:        {int(targets['n_mobilization_targets'].sum()):>10,}")
        typer.echo(f"    Unaffiliated/crossover regular voters:  {int(targets['n_persuasion_targets'].sum()):>10,}")
        return

    # ── Default: statewide summary ────────────────────────────────────────────
    total_active = universe_df["total_active_voters"].sum()
    total_mob    = universe_df["n_mobilization_targets"].sum()
    total_pers   = universe_df["n_persuasion_targets"].sum()

    typer.echo(f"\nStatewide voter universe (99 districts):\n")
    typer.echo(f"  Active registered voters:                {total_active:>10,}")
    typer.echo(f"  D-leaning low-propensity voters:         {int(total_mob):>10,}  ({total_mob/total_active:.1%})")
    typer.echo(f"  Unaffiliated/crossover regular voters:   {int(total_pers):>10,}  ({total_pers/total_active:.1%})")
    typer.echo(f"\n  Run with --targets-only to see all pickup opportunities.")
    typer.echo(f"  Run with --district N to see a specific district.")


@app.command("one-pager")
def run_one_pager(
    output: str = typer.Option("reports/ohio_house_2026_one_pager.pdf", "--output", help="Output PDF path."),
    author: str = typer.Option("", "--author", help="Attribution line for footer (optional)."),
    targeting: str = typer.Option(_DEFAULT_TARGETING, "--targeting"),
    scenarios: str = typer.Option(_DEFAULT_SCENARIOS, "--scenarios"),
    universe:  str = typer.Option(_DEFAULT_VOTER_UNIVERSE, "--universe"),
):
    """
    Generate a one-page pitch PDF for circulation to Ohio Democratic stakeholders.

    Includes: realistic pickup targets, open seat opportunities, path to 40 seats,
    defensive priorities, methodology credibility note, and voter file headline numbers.

      python cli.py one-pager
      python cli.py one-pager --author "Alex McEvoy"
      python cli.py one-pager --output reports/pitch_march_2026.pdf
    """
    import pandas as pd
    from pathlib import Path
    from src.export import generate_one_pager

    targeting_df = pd.read_csv(targeting)
    scenario_df  = pd.read_csv(scenarios)

    voter_universe_df = None
    if Path(universe).exists():
        voter_universe_df = pd.read_csv(universe)

    typer.echo(f"Generating one-pager → {output}")
    generate_one_pager(
        targeting_df=targeting_df,
        scenario_df=scenario_df,
        output_path=output,
        voter_universe_df=voter_universe_df,
        author=author,
    )


# ---------------------------------------------------------------------------
# Session 8: Probabilistic scenarios + resource allocation
# ---------------------------------------------------------------------------

_DEFAULT_SESSION8_DIR = "reports/session8"


@app.command("simulate")
def run_simulate(
    statewide_d: float = typer.Option(None, "--statewide-d",
        help="Statewide D% (e.g. 48.5). If omitted, runs full 40–55% sweep."),
    n_sims: int = typer.Option(10_000, "--n-sims", help="Number of Monte Carlo simulations."),
    with_incumbency: bool = typer.Option(False, "--with-incumbency",
        help="Apply literature incumbency adjustment for 2026 (confirmed districts only)."),
    targeting: str = typer.Option(_DEFAULT_TARGETING, help="Targeting CSV."),
    composite: str = typer.Option(_DEFAULT_COMPOSITE, help="Composite lean CSV."),
    seed: int = typer.Option(42, "--seed", help="Random seed for reproducibility."),
):
    """
    Session 8: Probabilistic scenario analysis with district-level uncertainty.

    Runs Monte Carlo simulations with Empirical Bayes shrinkage of district noise.
    Without --statewide-d, sweeps 40–55% and writes full output CSVs.
    With --statewide-d, prints summary for that single environment.
    """
    import pandas as pd
    from src.simulate import (
        SimConfig, compute_sigma_prior, estimate_district_sigma,
        run_simulations, run_probabilistic_scenario_table,
        build_investment_priority, build_district_win_prob_table,
    )

    targeting_df = pd.read_csv(targeting)
    composite_df = pd.read_csv(composite)

    config = SimConfig(
        n_sims=n_sims,
        include_incumbency=with_incumbency,
        random_seed=seed,
    )

    # Estimate district-level uncertainty
    sigma_prior = compute_sigma_prior(composite_df)
    sigma_df = estimate_district_sigma(targeting_df, sigma_prior, config)

    typer.echo(f"σ_prior (pooled candidate effect std): {sigma_prior:.4f}")
    typer.echo(f"Incumbency adjustment: {'ON (confirmed only)' if with_incumbency else 'OFF'}")
    typer.echo(f"Simulations per environment: {n_sims:,}\n")

    sigma_summary = sigma_df["sigma_source"].value_counts()
    for source, count in sigma_summary.items():
        typer.echo(f"  {source}: {count} districts")
    typer.echo()

    if statewide_d is not None:
        # Single-point mode
        sw = statewide_d / 100.0 if statewide_d > 1 else statewide_d
        result = run_simulations(targeting_df, sigma_df, sw, config)

        typer.echo(f"=== Probabilistic scenario at {sw:.1%} statewide D ===\n")
        typer.echo(f"  Mean D seats:   {result.mean_seats:.1f}")
        typer.echo(f"  Median:         {result.median_seats}")
        typer.echo(f"  80% CI:         [{result.p10_seats}, {result.p90_seats}]")
        typer.echo(f"  50% CI:         [{result.p25_seats}, {result.p75_seats}]")
        typer.echo(f"  Std dev:        {result.std_seats:.2f}")
        typer.echo(f"  P(hold ≥34):    {(result.seat_counts >= 34).mean():.1%}")
        typer.echo(f"  P(reach ≥40):   {(result.seat_counts >= 40).mean():.1%}")
        typer.echo(f"  P(majority 50): {(result.seat_counts >= 50).mean():.1%}")
        return

    # Full sweep mode
    typer.echo("Running probabilistic scenario sweep (40%–55%) …")
    out_dir = Path(_DEFAULT_SESSION8_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)

    scenario_df, results = run_probabilistic_scenario_table(
        targeting_df, sigma_df, config=config,
    )
    scenario_path = out_dir / "oh_house_probabilistic_scenarios.csv"
    scenario_df.to_csv(scenario_path, index=False)
    typer.echo(f"  → {scenario_path}")

    # District win prob table
    typer.echo("Building district win probability table …")
    wp_table = build_district_win_prob_table(targeting_df, sigma_df, config=config)
    wp_path = out_dir / "oh_house_district_win_probs.csv"
    wp_table.to_csv(wp_path, index=False)
    typer.echo(f"  → {wp_path}")

    # Investment priority at 48%
    typer.echo("Computing investment priorities at 48% statewide D …")
    invest_df = build_investment_priority(targeting_df, sigma_df, 0.48, config)
    invest_path = out_dir / "oh_house_investment_priority.csv"
    invest_df.to_csv(invest_path, index=False, float_format="%.4f")
    typer.echo(f"  → {invest_path}")

    # Print summary table
    typer.echo("\n=== Probabilistic Scenario Summary ===\n")
    typer.echo(f"{'Statewide D':>12}  {'Mean':>5}  {'Median':>6}  {'80% CI':>10}  "
               f"{'P(≥34)':>7}  {'P(≥40)':>7}  {'P(50)':>7}")
    typer.echo("-" * 72)
    for _, row in scenario_df.iterrows():
        typer.echo(
            f"{row['statewide_d_pct']:>11.1f}%  "
            f"{row['mean_d_seats']:>5.1f}  "
            f"{row['median_d_seats']:>6}  "
            f"[{row['p10_seats']:>2}, {row['p90_seats']:>2}]   "
            f"{row['prob_hold_34']:>6.1%}  "
            f"{row['prob_reach_40']:>6.1%}  "
            f"{row['prob_majority']:>6.1%}"
        )


@app.command("invest")
def run_invest(
    statewide_d: float = typer.Option(48.0, "--statewide-d",
        help="Assumed statewide D environment (e.g. 48.0)."),
    target_seats: int = typer.Option(40, "--target",
        help="Seat target to optimize toward."),
    max_districts: int = typer.Option(15, "--max-districts",
        help="Max districts in investment portfolio."),
    with_incumbency: bool = typer.Option(False, "--with-incumbency"),
    targeting: str = typer.Option(_DEFAULT_TARGETING, help="Targeting CSV."),
    composite: str = typer.Option(_DEFAULT_COMPOSITE, help="Composite lean CSV."),
    seed: int = typer.Option(42, "--seed"),
):
    """
    Resource allocation optimizer: which districts to invest in.

    Greedy algorithm selects districts that most increase P(target seats).
    Shows marginal win probability and expected seat gain per district.
    """
    import pandas as pd
    from src.simulate import (
        SimConfig, compute_sigma_prior, estimate_district_sigma,
        optimize_path_to_target,
    )

    targeting_df = pd.read_csv(targeting)
    composite_df = pd.read_csv(composite)

    config = SimConfig(
        include_incumbency=with_incumbency,
        random_seed=seed,
    )

    sw = statewide_d / 100.0 if statewide_d > 1 else statewide_d

    sigma_prior = compute_sigma_prior(composite_df)
    sigma_df = estimate_district_sigma(targeting_df, sigma_prior, config)

    typer.echo(f"=== Path to {target_seats} seats at {sw:.1%} statewide D ===\n")
    typer.echo(f"Investment delta: {config.investment_delta:.0%} lean shift per unit")
    typer.echo(f"Incumbency: {'ON' if with_incumbency else 'OFF'}\n")

    path_df = optimize_path_to_target(
        targeting_df, sigma_df, sw,
        target_seats=target_seats,
        max_districts=max_districts,
        config=config,
    )

    out_dir = Path(_DEFAULT_SESSION8_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)
    path_path = out_dir / "oh_house_path_optimizer.csv"
    path_df.to_csv(path_path, index=False)
    typer.echo(f"  → {path_path}\n")

    # Print table
    typer.echo(f"{'Rank':>4}  {'Dist':>4}  {'Base WP':>8}  {'After':>8}  "
               f"{'Δ Seats':>8}  {'Cum E[seats]':>12}  {'P(≥' + str(target_seats) + ')':>8}")
    typer.echo("-" * 64)
    for _, row in path_df.iterrows():
        typer.echo(
            f"{int(row['priority_rank']):>4}  "
            f"{int(row['district']):>4}  "
            f"{row['baseline_wp']:>7.1%}  "
            f"{row['invested_wp']:>7.1%}  "
            f"{row['marginal_gain']:>+7.3f}  "
            f"{row['cumulative_expected_seats']:>12.1f}  "
            f"{row['cumulative_prob_target']:>7.1%}"
        )


@app.command("win-prob")
def run_win_prob(
    district: int = typer.Option(None, "--district",
        help="Show win probability curve for a single district."),
    statewide_d: float = typer.Option(None, "--statewide-d",
        help="Show all district win probs at this environment."),
    with_incumbency: bool = typer.Option(False, "--with-incumbency"),
    targeting: str = typer.Option(_DEFAULT_TARGETING, help="Targeting CSV."),
    composite: str = typer.Option(_DEFAULT_COMPOSITE, help="Composite lean CSV."),
):
    """
    District-level win probabilities.

    --district: shows win prob across statewide environments for one district.
    --statewide-d: shows all districts' win probs at one environment.
    Both omitted: shows realistic targets' win probs at 48% statewide.
    """
    import pandas as pd
    from src.simulate import (
        SimConfig, compute_sigma_prior, estimate_district_sigma,
        compute_analytical_win_probs, district_win_prob_curve,
    )

    targeting_df = pd.read_csv(targeting)
    composite_df = pd.read_csv(composite)

    config = SimConfig(include_incumbency=with_incumbency)
    sigma_prior = compute_sigma_prior(composite_df)
    sigma_df = estimate_district_sigma(targeting_df, sigma_prior, config)

    if district is not None:
        # Single district curve
        typer.echo(f"=== Win probability curve for District {district} ===\n")
        curve = district_win_prob_curve(targeting_df, sigma_df, district, config=config)
        if curve.empty:
            typer.echo(f"District {district} not found.", err=True)
            raise typer.Exit(1)

        typer.echo(f"{'Statewide D':>12}  {'Win Prob':>9}  {'Margin':>7}")
        typer.echo("-" * 34)
        for _, row in curve.iterrows():
            typer.echo(
                f"{row['statewide_d_pct']:>11.1f}%  "
                f"{row['win_prob']:>8.1%}  "
                f"{row['margin']:>+6.3f}"
            )
        return

    if statewide_d is not None:
        sw = statewide_d / 100.0 if statewide_d > 1 else statewide_d
    else:
        sw = 0.48

    typer.echo(f"=== District win probabilities at {sw:.1%} statewide D ===\n")
    wp_df = compute_analytical_win_probs(targeting_df, sigma_df, sw, config)
    wp_df = wp_df.merge(
        targeting_df[["district", "tier", "current_holder"]], on="district"
    )

    # Show competitive districts (win_prob between 10% and 90%)
    competitive = wp_df[(wp_df["win_prob"] > 0.10) & (wp_df["win_prob"] < 0.90)]
    competitive = competitive.sort_values("win_prob", ascending=False)

    typer.echo(f"{'Dist':>4}  {'Lean':>6}  {'Tier':>10}  {'Holder':>6}  "
               f"{'Win Prob':>9}  {'Marg WP':>8}")
    typer.echo("-" * 52)
    for _, row in competitive.iterrows():
        typer.echo(
            f"{row['district']:>4}  "
            f"{row['composite_lean']:>+5.3f}  "
            f"{row['tier']:>10}  "
            f"{row['current_holder']:>6}  "
            f"{row['win_prob']:>8.1%}  "
            f"{row['marginal_wp']:>7.3f}"
        )

    safe_d = len(wp_df[wp_df["win_prob"] >= 0.90])
    safe_r = len(wp_df[wp_df["win_prob"] <= 0.10])
    typer.echo(f"\n  Safe D (≥90%): {safe_d}  |  Competitive: {len(competitive)}  |  Safe R (≤10%): {safe_r}")


@app.command("session8")
def run_session8(
    n_sims: int = typer.Option(10_000, "--n-sims"),
    with_incumbency: bool = typer.Option(False, "--with-incumbency"),
    targeting: str = typer.Option(_DEFAULT_TARGETING),
    composite: str = typer.Option(_DEFAULT_COMPOSITE),
    seed: int = typer.Option(42, "--seed"),
):
    """
    Full Session 8 pipeline: sigma estimation → probabilistic sweep →
    investment priorities → defensive scenarios.
    """
    import pandas as pd
    from src.simulate import (
        SimConfig, compute_sigma_prior, estimate_district_sigma,
        run_probabilistic_scenario_table, build_investment_priority,
        build_district_win_prob_table, run_defensive_scenarios,
        optimize_path_to_target,
    )

    targeting_df = pd.read_csv(targeting)
    composite_df = pd.read_csv(composite)

    config = SimConfig(
        n_sims=n_sims,
        include_incumbency=with_incumbency,
        random_seed=seed,
    )

    out_dir = Path(_DEFAULT_SESSION8_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)

    typer.echo("=== Session 8: Probabilistic Scenarios + Resource Allocation ===\n")

    # 1. Sigma estimation
    typer.echo("Step 1: Estimating district-level uncertainty …")
    sigma_prior = compute_sigma_prior(composite_df)
    sigma_df = estimate_district_sigma(targeting_df, sigma_prior, config)
    typer.echo(f"  σ_prior = {sigma_prior:.4f}")
    for source, count in sigma_df["sigma_source"].value_counts().items():
        typer.echo(f"  {source}: {count} districts")

    sigma_path = out_dir / "oh_house_district_sigma.csv"
    sigma_df.to_csv(sigma_path, index=False, float_format="%.6f")
    typer.echo(f"  → {sigma_path}\n")

    # 2. Probabilistic scenario sweep
    typer.echo("Step 2: Running probabilistic scenario sweep (40%–55%) …")
    scenario_df, results = run_probabilistic_scenario_table(
        targeting_df, sigma_df, config=config,
    )
    scenario_path = out_dir / "oh_house_probabilistic_scenarios.csv"
    scenario_df.to_csv(scenario_path, index=False)
    typer.echo(f"  → {scenario_path}")

    # 3. District win prob table
    typer.echo("\nStep 3: Building district win probability table …")
    wp_table = build_district_win_prob_table(targeting_df, sigma_df, config=config)
    wp_path = out_dir / "oh_house_district_win_probs.csv"
    wp_table.to_csv(wp_path, index=False)
    typer.echo(f"  → {wp_path}")

    # 4. Investment priorities
    typer.echo("\nStep 4: Computing investment priorities at 48% statewide D …")
    invest_df = build_investment_priority(targeting_df, sigma_df, 0.48, config)
    invest_path = out_dir / "oh_house_investment_priority.csv"
    invest_df.to_csv(invest_path, index=False, float_format="%.4f")
    typer.echo(f"  → {invest_path}")

    # 5. Path-to-target optimizer
    typer.echo("\nStep 5: Running path-to-40 optimizer at 48% statewide D …")
    path_df = optimize_path_to_target(
        targeting_df, sigma_df, 0.48,
        target_seats=40, max_districts=15, config=config,
    )
    path_path = out_dir / "oh_house_path_optimizer.csv"
    path_df.to_csv(path_path, index=False)
    typer.echo(f"  → {path_path}")

    # 6. Defensive scenarios
    typer.echo("\nStep 6: Running defensive scenarios (43%–48% statewide D) …")
    defense_df = run_defensive_scenarios(targeting_df, sigma_df, config=config)
    defense_path = out_dir / "oh_house_defensive_scenarios.csv"
    defense_df.to_csv(defense_path, index=False)
    typer.echo(f"  → {defense_path}")

    # Summary
    typer.echo("\n=== Session 8 Summary ===\n")

    # Key thresholds
    for pct_label, pct_val in [("45.0%", 45.0), ("48.0%", 48.0), ("50.0%", 50.0)]:
        row = scenario_df[scenario_df["statewide_d_pct"] == pct_val]
        if not row.empty:
            r = row.iloc[0]
            typer.echo(
                f"  At {pct_label}: {r['mean_d_seats']:.1f} mean seats "
                f"[{r['p10_seats']}–{r['p90_seats']} 80% CI], "
                f"P(≥40)={r['prob_reach_40']:.1%}"
            )

    # Top investment targets
    typer.echo(f"\n  Top 5 investment priorities (at 48%):")
    for _, row in invest_df.head(5).iterrows():
        typer.echo(
            f"    #{row['investment_rank']:>2}  District {row['district']:>2}  "
            f"WP={row['win_prob']:.1%}  Marginal={row['marginal_wp']:.3f}"
        )

    # Defensive alerts
    high_risk = defense_df[(defense_df["risk_level"] == "high") &
                           (defense_df["statewide_d_pct"] == 46.0)]
    if not high_risk.empty:
        dists = high_risk["district"].tolist()
        typer.echo(f"\n  ⚠ High-risk D seats at 46%: Districts {', '.join(map(str, dists))}")

    typer.echo(f"\nAll outputs written to {out_dir}/")


@app.command()
def geojson():
    """
    Convert district shapefile to GeoJSON for the Streamlit map view.

    One-time preprocessing step. Reads the district shapefile, reprojects to
    EPSG:4326 (WGS84), simplifies geometry for web performance, and writes
    to data/processed/oh_house_districts.geojson.
    """
    import geopandas as gpd

    shp_path = Path(DEFAULT_DISTRICT_SHP)
    out_path = Path("data/processed/oh_house_districts.geojson")

    typer.echo(f"Reading district shapefile: {shp_path}")
    gdf = gpd.read_file(shp_path)

    typer.echo("Reprojecting to EPSG:4326 (WGS84)...")
    gdf = gdf.to_crs("EPSG:4326")

    typer.echo("Simplifying geometry for web performance...")
    gdf["geometry"] = gdf["geometry"].simplify(0.001, preserve_topology=True)

    # Ensure DISTRICT column is integer for Plotly featureidkey matching
    if "DISTRICT" in gdf.columns:
        gdf["DISTRICT"] = gdf["DISTRICT"].astype(int)

    typer.echo(f"Writing GeoJSON to {out_path} ({len(gdf)} features)...")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_file(out_path, driver="GeoJSON")

    # Report file size
    size_kb = out_path.stat().st_size / 1024
    typer.echo(f"Done. File size: {size_kb:.0f} KB")


@app.command()
def backbone(
    build_geometry: bool = typer.Option(False, "--build-geometry", help="Build block geometry cache + county map"),
    build_maps: bool = typer.Option(False, "--build-maps", help="Build block-to-precinct and block-to-district maps"),
    build_surfaces: bool = typer.Option(False, "--build-surfaces", help="Disaggregate votes to block level"),
    validate: bool = typer.Option(False, "--validate", help="Run round-trip validation"),
    full: bool = typer.Option(False, "--full", help="Run all steps"),
    force: bool = typer.Option(False, "--force", help="Rebuild even if cached"),
):
    """
    Build the Census Block Backbone data store.

    Makes Census 2020 blocks the canonical storage unit. Precinct votes are
    disaggregated to blocks (proportional to population), then reaggregated
    to any district map instantly.

    Examples:
      python cli.py backbone --full             # build everything
      python cli.py backbone --full --force     # rebuild from scratch
      python cli.py backbone --validate         # validation only (requires existing data)
    """
    from src.backbone import (
        build_block_geometry_cache,
        build_block_county_map,
        build_block_precinct_map,
        build_block_district_map,
        disaggregate_precinct_votes,
        disaggregate_sos_via_vest,
        load_block_geometry,
        load_block_precinct_map,
        load_block_district_map,
        load_block_county_map,
        load_block_votes,
        compute_lean_from_blocks,
        build_composite_from_blocks,
    )
    from src.ingest import load_precincts, load_districts, get_vest_races, VEST_RACE_PATTERN
    from src.ingest_historical import (
        parse_2010_county_votes, parse_2012_county_votes,
        parse_2014_county_votes, county_name_to_fips,
    )
    from src.backbone import disaggregate_county_votes

    import pandas as pd

    if full:
        build_geometry = build_maps = build_surfaces = validate = True

    # Phase 1: Block geometry + county map
    if build_geometry:
        typer.echo("\n=== Phase 1: Block Geometry Cache ===")
        blocks = build_block_geometry_cache(force=force)
        build_block_county_map(blocks, force=force)

    # Phase 2: Block-to-precinct maps (VEST years)
    if build_maps:
        typer.echo("\n=== Phase 2: Block-to-Precinct Maps ===")
        blocks = load_block_geometry()

        vest_years = {
            "2016": "data/shapefiles/oh_2016/oh_2016.shp",
            "2018": "data/shapefiles/oh_2018/oh_2018.shp",
            "2020": DEFAULT_PRECINCT_SHP,
        }
        for year, shp_path in vest_years.items():
            typer.echo(f"\n--- VEST {year} ---")
            prec = load_precincts(shp_path)
            build_block_precinct_map(prec, year, blocks=blocks, force=force)

        typer.echo("\n=== Phase 3: Block-to-District Map ===")
        districts = load_districts(DEFAULT_DISTRICT_SHP)
        build_block_district_map(districts, "2024", blocks=blocks, force=force)

    # Phase 5-6: Block vote surfaces
    if build_surfaces:
        typer.echo("\n=== Phase 5: VEST Precinct Vote Surfaces (2016-2020) ===")
        vest_years_shp = {
            "2016": "data/shapefiles/oh_2016/oh_2016.shp",
            "2018": "data/shapefiles/oh_2018/oh_2018.shp",
            "2020": DEFAULT_PRECINCT_SHP,
        }
        for year, shp_path in vest_years_shp.items():
            typer.echo(f"\n--- VEST {year} ---")
            prec = load_precincts(shp_path)
            bpm = load_block_precinct_map(year)
            races = get_vest_races(prec)
            vote_col_pairs = []
            for race_code, cols in sorted(races.items()):
                d_cols = [c for c in cols if VEST_RACE_PATTERN.match(c.upper()).group(3) == "D"]
                r_cols = [c for c in cols if VEST_RACE_PATTERN.match(c.upper()).group(3) == "R"]
                if d_cols and r_cols:
                    vote_col_pairs.append((d_cols[0], r_cols[0]))
            pv = prec[["precinct_id"] + [c for p in vote_col_pairs for c in p]].copy()
            disaggregate_precinct_votes(pv, bpm, vote_col_pairs, year, force=force)

        typer.echo("\n=== Phase 5b: SOS→VEST Precinct Vote Surfaces (2022, 2024) ===")
        vest_2020 = load_precincts(DEFAULT_PRECINCT_SHP)
        bpm_2020 = load_block_precinct_map("2020")

        sos_files = {
            "2022": "data/raw/statewide-races-by-precinct.xlsx",
            "2024": "data/raw/statewide-races-precint-level.xlsx",
        }
        for year, sos_path in sos_files.items():
            typer.echo(f"\n--- SOS {year} via VEST 2020 ---")
            disaggregate_sos_via_vest(sos_path, vest_2020, bpm_2020, year, force=force)

        typer.echo("\n=== Phase 6: County-Level Vote Surfaces (2010-2014) ===")
        blocks = load_block_geometry()
        block_county_map = load_block_county_map()

        historical = {
            "2010": ("data/raw/2010precinct.xlsx", parse_2010_county_votes),
            "2012": ("data/raw/2012statewidebyprecinct.xlsx", parse_2012_county_votes),
            "2014": ("data/raw/2014.xlsx", parse_2014_county_votes),
        }
        for year, (path, parser) in historical.items():
            typer.echo(f"\n--- SOS {year} (county-level) ---")
            cv = parser(path)
            cv = county_name_to_fips(cv)
            county_wide = cv.pivot_table(
                index="county_fips", columns="race",
                values=["d_votes", "r_votes"], aggfunc="sum",
            ).reset_index()
            county_wide.columns = [
                f"{v}_{r}" if v != "county_fips" else "county_fips"
                for v, r in county_wide.columns
            ]
            races = cv["race"].unique()
            race_col_specs = [
                (f"{r}_{year}", f"d_votes_{r}", f"r_votes_{r}")
                for r in sorted(races)
            ]
            disaggregate_county_votes(county_wide, block_county_map, blocks, race_col_specs, year, force=force)

    # Validation
    if validate:
        typer.echo("\n=== Phase 7: Validation ===")
        bdm = load_block_district_map("2024")

        # Validation A: backbone composite vs existing (2016-2024)
        typer.echo("\nValidation A: Block backbone composite lean vs existing …")
        backbone_composite = build_composite_from_blocks(
            years=["2016", "2018", "2020", "2022", "2024"],
            block_district_map=bdm,
        )

        existing = pd.read_csv("reports/session2/oh_house_composite_lean.csv")
        merged = backbone_composite[["district", "composite_lean"]].merge(
            existing[["district", "composite_lean"]],
            on="district", suffixes=("_backbone", "_existing"),
        )
        diff = merged["composite_lean_backbone"] - merged["composite_lean_existing"]
        max_diff = diff.abs().max()
        mean_diff = diff.abs().mean()
        typer.echo(f"  Max difference: {max_diff:.6f}")
        typer.echo(f"  Mean difference: {mean_diff:.6f}")
        typer.echo(f"  Districts > 0.005: {(diff.abs() > 0.005).sum()}")

        if max_diff < 0.01:
            typer.echo("  ✓ PASS — backbone matches existing within 0.01 pts")
        else:
            typer.echo("  ✗ FAIL — backbone differs from existing by > 0.01 pts")

        # Validation B: extended composite (2010-2024)
        typer.echo("\nValidation B: Extended composite (2010-2024) …")
        extended = build_composite_from_blocks(
            years=["2010", "2012", "2014", "2016", "2018", "2020", "2022", "2024"],
            block_district_map=bdm,
        )
        merged_ext = extended[["district", "composite_lean"]].merge(
            existing[["district", "composite_lean"]],
            on="district", suffixes=("_extended", "_current"),
        )
        diff_ext = merged_ext["composite_lean_extended"] - merged_ext["composite_lean_current"]
        corr = merged_ext["composite_lean_extended"].corr(merged_ext["composite_lean_current"])
        typer.echo(f"  Correlation: {corr:.6f}")
        typer.echo(f"  Mean shift from historical data: {diff_ext.abs().mean():.4f}")
        typer.echo(f"  Max shift: {diff_ext.abs().max():.4f}")

    typer.echo("\n=== Backbone build complete ===")


@app.command("trends")
def run_trends(
    force: bool = typer.Option(False, help="Recompute even if cached CSV exists."),
):
    """
    Compute per-district partisan trend from 2010–2024 block vote surfaces.

    Fits a linear trend to each district's average statewide-race lean over
    8 election cycles. Outputs a CSV with trend slope, direction, R², and
    total shift. Also merges trend columns into the targeting CSV.
    """
    import pandas as pd
    from src.backbone import (
        compute_district_trends, load_block_district_map,
        DISTRICT_TRENDS_PATH,
    )

    typer.echo("=== Ohio House Model — District Partisan Trends (2010–2024) ===\n")

    if DISTRICT_TRENDS_PATH.exists() and not force:
        typer.echo(f"Loading cached trends from {DISTRICT_TRENDS_PATH} …")
        trend_df = pd.read_csv(DISTRICT_TRENDS_PATH)
    else:
        bdm = load_block_district_map()
        trend_df = compute_district_trends(bdm)

        # Save standalone CSV
        DISTRICT_TRENDS_PATH.parent.mkdir(parents=True, exist_ok=True)
        trend_df.to_csv(DISTRICT_TRENDS_PATH, index=False, float_format="%.6f")
        typer.echo(f"\nTrend CSV written to {DISTRICT_TRENDS_PATH}")

    # Print summary table: pickup targets with trends
    targeting_path = Path("reports/session3/oh_house_targeting.csv")
    if targeting_path.exists():
        targeting = pd.read_csv(targeting_path)
        # Drop any existing trend columns to avoid suffix collisions
        old_trend_cols = [c for c in targeting.columns if c.startswith("trend_")]
        if old_trend_cols:
            targeting = targeting.drop(columns=old_trend_cols)
        merged = targeting.merge(trend_df[["district", "trend_slope", "trend_shift",
                                           "trend_r2", "trend_dir"]], on="district", how="left")

        typer.echo("\n--- Top Pickup Targets by Trend ---")
        pickups = merged[merged["pickup_opportunity"] == True].copy()
        if not pickups.empty:
            pickups = pickups.sort_values("trend_slope", ascending=False)
            typer.echo(f"  {'Dist':>5s}  {'Lean':>7s}  {'Tier':>10s}  {'Open?':>5s}  "
                       f"{'Slope/yr':>9s}  {'Shift':>7s}  {'R²':>5s}  {'Dir':>12s}")
            typer.echo("  " + "─" * 75)
            for _, r in pickups.head(20).iterrows():
                open_str = "Yes" if r.get("open_seat_2026") else ""
                typer.echo(
                    f"  {r['district']:5.0f}  {r['composite_lean']:+.4f}  "
                    f"{r['tier']:>10s}  {open_str:>5s}  "
                    f"{r['trend_slope']:+.5f}  {r['trend_shift']:+.4f}  "
                    f"{r['trend_r2']:.3f}  {r['trend_dir']:>12s}"
                )

        # Merge trend columns into targeting CSV
        typer.echo("\nMerging trend columns into targeting CSV …")
        # Drop any old trend columns first
        old_trend_cols = [c for c in targeting.columns if c.startswith("trend_")]
        if old_trend_cols:
            targeting = targeting.drop(columns=old_trend_cols)
        targeting = targeting.merge(
            trend_df[["district", "trend_slope", "trend_shift", "trend_r2", "trend_dir"]],
            on="district", how="left",
        )
        targeting.to_csv(targeting_path, index=False, float_format="%.6f")
        typer.echo(f"  Updated {targeting_path}")

    # The key strategic insight: D-trending R-held districts
    typer.echo("\n--- Strategic Opportunities: D-trending R-held targets ---")
    if targeting_path.exists():
        d_trending_pickups = merged[
            (merged["pickup_opportunity"] == True) &
            (merged["trend_dir"] == "trending_d")
        ].sort_values("trend_slope", ascending=False)

        if not d_trending_pickups.empty:
            typer.echo(f"  {len(d_trending_pickups)} R-held pickup targets trending D:")
            for _, r in d_trending_pickups.iterrows():
                open_str = " [OPEN 2026]" if r.get("open_seat_2026") else ""
                typer.echo(
                    f"    District {r['district']:.0f}: lean {r['composite_lean']:+.4f}, "
                    f"trend {r['trend_slope']:+.5f}/yr ({r['trend_shift']:+.3f} total), "
                    f"R²={r['trend_r2']:.2f}{open_str}"
                )
        else:
            typer.echo("  No R-held pickup targets currently trending D.")

        r_trending_pickups = merged[
            (merged["pickup_opportunity"] == True) &
            (merged["trend_dir"] == "trending_r")
        ].sort_values("trend_slope", ascending=True)

        if not r_trending_pickups.empty:
            typer.echo(f"\n  ⚠ {len(r_trending_pickups)} R-held pickup targets trending R "
                       "(swimming against the current):")
            for _, r in r_trending_pickups.head(10).iterrows():
                typer.echo(
                    f"    District {r['district']:.0f}: lean {r['composite_lean']:+.4f}, "
                    f"trend {r['trend_slope']:+.5f}/yr ({r['trend_shift']:+.3f} total)"
                )

    typer.echo("\nDone.")


@app.command("export-gui")
def export_gui():
    """
    Copy all pre-computed outputs into gui_data/ for the Streamlit dashboard.

    Run this after any pipeline change to refresh the dashboard data.
    """
    import shutil

    gui_dir = Path("gui_data")
    gui_dir.mkdir(exist_ok=True)

    # Source → destination mapping
    copies = {
        "reports/session3/oh_house_targeting.csv": "targeting.csv",
        "reports/session2/oh_house_composite_lean.csv": "composite_lean.csv",
        "reports/session2/oh_house_actual_results.csv": "actual_results.csv",
        "reports/session3/oh_house_scenario_table.csv": "deterministic_scenarios.csv",
        "reports/session8/oh_house_probabilistic_scenarios.csv": "probabilistic_scenarios.csv",
        "reports/session8/oh_house_district_win_probs.csv": "district_win_probs.csv",
        "reports/session8/oh_house_investment_priority.csv": "investment_priority.csv",
        "reports/session8/oh_house_path_optimizer.csv": "path_optimizer.csv",
        "reports/session8/oh_house_defensive_scenarios.csv": "defensive_scenarios.csv",
        "reports/session8/oh_house_district_sigma.csv": "district_sigma.csv",
        "reports/redistricting_overlap.csv": "redistricting_overlap.csv",
        "reports/anomaly_flags.csv": "anomaly_flags.csv",
        "data/processed/oh_house_demographics.csv": "demographics.csv",
        "data/processed/oh_house_voter_universe.csv": "voter_universe.csv",
        "data/processed/oh_house_district_trends.csv": "district_trends.csv",
        "data/processed/oh_house_districts.geojson": "districts.geojson",
    }

    copied = 0
    missing = 0
    for src_rel, dst_name in copies.items():
        src = Path(src_rel)
        dst = gui_dir / dst_name
        if src.exists():
            shutil.copy2(src, dst)
            copied += 1
        else:
            typer.echo(f"  WARNING: {src_rel} not found, skipping")
            missing += 1

    typer.echo(f"\nExported {copied} files to gui_data/ ({missing} missing)")
    typer.echo("Dashboard data is ready for deployment.")


@app.command("backtest")
def run_backtest_cmd():
    """
    Session 12: Out-of-sample backtest — use pre-2024 data to predict 2024 outcomes.

    Builds composite lean from 2016-2022 statewide races only (via block backbone),
    classifies districts, estimates uncertainty from pre-2024 house results, and
    compares predicted win probabilities against actual 2024 house race outcomes.
    """
    from src.backtest import run_backtest, write_backtest_report, write_backtest_csvs
    from src.export import generate_backtest_one_pager

    typer.echo("=== Ohio House Model — Session 12: Historical Backtest ===\n")
    results = run_backtest()
    write_backtest_report(results)
    write_backtest_csvs(results)
    generate_backtest_one_pager(results)
    typer.echo("\nDone.")


if __name__ == "__main__":
    app()
