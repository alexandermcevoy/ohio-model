"""
cli.py — Typer CLI for the Ohio House Election Model.
"""

from __future__ import annotations

from pathlib import Path

import typer

app = typer.Typer(help="Ohio Forecast — multi-office partisan lean + forecast tool.")

# ── Per-office subcommand groups ─────────────────────────────────────────────
# `python cli.py house composite` etc. Each group hosts office-aware variants
# of the analytical pipeline. Top-level commands remain registered for
# backward compat and are auto-aliased into `house_app` at the bottom of this
# file (every top-level command also works as `python cli.py house <cmd>`).
house_app = typer.Typer(help="Ohio State House (99 districts) — default office.")
senate_app = typer.Typer(help="Ohio State Senate (33 districts, staggered terms).")
congress_app = typer.Typer(help="Ohio US House (15 districts).")
governor_app = typer.Typer(help="Ohio Governor (statewide).")
multi_app = typer.Typer(help="Cross-office helpers (build all backbone maps, etc.).")

app.add_typer(house_app, name="house")
app.add_typer(senate_app, name="senate")
app.add_typer(congress_app, name="congress")
app.add_typer(governor_app, name="governor")
app.add_typer(multi_app, name="multi")

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
    from src.ingest_race_results import parse_race_results, combine_race_results
    from src.office import HOUSE
    from src.composite import build_composite, compute_year_baselines, merge_composite_with_house_results

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
        hr = parse_race_results(sos, HOUSE)
        if not hr.empty:
            house_results_list.append(hr)

    house_long = pd.concat(house_results_list, ignore_index=True) if house_results_list else pd.DataFrame()
    house_wide = combine_race_results(house_results_list)

    # Save long-format house results
    Path("reports").mkdir(exist_ok=True)
    if not house_long.empty:
        house_long.to_csv("reports/session2/oh_house_actual_results.csv", index=False, float_format="%.6f")
        typer.echo(f"  House results written to reports/session2/oh_house_actual_results.csv")

    # ── Build composite lean ──────────────────────────────────────────────────
    typer.echo("\nBuilding composite lean index …")
    composite_df = build_composite(district_leans)

    # Year baselines for candidate effect: the reference statewide D 2-party
    # share each cycle (president in presidential years, mean of AG/Aud/SOS/
    # Treas in midterms — see src/composite.py REFERENCE_MIDTERM_RACES).
    # 2020 presidential comes from VEST (already validated exact match with SOS).
    year_baselines = compute_year_baselines(sos_files)
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
    from src.ingest_race_results import extract_candidate_names
    from src.office import HOUSE
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
        candidate_names_2024 = extract_candidate_names(sos_2024, HOUSE)
        n_names = candidate_names_2024["dem_candidate_2024"].notna().sum()
        typer.echo(f"  {n_names} districts with D candidate names, "
                   f"{candidate_names_2024['rep_candidate_2024'].notna().sum()} with R")
    else:
        typer.echo(f"  NOTE: 2024 SOS file not found at {sos_2024_path}; candidate names unavailable.")

    # ── Compute district uncertainty (sigma) for WP-based tiers ──────────────
    typer.echo("\nComputing district uncertainty for WP-based tiers …")
    from src.simulate import compute_sigma_prior, compute_shared_sigma, estimate_district_sigma
    from src.classify import compute_swing_metrics as _csm

    sigma_prior = compute_sigma_prior(composite_df)
    sigma_shared = compute_shared_sigma(composite_df)
    typer.echo(f"  Sigma prior (within-year candidate effect std): {sigma_prior:.4f}  |  "
               f"shared year sigma: {sigma_shared:.4f}")

    # Compute swing metrics on a scratch copy so we can estimate sigma before
    # the full classify pass.  compute_swing_metrics needs district + composite_lean.
    _swing_tmp = _csm(composite_df.copy(), house_long)
    sigma_df = estimate_district_sigma(_swing_tmp, sigma_prior, sigma_shared=sigma_shared)
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
        from src.ingest_race_results import apply_redistricting_filter as _apply_rd_filter
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
    from src.ingest_race_results import parse_race_results, combine_race_results
    from src.office import HOUSE
    from src.composite import build_composite, compute_year_baselines, merge_composite_with_house_results
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
        hr = parse_race_results(sos, HOUSE)
        if not hr.empty:
            house_results_list.append(hr)
    house_long = pd.concat(house_results_list, ignore_index=True) if house_results_list else pd.DataFrame()
    house_wide = combine_race_results(house_results_list)

    if not house_long.empty:
        house_long.to_csv("reports/session2/oh_house_actual_results.csv", index=False, float_format="%.6f")

    # Build composite
    typer.echo("\nBuilding composite lean index (pop-weighted) …")
    composite_df = build_composite(district_leans)

    year_baselines = compute_year_baselines(sos_files)
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
    statewide_d: float = typer.Option(None, "--statewide-d", help="Statewide D two-party share on the reference race (president; midterms: AG/Aud/SOS/Treas mean), e.g. 48.5."),
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
    from src.ingest_race_results import (
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
        help="Statewide D% on the reference race (president; midterms: AG/Aud/SOS/Treas mean), "
             "e.g. 48.5. If omitted, runs full 40–55% sweep."),
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
        SimConfig, compute_sigma_prior, compute_shared_sigma, estimate_district_sigma,
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
    sigma_shared = compute_shared_sigma(composite_df)
    sigma_df = estimate_district_sigma(targeting_df, sigma_prior, config, sigma_shared=sigma_shared)

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
        help="Assumed statewide D environment on the reference race "
             "(president; midterms: AG/Aud/SOS/Treas mean), e.g. 48.0."),
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
        SimConfig, compute_sigma_prior, compute_shared_sigma, estimate_district_sigma,
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
    sigma_shared = compute_shared_sigma(composite_df)
    sigma_df = estimate_district_sigma(targeting_df, sigma_prior, config, sigma_shared=sigma_shared)

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
        help="Show all district win probs at this statewide D% (reference race: "
             "president; midterms: AG/Aud/SOS/Treas mean)."),
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
        SimConfig, compute_sigma_prior, compute_shared_sigma, estimate_district_sigma,
        compute_analytical_win_probs, district_win_prob_curve,
    )

    targeting_df = pd.read_csv(targeting)
    composite_df = pd.read_csv(composite)

    config = SimConfig(include_incumbency=with_incumbency)
    sigma_prior = compute_sigma_prior(composite_df)
    sigma_shared = compute_shared_sigma(composite_df)
    sigma_df = estimate_district_sigma(targeting_df, sigma_prior, config, sigma_shared=sigma_shared)

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
        SimConfig, compute_sigma_prior, compute_shared_sigma, estimate_district_sigma,
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
    sigma_shared = compute_shared_sigma(composite_df)
    sigma_df = estimate_district_sigma(targeting_df, sigma_prior, config, sigma_shared=sigma_shared)
    typer.echo(f"  σ_prior (within-year) = {sigma_prior:.4f}  |  shared year σ = {sigma_shared:.4f}")
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
        build_block_district_map(districts, "house_2024", blocks=blocks, force=force)

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
        bdm = load_block_district_map("house_2024")

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


def _export_gui_for_office(office_name: str) -> tuple[int, int]:
    """Copy per-office outputs to gui_data/{office}/. Returns (copied, missing)."""
    import shutil
    from src.office import get_office

    office = get_office(office_name)
    prefix = office.output_prefix
    gui_dir = Path("gui_data") / office.name
    gui_dir.mkdir(parents=True, exist_ok=True)

    copies = {
        f"reports/session3/{prefix}_targeting.csv": "targeting.csv",
        f"reports/session2/{prefix}_composite_lean.csv": "composite_lean.csv",
        f"reports/session2/{prefix}_actual_results.csv": "actual_results.csv",
        f"reports/session3/{prefix}_scenario_table.csv": "deterministic_scenarios.csv",
        f"reports/session8/{prefix}_probabilistic_scenarios.csv": "probabilistic_scenarios.csv",
        f"reports/session8/{prefix}_district_win_probs.csv": "district_win_probs.csv",
        f"reports/session8/{prefix}_investment_priority.csv": "investment_priority.csv",
        f"reports/session8/{prefix}_district_sigma.csv": "district_sigma.csv",
    }
    # House-only artifacts (demographics, voter universe, trends, geojson, etc.)
    if office_name == "house":
        copies.update({
            "reports/session8/oh_house_path_optimizer.csv": "path_optimizer.csv",
            "reports/session8/oh_house_defensive_scenarios.csv": "defensive_scenarios.csv",
            "reports/redistricting_overlap.csv": "redistricting_overlap.csv",
            "reports/anomaly_flags.csv": "anomaly_flags.csv",
            "data/processed/oh_house_demographics.csv": "demographics.csv",
            "data/processed/oh_house_voter_universe.csv": "voter_universe.csv",
            "data/processed/oh_house_district_trends.csv": "district_trends.csv",
            "data/processed/oh_house_districts.geojson": "districts.geojson",
        })

    copied = missing = 0
    for src_rel, dst_name in copies.items():
        src = Path(src_rel)
        dst = gui_dir / dst_name
        if src.exists():
            shutil.copy2(src, dst)
            copied += 1
        else:
            typer.echo(f"  WARNING: {src_rel} not found, skipping")
            missing += 1
    return copied, missing


@app.command("export-gui")
def export_gui(
    office: str = typer.Option(
        "all", "--office",
        help="Office to export ('house', 'senate', 'congress', or 'all'). "
             "When 'all', also mirrors house artifacts at gui_data/ root for back-compat.",
    ),
):
    """
    Copy pre-computed outputs into gui_data/{office}/ for the Streamlit dashboard.

    Run this after any pipeline change to refresh the dashboard data.
    """
    import shutil

    if office == "all":
        offices_to_export = ["house", "senate", "congress"]
    else:
        offices_to_export = [office]

    total_copied = total_missing = 0
    for off in offices_to_export:
        typer.echo(f"\n=== Exporting GUI data for {off} ===")
        c, m = _export_gui_for_office(off)
        typer.echo(f"  {c} copied, {m} missing → gui_data/{off}/")
        total_copied += c
        total_missing += m

    # Back-compat: mirror house artifacts at gui_data/ root so existing pages
    # that don't yet pass an office param continue to work.
    if "house" in offices_to_export:
        gui_root = Path("gui_data")
        house_dir = gui_root / "house"
        if house_dir.exists():
            for src in house_dir.iterdir():
                if src.is_file():
                    shutil.copy2(src, gui_root / src.name)
            typer.echo(f"\n  (Mirrored house artifacts at gui_data/ root for back-compat.)")

    typer.echo(f"\nTotal: {total_copied} copied, {total_missing} missing.")
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


# =============================================================================
# Per-office subcommand groups (Phase A.8)
# =============================================================================
#
# Backward compatibility: every existing top-level command is auto-aliased as
# a `house <cmd>` subcommand below. This means `python cli.py composite` and
# `python cli.py house composite` invoke identical callbacks.
#
# Office-specific subcommands defined here (senate/congress/governor) wrap the
# small number of pieces that are *already* office-aware via OfficeConfig:
# block-district maps via build_block_district_map_for_office(). The full
# composite/classify/simulate pipelines for non-house offices come in A.9.

def _build_office_block_district_map(office_name: str, force: bool):
    """Shared body for `<office> backbone --build-maps`."""
    from src.backbone import (
        build_block_district_map_for_office,
        build_block_geometry_cache,
        load_block_geometry,
    )
    from src.office import get_office

    office = get_office(office_name)
    if office.shapefile_path is None:
        typer.echo(f"  Office '{office.name}' is statewide; no block-district map needed.")
        return

    typer.echo(f"=== Block-district map for {office.display_name} ===")
    # Block geometry cache must exist before centroid assignment.
    try:
        blocks = load_block_geometry()
    except FileNotFoundError:
        typer.echo("  Block geometry cache missing; building it now …")
        blocks = build_block_geometry_cache(force=False)

    build_block_district_map_for_office(office, blocks=blocks, force=force)


@senate_app.command("backbone")
def senate_backbone(
    build_maps: bool = typer.Option(False, "--build-maps", help="Build block-to-senate-district map (33 districts)"),
    force: bool = typer.Option(False, "--force", help="Rebuild even if cached"),
):
    """Senate-specific backbone steps. Today: --build-maps only."""
    if not build_maps:
        typer.echo("Use --build-maps. Other senate backbone steps come in A.9.")
        raise typer.Exit(0)
    _build_office_block_district_map("senate", force=force)


@congress_app.command("backbone")
def congress_backbone(
    build_maps: bool = typer.Option(False, "--build-maps", help="Build block-to-CD map (15 districts, Oct 2025 plan)"),
    force: bool = typer.Option(False, "--force", help="Rebuild even if cached"),
):
    """US House-specific backbone steps. Today: --build-maps only."""
    if not build_maps:
        typer.echo("Use --build-maps. Other congressional backbone steps come in A.9.")
        raise typer.Exit(0)
    _build_office_block_district_map("congress", force=force)


@governor_app.command("backbone")
def governor_backbone(
    build_maps: bool = typer.Option(False, "--build-maps", help="No-op: governor is statewide."),
    force: bool = typer.Option(False, "--force", help="(unused)"),
):
    """Governor backbone has no district map (statewide office)."""
    typer.echo("Governor is statewide — no per-district backbone artifacts.")


# ── Governor (statewide degenerate case) ─────────────────────────────────────
#
# Governor is a single statewide race; most legislative-chamber operations
# collapse. Phase A.10 ships a minimal baseline: historical statewide D% plus
# a normal-distribution placeholder posterior. Phase B will replace
# `governor simulate` with a polls + fundamentals nowcast.

@governor_app.command("composite")
def governor_composite():
    """Statewide D% history from prior gov cycles (2010, 2014, 2018, 2022)."""
    from src.governor import compute_statewide_history
    from src.office import get_office

    office = get_office("governor")
    typer.echo(f"=== {office.display_name} — statewide history ===\n")

    hist = compute_statewide_history()
    if hist.empty:
        typer.echo("ERROR: no governor cycles found in data/processed/block_votes_*.parquet", err=True)
        raise typer.Exit(1)

    out_path = Path(f"reports/session2/{office.output_prefix}_statewide_history.csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    hist.to_csv(out_path, index=False, float_format="%.6f")

    typer.echo(f"  Year  Race      D share")
    for _, r in hist.iterrows():
        typer.echo(f"  {int(r['year'])}  {r['race']:<9} {r['statewide_d_share']*100:>6.2f}%")
    typer.echo(f"\n  → {out_path}")


@governor_app.command("classify")
def governor_classify():
    """Governor classification is degenerate (one race, no tier)."""
    typer.echo("Governor is a single statewide race — no per-district tier classification.")
    typer.echo("Use `governor composite` for historical D%, `governor simulate` for projection.")


@governor_app.command("simulate")
def governor_simulate(
    n_samples: int = typer.Option(10_000, "--n-samples", help="Posterior sample size"),
    seed: int = typer.Option(42, "--seed"),
):
    """Placeholder posterior over 2026 statewide D% (Phase B will replace)."""
    import numpy as np
    import pandas as pd
    from src.governor import (
        compute_statewide_history,
        build_baseline_distribution,
        summarize_distribution,
    )
    from src.office import get_office

    office = get_office("governor")
    typer.echo(f"=== {office.display_name} — placeholder posterior ===\n")
    typer.echo(
        "  NOTE: Phase A baseline only — i.i.d. normal over historical gov cycles. "
        "Phase B will replace this with a polls + fundamentals nowcast.\n"
    )

    hist = compute_statewide_history()
    if hist.empty:
        typer.echo("ERROR: no governor cycles found. Run backbone first.", err=True)
        raise typer.Exit(1)

    summary, samples = build_baseline_distribution(hist, n_samples=n_samples, seed=seed)
    stats = summarize_distribution(samples)

    typer.echo(f"  Cycles used: {summary.cycles}")
    typer.echo(f"  Method:      {summary.method}")
    typer.echo(f"  Hist mean:   {summary.mean_d_share*100:.2f}%")
    typer.echo(f"  Hist sigma:  {summary.std_d_share*100:.2f} pts")
    typer.echo("")
    typer.echo("  Posterior summary (Phase A placeholder):")
    typer.echo(f"    Mean:     {stats['mean']*100:.2f}%")
    typer.echo(f"    Median:   {stats['median']*100:.2f}%")
    typer.echo(f"    80% CI:   [{stats['p10']*100:.2f}%, {stats['p90']*100:.2f}%]")
    typer.echo(f"    50% CI:   [{stats['p25']*100:.2f}%, {stats['p75']*100:.2f}%]")
    typer.echo(f"    P(D win): {stats['prob_d_win']*100:.1f}%")

    # Persist outputs.
    out_dir = Path("reports/session8")
    out_dir.mkdir(parents=True, exist_ok=True)
    samples_df = pd.DataFrame({"sample_idx": np.arange(len(samples)),
                               "statewide_d_share": samples})
    samples_path = out_dir / f"{office.output_prefix}_posterior_samples.parquet"
    samples_df.to_parquet(samples_path, index=False)
    typer.echo(f"\n  → {samples_path}")

    summary_path = out_dir / f"{office.output_prefix}_posterior_summary.csv"
    pd.DataFrame([{
        "method": summary.method,
        "n_cycles": summary.n_cycles,
        "cycles": ",".join(str(c) for c in summary.cycles),
        "hist_mean": summary.mean_d_share,
        "hist_sigma": summary.std_d_share,
        **stats,
    }]).to_csv(summary_path, index=False, float_format="%.6f")
    typer.echo(f"  → {summary_path}")


@governor_app.command("report")
def governor_report(
    output: Path = typer.Option(None, "--output", "-o", help="Output PDF path"),
):
    """Generate a one-page summary PDF for the governor race."""
    import pandas as pd
    from src.governor import (
        compute_statewide_history,
        build_baseline_distribution,
        summarize_distribution,
    )
    from src.office import get_office

    office = get_office("governor")
    if output is None:
        output = Path("reports/session14") / f"{office.output_prefix}_office_summary.pdf"
    output.parent.mkdir(parents=True, exist_ok=True)

    hist = compute_statewide_history()
    if hist.empty:
        typer.echo("ERROR: no governor cycles found. Run backbone first.", err=True)
        raise typer.Exit(1)
    summary, samples = build_baseline_distribution(hist)
    stats = summarize_distribution(samples)

    # Build a minimal PDF with reportlab.
    from datetime import date
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import LETTER
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.units import inch
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable
    from reportlab.lib.enums import TA_CENTER

    PAGE_W, _ = LETTER
    MARGIN = 0.55 * inch
    OHIO_BLUE  = colors.HexColor("#003478")
    LIGHT_BLUE = colors.HexColor("#E3EDF7")
    LIGHT_GRAY = colors.HexColor("#F5F5F5")
    MID_GRAY   = colors.HexColor("#555555")

    def ps(name, **kw): return ParagraphStyle(name, **kw)
    S = {
        "title":    ps("t",  fontName="Helvetica-Bold", fontSize=17, leading=20, textColor=colors.white),
        "subtitle": ps("st", fontName="Helvetica",      fontSize=9.5, leading=12, textColor=colors.HexColor("#B0C8E8")),
        "section":  ps("sc", fontName="Helvetica-Bold", fontSize=10, leading=12, textColor=OHIO_BLUE, spaceBefore=8, spaceAfter=3),
        "body":     ps("b",  fontName="Helvetica",      fontSize=9, leading=12),
        "caption":  ps("c",  fontName="Helvetica-Oblique", fontSize=8, leading=10, textColor=MID_GRAY),
        "footer":   ps("f",  fontName="Helvetica",      fontSize=7.5, leading=9, textColor=MID_GRAY, alignment=TA_CENTER),
    }

    flow = []
    banner = Table(
        [[Paragraph(f"{office.display_name} — 2026 Forecast Baseline", S["title"])],
         [Paragraph(f"Phase A placeholder   •   Generated {date.today().isoformat()}", S["subtitle"])]],
        colWidths=[PAGE_W - 2 * MARGIN],
    )
    banner.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), OHIO_BLUE),
        ("LEFTPADDING", (0, 0), (-1, -1), 10),
        ("RIGHTPADDING", (0, 0), (-1, -1), 10),
        ("TOPPADDING", (0, 0), (-1, -1), 7),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
    ]))
    flow.append(banner)
    flow.append(Spacer(1, 8))

    flow.append(Paragraph(
        "<b>Status:</b> Phase A baseline only. Posterior is a normal distribution "
        "fit to historical statewide D% from prior governor cycles. Phase B will "
        "replace this with a polls + fundamentals nowcast (polls already ingested "
        "to <code>data/polls/governor.csv</code>).",
        S["body"],
    ))

    flow.append(Paragraph("HISTORICAL STATEWIDE D% (TWO-PARTY)", S["section"]))
    hist_rows = [["Cycle", "Race", "Statewide D%"]]
    for _, r in hist.iterrows():
        hist_rows.append([str(int(r["year"])), r["race"], f"{r['statewide_d_share']*100:.2f}%"])
    cw = [(PAGE_W - 2 * MARGIN) / 3] * 3
    htbl = Table(hist_rows, colWidths=cw)
    htbl.setStyle(TableStyle([
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("BACKGROUND", (0, 0), (-1, 0), LIGHT_BLUE),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, LIGHT_GRAY]),
        ("GRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#BBBBBB")),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
    ]))
    flow.append(htbl)

    flow.append(Paragraph("BASELINE POSTERIOR (PHASE A PLACEHOLDER)", S["section"]))
    post_rows = [
        ["Stat", "Value"],
        ["Method", summary.method],
        ["Cycles used", ", ".join(str(c) for c in summary.cycles)],
        ["Historical mean", f"{summary.mean_d_share*100:.2f}%"],
        ["Historical sigma", f"{summary.std_d_share*100:.2f} pts"],
        ["Posterior mean", f"{stats['mean']*100:.2f}%"],
        ["Posterior median", f"{stats['median']*100:.2f}%"],
        ["80% CI", f"[{stats['p10']*100:.2f}%, {stats['p90']*100:.2f}%]"],
        ["50% CI", f"[{stats['p25']*100:.2f}%, {stats['p75']*100:.2f}%]"],
        ["P(D wins statewide)", f"{stats['prob_d_win']*100:.1f}%"],
    ]
    cw = [2.0 * inch, PAGE_W - 2 * MARGIN - 2.0 * inch]
    ptbl = Table(post_rows, colWidths=cw)
    ptbl.setStyle(TableStyle([
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("BACKGROUND", (0, 0), (-1, 0), LIGHT_BLUE),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, LIGHT_GRAY]),
        ("GRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#BBBBBB")),
        ("ALIGN", (1, 0), (1, -1), "RIGHT"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]))
    flow.append(ptbl)

    flow.append(Spacer(1, 6))
    flow.append(Paragraph(
        "<b>Caveats:</b> n=4 cycles is small; the historical mean is dragged by 2014's "
        "Strickland landslide loss (D 34.2%) and 2022 (Whaley vs DeWine, D 37.5%). The "
        "wide sigma reflects genuine cycle-to-cycle variance, not just sampling error. "
        "Treat the placeholder posterior as a credibility floor; once the Phase B nowcast "
        "incorporates 2026-specific polls + presidential approval, the band tightens substantially.",
        S["caption"],
    ))

    flow.append(Spacer(1, 6))
    flow.append(HRFlowable(width="100%", thickness=0.3, color=MID_GRAY))
    flow.append(Paragraph(
        f"Ohio Election Model • {office.display_name} • Phase A.10 governor degenerate case • "
        f"Methodology: reports/methodology.md",
        S["footer"],
    ))

    doc = SimpleDocTemplate(str(output), pagesize=LETTER,
                            leftMargin=MARGIN, rightMargin=MARGIN,
                            topMargin=MARGIN, bottomMargin=MARGIN)
    doc.build(flow)
    typer.echo(f"  → {output}")


# ── Office-aware composite via the block backbone ────────────────────────────
#
# House continues to use the legacy precinct-crosswalk path (top-level
# `composite` command, also aliased to `house composite`) for byte-exact
# back-compat with existing reports/session2/oh_house_composite_lean.csv.
#
# Senate, congress: backbone path. Same statewide races, reaggregated to the
# office's district geometry via the block-district map. Block backbone was
# validated in Session 10 to match the legacy crosswalk within 0.003 lean pts.

def _run_office_composite_backbone(office_name: str):
    """Build composite_lean + race-history CSV for a non-house office via backbone."""
    import json
    from src.backbone import load_block_district_map, build_composite_from_blocks
    from src.composite import compute_year_baselines, merge_composite_with_house_results
    from src.ingest_sos import load_sos_file, get_race_df
    from src.ingest_race_results import parse_race_results, combine_race_results
    from src.office import get_office

    office = get_office(office_name)
    if office.is_statewide:
        typer.echo(f"  Office '{office.name}' is statewide; use the dedicated forecast workflow (Phase B).")
        raise typer.Exit(1)
    if office.block_district_map_path is None:
        typer.echo(f"  No block-district map configured for {office.display_name}.", err=True)
        raise typer.Exit(1)

    typer.echo(f"=== {office.display_name} composite lean (via block backbone) ===\n")

    # Load block-district map for this office.
    bdm = load_block_district_map(office.map_plan_label)
    typer.echo(f"  Loaded block-district map: {len(bdm):,} blocks → {bdm['district_num'].nunique()} districts")

    # Build composite from all 8 cycles of statewide races (2010–2024).
    composite_df = build_composite_from_blocks(
        years=["2010", "2012", "2014", "2016", "2018", "2020", "2022", "2024"],
        block_district_map=bdm,
        all_districts=list(office.district_range),
    )

    # Parse this office's race history from each SOS XLSX year.
    typer.echo(f"\n  Aggregating {office.display_name} race results …")
    race_results_list = []
    sos_loaded: dict[str, object] = {}
    for year, path in SOS_FILES.items():
        if not Path(path).exists():
            continue
        sos = load_sos_file(path)
        sos_loaded[year] = sos
        rr = parse_race_results(sos, office)
        if not rr.empty:
            race_results_list.append(rr)

    race_long = (
        __import__("pandas").concat(race_results_list, ignore_index=True)
        if race_results_list else __import__("pandas").DataFrame()
    )
    race_wide = combine_race_results(race_results_list) if race_results_list else __import__("pandas").DataFrame()

    # Year baselines for candidate effect (same reference races as house —
    # see src/composite.py REFERENCE_MIDTERM_RACES).
    year_baselines = compute_year_baselines(sos_loaded)

    available_years = [y for y in SOS_FILES if Path(SOS_FILES[y]).exists()]
    final_df = merge_composite_with_house_results(
        composite_df, race_wide, available_years, year_baselines
    )

    # Write outputs under the office's prefix.
    out_dir = Path("reports/session2")
    out_dir.mkdir(parents=True, exist_ok=True)

    composite_path = out_dir / f"{office.output_prefix}_composite_lean.csv"
    final_df.to_csv(composite_path, index=False, float_format="%.6f")
    typer.echo(f"\n  Composite lean → {composite_path}")

    if not race_long.empty:
        results_path = out_dir / f"{office.output_prefix}_actual_results.csv"
        race_long.to_csv(results_path, index=False, float_format="%.6f")
        typer.echo(f"  Race history → {results_path}")

    # Summary
    cl = final_df["composite_lean"]
    typer.echo(f"\n  {office.display_name} composite lean summary:")
    typer.echo(f"    Districts: {len(final_df)}")
    typer.echo(f"    D-leaning (>0): {(cl > 0).sum()}   R-leaning (≤0): {(cl <= 0).sum()}")
    typer.echo(
        f"    Most D: District {int(final_df.loc[cl.idxmax(), 'district'])} ({cl.max():+.4f})  "
        f"Most R: District {int(final_df.loc[cl.idxmin(), 'district'])} ({cl.min():+.4f})"
    )

    typer.echo("\nDone.")


@senate_app.command("composite")
def senate_composite():
    """Build State Senate composite lean (33 districts) via block backbone."""
    _run_office_composite_backbone("senate")


@congress_app.command("composite")
def congress_composite():
    """Build US House composite lean (15 districts, Oct 2025 plan) via block backbone."""
    _run_office_composite_backbone("congress")


# ── Office-aware classification ──────────────────────────────────────────────
#
# Classify pipeline for non-house offices. Reads composite_lean.csv +
# actual_results.csv produced by `<office> composite`, applies sigma_i
# uncertainty + WP-tier classification, and writes:
#   reports/session3/{office.output_prefix}_targeting.csv
#   reports/session3/{office.output_prefix}_scenario_table.csv
#   reports/session3/{office.output_prefix}_pickup_ladder.txt
#
# Skips house-specific post-hoc validation steps (redistricting filter,
# anomaly detection, drop-one sensitivity) — those depend on artifacts
# that don't exist yet for senate/congress and are layered on later.

def _run_office_classify(office_name: str):
    import pandas as pd
    from src.ingest_sos import load_sos_file
    from src.ingest_race_results import extract_candidate_names
    from src.classify import (
        build_targeting_df,
        TIER_ORDER,
        REFERENCE_ENVIRONMENTS,
        compute_swing_metrics,
    )
    from src.simulate import compute_sigma_prior, compute_shared_sigma, estimate_district_sigma
    from src.scenarios import (
        run_scenario_table,
        build_pickup_ladder,
        build_defensive_list,
        format_pickup_ladder,
        print_scenario_summary,
    )
    from src.office import get_office

    office = get_office(office_name)
    if office.is_statewide:
        typer.echo(f"  Office '{office.name}' is statewide; classify not applicable.")
        raise typer.Exit(1)

    typer.echo(f"=== {office.display_name} — classification & targeting ===\n")

    composite_path = Path(f"reports/session2/{office.output_prefix}_composite_lean.csv")
    results_path = Path(f"reports/session2/{office.output_prefix}_actual_results.csv")

    if not composite_path.exists():
        typer.echo(f"ERROR: {composite_path} not found. Run `{office.name} composite` first.", err=True)
        raise typer.Exit(1)

    composite_df = pd.read_csv(composite_path)
    race_long = pd.read_csv(results_path) if results_path.exists() else pd.DataFrame()
    typer.echo(f"  Loaded {len(composite_df)} districts from {composite_path.name}")
    typer.echo(f"  Loaded {len(race_long)} race result rows from {results_path.name}")

    # 2024 candidate names from SOS for this office.
    candidate_names_2024 = None
    sos_2024_path = Path(SOS_FILES["2024"])
    if sos_2024_path.exists():
        sos_2024 = load_sos_file(sos_2024_path)
        candidate_names_2024 = extract_candidate_names(sos_2024, office)

    # Sigma_i: pooled-prior + EB shrinkage by n_contested.
    typer.echo("\n  Computing district uncertainty (sigma_i) …")
    sigma_prior = compute_sigma_prior(composite_df)
    sigma_shared = compute_shared_sigma(composite_df)
    typer.echo(f"    Sigma prior (within-year candidate effect std): {sigma_prior:.4f}  |  "
               f"shared year sigma: {sigma_shared:.4f}")
    _swing_tmp = compute_swing_metrics(composite_df.copy(), race_long)
    sigma_df = estimate_district_sigma(_swing_tmp, sigma_prior, sigma_shared=sigma_shared)
    typer.echo(
        f"    Sigma range: {sigma_df['sigma_i'].min():.4f} – {sigma_df['sigma_i'].max():.4f}"
    )

    # Targeting df. Pass office so open_seat lookup uses the right CSV.
    typer.echo("\n  Classifying districts …")
    targeting_df = build_targeting_df(
        composite_df, race_long, candidate_names_2024, sigma_df=sigma_df, office=office
    )

    # Tier summary
    typer.echo(f"\n  Tier distribution at 48% statewide D ({office.display_name}):")
    tier_counts = targeting_df["tier"].value_counts()
    for tier in TIER_ORDER:
        n = tier_counts.get(tier, 0)
        held = targeting_df[targeting_df["tier"] == tier]["current_holder"].value_counts().to_dict()
        held_str = "  ".join(f"{p}:{c}" for p, c in sorted(held.items()))
        typer.echo(f"    {tier:12s} {n:3d} districts   [{held_str}]")

    for env in REFERENCE_ENVIRONMENTS:
        env_label = f"{int(env * 100)}"
        tier_col = f"tier_{env_label}"
        if tier_col in targeting_df.columns:
            counts = targeting_df[tier_col].value_counts()
            typer.echo(
                f"    At {env_label}%: "
                f"{counts.get('tossup', 0)} tossup, "
                f"{counts.get('lean_d', 0)} lean_d, "
                f"{counts.get('lean_r', 0)} lean_r"
            )

    # Scenario + pickup ladder. Pass office so headers/seat counts use OfficeConfig.
    typer.echo("\n  Building scenario table …")
    scenario_df = run_scenario_table(targeting_df, office=office)
    print_scenario_summary(scenario_df)

    ladder_df = build_pickup_ladder(targeting_df)
    defensive_df = build_defensive_list(targeting_df)
    report_text = format_pickup_ladder(ladder_df, scenario_df, defensive_df, office=office)

    # Senate-only: highlight which seats are actually on the 2026 ballot.
    if office.name == "senate":
        seats_up = set(office.seats_up_2026)
        n_up = targeting_df["district"].isin(seats_up).sum()
        typer.echo(
            f"\n  Note: {n_up} of {len(targeting_df)} senate seats are on the 2026 ballot "
            f"(odd-numbered districts). Composite lean is computed for all 33 for chamber context."
        )

    # Write outputs
    out_dir = Path("reports/session3")
    out_dir.mkdir(parents=True, exist_ok=True)

    targeting_path = out_dir / f"{office.output_prefix}_targeting.csv"
    targeting_df.to_csv(targeting_path, index=False, float_format="%.6f")
    typer.echo(f"\n  Targeting → {targeting_path}")

    scenario_path = out_dir / f"{office.output_prefix}_scenario_table.csv"
    scenario_df.drop(columns=["cumulative_d_districts"]).to_csv(scenario_path, index=False)
    typer.echo(f"  Scenario  → {scenario_path}")

    ladder_path = out_dir / f"{office.output_prefix}_pickup_ladder.txt"
    ladder_path.write_text(report_text, encoding="utf-8")
    typer.echo(f"  Ladder    → {ladder_path}")

    typer.echo("\nDone.")


@senate_app.command("classify")
def senate_classify():
    """Classify Senate districts and build pickup ladder + scenarios."""
    _run_office_classify("senate")


@congress_app.command("classify")
def congress_classify():
    """Classify CDs and build pickup ladder + scenarios."""
    _run_office_classify("congress")


# ── Office-aware simulate (probabilistic scenarios + investment priority) ────
#
# Per-office MC sweep, district win-prob curves, and investment priority
# rankings. Mirrors the top-level `simulate` command but reads/writes paths
# under `{office.output_prefix}_*` and substitutes office-aware thresholds
# for P(hold)/P(majority) (e.g. ≥9 D for senate, ≥17 for senate majority,
# ≥5 D for current congressional delegation).
#
# Senate caveat: simulation runs chamber-wide on all 33 districts to surface
# "if every seat were on the ballot" composition. Only odd-numbered seats are
# actually up in 2026; the seats_up_2026 filter is applied to the per-district
# investment table but not to the seat-count distribution. Treat the senate
# scenario_table as chamber context, not a 2026 forecast.

def _office_thresholds(office) -> list[tuple[str, int, str]]:
    """Return (column_name, seat_threshold, label) tuples for office prob cols."""
    cd = office.current_d_seats or 0
    cols: list[tuple[str, int, str]] = [
        (f"prob_hold_{cd}", cd, f"P(≥{cd}: hold current)"),
    ]
    if office.majority_threshold is not None:
        cols.append((f"prob_majority_{office.majority_threshold}",
                     office.majority_threshold,
                     f"P(≥{office.majority_threshold}: majority)"))
    # Office-appropriate stretch goals
    if office.name == "senate":
        cols.insert(1, ("prob_gain_1", cd + 1, f"P(≥{cd + 1}: +1)"))
        cols.insert(2, ("prob_gain_2", cd + 2, f"P(≥{cd + 2}: +2)"))
    elif office.name == "congress":
        cols.append(("prob_gain_1", cd + 1, f"P(≥{cd + 1}: +1)"))
        cols.append(("prob_gain_2", cd + 2, f"P(≥{cd + 2}: +2)"))
    return cols


def _run_office_simulate(office_name: str, n_sims: int = 10_000,
                          with_incumbency: bool = False, seed: int = 42):
    import pandas as pd
    from src.simulate import (
        SimConfig, compute_sigma_prior, compute_shared_sigma, estimate_district_sigma,
        run_probabilistic_scenario_table, build_district_win_prob_table,
        build_investment_priority,
    )
    from src.office import get_office

    office = get_office(office_name)
    if office.is_statewide:
        typer.echo(f"  Office '{office.name}' is statewide; simulate not applicable.")
        raise typer.Exit(1)

    typer.echo(f"=== {office.display_name} — probabilistic simulation ===\n")

    targeting_path = Path(f"reports/session3/{office.output_prefix}_targeting.csv")
    composite_path = Path(f"reports/session2/{office.output_prefix}_composite_lean.csv")

    if not targeting_path.exists():
        typer.echo(f"ERROR: {targeting_path} not found. Run `{office.name} classify` first.", err=True)
        raise typer.Exit(1)
    if not composite_path.exists():
        typer.echo(f"ERROR: {composite_path} not found. Run `{office.name} composite` first.", err=True)
        raise typer.Exit(1)

    targeting_df = pd.read_csv(targeting_path)
    composite_df = pd.read_csv(composite_path)

    config = SimConfig(
        n_sims=n_sims,
        include_incumbency=with_incumbency,
        random_seed=seed,
    )

    sigma_prior = compute_sigma_prior(composite_df)
    sigma_shared = compute_shared_sigma(composite_df)
    sigma_df = estimate_district_sigma(targeting_df, sigma_prior, config, sigma_shared=sigma_shared)

    typer.echo(f"  σ_prior (within-year candidate effect std): {sigma_prior:.4f}  |  "
               f"shared year σ: {sigma_shared:.4f}")
    typer.echo(f"  Incumbency adjustment: {'ON (confirmed only)' if with_incumbency else 'OFF'}")
    typer.echo(f"  Simulations per environment: {n_sims:,}")
    typer.echo(f"  σ range: {sigma_df['sigma_i'].min():.4f} – {sigma_df['sigma_i'].max():.4f}\n")

    out_dir = Path(_DEFAULT_SESSION8_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Probabilistic scenario sweep across 40–55% statewide D.
    typer.echo("  Running probabilistic scenario sweep (40%–55%) …")
    scenario_df, results = run_probabilistic_scenario_table(
        targeting_df, sigma_df, config=config,
    )

    # Replace house-specific threshold columns with office-aware ones.
    threshold_cols = _office_thresholds(office)
    scenario_df = scenario_df.drop(
        columns=["prob_hold_34", "prob_reach_40", "prob_majority"],
        errors="ignore",
    )
    for col_name, seat_thr, _label in threshold_cols:
        scenario_df[col_name] = [
            round(float((results[round(sw / 100, 4)].seat_counts >= seat_thr).mean()), 4)
            for sw in scenario_df["statewide_d_pct"]
        ]

    scenario_path = out_dir / f"{office.output_prefix}_probabilistic_scenarios.csv"
    scenario_df.to_csv(scenario_path, index=False)
    typer.echo(f"    → {scenario_path}")

    # District win prob table.
    typer.echo("  Building district win probability table …")
    wp_table = build_district_win_prob_table(targeting_df, sigma_df, config=config)
    wp_path = out_dir / f"{office.output_prefix}_district_win_probs.csv"
    wp_table.to_csv(wp_path, index=False)
    typer.echo(f"    → {wp_path}")

    # District-level sigma summary.
    sigma_path = out_dir / f"{office.output_prefix}_district_sigma.csv"
    sigma_df.to_csv(sigma_path, index=False, float_format="%.6f")
    typer.echo(f"    → {sigma_path}")

    # Investment priority at 48% statewide D.
    typer.echo("  Computing investment priorities at 48% statewide D …")
    invest_df = build_investment_priority(targeting_df, sigma_df, 0.48, config)
    if office.name == "senate":
        seats_up = set(office.seats_up_2026)
        invest_df["seat_up_2026"] = invest_df["district"].isin(seats_up)
    invest_path = out_dir / f"{office.output_prefix}_investment_priority.csv"
    invest_df.to_csv(invest_path, index=False, float_format="%.4f")
    typer.echo(f"    → {invest_path}")

    # Summary table to stdout.
    typer.echo(f"\n=== {office.display_name} Probabilistic Scenario Summary ===\n")
    header = f"{'Statewide D':>12}  {'Mean':>5}  {'Med':>4}  {'80% CI':>10}"
    for _, _, label in threshold_cols:
        header += f"  {label:>22}"
    typer.echo(header)
    typer.echo("-" * len(header))
    for _, row in scenario_df.iterrows():
        line = (
            f"{row['statewide_d_pct']:>11.1f}%  "
            f"{row['mean_d_seats']:>5.1f}  "
            f"{row['median_d_seats']:>4}  "
            f"[{row['p10_seats']:>2}, {row['p90_seats']:>2}]"
        )
        for col_name, _, _ in threshold_cols:
            line += f"  {row[col_name]:>21.1%}"
        typer.echo(line)

    if office.name == "senate":
        typer.echo(
            f"\n  Note: simulation runs chamber-wide across all 33 senate seats. "
            f"Only odd-numbered districts ({len(office.seats_up_2026)} seats) are on the "
            f"2026 ballot due to staggered terms; treat seat-count distribution as chamber "
            f"context, not a 2026 forecast. The investment_priority CSV flags seat_up_2026."
        )
    if office.name == "congress" and office.majority_threshold is None:
        typer.echo(
            "\n  Note: congressional majority is decided nationally; thresholds shown are "
            "Ohio-delegation seat counts (current 5D-10R)."
        )

    typer.echo("\nDone.")


@senate_app.command("simulate")
def senate_simulate(
    n_sims: int = typer.Option(10_000, "--n-sims"),
    with_incumbency: bool = typer.Option(False, "--with-incumbency"),
    seed: int = typer.Option(42, "--seed"),
):
    """Probabilistic scenarios + investment priority for State Senate."""
    _run_office_simulate("senate", n_sims=n_sims,
                          with_incumbency=with_incumbency, seed=seed)


@congress_app.command("simulate")
def congress_simulate(
    n_sims: int = typer.Option(10_000, "--n-sims"),
    with_incumbency: bool = typer.Option(False, "--with-incumbency"),
    seed: int = typer.Option(42, "--seed"),
):
    """Probabilistic scenarios + investment priority for US House."""
    _run_office_simulate("congress", n_sims=n_sims,
                          with_incumbency=with_incumbency, seed=seed)


# ---------------------------------------------------------------------------
# Phase B — polls + fundamentals forecast (forecast_statewide.py)
# ---------------------------------------------------------------------------

def _print_statewide_headlines(headlines, live: bool = False) -> None:
    """Polling Average + Win Likelihood for each statewide race (src/headlines.py)."""
    import math

    typer.echo("  STATEWIDE RACES" + (" (live)" if live else ""))
    head = f"    {'Race':<22} {'Polling average':<42} {'80% range (margin)':<30}"
    if live:
        head += f" {'Live D%':>8} {'In':>6}"
    head += f"  {'Win likelihood':<30}"
    if live:
        head += " Call"
    typer.echo(head)
    for h in headlines:
        poll = h.polling.describe() if h.polling else "no public polls"
        band = h.polling.band_text().removeprefix("80% range ") if h.polling else ""
        line = f"    {h.label:<22} {poll:<42} {band:<30}"
        if live:
            live_d = "—" if h.live_d_share is None else f"{h.live_d_share:.1%}"
            frac = "final" if h.is_final else ("" if h.live_fraction is None else f"{h.live_fraction:.0%}")
            line += f" {live_d:>8} {frac:>6}"
        line += f"  {h.win_text():<30}"
        if live:
            line += f" {h.call}"
        typer.echo(line)
    typer.echo("    Polling average = weighted raw toplines. 80% range = where the final margin lands 4 times in 5")
    typer.echo("    if polls miss like they typically do (poll disagreement + a shared ±3-pt statewide miss).")
    typer.echo("    Win likelihood = chance each candidate wins.")
    typer.echo()


def _run_office_forecast(
    office_name: str,
    n_sims: int = 10_000,
    with_incumbency: bool = False,
    include_historical: bool = True,
    seed: int = 42,
    details: bool = False,
):
    import numpy as np
    import pandas as pd
    from src.forecast_statewide import build_forecast, print_forecast_summary
    from src.simulate import (
        SimConfig, compute_sigma_prior, compute_shared_sigma, estimate_district_sigma, run_simulations,
        compute_analytical_win_probs,
    )
    from src.office import get_office
    from src.ballot_overrides import overrides_for_office

    office = get_office(office_name)
    if office.is_statewide:
        typer.echo(f"  Office '{office.name}' is statewide; forecast not applicable here — "
                    "see src/forecast_statewide.py's module docstring for why a governor-"
                    "race-specific forecast is out of scope for this command.")
        raise typer.Exit(1)

    typer.echo(f"=== {office.display_name} — polls + fundamentals forecast ===\n")

    targeting_path = Path(f"reports/session3/{office.output_prefix}_targeting.csv")
    composite_path = Path(f"reports/session2/{office.output_prefix}_composite_lean.csv")
    if not targeting_path.exists():
        typer.echo(f"ERROR: {targeting_path} not found. Run `{office.name} classify` first.", err=True)
        raise typer.Exit(1)
    if not composite_path.exists():
        typer.echo(f"ERROR: {composite_path} not found. Run `{office.name} composite` first.", err=True)
        raise typer.Exit(1)

    targeting_df = pd.read_csv(targeting_path)
    composite_df = pd.read_csv(composite_path)

    config = SimConfig(n_sims=n_sims, include_incumbency=with_incumbency, random_seed=seed)
    sigma_prior = compute_sigma_prior(composite_df)
    sigma_shared = compute_shared_sigma(composite_df)
    sigma_df = estimate_district_sigma(targeting_df, sigma_prior, config, sigma_shared=sigma_shared)

    # The blended Ohio environment — one posterior shared across all three
    # chamber offices (see src/forecast_statewide.py module docstring).
    as_of = pd.Timestamp.now().normalize()
    summary, env_samples = build_forecast(
        as_of=as_of, n_samples=n_sims, seed=seed,
        include_historical_governor_baseline=include_historical,
    )
    # Headlines are the statewide races' Polling Average and Win Likelihood;
    # the blended environment itself is a model input, shown only with
    # --details (see src/headlines.py docstring).
    if details:
        print_forecast_summary(summary)
        typer.echo()
    else:
        from src.headlines import statewide_headlines
        _print_statewide_headlines(statewide_headlines(as_of=as_of))

    # Ballot overrides (src/ballot_overrides.py): manually curated 2026 filing
    # facts the fundamentals model has no way to know (e.g. no Democrat filed
    # in a given district). These districts are pulled OUT of the normal
    # composite-lean simulation entirely and replaced with their own
    # Bernoulli(effective_dem_prob) draw, summed back in — so the aggregate
    # seat distribution reflects real-world ballot facts, while the raw
    # fundamentals-only win_prob computed below is left completely untouched
    # for comparison. See src/ballot_overrides.py module docstring.
    overrides = overrides_for_office(office.name)
    override_districts = [d for d in overrides if d in set(targeting_df["district"])]
    if override_districts:
        sim_targeting = targeting_df[~targeting_df["district"].isin(override_districts)].copy()
        sim_sigma = sigma_df[~sigma_df["district"].isin(override_districts)].copy()
    else:
        sim_targeting, sim_sigma = targeting_df, sigma_df

    result = run_simulations(sim_targeting, sim_sigma, env_samples, config)

    if override_districts:
        rng_override = np.random.default_rng(seed + 999)
        forced_seats = np.zeros(len(result.seat_counts), dtype=int)
        for d in override_districts:
            ov = overrides[d]
            draws = rng_override.random(len(result.seat_counts)) < ov.effective_dem_prob
            forced_seats += draws.astype(int)
        result.seat_counts = result.seat_counts + forced_seats
        result.mean_seats = float(result.seat_counts.mean())
        result.median_seats = int(np.median(result.seat_counts))
        result.p10_seats = int(np.percentile(result.seat_counts, 10))
        result.p25_seats = int(np.percentile(result.seat_counts, 25))
        result.p75_seats = int(np.percentile(result.seat_counts, 75))
        result.p90_seats = int(np.percentile(result.seat_counts, 90))
        result.std_seats = float(result.seat_counts.std())

        typer.echo(f"  ⚠ {len(override_districts)} district(s) overridden by known ballot facts:")
        for d in override_districts:
            ov = overrides[d]
            typer.echo(f"    District {d}: {ov.status} — effective D win prob forced to {ov.effective_dem_prob:.0%}")
        typer.echo()

    typer.echo(f"=== {office.display_name} — unconditional seat forecast ===\n")
    win_thr = office.majority_threshold or office.current_d_seats
    win_label = "D majority" if office.majority_threshold else f"D hold {win_thr}+"
    typer.echo(f"  Win likelihood ({win_label}): {(result.seat_counts >= win_thr).mean():.1%}")
    typer.echo(f"  Mean D seats:   {result.mean_seats:.1f}")
    typer.echo(f"  Median:         {result.median_seats}")
    typer.echo(f"  80% CI:         [{result.p10_seats}, {result.p90_seats}]")
    typer.echo(f"  50% CI:         [{result.p25_seats}, {result.p75_seats}]")
    typer.echo(f"  Std dev:        {result.std_seats:.2f}")
    for col_name, seat_thr, label in _office_thresholds(office):
        prob = float((result.seat_counts >= seat_thr).mean())
        typer.echo(f"  {label:<26} {prob:.1%}")

    # Per-simulation seat counts + matching environment draw — the raw data
    # a distribution-of-outcomes visualization needs (one row per sim).
    out_dir = Path(_DEFAULT_SESSION8_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)
    dist_path = out_dir / f"{office.output_prefix}_forecast_seat_distribution.csv"
    pd.DataFrame({
        "sim_index": range(len(result.seat_counts)),
        "d_seats": result.seat_counts,
        "statewide_d_pct": (env_samples * 100).round(2),
    }).to_csv(dist_path, index=False)
    typer.echo(f"\n  → {dist_path}")

    # Per-district detail at the SAME blended environment (mean + sigma) and
    # SAME config (incumbency setting) as the seat distribution above —
    # margin = env + composite_lean + inc_shift, sigma = sqrt(env_sigma^2 +
    # sigma_i^2 + sigma_shared^2), computed analytically (exact under the model; no need for
    # per-simulation arrays — see src/simulate.py::compute_analytical_win_probs
    # docstring). Verified this reconciles with the Monte Carlo run above:
    # sum(win_prob) lands within 0.1 seat of result.mean_seats.
    from src.classify import assign_tier_from_wp

    wp_df = compute_analytical_win_probs(
        targeting_df, sigma_df, summary.ohio_value, config,
        environment_sigma=summary.ohio_sigma,
    )
    # targeting_df's own `tier` column is a snapshot at a FIXED 48% reference
    # environment (Session 13). Our blended environment (currently ~50.1%)
    # is a different point, so a district's tier can legitimately have
    # moved since that snapshot — carry the 48%-reference tier forward as
    # `tier_at_48` for comparison, but compute the tier actually shown
    # alongside win_prob fresh, from win_prob itself, via the same
    # WP_TIER_THRESHOLDS bucketing classify.py uses. Showing the stale
    # 48%-reference tier next to a win_prob computed at a different
    # environment produced self-contradictory-looking rows (e.g. "Lean R"
    # next to a 57% D win probability) even though each number was
    # individually correct — this was a presentation bug, not a modeling one.
    detail_cols = [c for c in ["district", "composite_lean", "tier", "current_holder",
                                "open_seat_2026"] if c in targeting_df.columns]
    district_detail = wp_df.merge(targeting_df[detail_cols], on="district", how="left",
                                   suffixes=("", "_dup"))
    district_detail = district_detail.rename(columns={"tier": "tier_at_48"})
    district_detail["tier"] = district_detail["win_prob"].apply(assign_tier_from_wp)
    district_detail["margin_mean"] = summary.ohio_value + district_detail["composite_lean"] + district_detail["inc_shift"]
    district_detail["margin_sigma"] = np.sqrt(
        sigma_df.set_index("district")["sigma_total"].reindex(district_detail["district"]).values ** 2
        + summary.ohio_sigma ** 2
    )
    # Overlay ballot facts on top of the untouched fundamentals win_prob.
    # `effective_win_prob` is what the headline counts below use;
    # `win_prob` (the model's raw, fundamentals-only number) is kept as-is
    # for every district, overridden or not — this is the "preserve the
    # underlying win probability" contract from src/ballot_overrides.py.
    district_detail["ballot_status"] = district_detail["district"].map(
        lambda d: overrides[d].status if d in overrides else "contested"
    )
    district_detail["ballot_note"] = district_detail["district"].map(
        lambda d: overrides[d].note if d in overrides else ""
    )
    district_detail["effective_win_prob"] = district_detail.apply(
        lambda row: overrides[row["district"]].effective_dem_prob
        if row["district"] in overrides else row["win_prob"],
        axis=1,
    )

    district_detail = district_detail[
        ["district", "composite_lean", "tier", "tier_at_48", "current_holder", "open_seat_2026",
         "margin_mean", "margin_sigma", "win_prob", "effective_win_prob", "ballot_status", "ballot_note"]
    ].sort_values("effective_win_prob", ascending=False).reset_index(drop=True)

    n_favored = int((district_detail["effective_win_prob"] > 0.5).sum())
    expected_seats = float(district_detail["effective_win_prob"].sum())
    typer.echo(
        f"\n  Districts favored (effective win_prob > 50%): {n_favored}  |  "
        f"sum of effective win probabilities (expected seats): {expected_seats:.1f}  |  "
        f"MC mean (with ballot overrides applied): {result.mean_seats:.1f}"
    )
    typer.echo(
        "  Note: these two counts are usually close but not identical — "
        "'favored' is a headcount of districts individually above 50%, "
        "'expected seats' sums the full probability distribution. Neither "
        "is a claim about which specific districts will flip."
    )
    if override_districts:
        typer.echo(
            f"  {len(override_districts)} district(s) above use effective_win_prob "
            "from a ballot override, not the model's raw win_prob — see ballot_status/ballot_note columns."
        )

    detail_path = out_dir / f"{office.output_prefix}_forecast_district_detail.csv"
    district_detail.to_csv(detail_path, index=False, float_format="%.4f")
    typer.echo(f"  → {detail_path}")

    typer.echo(
        "\n  Note: this is a research-grade nowcast, not the final model — see "
        "CLAUDE.md's Polling Data section (MIN_OHIO_SIGMA, CANDIDATE_IDIOSYNCRASY_SIGMA) "
        "for the judgment calls behind the environment uncertainty used here. "
        "--details shows the environment inputs behind every projection."
    )


@app.command("polls-refresh")
def polls_refresh(
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be added without writing"),
):
    """Merge new Ohio governor/Senate and national generic-ballot polls from VoteHub into data/polls/."""
    from src.ingest_votehub import refresh_polls

    reports = refresh_polls(write=not dry_run)
    for rep in reports:
        typer.echo(f"  {rep.file}: {rep.added_polls} new poll(s), {rep.duplicate_polls} already present")
        for item in rep.added:
            typer.echo(f"    + {item}")
    typer.echo("\n  Dry run: nothing written." if dry_run else
               "\n  Previous files copied to data/polls/archive/; raw VoteHub response in data/polls/votehub/.")


@app.command("forecast-trend")
def forecast_trend(
    office_name: str = typer.Option("house", "--office", help="house | senate | congress"),
    start: str = typer.Option(None, "--start", help="First snapshot date (YYYY-MM-DD). Default: earliest date with >=2 OH governor polls."),
    freq_days: int = typer.Option(7, "--freq-days", help="Spacing between snapshots, in days."),
    n_sims: int = typer.Option(3_000, "--n-sims", help="Sims per snapshot (kept smaller than `forecast` — this runs many snapshots)."),
    include_historical: bool = typer.Option(True, "--include-historical/--no-historical"),
    seed: int = typer.Option(42, "--seed"),
):
    """
    Phase B forecast reconstructed at each of several past dates, using only
    polls that existed as of that date — "what would we have forecast on
    6/1, 6/9, ... ?" Writes one row per snapshot to
    reports/session8/{office}_forecast_trend.csv.

    This is NOT a backtest against a past election (no historical 2022/2018
    polling archive exists — see CLAUDE.md Known Limitations). It replays
    THIS cycle's own polls forward in time, point by point.
    """
    import numpy as np
    import pandas as pd
    from src.forecast_statewide import build_forecast
    from src.simulate import SimConfig, compute_sigma_prior, compute_shared_sigma, estimate_district_sigma, run_simulations
    from src.office import get_office
    from src.ballot_overrides import overrides_for_office

    office = get_office(office_name)
    if office.is_statewide:
        typer.echo(f"  Office '{office.name}' is statewide; forecast-trend not applicable here.")
        raise typer.Exit(1)

    targeting_path = Path(f"reports/session3/{office.output_prefix}_targeting.csv")
    composite_path = Path(f"reports/session2/{office.output_prefix}_composite_lean.csv")
    if not targeting_path.exists() or not composite_path.exists():
        typer.echo(f"ERROR: run `{office.name} classify` and `{office.name} composite` first.", err=True)
        raise typer.Exit(1)

    targeting_df = pd.read_csv(targeting_path)
    composite_df = pd.read_csv(composite_path)
    sigma_prior = compute_sigma_prior(composite_df)
    sigma_shared = compute_shared_sigma(composite_df)

    overrides = overrides_for_office(office.name)
    override_districts = [d for d in overrides if d in set(targeting_df["district"])]
    if override_districts:
        sim_targeting = targeting_df[~targeting_df["district"].isin(override_districts)].copy()
    else:
        sim_targeting = targeting_df

    if start is None:
        gov_df = pd.read_csv("data/polls/governor.csv", low_memory=False)
        gov_df["end_date"] = pd.to_datetime(gov_df["end_date"], format="%m/%d/%y", errors="coerce")
        oh_dates = sorted(gov_df.loc[gov_df["state"] == "OH", "end_date"].dropna().unique())
        start_ts = pd.Timestamp(oh_dates[1]) if len(oh_dates) > 1 else pd.Timestamp(oh_dates[0])
    else:
        start_ts = pd.Timestamp(start)

    end_ts = pd.Timestamp.now().normalize()
    dates = pd.date_range(start_ts, end_ts, freq=f"{freq_days}D")
    if len(dates) == 0 or dates[-1] != end_ts:
        dates = dates.append(pd.DatetimeIndex([end_ts]))

    typer.echo(f"=== {office.display_name} — forecast trend, {dates[0].date()} to {dates[-1].date()} ({len(dates)} snapshots) ===\n")

    rows = []
    for i, as_of in enumerate(dates):
        config = SimConfig(n_sims=n_sims, random_seed=seed + i)
        sigma_df = estimate_district_sigma(targeting_df, sigma_prior, config, sigma_shared=sigma_shared)
        sim_sigma = sigma_df[~sigma_df["district"].isin(override_districts)] if override_districts else sigma_df

        try:
            summary, env_samples = build_forecast(
                as_of=as_of, n_samples=n_sims, seed=seed + i,
                include_historical_governor_baseline=include_historical,
            )
        except ValueError as e:
            typer.echo(f"  {as_of.date()}: skipped — {e}")
            continue

        result = run_simulations(sim_targeting, sim_sigma, env_samples, config)
        if override_districts:
            rng_override = np.random.default_rng(seed + i + 999)
            forced = np.zeros(len(result.seat_counts), dtype=int)
            for d in override_districts:
                draws = rng_override.random(len(result.seat_counts)) < overrides[d].effective_dem_prob
                forced += draws.astype(int)
            result.seat_counts = result.seat_counts + forced

        def _comp(label, field):
            match = next((c for c in summary.components if c.label == label), None)
            return getattr(match, field) if match is not None else None

        # summary.weights is a separate tuple aligned by index with
        # summary.components (each signal's share of total blend precision,
        # 1/sigma^2) — not a field on EnvironmentSignal itself. Built as a
        # label -> weight lookup here so the trend CSV can carry each
        # signal's actual influence on the blend at that date, not just its
        # value: a signal can read far from the blend and still be right,
        # if its weight is low. Without this, forecast-trend's own output
        # (and anything built from it, like the artifact's trend chart)
        # shows four value lines with no way to tell which ones mattered —
        # confirmed this reads as a discrepancy/bug to a viewer rather than
        # normal precision-weighting behavior.
        _weight_by_label = dict(zip((c.label for c in summary.components), summary.weights))

        rows.append({
            "as_of": as_of.date().isoformat(),
            "environment_d_pct": round(summary.ohio_value * 100, 2),
            "environment_sigma": round(summary.ohio_sigma * 100, 2),
            "national_fundamentals_pct": round(_comp("national_fundamentals", "mean") * 100, 2) if _comp("national_fundamentals", "mean") is not None else None,
            "governor_polls_pct": round(_comp("governor_polls", "mean") * 100, 2) if _comp("governor_polls", "mean") is not None else None,
            "senate_polls_pct": round(_comp("senate_polls", "mean") * 100, 2) if _comp("senate_polls", "mean") is not None else None,
            "historical_governor_pct": round(_comp("historical_governor", "mean") * 100, 2) if _comp("historical_governor", "mean") is not None else None,
            "governor_polls_raw_pct": round(_comp("governor_polls", "raw_mean") * 100, 2) if _comp("governor_polls", "raw_mean") is not None else None,
            "senate_polls_raw_pct": round(_comp("senate_polls", "raw_mean") * 100, 2) if _comp("senate_polls", "raw_mean") is not None else None,
            "n_gov_polls": _comp("governor_polls", "n"),
            "n_senate_polls": _comp("senate_polls", "n"),
            "n_generic_ballot_polls": _comp("national_fundamentals", "n"),
            "national_fundamentals_wt": round(_weight_by_label.get("national_fundamentals", 0.0), 4),
            "governor_polls_wt": round(_weight_by_label.get("governor_polls", 0.0), 4),
            "senate_polls_wt": round(_weight_by_label.get("senate_polls", 0.0), 4),
            "historical_governor_wt": round(_weight_by_label.get("historical_governor", 0.0), 4),
            "mean_seats": round(float(result.seat_counts.mean()), 2),
            "p10_seats": int(np.percentile(result.seat_counts, 10)),
            "p90_seats": int(np.percentile(result.seat_counts, 90)),
        })
        typer.echo(f"  {as_of.date()}: env={rows[-1]['environment_d_pct']:.1f}% D  "
                   f"mean_seats={rows[-1]['mean_seats']:.1f}  [{rows[-1]['p10_seats']}, {rows[-1]['p90_seats']}]")

    out_dir = Path(_DEFAULT_SESSION8_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{office.output_prefix}_forecast_trend.csv"
    pd.DataFrame(rows).to_csv(out_path, index=False)
    typer.echo(f"\n  → {out_path}")


@app.command("forecast")
def house_forecast(
    n_sims: int = typer.Option(10_000, "--n-sims"),
    with_incumbency: bool = typer.Option(False, "--with-incumbency"),
    include_historical: bool = typer.Option(True, "--include-historical/--no-historical"),
    seed: int = typer.Option(42, "--seed"),
    details: bool = typer.Option(False, "--details", help="Show the environment model inputs (blend) instead of race headlines"),
):
    """Polls + fundamentals seat forecast for Ohio State House (Phase B). Top-level default; also `house forecast`."""
    _run_office_forecast("house", n_sims=n_sims, with_incumbency=with_incumbency,
                          include_historical=include_historical, seed=seed, details=details)


@senate_app.command("forecast")
def senate_forecast(
    n_sims: int = typer.Option(10_000, "--n-sims"),
    with_incumbency: bool = typer.Option(False, "--with-incumbency"),
    include_historical: bool = typer.Option(True, "--include-historical/--no-historical"),
    seed: int = typer.Option(42, "--seed"),
    details: bool = typer.Option(False, "--details", help="Show the environment model inputs (blend) instead of race headlines"),
):
    """Polls + fundamentals seat forecast for Ohio State Senate (Phase B)."""
    _run_office_forecast("senate", n_sims=n_sims, with_incumbency=with_incumbency,
                          include_historical=include_historical, seed=seed, details=details)


@congress_app.command("forecast")
def congress_forecast(
    n_sims: int = typer.Option(10_000, "--n-sims"),
    with_incumbency: bool = typer.Option(False, "--with-incumbency"),
    include_historical: bool = typer.Option(True, "--include-historical/--no-historical"),
    seed: int = typer.Option(42, "--seed"),
    details: bool = typer.Option(False, "--details", help="Show the environment model inputs (blend) instead of race headlines"),
):
    """Polls + fundamentals seat forecast for Ohio US House (Phase B)."""
    _run_office_forecast("congress", n_sims=n_sims, with_incumbency=with_incumbency,
                          include_historical=include_historical, seed=seed, details=details)


# ---------------------------------------------------------------------------
# Election night: live returns -> joint projection for every Ohio race
# ---------------------------------------------------------------------------

def _fmt_pct(x, digits: int = 1) -> str:
    import math
    return "—" if x is None or (isinstance(x, float) and math.isnan(x)) else f"{x * 100:.{digits}f}%"


def _print_election_night(result, focus: str | None = None, n_close: int = 10, details: bool = False) -> None:
    """Terminal dashboard. focus=None prints every office; otherwise one chamber in full."""
    import math
    from src.headlines import statewide_headlines
    from src.office import get_office

    races = result.races
    typer.echo(f"=== Ohio Election Night — {result.as_of:%Y-%m-%d %H:%M:%S} ===\n")
    n_final = int(races["is_final"].sum())
    typer.echo(f"  Returns in: {result.n_observations} race(s) with partial counts ({n_final} fully counted)")
    for w in result.warnings:
        typer.echo(f"  ⚠ {w}")
    if details:
        e, e_sd = result.environment
        e0, e0_sd = result.environment_prior
        s, s_sd = result.skew
        typer.echo(
            f"  Model internals — environment (generic statewide race): {_fmt_pct(e)} ± {_fmt_pct(e_sd)}"
            f" [pre-election {_fmt_pct(e0)} ± {_fmt_pct(e0_sd)}];"
            f" early-count skew {s * 100:+.1f} ± {s_sd * 100:.1f} pts at 0% reporting"
        )
    typer.echo()

    _print_statewide_headlines(statewide_headlines(result), live=True)

    offices = [focus] if focus else ["house", "senate", "congress"]
    typer.echo("  CHAMBERS (D seats)")
    for row in result.chambers.itertuples(index=False):
        if row.office not in offices:
            continue
        office = get_office(row.office)
        typer.echo(
            f"    {office.display_name:<18} mean {row.mean_d_seats:5.1f}  80% [{row.p10_seats}, {row.p90_seats}]"
            f"   decided/≥99%: D {row.called_d} R {row.called_r}   reporting {row.n_reporting}"
        )
        chamber = result.chambers[result.chambers["office"] == row.office].iloc[0]
        probs = "   ".join(
            f"P({c[2:-1]}) {chamber[c]:.0%}" for c in result.chambers.columns
            if c.startswith("P(") and not math.isnan(chamber[c])
        )
        typer.echo(f"      {probs}")
    typer.echo()

    for oname in offices:
        office = get_office(oname)
        sub = races[(races["office"] == oname) & (races["fixed_reason"] == "")]
        sub = sub.assign(_d=(sub["win_prob"] - 0.5).abs()).sort_values("_d")
        if not focus:
            sub = sub.head(n_close)
        typer.echo(f"  {office.display_name.upper()} — {'all races' if focus else 'closest races'}")
        typer.echo(f"    {'Dist':>4} {'Hold':>4} {'Open':>4}  {'Live D%':>8} {'In':>6}  {'Proj D%':>8} {'80% range':>15} {'P(D)':>6}  Call")
        for r in sub.itertuples():
            frac = "final" if r.is_final else (_fmt_pct(r.live_fraction, 0) if not math.isnan(r.live_fraction) else "")
            rng = f"{_fmt_pct(r.proj_low80)}–{_fmt_pct(r.proj_high80)}"
            typer.echo(
                f"    {int(r.district):>4} {r.current_holder or '':>4} {'yes' if r.open_seat_2026 else '':>4}  "
                f"{_fmt_pct(r.live_d_share):>8} {frac:>6}  {_fmt_pct(r.proj_d_share):>8} {rng:>15} "
                f"{r.win_prob:6.1%}  {r.call}"
            )
        fixed = races[(races["office"] == oname) & (races["fixed_reason"] != "")]
        if len(fixed):
            summary = ", ".join(f"{int(r.district)} ({r.call})" for r in fixed.itertuples())
            typer.echo(f"    Fixed outcomes: {summary}")
        typer.echo()


def _run_election_night(
    focus: str | None = None,
    n_sims: int = 10_000,
    with_incumbency: bool = False,
    early_skew: float | None = None,
    seed: int = 42,
    write: bool = True,
    details: bool = False,
):
    """
    Read the live-returns CSVs under data/raw/, condition the joint pre-election
    model for every Ohio race on them (src/election_night.py), and print the
    projection. Rerun after each edit; there is no polling loop.
    """
    import pandas as pd
    from src import election_night as en

    changed = en.ensure_return_files()
    for p in changed:
        typer.echo(f"  Created/updated returns template: {p}")

    kwargs = {"with_incumbency": with_incumbency}
    if early_skew is not None:
        kwargs["early_skew_mean"] = early_skew / 100
    model = en.build_prior_model(en.prior_as_of(), **kwargs)
    try:
        result = en.run_election_night(model, n_sims=n_sims, seed=seed)
    except ValueError as exc:
        typer.echo(f"ERROR in live returns: {exc}", err=True)
        raise typer.Exit(1)

    _print_election_night(result, focus=focus, details=details)
    if write:
        paths = en.write_outputs(result)
        typer.echo("  → " + "   ".join(str(p) for p in paths.values()))
    typer.echo(
        "\n  Enter returns in data/raw/live_returns_oh_{statewide,house,senate,congress}.csv "
        "(or the Election Night dashboard page). pct_reporting = the source's % of expected vote "
        "counted; is_final = every vote counted. Not a substitute for AP/network calls."
    )


_EN_N_SIMS = typer.Option(10_000, "--n-sims")
_EN_INC = typer.Option(False, "--with-incumbency")
_EN_SKEW = typer.Option(
    None, "--early-skew",
    help="Prior mean of the early-count D skew, in points at 0 pct reporting (default 0).",
)
_EN_SEED = typer.Option(42, "--seed")
_EN_WRITE = typer.Option(True, "--write/--no-write", help="Write reports/election_night/ outputs")
_EN_DETAILS = typer.Option(False, "--details", help="Also show model internals (environment, early-count skew)")


@app.command("election-night")
def election_night_all(
    office: str = typer.Option(
        None, "--office", help="Show one chamber in full: house, senate or congress.",
    ),
    n_sims: int = _EN_N_SIMS, with_incumbency: bool = _EN_INC,
    early_skew: float = _EN_SKEW, seed: int = _EN_SEED, write: bool = _EN_WRITE,
    details: bool = _EN_DETAILS,
):
    """Live projection for every Ohio race from manually entered returns. Also `house election-night`."""
    if office not in (None, "house", "senate", "congress"):
        typer.echo("--office must be house, senate or congress", err=True)
        raise typer.Exit(1)
    _run_election_night(office, n_sims, with_incumbency, early_skew, seed, write, details)


@senate_app.command("election-night")
def senate_election_night(
    n_sims: int = _EN_N_SIMS, with_incumbency: bool = _EN_INC,
    early_skew: float = _EN_SKEW, seed: int = _EN_SEED, write: bool = _EN_WRITE,
    details: bool = _EN_DETAILS,
):
    """Election-night projection, Ohio State Senate in full (same joint model)."""
    _run_election_night("senate", n_sims, with_incumbency, early_skew, seed, write, details)


@congress_app.command("election-night")
def congress_election_night(
    n_sims: int = _EN_N_SIMS, with_incumbency: bool = _EN_INC,
    early_skew: float = _EN_SKEW, seed: int = _EN_SEED, write: bool = _EN_WRITE,
    details: bool = _EN_DETAILS,
):
    """Election-night projection, Ohio US House in full (same joint model)."""
    _run_election_night("congress", n_sims, with_incumbency, early_skew, seed, write, details)


@multi_app.command("backbone")
def multi_backbone(
    build_all_maps: bool = typer.Option(
        False, "--build-all-maps",
        help="Build block-district maps for house, senate, and congress in one go.",
    ),
    force: bool = typer.Option(False, "--force", help="Rebuild even if cached"),
):
    """Cross-office backbone helpers."""
    if not build_all_maps:
        typer.echo("Use --build-all-maps. Other cross-office backbone work comes in A.9.")
        raise typer.Exit(0)
    for office_name in ("house", "senate", "congress"):
        _build_office_block_district_map(office_name, force=force)
    typer.echo("\n=== All block-district maps built ===")


def _run_office_report(office_name: str, output: Path | None = None) -> Path:
    """Generate a per-office summary PDF (senate/congress/house)."""
    import pandas as pd
    from src.office import get_office
    from src.export import generate_office_summary

    office = get_office(office_name)
    if office.is_statewide:
        typer.echo(f"  Office '{office.name}' is statewide; per-office summary PDF not applicable.")
        raise typer.Exit(1)

    targeting_path = Path(f"reports/session3/{office.output_prefix}_targeting.csv")
    composite_path = Path(f"reports/session2/{office.output_prefix}_composite_lean.csv")
    prob_path      = Path(f"reports/session8/{office.output_prefix}_probabilistic_scenarios.csv")
    wp_path        = Path(f"reports/session8/{office.output_prefix}_district_win_probs.csv")
    invest_path    = Path(f"reports/session8/{office.output_prefix}_investment_priority.csv")

    if not targeting_path.exists():
        typer.echo(f"ERROR: {targeting_path} not found. Run `{office.name} classify` first.", err=True)
        raise typer.Exit(1)
    if not composite_path.exists():
        typer.echo(f"ERROR: {composite_path} not found. Run `{office.name} composite` first.", err=True)
        raise typer.Exit(1)

    targeting_df = pd.read_csv(targeting_path)
    composite_df = pd.read_csv(composite_path)
    prob_df    = pd.read_csv(prob_path)    if prob_path.exists()    else None
    wp_df      = pd.read_csv(wp_path)      if wp_path.exists()      else None
    invest_df  = pd.read_csv(invest_path)  if invest_path.exists()  else None

    if output is None:
        output = Path("reports/session14") / f"{office.output_prefix}_office_summary.pdf"

    typer.echo(f"=== {office.display_name} — generating office summary PDF ===")
    out_path = generate_office_summary(
        office=office,
        targeting_df=targeting_df,
        composite_df=composite_df,
        prob_scenario_df=prob_df,
        win_prob_df=wp_df,
        investment_df=invest_df,
        output_path=output,
    )
    typer.echo(f"  → {out_path}")
    return out_path


@senate_app.command("report")
def senate_report(
    output: Path = typer.Option(None, "--output", "-o", help="Output PDF path"),
):
    """Generate per-office summary PDF for State Senate."""
    _run_office_report("senate", output=output)


def _run_office_backtest(office_name: str):
    """Office-aware backtest: pre-2024 -> 2024 predictions."""
    from src.office import get_office
    from src.backtest import run_backtest_for_office, write_office_backtest_outputs

    office = get_office(office_name)
    if office.is_statewide:
        typer.echo(f"  Office '{office.name}' is statewide; backtest not applicable.")
        raise typer.Exit(1)

    results = run_backtest_for_office(office)
    summary_path = write_office_backtest_outputs(office, results)
    typer.echo(f"\n  → reports/session12/{office.output_prefix}_backtest_district_predictions.csv")
    typer.echo(f"  → {summary_path}")


@senate_app.command("backtest")
def senate_backtest():
    """Out-of-sample backtest for State Senate (even districts on 2024 ballot)."""
    _run_office_backtest("senate")


@congress_app.command("backtest")
def congress_backtest():
    """Out-of-sample backtest for US House (Ohio CDs).

    Note: composite is on the NEW Oct 2025 plan; 2024 results are on the prior
    map. Per-district matching is not geometrically valid — illustrative only.
    """
    _run_office_backtest("congress")


@congress_app.command("report")
def congress_report(
    output: Path = typer.Option(None, "--output", "-o", help="Output PDF path"),
):
    """Generate per-office summary PDF for US House (Ohio CDs)."""
    _run_office_report("congress", output=output)


@multi_app.command("report")
def multi_report():
    """Generate summary PDFs for all four offices (house, senate, congress, governor)."""
    for office_name in ("house", "senate", "congress"):
        _run_office_report(office_name)
    # Governor uses its own dedicated PDF generator (statewide degenerate case).
    governor_report(output=None)
    typer.echo("\n=== All office summary PDFs generated ===")


# ── Auto-alias top-level commands under `house` ──────────────────────────────
# Convention going forward: `python cli.py house composite` is canonical;
# `python cli.py composite` continues to work. Anything added below as a
# top-level command will be picked up automatically.
def _alias_top_level_into_house():
    seen: set[str] = set()
    for cmd in list(app.registered_commands):
        if cmd.name in seen:
            continue
        seen.add(cmd.name)
        house_app.command(name=cmd.name, help=cmd.help)(cmd.callback)


_alias_top_level_into_house()


if __name__ == "__main__":
    app()
