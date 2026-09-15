"""
composite.py — Weighted composite partisan lean index.

Weights are configurable. When a race/year is unavailable, its weight is
redistributed proportionally among the races that ARE available.
The effective weights are recorded in the output so analysts can see exactly
what went into any given district's composite score.
"""

from __future__ import annotations

import contextlib
import io
from pathlib import Path

import pandas as pd

# Default weights: (year, race_label) -> weight
# Gubernatorial years weighted more than presidential because turnout
# composition more closely resembles a state house midterm electorate.
DEFAULT_WEIGHTS: dict[tuple[str, str], float] = {
    ("2024", "pre"):          0.20,
    ("2024", "uss"):          0.05,
    ("2022", "gov"):          0.25,
    ("2022", "statewide_avg"): 0.15,
    ("2020", "pre"):          0.15,
    ("2018", "gov"):          0.15,
    ("2018", "statewide_avg"): 0.10,
    # Remaining 0.05 is split across uss races if available
    ("2022", "uss"):          0.025,
    ("2018", "uss"):          0.025,
}

# Which races feed the statewide_avg composite per year
STATEWIDE_AVG_INPUTS: dict[str, list[str]] = {
    "2022": ["atg", "aud", "sos_off", "tre"],
    "2018": ["atg", "aud", "sos_off", "tre"],
}


# ---------------------------------------------------------------------------
# Reference statewide race ("the statewide environment") per cycle
# ---------------------------------------------------------------------------

REFERENCE_MIDTERM_RACES: tuple[str, ...] = ("atg", "aud", "sos_off", "tre")
"""
Races averaged (unweighted mean of two-party D shares) to form the reference
statewide D share in a midterm year. Presidential years use the presidential
race alone. Same four races as the composite's `statewide_avg`.

The reference share is what "statewide D%" means everywhere downstream:
year_baselines.json, candidate effects, anomaly residuals, the simulation's
`statewide_d`, and the race-to-environment adjustments in
src/forecast_statewide.py.

Midterms used the governor race until Sept 2026. It was replaced because the
governor is the most candidate-driven race on a midterm ballot and drifts
far from the rest of the ticket: in 2022 Whaley (37.5%) ran 2.4-4.0 pts
behind every down-ballot executive race, and contested House Democrats beat
her by 6.8 pts on average. That one year's gap was being counted as
district-level noise (see simulate.compute_sigma_prior).
"""


def reference_share(race_shares: dict[str, float], year: int | str) -> float:
    """
    Reference statewide two-party D share for one cycle.

    `race_shares` maps short race label ('pre', 'atg', ...) to that race's
    statewide two-party D share. Presidential years return 'pre'; midterms
    return the mean of whichever REFERENCE_MIDTERM_RACES are present.
    """
    year = int(year)
    if year % 4 == 0:
        if "pre" not in race_shares:
            raise KeyError(f"No presidential race available for reference share in {year}")
        return float(race_shares["pre"])
    vals = [race_shares[r] for r in REFERENCE_MIDTERM_RACES if r in race_shares]
    if not vals:
        raise KeyError(
            f"None of {REFERENCE_MIDTERM_RACES} available for reference share in {year}"
        )
    return float(sum(vals) / len(vals))


def compute_year_baselines(sos_files: dict) -> dict[str, float]:
    """
    Reference statewide two-party D share for each loaded SOS year.

    `sos_files` maps year string -> SosFile (src.ingest_sos). Years lacking
    every reference race are skipped, not guessed.
    """
    from src.ingest_sos import get_race_df

    baselines: dict[str, float] = {}
    for year, sos in sos_files.items():
        races = ("pre",) if int(year) % 4 == 0 else REFERENCE_MIDTERM_RACES
        shares: dict[str, float] = {}
        for race in races:
            if race in sos.statewide:
                rd = get_race_df(sos, race)
                d, r = float(rd["d_votes"].sum()), float(rd["r_votes"].sum())
                if d + r > 0:
                    shares[race] = d / (d + r)
        if shares:
            baselines[str(year)] = reference_share(shares, year)
    return baselines


def compute_statewide_avg_lean(
    district_leans: dict[tuple[str, str], pd.Series],
    year: str,
) -> pd.Series | None:
    """
    Compute the unweighted mean lean across non-governor statewide races for a year.

    Parameters
    ----------
    district_leans : {(year, race_label): Series indexed by district_num}
    year : '2022' or '2018'

    Returns
    -------
    Series indexed by district_num, or None if no constituent races are available.
    """
    inputs = STATEWIDE_AVG_INPUTS.get(year, [])
    available = [
        district_leans[(year, lbl)]
        for lbl in inputs
        if (year, lbl) in district_leans
    ]
    if not available:
        return None

    stacked = pd.concat(available, axis=1)
    avg = stacked.mean(axis=1)
    print(
        f"  statewide_avg {year}: mean of "
        f"{[lbl for lbl in inputs if (year, lbl) in district_leans]}"
    )
    return avg


def build_composite(
    district_leans: dict[tuple[str, str], pd.Series],
    weights: dict[tuple[str, str], float] | None = None,
    all_districts: list[int] | None = None,
) -> pd.DataFrame:
    """
    Build a per-district composite lean index.

    Parameters
    ----------
    district_leans : {(year, race_label): Series indexed by district_num}
        Lean values (district D-share minus statewide D-share) for each race.
    weights : Weight dict; defaults to DEFAULT_WEIGHTS.
    all_districts : Expected district numbers. REQUIRED for non-house offices.
        For backward compatibility, defaults to Ohio House (1..99) when omitted.
        Pass `office.district_range` for senate, congress, etc.

    Returns
    -------
    DataFrame indexed by district with columns:
      composite_lean, <race>_lean (one per input), effective_weight_<race>
    """
    if weights is None:
        weights = DEFAULT_WEIGHTS
    if all_districts is None:
        all_districts = list(range(1, 100))  # default = Ohio House

    # Inject statewide_avg as a virtual race
    augmented = dict(district_leans)
    for year in ["2022", "2018"]:
        avg = compute_statewide_avg_lean(district_leans, year)
        if avg is not None:
            augmented[(year, "statewide_avg")] = avg

    # Determine which races are available
    available_keys = set(augmented.keys())
    requested_keys = set(weights.keys())
    present = requested_keys & available_keys
    missing = requested_keys - available_keys

    if missing:
        print(f"\n  Missing races (weight will be redistributed): {sorted(missing)}")

    # Redistribute weight from missing races proportionally to present ones
    total_present_weight = sum(weights[k] for k in present)
    total_all_weight = sum(weights.values())

    if total_present_weight == 0:
        raise ValueError("No races with non-zero weight are available.")

    # Scale up missing-race weight, then normalize so weights always sum to 1.0
    raw_effective = {k: weights[k] * (total_all_weight / total_present_weight) for k in present}
    norm = sum(raw_effective.values())
    effective_weights = {k: v / norm for k, v in raw_effective.items()}

    print("\n  Effective composite weights:")
    for k, w in sorted(effective_weights.items(), key=lambda x: -x[1]):
        print(f"    {k[0]} {k[1]:20s}  {w:.4f}")
    print(f"    {'Total':24s}  {sum(effective_weights.values()):.4f}")

    # Build output DataFrame
    idx = pd.Index(sorted(all_districts), name="district")
    out = pd.DataFrame(index=idx)

    for key in sorted(present):
        col_name = f"{key[1]}_{key[0]}_lean"
        series = augmented[key].reindex(idx)
        out[col_name] = series

    # Compute composite as weighted sum
    composite = pd.Series(0.0, index=idx)
    for key, w in effective_weights.items():
        col = f"{key[1]}_{key[0]}_lean"
        if col in out.columns:
            composite += out[col].fillna(0.0) * w

    out.insert(0, "composite_lean", composite)
    out = out.reset_index()

    # Report distribution
    cl = out["composite_lean"]
    print(f"\n  Composite lean distribution:")
    print(f"    Most D: District {out.loc[cl.idxmax(), 'district']:.0f}  ({cl.max():+.4f})")
    print(f"    Most R: District {out.loc[cl.idxmin(), 'district']:.0f}  ({cl.min():+.4f})")
    print(f"    Median: {cl.median():+.4f}  Mean: {cl.mean():+.4f}")
    print(f"    D-leaning (>0): {(cl > 0).sum()}  R-leaning (≤0): {(cl <= 0).sum()}")

    return out


def merge_composite_with_house_results(
    composite_df: pd.DataFrame,
    house_wide: pd.DataFrame,
    available_years: list[str],
    year_baselines: dict[str, float] | None = None,
) -> pd.DataFrame:
    """
    Join composite lean with actual house results and compute candidate effects.

    Candidate effect = actual house D share - expected D share, where:
        expected D share = statewide D baseline for that cycle + composite lean.

    Only computed for contested races where a year baseline is provided.

    Parameters
    ----------
    year_baselines : {year: statewide_dem_two_party_share} for the reference
        statewide race each cycle (see compute_year_baselines). If None or
        missing for a year, effect is left as NaN.
    """
    merged = composite_df.copy()
    if not house_wide.empty:
        merged = merged.merge(house_wide, on="district", how="left")

    merged = add_candidate_effects(merged, available_years, year_baselines or {})
    return merged.sort_values("composite_lean", ascending=False).reset_index(drop=True)


def add_candidate_effects(
    df: pd.DataFrame,
    available_years: list[str],
    year_baselines: dict[str, float],
) -> pd.DataFrame:
    """
    (Re)write candidate_effect_{year} = dem_share_{year} - (baseline + composite_lean)
    for contested district-years. Uncontested rows and years without a
    baseline are NaN.
    """
    df = df.copy()
    for year in available_years:
        share_col = f"dem_share_{year}"
        contested_col = f"contested_{year}"
        effect_col = f"candidate_effect_{year}"
        baseline = year_baselines.get(year)

        if share_col not in df.columns or contested_col not in df.columns:
            continue

        df[effect_col] = float("nan")
        if baseline is None:
            continue

        contested_mask = df[contested_col].fillna(False).astype(bool)
        expected = baseline + df["composite_lean"]
        df.loc[contested_mask, effect_col] = (
            df.loc[contested_mask, share_col] - expected.loc[contested_mask]
        )
    return df


# ---------------------------------------------------------------------------
# Drop-one sensitivity analysis
# ---------------------------------------------------------------------------

def drop_one_sensitivity(
    district_leans: dict[tuple[str, str], pd.Series],
    weights: dict[tuple[str, str], float] | None = None,
    all_districts: list[int] | None = None,
    output_path: str | Path | None = None,
) -> pd.DataFrame:
    """
    For each race in the composite, recompute lean without that race and
    record the per-district change.

    Returns a wide DataFrame:
      district, composite_lean_full,
      composite_lean_drop_{race}{year} (one per race in weights),
      max_change, most_sensitive_to

    Writes data/processed/drop_one_sensitivity.csv.
    """
    if weights is None:
        weights = DEFAULT_WEIGHTS
    if all_districts is None:
        all_districts = list(range(1, 100))

    def _build_silent(w: dict) -> pd.DataFrame:
        """Run build_composite with stdout suppressed."""
        with contextlib.redirect_stdout(io.StringIO()):
            return build_composite(district_leans, w, all_districts)

    # Full composite
    full_df = _build_silent(weights)
    result = full_df[["district", "composite_lean"]].rename(
        columns={"composite_lean": "composite_lean_full"}
    )

    change_cols: list[str] = []

    for drop_key in sorted(weights.keys()):
        reduced_weights = {k: v for k, v in weights.items() if k != drop_key}
        col_name = f"composite_lean_drop_{drop_key[1]}{drop_key[0]}"
        change_col = f"_change_{drop_key[1]}{drop_key[0]}"

        try:
            reduced_df = _build_silent(reduced_weights)
            reduced_lean = reduced_df[["district", "composite_lean"]].rename(
                columns={"composite_lean": col_name}
            )
            result = result.merge(reduced_lean, on="district", how="left")
            result[change_col] = result[col_name] - result["composite_lean_full"]
            change_cols.append(change_col)
        except Exception as exc:
            print(f"  WARNING: drop-one failed for {drop_key}: {exc}")

    # Summarise sensitivity
    if change_cols:
        result["max_change"] = result[change_cols].abs().max(axis=1)
        most_sens_raw = result[change_cols].abs().idxmax(axis=1)
        result["most_sensitive_to"] = most_sens_raw.str.replace("_change_", "", regex=False)
    else:
        result["max_change"] = 0.0
        result["most_sensitive_to"] = ""

    # Drop internal change columns (keep only lean + summary)
    result = result.drop(columns=change_cols)

    # ── Print summary ────────────────────────────────────────────────────────
    print("\nDROP-ONE SENSITIVITY ANALYSIS")
    print("=" * 50)
    n_2pt = (result["max_change"] > 0.02).sum()
    print(f"Districts where dropping any race changes lean by >2 pts: {n_2pt}")

    # Tier-change count: check if dropping any race pushes lean across a tier boundary
    from src.classify import assign_tier
    tier_changes = 0
    lean_drop_cols = [c for c in result.columns if c.startswith("composite_lean_drop_")]
    for _, row in result.iterrows():
        base_tier = assign_tier(row["composite_lean_full"])
        for col in lean_drop_cols:
            if assign_tier(row[col]) != base_tier:
                tier_changes += 1
                break
    print(f"Districts where dropping any race changes tier: {tier_changes}")

    top = result.nlargest(10, "max_change")
    if not top.empty:
        print(f"\nMost sensitive districts (top 10):")
        print(f"  {'Dist':>4}  {'Full Lean':>9}  {'Max Change':>10}  When Dropping")
        for _, row in top.iterrows():
            print(
                f"  {int(row['district']):>4}  {row['composite_lean_full']:>+9.3f}  "
                f"  {row['max_change']:>+9.3f}  {row['most_sensitive_to']}"
            )

    # Most influential race: mean absolute change across all districts
    if change_cols:
        # Rebuild change cols temporarily for this calc
        full_df2 = _build_silent(weights)
        res_temp = full_df2[["district", "composite_lean"]].rename(columns={"composite_lean": "composite_lean_full"})
        influence: dict[str, float] = {}
        for drop_key in sorted(weights.keys()):
            reduced_weights = {k: v for k, v in weights.items() if k != drop_key}
            race_label = f"{drop_key[1]}{drop_key[0]}"
            try:
                reduced_df = _build_silent(reduced_weights)
                diff = (
                    reduced_df.set_index("district")["composite_lean"]
                    - full_df2.set_index("district")["composite_lean"]
                ).abs().mean()
                influence[race_label] = float(diff)
            except Exception:
                pass
        if influence:
            most_inf = max(influence, key=influence.get)
            print(f"\nMost influential race: {most_inf} (mean |change| = {influence[most_inf]:.3f})")
            print("  Full ranking:")
            for race, val in sorted(influence.items(), key=lambda x: -x[1]):
                print(f"    {race:25s}  mean |Δ| = {val:.4f}")

    # ── Write output ─────────────────────────────────────────────────────────
    if output_path is None:
        out_path = Path(__file__).parent.parent / "data" / "processed" / "drop_one_sensitivity.csv"
    else:
        out_path = Path(output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(out_path, index=False, float_format="%.6f")
    print(f"\nDrop-one sensitivity written to: {out_path}")

    return result
