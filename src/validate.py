"""
validate.py — Validation summary report generation.

Collects results from crosswalk and partisan validation passes and writes
a human-readable text file to reports/.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd
import geopandas as gpd


REPORTS_DIR = Path(__file__).parent.parent / "reports"


def write_validation_summary(
    precincts: gpd.GeoDataFrame,
    districts: gpd.GeoDataFrame,
    fragments: gpd.GeoDataFrame,
    district_votes: pd.DataFrame,
    output_df: pd.DataFrame,
    crosswalk_issues: list[str],
    partisan_issues: list[str],
    vote_cols: list[str],
    output_path: str | Path | None = None,
) -> Path:
    """
    Write a validation summary to reports/session1/validation_summary.txt.

    Parameters
    ----------
    precincts, districts : GeoDataFrames from ingest step.
    fragments : GeoDataFrame from crosswalk step.
    district_votes : DataFrame with allocated votes per district.
    output_df : Final sorted output DataFrame.
    crosswalk_issues, partisan_issues : Validation message lists.
    vote_cols : All vote columns used.
    output_path : Override output file path (optional).

    Returns
    -------
    Path to written file.
    """
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = Path(output_path) if output_path else REPORTS_DIR / "validation_summary.txt"

    lines: list[str] = []

    def h(title: str):
        lines.append("")
        lines.append("=" * 70)
        lines.append(f"  {title}")
        lines.append("=" * 70)

    def sub(title: str):
        lines.append("")
        lines.append(f"--- {title} ---")

    lines.append("Ohio House Model — Validation Summary")
    lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # ------------------------------------------------------------------ #
    h("1. Data Overview")
    lines.append(f"  Precincts loaded:    {len(precincts):,}")
    lines.append(f"  Districts loaded:    {len(districts):,}")
    lines.append(f"  Overlay fragments:   {len(fragments):,}")
    lines.append(f"  Vote columns found:  {len(vote_cols)}")
    lines.append(f"  CRS used:            {precincts.crs}")

    # ------------------------------------------------------------------ #
    h("2. Vote Reconciliation (Precinct Total vs District Total)")
    for issue in crosswalk_issues:
        if "vote reconciliation" in issue or "PASS vote" in issue or "FAIL vote" in issue:
            lines.append(f"  {issue}")

    # ------------------------------------------------------------------ #
    h("3. Split-Precinct Statistics")
    n_split = (fragments.groupby("precinct_id")["district_num"].nunique() > 1).sum()
    total_precincts = fragments["precinct_id"].nunique()
    lines.append(f"  Precincts split across district boundaries: {n_split:,} / {total_precincts:,} ({n_split/total_precincts:.1%})")

    # Precinct area-fraction distribution
    frac_sums = fragments.groupby("precinct_id")["area_fraction"].sum()
    lines.append(f"  Area-fraction sum per precinct — min: {frac_sums.min():.6f}, max: {frac_sums.max():.6f}, mean: {frac_sums.mean():.6f}")

    # ------------------------------------------------------------------ #
    h("4. District Vote Coverage")
    if "partisan_lean" in output_df.columns and "total_two_party" in output_df.columns:
        low_vote_threshold = 1000
        low_districts = output_df[output_df["total_two_party"] < low_vote_threshold]
        if len(low_districts):
            lines.append(f"  WARNING: {len(low_districts)} district(s) with < {low_vote_threshold:,} two-party votes:")
            for _, row in low_districts.iterrows():
                lines.append(f"    District {int(row['district'])}: {row['total_two_party']:,.1f} votes")
        else:
            lines.append(f"  All districts have >= {low_vote_threshold:,} two-party votes.")

        lines.append("")
        lines.append(f"  Total two-party votes (statewide): {output_df['total_two_party'].sum():,.0f}")

    # ------------------------------------------------------------------ #
    h("5. Statewide Partisan Result Validation")
    for issue in partisan_issues:
        lines.append(f"  {issue}")

    # ------------------------------------------------------------------ #
    h("6. District Lean Distribution")
    if "partisan_lean" in output_df.columns:
        lean = output_df["partisan_lean"]
        lines.append(f"  Most D district:  {output_df.loc[lean.idxmax(), 'district']:.0f}  (lean = {lean.max():+.4f})")
        lines.append(f"  Most R district:  {output_df.loc[lean.idxmin(), 'district']:.0f}  (lean = {lean.min():+.4f})")
        lines.append(f"  Median lean:      {lean.median():+.4f}")
        lines.append(f"  Mean lean:        {lean.mean():+.4f}  (should be ~0 by construction)")
        d_leaning = (lean > 0).sum()
        r_leaning = (lean <= 0).sum()
        lines.append(f"  D-leaning districts (lean > 0): {d_leaning}")
        lines.append(f"  R-leaning districts (lean ≤ 0): {r_leaning}")

    # ------------------------------------------------------------------ #
    h("7. All Validation Checks")
    sub("Crosswalk")
    for issue in crosswalk_issues:
        lines.append(f"  {issue}")
    sub("Partisan / Statewide")
    for issue in partisan_issues:
        lines.append(f"  {issue}")

    # ------------------------------------------------------------------ #
    lines.append("")
    lines.append("=" * 70)
    lines.append("  End of validation summary")
    lines.append("=" * 70)
    lines.append("")

    out_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"\nValidation summary written to: {out_path}")
    return out_path


# ---------------------------------------------------------------------------
# Redistricting overlap check
# ---------------------------------------------------------------------------

JACCARD_SAME = 0.70
JACCARD_REDRAWN = 0.30


def check_precinct_redistricting_overlap(
    sos_files: dict,
    output_path: str | Path | None = None,
) -> pd.DataFrame:
    """
    Compute precinct-membership Jaccard similarity for two redistricting transitions:

    1. Old → Interim  (2020 vs 2022): identifies districts where pre-2022 house
       results are from a different electorate.  'relocated' or 'redrawn' → drop
       2018/2020 house data for that district.

    2. Interim → Final (2022 vs 2024): identifies districts where the 2022
       interim maps differed from the final 2024 maps.  'relocated' (Jaccard < 0.30)
       → drop 2022 house data for that district as well.

    Jaccard categories (same thresholds for both transitions):
      >= 0.70  → 'same'      (results usable)
      0.30–0.70 → 'redrawn'  (results suspect)
      < 0.30   → 'relocated' (results from a different electorate — drop)

    Output columns (per district):
      jaccard_similarity, overlap_category          — old→interim (2020 vs 2022)
      jaccard_interim_final, overlap_category_interim_final — interim→final (2022 vs 2024)
      years_reliable                                — comma-separated reliable house years
      n_precincts_2020, n_precincts_2022, n_precincts_shared (old→interim counts)

    Returns a DataFrame with one row per district (1–99).
    Writes reports/redistricting_overlap.csv.
    """
    from src.ingest_sos import COUNTY_COL, PREC_CODE_COL

    sos_old = sos_files.get("2020") or sos_files.get("2018")
    sos_interim = sos_files.get("2022")
    sos_final = sos_files.get("2024")

    if sos_old is None or sos_interim is None:
        raise ValueError(
            "Need at least a 2020 (or 2018) and a 2022 SOS file to run overlap check."
        )

    def _district_precincts(sos, dist_num: int) -> set:
        if dist_num not in sos.house:
            return set()
        spec = sos.house[dist_num]
        ph = sos.precinct_house
        vote_cols = [c for c in spec.d_cols + spec.r_cols if c in ph.columns]
        if not vote_cols:
            return set()
        mask = ph[vote_cols].sum(axis=1) > 0
        rows = ph[mask]
        return set(zip(
            rows[COUNTY_COL].str.strip().str.upper(),
            rows[PREC_CODE_COL].astype(str).str.strip(),
        ))

    def _jaccard_and_category(set_a: set, set_b: set) -> tuple[float, str]:
        union = set_a | set_b
        if not union:
            return float("nan"), "unknown"
        j = len(set_a & set_b) / len(union)
        if j >= JACCARD_SAME:
            return j, "same"
        if j >= JACCARD_REDRAWN:
            return j, "redrawn"
        return j, "relocated"

    records = []
    for dist in range(1, 100):
        prec_old      = _district_precincts(sos_old, dist)
        prec_interim  = _district_precincts(sos_interim, dist)
        prec_final    = _district_precincts(sos_final, dist) if sos_final else set()

        # Old → Interim
        j_oi, cat_oi = _jaccard_and_category(prec_old, prec_interim)

        # Interim → Final
        if sos_final is not None:
            j_if, cat_if = _jaccard_and_category(prec_interim, prec_final)
        else:
            j_if, cat_if = float("nan"), "unknown"

        # Years reliable: 2024 always; 2022 if interim→final not relocated;
        # 2018/2020 only if old→interim is 'same'
        reliable = ["2024"]
        if cat_if != "relocated":
            reliable.insert(0, "2022")
        if cat_oi == "same":
            reliable = ["2018", "2020"] + reliable

        records.append({
            "district": dist,
            # Old → interim (primary, backward-compatible column names)
            "jaccard_similarity": round(j_oi, 4) if pd.notna(j_oi) else float("nan"),
            "overlap_category": cat_oi,
            "n_precincts_2020": len(prec_old),
            "n_precincts_2022": len(prec_interim),
            "n_precincts_shared": len(prec_old & prec_interim),
            # Interim → final
            "jaccard_interim_final": round(j_if, 4) if pd.notna(j_if) else float("nan"),
            "overlap_category_interim_final": cat_if,
            "n_precincts_2024": len(prec_final),
            "n_precincts_shared_interim_final": len(prec_interim & prec_final),
            # Summary
            "years_reliable": ",".join(reliable),
        })

    overlap_df = pd.DataFrame(records)

    out_path = Path(output_path) if output_path else REPORTS_DIR / "redistricting_overlap.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    overlap_df.to_csv(out_path, index=False, float_format="%.4f")

    # ── Print summary ─────────────────────────────────────────────────────────
    def _print_comparison(label: str, cat_col: str, j_col: str,
                          pre_col: str, post_col: str, shared_col: str) -> None:
        print(f"\nPrecinct Redistricting Overlap Check ({label})")
        print("=" * 55)
        for cat in ["same", "redrawn", "relocated", "unknown"]:
            n = (overlap_df[cat_col] == cat).sum()
            print(f"  {cat:12s}: {n:2d} districts")
        relocated = overlap_df[overlap_df[cat_col] == "relocated"].sort_values(j_col)
        if len(relocated):
            print(f"\n  Relocated districts (Jaccard < {JACCARD_REDRAWN}):")
            for _, row in relocated.iterrows():
                j = row[j_col]
                js = f"{j:.4f}" if pd.notna(j) else "  NaN"
                print(f"    District {int(row['district']):3d}: Jaccard={js}  "
                      f"pre={int(row[pre_col]):4d}  post={int(row[post_col]):4d}  "
                      f"shared={int(row[shared_col])}")
        redrawn = overlap_df[overlap_df[cat_col] == "redrawn"].sort_values(j_col)
        if len(redrawn):
            print(f"\n  Redrawn districts (0.30 ≤ Jaccard < 0.70):")
            for _, row in redrawn.iterrows():
                j = row[j_col]
                js = f"{j:.4f}" if pd.notna(j) else "  NaN"
                print(f"    District {int(row['district']):3d}: Jaccard={js}  "
                      f"pre={int(row[pre_col]):4d}  post={int(row[post_col]):4d}  "
                      f"shared={int(row[shared_col])}")

    _print_comparison(
        "2020 → 2022 (old → interim)",
        "overlap_category", "jaccard_similarity",
        "n_precincts_2020", "n_precincts_2022", "n_precincts_shared",
    )
    if sos_final is not None:
        _print_comparison(
            "2022 → 2024 (interim → final)",
            "overlap_category_interim_final", "jaccard_interim_final",
            "n_precincts_2022", "n_precincts_2024", "n_precincts_shared_interim_final",
        )

    # Years-reliable summary
    only_2024 = (overlap_df["years_reliable"] == "2024").sum()
    with_2022 = overlap_df["years_reliable"].str.contains("2022").sum()
    with_pre = overlap_df["years_reliable"].str.contains("2018").sum()
    print(f"\n  Years-reliable summary:")
    print(f"    All 4 years (2018–2024): {with_pre:2d} districts")
    print(f"    2022 + 2024 only:        {with_2022 - with_pre:2d} districts")
    print(f"    2024 only:               {only_2024:2d} districts")

    print(f"\n  Overlap CSV written to: {out_path}")
    return overlap_df


# ---------------------------------------------------------------------------
# Residual anomaly detection
# ---------------------------------------------------------------------------

_PRE_REDISTRICTING_YEARS = {2018, 2020}
_REDISTRICTING_BAD_CATEGORIES = {"relocated", "redrawn"}


def detect_anomalies(
    composite_df: pd.DataFrame,
    actual_results_long: pd.DataFrame,
    year_baselines: dict[str, float],
    redistricting_df: pd.DataFrame | None = None,
    high_threshold: float = 0.15,
    moderate_threshold: float = 0.10,
    output_path: str | Path | None = None,
) -> pd.DataFrame:
    """
    Flag district-year observations where the actual house result diverges
    from what the composite lean predicts by more than a threshold.

    Expected D share = statewide_baseline_for_year + composite_lean.
    Residual = actual_dem_share - expected_d_share.

    Auto-explanation priority (first match wins):
      1. redistricting_artifact — pre-2022 year, district is relocated/redrawn
      2. nominal_candidate     — either candidate got <25% of two-party vote
      3. possible_open_seat    — winning party changed from prior contested cycle
      4. unexplained           — no automated explanation found

    Parameters
    ----------
    composite_df         : DataFrame with 'district' and 'composite_lean'.
    actual_results_long  : Long-format house results with columns
                           year, district, dem_share, contested, winner.
    year_baselines       : {year_str: statewide_d_two_party_share}.
    redistricting_df     : Output of check_precinct_redistricting_overlap()
                           with 'district' and 'overlap_category'.
    high_threshold       : |residual| cutoff for high severity.
    moderate_threshold   : |residual| cutoff for moderate severity.
    output_path          : Override output CSV path.

    Returns
    -------
    DataFrame of all flagged observations, sorted by abs_residual descending.
    """
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = Path(output_path) if output_path else REPORTS_DIR / "anomaly_flags.csv"

    # ── Build working dataset ────────────────────────────────────────────────
    lean = composite_df[["district", "composite_lean"]].copy()
    contested = actual_results_long[actual_results_long["contested"]].copy()
    contested["year"] = contested["year"].astype(int)

    data = contested.merge(lean, on="district", how="left")

    # Only rows where we have a baseline for this year
    data = data[data["year"].astype(str).isin(year_baselines)].copy()
    data["expected_d_share"] = data["year"].apply(
        lambda y: year_baselines[str(y)]
    ) + data["composite_lean"]
    data["residual"] = data["dem_share"] - data["expected_d_share"]
    data["abs_residual"] = data["residual"].abs()

    # ── Build prior-cycle winner-party lookup for open-seat check ────────────
    def _party(w: object) -> str | None:
        if pd.isna(w):
            return None
        s = str(w).upper()
        if s.startswith("D"):
            return "D"
        if s.startswith("R"):
            return "R"
        return None

    # All contested rows: {(district, year): winner_party}
    all_contested = actual_results_long[actual_results_long["contested"]].copy()
    all_contested["year"] = all_contested["year"].astype(int)
    prior_winner: dict[tuple[int, int], str | None] = {
        (int(r["district"]), int(r["year"])): _party(r["winner"])
        for _, r in all_contested.iterrows()
    }

    # ── Build redistricting lookups ──────────────────────────────────────────
    # old→interim: 2018/2020 data unreliable for 'relocated' or 'redrawn'
    redistricting_bad: set[int] = set()
    # interim→final: 2022 data unreliable for 'relocated' only (Jaccard < 0.30)
    interim_final_relocated: set[int] = set()
    if redistricting_df is not None:
        redistricting_bad = set(
            redistricting_df.loc[
                redistricting_df["overlap_category"].isin(_REDISTRICTING_BAD_CATEGORIES),
                "district",
            ].astype(int)
        )
        if "overlap_category_interim_final" in redistricting_df.columns:
            # Flag any district with interim→final overlap < 0.70 (redrawn OR relocated)
            # for 2022 residual explanation. We only DROP 2022 data for 'relocated'
            # (Jaccard < 0.30), but we label both 'redrawn' and 'relocated' as
            # redistricting_artifact in the anomaly explanation — a Jaccard of 0.30–0.70
            # still indicates substantial boundary change that can produce large residuals.
            interim_final_relocated = set(
                redistricting_df.loc[
                    redistricting_df["overlap_category_interim_final"].isin(
                        _REDISTRICTING_BAD_CATEGORIES
                    ),
                    "district",
                ].astype(int)
            )

    # ── Auto-explain each row ────────────────────────────────────────────────
    def _explain(row: pd.Series) -> str:
        dist = int(row["district"])
        year = int(row["year"])
        dem_share = float(row["dem_share"])

        # 1. Redistricting artifact: pre-2022 data for a relocated/redrawn district,
        #    OR 2022 data for a district relocated between interim→final maps
        if year in _PRE_REDISTRICTING_YEARS and dist in redistricting_bad:
            return "redistricting_artifact"
        if year == 2022 and dist in interim_final_relocated:
            return "redistricting_artifact"

        # 2. Nominal candidate: either candidate got <25% of two-party vote
        if dem_share < 0.25 or dem_share > 0.75:
            return "nominal_candidate"

        # 3. Possible open seat: winning party changed from prior contested cycle
        current_party = _party(row.get("winner"))
        prior_party = prior_winner.get((dist, year - 2))
        if prior_party is not None and current_party is not None and current_party != prior_party:
            return "possible_open_seat"

        return "unexplained"

    # ── Filter to flagged rows only ──────────────────────────────────────────
    flagged = data[data["abs_residual"] >= moderate_threshold].copy()
    flagged["severity"] = flagged["abs_residual"].apply(
        lambda x: "high" if x >= high_threshold else "moderate"
    )
    flagged["auto_explanation"] = flagged.apply(_explain, axis=1)

    out_cols = [
        "district", "year", "composite_lean", "expected_d_share",
        "actual_d_share", "residual", "abs_residual", "severity",
        "auto_explanation", "contested",
    ]
    flagged = flagged.rename(columns={"dem_share": "actual_d_share"})
    flagged = flagged[out_cols].sort_values("abs_residual", ascending=False).reset_index(drop=True)

    flagged.to_csv(out_path, index=False, float_format="%.4f")

    # ── Print summary ────────────────────────────────────────────────────────
    total_contested = len(data)
    explanations = ["nominal_candidate", "redistricting_artifact", "possible_open_seat", "unexplained"]

    print("\nANOMALY DETECTION SUMMARY")
    print("=" * 50)
    print(f"Total contested district-years examined: {total_contested}")

    for sev_label, sev_key in [("High severity (|residual| > 15 pts)", "high"),
                                ("Moderate severity (|residual| 10-15 pts)", "moderate")]:
        subset = flagged[flagged["severity"] == sev_key]
        print(f"\n{sev_label}: {len(subset)}")
        for exp in explanations:
            n = (subset["auto_explanation"] == exp).sum()
            if n > 0:
                print(f"  - {exp}: {n}")

    unexplained_high = flagged[
        (flagged["severity"] == "high") & (flagged["auto_explanation"] == "unexplained")
    ]
    if not unexplained_high.empty:
        print(f"\nUNEXPLAINED HIGH-SEVERITY ANOMALIES (require manual inspection):")
        print(f"{'Dist':>4}  {'Year':>4}  {'Lean':>7}  {'Expected':>9}  {'Actual':>7}  {'Residual':>9}")
        for _, row in unexplained_high.iterrows():
            print(
                f"{int(row['district']):>4}  {int(row['year']):>4}  "
                f"{row['composite_lean']:>+7.3f}  "
                f"{row['expected_d_share']*100:>8.1f}%  "
                f"{row['actual_d_share']*100:>6.1f}%  "
                f"{row['residual']*100:>+8.1f}"
            )
    else:
        print(f"\nNo unexplained high-severity anomalies. ✓")

    print(f"\nAnomaly flags written to: {out_path}")
    return flagged
