"""
ingest_house_results.py — Aggregate actual Ohio State House race results by district.

Each precinct votes in exactly one House district, so no spatial crosswalk is
needed: summing precinct rows within each district's vote columns gives district
totals directly.

Important caveat: the 2018 and 2022 house races were conducted under different
district maps than the current 2023 SOS plan. District numbers overlap but
boundaries differ. This module reports results as-filed by district number;
callers should interpret 2018/2022 results with this caveat in mind.

Uncontested races (only D or only R candidate) are flagged explicitly.
They cannot be used for partisan lean analysis but are useful as metadata
(candidate filing patterns, organizational strength).
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.ingest_sos import COUNTY_COL, PREC_CODE_COL, SosFile


def parse_house_results(sos: SosFile) -> pd.DataFrame:
    """
    Aggregate SOS precinct-level house data to the district level.

    Returns DataFrame with columns:
      year, district, dem_candidate_cols, rep_candidate_cols,
      dem_votes, rep_votes, total_two_party, dem_share, margin,
      winner (D/R/uncontested), contested (bool)
    """
    year = sos.year
    house_map = sos.house  # {district_num: RaceSpec}
    df = sos.precinct_house

    if df.empty or not house_map:
        print(f"  No house data found for {year}.")
        return pd.DataFrame()

    records = []
    for dist_num in sorted(house_map.keys()):
        spec = house_map[dist_num]

        d_vote_cols = [c for c in spec.d_cols if c in df.columns]
        r_vote_cols = [c for c in spec.r_cols if c in df.columns]

        d_votes = df[d_vote_cols].sum(axis=1).sum() if d_vote_cols else 0.0
        r_votes = df[r_vote_cols].sum(axis=1).sum() if r_vote_cols else 0.0

        two_party = d_votes + r_votes
        contested = bool(d_vote_cols and r_vote_cols)

        if two_party > 0 and contested:
            dem_share = d_votes / two_party
            margin = (d_votes - r_votes) / two_party
            if dem_share > 0.5:
                winner = "D"
            elif dem_share < 0.5:
                winner = "R"
            else:
                winner = "tie"
        elif d_vote_cols and not r_vote_cols:
            dem_share = 1.0
            margin = 1.0
            winner = "D_uncontested"
        elif r_vote_cols and not d_vote_cols:
            dem_share = 0.0
            margin = -1.0
            winner = "R_uncontested"
        else:
            dem_share = float("nan")
            margin = float("nan")
            winner = "no_data"

        records.append(
            {
                "year": year,
                "district": dist_num,
                "dem_votes": round(d_votes),
                "rep_votes": round(r_votes),
                "total_two_party": round(two_party),
                "dem_share": dem_share,
                "margin": margin,
                "winner": winner,
                "contested": contested,
            }
        )

    result = pd.DataFrame(records)

    # Summary
    n_total = len(result)
    n_contested = result["contested"].sum()
    n_d = (result["winner"] == "D").sum()
    n_r = (result["winner"] == "R").sum()
    n_d_unc = (result["winner"] == "D_uncontested").sum()
    n_r_unc = (result["winner"] == "R_uncontested").sum()

    print(f"\n  {year} House results — {n_total} districts:")
    print(f"    Contested: {n_contested}  (D won: {n_d}, R won: {n_r})")
    print(f"    Uncontested: D={n_d_unc}, R={n_r_unc}")
    if n_contested > 0:
        avg_d_share = result.loc[result["contested"], "dem_share"].mean()
        print(f"    Mean D share (contested only): {avg_d_share:.3f}")

    return result


def combine_house_results(results: list[pd.DataFrame]) -> pd.DataFrame:
    """Combine results from multiple years into a wide format per district."""
    if not results:
        return pd.DataFrame()

    combined = pd.concat([r for r in results if not r.empty], ignore_index=True)

    # Pivot to wide format: one row per district, columns per year
    metrics = ["dem_votes", "rep_votes", "dem_share", "margin", "winner", "contested"]
    frames = []

    for year in sorted(combined["year"].unique()):
        yr_df = combined[combined["year"] == year].copy()
        yr_df = yr_df.set_index("district")[metrics]
        yr_df.columns = [f"{col}_{year}" for col in yr_df.columns]
        frames.append(yr_df)

    if not frames:
        return pd.DataFrame()

    wide = frames[0]
    for f in frames[1:]:
        wide = wide.join(f, how="outer")

    wide = wide.reset_index().rename(columns={"index": "district"})
    wide = wide.sort_values("district").reset_index(drop=True)
    return wide


# ---------------------------------------------------------------------------
# Candidate name extraction
# ---------------------------------------------------------------------------

def extract_candidate_names(sos: SosFile) -> pd.DataFrame:
    """
    Extract D and R candidate names for each district from a SosFile.

    Returns DataFrame with columns:
      district, dem_candidate_{year}, rep_candidate_{year}

    Candidate names come from the XLSX row-1 header (e.g. "Jane Doe (D)").
    Only the first D and first R candidate per district are recorded;
    primary runoffs with multiple candidates of the same party are rare in
    Ohio general elections.
    """
    year = sos.year
    house_map = sos.house

    records = []
    for dist_num in sorted(house_map.keys()):
        spec = house_map[dist_num]
        dem_name = spec.d_candidate_names[0] if spec.d_candidate_names else None
        rep_name = spec.r_candidate_names[0] if spec.r_candidate_names else None
        records.append(
            {
                "district": dist_num,
                f"dem_candidate_{year}": dem_name,
                f"rep_candidate_{year}": rep_name,
            }
        )

    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# Redistricting contamination filter
# ---------------------------------------------------------------------------

_PRE_REDISTRICTING_YEARS = [2018, 2020]

_WIDE_COLS_TO_NULL = [
    "dem_votes", "rep_votes", "dem_share", "margin",
    "winner", "candidate_effect",
]


def apply_redistricting_filter(
    house_long: pd.DataFrame,
    overlap_df: pd.DataFrame,
) -> tuple[pd.DataFrame, dict]:
    """
    Drop contaminated house race rows from the long-format house results DataFrame.

    Two filters applied:
      1. Old→interim: 'relocated' or 'redrawn' districts lose 2018 and 2020 rows.
      2. Interim→final: 'relocated' districts (Jaccard < 0.30 between 2022 and 2024
         maps) also lose their 2022 row.  Only 'relocated' (not 'redrawn') triggers
         the 2022 drop — a redrawn district retains partial precinct overlap and its
         2022 result is considered usable.

    The second filter is applied only when 'overlap_category_interim_final' is
    present in overlap_df (i.e., after running the updated redistricting check).

    Returns (filtered_house_long, summary_dict).
    """
    # Filter 1: old→interim (2018/2020 for relocated/redrawn)
    contaminated_oi = overlap_df[
        overlap_df["overlap_category"].isin(["relocated", "redrawn"])
    ]["district"].tolist()

    drop_mask = (
        house_long["district"].isin(contaminated_oi)
        & house_long["year"].isin(_PRE_REDISTRICTING_YEARS)
    )

    # Filter 2: interim→final (2022 for relocated only)
    contaminated_if: list[int] = []
    if "overlap_category_interim_final" in overlap_df.columns:
        contaminated_if = overlap_df[
            overlap_df["overlap_category_interim_final"] == "relocated"
        ]["district"].tolist()
        drop_mask_2022 = (
            house_long["district"].isin(contaminated_if)
            & (house_long["year"] == 2022)
        )
        drop_mask = drop_mask | drop_mask_2022

    n_dropped = int(drop_mask.sum())
    filtered = house_long[~drop_mask].copy()

    relocated_n = int((overlap_df["overlap_category"] == "relocated").sum())
    redrawn_n = int((overlap_df["overlap_category"] == "redrawn").sum())
    relocated_if_n = len(contaminated_if)

    summary = {
        "n_obs_before": len(house_long),
        "n_obs_after": len(filtered),
        "n_dropped": n_dropped,
        "n_contaminated_districts": len(contaminated_oi),
        "n_relocated": relocated_n,
        "n_redrawn": redrawn_n,
        "n_interim_final_relocated": relocated_if_n,
        "contaminated_districts": sorted(contaminated_oi),
    }

    print(f"\n  Redistricting filter applied to house_long:")
    print(f"    Old→interim relocated (Jaccard < 0.30): {relocated_n} — drops 2018/2020")
    print(f"    Old→interim redrawn  (0.30–0.70):       {redrawn_n} — drops 2018/2020")
    if relocated_if_n:
        print(f"    Interim→final relocated (Jaccard < 0.30): {relocated_if_n} — also drops 2022")
    print(f"    Total observations dropped:             {n_dropped}")
    print(f"    Remaining observations:                 {len(filtered)}  (was {len(house_long)})")

    return filtered, summary


def apply_redistricting_filter_to_composite(
    composite_wide: pd.DataFrame,
    overlap_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    NaN out contaminated house result columns in the wide composite DataFrame.

    Two passes mirroring apply_redistricting_filter:
      1. 2018/2020 columns zeroed for old→interim relocated/redrawn districts.
      2. 2022 columns zeroed for interim→final relocated districts (when
         'overlap_category_interim_final' is present in overlap_df).

    Statewide race lean columns (e.g. gov_2018_lean) are NOT touched.
    """
    contaminated_oi = overlap_df[
        overlap_df["overlap_category"].isin(["relocated", "redrawn"])
    ]["district"].tolist()

    df = composite_wide.copy()
    mask_oi = df["district"].isin(contaminated_oi)

    for year in _PRE_REDISTRICTING_YEARS:
        for metric in _WIDE_COLS_TO_NULL:
            col = f"{metric}_{year}"
            if col in df.columns:
                df.loc[mask_oi, col] = float("nan")
        contested_col = f"contested_{year}"
        if contested_col in df.columns:
            df.loc[mask_oi, contested_col] = False

    n_if_cleared = 0
    if "overlap_category_interim_final" in overlap_df.columns:
        contaminated_if = overlap_df[
            overlap_df["overlap_category_interim_final"] == "relocated"
        ]["district"].tolist()
        mask_if = df["district"].isin(contaminated_if)
        for metric in _WIDE_COLS_TO_NULL:
            col = f"{metric}_2022"
            if col in df.columns:
                df.loc[mask_if, col] = float("nan")
        if "contested_2022" in df.columns:
            df.loc[mask_if, "contested_2022"] = False
        n_if_cleared = len(contaminated_if)

    print(f"\n  Composite wide NaN-out:")
    print(f"    Cleared 2018/2020 columns for {len(contaminated_oi)} old→interim contaminated districts.")
    if n_if_cleared:
        print(f"    Cleared 2022 columns for {n_if_cleared} interim→final relocated districts.")
    return df
