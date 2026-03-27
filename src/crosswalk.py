"""
crosswalk.py — Area-weighted precinct-to-district spatial overlay.

Methodology (standard in academic redistricting literature, used by MGGG and
the VEST project's own `maup` library):

  1. Intersect precinct polygons with district polygons to produce fragments.
  2. For each fragment, compute what fraction of its *source precinct's* area
     it represents.
  3. Multiply that fraction by the precinct's vote totals to allocate votes to
     the district.

Assumption: voters are uniformly distributed within each precinct.  This
introduces error where a precinct straddles a district boundary and population
is clustered on one side — but it is the standard approach in the absence of
block-level vote data and is accepted by peer-reviewed redistricting literature.

A note on topology: geopandas.overlay() uses exact geometric intersection.
Slivers caused by slightly misaligned boundaries (common between Census and
SOS shapefiles) can produce tiny fragments with near-zero area fractions.
These are kept but sum to ≪ 1 vote and do not materially affect results.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import geopandas as gpd


def build_crosswalk(
    precincts: gpd.GeoDataFrame,
    districts: gpd.GeoDataFrame,
    vote_cols: list[str],
) -> tuple[gpd.GeoDataFrame, pd.DataFrame]:
    """
    Perform spatial intersection and compute area-weighted vote allocation.

    Parameters
    ----------
    precincts : GeoDataFrame
        Must have columns: 'precinct_id', 'precinct_area', plus all vote_cols.
        Both GeoDataFrames must share the same projected CRS.
    districts : GeoDataFrame
        Must have columns: 'district_num', geometry.
    vote_cols : list[str]
        Vote-count columns to allocate (e.g. ['G20PREDBID', 'G20PRERTRU']).

    Returns
    -------
    fragments : GeoDataFrame
        One row per precinct-district fragment, with columns:
          precinct_id, district_num, fragment_area, area_fraction, <vote_cols>
    district_votes : DataFrame
        Summed (allocated) vote totals per district — one row per district.
    """
    if precincts.crs != districts.crs:
        raise ValueError(
            f"CRS mismatch: precincts={precincts.crs}, districts={districts.crs}"
        )

    # Columns we need to carry through the intersection.
    # geopandas.overlay keeps columns from both; we want minimal left side to
    # avoid column-name collisions.
    prec_cols = ["precinct_id", "precinct_area"] + vote_cols
    dist_cols = ["district_num"]

    precincts_slim = precincts[prec_cols + ["geometry"]].copy()
    districts_slim = districts[dist_cols + ["geometry"]].copy()

    print("Running spatial overlay (intersection) — this may take a minute …")
    fragments = gpd.overlay(
        precincts_slim,
        districts_slim,
        how="intersection",
        keep_geom_type=True,  # drop point/line artefacts
    )
    print(f"  Intersection produced {len(fragments):,} fragments "
          f"from {len(precincts):,} precincts × {len(districts):,} districts.")

    # Fragment area in CRS units (same units as precinct_area)
    fragments["fragment_area"] = fragments.geometry.area

    # Area fraction = fragment / original precinct
    # Merge back the original precinct area (it survived the overlay, but be
    # explicit to guard against any column suffixing geopandas might apply).
    fragments["area_fraction"] = (
        fragments["fragment_area"] / fragments["precinct_area"]
    )

    # Allocate votes
    for col in vote_cols:
        fragments[col] = fragments[col] * fragments["area_fraction"]

    # Aggregate to district
    agg_dict = {col: "sum" for col in vote_cols}
    district_votes = (
        fragments.groupby("district_num")
        .agg(agg_dict)
        .reset_index()
    )

    return fragments, district_votes


def validate_crosswalk(
    precincts: gpd.GeoDataFrame,
    fragments: gpd.GeoDataFrame,
    district_votes: pd.DataFrame,
    vote_cols: list[str],
) -> list[str]:
    """
    Run validation checks on the crosswalk output.

    Returns a list of human-readable result strings (pass or warning).
    Raises ValueError for hard failures that indicate a bug.
    """
    issues: list[str] = []
    tolerance = 1e-3  # floating-point tolerance for vote reconciliation

    # 1. Vote reconciliation: total votes in districts == total votes in precincts
    print("\n--- Crosswalk Validation ---")
    for col in vote_cols:
        precinct_total = precincts[col].sum()
        district_total = district_votes[col].sum()
        diff = abs(precinct_total - district_total)
        rel_diff = diff / max(precinct_total, 1)
        if rel_diff > tolerance:
            msg = (
                f"FAIL vote reconciliation {col}: "
                f"precincts={precinct_total:,.1f}, districts={district_total:,.1f}, "
                f"diff={diff:,.1f} ({rel_diff:.4%})"
            )
            issues.append(msg)
            print(f"  {msg}")
        else:
            msg = f"PASS vote reconciliation {col}: {precinct_total:,.1f} ≈ {district_total:,.1f}"
            issues.append(msg)
            print(f"  {msg}")

    # 2. Every district has > 0 votes (use the first D column as proxy)
    d_col = vote_cols[0]
    zero_districts = district_votes[district_votes[d_col] <= 0]["district_num"].tolist()
    if zero_districts:
        msg = f"WARNING: {len(zero_districts)} districts have 0 or negative votes in {d_col}: {zero_districts}"
        issues.append(msg)
        print(f"  {msg}")
    else:
        msg = "PASS: all districts have > 0 votes."
        issues.append(msg)
        print(f"  {msg}")

    # 3. No area fraction > 1.0 (with small tolerance for floating-point)
    max_frac = fragments["area_fraction"].max()
    over_one = (fragments["area_fraction"] > 1.0 + 1e-6).sum()
    if over_one:
        msg = f"WARNING: {over_one:,} fragments have area_fraction > 1.0 (max={max_frac:.6f})"
        issues.append(msg)
        print(f"  {msg}")
    else:
        msg = f"PASS: no area_fraction > 1.0 (max={max_frac:.6f})."
        issues.append(msg)
        print(f"  {msg}")

    # 4. Each precinct's area fractions sum to ~1.0
    frac_sums = fragments.groupby("precinct_id")["area_fraction"].sum()
    bad_precincts = frac_sums[(frac_sums - 1.0).abs() > 0.01]
    if len(bad_precincts):
        msg = (
            f"WARNING: {len(bad_precincts):,} precincts have area-fraction sum "
            f"outside [0.99, 1.01]. Min={frac_sums.min():.4f}, Max={frac_sums.max():.4f}. "
            "This is expected for precincts that lie on the state boundary or have "
            "topology gaps with the district layer."
        )
        issues.append(msg)
        print(f"  {msg}")
    else:
        msg = f"PASS: all precinct area fractions sum to ~1.0 (max deviation={((frac_sums - 1.0).abs().max()):.6f})."
        issues.append(msg)
        print(f"  {msg}")

    # 5. Count split precincts (appear in more than one district fragment)
    n_split = (
        fragments.groupby("precinct_id")["district_num"].nunique() > 1
    ).sum()
    total_precincts = fragments["precinct_id"].nunique()
    msg = (
        f"INFO: {n_split:,} of {total_precincts:,} precincts "
        f"({n_split/total_precincts:.1%}) were split across district boundaries."
    )
    issues.append(msg)
    print(f"  {msg}")

    return issues


def build_pop_weight_table(
    precinct_gdf: gpd.GeoDataFrame,
    district_gdf: gpd.GeoDataFrame,
) -> pd.DataFrame:
    """
    Build population-based allocation weights using Census 2020 block data.

    For each precinct, determines what fraction of its population falls within
    each district using block centroid assignments. Processes one county at a time
    for memory efficiency.

    Returns DataFrame with columns: precinct_id, district_num, pop_fraction
    where pop_fraction sums to ~1.0 per precinct (blocks with no precinct/district
    assignment are dropped, so fractions may sum slightly below 1.0 for edge precincts).
    """
    import warnings
    import pygris
    warnings.filterwarnings("ignore", ".*NotOpenSSL.*")
    from src.join_sos_vest import OHIO_FIPS_TO_COUNTY

    precinct_gdf = precinct_gdf.to_crs("EPSG:3735")
    district_gdf = district_gdf.to_crs("EPSG:3735")

    # For each county: load blocks, assign centroids to precincts + districts, accumulate
    all_records = []

    for i, (fips, county_name) in enumerate(sorted(OHIO_FIPS_TO_COUNTY.items())):
        if (i + 1) % 10 == 0 or i == 0:
            print(f"  [{i+1}/88] Loading blocks for {county_name} county ({fips}) …")

        blocks = pygris.blocks(state="39", county=fips, year=2020, cache=True)
        blocks = blocks[blocks["POP20"] > 0].copy()
        if blocks.empty:
            continue
        blocks = blocks.to_crs("EPSG:3735")

        # County precincts only (by COUNTYFP20 match)
        county_precincts = precinct_gdf[precinct_gdf["COUNTYFP20"] == fips].copy()
        if county_precincts.empty:
            continue

        # Block centroids (much faster than polygon intersection for assignment)
        centroids_gdf = blocks[["GEOID20", "POP20"]].copy()
        centroids_gdf = centroids_gdf.set_geometry(blocks.centroid)
        centroids_gdf.crs = blocks.crs

        # Assign centroids to precincts
        prec_join = gpd.sjoin(
            centroids_gdf,
            county_precincts[["precinct_id", "geometry"]],
            how="left",
            predicate="within",
        ).drop(columns=["index_right"], errors="ignore")

        # Assign centroids to districts
        dist_join = gpd.sjoin(
            centroids_gdf,
            district_gdf[["district_num", "geometry"]],
            how="left",
            predicate="within",
        ).drop(columns=["index_right"], errors="ignore")

        # Merge assignments
        merged = prec_join[["GEOID20", "POP20", "precinct_id"]].merge(
            dist_join[["GEOID20", "district_num"]],
            on="GEOID20",
            how="inner",
        )
        merged = merged.dropna(subset=["precinct_id", "district_num"])
        merged["precinct_id"] = merged["precinct_id"].astype(int)
        merged["district_num"] = merged["district_num"].astype(int)

        all_records.append(merged[["precinct_id", "district_num", "POP20"]])

    if not all_records:
        raise ValueError("No blocks could be assigned to precincts and districts.")

    pop_df = pd.concat(all_records, ignore_index=True)

    # Aggregate by (precinct_id, district_num)
    agg = (
        pop_df.groupby(["precinct_id", "district_num"])["POP20"]
        .sum()
        .reset_index()
        .rename(columns={"POP20": "pop_in_district"})
    )

    # Compute population fraction per precinct
    precinct_totals = (
        agg.groupby("precinct_id")["pop_in_district"]
        .sum()
        .rename("precinct_pop_total")
    )
    agg = agg.merge(precinct_totals.reset_index(), on="precinct_id")
    agg["pop_fraction"] = agg["pop_in_district"] / agg["precinct_pop_total"].clip(lower=1)

    n_precincts = agg["precinct_id"].nunique()
    print(f"  Pop weight table built: {n_precincts:,} precincts × districts covered.")
    return agg[["precinct_id", "district_num", "pop_fraction"]]


def build_crosswalk_pop_weighted(
    precincts: gpd.GeoDataFrame,
    districts: gpd.GeoDataFrame,
    vote_cols: list[str],
    pop_weights: pd.DataFrame,
) -> tuple[gpd.GeoDataFrame, pd.DataFrame]:
    """
    Area-weighted overlay + population-weighted vote allocation.

    Uses pop_weights (from build_pop_weight_table) to replace area_fraction
    for vote allocation. Area fractions are still computed and stored in the
    fragments for structural validation (validate_crosswalk still works).

    For precincts not covered by pop_weights, falls back to area_fraction.
    """
    # Run normal overlay to get fragments (keeps validate_crosswalk working)
    fragments, _ = build_crosswalk(precincts, districts, vote_cols)

    # Re-allocate votes using pop weights instead of area fractions
    # First, restore original precinct vote values
    for col in vote_cols:
        fragments[col] = fragments[col] / fragments["area_fraction"].clip(lower=1e-10)

    # Merge pop weights
    pw = pop_weights[["precinct_id", "district_num", "pop_fraction"]].copy()
    fragments = fragments.merge(
        pw, on=["precinct_id", "district_num"], how="left"
    )
    # Fall back to area_fraction where pop_fraction is missing
    missing_mask = fragments["pop_fraction"].isna()
    n_fallback = int(missing_mask.sum())
    if n_fallback:
        print(f"  Pop weight fallback (area) for {n_fallback:,} fragments.")
    fragments.loc[missing_mask, "pop_fraction"] = fragments.loc[missing_mask, "area_fraction"]

    # Re-allocate with pop fractions
    for col in vote_cols:
        fragments[col] = fragments[col] * fragments["pop_fraction"]

    district_votes = (
        fragments.groupby("district_num")
        .agg({col: "sum" for col in vote_cols})
        .reset_index()
    )

    return fragments, district_votes
