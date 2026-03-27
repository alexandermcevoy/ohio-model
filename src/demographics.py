"""
demographics.py — ACS demographic data pull and district-level aggregation.

Pulls ACS 2023 5-year estimates at block-group level for all 88 Ohio counties,
performs spatial overlay onto House districts, and aggregates to district-level
demographic metrics.

Methodology:
  - Area-fractional allocation for count variables (assuming uniform distribution
    within block groups — standard for ACS BG data).
  - Population-weighted mean for median income and median age (can't sum medians).
  - Pop density computed from EPSG:3735 district geometry area (feet → sqmi).
"""

from __future__ import annotations

import warnings
import os
import math

import numpy as np
import pandas as pd
import geopandas as gpd
import requests
from dotenv import load_dotenv

warnings.filterwarnings("ignore", ".*NotOpenSSL.*")

load_dotenv()

ACS_YEAR = 2023

# All 14 ACS variables to pull
ACS_VARIABLES = [
    "B01003_001E",  # total population
    "B03002_001E",  # race/ethnicity denominator
    "B03002_003E",  # white alone, not Hispanic
    "B03002_004E",  # Black alone
    "B03002_012E",  # Hispanic/Latino
    "B15003_001E",  # educational attainment denominator (25+)
    "B15003_022E",  # bachelor's degree
    "B15003_023E",  # master's degree
    "B15003_024E",  # professional degree
    "B15003_025E",  # doctorate degree
    "B19013_001E",  # median household income (NOT a count — weighted separately)
    "B25003_001E",  # housing tenure denominator
    "B25003_002E",  # owner-occupied units
    "B01002_001E",  # median age (NOT a count — weighted separately)
]

# Variables that are medians (cannot be area-allocated as counts)
MEDIAN_VARS = {"B19013_001E", "B01002_001E"}

# Count variables eligible for area-fractional allocation
COUNT_VARS = [v for v in ACS_VARIABLES if v not in MEDIAN_VARS]

CENSUS_SUPPRESSION = -666666666

ACS_BASE_URL = "https://api.census.gov/data/{year}/acs/acs5"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def pull_acs_blockgroups(year: int = ACS_YEAR) -> pd.DataFrame:
    """Pull ACS variables for all Ohio BGs. Returns raw DataFrame with GEOID."""
    api_key = os.getenv("CENSUS_API_KEY")
    if not api_key:
        raise EnvironmentError(
            "CENSUS_API_KEY not found. Set it in your .env file."
        )

    from src.join_sos_vest import OHIO_FIPS_TO_COUNTY

    url = ACS_BASE_URL.format(year=year)
    var_str = ",".join(["NAME"] + ACS_VARIABLES)

    all_records: list[pd.DataFrame] = []
    fips_list = sorted(OHIO_FIPS_TO_COUNTY.keys())

    print(f"Pulling ACS {year} block-group data for {len(fips_list)} Ohio counties …")

    for i, fips in enumerate(fips_list):
        county_name = OHIO_FIPS_TO_COUNTY[fips]
        if (i + 1) % 10 == 0 or i == 0:
            print(f"  [{i+1}/{len(fips_list)}] {county_name} county ({fips}) …")

        params = {
            "get": var_str,
            "for": "block group:*",
            "in": f"state:39 county:{fips}",
            "key": api_key,
        }
        resp = requests.get(url, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        headers = data[0]
        rows = data[1:]
        df = pd.DataFrame(rows, columns=headers)

        # Build GEOID
        df["GEOID"] = (
            df["state"].str.zfill(2)
            + df["county"].str.zfill(3)
            + df["tract"].str.zfill(6)
            + df["block group"].str.zfill(1)
        )

        # Cast ACS columns to numeric, replace suppression sentinel with NaN
        for col in ACS_VARIABLES:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df.loc[df[col] == CENSUS_SUPPRESSION, col] = np.nan

        all_records.append(df[["GEOID"] + ACS_VARIABLES])

    raw_df = pd.concat(all_records, ignore_index=True)
    print(f"  Pulled {len(raw_df):,} block groups total.")
    return raw_df


def load_blockgroup_geometry(year: int = ACS_YEAR) -> gpd.GeoDataFrame:
    """Load BG geometries for all Ohio counties via pygris. Returns GDF with GEOID column."""
    import pygris
    from src.join_sos_vest import OHIO_FIPS_TO_COUNTY

    fips_list = sorted(OHIO_FIPS_TO_COUNTY.keys())
    print(f"Loading block-group geometries for {len(fips_list)} counties …")

    all_gdfs: list[gpd.GeoDataFrame] = []

    for i, fips in enumerate(fips_list):
        county_name = OHIO_FIPS_TO_COUNTY[fips]
        if (i + 1) % 10 == 0 or i == 0:
            print(f"  [{i+1}/{len(fips_list)}] {county_name} county ({fips}) …")

        bg = pygris.block_groups(state="39", county=fips, year=year, cache=True)
        all_gdfs.append(bg)

    bg_gdf = pd.concat(all_gdfs, ignore_index=True)
    bg_gdf = gpd.GeoDataFrame(bg_gdf, geometry="geometry")

    # Build GEOID to match ACS data
    bg_gdf["GEOID"] = (
        bg_gdf["STATEFP"].str.zfill(2)
        + bg_gdf["COUNTYFP"].str.zfill(3)
        + bg_gdf["TRACTCE"].str.zfill(6)
        + bg_gdf["BLKGRPCE"].str.zfill(1)
    )

    bg_gdf = bg_gdf.to_crs("EPSG:3735")
    print(f"  Loaded {len(bg_gdf):,} block groups.")
    return bg_gdf


def build_district_demographics(
    district_gdf: gpd.GeoDataFrame,
    year: int = ACS_YEAR,
    cache_path: str | None = None,
) -> pd.DataFrame:
    """
    Full pipeline: pull ACS → load geometry → overlay → aggregate to districts.
    If cache_path is set and the file exists, load from cache instead of re-pulling.
    Returns DataFrame indexed by district_num with all computed metrics.
    Raises ValueError at checkpoints if validation fails hard.
    """
    # --- Step 1: Get raw ACS data (from cache or API) ---
    raw_acs: pd.DataFrame
    if cache_path and os.path.exists(cache_path):
        print(f"Loading cached ACS data from {cache_path} …")
        raw_acs = pd.read_parquet(cache_path)
    else:
        raw_acs = pull_acs_blockgroups(year=year)
        if cache_path:
            os.makedirs(os.path.dirname(cache_path), exist_ok=True)
            raw_acs.to_parquet(cache_path, index=False)
            print(f"Saved ACS cache to {cache_path}")

    # Validation checkpoint 1: total Ohio population
    total_pop = raw_acs["B01003_001E"].sum()
    print(f"\n  [Checkpoint 1] Total Ohio BG population: {total_pop:,.0f}")
    if not (11_000_000 <= total_pop <= 12_500_000):
        raise ValueError(
            f"Total population {total_pop:,.0f} is outside expected 11–12.5M range. "
            "Check ACS pull."
        )

    # --- Step 2: Load block-group geometry ---
    bg_gdf = load_blockgroup_geometry(year=year)

    # --- Step 3: Join ACS data onto geometry ---
    bg_gdf = bg_gdf.merge(raw_acs, on="GEOID", how="left")

    # Record original BG area (in EPSG:3735 sq ft)
    bg_gdf["bg_area"] = bg_gdf.geometry.area

    # --- Step 4: Overlay BGs onto districts ---
    district_gdf_proj = district_gdf.to_crs("EPSG:3735")

    # Keep only essential columns for overlay
    dist_slim = district_gdf_proj[["district_num", "geometry"]].copy()
    bg_slim = bg_gdf[["GEOID", "bg_area"] + ACS_VARIABLES + ["geometry"]].copy()

    print("Running BG → district overlay (intersection) …")
    fragments = gpd.overlay(
        bg_slim,
        dist_slim,
        how="intersection",
        keep_geom_type=False,
    )
    print(f"  Overlay produced {len(fragments):,} fragments.")

    # --- Step 5: Compute area fractions ---
    fragments["fragment_area"] = fragments.geometry.area
    fragments["area_frac"] = fragments["fragment_area"] / fragments["bg_area"].clip(lower=1)

    # --- Step 6: Allocate count variables ---
    alloc_cols = []
    for col in COUNT_VARS:
        alloc_col = f"alloc_{col}"
        fragments[alloc_col] = fragments["area_frac"] * fragments[col].fillna(0)
        alloc_cols.append(alloc_col)

    # --- Step 7: Weighted allocation for median vars ---
    # Only include fragments where the median var is not NaN
    fragments["alloc_pop_weight"] = fragments["area_frac"] * fragments["B01003_001E"].fillna(0)

    # Income weighting
    income_mask = fragments["B19013_001E"].notna()
    fragments["alloc_income_wt"] = np.where(
        income_mask,
        fragments["alloc_pop_weight"] * fragments["B19013_001E"],
        np.nan,
    )

    # Age weighting
    age_mask = fragments["B01002_001E"].notna()
    fragments["alloc_age_wt"] = np.where(
        age_mask,
        fragments["alloc_pop_weight"] * fragments["B01002_001E"],
        np.nan,
    )

    # --- Step 8: Aggregate to districts ---
    agg_dict = {col: "sum" for col in alloc_cols}
    agg_dict["alloc_pop_weight"] = "sum"
    agg_dict["alloc_income_wt"] = "sum"
    agg_dict["alloc_age_wt"] = "sum"

    district_df = (
        fragments.groupby("district_num")
        .agg(agg_dict)
        .reset_index()
    )

    # --- Step 9: Compute district-level metrics ---
    district_df["total_pop"] = district_df["alloc_B01003_001E"]

    def _safe_div(num: pd.Series, den: pd.Series) -> pd.Series:
        return num / den.replace(0, np.nan)

    district_df["white_pct"] = _safe_div(
        district_df["alloc_B03002_003E"],
        district_df["alloc_B03002_001E"],
    )
    district_df["black_pct"] = _safe_div(
        district_df["alloc_B03002_004E"],
        district_df["alloc_B03002_001E"],
    )
    district_df["hispanic_pct"] = _safe_div(
        district_df["alloc_B03002_012E"],
        district_df["alloc_B03002_001E"],
    )
    district_df["college_pct"] = _safe_div(
        district_df["alloc_B15003_022E"]
        + district_df["alloc_B15003_023E"]
        + district_df["alloc_B15003_024E"]
        + district_df["alloc_B15003_025E"],
        district_df["alloc_B15003_001E"],
    )
    district_df["owner_occ_pct"] = _safe_div(
        district_df["alloc_B25003_002E"],
        district_df["alloc_B25003_001E"],
    )

    # Weighted medians
    district_df["median_income"] = _safe_div(
        district_df["alloc_income_wt"],
        district_df["alloc_pop_weight"],
    )
    district_df["median_age"] = _safe_div(
        district_df["alloc_age_wt"],
        district_df["alloc_pop_weight"],
    )

    # --- Step 10: Pop density ---
    # EPSG:3735 is in feet; 1 sqft = 1/27878400 sqmi
    sqft_per_sqmi = 27_878_400.0
    dist_areas = district_gdf_proj[["district_num"]].copy()
    dist_areas["district_land_area_sqmi"] = district_gdf_proj.geometry.area / sqft_per_sqmi
    district_df = district_df.merge(dist_areas, on="district_num", how="left")
    district_df["pop_density"] = _safe_div(
        district_df["total_pop"],
        district_df["district_land_area_sqmi"],
    )

    district_df = district_df.set_index("district_num")

    # --- Validation checkpoint 2: population deviation ---
    mean_pop = district_df["total_pop"].mean()
    deviant = district_df[
        (district_df["total_pop"] - mean_pop).abs() / mean_pop > 0.15
    ]["total_pop"]
    print(f"\n  [Checkpoint 2] Mean district population: {mean_pop:,.0f}")
    if len(deviant):
        print(
            f"  WARNING: {len(deviant)} districts deviate >15% from mean population "
            f"(possible redistricting/geometry issue):"
        )
        for d, p in deviant.items():
            pct = (p - mean_pop) / mean_pop * 100
            print(f"    District {d}: {p:,.0f} ({pct:+.1f}%)")
    else:
        print("  All districts within ±15% of mean population.")

    # --- Validation checkpoint 3: statewide averages ---
    pop_w = district_df["total_pop"]
    sw_college = (district_df["college_pct"] * pop_w).sum() / pop_w.sum()
    sw_income = (district_df["median_income"] * pop_w).sum() / pop_w.sum()
    sw_white = (district_df["white_pct"] * pop_w).sum() / pop_w.sum()
    print(f"\n  [Checkpoint 3] Statewide averages (pop-weighted):")
    print(f"    college_pct:    {sw_college:.3f}")
    print(f"    median_income:  ${sw_income:,.0f}")
    print(f"    white_pct:      {sw_white:.3f}")

    # --- Validation checkpoint 4: top/bottom 5 by college_pct and median_income ---
    print(f"\n  [Checkpoint 4] Top/bottom 5 districts by college_pct:")
    cp_sorted = district_df["college_pct"].sort_values(ascending=False)
    print("    Top 5:")
    for d, v in cp_sorted.head(5).items():
        print(f"      District {d}: {v:.3f}")
    print("    Bottom 5:")
    for d, v in cp_sorted.tail(5).items():
        print(f"      District {d}: {v:.3f}")

    print(f"\n  [Checkpoint 4] Top/bottom 5 districts by median_income:")
    mi_sorted = district_df["median_income"].sort_values(ascending=False)
    print("    Top 5:")
    for d, v in mi_sorted.head(5).items():
        print(f"      District {d}: ${v:,.0f}")
    print("    Bottom 5:")
    for d, v in mi_sorted.tail(5).items():
        print(f"      District {d}: ${v:,.0f}")

    return district_df
