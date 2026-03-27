"""
backbone.py — Census Block Backbone Architecture.

Makes Census 2020 blocks the canonical storage unit for election data.
Precinct (or county) votes are disaggregated to blocks proportional to
population, then reaggregated to any district map instantly.

This eliminates redistricting contamination structurally and enables
historical depth from 2010–2024 on a single canonical geometry.

Key data stores (all parquet in data/processed/):
  block_geometry.parquet       — GEOID20, POP20, centroid coords (~220k rows)
  block_county_map.parquet     — GEOID20 → county_fips
  block_precinct_map_{year}    — GEOID20 → precinct_id + pop_fraction
  block_district_map_{plan}    — GEOID20 → district_num
  block_votes_{year}           — GEOID20 × race → d_votes, r_votes
"""

from __future__ import annotations

import warnings
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

from src.ingest import TARGET_CRS

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parent.parent
PROCESSED_DIR = _REPO_ROOT / "data" / "processed"

BLOCK_GEOMETRY_PATH = PROCESSED_DIR / "block_geometry.parquet"
BLOCK_COUNTY_MAP_PATH = PROCESSED_DIR / "block_county_map.parquet"


def _block_precinct_map_path(year: str) -> Path:
    return PROCESSED_DIR / f"block_precinct_map_{year}.parquet"


def _block_district_map_path(plan: str) -> Path:
    return PROCESSED_DIR / f"block_district_map_{plan}.parquet"


def _block_votes_path(year: str) -> Path:
    return PROCESSED_DIR / f"block_votes_{year}.parquet"


# =========================================================================
# Phase 1 — Block Geometry Cache + County Map
# =========================================================================

def build_block_geometry_cache(force: bool = False) -> pd.DataFrame:
    """
    Load Census 2020 blocks for all 88 Ohio counties via pygris.

    Stores GEOID20, POP20, centroid_x, centroid_y (EPSG:3735).
    Filters to POP20 > 0 (populated blocks only).
    Caches to block_geometry.parquet.

    Returns DataFrame with columns: block_geoid, pop, centroid_x, centroid_y
    """
    if BLOCK_GEOMETRY_PATH.exists() and not force:
        print(f"  Block geometry cache exists at {BLOCK_GEOMETRY_PATH}")
        return pd.read_parquet(BLOCK_GEOMETRY_PATH)

    import pygris
    warnings.filterwarnings("ignore", ".*NotOpenSSL.*")
    from src.join_sos_vest import OHIO_FIPS_TO_COUNTY

    print("Building block geometry cache from Census 2020 blocks …")
    all_blocks = []

    for i, (fips, county_name) in enumerate(sorted(OHIO_FIPS_TO_COUNTY.items())):
        if (i + 1) % 10 == 0 or i == 0:
            print(f"  [{i+1}/88] Loading blocks for {county_name} county ({fips}) …")

        blocks = pygris.blocks(state="39", county=fips, year=2020, cache=True)
        blocks = blocks[blocks["POP20"] > 0].copy()
        if blocks.empty:
            continue

        blocks = blocks.to_crs(TARGET_CRS)

        # Compute centroids in projected CRS
        centroids = blocks.geometry.centroid
        chunk = pd.DataFrame({
            "block_geoid": blocks["GEOID20"].values,
            "pop": blocks["POP20"].astype(int).values,
            "centroid_x": centroids.x.values,
            "centroid_y": centroids.y.values,
        })
        all_blocks.append(chunk)

    if not all_blocks:
        raise ValueError("No populated blocks found across 88 Ohio counties.")

    result = pd.concat(all_blocks, ignore_index=True)

    # Validate
    n_blocks = len(result)
    n_counties = result["block_geoid"].str[2:5].nunique()
    total_pop = result["pop"].sum()
    print(f"\n  Block geometry cache built:")
    print(f"    {n_blocks:,} populated blocks across {n_counties} counties")
    print(f"    Total population: {total_pop:,}")

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    result.to_parquet(BLOCK_GEOMETRY_PATH, index=False)
    print(f"    Saved to {BLOCK_GEOMETRY_PATH} ({BLOCK_GEOMETRY_PATH.stat().st_size / 1e6:.1f} MB)")

    return result


def build_block_county_map(blocks: pd.DataFrame | None = None, force: bool = False) -> pd.DataFrame:
    """
    Extract county FIPS from GEOID20.

    Census 2020 block GEOID format: SSCCCTTTTTTBBBB
      SS    = state FIPS (39 for Ohio)
      CCC   = county FIPS (001–175)
      TTTTTT = tract
      BBBB  = block

    Returns DataFrame with columns: block_geoid, county_fips
    """
    if BLOCK_COUNTY_MAP_PATH.exists() and not force:
        print(f"  Block county map exists at {BLOCK_COUNTY_MAP_PATH}")
        return pd.read_parquet(BLOCK_COUNTY_MAP_PATH)

    if blocks is None:
        blocks = load_block_geometry()

    print("Building block → county map …")
    result = pd.DataFrame({
        "block_geoid": blocks["block_geoid"],
        "county_fips": blocks["block_geoid"].str[2:5],
    })

    n_counties = result["county_fips"].nunique()
    print(f"  {len(result):,} blocks mapped to {n_counties} counties")

    result.to_parquet(BLOCK_COUNTY_MAP_PATH, index=False)
    print(f"  Saved to {BLOCK_COUNTY_MAP_PATH}")

    return result


# =========================================================================
# Phase 2 — Block-to-Precinct Maps
# =========================================================================

def build_block_precinct_map(
    precinct_gdf: gpd.GeoDataFrame,
    year: str,
    blocks: pd.DataFrame | None = None,
    force: bool = False,
) -> pd.DataFrame:
    """
    Centroid-assign blocks to precincts for a VEST year.

    Returns DataFrame with columns: block_geoid, precinct_id, pop_fraction
    where pop_fraction = block_pop / precinct_total_pop.

    One map per VEST year (2016, 2018, 2020).
    """
    out_path = _block_precinct_map_path(year)
    if out_path.exists() and not force:
        print(f"  Block-precinct map for {year} exists at {out_path}")
        return pd.read_parquet(out_path)

    if blocks is None:
        blocks = load_block_geometry()

    print(f"Building block → precinct map for VEST {year} …")

    precinct_gdf = precinct_gdf.to_crs(TARGET_CRS)

    # Build GeoDataFrame of block centroids
    centroids_gdf = gpd.GeoDataFrame(
        blocks[["block_geoid", "pop"]],
        geometry=gpd.points_from_xy(blocks["centroid_x"], blocks["centroid_y"]),
        crs=TARGET_CRS,
    )

    # Spatial join: assign each block centroid to its containing precinct
    joined = gpd.sjoin(
        centroids_gdf,
        precinct_gdf[["precinct_id", "geometry"]],
        how="left",
        predicate="within",
    ).drop(columns=["index_right"], errors="ignore")

    # Drop blocks that didn't land in any precinct (edge cases)
    assigned = joined.dropna(subset=["precinct_id"]).copy()
    assigned["precinct_id"] = assigned["precinct_id"].astype(int)
    n_dropped = len(joined) - len(assigned)
    if n_dropped > 0:
        print(f"  Warning: {n_dropped:,} blocks not assigned to any precinct (edge/boundary)")

    # Compute pop_fraction = block_pop / precinct_total_pop
    precinct_totals = (
        assigned.groupby("precinct_id")["pop"]
        .sum()
        .rename("precinct_pop_total")
    )
    assigned = assigned.merge(precinct_totals.reset_index(), on="precinct_id")
    assigned["pop_fraction"] = assigned["pop"] / assigned["precinct_pop_total"].clip(lower=1)

    result = assigned[["block_geoid", "precinct_id", "pop_fraction"]].copy()

    # Validate
    n_precincts = result["precinct_id"].nunique()
    frac_check = result.groupby("precinct_id")["pop_fraction"].sum()
    bad_fracs = (frac_check - 1.0).abs() > 0.01
    print(f"  Assigned {len(result):,} blocks to {n_precincts:,} precincts")
    if bad_fracs.any():
        print(f"  Warning: {bad_fracs.sum()} precincts have pop_fraction sum > 1% off from 1.0")

    result.to_parquet(out_path, index=False)
    print(f"  Saved to {out_path}")
    return result


# =========================================================================
# Phase 3 — Block-to-District Map
# =========================================================================

def build_block_district_map(
    district_gdf: gpd.GeoDataFrame,
    plan: str = "2024",
    blocks: pd.DataFrame | None = None,
    force: bool = False,
) -> pd.DataFrame:
    """
    Centroid-assign blocks to districts.

    Returns DataFrame with columns: block_geoid, district_num
    One map per district plan (currently only 2024 final map).
    """
    out_path = _block_district_map_path(plan)
    if out_path.exists() and not force:
        print(f"  Block-district map for plan {plan} exists at {out_path}")
        return pd.read_parquet(out_path)

    if blocks is None:
        blocks = load_block_geometry()

    print(f"Building block → district map for plan {plan} …")

    district_gdf = district_gdf.to_crs(TARGET_CRS)

    # Build GeoDataFrame of block centroids
    centroids_gdf = gpd.GeoDataFrame(
        blocks[["block_geoid"]],
        geometry=gpd.points_from_xy(blocks["centroid_x"], blocks["centroid_y"]),
        crs=TARGET_CRS,
    )

    joined = gpd.sjoin(
        centroids_gdf,
        district_gdf[["district_num", "geometry"]],
        how="left",
        predicate="within",
    ).drop(columns=["index_right"], errors="ignore")

    assigned = joined.dropna(subset=["district_num"]).copy()
    assigned["district_num"] = assigned["district_num"].astype(int)
    n_dropped = len(joined) - len(assigned)
    if n_dropped > 0:
        print(f"  Warning: {n_dropped:,} blocks not assigned to any district (edge/boundary)")

    result = assigned[["block_geoid", "district_num"]].copy()

    # Validate
    n_districts = result["district_num"].nunique()
    print(f"  Assigned {len(result):,} blocks to {n_districts} districts")
    if n_districts != 99:
        print(f"  WARNING: Expected 99 districts, got {n_districts}")

    result.to_parquet(out_path, index=False)
    print(f"  Saved to {out_path}")
    return result


# =========================================================================
# Phase 5 — Disaggregation (precinct votes → block votes)
# =========================================================================

def disaggregate_precinct_votes(
    precinct_votes: pd.DataFrame,
    block_precinct_map: pd.DataFrame,
    vote_col_pairs: list[tuple[str, str]],
    year: str,
    force: bool = False,
) -> pd.DataFrame:
    """
    Allocate precinct-level votes to blocks proportional to population.

    Parameters
    ----------
    precinct_votes : DataFrame
        One row per precinct. Must have 'precinct_id' and vote columns.
    block_precinct_map : DataFrame
        From build_block_precinct_map(). Columns: block_geoid, precinct_id, pop_fraction.
    vote_col_pairs : list of (d_col, r_col) tuples
        Each pair is (dem_vote_column, rep_vote_column) in precinct_votes.
        The race name is derived from the column name.
    year : str
        Election year label for output file.

    Returns
    -------
    DataFrame in long format: block_geoid, race, d_votes, r_votes
    """
    out_path = _block_votes_path(year)
    if out_path.exists() and not force:
        print(f"  Block vote surface for {year} exists at {out_path}")
        return pd.read_parquet(out_path)

    print(f"Disaggregating precinct votes to blocks for {year} …")

    # Merge block map with precinct votes
    merged = block_precinct_map.merge(precinct_votes, on="precinct_id", how="left")

    records = []
    for d_col, r_col in vote_col_pairs:
        # Derive race label from column name
        race = _race_label_from_vest_col(d_col, year)

        block_d = merged["pop_fraction"] * merged[d_col].fillna(0)
        block_r = merged["pop_fraction"] * merged[r_col].fillna(0)

        chunk = pd.DataFrame({
            "block_geoid": merged["block_geoid"],
            "race": race,
            "d_votes": block_d,
            "r_votes": block_r,
        })
        records.append(chunk)

    result = pd.concat(records, ignore_index=True)

    # Validation: total votes should match precinct totals
    for d_col, r_col in vote_col_pairs:
        race = _race_label_from_vest_col(d_col, year)
        orig_d = precinct_votes[d_col].sum()
        block_d = result[result["race"] == race]["d_votes"].sum()
        diff_pct = abs(block_d - orig_d) / max(orig_d, 1) * 100
        if diff_pct > 0.1:
            print(f"  WARNING: {race} D votes differ by {diff_pct:.2f}% (orig={orig_d:.0f}, blocks={block_d:.0f})")

    n_races = result["race"].nunique()
    print(f"  Block vote surface: {len(result):,} rows, {n_races} races")

    result.to_parquet(out_path, index=False)
    print(f"  Saved to {out_path}")
    return result


def _race_label_from_vest_col(col: str, year: str) -> str:
    """Convert VEST column name to race label, e.g. 'G20PREDBID' → 'pre_2020'.

    Maps VEST race codes to the labels used by the composite pipeline:
      PRE → pre, USS → uss, GOV → gov, ATG → atg, AUD → aud, SOS → sos_off, TRE → tre
    """
    from src.ingest import VEST_RACE_PATTERN

    # VEST race code → composite label
    _VEST_TO_COMPOSITE = {
        "pre": "pre",
        "uss": "uss",
        "gov": "gov",
        "atg": "atg",
        "aud": "aud",
        "sos": "sos_off",  # VEST uses SOS, composite uses sos_off
        "tre": "tre",
    }

    m = VEST_RACE_PATTERN.match(col.upper())
    if m:
        race_code = m.group(2).lower()
        label = _VEST_TO_COMPOSITE.get(race_code, race_code)
        return f"{label}_{year}"
    # Fallback: use column name directly
    return f"{col}_{year}"


# =========================================================================
# Phase 5b — SOS precinct → VEST precinct → block disaggregation
# =========================================================================

def disaggregate_sos_via_vest(
    sos_path: str | Path,
    vest_gdf: gpd.GeoDataFrame,
    block_precinct_map: pd.DataFrame,
    year: str,
    force: bool = False,
) -> pd.DataFrame:
    """
    For SOS years without their own VEST geometry (2022, 2024), join SOS
    precinct data to VEST 2020 precincts and disaggregate to blocks.

    This gives precinct-level precision (not county-level), matching the
    existing composite pipeline's approach.

    Parameters
    ----------
    sos_path : path to SOS XLSX file
    vest_gdf : VEST 2020 GeoDataFrame (with precinct_id)
    block_precinct_map : from build_block_precinct_map("2020")
    year : election year label
    """
    out_path = _block_votes_path(year)
    if out_path.exists() and not force:
        print(f"  Block vote surface for {year} exists at {out_path}")
        return pd.read_parquet(out_path)

    from src.ingest_sos import load_sos_file, get_race_df, COUNTY_COL, PREC_CODE_COL
    from src.join_sos_vest import join_sos_to_vest, build_county_lookup

    print(f"Disaggregating SOS {year} via VEST 2020 precincts to blocks …")

    sos = load_sos_file(sos_path)
    county_lookup = build_county_lookup(vest_gdf, sos.precinct_statewide)

    # Join each statewide race to VEST precincts
    vote_col_pairs: list[tuple[str, str]] = []
    gdf = vest_gdf.copy()

    for label, spec in sos.statewide.items():
        if not spec.has_contest():
            continue

        race_label = f"{label}_{year}"
        d_col = f"{race_label}_d"
        r_col = f"{race_label}_r"

        race_df = get_race_df(sos, label)
        gdf = join_sos_to_vest(
            gdf, race_df, d_col, r_col, county_lookup, year, label
        )
        vote_col_pairs.append((d_col, r_col))

    # Now disaggregate VEST precinct votes to blocks
    precinct_votes = gdf[["precinct_id"] + [c for pair in vote_col_pairs for c in pair]].copy()

    # Use the standard disaggregation with a custom race label extractor
    merged = block_precinct_map.merge(precinct_votes, on="precinct_id", how="left")

    records = []
    for d_col, r_col in vote_col_pairs:
        # Race label is embedded in column name: "{label}_{year}_d" → "{label}_{year}"
        race = d_col.rsplit("_", 1)[0]  # strip "_d"

        block_d = merged["pop_fraction"] * merged[d_col].fillna(0)
        block_r = merged["pop_fraction"] * merged[r_col].fillna(0)

        chunk = pd.DataFrame({
            "block_geoid": merged["block_geoid"],
            "race": race,
            "d_votes": block_d,
            "r_votes": block_r,
        })
        records.append(chunk)

    result = pd.concat(records, ignore_index=True)

    # Validation
    for d_col, r_col in vote_col_pairs:
        race = d_col.rsplit("_", 1)[0]
        orig_d = precinct_votes[d_col].sum()
        block_d = result[result["race"] == race]["d_votes"].sum()
        diff_pct = abs(block_d - orig_d) / max(orig_d, 1) * 100
        if diff_pct > 0.5:
            print(f"  WARNING: {race} D votes differ by {diff_pct:.2f}%")

    n_races = result["race"].nunique()
    print(f"  Block vote surface: {len(result):,} rows, {n_races} races")

    result.to_parquet(out_path, index=False)
    print(f"  Saved to {out_path}")
    return result


# =========================================================================
# Phase 6 — Disaggregation (county votes → block votes)
# =========================================================================

def disaggregate_county_votes(
    county_votes: pd.DataFrame,
    block_county_map: pd.DataFrame,
    block_pop: pd.DataFrame,
    race_cols: list[tuple[str, str, str]],
    year: str,
    force: bool = False,
) -> pd.DataFrame:
    """
    Allocate county-level votes to blocks proportional to population.

    Parameters
    ----------
    county_votes : DataFrame
        One row per county. Must have 'county_fips' and vote columns.
    block_county_map : DataFrame
        From build_block_county_map(). Columns: block_geoid, county_fips.
    block_pop : DataFrame
        Block population. Columns: block_geoid, pop.
    race_cols : list of (race_label, d_col, r_col) tuples
        Each entry is (race_name, dem_vote_column, rep_vote_column).
    year : str
        Election year label for output file.

    Returns
    -------
    DataFrame in long format: block_geoid, race, d_votes, r_votes
    """
    out_path = _block_votes_path(year)
    if out_path.exists() and not force:
        print(f"  Block vote surface for {year} exists at {out_path}")
        return pd.read_parquet(out_path)

    print(f"Disaggregating county votes to blocks for {year} …")

    # Merge blocks with county and population
    blocks_with_county = block_county_map.merge(
        block_pop[["block_geoid", "pop"]], on="block_geoid"
    )

    # Compute county population totals
    county_pop_totals = (
        blocks_with_county.groupby("county_fips")["pop"]
        .sum()
        .rename("county_pop_total")
    )
    blocks_with_county = blocks_with_county.merge(
        county_pop_totals.reset_index(), on="county_fips"
    )
    blocks_with_county["county_pop_fraction"] = (
        blocks_with_county["pop"] / blocks_with_county["county_pop_total"].clip(lower=1)
    )

    # Merge with county votes
    merged = blocks_with_county.merge(county_votes, on="county_fips", how="left")

    records = []
    for race_label, d_col, r_col in race_cols:
        block_d = merged["county_pop_fraction"] * merged[d_col].fillna(0)
        block_r = merged["county_pop_fraction"] * merged[r_col].fillna(0)

        chunk = pd.DataFrame({
            "block_geoid": merged["block_geoid"],
            "race": race_label,
            "d_votes": block_d,
            "r_votes": block_r,
        })
        records.append(chunk)

    result = pd.concat(records, ignore_index=True)

    # Validation
    for race_label, d_col, r_col in race_cols:
        orig_d = county_votes[d_col].sum()
        block_d = result[result["race"] == race_label]["d_votes"].sum()
        diff_pct = abs(block_d - orig_d) / max(orig_d, 1) * 100
        if diff_pct > 0.1:
            print(f"  WARNING: {race_label} D votes differ by {diff_pct:.2f}%")

    n_races = result["race"].nunique()
    print(f"  Block vote surface: {len(result):,} rows, {n_races} races")

    result.to_parquet(out_path, index=False)
    print(f"  Saved to {out_path}")
    return result


# =========================================================================
# Reaggregation (block votes → district totals)
# =========================================================================

def reaggregate_to_districts(
    block_votes: pd.DataFrame,
    block_district_map: pd.DataFrame,
) -> pd.DataFrame:
    """
    Aggregate block votes to district totals.

    Parameters
    ----------
    block_votes : DataFrame
        Long format: block_geoid, race, d_votes, r_votes
    block_district_map : DataFrame
        Columns: block_geoid, district_num

    Returns
    -------
    DataFrame: district_num, race, d_votes, r_votes, dem_share
    """
    merged = block_votes.merge(block_district_map, on="block_geoid", how="inner")

    agg = (
        merged.groupby(["district_num", "race"])[["d_votes", "r_votes"]]
        .sum()
        .reset_index()
    )

    two_party = agg["d_votes"] + agg["r_votes"]
    agg["dem_share"] = agg["d_votes"] / two_party.clip(lower=1)

    return agg


def compute_lean_from_blocks(
    block_votes: pd.DataFrame,
    block_district_map: pd.DataFrame,
) -> pd.DataFrame:
    """
    Full pipeline: reaggregate → compute two-party D share → lean.

    Lean = district_dem_share − statewide_dem_share for each race.

    Returns DataFrame: district_num, race, d_votes, r_votes, dem_share, lean
    """
    district_results = reaggregate_to_districts(block_votes, block_district_map)

    # Compute statewide totals per race
    statewide = (
        district_results.groupby("race")[["d_votes", "r_votes"]]
        .sum()
        .reset_index()
    )
    statewide["statewide_dem_share"] = (
        statewide["d_votes"] / (statewide["d_votes"] + statewide["r_votes"]).clip(lower=1)
    )

    district_results = district_results.merge(
        statewide[["race", "statewide_dem_share"]], on="race"
    )
    district_results["lean"] = district_results["dem_share"] - district_results["statewide_dem_share"]

    return district_results


# =========================================================================
# Trend analysis
# =========================================================================

DISTRICT_TRENDS_PATH = PROCESSED_DIR / "oh_house_district_trends.csv"

# All 8 backbone years
ALL_BACKBONE_YEARS = ["2010", "2012", "2014", "2016", "2018", "2020", "2022", "2024"]

# Stability threshold: |slope| < this → "stable"
_STABLE_THRESHOLD = 0.0005  # 0.05 pts/year ≈ 0.4 pts over 8 years


def compute_district_trends(
    block_district_map: pd.DataFrame,
    years: list[str] | None = None,
) -> pd.DataFrame:
    """
    Compute per-district partisan trend from block vote surfaces.

    For each district and each year, averages the lean across all available
    statewide races to get one "year lean" observation. Then fits OLS per
    district: year_lean ~ year. The slope measures how much the district is
    trending D (positive) or R (negative) *relative to the state* per year.

    Parameters
    ----------
    block_district_map : DataFrame with block_geoid, district_num
    years : list of year strings to include (default: all 8 backbone years)

    Returns
    -------
    DataFrame with columns:
        district       — district number (1–99)
        trend_slope    — lean change per year (positive = trending D)
        trend_r2       — R² of the linear fit (high = consistent trend)
        trend_shift    — total lean change from first to last year (slope × span)
        trend_dir      — "trending_d" / "trending_r" / "stable"
        n_years        — number of years with data
        lean_earliest  — average lean in earliest year
        lean_latest    — average lean in latest year
    """
    if years is None:
        years = ALL_BACKBONE_YEARS

    print("Computing district-level partisan trends …")

    # Collect per-district, per-year average lean
    records = []
    for year in years:
        vote_path = _block_votes_path(year)
        if not vote_path.exists():
            print(f"  Warning: no block vote surface for {year}, skipping")
            continue

        block_votes = pd.read_parquet(vote_path)
        lean_df = compute_lean_from_blocks(block_votes, block_district_map)

        # Average lean across all races for this year → one value per district
        year_avg = (
            lean_df.groupby("district_num")["lean"]
            .mean()
            .reset_index()
        )
        year_avg["year"] = int(year)
        records.append(year_avg)

    if not records:
        raise ValueError("No block vote surfaces found for trend analysis.")

    panel = pd.concat(records, ignore_index=True)
    print(f"  Panel: {len(panel)} district-year observations "
          f"({panel['district_num'].nunique()} districts × "
          f"{panel['year'].nunique()} years)")

    # Fit OLS per district: lean ~ year
    results = []
    for district, group in panel.groupby("district_num"):
        group = group.sort_values("year")
        n_years = len(group)

        if n_years < 3:
            # Not enough data for a meaningful trend
            results.append({
                "district": int(district),
                "trend_slope": float("nan"),
                "trend_r2": float("nan"),
                "trend_shift": float("nan"),
                "trend_dir": "insufficient_data",
                "n_years": n_years,
                "lean_earliest": group["lean"].iloc[0],
                "lean_latest": group["lean"].iloc[-1],
            })
            continue

        x = group["year"].values.astype(float)
        y = group["lean"].values.astype(float)

        # OLS: y = a + b*x
        x_mean = x.mean()
        y_mean = y.mean()
        ss_xy = ((x - x_mean) * (y - y_mean)).sum()
        ss_xx = ((x - x_mean) ** 2).sum()
        slope = ss_xy / ss_xx if ss_xx > 0 else 0.0

        # R²
        y_hat = y_mean + slope * (x - x_mean)
        ss_res = ((y - y_hat) ** 2).sum()
        ss_tot = ((y - y_mean) ** 2).sum()
        r2 = 1.0 - (ss_res / ss_tot) if ss_tot > 0 else 0.0

        year_span = x[-1] - x[0]
        total_shift = slope * year_span

        if slope > _STABLE_THRESHOLD:
            direction = "trending_d"
        elif slope < -_STABLE_THRESHOLD:
            direction = "trending_r"
        else:
            direction = "stable"

        results.append({
            "district": int(district),
            "trend_slope": round(slope, 6),
            "trend_r2": round(r2, 4),
            "trend_shift": round(total_shift, 4),
            "trend_dir": direction,
            "n_years": n_years,
            "lean_earliest": round(group["lean"].iloc[0], 4),
            "lean_latest": round(group["lean"].iloc[-1], 4),
        })

    trend_df = pd.DataFrame(results)

    # Summary
    n_d = (trend_df["trend_dir"] == "trending_d").sum()
    n_r = (trend_df["trend_dir"] == "trending_r").sum()
    n_s = (trend_df["trend_dir"] == "stable").sum()
    print(f"  Trends: {n_d} trending D, {n_r} trending R, {n_s} stable")

    # Top movers
    trending_d = trend_df[trend_df["trend_dir"] == "trending_d"].nlargest(5, "trend_slope")
    trending_r = trend_df[trend_df["trend_dir"] == "trending_r"].nsmallest(5, "trend_slope")

    if not trending_d.empty:
        print("  Fastest D-trending districts:")
        for _, row in trending_d.iterrows():
            print(f"    District {row['district']:3.0f}: {row['trend_slope']:+.4f}/yr "
                  f"(shift {row['trend_shift']:+.3f}, R²={row['trend_r2']:.2f})")

    if not trending_r.empty:
        print("  Fastest R-trending districts:")
        for _, row in trending_r.iterrows():
            print(f"    District {row['district']:3.0f}: {row['trend_slope']:+.4f}/yr "
                  f"(shift {row['trend_shift']:+.3f}, R²={row['trend_r2']:.2f})")

    return trend_df


# =========================================================================
# Composite integration
# =========================================================================

def build_composite_from_blocks(
    years: list[str],
    block_district_map: pd.DataFrame,
    weights: dict[tuple[str, str], float] | None = None,
) -> pd.DataFrame:
    """
    End-to-end: load block vote surfaces → district leans → weighted composite.

    Calls compute_lean_from_blocks() for each year, then feeds into
    composite.build_composite() for the weighted average.

    Parameters
    ----------
    years : list of year strings to include
    block_district_map : DataFrame from build_block_district_map()
    weights : optional custom weights (same format as composite.DEFAULT_WEIGHTS)

    Returns
    -------
    DataFrame from composite.build_composite() — one row per district with
    composite_lean and per-race lean columns.
    """
    from src.composite import build_composite

    # Load all vote surfaces and compute leans
    district_leans: dict[tuple[str, str], pd.Series] = {}

    for year in years:
        vote_path = _block_votes_path(year)
        if not vote_path.exists():
            print(f"  Warning: no block vote surface for {year}, skipping")
            continue

        block_votes = pd.read_parquet(vote_path)
        lean_df = compute_lean_from_blocks(block_votes, block_district_map)

        # Convert to the dict format expected by build_composite:
        # {(year, race_code): Series(district_num → lean)}
        for race_label in lean_df["race"].unique():
            race_data = lean_df[lean_df["race"] == race_label]
            # Parse race label: "pre_2020" → ("2020", "pre")
            parts = race_label.rsplit("_", 1)
            if len(parts) == 2:
                race_code, race_year = parts[0], parts[1]
            else:
                race_code, race_year = race_label, year

            lean_series = race_data.set_index("district_num")["lean"]
            district_leans[(race_year, race_code)] = lean_series

    if not district_leans:
        raise ValueError("No block vote surfaces found for any requested year.")

    # Compute statewide_avg for historical years (2010, 2014) that have
    # ATG/AUD/SOS_OFF/TRE races. The existing composite only computes
    # statewide_avg for 2018 and 2022; we extend it here.
    _STATEWIDE_AVG_RACES = ["atg", "aud", "sos_off", "tre"]
    for year in years:
        if (year, "statewide_avg") not in district_leans:
            available_races = [
                district_leans[(year, r)]
                for r in _STATEWIDE_AVG_RACES
                if (year, r) in district_leans
            ]
            if len(available_races) >= 2:  # need at least 2 races for a meaningful average
                stacked = pd.concat(available_races, axis=1)
                district_leans[(year, "statewide_avg")] = stacked.mean(axis=1)
                race_names = [r for r in _STATEWIDE_AVG_RACES if (year, r) in district_leans]
                print(f"  Computed statewide_avg for {year}: mean of {race_names}")

    print(f"  Building composite from {len(district_leans)} race-year combinations …")
    composite_df = build_composite(district_leans, weights=weights)
    return composite_df


# =========================================================================
# Loaders (for cached parquets)
# =========================================================================

def load_block_geometry() -> pd.DataFrame:
    """Load cached block geometry. Raises if not built yet."""
    if not BLOCK_GEOMETRY_PATH.exists():
        raise FileNotFoundError(
            f"Block geometry cache not found at {BLOCK_GEOMETRY_PATH}. "
            "Run `python cli.py backbone --build-geometry` first."
        )
    return pd.read_parquet(BLOCK_GEOMETRY_PATH)


def load_block_county_map() -> pd.DataFrame:
    """Load cached block → county map."""
    if not BLOCK_COUNTY_MAP_PATH.exists():
        raise FileNotFoundError(
            f"Block county map not found at {BLOCK_COUNTY_MAP_PATH}. "
            "Run `python cli.py backbone --build-geometry` first."
        )
    return pd.read_parquet(BLOCK_COUNTY_MAP_PATH)


def load_block_precinct_map(year: str) -> pd.DataFrame:
    """Load cached block → precinct map for a VEST year."""
    path = _block_precinct_map_path(year)
    if not path.exists():
        raise FileNotFoundError(
            f"Block-precinct map for {year} not found at {path}. "
            "Run `python cli.py backbone --build-maps` first."
        )
    return pd.read_parquet(path)


def load_block_district_map(plan: str = "2024") -> pd.DataFrame:
    """Load cached block → district map."""
    path = _block_district_map_path(plan)
    if not path.exists():
        raise FileNotFoundError(
            f"Block-district map for plan {plan} not found at {path}. "
            "Run `python cli.py backbone --build-maps` first."
        )
    return pd.read_parquet(path)


def load_block_votes(year: str) -> pd.DataFrame:
    """Load cached block vote surface for an election year."""
    path = _block_votes_path(year)
    if not path.exists():
        raise FileNotFoundError(
            f"Block vote surface for {year} not found at {path}. "
            "Run `python cli.py backbone --build-surfaces` first."
        )
    return pd.read_parquet(path)
