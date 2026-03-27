"""
ingest.py — Load and inspect VEST precinct and Census SLDL district shapefiles.

Reprojects both layers to EPSG:3735 (Ohio State Plane South, feet) for area
calculations. We prefer a State Plane projection over UTM because it minimises
distortion within Ohio and is the standard used by the Ohio Secretary of State.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import geopandas as gpd
import pandas as pd

# Target CRS for all area calculations — Ohio State Plane South (NAD83), US feet.
# Alternatively use EPSG:32617 (UTM zone 17N, metres) — both are fine; we pick
# 3735 to stay consistent with Ohio government GIS workflows.
TARGET_CRS = "EPSG:3735"

# VEST column-name patterns (uppercase after normalisation).
# Pattern groups: G{YY}{RACE}{PARTY}{CANDIDATE_SUFFIX}
# e.g. G20PREDBID = 2020 General, PRESident, Democrat, BIDen
VEST_RACE_PATTERN = re.compile(
    r"^G(\d{2})"        # election year (2-digit)
    r"([A-Z]{3})"       # race code (PRE, USS, GOV, ATG, …)
    r"([DR]|[OT])"      # party: D, R, O(ther), T(otal)
    r"([A-Z]+)$"        # candidate/suffix
)


def _detect_vest_cols(gdf: gpd.GeoDataFrame) -> dict[str, list[str]]:
    """
    Scan column names for VEST vote-count columns.

    Returns a dict mapping race_code -> list of matching column names.
    Only includes races where we find at least one D column and one R column
    (minimum needed for partisan lean).
    """
    by_race: dict[str, list[str]] = {}
    for col in gdf.columns:
        m = VEST_RACE_PATTERN.match(col.upper())
        if m:
            race = m.group(2)
            by_race.setdefault(race, []).append(col)

    # Keep only races with both D and R candidates
    contested = {
        race: cols
        for race, cols in by_race.items()
        if any(VEST_RACE_PATTERN.match(c.upper()).group(3) == "D" for c in cols)
        and any(VEST_RACE_PATTERN.match(c.upper()).group(3) == "R" for c in cols)
    }
    return contested


def load_precincts(path: str | Path) -> gpd.GeoDataFrame:
    """
    Load VEST 2020 Ohio precinct shapefile and reproject to TARGET_CRS.

    Returns GeoDataFrame with:
      - All original columns preserved
      - 'precinct_area_m2': area in CRS units (sq feet for EPSG:3735)
      - 'precinct_id': stable integer index
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Precinct shapefile not found: {path}")

    print(f"Loading precincts from {path} …")
    gdf = gpd.read_file(path)
    print(f"  Raw CRS: {gdf.crs}")
    print(f"  Raw shape: {gdf.shape}")

    # Reproject
    gdf = gdf.to_crs(TARGET_CRS)
    gdf["precinct_area"] = gdf.geometry.area  # sq-ft in EPSG:3735
    gdf["precinct_id"] = range(len(gdf))

    # Surface VEST columns
    vest_races = _detect_vest_cols(gdf)
    print(f"\n  Detected {len(vest_races)} contested statewide race(s) in VEST data:")
    for race, cols in sorted(vest_races.items()):
        d_cols = [c for c in cols if VEST_RACE_PATTERN.match(c.upper()).group(3) == "D"]
        r_cols = [c for c in cols if VEST_RACE_PATTERN.match(c.upper()).group(3) == "R"]
        print(f"    {race}: D={d_cols}  R={r_cols}")

    # Statewide vote totals sanity check
    print("\n  Statewide vote totals (sum across all precincts):")
    for race, cols in sorted(vest_races.items()):
        for col in cols:
            total = gdf[col].sum()
            print(f"    {col}: {total:,.0f}")

    print(f"\n  Precincts loaded: {len(gdf):,}")
    print(f"  CRS after reproject: {gdf.crs}\n")
    return gdf


def load_districts(path: str | Path) -> gpd.GeoDataFrame:
    """
    Load Ohio House district shapefile and reproject.

    Supports both:
      - Census TIGER/Line SLDL format (column: SLDLST)
      - Ohio SOS redistricting plan format (column: DISTRICT)

    Normalises to 'district_num' (integer) in either case.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"District shapefile not found: {path}")

    print(f"Loading districts from {path} …")
    gdf = gpd.read_file(path)
    print(f"  Raw CRS: {gdf.crs}")
    print(f"  Raw shape: {gdf.shape}")
    print(f"  Columns: {list(gdf.columns)}")

    gdf = gdf.to_crs(TARGET_CRS)

    # Confirm we have the expected 99 Ohio House districts
    n = len(gdf)
    if n != 99:
        print(f"  WARNING: Expected 99 districts, got {n}. Verify this is the SLDL layer.")

    # Detect district number column — support Census TIGER/Line and Ohio SOS formats
    if "SLDLST" in gdf.columns:
        gdf["district_num"] = gdf["SLDLST"].astype(int)
    elif "DISTRICT" in gdf.columns:
        gdf["district_num"] = gdf["DISTRICT"].astype(int)
    else:
        available = [c for c in gdf.columns if c != "geometry"]
        raise KeyError(
            f"No district number column found (tried SLDLST, DISTRICT). "
            f"Available columns: {available}"
        )

    print(f"\n  Districts loaded: {len(gdf):,}")
    print(f"  District range: {gdf['district_num'].min()} – {gdf['district_num'].max()}")
    print(f"  CRS after reproject: {gdf.crs}\n")
    return gdf


def get_vest_races(precincts: gpd.GeoDataFrame) -> dict[str, list[str]]:
    """Public helper: return the detected VEST race -> column mapping."""
    return _detect_vest_cols(precincts)
