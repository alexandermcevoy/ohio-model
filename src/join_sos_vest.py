"""
join_sos_vest.py — Join Ohio SOS precinct results to VEST 2020 geometry.

Join key: (county_name, precinct_code)

VEST uses COUNTYFP20 (standard county FIPS, e.g. '049' for Franklin) while
SOS uses county name strings (e.g. 'FRANKLIN'). We use the authoritative Ohio
county FIPS table for this mapping; Ohio has 88 counties with stable FIPS codes.

Match rate threshold: 90% by precinct count. If fewer than 90% of a VEST
precincts receive non-zero SOS votes, we stop and report.
"""

from __future__ import annotations

import geopandas as gpd
import pandas as pd

from src.ingest_sos import COUNTY_COL, PREC_CODE_COL, SosFile, get_race_df

MATCH_THRESHOLD = 0.90  # minimum fraction of VEST precincts that must match

# Authoritative Ohio county FIPS (3-digit string) → uppercase county name.
# These are the 88 Ohio counties per the US Census FIPS standard (state FIPS 39).
OHIO_FIPS_TO_COUNTY: dict[str, str] = {
    "001": "ADAMS",      "003": "ALLEN",       "005": "ASHLAND",
    "007": "ASHTABULA",  "009": "ATHENS",      "011": "AUGLAIZE",
    "013": "BELMONT",    "015": "BROWN",       "017": "BUTLER",
    "019": "CARROLL",    "021": "CHAMPAIGN",   "023": "CLARK",
    "025": "CLERMONT",   "027": "CLINTON",     "029": "COLUMBIANA",
    "031": "COSHOCTON",  "033": "CRAWFORD",    "035": "CUYAHOGA",
    "037": "DARKE",      "039": "DEFIANCE",    "041": "DELAWARE",
    "043": "ERIE",       "045": "FAIRFIELD",   "047": "FAYETTE",
    "049": "FRANKLIN",   "051": "FULTON",      "053": "GALLIA",
    "055": "GEAUGA",     "057": "GREENE",      "059": "GUERNSEY",
    "061": "HAMILTON",   "063": "HANCOCK",     "065": "HARDIN",
    "067": "HARRISON",   "069": "HENRY",       "071": "HIGHLAND",
    "073": "HOCKING",    "075": "HOLMES",      "077": "HURON",
    "079": "JACKSON",    "081": "JEFFERSON",   "083": "KNOX",
    "085": "LAKE",       "087": "LAWRENCE",    "089": "LICKING",
    "091": "LOGAN",      "093": "LORAIN",      "095": "LUCAS",
    "097": "MADISON",    "099": "MAHONING",    "101": "MARION",
    "103": "MEDINA",     "105": "MEIGS",       "107": "MERCER",
    "109": "MIAMI",      "111": "MONROE",      "113": "MONTGOMERY",
    "115": "MORGAN",     "117": "MORROW",      "119": "MUSKINGUM",
    "121": "NOBLE",      "123": "OTTAWA",      "125": "PAULDING",
    "127": "PERRY",      "129": "PICKAWAY",    "131": "PIKE",
    "133": "PORTAGE",    "135": "PREBLE",      "137": "PUTNAM",
    "139": "RICHLAND",   "141": "ROSS",        "143": "SANDUSKY",
    "145": "SCIOTO",     "147": "SENECA",      "149": "SHELBY",
    "151": "STARK",      "153": "SUMMIT",      "155": "TRUMBULL",
    "157": "TUSCARAWAS", "159": "UNION",       "161": "VAN WERT",
    "163": "VINTON",     "165": "WARREN",      "167": "WASHINGTON",
    "169": "WAYNE",      "171": "WILLIAMS",    "173": "WOOD",
    "175": "WYANDOT",
}


# ---------------------------------------------------------------------------
# County FIPS → name lookup
# ---------------------------------------------------------------------------

def build_county_lookup(
    vest_gdf: gpd.GeoDataFrame,
    sos_precinct_df: pd.DataFrame,
) -> dict[str, str]:
    """
    Build {COUNTYFP20: county_name_upper} from the Ohio FIPS table.

    Uses OHIO_FIPS_TO_COUNTY for all 88 Ohio counties. The sos_precinct_df
    argument is accepted for API compatibility but not used.
    """
    all_fips = vest_gdf["COUNTYFP20"].unique()
    lookup: dict[str, str] = {}
    unmapped: list[str] = []

    for fips in sorted(all_fips):
        name = OHIO_FIPS_TO_COUNTY.get(str(fips))
        if name is not None:
            lookup[fips] = name
        else:
            unmapped.append(fips)

    if unmapped:
        print(
            f"  WARNING: {len(unmapped)} VEST county FIPS not in Ohio FIPS table: {unmapped}"
        )
    print(f"  County lookup built: {len(lookup)}/88 FIPS mapped.")
    return lookup


# ---------------------------------------------------------------------------
# Join logic
# ---------------------------------------------------------------------------

def join_sos_to_vest(
    vest_gdf: gpd.GeoDataFrame,
    race_df: pd.DataFrame,
    d_col: str,
    r_col: str,
    county_lookup: dict[str, str],
    year: str,
    race_label: str,
) -> gpd.GeoDataFrame:
    """
    Attach SOS vote columns to VEST GDF via (county_name, precinct_code) join.

    Parameters
    ----------
    vest_gdf : GeoDataFrame with COUNTYFP20, PRECINCT20 columns.
    race_df  : DataFrame from get_race_df() with county_name, precinct_code,
               d_votes, r_votes.
    d_col, r_col : Target column names to create in the returned GDF.
    county_lookup : {COUNTYFP20: upper_county_name} from build_county_lookup().
    year, race_label : Used for column naming in output.

    Returns
    -------
    GeoDataFrame — vest_gdf with two new columns: d_col, r_col.
    """
    # Build join key on VEST side
    gdf = vest_gdf.copy()
    gdf["_county_upper"] = gdf["COUNTYFP20"].map(county_lookup)
    gdf["_prec_upper"] = gdf["PRECINCT20"].str.strip().str.upper()

    # Build join key on SOS side
    sos = race_df.copy()
    sos["_county_upper"] = sos[COUNTY_COL].str.strip().str.upper()
    sos["_prec_upper"] = sos[PREC_CODE_COL].str.strip().str.upper()
    sos = sos.rename(columns={"d_votes": d_col, "r_votes": r_col})

    merged = gdf.merge(
        sos[["_county_upper", "_prec_upper", d_col, r_col]],
        on=["_county_upper", "_prec_upper"],
        how="left",
    )
    merged[d_col] = merged[d_col].fillna(0.0)
    merged[r_col] = merged[r_col].fillna(0.0)
    merged = merged.drop(columns=["_county_upper", "_prec_upper"])

    # Compute match rate: fraction of VEST precincts that received non-zero SOS votes.
    # We use the precinct-count rate (not vote-count rate) to avoid confusion from
    # year-to-year boundary changes that can inflate apparent vote totals.
    matched_mask = (merged[d_col] > 0) | (merged[r_col] > 0)
    n_matched = int(matched_mask.sum())
    n_total = len(merged)
    match_rate = n_matched / n_total if n_total > 0 else 0.0

    # Also report how much of SOS total vote is captured (informational only)
    sos_total = race_df["d_votes"].sum() + race_df["r_votes"].sum()
    vest_total = merged[d_col].sum() + merged[r_col].sum()
    vote_capture = vest_total / sos_total if sos_total > 0 else 0.0

    print(
        f"  {year} {race_label}: matched {n_matched:,}/{n_total:,} VEST precincts "
        f"({match_rate:.1%}) | SOS vote capture = {vote_capture:.1%}"
    )

    if match_rate < MATCH_THRESHOLD:
        raise ValueError(
            f"Match rate {match_rate:.1%} is below the {MATCH_THRESHOLD:.0%} threshold "
            f"for {year} {race_label}. Investigate precinct identifier discrepancies "
            f"before proceeding."
        )

    return merged


# ---------------------------------------------------------------------------
# Cross-check: VEST 2020 presidential vs SOS 2020 presidential
# ---------------------------------------------------------------------------

def crosscheck_vest_sos_2020(
    vest_gdf: gpd.GeoDataFrame,
    sos_2020: SosFile,
    county_lookup: dict[str, str],
) -> None:
    """
    Compare VEST 2020 precinct-level presidential votes to SOS 2020.
    Prints a summary. Does not raise on discrepancy — just informs.
    """
    print("\n--- Cross-check: VEST 2020 presidential vs SOS 2020 ---")
    if "pre" not in sos_2020.statewide:
        print("  SOS 2020 has no presidential race — skipping cross-check.")
        return

    race_df = get_race_df(sos_2020, "pre")

    vest_biden = vest_gdf["G20PREDBID"].sum()
    vest_trump = vest_gdf["G20PRERTRU"].sum()
    sos_biden = race_df["d_votes"].sum()
    sos_trump = race_df["r_votes"].sum()

    print(
        f"  Biden  — VEST: {vest_biden:>10,.0f}  SOS: {sos_biden:>10,.0f}  "
        f"diff: {abs(vest_biden - sos_biden):,.0f}"
    )
    print(
        f"  Trump  — VEST: {vest_trump:>10,.0f}  SOS: {sos_trump:>10,.0f}  "
        f"diff: {abs(vest_trump - sos_trump):,.0f}"
    )

    vest_d_share = vest_biden / (vest_biden + vest_trump)
    sos_d_share = sos_biden / (sos_biden + sos_trump)
    print(
        f"  D 2-party share — VEST: {vest_d_share:.4f}  SOS: {sos_d_share:.4f}  "
        f"delta: {abs(vest_d_share - sos_d_share):.4f}"
    )
