"""
partisan.py — Partisan lean computation.

Partisan lean is defined here as:
  lean = district_two_party_D_share - statewide_two_party_D_share

A positive value means the district is more Democratic than the state average;
negative means more Republican.  We express this as a signed float (e.g. +0.052
means D+5.2 relative to state).

We compute lean for every contested statewide race detected in the VEST data,
not just the presidential race.  The presidential race is the canonical
benchmark; additional races will be used for a composite index in a later
session.

Two-party share excludes third-party/other/write-in votes from the denominator.
This matches standard political science practice (VEST, DRA, Dave's
Redistricting App all use two-party presidential share as the primary lean
metric).
"""

from __future__ import annotations

import re

import pandas as pd

# Same pattern as ingest.py — repeated here to keep modules self-contained
VEST_RACE_PATTERN = re.compile(
    r"^G(\d{2})([A-Z]{3})([DR]|[OT])([A-Z]+)$"
)


def _party(col: str) -> str:
    """Return 'D', 'R', or 'O' for a VEST column name."""
    m = VEST_RACE_PATTERN.match(col.upper())
    if m:
        return m.group(3)
    return "O"


def _race(col: str) -> str:
    m = VEST_RACE_PATTERN.match(col.upper())
    if m:
        return m.group(2)
    return "UNK"


def compute_lean(
    district_votes: pd.DataFrame,
    vote_cols: list[str],
    statewide_totals: dict[str, float],
) -> pd.DataFrame:
    """
    Compute partisan lean for each district and each race.

    Parameters
    ----------
    district_votes : DataFrame
        One row per district; must include 'district_num' and all vote_cols.
    vote_cols : list[str]
        All VEST vote columns present (D, R, and optionally O/T columns).
    statewide_totals : dict[str, float]
        Precinct-level statewide sum for each vote column.
        Used to compute the statewide two-party benchmark.

    Returns
    -------
    DataFrame with one row per district and computed lean columns.
    """
    df = district_votes.copy()

    # Group columns by race
    races: dict[str, dict[str, list[str]]] = {}
    for col in vote_cols:
        m = VEST_RACE_PATTERN.match(col.upper())
        if not m:
            continue
        r = m.group(2)
        p = m.group(3)
        races.setdefault(r, {"D": [], "R": [], "O": []})
        races[r][p].append(col)

    results: list[pd.DataFrame] = [df[["district_num"]].copy()]

    for race, party_cols in sorted(races.items()):
        d_cols = party_cols["D"]
        r_cols = party_cols["R"]

        if not d_cols or not r_cols:
            print(f"  Skipping {race}: missing D or R columns.")
            continue

        # Sum across multiple candidates of same party (rare in statewide races
        # but possible in primaries or where VEST splits by candidate)
        df[f"{race}_d_votes"] = df[d_cols].sum(axis=1)
        df[f"{race}_r_votes"] = df[r_cols].sum(axis=1)
        df[f"{race}_two_party"] = df[f"{race}_d_votes"] + df[f"{race}_r_votes"]

        # District two-party D share (guard against zero-vote districts)
        df[f"{race}_dem_share"] = df[f"{race}_d_votes"] / df[f"{race}_two_party"].replace(0, pd.NA)

        # Raw margin: (D - R) / two_party
        df[f"{race}_raw_margin"] = (
            (df[f"{race}_d_votes"] - df[f"{race}_r_votes"])
            / df[f"{race}_two_party"].replace(0, pd.NA)
        )

        # Statewide benchmark
        sw_d = sum(statewide_totals.get(c, 0.0) for c in d_cols)
        sw_r = sum(statewide_totals.get(c, 0.0) for c in r_cols)
        sw_two_party = sw_d + sw_r
        sw_dem_share = sw_d / sw_two_party if sw_two_party > 0 else float("nan")

        df[f"{race}_lean"] = df[f"{race}_dem_share"] - sw_dem_share

        race_label = _race_label(race)
        print(
            f"  {race_label} statewide two-party D share: {sw_dem_share:.4f} "
            f"({sw_dem_share*100:.2f}%) — "
            f"D={sw_d:,.0f} / R={sw_r:,.0f} / 2P-total={sw_two_party:,.0f}"
        )

    return df


def _race_label(race_code: str) -> str:
    labels = {
        "PRE": "President",
        "USS": "US Senate",
        "GOV": "Governor",
        "ATG": "Attorney General",
        "SOS": "Secretary of State",
        "TRE": "Treasurer",
        "AUD": "Auditor",
        "LTG": "Lt. Governor",
    }
    return labels.get(race_code, race_code)


def build_output(df: pd.DataFrame, primary_race: str = "PRE") -> pd.DataFrame:
    """
    Build the final output DataFrame sorted by partisan lean (most D to most R).

    Uses the presidential race as the primary lean column. Falls back to the
    first available race if PRE is not present.
    """
    lean_col = f"{primary_race}_lean"
    if lean_col not in df.columns:
        available = [c for c in df.columns if c.endswith("_lean")]
        if not available:
            raise ValueError("No lean columns found. Was compute_lean() called?")
        lean_col = available[0]
        race_used = lean_col.replace("_lean", "")
        print(f"  NOTE: PRE not found; sorting by {_race_label(race_used)} lean.")

    # Canonical output columns
    output_cols = ["district_num"]

    # Presidential race columns first (if available)
    for suffix in ["d_votes", "r_votes", "two_party", "dem_share", "raw_margin", "lean"]:
        col = f"{primary_race}_{suffix}"
        if col in df.columns:
            output_cols.append(col)

    # All other race lean/share columns
    for col in df.columns:
        if col not in output_cols and col != "district_num":
            output_cols.append(col)

    # Rename PRE columns to friendlier names for the primary output CSV
    rename = {}
    if f"{primary_race}_d_votes" in df.columns:
        rename[f"{primary_race}_d_votes"] = "biden_votes"
        rename[f"{primary_race}_r_votes"] = "trump_votes"
        rename[f"{primary_race}_two_party"] = "total_two_party"
        rename[f"{primary_race}_dem_share"] = "dem_two_party_share"
        rename[f"{primary_race}_raw_margin"] = "raw_margin"
        rename[f"{primary_race}_lean"] = "partisan_lean"

    out = (
        df[output_cols]
        .rename(columns={**rename, "district_num": "district"})
        .sort_values("partisan_lean" if "partisan_lean" in rename.values() else lean_col,
                     ascending=False)
        .reset_index(drop=True)
    )
    return out


def validate_statewide_result(
    statewide_totals: dict[str, float],
    vote_cols: list[str],
    known_dem_share: float = 0.459,
    tolerance: float = 0.005,
) -> list[str]:
    """
    Check computed statewide two-party D share against the known 2020 Ohio
    presidential result (~45.9% D two-party, i.e. 53.3% Trump / 45.2% Biden
    out of the full vote; two-party share adjusts for minor-party votes).

    Returns list of validation message strings.
    """
    issues = []

    # Find PRE D and R columns
    d_cols = [c for c in vote_cols if VEST_RACE_PATTERN.match(c.upper())
               and VEST_RACE_PATTERN.match(c.upper()).group(2) == "PRE"
               and VEST_RACE_PATTERN.match(c.upper()).group(3) == "D"]
    r_cols = [c for c in vote_cols if VEST_RACE_PATTERN.match(c.upper())
               and VEST_RACE_PATTERN.match(c.upper()).group(2) == "PRE"
               and VEST_RACE_PATTERN.match(c.upper()).group(3) == "R"]

    if not d_cols or not r_cols:
        msg = "WARNING: Cannot find PRE D/R columns to validate statewide result."
        issues.append(msg)
        print(f"  {msg}")
        return issues

    sw_d = sum(statewide_totals.get(c, 0.0) for c in d_cols)
    sw_r = sum(statewide_totals.get(c, 0.0) for c in r_cols)
    sw_two_party = sw_d + sw_r
    computed_share = sw_d / sw_two_party if sw_two_party > 0 else float("nan")
    diff = abs(computed_share - known_dem_share)

    if diff > tolerance:
        msg = (
            f"FLAG statewide result: computed D two-party share = {computed_share:.4f} "
            f"({computed_share*100:.2f}%), expected ~{known_dem_share*100:.1f}%, "
            f"difference = {diff*100:.2f} points (threshold {tolerance*100:.1f} pts). "
            "Investigate data source."
        )
        issues.append(msg)
        print(f"  {msg}")
    else:
        msg = (
            f"PASS statewide result: computed D two-party share = {computed_share:.4f} "
            f"({computed_share*100:.2f}%), expected ~{known_dem_share*100:.1f}%, "
            f"difference = {diff*100:.2f} pts."
        )
        issues.append(msg)
        print(f"  {msg}")

    return issues
