"""
voterfile.py — Ohio SOS public voter file ingestion, scoring, and district aggregation.

Data source: Ohio Secretary of State statewide voter file (public record).
Files: SWVF_1_22.txt, SWVF_23_44.txt, SWVF_45_66.txt, SWVF_67_88.txt
       Batched by county number, stored in data/voterfiles/.
Coverage: ~7.9M registered voters across all 88 Ohio counties.

Key design decisions:
  - STATE_REPRESENTATIVE_DISTRICT pre-assigns each voter to an HD. No geocoding needed.
  - PII (names, addresses, DOB) is dropped on load and never written to any output.
  - Election history recoded to int8 (0=none, 1=X, 2=D_primary, 3=R_primary).
  - District aggregation computes mobilization and persuasion universes for targeting.

Ohio open primary note: any voter can pull any party's ballot in a primary. Primary
history is a signal of partisan lean, not a definitive party ID. See CLAUDE.md.
"""

from __future__ import annotations

import re
from pathlib import Path

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Column name constants (Ohio SOS voter file schema)
# ---------------------------------------------------------------------------

COL_VOTER_ID = "SOS_VOTERID"
COL_COUNTY   = "COUNTY_NUMBER"
COL_STATUS   = "VOTER_STATUS"
COL_PARTY    = "PARTY_AFFILIATION"
COL_PRECINCT = "PRECINCT_CODE"
COL_DISTRICT = "STATE_REPRESENTATIVE_DISTRICT"
COL_REG_DATE = "REGISTRATION_DATE"

# PII — dropped immediately on load, never written to any output file.
_PII_COLS: frozenset[str] = frozenset({
    "LAST_NAME", "FIRST_NAME", "MIDDLE_NAME", "SUFFIX", "DATE_OF_BIRTH",
    "RESIDENTIAL_ADDRESS1", "RESIDENTIAL_SECONDARY_ADDR", "RESIDENTIAL_CITY",
    "RESIDENTIAL_STATE", "RESIDENTIAL_ZIP", "RESIDENTIAL_ZIP_PLUS4",
    "RESIDENTIAL_COUNTRY", "RESIDENTIAL_POSTALCODE",
    "MAILING_ADDRESS1", "MAILING_SECONDARY_ADDRESS", "MAILING_CITY",
    "MAILING_STATE", "MAILING_ZIP", "MAILING_ZIP_PLUS4",
    "MAILING_COUNTRY", "MAILING_POSTAL_CODE",
})

# Administrative columns not needed for analysis
_DROP_COLS: frozenset[str] = frozenset({
    "COUNTY_ID", "CAREER_CENTER", "CITY", "CITY_SCHOOL_DISTRICT",
    "COUNTY_COURT_DISTRICT", "COURT_OF_APPEALS", "EDU_SERVICE_CENTER_DISTRICT",
    "EXEMPTED_VILL_SCHOOL_DISTRICT", "LIBRARY", "LOCAL_SCHOOL_DISTRICT",
    "MUNICIPAL_COURT_DISTRICT", "STATE_BOARD_OF_EDUCATION", "PRECINCT_NAME",
    "TOWNSHIP", "VILLAGE", "WARD",
    "CONGRESSIONAL_DISTRICT", "STATE_SENATE_DISTRICT",
})

# Election history encoding
_VOTE_RECODE: dict[str, int] = {"": 0, "X": 1, "D": 2, "R": 3}

# Key November generals for propensity scoring (federal/statewide cycles)
_KEY_GENERAL_YEARS: frozenset[int] = frozenset({2018, 2020, 2022, 2024})
_PRES_GENERAL_YEARS: frozenset[int] = frozenset({2020, 2024})
_MIDTERM_GENERAL_YEARS: frozenset[int] = frozenset({2018, 2022})

# Primary history window for partisan lean scoring
_PRIMARY_MIN_YEAR: int = 2016

# Mobilization/persuasion threshold: share of active voters that must qualify
# for a district to be classified as that mode. Print distribution to calibrate.
TARGETING_THRESHOLD: float = 0.05

# Default paths
DEFAULT_VOTERFILE_DIR    = "data/voterfiles"
DEFAULT_CLEAN_PARQUET    = "data/processed/voter_file_clean.parquet"
DEFAULT_INACTIVE_PARQUET = "data/processed/voter_file_inactive_counts.parquet"
DEFAULT_VOTER_UNIVERSE   = "data/processed/oh_house_voter_universe.csv"


# ---------------------------------------------------------------------------
# Election column utilities
# ---------------------------------------------------------------------------

_ELEC_RE = re.compile(r"^(PRIMARY|GENERAL|SPECIAL)-(\d{2})/\d{2}/(\d{4})$")


def _parse_election_col(col: str) -> tuple[str, int, int] | None:
    """Return (type, month, year) from an election column name, or None."""
    m = _ELEC_RE.match(col)
    if not m:
        return None
    return m.group(1), int(m.group(2)), int(m.group(3))


def identify_election_groups(all_cols: list[str]) -> dict[str, list[str]]:
    """
    Categorize election columns by role.

    Returns dict with keys:
      general_key    — November generals for 2018/2020/2022/2024 (propensity scoring)
      pres_general   — 2020/2024 November generals (presidential_only flag)
      midterm_general— 2018/2022 November generals
      primary_recent — All primaries from 2016 onward (partisan lean scoring)
      all_election   — Every election column (any type, any year)
    """
    groups: dict[str, list[str]] = {
        "general_key": [],
        "pres_general": [],
        "midterm_general": [],
        "primary_recent": [],
        "all_election": [],
    }
    for col in all_cols:
        parsed = _parse_election_col(col)
        if parsed is None:
            continue
        elec_type, month, year = parsed
        groups["all_election"].append(col)
        if elec_type == "GENERAL" and month == 11:
            if year in _KEY_GENERAL_YEARS:
                groups["general_key"].append(col)
                if year in _PRES_GENERAL_YEARS:
                    groups["pres_general"].append(col)
                else:
                    groups["midterm_general"].append(col)
        elif elec_type == "PRIMARY" and year >= _PRIMARY_MIN_YEAR:
            groups["primary_recent"].append(col)

    for k in groups:
        groups[k] = sorted(groups[k])
    return groups


# ---------------------------------------------------------------------------
# Per-voter scoring (vectorized, applied to chunks)
# ---------------------------------------------------------------------------

def _recode_election_vals(series: pd.Series) -> pd.Series:
    """Recode election column: '' → 0, 'X' → 1, 'D' → 2, 'R' → 3, as int8."""
    return series.fillna("").map(_VOTE_RECODE).fillna(0).astype(np.int8)


def score_turnout_propensity(
    chunk: pd.DataFrame,
    general_key_cols: list[str],
    pres_cols: list[str],
    midterm_cols: list[str],
) -> tuple[pd.Series, pd.Series]:
    """
    Compute turnout propensity tier and presidential_only flag.

    Propensity tiers (based on last 4 key November generals):
      high      : voted in 4/4
      medium    : voted in 2–3/4
      low       : voted in 1/4
      very_low  : voted in 0/4

    presidential_only: voted in 2020 and/or 2024 general but NOT 2018 or 2022.
    These are the mobilization universe — they show up for president but not
    midterm/gubernatorial cycles.

    Returns (propensity Series[category], presidential_only Series[bool]).
    """
    # Count generals participated in (any non-zero value = voted)
    if general_key_cols:
        n_voted = sum(chunk[col].ne(0) for col in general_key_cols)
    else:
        n_voted = pd.Series(0, index=chunk.index)

    pres_voted = (
        pd.concat([chunk[c].ne(0) for c in pres_cols], axis=1).any(axis=1)
        if pres_cols
        else pd.Series(False, index=chunk.index)
    )
    midterm_voted = (
        pd.concat([chunk[c].ne(0) for c in midterm_cols], axis=1).any(axis=1)
        if midterm_cols
        else pd.Series(False, index=chunk.index)
    )
    presidential_only = pres_voted & ~midterm_voted

    propensity_raw = np.select(
        [n_voted == 4, (n_voted == 2) | (n_voted == 3), n_voted == 1],
        ["high", "medium", "low"],
        default="very_low",
    )
    propensity = pd.Categorical(
        propensity_raw,
        categories=["very_low", "low", "medium", "high"],
        ordered=True,
    )
    return pd.Series(propensity, index=chunk.index), presidential_only.astype(bool)


def score_partisan_lean(
    chunk: pd.DataFrame,
    primary_recent_cols: list[str],
) -> pd.Series:
    """
    Classify voter partisan lean from primary ballot history (2016+).

    Ohio has open primaries: any voter can pull any party's ballot. Primary history
    is a signal of partisan lean, not a definitive party ID.

    Classifications:
      strong_d     : pulled D ballot 3+ times, never R
      lean_d       : pulled D ballot at least once, never R
      strong_r     : pulled R ballot 3+ times, never D
      lean_r       : pulled R ballot at least once, never D
      crossover    : pulled both D and R in different cycles
      unaffiliated : never pulled a partisan primary ballot

    Note: 'X' in a primary column = voted in a non-partisan primary (no party signal).
    Only 'D' (value 2) and 'R' (value 3) are counted for partisan classification.
    """
    if not primary_recent_cols:
        return pd.Series(
            pd.Categorical(["unaffiliated"] * len(chunk), categories=[
                "strong_d", "lean_d", "crossover", "unaffiliated", "lean_r", "strong_r"
            ]),
            index=chunk.index,
        )

    d_ballots = (chunk[primary_recent_cols] == 2).sum(axis=1)  # value 2 = D
    r_ballots = (chunk[primary_recent_cols] == 3).sum(axis=1)  # value 3 = R

    both = (d_ballots > 0) & (r_ballots > 0)
    lean_raw = np.select(
        [
            both,                                        # crossover (takes priority)
            (d_ballots >= 3) & ~both,                   # strong_d
            (d_ballots > 0) & (r_ballots == 0),         # lean_d
            (r_ballots >= 3) & ~both,                   # strong_r
            (r_ballots > 0) & (d_ballots == 0),         # lean_r
        ],
        ["crossover", "strong_d", "lean_d", "strong_r", "lean_r"],
        default="unaffiliated",
    )
    return pd.Series(
        pd.Categorical(
            lean_raw,
            categories=["strong_d", "lean_d", "crossover", "unaffiliated", "lean_r", "strong_r"],
        ),
        index=chunk.index,
    )


# ---------------------------------------------------------------------------
# Load voter file → parquet
# ---------------------------------------------------------------------------

def load_voter_file(
    voterfile_dir: str = DEFAULT_VOTERFILE_DIR,
    output_parquet: str = DEFAULT_CLEAN_PARQUET,
    force: bool = False,
    chunk_size: int = 200_000,
    verbose: bool = True,
) -> None:
    """
    Load all four SOS voter file batches, score each voter, write clean parquet.

    ACTIVE voters go to the main parquet. INACTIVE voters are tallied per district
    and written to a companion parquet (voter_file_inactive_counts.parquet).

    PII columns (names, addresses, DOB) are dropped immediately and never persisted.
    All election history is retained in the parquet, recoded as int8.

    Requires ~2–3 GB RAM during processing; parquet on disk is ~300–500 MB.
    """
    output_path = Path(output_parquet)
    if output_path.exists() and not force:
        if verbose:
            print(f"  Parquet already exists at {output_path}. Pass force=True to rebuild.")
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)

    voterfile_paths = sorted(Path(voterfile_dir).glob("SWVF_*.txt"))
    if not voterfile_paths:
        raise FileNotFoundError(f"No voter file batches (SWVF_*.txt) found in {voterfile_dir}")

    if verbose:
        print(f"  Found {len(voterfile_paths)} voter file batch(es):")
        for p in voterfile_paths:
            print(f"    {p.name}")

    # ── Inspect header from first file ────────────────────────────────────────
    header_df = pd.read_csv(voterfile_paths[0], nrows=0, encoding="cp1252")
    all_cols = list(header_df.columns)

    groups = identify_election_groups(all_cols)
    if verbose:
        print(f"\n  Total columns in voter file: {len(all_cols)}")
        print(f"  Key generals (propensity scoring): {len(groups['general_key'])} → {groups['general_key']}")
        print(f"  Recent primaries (2016+):          {len(groups['primary_recent'])}")
        print(f"  All election columns:              {len(groups['all_election'])}")

    # Columns to read: meta (no PII) + all election history
    keep_meta = [
        c for c in all_cols
        if c not in _PII_COLS
        and c not in _DROP_COLS
        and _parse_election_col(c) is None
    ]
    keep_cols = keep_meta + groups["all_election"]

    if verbose:
        print(f"\n  Reading {len(keep_cols)} columns per voter "
              f"({len(keep_meta)} meta + {len(groups['all_election'])} election history)")
        print(f"  PII columns dropped on load: {len(_PII_COLS)}")

    # ── Process all files ─────────────────────────────────────────────────────
    all_active_chunks: list[pd.DataFrame] = []
    inactive_counts: dict[int, int] = {}
    total_active = 0
    total_inactive = 0
    total_missing_district = 0

    for fpath in voterfile_paths:
        if verbose:
            print(f"\n  Loading {fpath.name} …")
        file_active = 0
        file_inactive = 0

        for chunk in pd.read_csv(
            fpath,
            usecols=keep_cols,
            chunksize=chunk_size,
            dtype=str,
            keep_default_na=False,
            encoding="cp1252",
        ):
            # Recode all election history columns to int8
            for col in groups["all_election"]:
                if col in chunk.columns:
                    chunk[col] = _recode_election_vals(chunk[col])

            # Separate active / inactive
            is_active = chunk[COL_STATUS].str.upper() == "ACTIVE"
            inactive_chunk = chunk[~is_active]
            active_chunk = chunk[is_active].copy()

            file_active   += len(active_chunk)
            file_inactive += len(inactive_chunk)

            # Tally inactive counts per district
            if len(inactive_chunk) > 0:
                inact_dist = pd.to_numeric(
                    inactive_chunk[COL_DISTRICT].replace("", np.nan),
                    errors="coerce",
                ).dropna().astype(int)
                for dist, cnt in inact_dist.value_counts().items():
                    inactive_counts[int(dist)] = inactive_counts.get(int(dist), 0) + int(cnt)

            if len(active_chunk) == 0:
                continue

            # Validate and coerce district column
            dist_numeric = pd.to_numeric(
                active_chunk[COL_DISTRICT].replace("", np.nan),
                errors="coerce",
            )
            missing = int(dist_numeric.isna().sum())
            total_missing_district += missing
            active_chunk[COL_DISTRICT] = dist_numeric
            active_chunk = active_chunk[active_chunk[COL_DISTRICT].notna()].copy()
            active_chunk[COL_DISTRICT] = active_chunk[COL_DISTRICT].astype(int)

            # Per-voter scoring
            active_chunk["turnout_propensity"], active_chunk["presidential_only"] = (
                score_turnout_propensity(
                    active_chunk,
                    groups["general_key"],
                    groups["pres_general"],
                    groups["midterm_general"],
                )
            )
            active_chunk["partisan_lean"] = score_partisan_lean(
                active_chunk, groups["primary_recent"]
            )

            # Drop VOTER_STATUS (all rows are ACTIVE; no longer needed)
            active_chunk.drop(columns=[COL_STATUS], inplace=True, errors="ignore")

            all_active_chunks.append(active_chunk)

        if verbose:
            print(f"    Active: {file_active:,}   Inactive: {file_inactive:,}")
        total_active   += file_active
        total_inactive += file_inactive

    if not all_active_chunks:
        raise ValueError("No active voter records found across all voter file batches.")

    if verbose:
        print(f"\n  Concatenating {len(all_active_chunks)} processed chunk(s) …")

    voter_df = pd.concat(all_active_chunks, ignore_index=True)

    if verbose:
        print(f"  Total active voters:               {len(voter_df):,}")
        print(f"  Total inactive voters:             {total_inactive:,}")
        print(f"  Voters with missing district:      {total_missing_district:,}")
        print(f"  Districts covered:                 {voter_df[COL_DISTRICT].nunique()} of 99")
        # Statewide PARTY_AFFILIATION breakdown
        pa_counts = voter_df[COL_PARTY].value_counts()
        print(f"\n  PARTY_AFFILIATION top values (reflects most recent primary ballot):")
        for val, cnt in pa_counts.head(8).items():
            print(f"    {val!r:<6}: {cnt:>9,}  ({cnt/len(voter_df):.1%})")

    # Save inactive counts
    inactive_df = pd.DataFrame(
        sorted(inactive_counts.items()),
        columns=["district", "inactive_voters"],
    )
    inactive_path = Path(output_parquet).parent / "voter_file_inactive_counts.parquet"
    inactive_df.to_parquet(inactive_path, index=False)

    # Write clean parquet
    if verbose:
        print(f"\n  Writing clean parquet to {output_path} …")
    voter_df.to_parquet(output_path, index=False, compression="snappy")

    if verbose:
        size_mb = output_path.stat().st_size / 1_048_576
        print(f"  Done. Parquet size: {size_mb:.1f} MB")


# ---------------------------------------------------------------------------
# District-level aggregation → voter universe
# ---------------------------------------------------------------------------

def build_voter_universe(
    parquet_path: str = DEFAULT_CLEAN_PARQUET,
    output_csv: str = DEFAULT_VOTER_UNIVERSE,
    verbose: bool = True,
) -> pd.DataFrame:
    """
    Aggregate per-voter scores to district level.

    Computes partisan composition, turnout rates, mobilization universe,
    persuasion universe, and voter-file-based targeting mode for all 99 districts.

    Returns the voter universe DataFrame and writes it to output_csv.
    """
    ppath = Path(parquet_path)
    if not ppath.exists():
        raise FileNotFoundError(
            f"Voter file parquet not found at {ppath}. "
            "Run load_voter_file() first (or: python cli.py voters --build)."
        )

    if verbose:
        print(f"  Loading voter file parquet from {ppath} …")

    voter_df = pd.read_parquet(ppath)

    if verbose:
        print(f"  {len(voter_df):,} active voters across "
              f"{voter_df[COL_DISTRICT].nunique()} districts")

    # Re-identify election groups from parquet columns
    all_cols = list(voter_df.columns)
    groups = identify_election_groups(all_cols)

    # ── Load inactive counts ─────────────────────────────────────────────────
    inactive_path = ppath.parent / "voter_file_inactive_counts.parquet"
    inactive_df = (
        pd.read_parquet(inactive_path)
        if inactive_path.exists()
        else pd.DataFrame(columns=["district", "inactive_voters"])
    )

    # ── Precompute per-voter turnout flags for each key general ───────────────
    year_voted: dict[int, pd.Series] = {}
    for col in groups["general_key"]:
        parsed = _parse_election_col(col)
        if parsed:
            year_voted[parsed[2]] = voter_df[col].ne(0)

    # ── Groupby district ──────────────────────────────────────────────────────
    if verbose:
        print("  Aggregating to district level …")

    grp = voter_df.groupby(COL_DISTRICT)

    total_active = grp.size().rename("total_active_voters")

    # Partisan composition counts
    partisan_raw = (
        voter_df.groupby([COL_DISTRICT, "partisan_lean"])
        .size()
        .unstack(fill_value=0)
    )
    for lbl in ["strong_d", "lean_d", "crossover", "unaffiliated", "lean_r", "strong_r"]:
        if lbl not in partisan_raw.columns:
            partisan_raw[lbl] = 0
    partisan_counts = partisan_raw[
        ["strong_d", "lean_d", "crossover", "unaffiliated", "lean_r", "strong_r"]
    ].rename(columns=lambda c: f"n_{c}")

    # Turnout propensity counts
    prop_raw = (
        voter_df.groupby([COL_DISTRICT, "turnout_propensity"])
        .size()
        .unstack(fill_value=0)
    )
    for lbl in ["high", "medium", "low", "very_low"]:
        if lbl not in prop_raw.columns:
            prop_raw[lbl] = 0
    prop_counts = prop_raw[["high", "medium", "low", "very_low"]].rename(
        columns=lambda c: f"n_{c}_propensity"
    )

    # Presidential-only count
    pres_only = grp["presidential_only"].sum().rename("n_presidential_only")

    # Turnout rates per key general year
    turnout_rates: dict[str, pd.Series] = {}
    for year, voted_series in year_voted.items():
        tmp = voted_series.rename(f"_v{year}")
        voter_df[f"_v{year}"] = tmp
        turnout_rates[f"turnout_{year}"] = (
            voter_df.groupby(COL_DISTRICT)[f"_v{year}"].mean()
        )
    voter_df.drop(
        columns=[c for c in voter_df.columns if c.startswith("_v")],
        inplace=True,
    )

    # ── Mobilization targets: D-leaning + low-propensity or presidential-only ─
    mob_mask = (
        voter_df["partisan_lean"].isin(["strong_d", "lean_d"])
        & (
            voter_df["presidential_only"]
            | voter_df["turnout_propensity"].isin(["low", "very_low"])
        )
    )
    mob_counts = (
        voter_df[mob_mask]
        .groupby(COL_DISTRICT)
        .size()
        .rename("n_mobilization_targets")
    )

    # ── Persuasion targets: unaffiliated/crossover + regular voters ───────────
    pers_mask = (
        voter_df["partisan_lean"].isin(["crossover", "unaffiliated"])
        & voter_df["turnout_propensity"].isin(["medium", "high"])
    )
    pers_counts = (
        voter_df[pers_mask]
        .groupby(COL_DISTRICT)
        .size()
        .rename("n_persuasion_targets")
    )

    # ── Assemble ──────────────────────────────────────────────────────────────
    universe = total_active.reset_index()
    universe = universe.merge(partisan_counts.reset_index(), on=COL_DISTRICT, how="left")
    universe = universe.merge(prop_counts.reset_index(), on=COL_DISTRICT, how="left")
    universe = universe.merge(pres_only.reset_index(), on=COL_DISTRICT, how="left")

    for col_name, series in turnout_rates.items():
        universe = universe.merge(
            series.rename(col_name).reset_index(),
            on=COL_DISTRICT,
            how="left",
        )

    universe = universe.merge(
        mob_counts.reset_index(), on=COL_DISTRICT, how="left"
    )
    universe = universe.merge(
        pers_counts.reset_index(), on=COL_DISTRICT, how="left"
    )
    universe = universe.merge(
        inactive_df.rename(columns={"district": COL_DISTRICT}),
        on=COL_DISTRICT,
        how="left",
    )

    # Rename district column to match rest of project
    universe = universe.rename(columns={COL_DISTRICT: "district"})

    # Fill any nulls from sparse districts
    int_cols = [c for c in universe.columns if c.startswith("n_")]
    universe[int_cols] = universe[int_cols].fillna(0).astype(int)
    universe["inactive_voters"] = universe["inactive_voters"].fillna(0).astype(int)

    # ── Derived metrics ───────────────────────────────────────────────────────
    n = universe["total_active_voters"].clip(lower=1)  # avoid /0

    for lbl in ["strong_d", "lean_d", "crossover", "unaffiliated", "lean_r", "strong_r"]:
        universe[f"pct_{lbl}"] = universe[f"n_{lbl}"] / n

    universe["partisan_advantage"] = (
        (universe["n_strong_d"] + universe["n_lean_d"])
        - (universe["n_strong_r"] + universe["n_lean_r"])
    ) / n

    universe["pct_presidential_only"]   = universe["n_presidential_only"] / n
    universe["pct_mobilization_targets"] = universe["n_mobilization_targets"] / n
    universe["pct_persuasion_targets"]   = universe["n_persuasion_targets"] / n

    # Turnout dropoff: midterm rate / presidential rate (lower = bigger drop = more mob opportunity)
    if "turnout_2022" in universe.columns and "turnout_2024" in universe.columns:
        universe["turnout_dropoff"] = (
            universe["turnout_2022"] / universe["turnout_2024"].clip(lower=0.001)
        )

    universe = universe.sort_values("district").reset_index(drop=True)

    # ── Validation ────────────────────────────────────────────────────────────
    if verbose:
        _print_validation(universe, statewide_turnout={
            year: float(series.mean()) for year, series in year_voted.items()
        })

    # ── Write output ──────────────────────────────────────────────────────────
    Path(output_csv).parent.mkdir(parents=True, exist_ok=True)
    universe.to_csv(output_csv, index=False, float_format="%.6f")
    if verbose:
        print(f"\n  Voter universe written to {output_csv}")

    return universe


# ---------------------------------------------------------------------------
# Targeting mode classification
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Merge voter universe into targeting CSV
# ---------------------------------------------------------------------------

def merge_voter_universe_into_targeting(
    targeting_df: pd.DataFrame,
    universe_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Merge voter universe metrics into the targeting DataFrame.

    Drops the old aggregate-based target_mode column (was insufficient_data
    for 86/99 districts and reflects a strategic framing the data can't support).
    The voter file adds factual counts — partisan composition, turnout rates,
    and contact universe sizes — without imposing a mobilization/persuasion label.
    """
    targeting_df = targeting_df.copy()

    # Drop the aggregate targeting mode — it was insufficient_data for 86/99
    # districts and carried a strategic framing the data doesn't support.
    # Also drop any voter universe columns already present (from a prior build)
    # to avoid _x/_y suffix collisions on re-run.
    cols_to_drop = [c for c in ["target_mode", "target_mode_aggregate"] if c in targeting_df.columns]
    cols_to_drop += [c for c in universe_df.columns if c != "district" and c in targeting_df.columns]
    targeting_df.drop(columns=cols_to_drop, inplace=True)

    universe_cols = [
        "district",
        "total_active_voters", "inactive_voters",
        "partisan_advantage",
        "n_strong_d", "n_lean_d", "n_strong_r", "n_lean_r",
        "n_crossover", "n_unaffiliated",
        "pct_strong_d", "pct_lean_d", "pct_strong_r", "pct_lean_r",
        "pct_crossover", "pct_unaffiliated",
        "n_mobilization_targets", "pct_mobilization_targets",
        "n_persuasion_targets", "pct_persuasion_targets",
        "pct_presidential_only", "turnout_dropoff",
        "turnout_2024", "turnout_2022", "turnout_2020", "turnout_2018",
    ]
    universe_cols = [c for c in universe_cols if c in universe_df.columns]

    return targeting_df.merge(universe_df[universe_cols], on="district", how="left")


# ---------------------------------------------------------------------------
# Contact universe export
# ---------------------------------------------------------------------------

def export_contact_universe(
    district: int,
    target_type: str,
    output_path: str,
    parquet_path: str = DEFAULT_CLEAN_PARQUET,
) -> int:
    """
    Export a filtered contact list for a specific district to CSV.

    target_type:
      'mobilization' — D-leaning voters with low turnout propensity or presidential-only
      'persuasion'   — Unaffiliated/crossover voters who vote regularly
      'all_targets'  — Union of both

    Output columns: voter_id, precinct_code, party_affiliation, turnout_propensity,
                    partisan_lean, presidential_only.

    PII policy: ONLY voter_id is exported (no names, addresses, or DOB).
    Operatives join on voter_id to their own SOS/VAN copy for contact info.

    Returns the number of records exported.
    """
    ppath = Path(parquet_path)
    if not ppath.exists():
        raise FileNotFoundError(
            f"Voter file parquet not found: {ppath}. "
            "Run: python cli.py voters --build"
        )

    voter_df = pd.read_parquet(
        ppath,
        columns=[
            COL_VOTER_ID, COL_PRECINCT, COL_PARTY, COL_DISTRICT,
            "turnout_propensity", "partisan_lean", "presidential_only",
        ],
    )

    dist_df = voter_df[voter_df[COL_DISTRICT] == district]
    if dist_df.empty:
        raise ValueError(f"No active voters found for district {district}")

    if target_type == "mobilization":
        mask = (
            dist_df["partisan_lean"].isin(["strong_d", "lean_d"])
            & (
                dist_df["presidential_only"]
                | dist_df["turnout_propensity"].isin(["low", "very_low"])
            )
        )
    elif target_type == "persuasion":
        mask = (
            dist_df["partisan_lean"].isin(["crossover", "unaffiliated"])
            & dist_df["turnout_propensity"].isin(["medium", "high"])
        )
    elif target_type == "all_targets":
        mob = (
            dist_df["partisan_lean"].isin(["strong_d", "lean_d"])
            & (
                dist_df["presidential_only"]
                | dist_df["turnout_propensity"].isin(["low", "very_low"])
            )
        )
        pers = (
            dist_df["partisan_lean"].isin(["crossover", "unaffiliated"])
            & dist_df["turnout_propensity"].isin(["medium", "high"])
        )
        mask = mob | pers
    else:
        raise ValueError(
            f"target_type must be 'mobilization', 'persuasion', or 'all_targets'. "
            f"Got: {target_type!r}"
        )

    contact_df = dist_df[mask].copy()
    contact_df = contact_df[
        [COL_VOTER_ID, COL_PRECINCT, COL_PARTY,
         "turnout_propensity", "partisan_lean", "presidential_only"]
    ].rename(columns={
        COL_VOTER_ID: "voter_id",
        COL_PRECINCT: "precinct_code",
        COL_PARTY: "party_affiliation",
    })
    contact_df = contact_df.sort_values(
        ["partisan_lean", "turnout_propensity"], ascending=[True, False]
    )

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    contact_df.to_csv(output_path, index=False)
    return len(contact_df)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def _print_validation(
    universe: pd.DataFrame,
    statewide_turnout: dict[int, float],
) -> None:
    """Print validation checks to stdout."""
    print("\n" + "=" * 62)
    print("VOTER FILE VALIDATION")
    print("=" * 62)

    total = universe["total_active_voters"].sum()
    print(f"  Active voters statewide:          {total:>10,}")
    print(f"  Districts covered:                {len(universe):>10} of 99")
    print(f"  Inactive voters statewide:        {universe['inactive_voters'].sum():>10,}")

    # Partisan composition
    sd = universe["n_strong_d"].sum()
    ld = universe["n_lean_d"].sum()
    sr = universe["n_strong_r"].sum()
    lr = universe["n_lean_r"].sum()
    co = universe["n_crossover"].sum()
    ua = universe["n_unaffiliated"].sum()

    print("\n  Statewide partisan composition (active registered voters):")
    for label, count in [
        ("Strong D", sd), ("Lean D", ld), ("Strong R", sr),
        ("Lean R", lr), ("Crossover", co), ("Unaffiliated", ua),
    ]:
        print(f"    {label:<14} {count:>9,}  ({count/total:5.1%})")

    # Check D share of primary-participating voters (expect 35–45%)
    primary_participants = sd + ld + sr + lr + co
    if primary_participants > 0:
        d_of_primary = (sd + ld) / primary_participants
        flag = "  ⚠ CHECK" if not (0.25 <= d_of_primary <= 0.60) else ""
        print(f"\n  D share of primary participants: {d_of_primary:.1%}{flag}")
        print(f"  (Ohio leans R; expect D ~35–45% of primary participants)")

    # Turnout rates
    print("\n  Statewide voter-file turnout (voters participated / active registered):")
    for year in sorted(statewide_turnout.keys(), reverse=True):
        rate = statewide_turnout[year]
        print(f"    {year}: {rate:.1%}")
    print("  (Compare to SOS-reported turnout; should be close but not identical)")

    # Spot-check
    print("\n  Spot-check (partisan composition):")
    checks = [
        (18, "safe_d  (Cleveland urban)"),
        (52, "tossup  (competitive NE Ohio)"),
        (80, "safe_r  (rural SE Ohio)"),
    ]
    for dist, desc in checks:
        row = universe[universe["district"] == dist]
        if row.empty:
            print(f"    District {dist} ({desc}): not found")
            continue
        r = row.iloc[0]
        d_pct  = r["pct_strong_d"] + r["pct_lean_d"]
        r_pct  = r["pct_strong_r"] + r["pct_lean_r"]
        ua_pct = r["pct_unaffiliated"]
        mob    = int(r["n_mobilization_targets"])
        print(
            f"    HD{dist:02d} ({desc}):  "
            f"D={d_pct:.0%}  R={r_pct:.0%}  Unaffiliated={ua_pct:.0%}  "
            f"mob_targets={mob:,}"
        )

    print("=" * 62)


# ---------------------------------------------------------------------------
# District summary for CLI display
# ---------------------------------------------------------------------------

def format_district_voter_summary(district: int, universe_df: pd.DataFrame) -> str:
    """Return a formatted text summary of voter universe metrics for one district."""
    row = universe_df[universe_df["district"] == district]
    if row.empty:
        return f"  No voter universe data for District {district}."

    r = row.iloc[0]
    n = int(r["total_active_voters"])

    lines = [
        f"\n  VOTER UNIVERSE — District {district}",
        f"  {'Active registered voters:':<34} {n:>8,}",
        f"  {'Inactive voters:':<34} {int(r.get('inactive_voters', 0)):>8,}",
        "",
        f"  Partisan composition (primary history, 2016+):",
        f"    {'Strong D:':<20} {int(r['n_strong_d']):>7,}  ({r['pct_strong_d']:.1%})",
        f"    {'Lean D:':<20} {int(r['n_lean_d']):>7,}  ({r['pct_lean_d']:.1%})",
        f"    {'Crossover:':<20} {int(r['n_crossover']):>7,}  ({r['pct_crossover']:.1%})",
        f"    {'Unaffiliated:':<20} {int(r['n_unaffiliated']):>7,}  ({r['pct_unaffiliated']:.1%})",
        f"    {'Lean R:':<20} {int(r['n_lean_r']):>7,}  ({r['pct_lean_r']:.1%})",
        f"    {'Strong R:':<20} {int(r['n_strong_r']):>7,}  ({r['pct_strong_r']:.1%})",
        f"    {'Partisan advantage:':<20} {r['partisan_advantage']:>+.3f}",
    ]

    if "turnout_2024" in r:
        lines += [
            "",
            f"  Turnout (general elections):",
        ]
        for yr in [2024, 2022, 2020, 2018]:
            col = f"turnout_{yr}"
            if col in r and not pd.isna(r[col]):
                lines.append(f"    {yr}: {r[col]:.1%}")
        if "turnout_dropoff" in r and not pd.isna(r.get("turnout_dropoff")):
            lines.append(f"    Dropoff (2022/2024): {r['turnout_dropoff']:.2f}")

    lines += [
        "",
        f"  Contact universes:",
        f"    {'D-leaning low-propensity:':<34} {int(r['n_mobilization_targets']):>6,}  ({r['pct_mobilization_targets']:.1%} of active voters)",
        f"    {'Unaffiliated/crossover regular:':<34} {int(r['n_persuasion_targets']):>6,}  ({r['pct_persuasion_targets']:.1%} of active voters)",
        f"    {'Presidential-only voters:':<34} {int(r['n_presidential_only']):>6,}  ({r['pct_presidential_only']:.1%})",
        "",
        f"  Note: partisan advantage reflects primary ballot history — a contact signal,",
        f"  not a measure of district partisanship. See composite lean for the latter.",
    ]

    return "\n".join(lines)
