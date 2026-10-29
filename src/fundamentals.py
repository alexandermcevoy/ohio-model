"""
fundamentals.py — national-level polling aggregates (Phase B).

Two national signals feed national_to_state.py's Ohio translation:
  - the generic congressional ballot (primary — see national_to_state.py's
    module docstring for why this is preferred over approval alone)
  - presidential approval (secondary / fallback signal)

Each lives in a different raw schema (see CLAUDE.md's Polling Data section),
so this module normalizes both to a common two-party-style share before
handing them to ingest_polls.weighted_average(). Unlike the OH governor/
Senate files, these are national — no `state` column to filter on.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from src.constants import POLL_PARTISAN_DISCOUNT, POLL_RECENCY_HALF_LIFE_DAYS
from src.ingest_polls import POLLS, weighted_average


@dataclass(frozen=True)
class NationalSignal:
    """One national polling aggregate: value, uncertainty, and provenance."""

    value: float  # two-party (or approve-share) mean, 0-1 scale
    sigma: float  # weighted std across contributing polls
    n_polls: int
    as_of: pd.Timestamp
    source: str  # "generic_ballot" | "approval"


# ---------------------------------------------------------------------------
# Generic congressional ballot (primary signal)
# ---------------------------------------------------------------------------

def load_generic_ballot_polls(path: Path | str | None = None) -> pd.DataFrame:
    """
    Load the Silver Bulletin generic-ballot export.

    Different schema from the governor/senate/approval feeds: one row per
    poll (not per candidate-answer), dem/rep as direct raw topline columns,
    and dates as full-4-digit-year m/d/YYYY strings — NOT the 2-digit m/d/yy
    used in the other three files. Using the wrong format string here
    silently produces NaT rather than raising, so it's called out explicitly.
    """
    if path is None:
        path = POLLS / "generic_ballot.csv"
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(
            f"Generic ballot polls file not found: {path}. See CLAUDE.md's "
            "Polling Data section for the acquisition source (Silver Bulletin)."
        )

    df = pd.read_csv(path, low_memory=False)
    df["end_date"] = pd.to_datetime(df["enddate"], format="%m/%d/%Y", errors="coerce")
    df["population"] = df["population"].str.strip().str.lower()

    # Two-party normalize the raw (unadjusted) topline — deliberately not
    # using Silver Bulletin's own adjusted_dem/adjusted_rep/influence
    # columns, so this signal goes through the same transparent recency +
    # quality weighting as the OH governor/Senate polls rather than mixing
    # in someone else's opaque house-effects adjustment for just one input.
    df["dem_two_party"] = df["dem"] / (df["dem"] + df["rep"])

    # Silver Bulletin's own schema names this "samplesize" (no underscore)
    # and has no separate undecided/other rows to drop — decided_frac comes
    # straight from how much of the raw dem+rep topline is under 100.
    df["sample_size"] = df["samplesize"]
    df["decided_frac"] = (df["dem"] + df["rep"]) / 100.0
    return df


def national_generic_ballot_average(
    as_of: pd.Timestamp | None = None,
    path: Path | str | None = None,
) -> NationalSignal:
    """Recency- and quality-weighted national generic-ballot two-party D share."""
    df = load_generic_ballot_polls(path)
    if as_of is None:
        as_of = pd.Timestamp.now().normalize()
    mean, std, n = weighted_average(
        df,
        "dem_two_party",
        as_of=as_of,
        half_life_days=POLL_RECENCY_HALF_LIFE_DAYS,
        partisan_discount=POLL_PARTISAN_DISCOUNT,
    )
    return NationalSignal(value=mean, sigma=std, n_polls=n, as_of=as_of, source="generic_ballot")


# ---------------------------------------------------------------------------
# Presidential approval (secondary / fallback signal)
# ---------------------------------------------------------------------------

def load_approval_polls(path: Path | str | None = None) -> pd.DataFrame:
    """
    Load approval_polls.csv (poll-level Trump approval).

    Unlike governor.csv/senate.csv this file is already one row per poll —
    yes=approve pct, no=disapprove pct — not long-format candidate rows.
    """
    if path is None:
        path = POLLS / "approval_polls.csv"
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Approval polls file not found: {path}")

    df = pd.read_csv(path, low_memory=False)
    df["end_date"] = pd.to_datetime(df["end_date"], format="%m/%d/%y", errors="coerce")
    df = df.dropna(subset=["yes", "no"])
    df["approve_share"] = df["yes"] / (df["yes"] + df["no"])
    df["decided_frac"] = (df["yes"] + df["no"]) / 100.0
    return df


def national_approval_average(
    as_of: pd.Timestamp | None = None,
    path: Path | str | None = None,
) -> NationalSignal:
    """Recency- and quality-weighted national Trump approve-share (of approve+disapprove)."""
    df = load_approval_polls(path)
    if as_of is None:
        as_of = pd.Timestamp.now().normalize()
    mean, std, n = weighted_average(
        df,
        "approve_share",
        as_of=as_of,
        half_life_days=POLL_RECENCY_HALF_LIFE_DAYS,
        partisan_discount=POLL_PARTISAN_DISCOUNT,
    )
    return NationalSignal(value=mean, sigma=std, n_polls=n, as_of=as_of, source="approval")
