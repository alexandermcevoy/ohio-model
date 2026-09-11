"""
ingest_polls.py — shared utilities for the 538-schema raw polls CSVs.

Ohio-specific and national polls (data/polls/*.csv) share one schema family:
one row per (poll, candidate-or-answer). This module holds the generic
loading, two-party normalization, and weighting logic so it's written once
rather than re-derived per office or per race. `fundamentals.py` and
`national_to_state.py` build on top of it.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
POLLS = REPO_ROOT / "data" / "polls"

# Non-candidate response options that appear as rows in the raw feed and must
# be excluded before computing a two-party share (they're in the topline pct
# but aren't a vote for either party).
NON_CANDIDATE_ANSWERS = {"Don't know", "Someone else", "Would not vote"}


def load_polls(
    path: Path | str,
    state: str | None = None,
    cycle: int | None = None,
    office_type: str | None = None,
    stage: str | None = None,
) -> pd.DataFrame:
    """
    Load a 538-schema raw polls CSV and apply standard filters.

    Parses start_date/end_date as real dates. The raw m/d/yy strings are NOT
    zero-padded, so a naive string sort silently mis-orders them (e.g.
    "9/22/25" sorts before "1/12/26" alphabetically despite being earlier) —
    confirmed this bites in practice when checking date ranges on these files.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Polls file not found: {path}")

    df = pd.read_csv(path, low_memory=False)
    for col in ("start_date", "end_date"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format="%m/%d/%y", errors="coerce")

    if state is not None and "state" in df.columns:
        df = df[df["state"] == state]
    if cycle is not None and "cycle" in df.columns:
        df = df[df["cycle"] == cycle]
    if office_type is not None and "office_type" in df.columns:
        df = df[df["office_type"] == office_type]
    if stage is not None and "stage" in df.columns:
        df = df[df["stage"] == stage]

    return df.reset_index(drop=True)


def two_party_share(
    df: pd.DataFrame,
    dem_name: str,
    rep_name: str,
) -> pd.DataFrame:
    """
    Reduce a long-format polls DataFrame (one row per candidate-answer) to
    one row per poll with a two-party D share.

    Drops "don't know" / "someone else" / "would not vote" rows before
    normalizing — those toplines are not part of the two-party share. This
    matters materially: an OH governor poll reads Acton 46.67 / Ramaswamy
    46.85 on the topline (6.06% "don't know" absorbs the rest), which is
    49.9% two-party D, not 46.7%.
    """
    sub = df[df["candidate_name"].isin([dem_name, rep_name])]
    piv = sub.pivot_table(
        index="poll_id", columns="candidate_name", values="pct", aggfunc="first"
    )
    piv = piv.dropna(subset=[dem_name, rep_name])

    if piv.empty:
        return pd.DataFrame(
            columns=["poll_id", "dem_pct", "rep_pct", "dem_two_party", "end_date"]
        )

    result = piv.rename(columns={dem_name: "dem_pct", rep_name: "rep_pct"}).reset_index()
    result["dem_two_party"] = result["dem_pct"] / (result["dem_pct"] + result["rep_pct"])

    meta_cols = [
        c
        for c in [
            "poll_id",
            "pollster",
            "end_date",
            "sample_size",
            "population",
            "numeric_grade",
            "pollscore",
            "partisan",
            "internal",
        ]
        if c in df.columns
    ]
    meta = df[meta_cols].drop_duplicates(subset="poll_id")
    return result.merge(meta, on="poll_id", how="left")


def recency_weight(
    end_date: pd.Series, as_of: pd.Timestamp, half_life_days: float = 21.0
) -> pd.Series:
    """Exponential recency decay: weight halves every `half_life_days`."""
    age_days = (as_of - end_date).dt.days.clip(lower=0)
    return 0.5 ** (age_days / half_life_days)


def quality_weight(df: pd.DataFrame, partisan_discount: float = 0.5) -> pd.Series:
    """
    Pollster-quality weight from 538's numeric_grade (roughly 0-3 scale),
    scaled to [0.3, 1.0]. Missing grades (common for newer pollsters) are
    treated as below-average (1.5) rather than excluded — dropping ungraded
    pollsters entirely would throw away real polling data, not just noise.

    Partisan-sponsored or internal-campaign polls are downweighted, not
    excluded, since they still carry information but skew predictably.
    """
    grade = (
        df["numeric_grade"] if "numeric_grade" in df.columns
        else pd.Series(np.nan, index=df.index)
    )
    grade_weight = (grade.fillna(1.5) / 3.0).clip(0.3, 1.0)

    partisan_flag = (
        df["partisan"].notna() if "partisan" in df.columns
        else pd.Series(False, index=df.index)
    )
    internal_flag = (
        df["internal"].fillna(False) if "internal" in df.columns
        else pd.Series(False, index=df.index)
    )
    discount = np.where(partisan_flag | internal_flag, partisan_discount, 1.0)

    return grade_weight * discount


def weighted_average(
    df: pd.DataFrame,
    value_col: str,
    as_of: pd.Timestamp | None = None,
    half_life_days: float = 21.0,
    partisan_discount: float = 0.5,
) -> tuple[float, float, int]:
    """
    Recency- and quality-weighted mean + weighted std + poll count.

    Returns (weighted_mean, weighted_std, n_polls). std is NaN when only one
    poll is present (undefined, not zero — a single poll doesn't mean zero
    uncertainty).

    Polls with `end_date` after `as_of` are dropped before weighting, not
    just down-weighted — `recency_weight()`'s age calculation clips negative
    ages to 0, so a poll from the future relative to `as_of` would otherwise
    get treated as maximally fresh (same weight as a same-day poll) instead
    of being excluded. That's look-ahead bias: reconstructing "what the
    forecast said on a past date" must not see polls released after that
    date. Confirmed this actually bit — before this fix, asking for
    `as_of=2026-06-01` still pulled in every poll through September.
    """
    if df.empty:
        raise ValueError("weighted_average called on an empty DataFrame")

    if as_of is None:
        as_of = pd.Timestamp.now().normalize()

    df = df[df["end_date"] <= as_of]
    if df.empty:
        raise ValueError(f"No polls with end_date <= {as_of.date()} — cannot compute a point-in-time average")

    w_recency = recency_weight(df["end_date"], as_of, half_life_days)
    w_quality = quality_weight(df, partisan_discount)
    weights = (w_recency * w_quality).to_numpy()

    if weights.sum() <= 0:
        weights = np.ones(len(df))

    values = df[value_col].to_numpy()
    mean = float(np.average(values, weights=weights))

    if len(df) > 1:
        variance = float(np.average((values - mean) ** 2, weights=weights))
        std = float(np.sqrt(variance))
    else:
        std = float("nan")

    return mean, std, len(df)
