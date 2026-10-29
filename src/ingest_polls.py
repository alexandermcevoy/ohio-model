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

# Ceiling on a single poll's effective N, expressed as a multiple of the
# median effective N among the polls being combined in one weighted_average()
# call. Without this, a high-frequency large-panel tracker can numerically
# swallow the entire blend: confirmed on data/polls/generic_ballot.csv that
# Morning Consult (median ~25,500-respondent completes, 119 of 581 polls in
# that file, vs. ~1,000-2,000 for a typical pollster) would otherwise receive
# ~79% of the total weight on raw effective-N alone — turning an aggregate of
# 40+ pollsters into essentially one house's number. Real-world large online
# panels carry design effects that make their true information content far
# below their raw completed-interview count, which is the standard reason
# aggregators cap this rather than weight by raw N linearly. 3.0 is a
# judgment call, not a fitted value: generous enough that a genuinely large,
# well-conducted poll still outweighs a small one, tight enough that no
# single poll (or house running many similar-sized polls) can dominate a
# blend of dozens.
MAX_EFFECTIVE_N_MULTIPLE = 3.0


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

    Grouped by (poll_id, question_id) rather than poll_id alone: a single
    poll can test several hypothetical matchups as separate questions (e.g.
    an OH governor poll pairing Acton against Ramaswamy in one question and
    against a different Republican in another). Pivoting on poll_id alone
    would pick whichever row for each candidate name came first in the
    dataframe, which can silently pair the dem's answer to one question with
    the rep's answer from a different one. Grouping by question_id first
    keeps each matchup's two numbers together; if a poll asks about the same
    dem-vs-rep pairing in more than one question (repeat/tracking questions
    within one field period), those question-level shares are averaged into
    one poll-level row rather than arbitrarily taking the first.
    """
    sub = df[df["candidate_name"].isin([dem_name, rep_name])]
    group_cols = ["poll_id", "question_id"] if "question_id" in sub.columns else ["poll_id"]
    piv = sub.pivot_table(
        index=group_cols, columns="candidate_name", values="pct", aggfunc="first"
    )
    piv = piv.dropna(subset=[dem_name, rep_name])

    if piv.empty:
        return pd.DataFrame(
            columns=["poll_id", "dem_pct", "rep_pct", "dem_two_party", "end_date"]
        )

    result = piv.rename(columns={dem_name: "dem_pct", rep_name: "rep_pct"}).reset_index()
    result["dem_two_party"] = result["dem_pct"] / (result["dem_pct"] + result["rep_pct"])

    if "question_id" in result.columns:
        result = result.groupby("poll_id", as_index=False).agg(
            {"dem_pct": "mean", "rep_pct": "mean", "dem_two_party": "mean"}
        )

    # Fraction of the topline that actually landed on the dem or the rep —
    # the complement is "don't know" / "someone else" / third-party
    # candidates. Feeds weighted_average()'s effective-sample-size weighting:
    # a poll with a lot of undecided respondents has fewer of them actually
    # backing its reported dem/rep ratio than the headline sample size
    # implies.
    result["decided_frac"] = (result["dem_pct"] + result["rep_pct"]) / 100.0

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


def effective_n_weight(
    df: pd.DataFrame,
    value_col: str,
    n_col: str = "sample_size",
    decided_frac_col: str = "decided_frac",
) -> pd.Series:
    """
    Inverse-variance weight from each poll's own sampling error, treating its
    reported share as estimated only from respondents who landed on one of
    the two named outcomes — not from the full headline sample size.

    A poll's `value_col` (e.g. two-party D share) is a ratio among decided
    respondents. When a large share picked "don't know," a third-party
    candidate, or (for approval polls) "no opinion," the headline sample
    size overstates how many respondents actually back that ratio. Effective
    N = sample_size * decided_frac; standard binomial SE on that effective N
    gives each poll's own variance, and weight = 1/variance (normalized to
    have mean 1 across the polls being combined, so it composes with
    `recency_weight`/`quality_weight` without changing their overall scale).

    This also means, as a side effect, that raw sample size now matters at
    all — previously two polls got equal weight regardless of N=400 vs
    N=1500. That wasn't a deliberate prior choice, just something the old
    recency x quality weighting never accounted for.

    Falls back to uniform weight (1.0) when `n_col` or `decided_frac_col`
    isn't present, so callers that don't provide sample size or a decided
    fraction are unaffected.
    """
    if n_col not in df.columns or decided_frac_col not in df.columns:
        return pd.Series(1.0, index=df.index)

    n = pd.to_numeric(df[n_col], errors="coerce")
    decided_frac = df[decided_frac_col].clip(lower=0.05, upper=1.0)
    effective_n = n * decided_frac

    # Cap before computing variance, not after computing weight — see
    # MAX_EFFECTIVE_N_MULTIPLE docstring. Median is over this same call's
    # poll set (post as_of filtering), so the cap tracks whatever's actually
    # being combined rather than a fixed global constant.
    median_n = effective_n.median()
    if pd.notna(median_n) and median_n > 0:
        effective_n = effective_n.clip(upper=median_n * MAX_EFFECTIVE_N_MULTIPLE)

    p = df[value_col].clip(lower=0.05, upper=0.95)
    variance = p * (1 - p) / effective_n

    raw_weight = 1.0 / variance
    if not np.isfinite(raw_weight.to_numpy()).any():
        return pd.Series(1.0, index=df.index)

    raw_weight = raw_weight.fillna(raw_weight.median())
    mean_weight = raw_weight.mean()
    if not np.isfinite(mean_weight) or mean_weight <= 0:
        return pd.Series(1.0, index=df.index)

    return raw_weight / mean_weight


def poll_weights(
    df: pd.DataFrame,
    value_col: str,
    as_of: pd.Timestamp | None = None,
    half_life_days: float = 21.0,
    partisan_discount: float = 0.5,
) -> tuple[pd.DataFrame, np.ndarray]:
    """
    The polls weighted_average() uses (end_date <= as_of) and their weights,
    so other averages over the same polls (e.g. raw candidate toplines) can
    reuse exactly the same weighting.
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
    w_precision = effective_n_weight(df, value_col)
    weights = (w_recency * w_quality * w_precision).to_numpy()

    if weights.sum() <= 0:
        weights = np.ones(len(df))
    return df, weights


def weighted_average(
    df: pd.DataFrame,
    value_col: str,
    as_of: pd.Timestamp | None = None,
    half_life_days: float = 21.0,
    partisan_discount: float = 0.5,
) -> tuple[float, float, int]:
    """
    Recency-, quality-, and effective-sample-size-weighted mean + weighted
    std + poll count.

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
    df, weights = poll_weights(df, value_col, as_of, half_life_days, partisan_discount)
    values = df[value_col].to_numpy()
    mean = float(np.average(values, weights=weights))

    if len(df) > 1:
        variance = float(np.average((values - mean) ** 2, weights=weights))
        std = float(np.sqrt(variance))
    else:
        std = float("nan")

    return mean, std, len(df)
