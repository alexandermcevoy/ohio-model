"""
forecast_statewide.py — blend the national fundamentals prior with direct
Ohio statewide polling into one posterior over the 2026 Ohio generic
partisan environment (Phase B, final step of the polls+fundamentals cluster
started with ingest_polls.py / fundamentals.py / national_to_state.py).

Scope, deliberately: this module estimates ONE shared latent quantity — "the
generic Ohio 2026 partisan environment" — the same kind of number a manually
chosen `--statewide-d` currently supplies to `simulate.py` for State House,
State Senate, and US House (applied uniformly to every district via
composite_lean). It does NOT attempt a governor- or Senate-race-specific
point forecast (e.g. "Acton's predicted vote share") — that would require
separating each race's own idiosyncratic deviation from the shared
environment used to estimate it in the first place, which double-counts
information without a more careful treatment than this pass attempts.
`src/governor.py`'s existing (pre-Phase-B) machinery is left as-is for that.

Method: Ohio's governor race (governor.csv) and US Senate special
(senate.csv) are two INDEPENDENT, race-specific, noisy readings of that same
shared environment — each carries real signal but also its own candidate-
specific idiosyncrasy (Ramaswamy's celebrity profile, Brown's decades of
incumbency brand, etc. can each pull a race away from the generic
environment in ways that don't reflect anything about the environment
itself). Treating each race's poll average, `national_to_state.py`'s
fundamentals prior, and `governor.py`'s own historical baseline as four
independent Gaussian estimates of one shared parameter, the standard
combination is inverse-variance ("precision") weighting: each signal's
weight is proportional to 1/sigma^2, so noisier signals contribute less
without being discarded outright.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from src.governor import build_baseline_distribution, compute_statewide_history
from src.ingest_polls import POLLS, load_polls, two_party_share, weighted_average
from src.constants import POLL_PARTISAN_DISCOUNT, POLL_RECENCY_HALF_LIFE_DAYS
from src.national_to_state import build_environment_prior

# ---------------------------------------------------------------------------
# 2026 Ohio statewide race candidates (update each cycle, same pattern as
# OPEN_SEATS_2026 in src/classify.py)
# ---------------------------------------------------------------------------

OHIO_2026_RACES: dict[str, tuple[str, str]] = {
    "governor": ("Amy Acton", "Vivek Ramaswamy"),
    "us_senate": ("Sherrod Brown", "Jon Husted"),
}

# ---------------------------------------------------------------------------
# Candidate/race idiosyncrasy — a documented judgment call, not a fit value
# ---------------------------------------------------------------------------

MIN_OHIO_SIGMA: float = 0.02
"""
Floor applied to the final blended sigma, regardless of how many signals
went into it or how tight each one's own sigma is.

The precision-weighted blend below treats the national fundamentals prior,
governor polls, and Senate polls as INDEPENDENT estimates of the shared
environment. They aren't fully independent: a systematic Ohio-wide polling
bias this cycle (e.g. a shared turnout-model blind spot affecting most OH
pollsters) would move the governor and Senate polling averages together,
not independently — that correlated-error component isn't modeled here
(full correlated multi-office posteriors are Phase C's job per ROADMAP.md,
not this pass's). Combining several signals via naive inverse-variance
weighting without accounting for that shared risk understates the true
final uncertainty. This floor is a blunt, documented guard against that
overconfidence rather than a fitted correction — 2 points, the same order
of magnitude as CANDIDATE_IDIOSYNCRASY_SIGMA below.
"""

SINGLE_POLL_FALLBACK_SIGMA: float = 0.06
"""
Fallback poll-spread sigma when a race has too few polls (n=1) for
`weighted_average()`'s own std to be defined — it correctly returns NaN
there (a single poll's spread genuinely is undefined, not zero), but an
undefined sigma can't be fed into the precision-weighted blend: NaN
propagates and poisons the entire posterior (confirmed this happening in
practice, early in the 2026 cycle when only one OH poll existed yet —
`build_forecast()` silently returned NaN for the whole blended environment).
0.06 matches this project's other "insufficient data" conservative
fallbacks (e.g. `compute_sigma_prior()`'s same default) rather than
inventing a new number.
"""

CANDIDATE_IDIOSYNCRASY_SIGMA: float = 0.02
"""
Additional uncertainty (in quadrature) applied to each individual race's
polling average when treating it as a measurement of the SHARED generic
environment, reflecting that a specific race's result is the environment
plus a candidate-specific deviation, not the environment itself.

NOT fit from data — no historical archive of within-cycle statewide races
(e.g. governor vs. US Senate results in the same Ohio election year) is
currently persisted anywhere in this repo; that comparison happens
transiently inside the composite-lean pipeline (src/partisan.py) and isn't
written to a reusable file (see CLAUDE.md Known Limitations). 0.02 (2 pts)
is a round-number judgment call, informed by but not derived from this
cycle's own observed gap between the OH governor and Senate polling
averages (0.8-2.7 pts depending on weighting — see `build_forecast()`'s
printed diagnostics). Treat this the same way as
LITERATURE_INCUMBENCY_ADVANTAGE (src/constants.py): a documented estimate
to revisit once better data exists, not a validated coefficient.
"""


@dataclass(frozen=True)
class EnvironmentSignal:
    """One independent estimate of the shared latent Ohio 2026 environment."""

    label: str  # "national_fundamentals" | "governor_polls" | "senate_polls" | "historical_governor"
    mean: float
    sigma: float
    n: int  # n_polls, or n_cycles for the historical baseline


@dataclass(frozen=True)
class ForecastSummary:
    """Posterior over the 2026 Ohio generic statewide two-party D share."""

    ohio_value: float
    ohio_sigma: float
    as_of: pd.Timestamp
    method: str  # "precision_weighted_blend"
    components: tuple[EnvironmentSignal, ...]
    weights: tuple[float, ...]  # each signal's share of total precision; sums to 1.0


# ---------------------------------------------------------------------------
# Individual signals
# ---------------------------------------------------------------------------

def _oh_race_signal(
    path,
    dem_name: str,
    rep_name: str,
    label: str,
    as_of: pd.Timestamp,
) -> EnvironmentSignal:
    """
    OH statewide race polling average, inflated by CANDIDATE_IDIOSYNCRASY_SIGMA
    to reflect that this is a race-specific reading of the generic
    environment, not the environment itself.
    """
    df = load_polls(path, state="OH", cycle=2026, stage="general")
    two_party = two_party_share(df, dem_name, rep_name)
    poll_mean, poll_sigma, n = weighted_average(
        two_party,
        "dem_two_party",
        as_of=as_of,
        half_life_days=POLL_RECENCY_HALF_LIFE_DAYS,
        partisan_discount=POLL_PARTISAN_DISCOUNT,
    )
    if np.isnan(poll_sigma):
        poll_sigma = SINGLE_POLL_FALLBACK_SIGMA
    total_sigma = float(np.sqrt(poll_sigma**2 + CANDIDATE_IDIOSYNCRASY_SIGMA**2))
    return EnvironmentSignal(label=label, mean=poll_mean, sigma=total_sigma, n=n)


def _fundamentals_signal(as_of: pd.Timestamp) -> EnvironmentSignal:
    prior_summary, _ = build_environment_prior(as_of=as_of)
    return EnvironmentSignal(
        label="national_fundamentals",
        mean=prior_summary.ohio_value,
        sigma=prior_summary.ohio_sigma,
        n=prior_summary.n_polls,
    )


def _historical_governor_signal() -> EnvironmentSignal:
    history = compute_statewide_history()
    baseline, _ = build_baseline_distribution(history)
    return EnvironmentSignal(
        label="historical_governor",
        mean=baseline.mean_d_share,
        sigma=baseline.std_d_share,
        n=baseline.n_cycles,
    )


# ---------------------------------------------------------------------------
# Precision-weighted blend
# ---------------------------------------------------------------------------

def build_forecast(
    as_of: pd.Timestamp | None = None,
    n_samples: int = 10_000,
    seed: int = 42,
    include_historical_governor_baseline: bool = True,
) -> tuple[ForecastSummary, np.ndarray]:
    """
    Blend all available independent estimates of the 2026 Ohio generic
    statewide two-party D share into one posterior via inverse-variance
    ("precision") weighting: mean = sum(mean_i / sigma_i^2) / sum(1/sigma_i^2),
    var = 1 / sum(1/sigma_i^2). Standard combination for independent Gaussian
    estimates of one shared parameter — noisier signals (wide sigma) get
    less weight without being discarded.

    Poll-derived sigmas are NOT shrunk by sqrt(n_polls): this project follows
    standard election-forecasting practice of treating cross-poll dispersion
    itself as the uncertainty estimate, since polling error is substantially
    correlated across polls (shared house effects, correlated methodology
    misses) rather than independent sampling noise that would average out
    with more polls.

    Returns
    -------
    (ForecastSummary, samples) — samples is `n_samples` draws from
    Normal(ohio_value, ohio_sigma), clipped to [0.20, 0.80], ready to feed
    `simulate.py::run_simulations()`'s distribution mode directly.
    """
    if as_of is None:
        as_of = pd.Timestamp.now().normalize()

    dem_gov, rep_gov = OHIO_2026_RACES["governor"]
    dem_sen, rep_sen = OHIO_2026_RACES["us_senate"]

    signals = [
        _fundamentals_signal(as_of),
        _oh_race_signal(POLLS / "governor.csv", dem_gov, rep_gov, "governor_polls", as_of),
        _oh_race_signal(POLLS / "senate.csv", dem_sen, rep_sen, "senate_polls", as_of),
    ]
    if include_historical_governor_baseline:
        signals.append(_historical_governor_signal())

    precisions = np.array([1.0 / s.sigma**2 for s in signals])
    means = np.array([s.mean for s in signals])

    total_precision = precisions.sum()
    ohio_value = float((means * precisions).sum() / total_precision)
    ohio_sigma = max(float(np.sqrt(1.0 / total_precision)), MIN_OHIO_SIGMA)
    weights = tuple((precisions / total_precision).tolist())

    summary = ForecastSummary(
        ohio_value=ohio_value,
        ohio_sigma=ohio_sigma,
        as_of=as_of,
        method="precision_weighted_blend",
        components=tuple(signals),
        weights=weights,
    )

    rng = np.random.default_rng(seed)
    samples = rng.normal(loc=ohio_value, scale=ohio_sigma, size=n_samples)
    samples = np.clip(samples, 0.20, 0.80)
    return summary, samples


def print_forecast_summary(summary: ForecastSummary) -> None:
    """Human-readable breakdown of each input signal and its blend weight."""
    print(f"Ohio 2026 generic environment forecast — as of {summary.as_of.date()}")
    print(f"Method: {summary.method}")
    print()
    print(f"{'Signal':<22} {'Mean D%':>8} {'Sigma':>7} {'N':>6} {'Weight':>8}")
    print("-" * 55)
    for signal, weight in zip(summary.components, summary.weights):
        print(
            f"{signal.label:<22} {signal.mean*100:>7.1f}% {signal.sigma*100:>6.2f}p "
            f"{signal.n:>6} {weight*100:>7.1f}%"
        )
    print("-" * 55)
    print(
        f"{'BLENDED':<22} {summary.ohio_value*100:>7.1f}% "
        f"{summary.ohio_sigma*100:>6.2f}p"
    )
    if abs(summary.ohio_sigma - MIN_OHIO_SIGMA) < 1e-9:
        print(
            f"  (sigma floored at {MIN_OHIO_SIGMA*100:.1f} pts — the naive "
            "precision-weighted combination came in tighter than that; see "
            "MIN_OHIO_SIGMA docstring for why it's floored)"
        )
