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

Before blending, each race's poll average is put on the reference scale
(src/composite.py REFERENCE_MIDTERM_RACES — the same "statewide D%" the
House model is calibrated to) using that race's measured deviation from the
reference in past Ohio cycles: see RACE_ADJUSTMENTS.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache

import numpy as np
import pandas as pd

from src.composite import reference_share
from src.governor import PROCESSED, build_baseline_distribution, compute_statewide_history
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

Separate from simulate.compute_shared_sigma(): that term covers the House's
offset from the reference race even when the reference is known exactly;
this floor covers error in estimating the reference itself.
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
Floor on the race-specific sigma added (in quadrature) to each race's
polling average when treating it as a reading of the shared environment.

The race-specific sigma is now measured rather than assumed: it is the
predictive spread of that race's past deviations from the reference race,
computed from the block vote surfaces (RACE_ADJUSTMENTS below). This 2-pt
round number, the original judgment call, only applies if a measured spread
comes in smaller.
"""


@dataclass(frozen=True)
class RaceAdjustment:
    """How one statewide race's polls are translated to the reference scale."""

    race: str                 # race label in block_votes_{year}.parquet, e.g. "uss"
    cycles: tuple[int, ...]   # past Ohio cycles the deviation is measured over
    apply_mean: bool          # subtract the mean deviation from the poll average?
    basis: str                # plain-English description for printed output


RACE_ADJUSTMENTS: dict[str, RaceAdjustment] = {
    # Brown ran ahead of the reference race in all three of his prior Ohio
    # races. The premium is his, not the Senate office's: Fisher (2010) and
    # Strickland (2016) ran well BEHIND the rest of the ticket.
    "us_senate": RaceAdjustment(
        race="uss", cycles=(2012, 2018, 2024), apply_mean=True,
        basis="Sherrod Brown's prior Ohio Senate races",
    ),
    # Acton has no Ohio electoral record, so no mean shift is applied (its
    # sign is unknown). The spread of past governor races around the
    # reference, with a different matchup each time, sets how noisy this
    # race is as a reading of the environment.
    "governor": RaceAdjustment(
        race="gov", cycles=(2010, 2014, 2018, 2022), apply_mean=False,
        basis="Ohio governor races 2010-2022; spread only, mean not applied",
    ),
}


@dataclass(frozen=True)
class RaceDeviation:
    """A race's measured deviation from the reference race in past cycles."""

    race: str
    basis: str
    per_cycle: tuple[tuple[int, float], ...]  # (year, race share - reference share)
    mean: float
    predictive_sd: float   # sd * sqrt(1 + 1/n): spread of a NEW cycle's deviation
    applied_offset: float  # subtracted from the poll average (0 when mean not applied)


@lru_cache(maxsize=None)
def _statewide_shares(year: int) -> dict[str, float]:
    """Statewide two-party D share per race label ('uss', 'atg', ...) for one year."""
    path = PROCESSED / f"block_votes_{year}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"{path} not found — run `python cli.py backbone --build-surfaces`.")
    df = pd.read_parquet(path, columns=["race", "d_votes", "r_votes"])
    totals = df.groupby("race")[["d_votes", "r_votes"]].sum()
    suffix = f"_{year}"
    return {
        race[: -len(suffix)]: float(d / (d + r))
        for race, (d, r) in totals.iterrows()
        if race.endswith(suffix) and d + r > 0
    }


def race_deviation(adj: RaceAdjustment) -> RaceDeviation:
    """Measure `adj.race` against the reference race in each of `adj.cycles`."""
    per_cycle = []
    for year in adj.cycles:
        shares = _statewide_shares(year)
        if adj.race not in shares:
            raise KeyError(f"Race '{adj.race}' not in block_votes_{year}.parquet")
        per_cycle.append((year, shares[adj.race] - reference_share(shares, year)))
    if len(per_cycle) < 2:
        raise ValueError(f"Need >=2 cycles to measure a race deviation, got {adj.cycles}")

    devs = np.array([d for _, d in per_cycle])
    mean = float(devs.mean())
    predictive_sd = float(devs.std(ddof=1) * np.sqrt(1 + 1 / len(devs)))
    return RaceDeviation(
        race=adj.race,
        basis=adj.basis,
        per_cycle=tuple(per_cycle),
        mean=mean,
        predictive_sd=predictive_sd,
        applied_offset=mean if adj.apply_mean else 0.0,
    )


@dataclass(frozen=True)
class EnvironmentSignal:
    """One independent estimate of the shared latent Ohio 2026 environment."""

    label: str  # "national_fundamentals" | "governor_polls" | "senate_polls" | "historical_governor"
    mean: float
    sigma: float
    n: int  # n_polls, or n_cycles for the historical baseline
    raw_mean: float | None = None  # poll signals: average before the race-to-reference adjustment


@dataclass(frozen=True)
class ForecastSummary:
    """Posterior over the 2026 Ohio generic statewide two-party D share."""

    ohio_value: float
    ohio_sigma: float
    as_of: pd.Timestamp
    method: str  # "precision_weighted_blend"
    components: tuple[EnvironmentSignal, ...]
    weights: tuple[float, ...]  # each signal's share of total precision; sums to 1.0
    race_deviations: tuple[RaceDeviation, ...] = ()


# ---------------------------------------------------------------------------
# Individual signals
# ---------------------------------------------------------------------------

def _oh_race_signal(
    path,
    dem_name: str,
    rep_name: str,
    label: str,
    as_of: pd.Timestamp,
    deviation: RaceDeviation,
) -> EnvironmentSignal:
    """
    OH statewide race polling average put on the reference scale: minus the
    race's measured mean deviation (when applied), with the race's measured
    deviation spread (floored at CANDIDATE_IDIOSYNCRASY_SIGMA) added to the
    poll spread in quadrature.
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
    race_sigma = max(deviation.predictive_sd, CANDIDATE_IDIOSYNCRASY_SIGMA)
    total_sigma = float(np.sqrt(poll_sigma**2 + race_sigma**2))
    return EnvironmentSignal(
        label=label,
        mean=poll_mean - deviation.applied_offset,
        sigma=total_sigma,
        n=n,
        raw_mean=poll_mean,
    )


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
    gov_dev = race_deviation(RACE_ADJUSTMENTS["governor"])
    sen_dev = race_deviation(RACE_ADJUSTMENTS["us_senate"])

    signals = [
        _fundamentals_signal(as_of),
        _oh_race_signal(POLLS / "governor.csv", dem_gov, rep_gov, "governor_polls", as_of, gov_dev),
        _oh_race_signal(POLLS / "senate.csv", dem_sen, rep_sen, "senate_polls", as_of, sen_dev),
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
        race_deviations=(gov_dev, sen_dev),
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
    print(f"{'Signal':<22} {'Raw D%':>7} {'Adj':>6} {'Mean D%':>8} {'Sigma':>7} {'N':>6} {'Weight':>8}")
    print("-" * 70)
    for signal, weight in zip(summary.components, summary.weights):
        raw = signal.raw_mean if signal.raw_mean is not None else signal.mean
        print(
            f"{signal.label:<22} {raw*100:>6.1f}% {(signal.mean - raw)*100:>+5.1f}p "
            f"{signal.mean*100:>7.1f}% {signal.sigma*100:>6.2f}p "
            f"{signal.n:>6} {weight*100:>7.1f}%"
        )
    print("-" * 70)
    print(
        f"{'BLENDED':<22} {'':>7} {'':>6} {summary.ohio_value*100:>7.1f}% "
        f"{summary.ohio_sigma*100:>6.2f}p"
    )
    if summary.race_deviations:
        print("\nRace vs. reference race in past Ohio cycles (race D% minus reference D%):")
        for dev in summary.race_deviations:
            cycles = ", ".join(f"{y} {d*100:+.1f}" for y, d in dev.per_cycle)
            applied = (f"mean {dev.applied_offset*100:+.1f} subtracted"
                       if dev.applied_offset else "mean not applied")
            print(
                f"  {dev.race}: {cycles} -> mean {dev.mean*100:+.1f}, "
                f"predictive sd {dev.predictive_sd*100:.1f} pts; {applied}  [{dev.basis}]"
            )
    if abs(summary.ohio_sigma - MIN_OHIO_SIGMA) < 1e-9:
        print(
            f"  (sigma floored at {MIN_OHIO_SIGMA*100:.1f} pts — the naive "
            "precision-weighted combination came in tighter than that; see "
            "MIN_OHIO_SIGMA docstring for why it's floored)"
        )
