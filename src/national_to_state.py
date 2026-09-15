"""
national_to_state.py — translate the national political environment into an
Ohio-equivalent statewide two-party D share (Phase B fundamentals prior).

Role, given what's now in data/polls/ (Sept 2026):
  - Ohio has DIRECT statewide polling for governor and US Senate
    (data/polls/governor.csv, senate.csv). For those two races, this
    module's output is a PRIOR to be blended with the direct OH polling
    average — that blend is forecast_statewide.py's job (not built yet),
    not this module's.
  - State House, State Senate, and US House have no direct race-level
    polling. For those three offices, this module's output is the SOLE
    environment estimate feeding simulate.py's statewide_d input, applied
    uniformly across all districts the same way a manually-chosen
    --statewide-d value is today.

Why the generic ballot over presidential approval: research on midterm
forecasting models (e.g. Sabato's Crystal Ball write-ups on House seat-swing
models) finds that once the generic congressional ballot is known, adding
presidential approval doesn't meaningfully improve predictive accuracy — the
estimated coefficient is small and not significant. So this module prefers
the generic ballot and only falls back to approval (with a cruder, more
heavily-discounted conversion) if generic-ballot data is unavailable.

Output shape: (summary, samples) — the same two-piece contract as
src/governor.py::build_baseline_distribution(), so a caller can feed
`samples` to governor.summarize_distribution() either way. The summary
dataclass here is its own type (EnvironmentPrior), not governor.py's
StatewideBaseline — that dataclass's `cycles`/`n_cycles` fields describe
historical Ohio election years, which don't apply to a polling-based
national translation.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from src.fundamentals import (
    NationalSignal,
    national_approval_average,
    national_generic_ballot_average,
)

# ---------------------------------------------------------------------------
# Ohio vs. national offset — a cited reference constant, NOT a fit regression
# ---------------------------------------------------------------------------

# Ohio's two-party presidential vote share (data/processed/year_baselines.json,
# this project's own composite-lean statewide baselines) vs. the certified
# national two-party popular vote share the same cycle (public record,
# verified against final certified vote totals):
#   2020: Ohio 45.92%  vs. national 52.27% (Biden 81,283,501 / Trump
#         74,223,975, two-party share)                          -> -6.35 pts
#   2024: Ohio 44.34%  vs. national 49.25% (Harris 75,017,613 / Trump
#         77,302,580, two-party share)                          -> -4.91 pts
# Two data points, mean -5.63 pts. Kept for reference only — every current
# caller of translate_national_to_ohio() (governor, US Senate, State House,
# State Senate, US House, all 2026) is a MIDTERM cycle, so this presidential
# figure was always the wrong-flavor prior for what it was being used for;
# it was the only data available until the midterm figure below existed.
OHIO_VS_NATIONAL_OFFSET_PRESIDENTIAL_MEAN: float = -0.0563
OHIO_VS_NATIONAL_OFFSET_PRESIDENTIAL_SIGMA: float = 0.02

# Ohio's own midterm reference-race D share (AG/Auditor/SOS/Treasurer mean —
# src.composite.REFERENCE_MIDTERM_RACES, src.composite.reference_share()) vs.
# the certified national U.S. House two-party popular vote, each midterm
# cycle. Ohio shares read from data/processed/block_votes_{year}.parquet
# (Session 10 block backbone — already ingested, not newly collected).
# National shares are certified House vote totals (public record):
#   2010: Ohio 45.68% vs. national 46.51% (D 38,980,192 / R 44,829,751)  -> -0.83 pts
#   2014: Ohio 39.83% vs. national 47.06% (D 35,624,357 / R 40,081,282)  -> -7.23 pts
#   2018: Ohio 47.71% vs. national 54.36% (D 60,572,245 / R 50,861,970)  -> -6.65 pts
#   2022: Ohio 40.68% vs. national 48.60% (D 51,280,463 / R 54,227,992)  -> -7.92 pts
# n=4, mean -5.66 pts, sample std 3.26 pts (statistics.stdev, n-1). The mean
# lands almost exactly on the old presidential-only figure (-5.63 vs -5.66)
# — the point estimate wasn't wrong — but the true spread is far wider than
# the 2-pt padding previously assumed: 2010 (a nationalized wave election)
# shows almost no Ohio/national gap at all, while 2014/2018/2022 all show
# Ohio running 6.6-7.9 pts more Republican than the national environment.
# A single national environment reading translates to a much less certain
# Ohio number than the old sigma implied. Still only 4 points — not a fit,
# still a documented judgment call — but a measured one instead of an
# assumed one.
OHIO_VS_NATIONAL_OFFSET_MIDTERM_MEAN: float = -0.0566
OHIO_VS_NATIONAL_OFFSET_MIDTERM_SIGMA: float = 0.0326

# Used by translate_national_to_ohio() — every current caller is a midterm
# cycle (see above), so this is the one actually applied on the live path.
OHIO_VS_NATIONAL_OFFSET_MEAN: float = OHIO_VS_NATIONAL_OFFSET_MIDTERM_MEAN
OHIO_VS_NATIONAL_OFFSET_SIGMA: float = OHIO_VS_NATIONAL_OFFSET_MIDTERM_SIGMA
OHIO_VS_NATIONAL_OFFSET_SOURCE: str = (
    "Ohio midterm reference-race two-party share (AG/Auditor/SOS/Treasurer "
    "mean, from data/processed/block_votes_{year}.parquet) vs. certified "
    "national U.S. House two-party popular vote, 2010/2014/2018/2022 "
    "(-0.83, -7.23, -6.65, -7.92 pts). Four data points — a measured "
    "spread, not a fit coefficient. See OHIO_VS_NATIONAL_OFFSET_PRESIDENTIAL_* "
    "for the (very similar-mean, much narrower-sigma) presidential-cycle figure."
)


@dataclass(frozen=True)
class EnvironmentPrior:
    """Summary of the national-fundamentals-derived Ohio environment prior."""

    national_value: float
    national_sigma: float
    national_source: str  # "generic_ballot" | "approval_fallback"
    n_polls: int
    ohio_value: float
    ohio_sigma: float
    as_of: pd.Timestamp
    method: str  # "national_fundamentals_prior"


# ---------------------------------------------------------------------------
# Translation
# ---------------------------------------------------------------------------

def translate_national_to_ohio(
    national_value: float,
    national_sigma: float,
) -> tuple[float, float]:
    """
    Apply the Ohio-vs-national offset to a national environment estimate.

    Widens sigma in quadrature by OHIO_VS_NATIONAL_OFFSET_SIGMA to reflect
    that the offset itself is a measured spread across four midterm cycles
    (2010/2014/2018/2022), not a fit — see OHIO_VS_NATIONAL_OFFSET_MIDTERM_*
    above for the derivation.
    """
    ohio_value = national_value + OHIO_VS_NATIONAL_OFFSET_MEAN
    ohio_sigma = float(np.sqrt(national_sigma**2 + OHIO_VS_NATIONAL_OFFSET_SIGMA**2))
    return ohio_value, ohio_sigma


def _approval_fallback_signal(as_of: pd.Timestamp) -> NationalSignal:
    """
    Crude fallback when generic-ballot data is unavailable: treat Trump
    disapproval share as a rough stand-in for the opposition (D) party's
    national environment (a "referendum" heuristic — common in punditry,
    not a fit or literature-sourced coefficient). Sigma is inflated 1.5x on
    top of the poll-level uncertainty to reflect that this conversion itself
    is unvalidated, per the research finding in the module docstring that
    approval alone is a weak proxy once the generic ballot is available.
    """
    approval = national_approval_average(as_of=as_of)
    referendum_d_share = 1.0 - approval.value
    return NationalSignal(
        value=referendum_d_share,
        sigma=approval.sigma * 1.5,
        n_polls=approval.n_polls,
        as_of=approval.as_of,
        source="approval_fallback",
    )


def build_environment_prior(
    as_of: pd.Timestamp | None = None,
    n_samples: int = 10_000,
    seed: int = 42,
) -> tuple[EnvironmentPrior, np.ndarray]:
    """
    Build a posterior over 2026 Ohio statewide two-party D share from
    national fundamentals alone — no Ohio-specific polling (see module
    docstring for why this is a PRIOR, not a final forecast, for governor
    and US Senate specifically, but the sole estimate for the other three
    offices).

    Prefers the generic congressional ballot; falls back to presidential
    approval (flagged via `EnvironmentPrior.national_source`) if
    data/polls/generic_ballot.csv is unavailable.
    """
    if as_of is None:
        as_of = pd.Timestamp.now().normalize()

    try:
        signal = national_generic_ballot_average(as_of=as_of)
    except FileNotFoundError:
        signal = _approval_fallback_signal(as_of)

    ohio_value, ohio_sigma = translate_national_to_ohio(signal.value, signal.sigma)

    summary = EnvironmentPrior(
        national_value=signal.value,
        national_sigma=signal.sigma,
        national_source=signal.source,
        n_polls=signal.n_polls,
        ohio_value=ohio_value,
        ohio_sigma=ohio_sigma,
        as_of=as_of,
        method="national_fundamentals_prior",
    )

    rng = np.random.default_rng(seed)
    samples = rng.normal(loc=ohio_value, scale=ohio_sigma, size=n_samples)
    samples = np.clip(samples, 0.20, 0.80)
    return summary, samples
