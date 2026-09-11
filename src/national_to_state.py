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
# Two data points only, mean -5.63 pts. This is NOT a regression — it's a
# small, documented reference range in the same spirit as
# LITERATURE_INCUMBENCY_ADVANTAGE (src/constants.py): a point estimate with
# deliberately wide uncertainty, to be replaced once more cycles or a real
# backtest exist. Midterm-year (non-presidential) national vote share is a
# different, not-yet-collected number — see CLAUDE.md Known Limitations.
OHIO_VS_NATIONAL_OFFSET_MEAN: float = -0.0563
OHIO_VS_NATIONAL_OFFSET_SIGMA: float = 0.02
OHIO_VS_NATIONAL_OFFSET_SOURCE: str = (
    "Ohio two-party presidential share (data/processed/year_baselines.json, "
    "2020 & 2024) vs. certified national two-party popular vote share the "
    "same cycles (-6.35 and -4.91 pts). Two data points only — treat as a "
    "wide prior, not a fit coefficient."
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
    that the offset itself is estimated from only two cycles, not fit.
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
