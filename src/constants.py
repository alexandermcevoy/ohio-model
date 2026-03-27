"""
constants.py — Configurable model constants for the Ohio House Election Model.

Centralized here so all modules reference one value. To update a constant,
change it here and re-run the relevant pipeline commands.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Incumbency advantage
# ---------------------------------------------------------------------------

LITERATURE_INCUMBENCY_ADVANTAGE: float = 0.06
INCUMBENCY_ADVANTAGE_LOW: float = 0.05
INCUMBENCY_ADVANTAGE_HIGH: float = 0.07
"""
Literature-based incumbency advantage for state legislative races (probability scale).

Value  : 0.06 (6 percentage points, symmetric for D and R)
Source : Midpoint of the 5–7% range in state legislative incumbency research.
         Key references:
           - Ansolabehere & Snyder (2002), "The Incumbency Advantage in U.S.
             Elections" — estimate 5–8% depending on office.
           - Fowler & Hall (2014), "Electoral Effects of Partisan Gerrymander-
             ing" — state legislative incumbency ~5–7%.
           - Jacobson (2015), "It's Nothing Personal: The Decline of the
             Incumbency Advantage in U.S. House Elections" (congressional,
             upper bound reference).
         State-level estimates cluster around 5–8% across states and cycles.

Ohio-specific note:
  The Ohio House GLM regression previously estimated:
    Pre-redistricting-fix  (contaminated): R incumbency AME = −6.1%
    Post-redistricting-fix (data-limited): R incumbency AME = −2.1%
  Neither estimate is reliable. The pre-fix value was biased by cross-geography
  contamination (71 districts had zero precinct overlap between 2020 and 2022
  SOS filings). The post-fix value is limited by insufficient within-district
  variation when only 2 post-redistricting cycles are available.

  Use this literature prior until 3+ clean post-redistricting cycles exist.
  Expected update window: after the 2026 and 2028 Ohio House elections.

Sensitivity bounds:
  INCUMBENCY_ADVANTAGE_LOW  = 0.05  (lower end of literature range)
  INCUMBENCY_ADVANTAGE_HIGH = 0.07  (upper end of literature range)
  Both are included in the targeting CSV as flip_threshold_inc_lo and
  flip_threshold_inc_hi so analysts can see how sensitive targeting is
  to the incumbency assumption.

To update with Ohio data:
  1. Run `python cli.py redistricting-fix` after 2028 results are available.
  2. If the OLS R_inc coefficient is stable across the 2022/2024/2026/2028
     panel (delta < 0.01 between 3-cycle and 4-cycle estimates), replace this
     value with the empirical estimate from regression_summary.txt.
  3. Re-run `python cli.py session5` to regenerate district profiles and PDFs.
"""

# ---------------------------------------------------------------------------
# Simulation defaults (Session 8)
# ---------------------------------------------------------------------------

DEFAULT_N_SIMS: int = 10_000
"""Number of Monte Carlo simulations per statewide environment point."""

INCUMBENCY_SD: float = 0.01
"""Uncertainty in the incumbency estimate itself (drawn per simulation)."""

SHRINKAGE_WEIGHTS: dict[int, float] = {
    0: 0.0,   # no contested cycles → pure statewide prior
    1: 0.0,   # 1 cycle → pure prior (single observation is noise)
    2: 0.3,   # 2 cycles → mostly prior, some district signal
    3: 0.7,   # 3 cycles → mostly district signal
    4: 0.85,  # 4 cycles → strong district signal
}
"""
Empirical Bayes shrinkage weights for district-level outcome variance.

Maps n_contested → weight on district-specific swing_sd (vs. statewide prior).
sigma_i = sqrt(w * swing_sd² + (1-w) * sigma_prior²)

Districts with fewer contested cycles shrink more toward the statewide pool,
which is honest about the limited data rather than excluding those districts.
"""

INVESTMENT_DELTA: float = 0.01
"""
Size of the hypothetical lean improvement per investment unit (1 percentage point).
Used in the path-to-target optimizer: each 'investment' shifts a district's
effective lean by this amount. Abstract — not tied to a specific dollar amount.
"""
