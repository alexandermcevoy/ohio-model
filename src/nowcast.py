"""
nowcast.py — election-night live posterior update (Phase B extension).

The pre-election model already defines a full joint distribution over every
district's final D two-party share: src/simulate.py::run_simulations() draws
share_i = statewide_d + composite_lean_i + inc_i + eta + epsilon_i, where eta
(the year-level shared shock, sigma_shared) is drawn ONCE per simulation and
applied to every district, and epsilon_i (sigma_i) is independent per
district. That is a multivariate normal over all districts' shares — it has
just only ever been *sampled* from, for one-shot pre-election forecasting.

This module makes that joint distribution explicit (build_prior) so it can
instead be *conditioned* on live partial returns as they come in on election
night (update_posterior) — standard Gaussian conditioning, a single
Kalman-filter step. Because every district shares the same eta/environment
covariance term, a partial result in one district updates every other
district's posterior too, not just the one reporting — this is the
mechanism, not an approximation of it.

Deliberately manual: this module takes vote totals the user types in (having
read them off NYT, VoteHub, county results, etc.), not a live data feed. No
scraping, no new data source — see the design discussion that led here.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
from scipy.stats import norm


@dataclass(frozen=True)
class NowcastConfig:
    """
    Observation-noise judgment calls — NOT fitted. There is no historical
    archive of minute-by-minute Ohio State House reporting to calibrate
    against, same category as MIN_OHIO_SIGMA / CANDIDATE_IDIOSYNCRASY_SIGMA
    elsewhere in this project: a documented default, not a derived constant.

    tau_i(f)^2 = tau_floor^2 + tau_scale^2 * (1 - f)^2, where f is the
    fraction of a district's expected total vote currently counted. tau_floor
    is a residual floor even near f=1, since "99% reporting" from any feed is
    itself an estimate (late/provisional ballots aren't in it) — the only way
    a district's uncertainty truly collapses to ~0 is is_final=True.
    """

    tau_floor: float = 0.01
    tau_scale: float = 0.175
    call_threshold: float = 0.99  # win_prob >= this (or <= 1 - this) is "callable"


@dataclass(frozen=True)
class LiveObservation:
    """One district's current live return, as typed in by the user."""

    district: int
    dem_votes: int
    rep_votes: int
    is_final: bool = False


# ---------------------------------------------------------------------------
# Expected total turnout per district (denominator for "fraction reported")
# ---------------------------------------------------------------------------

def expected_total_votes(composite_lean_df: pd.DataFrame) -> pd.Series:
    """
    Expected total two-party vote for each district in the upcoming midterm,
    used only to convert a partial vote count into a reporting fraction.

    Uses the most recent COMPARABLE (midterm) cycle's actual two-party vote
    total, 2022 dem_votes_2022 + rep_votes_2022 — 2026 is a midterm, and
    turnout composition in a midterm is closer to another midterm than to a
    presidential year (the same reasoning behind REFERENCE_MIDTERM_RACES in
    composite.py). Falls back to 2018's midterm total where 2022 is missing
    or zero (redistricting-filtered districts — see
    apply_redistricting_filter() / years_reliable in redistricting_overlap.csv).
    Districts with neither get NaN; callers treat that as "can't estimate
    reporting fraction," not zero.
    """
    df = composite_lean_df.set_index("district")

    def _total(year: int) -> pd.Series:
        d_col, r_col = f"dem_votes_{year}", f"rep_votes_{year}"
        if d_col not in df.columns or r_col not in df.columns:
            return pd.Series(np.nan, index=df.index)
        return df[d_col].fillna(0) + df[r_col].fillna(0)

    total_2022 = _total(2022)
    total_2018 = _total(2018)
    expected = total_2022.where(total_2022 > 0, total_2018)
    expected = expected.where(expected > 0, np.nan)
    return expected


# ---------------------------------------------------------------------------
# Prior: the joint distribution over every district's final share
# ---------------------------------------------------------------------------

def build_prior(
    targeting_df: pd.DataFrame,
    sigma_df: pd.DataFrame,
    statewide_d: float,
    env_sigma: float,
    *,
    with_incumbency: bool = False,
) -> tuple[pd.Series, pd.DataFrame]:
    """
    Build (mean, covariance) for the joint normal over every district's
    final D two-party share, matching run_simulations()'s generative model
    exactly:

      Sigma_ii = sigma_i^2 + sigma_shared^2 + env_sigma^2   (own variance)
      Sigma_ij = sigma_shared^2 + env_sigma^2   for i != j   (shared variance
                 every pair of districts has in common — eta and the
                 environment draw are both common to every district)

    i.e. Sigma = diag(sigma_i^2) + shared_var * ones(n, n): independent noise
    on the diagonal, plus a rank-1 "everyone moves together" term. This is
    the same sigma_shared/sigma_i split already computed for pre-election
    simulation — nothing new is estimated here, it's just assembled as an
    explicit covariance matrix instead of redrawn per Monte Carlo simulation.

    Returns mean and covariance both indexed (and, for covariance, also
    columned) by district number, sorted ascending.
    """
    from src.simulate import SimConfig, compute_incumbency_shifts, shared_sigma_of

    merged = targeting_df[["district", "composite_lean"]].merge(
        sigma_df[["district", "sigma_i"]], on="district"
    ).sort_values("district").reset_index(drop=True)

    n = len(merged)
    districts = merged["district"].to_numpy()

    inc = np.zeros(n)
    if with_incumbency:
        inc_shifts = compute_incumbency_shifts(targeting_df, SimConfig(include_incumbency=True))
        inc = inc_shifts.reindex(merged["district"]).to_numpy()

    mean = statewide_d + merged["composite_lean"].to_numpy() + inc
    sigma_i2 = merged["sigma_i"].to_numpy() ** 2
    sigma_shared = shared_sigma_of(sigma_df)
    shared_var = sigma_shared**2 + env_sigma**2

    cov = np.diag(sigma_i2) + shared_var * np.ones((n, n))

    mean_s = pd.Series(mean, index=districts, name="mean")
    cov_df = pd.DataFrame(cov, index=districts, columns=districts)
    return mean_s, cov_df


# ---------------------------------------------------------------------------
# Posterior: condition on live partial returns
# ---------------------------------------------------------------------------

def update_posterior(
    mean: pd.Series,
    cov: pd.DataFrame,
    observations: list[LiveObservation],
    expected_total: pd.Series,
    config: NowcastConfig | None = None,
) -> tuple[pd.Series, pd.DataFrame]:
    """
    Condition the prior joint distribution on every currently-known live
    return: y_i = share_i + N(0, tau_i^2) (see NowcastConfig). Recomputes
    from the ORIGINAL prior using every current observation each call, rather
    than filtering sequentially — re-entering an updated (larger) vote count
    for a district already on file never double-counts its earlier, noisier
    read alongside the new one.

    Observations with zero total votes (nothing entered yet) are ignored.
    Districts with no expected_total baseline (see expected_total_votes) and
    not marked is_final are treated as maximally uncertain (f=0) rather than
    raising — a district's own reported total is still informative even
    without a turnout denominator, just discounted hard.
    """
    config = config or NowcastConfig()
    districts = mean.index.to_numpy()

    obs_by_district = {
        o.district: o for o in observations if (o.dem_votes + o.rep_votes) > 0
    }
    reporting = [d for d in districts if d in obs_by_district]
    if not reporting:
        return mean, cov

    y = np.empty(len(reporting))
    tau2 = np.empty(len(reporting))
    for i, d in enumerate(reporting):
        o = obs_by_district[d]
        total = o.dem_votes + o.rep_votes
        y[i] = o.dem_votes / total
        if o.is_final:
            tau2[i] = 1e-8
            continue
        exp_total = expected_total.get(d, np.nan)
        f = 0.0 if (pd.isna(exp_total) or exp_total <= 0) else min(total / exp_total, 0.999)
        tau2[i] = config.tau_floor**2 + config.tau_scale**2 * (1 - f) ** 2

    idx = {d: i for i, d in enumerate(districts)}
    rows = [idx[d] for d in reporting]
    C = np.zeros((len(reporting), len(districts)))
    C[np.arange(len(reporting)), rows] = 1.0
    T = np.diag(tau2)

    Sigma = cov.to_numpy()
    m_vec = mean.to_numpy()

    S = C @ Sigma @ C.T + T
    K = Sigma @ C.T @ np.linalg.solve(S, np.eye(len(reporting)))
    m_post = m_vec + K @ (y - C @ m_vec)
    Sigma_post = Sigma - K @ C @ Sigma
    # Numerical hygiene: Sigma_post is PSD by construction, but floating-point
    # error can leave it just barely non-PSD, which breaks a later Cholesky
    # draw in simulate_posterior_seats(). A tiny diagonal jitter is cheap
    # insurance and doesn't meaningfully change any result.
    Sigma_post = Sigma_post + np.eye(len(districts)) * 1e-10

    return (
        pd.Series(m_post, index=districts, name="mean"),
        pd.DataFrame(Sigma_post, index=districts, columns=districts),
    )


# ---------------------------------------------------------------------------
# Reading the posterior
# ---------------------------------------------------------------------------

def win_probs_from_posterior(mean: pd.Series, cov: pd.DataFrame) -> pd.DataFrame:
    """Per-district P(D wins) from the posterior marginal N(mean_i, Sigma_ii)."""
    sigma = np.sqrt(np.clip(np.diag(cov.to_numpy()), 1e-12, None))
    win_prob = 1 - norm.cdf((0.5 - mean.to_numpy()) / sigma)
    return pd.DataFrame({
        "district": mean.index,
        "mean_share": mean.to_numpy(),
        "sigma": sigma,
        "win_prob": win_prob,
    })


def call_status(
    win_probs: pd.DataFrame,
    observations: list[LiveObservation],
    config: NowcastConfig | None = None,
) -> pd.Series:
    """
    "CERTIFIED {D,R}" for districts marked is_final; "CALL {D,R}" where the
    posterior win probability clears config.call_threshold without being
    marked final (the model is confident, not that the count is complete —
    these two are kept distinct); otherwise "too close to call".
    """
    config = config or NowcastConfig()
    final_districts = {o.district for o in observations if o.is_final}

    def label(row) -> str:
        if row["district"] in final_districts:
            return "CERTIFIED D" if row["win_prob"] > 0.5 else "CERTIFIED R"
        if row["win_prob"] >= config.call_threshold:
            return "CALL D"
        if row["win_prob"] <= 1 - config.call_threshold:
            return "CALL R"
        return "too close to call"

    return win_probs.apply(label, axis=1)


def simulate_posterior_seats(
    mean: pd.Series,
    cov: pd.DataFrame,
    n_sims: int = 10_000,
    seed: int = 42,
) -> dict:
    """
    Draw n_sims joint samples from the posterior N(mean, cov) and summarize
    the resulting seat-count distribution — same summary shape as
    simulate.SimResult, just from the conditioned posterior instead of the
    unconditional prior.
    """
    rng = np.random.default_rng(seed)
    draws = rng.multivariate_normal(mean.to_numpy(), cov.to_numpy(), size=n_sims)
    wins = draws >= 0.5
    seat_counts = wins.sum(axis=1)
    return {
        "seat_counts": seat_counts,
        "mean_seats": float(seat_counts.mean()),
        "median_seats": int(np.median(seat_counts)),
        "p10_seats": int(np.percentile(seat_counts, 10)),
        "p90_seats": int(np.percentile(seat_counts, 90)),
        "std_seats": float(seat_counts.std()),
    }
