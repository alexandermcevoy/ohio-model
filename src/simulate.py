"""
simulate.py — Stochastic swing model with district-level uncertainty.

Monte Carlo engine: draws statewide environment + district-specific noise,
counts D seats per simulation, produces distributional outputs.

All simulations condition on composite_lean as the anchor metric.
The probabilistic layer adds noise around it; it does not replace it.

Session 8. March 2026.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from dataclasses import dataclass, field
from scipy.stats import norm

from src.constants import (
    DEFAULT_N_SIMS,
    INCUMBENCY_SD,
    INVESTMENT_DELTA,
    LITERATURE_INCUMBENCY_ADVANTAGE,
    SHRINKAGE_WEIGHTS,
)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class SimConfig:
    """Simulation parameters."""

    n_sims: int = DEFAULT_N_SIMS
    include_incumbency: bool = False
    incumbency_advantage: float = LITERATURE_INCUMBENCY_ADVANTAGE
    incumbency_sd: float = INCUMBENCY_SD
    shrinkage_weights: dict[int, float] = field(default_factory=lambda: dict(SHRINKAGE_WEIGHTS))
    investment_delta: float = INVESTMENT_DELTA
    random_seed: int = 42


# ---------------------------------------------------------------------------
# Result container
# ---------------------------------------------------------------------------

@dataclass
class SimResult:
    """Container for simulation outputs at a single statewide environment."""

    statewide_d: float
    n_sims: int
    config: SimConfig

    # Per-district win probabilities: DataFrame with district, win_prob
    district_win_prob: pd.DataFrame

    # Seat distribution across simulations
    seat_counts: np.ndarray  # shape (n_sims,)
    mean_seats: float
    median_seats: int
    p10_seats: int
    p25_seats: int
    p75_seats: int
    p90_seats: int
    std_seats: float


# ---------------------------------------------------------------------------
# District uncertainty estimation (Empirical Bayes shrinkage)
# ---------------------------------------------------------------------------

def compute_sigma_prior(composite_lean_df: pd.DataFrame) -> float:
    """
    Compute the statewide prior for district-level outcome noise.

    Uses pooled std of candidate_effect across all contested district-years.
    Falls back to a conservative default (0.06) if insufficient data.
    """
    effect_cols = [c for c in composite_lean_df.columns if c.startswith("candidate_effect_")]
    contested_cols = [c for c in composite_lean_df.columns if c.startswith("contested_")]

    effects = []
    for ecol in effect_cols:
        year = ecol.replace("candidate_effect_", "")
        ccol = f"contested_{year}"
        if ccol in composite_lean_df.columns:
            mask = composite_lean_df[ccol].fillna(False)
            vals = composite_lean_df.loc[mask, ecol].dropna()
            effects.extend(vals.tolist())

    if len(effects) < 10:
        return 0.06  # conservative fallback

    return float(np.std(effects, ddof=1))


def estimate_district_sigma(
    targeting_df: pd.DataFrame,
    sigma_prior: float,
    config: SimConfig | None = None,
) -> pd.DataFrame:
    """
    Compute per-district outcome uncertainty (sigma_i) using EB shrinkage.

    Parameters
    ----------
    targeting_df : DataFrame with columns: district, swing_sd, n_contested
    sigma_prior : statewide prior std from compute_sigma_prior()
    config : simulation config (for shrinkage weights)

    Returns
    -------
    DataFrame with columns: district, sigma_i, sigma_source
    """
    if config is None:
        config = SimConfig()

    rows = []
    for _, row in targeting_df.iterrows():
        n = int(row.get("n_contested", 0))
        swing_sd = row.get("swing_sd", float("nan"))
        weight = config.shrinkage_weights.get(n, 0.0)

        if pd.isna(swing_sd) or weight == 0.0:
            sigma_i = sigma_prior
            source = "prior_only"
        else:
            sigma_i = np.sqrt(weight * swing_sd**2 + (1 - weight) * sigma_prior**2)
            source = f"shrunk_{n}pt"

        rows.append({
            "district": int(row["district"]),
            "sigma_i": sigma_i,
            "sigma_source": source,
        })

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Incumbency shift computation
# ---------------------------------------------------------------------------

def compute_incumbency_shifts(
    targeting_df: pd.DataFrame,
    config: SimConfig,
) -> pd.Series:
    """
    Compute per-district incumbency shift for 2026 projections.

    Only applied to districts with confirmed incumbent status.
    Returns a Series indexed by district number.
    """
    shifts = pd.Series(0.0, index=targeting_df["district"])

    if not config.include_incumbency:
        return shifts

    for _, row in targeting_df.iterrows():
        d = int(row["district"])
        status = row.get("incumbent_status_2026", "unknown")
        holder = row.get("current_holder", "")

        if status == "true_incumbent":
            if holder == "R":
                shifts[d] = -config.incumbency_advantage  # R incumbent depresses D share
            elif holder == "D":
                shifts[d] = +config.incumbency_advantage   # D incumbent boosts D share
        # open_seat and unknown: 0.0

    return shifts


# ---------------------------------------------------------------------------
# Simulation engine
# ---------------------------------------------------------------------------

def run_simulations(
    targeting_df: pd.DataFrame,
    sigma_df: pd.DataFrame,
    statewide_d: float,
    config: SimConfig | None = None,
) -> SimResult:
    """
    Run N Monte Carlo simulations at a fixed statewide D share.

    For each simulation:
      1. Draw district-specific noise: epsilon_i ~ N(0, sigma_i)
      2. Optionally draw incumbency noise per sim
      3. District D share = statewide_d + composite_lean_i + inc_i + epsilon_i
      4. District goes D if d_share > 0.50
      5. Count total D seats

    Parameters
    ----------
    targeting_df : must contain columns: district, composite_lean, current_holder,
                   incumbent_status_2026
    sigma_df : from estimate_district_sigma(), columns: district, sigma_i
    statewide_d : statewide D two-party share (e.g. 0.48)
    config : simulation config
    """
    if config is None:
        config = SimConfig()

    rng = np.random.default_rng(config.random_seed)

    # Align data
    merged = targeting_df[["district", "composite_lean"]].merge(
        sigma_df[["district", "sigma_i"]], on="district"
    ).sort_values("district").reset_index(drop=True)

    n_districts = len(merged)
    leans = merged["composite_lean"].values
    sigmas = merged["sigma_i"].values

    # Incumbency shifts (deterministic component)
    inc_shifts = compute_incumbency_shifts(targeting_df, config)
    inc_base = inc_shifts.reindex(merged["district"]).values

    # Monte Carlo draws
    # epsilon: (n_sims, n_districts)
    epsilon = rng.normal(0.0, 1.0, size=(config.n_sims, n_districts)) * sigmas[np.newaxis, :]

    # Incumbency noise (if enabled, add per-sim noise around the literature value)
    if config.include_incumbency:
        inc_noise = rng.normal(0.0, config.incumbency_sd, size=(config.n_sims, n_districts))
        # Only apply noise to districts with nonzero incumbency shift
        inc_mask = (inc_base != 0.0).astype(float)
        inc_total = inc_base[np.newaxis, :] + inc_noise * inc_mask[np.newaxis, :]
    else:
        inc_total = np.zeros((config.n_sims, n_districts))

    # District D share per simulation
    d_shares = statewide_d + leans[np.newaxis, :] + inc_total + epsilon

    # D wins if d_share >= 0.50 (matches deterministic convention: flip_threshold <= sw_d)
    d_wins = d_shares >= 0.50

    # Per-district win probability
    win_probs = d_wins.mean(axis=0)

    district_wp = pd.DataFrame({
        "district": merged["district"].values,
        "win_prob": win_probs,
    })

    # Seat distribution
    seat_counts = d_wins.sum(axis=1)

    return SimResult(
        statewide_d=statewide_d,
        n_sims=config.n_sims,
        config=config,
        district_win_prob=district_wp,
        seat_counts=seat_counts,
        mean_seats=float(seat_counts.mean()),
        median_seats=int(np.median(seat_counts)),
        p10_seats=int(np.percentile(seat_counts, 10)),
        p25_seats=int(np.percentile(seat_counts, 25)),
        p75_seats=int(np.percentile(seat_counts, 75)),
        p90_seats=int(np.percentile(seat_counts, 90)),
        std_seats=float(seat_counts.std()),
    )


# ---------------------------------------------------------------------------
# Scenario sweep
# ---------------------------------------------------------------------------

def run_probabilistic_scenario_table(
    targeting_df: pd.DataFrame,
    sigma_df: pd.DataFrame,
    d_range: tuple[float, float] = (0.40, 0.55),
    step: float = 0.005,
    config: SimConfig | None = None,
) -> tuple[pd.DataFrame, dict[float, SimResult]]:
    """
    Run simulations at each statewide D share in the range.

    Returns
    -------
    scenario_df : DataFrame with columns: statewide_d_pct, mean_d_seats,
        median_d_seats, p10_seats, p25_seats, p75_seats, p90_seats, std_seats,
        prob_hold_34, prob_reach_40, prob_majority
    results : dict mapping statewide_d → SimResult (for downstream use)
    """
    if config is None:
        config = SimConfig()

    points = np.arange(d_range[0], d_range[1] + step / 2, step)
    rows = []
    results = {}

    for sw_d in points:
        # Use a different seed per point so they're independent but reproducible
        point_config = SimConfig(
            n_sims=config.n_sims,
            include_incumbency=config.include_incumbency,
            incumbency_advantage=config.incumbency_advantage,
            incumbency_sd=config.incumbency_sd,
            shrinkage_weights=config.shrinkage_weights,
            investment_delta=config.investment_delta,
            random_seed=config.random_seed + int(round(sw_d * 10000)),
        )

        result = run_simulations(targeting_df, sigma_df, sw_d, point_config)
        results[round(sw_d, 4)] = result

        rows.append({
            "statewide_d_pct": round(sw_d * 100, 1),
            "mean_d_seats": round(result.mean_seats, 1),
            "median_d_seats": result.median_seats,
            "p10_seats": result.p10_seats,
            "p25_seats": result.p25_seats,
            "p75_seats": result.p75_seats,
            "p90_seats": result.p90_seats,
            "std_seats": round(result.std_seats, 2),
            "prob_hold_34": round(float((result.seat_counts >= 34).mean()), 4),
            "prob_reach_40": round(float((result.seat_counts >= 40).mean()), 4),
            "prob_majority": round(float((result.seat_counts >= 50).mean()), 4),
        })

    return pd.DataFrame(rows), results


# ---------------------------------------------------------------------------
# Analytical win probability and marginal value
# ---------------------------------------------------------------------------

def compute_analytical_win_probs(
    targeting_df: pd.DataFrame,
    sigma_df: pd.DataFrame,
    statewide_d: float,
    config: SimConfig | None = None,
) -> pd.DataFrame:
    """
    Compute per-district win probability and marginal WP analytically.

    Uses the normal CDF: P(win) = Φ(margin / sigma_i)
    Marginal WP = φ(margin / sigma_i) / sigma_i

    Returns DataFrame: district, composite_lean, margin, sigma_i,
        win_prob, marginal_wp
    """
    if config is None:
        config = SimConfig()

    merged = targeting_df[["district", "composite_lean"]].merge(
        sigma_df[["district", "sigma_i"]], on="district"
    )

    inc_shifts = compute_incumbency_shifts(targeting_df, config)
    merged["inc_shift"] = merged["district"].map(inc_shifts).fillna(0.0)

    merged["margin"] = statewide_d + merged["composite_lean"] + merged["inc_shift"] - 0.50
    merged["z_score"] = merged["margin"] / merged["sigma_i"]
    merged["win_prob"] = norm.cdf(merged["z_score"])
    merged["marginal_wp"] = norm.pdf(merged["z_score"]) / merged["sigma_i"]

    return merged[["district", "composite_lean", "margin", "sigma_i",
                    "inc_shift", "win_prob", "marginal_wp"]].copy()


def build_investment_priority(
    targeting_df: pd.DataFrame,
    sigma_df: pd.DataFrame,
    statewide_d: float,
    config: SimConfig | None = None,
) -> pd.DataFrame:
    """
    Rank districts by marginal win probability (bang for buck).

    Includes targeting context columns for operational use.
    """
    wp_df = compute_analytical_win_probs(targeting_df, sigma_df, statewide_d, config)

    # Merge context columns
    context_cols = ["district", "tier", "open_seat_2026", "current_holder",
                    "incumbent_status_2026"]
    # Add target_mode — could be voterfile or aggregate
    for col in ["target_mode_voterfile", "target_mode"]:
        if col in targeting_df.columns:
            context_cols.append(col)
            break
    # Add voter file counts if available
    for col in ["n_mobilization_targets", "n_persuasion_targets"]:
        if col in targeting_df.columns:
            context_cols.append(col)

    available = [c for c in context_cols if c in targeting_df.columns]
    result = wp_df.merge(targeting_df[available], on="district", how="left")

    # Rank by marginal WP (highest first)
    result["investment_rank"] = result["marginal_wp"].rank(ascending=False, method="min").astype(int)
    result = result.sort_values("investment_rank")

    return result


# ---------------------------------------------------------------------------
# Path-to-target optimizer
# ---------------------------------------------------------------------------

def optimize_path_to_target(
    targeting_df: pd.DataFrame,
    sigma_df: pd.DataFrame,
    statewide_d: float,
    target_seats: int = 40,
    max_districts: int = 15,
    config: SimConfig | None = None,
) -> pd.DataFrame:
    """
    Greedy optimizer: which districts to invest in to maximize P(target_seats)?

    Algorithm:
      1. Compute baseline win probabilities for all districts.
      2. Select the R-held district with highest marginal WP.
      3. "Invest" = shift its effective lean by +investment_delta.
      4. Recompute marginal WPs. Repeat until max_districts reached.

    Returns DataFrame: priority_rank, district, baseline_wp, invested_wp,
        marginal_gain, cumulative_expected_seats, cumulative_prob_target
    """
    if config is None:
        config = SimConfig()

    delta = config.investment_delta

    # Work with mutable lean adjustments
    lean_adj = pd.Series(0.0, index=targeting_df["district"].values)

    # Candidate districts: R-held or competitive
    r_held = set(targeting_df.loc[targeting_df["current_holder"] == "R", "district"])

    rows = []
    invested_districts = set()

    for step in range(max_districts):
        # Build adjusted targeting df
        adj_df = targeting_df.copy()
        adj_df["composite_lean"] = adj_df["composite_lean"] + adj_df["district"].map(lean_adj)

        # Compute analytical win probs
        wp_df = compute_analytical_win_probs(adj_df, sigma_df, statewide_d, config)

        # Expected seats
        expected_seats = wp_df["win_prob"].sum()

        # Prob of reaching target (use MC for this single point)
        point_config = SimConfig(
            n_sims=config.n_sims,
            include_incumbency=config.include_incumbency,
            incumbency_advantage=config.incumbency_advantage,
            incumbency_sd=config.incumbency_sd,
            shrinkage_weights=config.shrinkage_weights,
            random_seed=config.random_seed + step * 1000,
        )
        sim_result = run_simulations(adj_df, sigma_df, statewide_d, point_config)
        prob_target = float((sim_result.seat_counts >= target_seats).mean())

        # Find best uninvested R-held district by marginal WP
        candidates = wp_df[
            (wp_df["district"].isin(r_held)) &
            (~wp_df["district"].isin(invested_districts))
        ]

        if candidates.empty:
            break

        best = candidates.loc[candidates["marginal_wp"].idxmax()]
        best_district = int(best["district"])
        baseline_wp = float(best["win_prob"])

        # Invest: shift lean by delta
        lean_adj[best_district] += delta
        invested_districts.add(best_district)

        # Compute new win prob for this district
        adj_df2 = targeting_df.copy()
        adj_df2["composite_lean"] = adj_df2["composite_lean"] + adj_df2["district"].map(lean_adj)
        wp_df2 = compute_analytical_win_probs(adj_df2, sigma_df, statewide_d, config)
        invested_wp = float(wp_df2.loc[wp_df2["district"] == best_district, "win_prob"].iloc[0])
        new_expected = wp_df2["win_prob"].sum()

        rows.append({
            "priority_rank": step + 1,
            "district": best_district,
            "baseline_wp": round(baseline_wp, 4),
            "invested_wp": round(invested_wp, 4),
            "marginal_gain": round(new_expected - expected_seats, 4),
            "cumulative_expected_seats": round(new_expected, 2),
            "cumulative_prob_target": round(prob_target, 4),
        })

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Defensive scenarios
# ---------------------------------------------------------------------------

def run_defensive_scenarios(
    targeting_df: pd.DataFrame,
    sigma_df: pd.DataFrame,
    statewide_d_range: tuple[float, float] = (0.43, 0.48),
    step: float = 0.005,
    config: SimConfig | None = None,
) -> pd.DataFrame:
    """
    For D-held seats: P(hold) at each statewide environment.

    Returns DataFrame: district, statewide_d_pct, prob_hold, risk_level
    """
    if config is None:
        config = SimConfig()

    d_held = targeting_df.loc[targeting_df["current_holder"] == "D", "district"].tolist()
    if not d_held:
        return pd.DataFrame(columns=["district", "statewide_d_pct", "prob_hold", "risk_level"])

    points = np.arange(statewide_d_range[0], statewide_d_range[1] + step / 2, step)
    rows = []

    for sw_d in points:
        wp_df = compute_analytical_win_probs(targeting_df, sigma_df, sw_d, config)
        for _, row in wp_df[wp_df["district"].isin(d_held)].iterrows():
            prob_hold = row["win_prob"]
            if prob_hold < 0.60:
                risk = "high"
            elif prob_hold < 0.80:
                risk = "moderate"
            else:
                risk = "low"

            rows.append({
                "district": int(row["district"]),
                "statewide_d_pct": round(sw_d * 100, 1),
                "prob_hold": round(prob_hold, 4),
                "risk_level": risk,
            })

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# District win probability curve (across statewide environments)
# ---------------------------------------------------------------------------

def district_win_prob_curve(
    targeting_df: pd.DataFrame,
    sigma_df: pd.DataFrame,
    district: int,
    d_range: tuple[float, float] = (0.40, 0.55),
    step: float = 0.005,
    config: SimConfig | None = None,
) -> pd.DataFrame:
    """
    Win probability for a single district across statewide environments.

    Returns DataFrame: statewide_d_pct, win_prob, margin
    """
    points = np.arange(d_range[0], d_range[1] + step / 2, step)
    rows = []

    for sw_d in points:
        wp_df = compute_analytical_win_probs(targeting_df, sigma_df, sw_d, config)
        row = wp_df[wp_df["district"] == district]
        if row.empty:
            continue
        rows.append({
            "statewide_d_pct": round(sw_d * 100, 1),
            "win_prob": round(float(row["win_prob"].iloc[0]), 4),
            "margin": round(float(row["margin"].iloc[0]), 4),
        })

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Full district win prob table (all districts × all environments)
# ---------------------------------------------------------------------------

def build_district_win_prob_table(
    targeting_df: pd.DataFrame,
    sigma_df: pd.DataFrame,
    d_range: tuple[float, float] = (0.40, 0.55),
    step: float = 0.005,
    config: SimConfig | None = None,
) -> pd.DataFrame:
    """
    Win probabilities for all 99 districts at every statewide environment.

    Returns DataFrame: district, statewide_d_pct, win_prob, sigma_i,
        sigma_source, incumbency_shift
    """
    if config is None:
        config = SimConfig()

    points = np.arange(d_range[0], d_range[1] + step / 2, step)
    all_rows = []

    # Pre-compute static columns
    inc_shifts = compute_incumbency_shifts(targeting_df, config)

    for sw_d in points:
        wp_df = compute_analytical_win_probs(targeting_df, sigma_df, sw_d, config)
        wp_df["statewide_d_pct"] = round(sw_d * 100, 1)

        # Add sigma source and incumbency shift
        wp_merged = wp_df.merge(
            sigma_df[["district", "sigma_source"]], on="district", how="left"
        )
        wp_merged["incumbency_shift"] = wp_merged["district"].map(inc_shifts).fillna(0.0)

        all_rows.append(wp_merged[["district", "statewide_d_pct", "win_prob",
                                    "sigma_i", "sigma_source", "incumbency_shift"]])

    result = pd.concat(all_rows, ignore_index=True)
    result["win_prob"] = result["win_prob"].round(4)
    result["incumbency_shift"] = result["incumbency_shift"].round(4)
    return result
