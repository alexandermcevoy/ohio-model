"""
compute.py — Thin wrappers around src.simulate for live GUI computation.

These functions are called when the statewide D% slider changes,
enabling instant recomputation of win probabilities and investment rankings.
"""

from __future__ import annotations

import pandas as pd


def live_win_probs(
    targeting_df: pd.DataFrame,
    sigma_df: pd.DataFrame,
    statewide_d: float,
) -> pd.DataFrame:
    """
    Compute analytical win probabilities at an arbitrary statewide D%.

    Returns DataFrame with: district, composite_lean, margin, sigma_i,
        inc_shift, win_prob, marginal_wp
    Runs in < 1ms for 99 districts (vectorized normal CDF).
    """
    from src.simulate import compute_analytical_win_probs, SimConfig
    return compute_analytical_win_probs(targeting_df, sigma_df, statewide_d, SimConfig())


def live_investment_priority(
    targeting_df: pd.DataFrame,
    sigma_df: pd.DataFrame,
    statewide_d: float,
) -> pd.DataFrame:
    """Recompute investment ranking at a new statewide D%."""
    from src.simulate import build_investment_priority, SimConfig
    return build_investment_priority(targeting_df, sigma_df, statewide_d, SimConfig())


def classify_portfolio(win_probs_df: pd.DataFrame) -> pd.DataFrame:
    """
    Classify R-held pickup targets into Core / Stretch / Long-Shot tiers.

    Logic (from Session 9 portfolio framing):
    - Core: win_prob >= 0.25 at 46% statewide D
    - Stretch: win_prob >= 0.25 at 48% but not Core
    - Long-Shot: not Core and not Stretch, but win_prob >= 0.10 at 50%

    Returns DataFrame: district, portfolio_tier
    """
    # Get win probs at the three reference environments
    wp_46 = win_probs_df[win_probs_df["statewide_d_pct"] == 46.0].set_index("district")["win_prob"]
    wp_48 = win_probs_df[win_probs_df["statewide_d_pct"] == 48.0].set_index("district")["win_prob"]
    wp_50 = win_probs_df[win_probs_df["statewide_d_pct"] == 50.0].set_index("district")["win_prob"]

    districts = wp_46.index.union(wp_48.index).union(wp_50.index)
    result = []
    for d in districts:
        p46 = wp_46.get(d, 0)
        p48 = wp_48.get(d, 0)
        p50 = wp_50.get(d, 0)
        if p46 >= 0.25:
            tier = "Core"
        elif p48 >= 0.25:
            tier = "Stretch"
        elif p50 >= 0.10:
            tier = "Long-Shot"
        else:
            tier = None  # not a pickup target
        if tier is not None:
            result.append({"district": d, "portfolio_tier": tier})

    return pd.DataFrame(result)
