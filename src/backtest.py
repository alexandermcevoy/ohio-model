"""
backtest.py — Out-of-sample validation: use pre-2024 data to predict 2024 outcomes.

Builds composite lean from 2016-2022 statewide races only, classifies districts,
estimates uncertainty from pre-2024 house results, and compares predicted win
probabilities against actual 2024 house race outcomes.

Session 12. March 2026.
"""

from __future__ import annotations

import json
import numpy as np
import pandas as pd
from pathlib import Path
from scipy.stats import spearmanr

from src.backbone import (
    build_composite_from_blocks,
    load_block_district_map,
)
from src.classify import (
    assign_tier,
    classify_districts,
    compute_swing_metrics,
    _assign_tier_from_lean,
)
from src.composite import (
    DEFAULT_WEIGHTS,
    merge_composite_with_house_results,
)
from src.ingest_house_results import (
    apply_redistricting_filter,
    combine_house_results,
)
from src.simulate import (
    SimConfig,
    compute_sigma_prior,
    estimate_district_sigma,
    compute_analytical_win_probs,
    run_simulations,
)

REPORTS_DIR = Path("reports/session12")

# Exclude 2024 races, keep relative proportions of 2016-2022 races.
# build_composite() handles weight normalization automatically.
PRE2024_WEIGHTS: dict[tuple[str, str], float] = {
    k: v for k, v in DEFAULT_WEIGHTS.items() if k[0] != "2024"
}


def build_pre2024_composite() -> pd.DataFrame:
    """Build composite lean using only 2016-2022 block vote surfaces."""
    bdm = load_block_district_map("2024")
    return build_composite_from_blocks(
        years=["2016", "2018", "2020", "2022"],
        block_district_map=bdm,
        weights=PRE2024_WEIGHTS,
    )


def load_actual_2024_results() -> pd.DataFrame:
    """Load 2024 house results from the long-format CSV."""
    house = pd.read_csv("reports/session2/oh_house_actual_results.csv")
    return house[house["year"] == 2024].copy()


def build_pre2024_house_long() -> pd.DataFrame:
    """Load pre-2024 house results with redistricting filter applied."""
    house = pd.read_csv("reports/session2/oh_house_actual_results.csv")
    house_pre = house[house["year"] < 2024].copy()

    overlap_path = Path("reports/redistricting_overlap.csv")
    if overlap_path.exists():
        overlap = pd.read_csv(overlap_path)
        house_pre, _ = apply_redistricting_filter(house_pre, overlap)

    return house_pre


def build_pre2024_targeting(
    composite_df: pd.DataFrame,
    house_pre: pd.DataFrame,
    actual_2024: pd.DataFrame,
    sigma_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Build a targeting-like DataFrame from pre-2024 data only.

    Uses 2024 actual winner column only for current_holder derivation
    (ground truth). Composite lean and swing metrics come entirely
    from pre-2024 data.
    """
    winners = actual_2024[["district", "winner"]].rename(
        columns={"winner": "winner_2024"}
    )
    df = composite_df.merge(winners, on="district", how="left")
    df = classify_districts(df, sigma_df=sigma_df)
    df = compute_swing_metrics(df, house_pre)
    df["flip_threshold"] = (0.50 - df["composite_lean"]).round(4)
    df["incumbent_status_2026"] = "unknown"
    return df


def run_backtest() -> dict:
    """
    Full backtest pipeline. Returns dict with all result DataFrames.
    """
    print("=" * 60)
    print("HISTORICAL BACKTEST: Pre-2024 Data -> 2024 Predictions")
    print("=" * 60)

    # Step 1: Build pre-2024 composite
    print("\n--- Step 1: Building pre-2024 composite lean (2016-2022 only) ---")
    composite_pre = build_pre2024_composite()

    # Step 2: Load actual 2024 results (ground truth)
    print("\n--- Step 2: Loading actual 2024 house results ---")
    actual_2024 = load_actual_2024_results()
    n_contested = actual_2024["contested"].sum()
    n_uncontested = (~actual_2024["contested"]).sum()
    print(f"  2024 house races: {len(actual_2024)} total, "
          f"{n_contested} contested, {n_uncontested} uncontested")

    # Step 3: Build pre-2024 house results (for swing metrics)
    print("\n--- Step 3: Building pre-2024 house results (2018-2022, filtered) ---")
    house_pre = build_pre2024_house_long()
    print(f"  Pre-2024 house rows after redistricting filter: {len(house_pre)}")

    # Step 4: Merge composite with pre-2024 house results for candidate effects
    print("\n--- Step 4: Computing candidate effects + district uncertainty ---")
    house_pre_list = [
        house_pre[house_pre["year"] == y] for y in house_pre["year"].unique()
    ]
    house_wide_pre = combine_house_results(house_pre_list)

    all_baselines = json.loads(
        Path("data/processed/year_baselines.json").read_text()
    )
    baselines_pre = {k: v for k, v in all_baselines.items() if k != "2024"}
    available_years = sorted(house_pre["year"].unique().astype(str))

    composite_with_effects = merge_composite_with_house_results(
        composite_pre, house_wide_pre, available_years, baselines_pre,
    )

    # Estimate district uncertainty from pre-2024 data
    sigma_prior = compute_sigma_prior(composite_with_effects)
    print(f"  Sigma prior (pre-2024 candidate effects): {sigma_prior:.4f}")

    # Need swing metrics for sigma estimation — compute on a scratch copy first
    _scratch = compute_swing_metrics(composite_pre.copy(), house_pre)
    sigma_df = estimate_district_sigma(_scratch, sigma_prior)
    print(f"  Sigma range: {sigma_df['sigma_i'].min():.4f} - "
          f"{sigma_df['sigma_i'].max():.4f}")

    # Step 5: Build targeting DF with WP-based tiers
    print("\n--- Step 5: Classifying districts from pre-2024 data (WP-based tiers) ---")
    targeting_pre = build_pre2024_targeting(
        composite_pre, house_pre, actual_2024, sigma_df=sigma_df,
    )

    # Step 6: Run predictions at actual 2024 statewide environment
    statewide_d_2024 = all_baselines["2024"]
    print(f"\n--- Step 6: Running predictions at 2024 statewide D = "
          f"{statewide_d_2024:.4f} ({statewide_d_2024 * 100:.1f}%) ---")

    wp_df = compute_analytical_win_probs(
        targeting_pre, sigma_df, statewide_d_2024
    )

    config = SimConfig(n_sims=10_000, random_seed=42)
    sim_result = run_simulations(
        targeting_pre, sigma_df, statewide_d_2024, config
    )

    # Step 7: Compare predictions vs actual outcomes
    print("\n--- Step 7: Evaluating backtest accuracy ---")

    # Rename wp_df margin (predicted) to avoid collision with actual margin
    wp_renamed = wp_df.rename(columns={"margin": "predicted_margin"})
    eval_df = wp_renamed.merge(
        actual_2024[["district", "winner", "contested", "dem_share", "margin"]],
        on="district", how="left",
    )
    eval_df["actual_d_win"] = eval_df["winner"].str.startswith("D").fillna(False)

    eval_df = eval_df.merge(
        targeting_pre[["district", "tier", "flip_threshold",
                        "n_contested", "swing_sd"]],
        on="district", how="left",
    )

    results = _compute_accuracy_metrics(eval_df, sim_result)
    results["eval_df"] = eval_df
    results["composite_pre"] = composite_pre
    results["targeting_pre"] = targeting_pre
    results["sigma_df"] = sigma_df
    results["sim_result"] = sim_result
    results["statewide_d_2024"] = statewide_d_2024

    # Step 8: Compare pre-2024 composite to full composite
    print("\n--- Step 8: Composite stability (pre-2024 vs full) ---")
    full_composite = pd.read_csv(
        "reports/session2/oh_house_composite_lean.csv"
    )
    comparison = composite_pre[["district", "composite_lean"]].merge(
        full_composite[["district", "composite_lean"]],
        on="district", suffixes=("_pre2024", "_full"),
    )
    diff = comparison["composite_lean_pre2024"] - comparison["composite_lean_full"]
    corr = comparison["composite_lean_pre2024"].corr(
        comparison["composite_lean_full"]
    )
    print(f"  Mean |delta lean|: {diff.abs().mean():.4f}")
    print(f"  Max  |delta lean|: {diff.abs().max():.4f}")
    print(f"  Correlation:       {corr:.6f}")

    # Use lean-based tiers for composite stability comparison (not WP-based,
    # since we're comparing how much the lean itself shifts — not projected outcomes).
    comparison["tier_pre"] = comparison["composite_lean_pre2024"].apply(_assign_tier_from_lean)
    comparison["tier_full"] = comparison["composite_lean_full"].apply(_assign_tier_from_lean)
    n_tier_change = (comparison["tier_pre"] != comparison["tier_full"]).sum()
    print(f"  Tier changes:      {n_tier_change}")
    results["composite_comparison"] = comparison
    results["composite_correlation"] = corr

    return results


def _compute_accuracy_metrics(
    eval_df: pd.DataFrame,
    sim_result,
) -> dict:
    """Compute all accuracy metrics and print summary."""

    all_districts = eval_df.copy()
    contested = eval_df[eval_df["contested"]].copy()

    # 1. Binary prediction accuracy
    all_districts["predicted_d"] = all_districts["win_prob"] > 0.5
    all_districts["correct"] = (
        all_districts["predicted_d"] == all_districts["actual_d_win"]
    )
    overall_accuracy = all_districts["correct"].mean()
    print(f"\n  Overall binary accuracy (99 districts): {overall_accuracy:.1%} "
          f"({all_districts['correct'].sum()}/{len(all_districts)})")

    # 2. Contested-only accuracy
    contested["predicted_d"] = contested["win_prob"] > 0.5
    contested["correct"] = contested["predicted_d"] == contested["actual_d_win"]
    contested_accuracy = contested["correct"].mean()
    print(f"  Contested-only accuracy ({len(contested)} races): "
          f"{contested_accuracy:.1%} "
          f"({contested['correct'].sum()}/{len(contested)})")

    # 3. Misclassifications
    misses = all_districts[~all_districts["correct"]].sort_values("win_prob")
    print(f"\n  Misclassified districts: {len(misses)}")
    if len(misses) > 0:
        print(f"  {'Dist':>5}  {'Tier':>10}  {'WP':>6}  {'Pred':>5}  "
              f"{'Actual':>6}  {'Margin':>8}  {'Contested':>9}")
        for _, row in misses.iterrows():
            pred = "D" if row["predicted_d"] else "R"
            actual = "D" if row["actual_d_win"] else "R"
            margin_str = (f"{row['margin']:+.3f}"
                          if pd.notna(row["margin"]) else "N/A")
            print(f"  {int(row['district']):>5}  {row['tier']:>10}  "
                  f"{row['win_prob']:>6.3f}  {pred:>5}  {actual:>6}  "
                  f"{margin_str:>8}  {str(row['contested']):>9}")

    # 4. Calibration
    print("\n  Calibration (win probability bins):")
    bins = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.01]
    all_districts["wp_bin"] = pd.cut(
        all_districts["win_prob"], bins=bins, right=False
    )
    cal = all_districts.groupby("wp_bin", observed=True).agg(
        n=("district", "count"),
        mean_wp=("win_prob", "mean"),
        actual_win_rate=("actual_d_win", "mean"),
    )
    print(f"  {'Bin':>15}  {'N':>3}  {'Mean WP':>7}  {'Actual':>7}  {'Gap':>6}")
    for bin_label, row in cal.iterrows():
        gap = row["actual_win_rate"] - row["mean_wp"]
        print(f"  {str(bin_label):>15}  {int(row['n']):>3}  "
              f"{row['mean_wp']:>7.3f}  {row['actual_win_rate']:>7.3f}  "
              f"{gap:>+6.3f}")

    # 5. Seat count prediction
    actual_d_seats = int(all_districts["actual_d_win"].sum())
    predicted_d_seats = all_districts["win_prob"].sum()
    mc_mean = sim_result.mean_seats

    print(f"\n  Seat count prediction:")
    print(f"    Actual D seats (2024):          {actual_d_seats}")
    print(f"    Expected (sum of win probs):    {predicted_d_seats:.1f}")
    print(f"    MC mean:                        {mc_mean:.1f}")
    print(f"    MC median:                      {sim_result.median_seats}")
    print(f"    MC 80% CI:                      "
          f"[{sim_result.p10_seats}, {sim_result.p90_seats}]")
    print(f"    MC 50% CI:                      "
          f"[{sim_result.p25_seats}, {sim_result.p75_seats}]")

    in_80ci = sim_result.p10_seats <= actual_d_seats <= sim_result.p90_seats
    in_50ci = sim_result.p25_seats <= actual_d_seats <= sim_result.p75_seats
    print(f"    Actual in 80% CI: {'YES' if in_80ci else 'NO'}")
    print(f"    Actual in 50% CI: {'YES' if in_50ci else 'NO'}")

    # 6. Brier score
    brier = (
        (all_districts["win_prob"] - all_districts["actual_d_win"].astype(float))
        ** 2
    ).mean()
    brier_contested = (
        (contested["win_prob"] - contested["actual_d_win"].astype(float)) ** 2
    ).mean()
    brier_naive = (
        (0.5 - contested["actual_d_win"].astype(float)) ** 2
    ).mean()
    brier_skill = 1 - brier_contested / brier_naive

    print(f"\n  Brier score (all, lower=better):       {brier:.4f}")
    print(f"  Brier score (contested only):          {brier_contested:.4f}")
    print(f"  Brier score (naive 0.5 baseline):      {brier_naive:.4f}")
    print(f"  Brier skill score vs naive:            {brier_skill:.4f}")

    # 7. Log loss
    eps = 1e-6
    wp_clipped = all_districts["win_prob"].clip(eps, 1 - eps)
    y = all_districts["actual_d_win"].astype(float)
    log_loss = -(
        y * np.log(wp_clipped) + (1 - y) * np.log(1 - wp_clipped)
    ).mean()
    print(f"  Log loss (all):                        {log_loss:.4f}")

    # 8. Rank-order accuracy
    contested_with_share = contested.dropna(subset=["dem_share"])
    rho, p_val = None, None
    if len(contested_with_share) > 5:
        rho, p_val = spearmanr(
            contested_with_share["win_prob"],
            contested_with_share["dem_share"],
        )
        print(f"\n  Rank-order (contested, win_prob vs dem_share):")
        print(f"    Spearman rho = {rho:.4f}  (p = {p_val:.2e})")

    # 9. Competitive district accuracy
    competitive = contested[
        contested["tier"].isin(["tossup", "lean_r", "lean_d"])
    ].copy()
    comp_accuracy = None
    if len(competitive) > 0:
        competitive["correct"] = (
            competitive["predicted_d"] == competitive["actual_d_win"]
        )
        comp_accuracy = competitive["correct"].mean()
        print(f"\n  Competitive district accuracy "
              f"({len(competitive)} races): {comp_accuracy:.1%}")
        print(f"  {'Dist':>5}  {'Tier':>10}  {'Lean':>7}  {'WP':>6}  "
              f"{'Pred':>5}  {'Actual':>6}  {'DemShr':>7}  {'':>2}")
        for _, row in competitive.sort_values(
            "win_prob", ascending=False
        ).iterrows():
            pred = "D" if row["predicted_d"] else "R"
            actual = "D" if row["actual_d_win"] else "R"
            check = "Y" if row["correct"] else "X"
            print(f"  {int(row['district']):>5}  {row['tier']:>10}  "
                  f"{row['composite_lean']:>+7.3f}  {row['win_prob']:>6.3f}  "
                  f"{pred:>5}  {actual:>6}  {row['dem_share']:>7.3f}  "
                  f"{check:>2}")

    return {
        "overall_accuracy": overall_accuracy,
        "contested_accuracy": contested_accuracy,
        "n_misses": len(misses),
        "misses": misses,
        "actual_d_seats": actual_d_seats,
        "predicted_d_seats": predicted_d_seats,
        "mc_mean_seats": mc_mean,
        "in_80ci": in_80ci,
        "in_50ci": in_50ci,
        "brier": brier,
        "brier_contested": brier_contested,
        "brier_skill": brier_skill,
        "log_loss": log_loss,
        "spearman_rho": rho,
        "competitive_accuracy": comp_accuracy,
    }


def write_backtest_report(results: dict) -> Path:
    """Write a text report summarizing backtest results."""
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = REPORTS_DIR / "backtest_summary.txt"

    comp = results["composite_comparison"]
    sim = results["sim_result"]
    diff = comp["composite_lean_pre2024"] - comp["composite_lean_full"]

    lines = [
        "=" * 70,
        "HISTORICAL BACKTEST: Pre-2024 Data -> 2024 Predictions",
        "=" * 70,
        "",
        "Methodology:",
        "  Composite lean built from 2016-2022 statewide races only (no 2024 data)",
        "  Block backbone used for spatial crosswalk (same as production pipeline)",
        f"  Pre-2024 weights: {dict(sorted(PRE2024_WEIGHTS.items()))}",
        f"  Simulated at actual 2024 statewide D share: "
        f"{results['statewide_d_2024']:.4f} "
        f"({results['statewide_d_2024']*100:.1f}%)",
        "  District uncertainty: sigma_prior from pre-2024 candidate effects",
        "",
        "RESULTS",
        "-" * 70,
        "",
        f"Binary accuracy (all 99 districts):     "
        f"{results['overall_accuracy']:.1%}",
        f"Binary accuracy (contested only):       "
        f"{results['contested_accuracy']:.1%}",
        f"Misclassified districts:                {results['n_misses']}",
        "",
        f"Actual D seats (2024):                  {results['actual_d_seats']}",
        f"Predicted (expected seats):             "
        f"{results['predicted_d_seats']:.1f}",
        f"MC mean:                                {results['mc_mean_seats']:.1f}",
        f"MC 80% CI:                              "
        f"[{sim.p10_seats}, {sim.p90_seats}]",
        f"Actual in 80% CI:                       "
        f"{'YES' if results['in_80ci'] else 'NO'}",
        f"Actual in 50% CI:                       "
        f"{'YES' if results['in_50ci'] else 'NO'}",
        "",
        f"Brier score (all):                      {results['brier']:.4f}",
        f"Brier score (contested):                "
        f"{results['brier_contested']:.4f}",
        f"Brier skill score vs naive:             {results['brier_skill']:.4f}",
        f"Log loss:                               {results['log_loss']:.4f}",
    ]

    if results.get("spearman_rho") is not None:
        lines.append(
            f"Spearman rho (contested):               "
            f"{results['spearman_rho']:.4f}"
        )
    if results.get("competitive_accuracy") is not None:
        lines.append(
            f"Competitive district accuracy:          "
            f"{results['competitive_accuracy']:.1%}"
        )

    lines += [
        "",
        "COMPOSITE STABILITY (pre-2024 vs full 2016-2024)",
        "-" * 70,
        f"Mean |delta lean|:         {diff.abs().mean():.4f}",
        f"Max  |delta lean|:         {diff.abs().max():.4f}",
        f"Correlation:               {results['composite_correlation']:.6f}",
        f"Tier changes:              "
        f"{(comp['tier_pre'] != comp['tier_full']).sum()}",
        "",
        "MISCLASSIFIED DISTRICTS",
        "-" * 70,
    ]

    misses = results["misses"]
    if len(misses) > 0:
        lines.append(
            f"{'Dist':>5}  {'Tier':>10}  {'WinProb':>7}  {'Predicted':>9}  "
            f"{'Actual':>8}  {'Margin':>8}"
        )
        for _, row in misses.iterrows():
            pred = "D" if row.get("predicted_d", row["win_prob"] > 0.5) else "R"
            actual = "D" if row["actual_d_win"] else "R"
            margin_str = (f"{row['margin']:+.3f}"
                          if pd.notna(row["margin"]) else "N/A")
            lines.append(
                f"{int(row['district']):>5}  {row['tier']:>10}  "
                f"{row['win_prob']:>7.3f}  {pred:>9}  {actual:>8}  "
                f"{margin_str:>8}"
            )
    else:
        lines.append("  (none)")

    lines += [
        "",
        "INTERPRETATION",
        "-" * 70,
        "This backtest asks: if we had built this model before the 2024 election",
        "using only 2016-2022 data, how well would it have predicted 2024 outcomes?",
        "",
        "Key questions answered:",
        "  1. Does the composite lean correctly rank-order competitive districts?",
        "  2. Are the probabilistic win probabilities well-calibrated?",
        "  3. Does the seat count prediction contain the actual result?",
        "  4. How stable is the composite when 2024 data is added?",
    ]

    path.write_text("\n".join(lines), encoding="utf-8")
    print(f"\nBacktest report written to {path}")
    return path


def write_backtest_csvs(results: dict) -> None:
    """Write evaluation DataFrames to CSV."""
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    # Per-district predictions vs actuals
    eval_df = results["eval_df"].copy()
    eval_cols = [
        "district", "composite_lean", "tier", "win_prob", "margin",
        "sigma_i", "actual_d_win", "winner", "contested", "dem_share",
        "flip_threshold", "n_contested",
    ]
    available = [c for c in eval_cols if c in eval_df.columns]
    eval_out = eval_df[available].sort_values("district")
    eval_out.to_csv(
        REPORTS_DIR / "backtest_district_predictions.csv",
        index=False, float_format="%.6f",
    )

    # Composite comparison
    results["composite_comparison"].to_csv(
        REPORTS_DIR / "backtest_composite_comparison.csv",
        index=False, float_format="%.6f",
    )

    print(f"Backtest CSVs written to {REPORTS_DIR}/")
