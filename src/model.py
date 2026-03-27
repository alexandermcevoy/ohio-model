"""
model.py — GLM (binomial/logit) regression model for Ohio House district outcomes.

Models actual dem_share in contested house races using a logistic link function,
which handles the S-shaped compression between partisan lean and vote share in
safe seats. Produces both log-odds coefficients and average marginal effects (AMEs)
on the probability scale.

OLS is also fit for comparison purposes.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import statsmodels.api as sm
import statsmodels.formula.api as smf
from scipy import stats


# ---------------------------------------------------------------------------
# Helper: population-weighted statewide mean
# ---------------------------------------------------------------------------

def _pop_weighted_mean(series: pd.Series, pop: pd.Series) -> float:
    valid = series.notna() & pop.notna() & (pop > 0)
    return float((series[valid] * pop[valid]).sum() / pop[valid].sum())


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_regression_df(
    composite_df: pd.DataFrame,
    demographics_df: pd.DataFrame,
) -> pd.DataFrame:
    """Build district-year regression dataset."""

    # --- Merge composite + demographics ---
    demo = demographics_df.reset_index() if demographics_df.index.name == "district_num" else demographics_df.copy()
    demo = demo.rename(columns={"district_num": "district"}) if "district_num" in demo.columns else demo

    merged = composite_df.merge(demo, on="district", how="left")

    # --- Compute centered demographic variables ---
    pop = merged["total_pop"]

    sw_college = _pop_weighted_mean(merged["college_pct"], pop)
    sw_log_income = _pop_weighted_mean(np.log(merged["median_income"].clip(lower=1)), pop)
    sw_white = _pop_weighted_mean(merged["white_pct"], pop)
    sw_log_density = _pop_weighted_mean(np.log(merged["pop_density"].clip(lower=0.001)), pop)

    merged["college_pct_c"] = merged["college_pct"] - sw_college
    merged["log_income_c"] = np.log(merged["median_income"].clip(lower=1)) - sw_log_income
    merged["white_pct_c"] = merged["white_pct"] - sw_white
    merged["log_density_c"] = np.log(merged["pop_density"].clip(lower=0.001)) - sw_log_density

    # --- Build long-format dataset: one row per (district, year) ---
    years = [2018, 2020, 2022, 2024]

    lagged_winner_col = {
        2018: None,
        2020: "winner_2018",
        2022: "winner_2020",
        2024: "winner_2022",
    }

    rows: list[dict] = []

    for _, district_row in merged.iterrows():
        district_id = district_row["district"]

        for year in years:
            contested_col = f"contested_{year}"
            if contested_col not in district_row.index:
                continue
            if not district_row[contested_col]:
                continue

            dem_share_col = f"dem_share_{year}"
            if dem_share_col not in district_row.index or pd.isna(district_row[dem_share_col]):
                continue
            dem_share = district_row[dem_share_col]

            lag_col = lagged_winner_col[year]
            if lag_col is None or lag_col not in district_row.index or pd.isna(district_row[lag_col]):
                incumbent_party = "open"
            else:
                w = str(district_row[lag_col])
                if w in ("D", "D_uncontested"):
                    incumbent_party = "D_inc"
                elif w in ("R", "R_uncontested"):
                    incumbent_party = "R_inc"
                else:
                    incumbent_party = "open"

            rows.append({
                "district": district_id,
                "year": year,
                "dem_share": dem_share,
                "composite_lean": district_row["composite_lean"],
                "college_pct_c": district_row["college_pct_c"],
                "log_income_c": district_row["log_income_c"],
                "white_pct_c": district_row["white_pct_c"],
                "log_density_c": district_row["log_density_c"],
                "incumbent_party": incumbent_party,
                "presidential_year": 1 if year in (2020, 2024) else 0,
                "total_pop": district_row.get("total_pop", np.nan),
            })

    reg_df = pd.DataFrame(rows)
    print(f"Regression dataset: {len(reg_df):,} contested district-year observations "
          f"across {reg_df['district'].nunique()} districts.")
    return reg_df


def _compute_ames(glm_results, reg_df: pd.DataFrame) -> dict[str, float]:
    """
    Compute average marginal effects (AMEs) for all predictors on the probability scale.

    For continuous predictors: AME_j = mean_i[ β_j * P_i * (1 − P_i) ]
    For composite_lean, accounts for the interaction with presidential_year.
    For dummy predictors (incumbent_party), same delta-method approximation.
    """
    mu = glm_results.fittedvalues.values
    dP_deta = mu * (1 - mu)  # Bernoulli variance at each observation

    ames: dict[str, float] = {}
    for var_name, coef in glm_results.params.items():
        ames[var_name] = float(coef * dP_deta.mean())

    # Composite_lean total AME: accounts for interaction
    interact_key = "composite_lean:presidential_year"
    if interact_key in glm_results.params.index:
        beta_interact = float(glm_results.params[interact_key])
        total_lean_effect = (
            float(glm_results.params["composite_lean"])
            + beta_interact * reg_df["presidential_year"].values
        )
        ames["composite_lean_total_AME"] = float((total_lean_effect * dP_deta).mean())

    return ames


def run_regression(
    composite_df: pd.DataFrame,
    demographics_df: pd.DataFrame,
) -> tuple[object, object, pd.DataFrame]:
    """
    Fit OLS (for comparison) and GLM binomial/logit (primary model).
    Returns (ols_results, glm_results, reg_df).

    reg_df is augmented with columns:
      ols_predicted, ols_residual, glm_predicted, glm_residual
    """
    reg_df = build_regression_df(composite_df, demographics_df)

    if len(reg_df) < 10:
        raise ValueError(
            f"Only {len(reg_df)} contested observations — not enough to fit a model."
        )

    formula_rhs = (
        "composite_lean + college_pct_c + log_income_c + "
        "white_pct_c + log_density_c + "
        "C(incumbent_party, Treatment('open')) + presidential_year + "
        "composite_lean:presidential_year"
    )

    # ── OLS (kept for side-by-side comparison) ───────────────────────────────
    ols_results = smf.ols(
        formula=f"dem_share ~ {formula_rhs}",
        data=reg_df,
    ).fit(cov_type="cluster", cov_kwds={"groups": reg_df["district"]})

    # ── GLM binomial/logit ────────────────────────────────────────────────────
    # Clip dem_share away from 0/1 for numerical stability in logit
    reg_df = reg_df.copy()
    reg_df["dem_share_clipped"] = reg_df["dem_share"].clip(0.001, 0.999)

    glm_results = smf.glm(
        formula=f"dem_share_clipped ~ {formula_rhs}",
        data=reg_df,
        family=sm.families.Binomial(),
    ).fit(cov_type="cluster", cov_kwds={"groups": reg_df["district"]})

    # ── Residuals on probability scale ───────────────────────────────────────
    reg_df["ols_predicted"] = ols_results.fittedvalues.values
    reg_df["ols_residual"] = reg_df["dem_share"] - reg_df["ols_predicted"]
    reg_df["glm_predicted"] = glm_results.fittedvalues.values
    reg_df["glm_residual"] = reg_df["dem_share"] - reg_df["glm_predicted"]

    # ── Null model for pseudo-R² ──────────────────────────────────────────────
    null_glm = smf.glm(
        formula="dem_share_clipped ~ 1",
        data=reg_df,
        family=sm.families.Binomial(),
    ).fit()
    llf_null = float(null_glm.llf)
    null_deviance = float(null_glm.deviance)

    pseudo_r2 = float(1 - glm_results.llf / llf_null)
    deviance_explained = float(1 - glm_results.deviance / null_deviance)

    # ── AMEs ─────────────────────────────────────────────────────────────────
    ames = _compute_ames(glm_results, reg_df)

    # ── RMSE & max residual ───────────────────────────────────────────────────
    ols_rmse = float(np.sqrt((reg_df["ols_residual"] ** 2).mean()))
    glm_rmse = float(np.sqrt((reg_df["glm_residual"] ** 2).mean()))
    ols_max = float(reg_df["ols_residual"].abs().max())
    glm_max = float(reg_df["glm_residual"].abs().max())

    # ── Validation checkpoint 1: Fit statistics ──────────────────────────────
    # Note: McFadden pseudo-R² is unreliable for continuous fractional outcomes
    # (dem_share ∈ [0,1]) because the null deviance is far lower than for binary
    # data, making the ratio misleadingly small. Use deviance explained instead.
    print(f"\n  [Checkpoint 1] Deviance explained = {deviance_explained:.4f}  "
          f"(McFadden pseudo-R² = {pseudo_r2:.4f} — unreliable for fractional outcomes)")
    print(f"             OLS R² = {ols_results.rsquared:.4f} for comparison")
    if deviance_explained >= 0.55:
        print(f"  PASS: deviance explained {deviance_explained:.4f} ≥ 0.55.")
    else:
        print(f"  WARNING: deviance explained {deviance_explained:.4f} < 0.55.")

    # ── Validation checkpoint 2: Composite lean ME at lean=0 ─────────────────
    beta_lean = float(glm_results.params.get("composite_lean", np.nan))
    mean_pres_year = float(reg_df["presidential_year"].mean())
    eta_at_lean0 = (
        float(glm_results.params["Intercept"])
        + float(glm_results.params.get("presidential_year", 0)) * mean_pres_year
    )
    p_at_lean0 = float(1 / (1 + np.exp(-eta_at_lean0)))
    me_lean_at_0 = float(beta_lean * p_at_lean0 * (1 - p_at_lean0))

    print(f"\n  [Checkpoint 2] Composite_lean marginal effect at lean=0:")
    print(f"    β (log-odds) = {beta_lean:.4f}")
    print(f"    P(D | lean=0, other vars at mean) = {p_at_lean0:.4f}")
    print(f"    ME at lean=0 = {me_lean_at_0:.4f}  (target: 0.85–1.15)")
    if 0.85 <= me_lean_at_0 <= 1.15:
        print(f"  PASS: ME at lean=0 within [0.85, 1.15].")
    else:
        print(f"  WARNING: ME at lean=0 = {me_lean_at_0:.4f} is outside [0.85, 1.15].")

    # ── Validation checkpoint 3: Incumbency AMEs ─────────────────────────────
    d_inc_key = "C(incumbent_party, Treatment('open'))[T.D_inc]"
    r_inc_key = "C(incumbent_party, Treatment('open'))[T.R_inc]"
    ame_d_inc = ames.get(d_inc_key, np.nan)
    ame_r_inc = ames.get(r_inc_key, np.nan)
    print(f"\n  [Checkpoint 3] Incumbency AMEs (probability scale):")
    print(f"    D incumbent: {ame_d_inc:+.4f}  (target: +0.03 to +0.10)")
    print(f"    R incumbent: {ame_r_inc:+.4f}  (target: -0.10 to -0.03)")
    if not (0.03 <= ame_d_inc <= 0.10):
        print(f"  WARNING: D incumbent AME {ame_d_inc:+.4f} outside [+0.03, +0.10].")
    if not (-0.10 <= ame_r_inc <= -0.03):
        print(f"  WARNING: R incumbent AME {ame_r_inc:+.4f} outside [-0.10, -0.03].")

    # ── Validation checkpoint 4: Predicted values by lean tier ───────────────
    safe_d_mask = reg_df["composite_lean"] > 0.30
    tossup_mask = reg_df["composite_lean"].abs() < 0.03
    print(f"\n  [Checkpoint 4] Predicted D share by lean tier (GLM):")
    if safe_d_mask.any():
        mean_pred_safe = reg_df.loc[safe_d_mask, "glm_predicted"].mean()
        mean_act_safe  = reg_df.loc[safe_d_mask, "dem_share"].mean()
        print(f"    Safe-D (lean > 0.30):   predicted {mean_pred_safe:.3f}  actual {mean_act_safe:.3f}  "
              f"(expect predicted 0.75–0.90)")
    if tossup_mask.any():
        mean_pred_toss = reg_df.loc[tossup_mask, "glm_predicted"].mean()
        mean_act_toss  = reg_df.loc[tossup_mask, "dem_share"].mean()
        print(f"    Tossup (|lean| < 0.03): predicted {mean_pred_toss:.3f}  actual {mean_act_toss:.3f}  "
              f"(expect ~0.45–0.52)")

    # ── Validation checkpoint 5: Residuals + 2018 Columbus districts ─────────
    glm_resid_sorted = reg_df[
        ["district", "year", "dem_share", "composite_lean", "glm_residual"]
    ].sort_values("glm_residual", ascending=False)

    print(f"\n  [Checkpoint 5] Top 10 positive residuals (GLM):")
    print(glm_resid_sorted.head(10).to_string(index=False))
    print(f"\n  Top 10 negative residuals (GLM):")
    print(glm_resid_sorted.tail(10).to_string(index=False))

    columbus_2018 = reg_df[
        reg_df["district"].isin([1, 2, 3, 4, 7]) & (reg_df["year"] == 2018)
    ].sort_values("district")
    if not columbus_2018.empty:
        print(f"\n  2018 Columbus safe-D districts — residual comparison:")
        print(f"  {'Dist':>5}  {'Actual':>7}  {'OLS res':>9}  {'GLM res':>9}")
        for _, row in columbus_2018.iterrows():
            print(
                f"  {int(row['district']):>5}  {row['dem_share']:>7.4f}"
                f"  {row['ols_residual']:>+9.4f}  {row['glm_residual']:>+9.4f}"
            )

    # ── Side-by-side comparison summary ──────────────────────────────────────
    ols_lean_coef = float(ols_results.params.get("composite_lean", np.nan))
    ols_d_inc = float(ols_results.params.get(d_inc_key, np.nan))
    ols_r_inc = float(ols_results.params.get(r_inc_key, np.nan))

    print(f"\n  Model comparison:")
    print(f"  {'Metric':<40} {'OLS':>10}  {'GLM (logit)':>12}")
    print(f"  {'-'*65}")
    print(f"  {'R² / McFadden pseudo-R²':<40} {ols_results.rsquared:>10.4f}  {pseudo_r2:>12.4f}")
    print(f"  {'Deviance explained':<40} {'—':>10}  {deviance_explained:>12.4f}")
    print(f"  {'Composite lean coeff (raw)':<40} {ols_lean_coef:>10.4f}  {beta_lean:>12.4f}")
    print(f"  {'  → marginal effect at lean=0':<40} {ols_lean_coef:>10.4f}  {me_lean_at_0:>12.4f}")
    print(f"  {'Incumbency D (coeff/AME)':<40} {ols_d_inc:>+10.4f}  {ame_d_inc:>+12.4f}")
    print(f"  {'Incumbency R (coeff/AME)':<40} {ols_r_inc:>+10.4f}  {ame_r_inc:>+12.4f}")
    print(f"  {'RMSE (probability scale)':<40} {ols_rmse:>10.4f}  {glm_rmse:>12.4f}")
    print(f"  {'Max |residual|':<40} {ols_max:>10.4f}  {glm_max:>12.4f}")

    # Attach summary stats for use in format_regression_summary
    glm_results._ohio_meta = {
        "pseudo_r2": pseudo_r2,
        "deviance_explained": deviance_explained,
        "me_lean_at_0": me_lean_at_0,
        "p_at_lean0": p_at_lean0,
        "ames": ames,
        "glm_rmse": glm_rmse,
        "ols_rmse": ols_rmse,
        "glm_max_resid": glm_max,
        "ols_max_resid": ols_max,
        "null_deviance": null_deviance,
    }

    return ols_results, glm_results, reg_df


def format_regression_summary(ols_results, glm_results, reg_df: pd.DataFrame) -> str:
    """Return human-readable summary with OLS + GLM results and side-by-side comparison."""
    meta = getattr(glm_results, "_ohio_meta", {})
    pseudo_r2 = meta.get("pseudo_r2", np.nan)
    deviance_explained = meta.get("deviance_explained", np.nan)
    me_lean_at_0 = meta.get("me_lean_at_0", np.nan)
    p_at_lean0 = meta.get("p_at_lean0", np.nan)
    ames = meta.get("ames", {})
    glm_rmse = meta.get("glm_rmse", np.nan)
    ols_rmse = meta.get("ols_rmse", np.nan)
    glm_max = meta.get("glm_max_resid", np.nan)
    ols_max = meta.get("ols_max_resid", np.nan)
    null_deviance = meta.get("null_deviance", np.nan)

    d_inc_key = "C(incumbent_party, Treatment('open'))[T.D_inc]"
    r_inc_key = "C(incumbent_party, Treatment('open'))[T.R_inc]"
    ols_lean_coef = float(ols_results.params.get("composite_lean", np.nan))
    beta_lean = float(glm_results.params.get("composite_lean", np.nan))
    ols_d_inc = float(ols_results.params.get(d_inc_key, np.nan))
    ols_r_inc = float(ols_results.params.get(r_inc_key, np.nan))
    ame_d_inc = ames.get(d_inc_key, np.nan)
    ame_r_inc = ames.get(r_inc_key, np.nan)

    lines: list[str] = []

    # ── OLS section ──────────────────────────────────────────────────────────
    lines += [
        "=" * 80,
        "Ohio House OLS Regression (for comparison)",
        "=" * 80,
        f"Observations:    {int(ols_results.nobs):,}",
        f"R²:              {ols_results.rsquared:.4f}",
        f"Adjusted R²:     {ols_results.rsquared_adj:.4f}",
        f"F-statistic:     {ols_results.fvalue:.2f}  (p={ols_results.f_pvalue:.4f})",
        f"RMSE:            {ols_rmse:.4f}",
        "",
        "Coefficients (clustered SEs by district):",
        f"{'Variable':<45} {'Coef':>8}  {'Std Err':>8}  {'p':>6}  {'[0.025':>8}  {'0.975]':>8}",
        "-" * 92,
    ]
    ci_ols = ols_results.conf_int()
    for var in ols_results.params.index:
        coef = ols_results.params[var]
        se = ols_results.bse[var]
        pval = ols_results.pvalues[var]
        lo = ci_ols.loc[var, 0]
        hi = ci_ols.loc[var, 1]
        sig = "***" if pval < 0.001 else ("**" if pval < 0.01 else ("*" if pval < 0.05 else ""))
        lines.append(f"  {var:<43} {coef:>8.4f}  {se:>8.4f}  {pval:>6.4f}  {lo:>8.4f}  {hi:>8.4f}  {sig}")

    # ── GLM section ───────────────────────────────────────────────────────────
    lines += [
        "",
        "=" * 80,
        "Ohio House GLM — Binomial/Logit (primary model)",
        "=" * 80,
        f"Observations:        {int(glm_results.nobs):,}",
        f"McFadden pseudo-R²:  {pseudo_r2:.4f}  (unreliable for fractional outcomes — see deviance explained)",
        f"Deviance explained:  {deviance_explained:.4f}",
        f"Residual deviance:   {glm_results.deviance:.2f}",
        f"Null deviance:       {null_deviance:.2f}",
        f"RMSE (prob scale):   {glm_rmse:.4f}",
        "",
        "Coefficients on log-odds scale (clustered SEs by district):",
        f"{'Variable':<45} {'Coef':>8}  {'Std Err':>8}  {'p':>6}  {'[0.025':>8}  {'0.975]':>8}",
        "-" * 92,
    ]
    ci_glm = glm_results.conf_int()
    for var in glm_results.params.index:
        coef = glm_results.params[var]
        se = glm_results.bse[var]
        pval = glm_results.pvalues[var]
        lo = float(ci_glm.loc[var, 0])
        hi = float(ci_glm.loc[var, 1])
        sig = "***" if pval < 0.001 else ("**" if pval < 0.01 else ("*" if pval < 0.05 else ""))
        lines.append(f"  {var:<43} {coef:>8.4f}  {se:>8.4f}  {pval:>6.4f}  {lo:>8.4f}  {hi:>8.4f}  {sig}")

    # ── AME section ───────────────────────────────────────────────────────────
    lines += [
        "",
        "Average Marginal Effects (probability scale — interpretable as pct-pt changes):",
        f"{'Variable':<45} {'AME':>8}",
        "-" * 56,
    ]
    for var, ame in ames.items():
        lines.append(f"  {var:<43} {ame:>+8.4f}")

    lines += [
        "",
        f"Marginal effect of composite_lean at lean=0 (tossup):",
        f"  P(D | lean=0) = {p_at_lean0:.4f}",
        f"  ME at lean=0  = {me_lean_at_0:.4f}  (1.0 = perfect calibration)",
    ]

    # ── Side-by-side comparison ───────────────────────────────────────────────
    lines += [
        "",
        "=" * 80,
        "Model Comparison",
        "=" * 80,
        f"  {'Metric':<40} {'OLS':>10}  {'GLM (logit)':>12}",
        f"  {'-'*65}",
        f"  {'R² / McFadden pseudo-R²':<40} {ols_results.rsquared:>10.4f}  {pseudo_r2:>12.4f}",
        f"  {'Deviance explained':<40} {'—':>10}  {deviance_explained:>12.4f}",
        f"  {'Composite lean coeff (raw)':<40} {ols_lean_coef:>10.4f}  {beta_lean:>12.4f}",
        f"  {'  → marginal effect at lean=0':<40} {ols_lean_coef:>10.4f}  {me_lean_at_0:>12.4f}",
        f"  {'Incumbency D (coeff/AME)':<40} {ols_d_inc:>+10.4f}  {ame_d_inc:>+12.4f}",
        f"  {'Incumbency R (coeff/AME)':<40} {ols_r_inc:>+10.4f}  {ame_r_inc:>+12.4f}",
        f"  {'RMSE (probability scale)':<40} {ols_rmse:>10.4f}  {glm_rmse:>12.4f}",
        f"  {'Max |residual|':<40} {ols_max:>10.4f}  {glm_max:>12.4f}",
        "",
        f"Contested district-years: {len(reg_df):,}",
        f"Districts: {reg_df['district'].nunique()}",
        f"Years: {sorted(reg_df['year'].unique())}",
    ]

    return "\n".join(lines)
