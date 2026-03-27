"""
district_data.py — Assemble all data for a single district into a flat dict.

Extracted from src/export.py to avoid importing reportlab/geopandas in the GUI.
"""

from __future__ import annotations

from typing import Any

import pandas as pd


def _get_val(row: Any, col: str) -> float | None:
    if row is None:
        return None
    try:
        v = row[col]
        if pd.isna(v):
            return None
        return float(v)
    except (KeyError, TypeError, ValueError):
        return None


def get_district_data(
    district: int,
    targeting_df: pd.DataFrame,
    composite_df: pd.DataFrame,
    demographics_df: pd.DataFrame,
    redistricting_df: pd.DataFrame | None = None,
    anomaly_df: pd.DataFrame | None = None,
    voter_universe_df: pd.DataFrame | None = None,
    win_prob_df: pd.DataFrame | None = None,
    investment_df: pd.DataFrame | None = None,
    sigma_df: pd.DataFrame | None = None,
) -> dict[str, Any]:
    """Pull all data for a single district into a flat dict."""
    t = targeting_df[targeting_df["district"] == district]
    c = composite_df[composite_df["district"] == district]
    d = demographics_df[demographics_df.get("district_num", demographics_df.index) == district] if demographics_df is not None else pd.DataFrame()

    if t.empty or c.empty:
        raise ValueError(f"District {district} not found in targeting or composite data.")

    tr = t.iloc[0]
    cr = c.iloc[0]
    dr = d.iloc[0] if not d.empty else None

    # Redistricting row
    rr = None
    if redistricting_df is not None:
        rm = redistricting_df[redistricting_df["district"] == district]
        rr = rm.iloc[0] if not rm.empty else None

    # Anomaly flags for this district
    district_anomalies: list[dict] = []
    if anomaly_df is not None and not anomaly_df.empty:
        af = anomaly_df[anomaly_df["district"] == district]
        for _, arow in af.iterrows():
            district_anomalies.append({
                "year": int(arow["year"]),
                "severity": str(arow.get("severity", "")),
                "residual": float(arow.get("residual", 0)),
                "explanation": str(arow.get("auto_explanation", "")),
            })

    data: dict[str, Any] = {
        "district": district,
        # Classification
        "composite_lean": float(cr["composite_lean"]),
        "tier": str(tr.get("tier", "unknown")),
        "current_holder": str(tr.get("current_holder", "unknown")),
        "pickup_opportunity": bool(tr.get("pickup_opportunity", False)),
        "defensive_priority": bool(tr.get("defensive_priority", False)),
        # 2026 open seat
        "open_seat_2026": bool(tr.get("open_seat_2026", False)),
        "open_seat_reason": str(tr.get("open_seat_reason", "") or ""),
        "incumbent_status_2026": str(tr.get("incumbent_status_2026", "unknown") or "unknown"),
        "current_incumbent_name": str(tr.get("current_incumbent_name", "") or ""),
        # Candidate names
        "dem_candidate_2024": str(tr.get("dem_candidate_2024", "") or ""),
        "rep_candidate_2024": str(tr.get("rep_candidate_2024", "") or ""),
        # Individual race leans
        "lean_pre_2024": _get_val(cr, "pre_2024_lean"),
        "lean_uss_2024": _get_val(cr, "uss_2024_lean"),
        "lean_gov_2022": _get_val(cr, "gov_2022_lean"),
        "lean_uss_2022": _get_val(cr, "uss_2022_lean"),
        "lean_sw_avg_2022": _get_val(cr, "statewide_avg_2022_lean"),
        "lean_pre_2020": _get_val(cr, "pre_2020_lean"),
        "lean_gov_2018": _get_val(cr, "gov_2018_lean"),
        "lean_uss_2018": _get_val(cr, "uss_2018_lean"),
        "lean_sw_avg_2018": _get_val(cr, "statewide_avg_2018_lean"),
        # House results
        "dem_share_2024": _get_val(cr, "dem_share_2024"),
        "dem_share_2022": _get_val(cr, "dem_share_2022"),
        "dem_share_2020": _get_val(cr, "dem_share_2020"),
        "dem_share_2018": _get_val(cr, "dem_share_2018"),
        "margin_2024": _get_val(cr, "margin_2024"),
        "margin_2022": _get_val(cr, "margin_2022"),
        "margin_2020": _get_val(cr, "margin_2020"),
        "margin_2018": _get_val(cr, "margin_2018"),
        "winner_2024": _get_val(cr, "winner_2024"),
        "winner_2022": _get_val(cr, "winner_2022"),
        "winner_2020": _get_val(cr, "winner_2020"),
        "winner_2018": _get_val(cr, "winner_2018"),
        "contested_2024": bool(_get_val(cr, "contested_2024") or False),
        "contested_2022": bool(_get_val(cr, "contested_2022") or False),
        "contested_2020": bool(_get_val(cr, "contested_2020") or False),
        "contested_2018": bool(_get_val(cr, "contested_2018") or False),
        # Candidate effects
        "glm_effect_2024": _get_val(cr, "candidate_effect_2024"),
        "glm_effect_2022": _get_val(cr, "candidate_effect_2022"),
        "glm_effect_2020": _get_val(cr, "candidate_effect_2020"),
        "glm_effect_2018": _get_val(cr, "candidate_effect_2018"),
        # Targeting
        "target_mode": str(tr.get("target_mode", "unknown")),
        "swing_sd": _get_val(tr, "swing_sd"),
        "n_contested": int(tr.get("n_contested", 0)),
        "turnout_elasticity": _get_val(tr, "turnout_elasticity"),
        "flip_threshold": _get_val(tr, "flip_threshold"),
        "composite_sensitivity": _get_val(tr, "composite_sensitivity"),
        "most_sensitive_race": str(tr.get("most_sensitive_race", "") or ""),
        # Redistricting
        "overlap_category": str(rr["overlap_category"]) if rr is not None else "unknown",
        "overlap_category_interim_final": str(rr["overlap_category_interim_final"]) if rr is not None else "unknown",
        "years_reliable": str(rr["years_reliable"]) if rr is not None else "unknown",
        "jaccard_old_interim": _get_val(rr, "jaccard_similarity") if rr is not None else None,
        "jaccard_interim_final": _get_val(rr, "jaccard_interim_final") if rr is not None else None,
        # Anomaly flags
        "anomaly_flags": district_anomalies,
        # Demographics
        "total_pop": int(dr["total_pop"]) if dr is not None and "total_pop" in dr else None,
        "college_pct": _get_val(dr, "college_pct") if dr is not None else None,
        "median_income": _get_val(dr, "median_income") if dr is not None else None,
        "white_pct": _get_val(dr, "white_pct") if dr is not None else None,
        "black_pct": _get_val(dr, "black_pct") if dr is not None else None,
        "hispanic_pct": _get_val(dr, "hispanic_pct") if dr is not None else None,
        "pop_density": _get_val(dr, "pop_density") if dr is not None else None,
        "land_area_sqmi": _get_val(dr, "district_land_area_sqmi") if dr is not None else None,
    }

    # Voter universe (optional)
    vr = None
    if voter_universe_df is not None:
        vm = voter_universe_df[voter_universe_df["district"] == district]
        vr = vm.iloc[0] if not vm.empty else None
    data["voter_universe"] = {
        "total_active_voters":      int(vr["total_active_voters"])           if vr is not None else None,
        "inactive_voters":          int(vr.get("inactive_voters", 0) or 0)   if vr is not None else None,
        "partisan_advantage":       _get_val(vr, "partisan_advantage")       if vr is not None else None,
        "pct_strong_d":             _get_val(vr, "pct_strong_d")             if vr is not None else None,
        "pct_lean_d":               _get_val(vr, "pct_lean_d")               if vr is not None else None,
        "pct_strong_r":             _get_val(vr, "pct_strong_r")             if vr is not None else None,
        "pct_lean_r":               _get_val(vr, "pct_lean_r")               if vr is not None else None,
        "pct_crossover":            _get_val(vr, "pct_crossover")            if vr is not None else None,
        "pct_unaffiliated":         _get_val(vr, "pct_unaffiliated")         if vr is not None else None,
        "turnout_2024":             _get_val(vr, "turnout_2024")             if vr is not None else None,
        "turnout_2022":             _get_val(vr, "turnout_2022")             if vr is not None else None,
        "turnout_dropoff":          _get_val(vr, "turnout_dropoff")          if vr is not None else None,
        "pct_presidential_only":    _get_val(vr, "pct_presidential_only")    if vr is not None else None,
        "n_mobilization_targets":   int(vr["n_mobilization_targets"])        if vr is not None else None,
        "pct_mobilization_targets": _get_val(vr, "pct_mobilization_targets") if vr is not None else None,
        "n_persuasion_targets":     int(vr["n_persuasion_targets"])          if vr is not None else None,
        "pct_persuasion_targets":   _get_val(vr, "pct_persuasion_targets")   if vr is not None else None,
        "target_mode_voterfile":    str(tr.get("target_mode_voterfile", "")) if "target_mode_voterfile" in tr.index else None,
    } if vr is not None else None

    # Probabilistic outlook (optional)
    prob_data = None
    if win_prob_df is not None and not win_prob_df.empty:
        dp = win_prob_df[win_prob_df["district"] == district]
        if not dp.empty:
            prob_data = {}
            for sw_pct in [46.0, 48.0, 50.0]:
                row_wp = dp[dp["statewide_d_pct"] == sw_pct]
                if not row_wp.empty:
                    prob_data[f"wp_{sw_pct:.0f}"] = float(row_wp["win_prob"].iloc[0])
            if sigma_df is not None:
                sr = sigma_df[sigma_df["district"] == district]
                if not sr.empty:
                    prob_data["sigma_i"] = float(sr["sigma_i"].iloc[0])
                    prob_data["sigma_source"] = str(sr["sigma_source"].iloc[0])
            if investment_df is not None:
                ir = investment_df[investment_df["district"] == district]
                if not ir.empty:
                    prob_data["investment_rank"] = int(ir["investment_rank"].iloc[0])
                    prob_data["marginal_wp"] = float(ir["marginal_wp"].iloc[0])
    data["probabilistic"] = prob_data

    return data
