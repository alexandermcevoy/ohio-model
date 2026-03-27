"""
Scenario Explorer — Seat projections with confidence intervals.

Answer: "How many seats do we win if the environment is X%?"
"""

import streamlit as st
import pandas as pd

from gui.data_loader import load_scenarios, load_win_probs, load_targeting
from gui.charts import seat_distribution_chart
from gui.styles import fmt_pct, fmt_margin, lean_to_margin, TIER_LABELS

st.set_page_config(page_title="Scenario Explorer", page_icon=":chart_with_upwards_trend:", layout="wide")
st.title("Scenario Explorer")

# ---------------------------------------------------------------------------
# Shared state
# ---------------------------------------------------------------------------

if "statewide_d" not in st.session_state:
    st.session_state["statewide_d"] = 48.0

selected_d = st.session_state["statewide_d"]

# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------

scenarios = load_scenarios()
win_probs = load_win_probs()
targeting = load_targeting()

# Closest scenario row
scenario_row = scenarios.iloc[(scenarios["statewide_d_pct"] - selected_d).abs().argsort().iloc[0]]

# ---------------------------------------------------------------------------
# Seat distribution chart
# ---------------------------------------------------------------------------

st.plotly_chart(seat_distribution_chart(scenarios, selected_d), use_container_width=True)

# ---------------------------------------------------------------------------
# Probability gauges
# ---------------------------------------------------------------------------

st.subheader(f"Threshold Probabilities @ {selected_d:.1f}% Statewide D")

col1, col2, col3 = st.columns(3)

with col1:
    p34 = scenario_row["prob_hold_34"]
    st.metric("P(Hold 34+ seats)", fmt_pct(p34))
    st.progress(min(p34, 1.0))

with col2:
    p40 = scenario_row["prob_reach_40"]
    st.metric("P(Reach 40+ seats)", fmt_pct(p40))
    st.progress(min(p40, 1.0))

with col3:
    pmaj = scenario_row["prob_majority"]
    st.metric("P(Majority 50+ seats)", fmt_pct(pmaj))
    st.progress(min(pmaj, 1.0))

# ---------------------------------------------------------------------------
# Flip table — which districts are >50% WP at this environment
# ---------------------------------------------------------------------------

st.divider()
st.subheader(f"Districts with >50% Win Probability @ {selected_d:.1f}%")

wp_at_sel = win_probs[win_probs["statewide_d_pct"].round(1) == round(selected_d, 1)]

# R-held districts that flip (WP > 50%)
r_held = targeting[targeting["current_holder"] == "R"]["district"]
flips = wp_at_sel[
    (wp_at_sel["district"].isin(r_held)) & (wp_at_sel["win_prob"] >= 0.50)
].merge(
    targeting[["district", "composite_lean", "tier", "open_seat_2026"]],
    on="district", how="left",
).sort_values("win_prob", ascending=False)

if flips.empty:
    st.info("No R-held districts reach >50% win probability at this environment.")
else:
    flips["expected_margin"] = flips["composite_lean"].apply(lambda x: lean_to_margin(x, selected_d))
    display = flips[["district", "expected_margin", "tier", "open_seat_2026", "win_prob"]].copy()
    display.columns = ["District", "Exp. Margin", "Tier", "Open 2026?", "Win Prob"]
    display["Exp. Margin"] = display["Exp. Margin"].apply(fmt_margin)
    display["Tier"] = display["Tier"].map(TIER_LABELS).fillna(display["Tier"])
    display["Win Prob"] = display["Win Prob"].apply(lambda x: fmt_pct(x))
    display["Open 2026?"] = display["Open 2026?"].map({True: "Yes", False: ""})
    st.dataframe(display, use_container_width=True, hide_index=True)
    st.caption(f"**{len(flips)} districts** projected to flip at {selected_d:.1f}% statewide D.")

# ---------------------------------------------------------------------------
# D-held seats at risk
# ---------------------------------------------------------------------------

st.divider()
st.subheader(f"D-Held Seats at Risk @ {selected_d:.1f}%")

d_held = targeting[targeting["current_holder"] == "D"]["district"]
at_risk = wp_at_sel[
    (wp_at_sel["district"].isin(d_held)) & (wp_at_sel["win_prob"] < 0.60)
].merge(
    targeting[["district", "composite_lean", "tier"]],
    on="district", how="left",
).sort_values("win_prob")

if at_risk.empty:
    st.success("All D-held seats have >60% probability of holding at this environment.")
else:
    at_risk["expected_margin"] = at_risk["composite_lean"].apply(lambda x: lean_to_margin(x, selected_d))
    display_risk = at_risk[["district", "expected_margin", "tier", "win_prob"]].copy()
    display_risk.columns = ["District", "Exp. Margin", "Tier", "P(Hold)"]
    display_risk["Exp. Margin"] = display_risk["Exp. Margin"].apply(fmt_margin)
    display_risk["Tier"] = display_risk["Tier"].map(TIER_LABELS).fillna(display_risk["Tier"])
    display_risk["P(Hold)"] = display_risk["P(Hold)"].apply(lambda x: fmt_pct(x))
    st.dataframe(display_risk, use_container_width=True, hide_index=True)

# ---------------------------------------------------------------------------
# Full scenario table
# ---------------------------------------------------------------------------

with st.expander("Full Scenario Table"):
    full = scenarios[["statewide_d_pct", "mean_d_seats", "p10_seats", "p25_seats",
                       "p75_seats", "p90_seats", "prob_hold_34", "prob_reach_40", "prob_majority"]].copy()
    full.columns = ["Statewide D%", "Mean Seats", "p10", "p25", "p75", "p90",
                     "P(Hold 34)", "P(Reach 40)", "P(Majority)"]
    full["Mean Seats"] = full["Mean Seats"].apply(lambda x: f"{x:.1f}")
    for col in ["P(Hold 34)", "P(Reach 40)", "P(Majority)"]:
        full[col] = full[col].apply(lambda x: fmt_pct(x))
    st.dataframe(full, use_container_width=True, hide_index=True)
