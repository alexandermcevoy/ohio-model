"""
Investment Priority — Where the next dollar matters most.

Ranked table by marginal win probability + path-to-target optimizer.
"""

import streamlit as st
import pandas as pd

from gui.data_loader import (
    load_targeting, load_investment_priority, load_path_optimizer,
    load_sigma, load_win_probs,
)
from gui.compute import live_investment_priority
from gui.charts import marginal_wp_bar_chart, path_to_target_chart
from gui.styles import fmt_pct, fmt_margin, lean_to_margin, TIER_LABELS

st.set_page_config(page_title="Investment Priority", page_icon=":moneybag:", layout="wide")
st.title("Investment Priority")

st.markdown(
    "**Marginal win probability** answers: if we improve this district's D performance "
    "by a tiny amount, how much does its chance of flipping increase? "
    "Tossup districts have the highest marginal WP; safe seats are near zero."
)

# ---------------------------------------------------------------------------
# Shared state
# ---------------------------------------------------------------------------

if "statewide_d" not in st.session_state:
    st.session_state["statewide_d"] = 48.0

selected_d = st.session_state["statewide_d"]

# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------

targeting = load_targeting()
sigma = load_sigma()
path_optimizer = load_path_optimizer()

# Use pre-computed investment priority at 48%, or recompute for other environments
REFERENCE_ENV = 48.0

if abs(selected_d - REFERENCE_ENV) < 0.01:
    investment = load_investment_priority()
else:
    with st.spinner(f"Computing investment priority at {selected_d:.1f}%..."):
        investment = live_investment_priority(targeting, sigma, selected_d / 100.0)

# ---------------------------------------------------------------------------
# Top targets bar chart
# ---------------------------------------------------------------------------

st.plotly_chart(
    marginal_wp_bar_chart(investment, targeting, n=20),
    use_container_width=True,
)

# ---------------------------------------------------------------------------
# Full ranked table
# ---------------------------------------------------------------------------

st.divider()
st.subheader(f"Full Investment Ranking @ {selected_d:.1f}% Statewide D")

# Use investment data directly (already has tier, open_seat_2026, current_holder)
display_inv = investment.copy()

# Merge any missing context columns from targeting
missing_cols = [c for c in ["tier", "open_seat_2026", "current_holder"] if c not in display_inv.columns]
if missing_cols:
    display_inv = display_inv.merge(
        targeting[["district"] + missing_cols], on="district", how="left",
    )

# Filter options
with st.sidebar:
    st.subheader("Filters")
    holder_filter = st.radio("Show", ["R-held only", "All districts"], index=0)
    open_only = st.checkbox("Open seats only", value=False)

if holder_filter == "R-held only":
    display_inv = display_inv[display_inv["current_holder"] == "R"]
if open_only:
    display_inv = display_inv[display_inv["open_seat_2026"] == True]

display_inv = display_inv.sort_values("marginal_wp", ascending=False).head(30)

display_inv["expected_margin"] = display_inv["composite_lean"].apply(
    lambda x: lean_to_margin(x, selected_d)
)
table = display_inv[["district", "win_prob", "marginal_wp", "tier",
                      "open_seat_2026", "expected_margin"]].copy()
table.columns = ["District", "Win Prob", "Marginal WP", "Tier", "Open 2026?", "Exp. Margin"]
table["Win Prob"] = table["Win Prob"].apply(lambda x: fmt_pct(x))
table["Marginal WP"] = table["Marginal WP"].apply(lambda x: f"{x:.2f}")
table["Tier"] = table["Tier"].map(TIER_LABELS).fillna(table["Tier"])
table["Open 2026?"] = table["Open 2026?"].map({True: "Yes", False: ""})
table["Exp. Margin"] = table["Exp. Margin"].apply(fmt_margin)

st.dataframe(table, use_container_width=True, hide_index=True)

# ---------------------------------------------------------------------------
# Path-to-target optimizer
# ---------------------------------------------------------------------------

st.divider()
st.subheader("Path to 40 Seats — Greedy Optimizer")

st.markdown(
    "Each step = an abstract +1 percentage point improvement in a district's D lean. "
    "The optimizer greedily selects the district where that shift most increases expected seats."
)

if not path_optimizer.empty:
    st.plotly_chart(path_to_target_chart(path_optimizer), use_container_width=True)

    # Show optimizer steps table
    with st.expander("Optimizer Steps"):
        path_display = path_optimizer[["priority_rank", "district", "baseline_wp",
                                        "invested_wp", "cumulative_expected_seats"]].copy()
        path_display.columns = ["Step", "District", "Before", "After", "Cumulative E[seats]"]
        path_display["Step"] = path_display["Step"].astype(int)
        path_display["District"] = path_display["District"].astype(int)
        path_display["Before"] = path_display["Before"].apply(lambda x: fmt_pct(x))
        path_display["After"] = path_display["After"].apply(lambda x: fmt_pct(x))
        path_display["Cumulative E[seats]"] = path_display["Cumulative E[seats]"].apply(lambda x: f"{x:.1f}")
        st.dataframe(path_display, use_container_width=True, hide_index=True)

    st.caption("Path optimizer is pre-computed at 48% statewide D. Results at other environments are approximate.")
else:
    st.info("Path optimizer data not available. Run `python cli.py session8` to generate.")
