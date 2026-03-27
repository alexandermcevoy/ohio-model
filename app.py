"""
Ohio House Election Model — Stakeholder Dashboard

Entry point for the Streamlit multi-page app.
Run with: streamlit run app.py
"""

import streamlit as st

st.set_page_config(
    page_title="Ohio House Election Model",
    page_icon=":ballot_box:",
    layout="wide",
)

from gui.data_loader import load_targeting, load_scenarios, load_win_probs, data_refresh_time
from gui.styles import WIDE_CSS, tier_badge, fmt_pct, fmt_margin, lean_to_margin, TIER_LABELS

# ---------------------------------------------------------------------------
# CSS
# ---------------------------------------------------------------------------

st.markdown(WIDE_CSS, unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Sidebar — shared across all pages
# ---------------------------------------------------------------------------

with st.sidebar:
    st.title("Ohio House Model")
    st.caption("2026 Targeting Dashboard")

    if "statewide_d" not in st.session_state:
        st.session_state["statewide_d"] = 48.0

    st.session_state["statewide_d"] = st.slider(
        "Statewide D Environment (%)",
        min_value=40.0, max_value=55.0, step=0.5,
        value=st.session_state["statewide_d"],
        help="Set the assumed statewide two-party Democratic share. "
             "This is a planning assumption, not a prediction.",
    )

    st.divider()
    st.caption(f"Data refreshed: {data_refresh_time()}")
    st.caption("Model v1.6 | 10,000 sims/env")

# ---------------------------------------------------------------------------
# Main page — Overview Dashboard
# ---------------------------------------------------------------------------

st.title("Ohio House Election Model")
st.markdown("**2026 Targeting Dashboard** — Explore scenarios, pickup targets, and district profiles.")

# Load data
targeting = load_targeting()
scenarios = load_scenarios()

selected_d = st.session_state["statewide_d"]

# Find the closest scenario row
scenario_row = scenarios.iloc[(scenarios["statewide_d_pct"] - selected_d).abs().argsort().iloc[0]]

# ---------------------------------------------------------------------------
# Key metrics
# ---------------------------------------------------------------------------

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        label="Expected D Seats",
        value=f"{scenario_row['mean_d_seats']:.1f}",
        delta=f"{scenario_row['mean_d_seats'] - 34:+.1f} vs current",
    )

with col2:
    st.metric(
        label="P(Hold 34+)",
        value=fmt_pct(scenario_row["prob_hold_34"]),
    )

with col3:
    st.metric(
        label="P(Reach 40+)",
        value=fmt_pct(scenario_row["prob_reach_40"]),
    )

with col4:
    st.metric(
        label="P(Majority 50+)",
        value=fmt_pct(scenario_row["prob_majority"]),
    )

st.caption(f"At **{selected_d:.1f}%** statewide D | 80% CI: [{scenario_row['p10_seats']:.0f}, {scenario_row['p90_seats']:.0f}] seats")

# ---------------------------------------------------------------------------
# Summary stats
# ---------------------------------------------------------------------------

st.divider()

n_d_held = int((targeting["current_holder"] == "D").sum())
n_pickup = int(targeting["pickup_opportunity"].sum())
n_open_r = int(targeting[targeting["open_seat_2026"] & (targeting["current_holder"] == "R")].shape[0])
n_defensive = int(targeting["defensive_priority"].sum()) if "defensive_priority" in targeting.columns else 0

scol1, scol2, scol3, scol4 = st.columns(4)
scol1.metric("D-Held Seats", n_d_held)
scol2.metric("Pickup Targets", n_pickup)
scol3.metric("Open R Seats", n_open_r)
scol4.metric("Defensive Priorities", n_defensive)

# ---------------------------------------------------------------------------
# Core portfolio summary
# ---------------------------------------------------------------------------

st.divider()
st.subheader("Core Portfolio — Invest Regardless of Environment")

# Load win probs to get current WP
win_probs = load_win_probs()

# Core districts (from Session 9 analysis)
core_districts = [31, 36, 49, 52, 35, 64, 17]

# Get win probs at selected environment
wp_at_sel = win_probs[win_probs["statewide_d_pct"].round(1) == round(selected_d, 1)]

core_data = targeting[targeting["district"].isin(core_districts)].merge(
    wp_at_sel[["district", "win_prob"]], on="district", how="left",
).sort_values("composite_lean", ascending=False)

core_data["expected_margin"] = core_data["composite_lean"].apply(
    lambda x: lean_to_margin(x, selected_d)
)

display_cols = {
    "district": "District",
    "expected_margin": "Exp. Margin",
    "tier": "Tier",
    "open_seat_2026": "Open 2026?",
    "win_prob": f"Win Prob @ {selected_d:.0f}%",
}

if not core_data.empty:
    display_df = core_data[list(display_cols.keys())].rename(columns=display_cols)
    display_df["Exp. Margin"] = display_df["Exp. Margin"].apply(fmt_margin)
    display_df[f"Win Prob @ {selected_d:.0f}%"] = display_df[f"Win Prob @ {selected_d:.0f}%"].apply(
        lambda x: fmt_pct(x) if x is not None and not (isinstance(x, float) and x != x) else "n/a"
    )
    display_df["Tier"] = display_df["Tier"].map(TIER_LABELS).fillna(display_df["Tier"])
    display_df["Open 2026?"] = display_df["Open 2026?"].map({True: "Yes", False: ""})
    st.dataframe(display_df, use_container_width=True, hide_index=True)

st.markdown(
    "These 7 R-held districts have >25% win probability even in a mediocre 46% D environment. "
    "Three are **open seats** (31, 52, 35) where the incumbency advantage disappears entirely."
)

st.info(
    "Use the sidebar slider to explore different statewide environments. "
    "Navigate to **Scenario Explorer**, **Pickup Portfolio**, or **District Profiles** "
    "in the sidebar for deeper analysis.",
    icon="\u2139\ufe0f",
)
