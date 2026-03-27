"""
District Profiles — Single-district deep dive.

Everything about one district on one screen: lean, win probability,
voter universe, demographics, house history, anomaly flags.
"""

import streamlit as st
import pandas as pd
import numpy as np

from gui.data_loader import (
    load_targeting, load_composite_lean, load_demographics,
    load_redistricting, load_anomaly_flags, load_voter_universe,
    load_win_probs, load_investment_priority, load_sigma,
)
from gui.charts import district_win_prob_chart, race_lean_chart, voter_composition_chart
from gui.styles import tier_badge, fmt_pct, fmt_lean, fmt_margin, lean_to_margin, fmt_dollar, TIER_LABELS

st.set_page_config(page_title="District Profiles", page_icon=":mag:", layout="wide")

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
composite = load_composite_lean()
demographics = load_demographics()
redistricting = load_redistricting()
anomaly_flags = load_anomaly_flags()
voter_universe = load_voter_universe()
win_probs = load_win_probs()
investment = load_investment_priority()
sigma = load_sigma()

# ---------------------------------------------------------------------------
# District selector
# ---------------------------------------------------------------------------

# Build selector labels
selector_options = []
for _, row in targeting.sort_values("district").iterrows():
    d = int(row["district"])
    tier_label = TIER_LABELS.get(row.get("tier", ""), "")
    lean = row.get("composite_lean", 0)
    lean_pts = lean * 100
    lean_str = f"D+{lean_pts:.1f}" if lean_pts > 0.05 else (f"R+{abs(lean_pts):.1f}" if lean_pts < -0.05 else "EVEN")
    holder = row.get("current_holder", "?")
    open_tag = " [OPEN]" if row.get("open_seat_2026", False) else ""
    selector_options.append(f"District {d} — {tier_label} — {lean_str} ({holder}){open_tag}")

# Default to District 52 if available, else first
default_idx = 0
for i, opt in enumerate(selector_options):
    if "District 52" in opt:
        default_idx = i
        break

if "selected_district" in st.session_state:
    # If navigated from another page
    for i, opt in enumerate(selector_options):
        if f"District {st.session_state['selected_district']}" in opt:
            default_idx = i
            break

selected_label = st.selectbox("Select District", selector_options, index=default_idx)
district = int(selected_label.split(" ")[1])
st.session_state["selected_district"] = district

# ---------------------------------------------------------------------------
# Get district data using export.py's data assembly function
# ---------------------------------------------------------------------------

from gui.district_data import get_district_data

data = get_district_data(
    district=district,
    targeting_df=targeting,
    composite_df=composite,
    demographics_df=demographics,
    redistricting_df=redistricting,
    anomaly_df=anomaly_flags,
    voter_universe_df=voter_universe,
    win_prob_df=win_probs,
    investment_df=investment,
    sigma_df=sigma,
)

# ---------------------------------------------------------------------------
# Header banner
# ---------------------------------------------------------------------------

st.markdown(f"# District {district}")

bcol1, bcol2, bcol3, bcol4, bcol5 = st.columns(5)

with bcol1:
    tier = data["tier"]
    st.markdown(f"**Tier (48%):** {tier_badge(tier)}", unsafe_allow_html=True)
    # Show all three environment tiers if available
    row_data = targeting[targeting["district"] == district]
    if not row_data.empty:
        r = row_data.iloc[0]
        t46 = TIER_LABELS.get(r.get("tier_46", ""), "")
        t50 = TIER_LABELS.get(r.get("tier_50", ""), "")
        if t46 and t50:
            st.caption(f"46%: {t46} · 50%: {t50}")

with bcol2:
    margin = lean_to_margin(data["composite_lean"], selected_d)
    st.metric(f"Exp. Margin @ {selected_d:.0f}%", fmt_margin(margin))

with bcol3:
    st.metric("Current Holder", data["current_holder"].upper())

with bcol4:
    if data["open_seat_2026"]:
        st.success(f"OPEN SEAT — {data['open_seat_reason']}")
    else:
        st.info(f"Incumbent: {data.get('current_incumbent_name', 'Unknown')}")

with bcol5:
    flip_thresh = data.get("flip_threshold")
    if flip_thresh is not None:
        st.metric("Flip Threshold", f"{flip_thresh * 100:.1f}%")

# ---------------------------------------------------------------------------
# Anomaly flags
# ---------------------------------------------------------------------------

if data.get("anomaly_flags"):
    for flag in data["anomaly_flags"]:
        st.warning(
            f"**{flag['year']} anomaly** ({flag['severity']}): "
            f"residual = {flag['residual']:+.3f} — {flag['explanation']}",
            icon="\u26a0\ufe0f",
        )

# ---------------------------------------------------------------------------
# Main content: four columns
# ---------------------------------------------------------------------------

st.divider()
col1, col2 = st.columns(2)

# --- Column 1: Partisan lean ---
with col1:
    st.subheader("Partisan Profile")
    st.plotly_chart(race_lean_chart(data), use_container_width=True)

    # Targeting info
    st.markdown(f"**Target Mode:** {data.get('target_mode', 'unknown')}")
    st.markdown(f"**Swing SD:** {data.get('swing_sd', 'n/a')}")
    st.markdown(f"**Contested Cycles:** {data.get('n_contested', 0)}")
    st.markdown(f"**Sensitivity:** {data.get('composite_sensitivity', 'n/a')} "
                f"(most sensitive to: {data.get('most_sensitive_race', 'n/a')})")

# --- Column 2: Win probability ---
with col2:
    st.subheader("Win Probability")
    st.plotly_chart(
        district_win_prob_chart(win_probs, district, selected_d),
        use_container_width=True,
    )

    prob = data.get("probabilistic")
    if prob:
        pcol1, pcol2, pcol3 = st.columns(3)
        pcol1.metric("WP @ 46%", fmt_pct(prob.get("wp_46")))
        pcol2.metric("WP @ 48%", fmt_pct(prob.get("wp_48")))
        pcol3.metric("WP @ 50%", fmt_pct(prob.get("wp_50")))

        scol1, scol2 = st.columns(2)
        scol1.markdown(f"**Investment Rank:** #{prob.get('investment_rank', 'n/a')}")
        scol2.markdown(f"**Marginal WP:** {prob.get('marginal_wp', 'n/a'):.2f}" if prob.get("marginal_wp") else "**Marginal WP:** n/a")
        st.caption(f"sigma_i = {prob.get('sigma_i', 'n/a'):.4f} ({prob.get('sigma_source', 'n/a')})" if prob.get("sigma_i") else "")

# ---------------------------------------------------------------------------
# Second row: voter universe + demographics
# ---------------------------------------------------------------------------

st.divider()
col3, col4 = st.columns(2)

# --- Column 3: Voter Universe ---
with col3:
    st.subheader("Voter Universe")
    vu = data.get("voter_universe")
    if vu:
        st.plotly_chart(voter_composition_chart(vu), use_container_width=True)

        vcol1, vcol2 = st.columns(2)
        vcol1.metric("Active Voters", f"{vu['total_active_voters']:,}")
        vcol2.metric("Partisan Advantage", f"{vu['partisan_advantage']:+.1%}" if vu.get("partisan_advantage") else "n/a")

        mcol1, mcol2 = st.columns(2)
        mcol1.metric("Mobilization Targets", f"{vu['n_mobilization_targets']:,}" if vu.get("n_mobilization_targets") else "n/a")
        mcol2.metric("Persuasion Targets", f"{vu['n_persuasion_targets']:,}" if vu.get("n_persuasion_targets") else "n/a")

        tcol1, tcol2, tcol3 = st.columns(3)
        tcol1.markdown(f"**Turnout 2024:** {vu['turnout_2024']:.1%}" if vu.get("turnout_2024") else "")
        tcol2.markdown(f"**Turnout 2022:** {vu['turnout_2022']:.1%}" if vu.get("turnout_2022") else "")
        tcol3.markdown(f"**Dropoff:** {vu['turnout_dropoff']:.2f}" if vu.get("turnout_dropoff") else "")

        st.markdown(f"**Target Mode (Voter File):** {vu.get('target_mode_voterfile', 'n/a')}")
    else:
        st.info("Voter universe data not available. Run `python cli.py voters --build` to generate.")

# --- Column 4: Demographics ---
with col4:
    st.subheader("Demographics")

    demo_items = [
        ("College Attainment", data.get("college_pct"), fmt_pct),
        ("Median Income", data.get("median_income"), fmt_dollar),
        ("White %", data.get("white_pct"), fmt_pct),
        ("Black %", data.get("black_pct"), fmt_pct),
        ("Hispanic %", data.get("hispanic_pct"), fmt_pct),
        ("Pop Density", data.get("pop_density"), lambda x: f"{x:,.0f} /sq mi" if x else "n/a"),
    ]

    for label, value, formatter in demo_items:
        st.markdown(f"**{label}:** {formatter(value)}")

    if data.get("total_pop"):
        st.markdown(f"**Total Population:** {data['total_pop']:,}")

# ---------------------------------------------------------------------------
# House race history
# ---------------------------------------------------------------------------

st.divider()
st.subheader("House Race History")

years = [2024, 2022, 2020, 2018]
history_rows = []
reliable_years = data.get("years_reliable", "")

for y in years:
    reliable = str(y) in str(reliable_years)
    history_rows.append({
        "Year": y,
        "D Share": fmt_pct(data.get(f"dem_share_{y}")) if data.get(f"dem_share_{y}") is not None else "—",
        "Margin": f"{data.get(f'margin_{y}', 0):+.1%}" if data.get(f"margin_{y}") is not None else "—",
        "Winner": data.get(f"winner_{y}", "—") or "—",
        "Contested": "Yes" if data.get(f"contested_{y}") else "No",
        "Candidate Effect": f"{data.get(f'glm_effect_{y}', 0):+.3f}" if data.get(f"glm_effect_{y}") is not None else "—",
        "Reliable": "Yes" if reliable else "No",
    })

st.dataframe(pd.DataFrame(history_rows), use_container_width=True, hide_index=True)

# ---------------------------------------------------------------------------
# Redistricting context
# ---------------------------------------------------------------------------

with st.expander("Redistricting Context"):
    st.markdown(f"**Old → Interim:** {data.get('overlap_category', 'unknown')} (Jaccard: {data.get('jaccard_old_interim', 'n/a')})")
    st.markdown(f"**Interim → Final:** {data.get('overlap_category_interim_final', 'unknown')} (Jaccard: {data.get('jaccard_interim_final', 'n/a')})")
    st.markdown(f"**Reliable Years:** {data.get('years_reliable', 'unknown')}")
