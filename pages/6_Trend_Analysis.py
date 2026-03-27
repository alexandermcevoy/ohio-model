"""
6_Trend_Analysis.py — Per-district partisan trend visualization.

Shows which districts are trending D vs. R over 2010-2024, using the block
backbone's 8-year statewide race history. Key views:
  1. Scatter: expected margin (x) vs trend slope (y) — the "money chart"
  2. Arrow chart: current margin → projected margin for pickup targets
  3. Filtered data table
"""

import streamlit as st
import pandas as pd

from gui.data_loader import load_targeting, load_trends
from gui.charts import lean_vs_trend_scatter, trend_arrow_chart
from gui.styles import (
    WIDE_CSS, TIER_COLORS, TIER_LABELS, TIER_ORDER,
    fmt_lean, fmt_margin, lean_to_margin,
)

st.set_page_config(page_title="Trend Analysis", page_icon=":chart_with_upwards_trend:", layout="wide")
st.markdown(WIDE_CSS, unsafe_allow_html=True)

st.title("District Partisan Trends (2010-2024)")
st.caption(
    "Linear trend in average statewide-race lean over 8 election cycles. "
    "Positive trend = district shifting toward Democrats relative to Ohio. "
    "Built from Census block backbone vote surfaces."
)

# ── Shared state ──────────────────────────────────────────────────────────────

if "statewide_d" not in st.session_state:
    st.session_state["statewide_d"] = 48.0

selected_d = st.session_state["statewide_d"]

# ── Load data ──────────────────────────────────────────────────────────────────

targeting = load_targeting()
trends = load_trends()

if trends is None:
    st.error("Trend data not found. Run `python cli.py trends` first.")
    st.stop()

# Merge
df = targeting.merge(trends, on="district", how="left", suffixes=("", "_trend"))

# Precompute margin columns
df["expected_margin"] = df["composite_lean"].apply(lambda x: lean_to_margin(x, selected_d))

# ── Key metrics ────────────────────────────────────────────────────────────────

n_d = (df["trend_dir"] == "trending_d").sum()
n_r = (df["trend_dir"] == "trending_r").sum()
n_s = (df["trend_dir"] == "stable").sum()

pickups = df[df["pickup_opportunity"] == True]
pickups_d = pickups[pickups["trend_dir"] == "trending_d"]
pickups_r = pickups[pickups["trend_dir"] == "trending_r"]

col1, col2, col3, col4 = st.columns(4)
col1.metric("Trending D", n_d, help="Districts shifting toward D relative to state")
col2.metric("Trending R", n_r, help="Districts shifting toward R relative to state")
col3.metric("Pickup targets trending D", len(pickups_d))
col4.metric("Pickup targets trending R", f"{len(pickups_r)}", delta=f"-{len(pickups_r)}", delta_color="inverse")

# ── Scatter: margin vs trend ─────────────────────────────────────────────────

st.subheader("Expected Margin vs. Trend")
st.caption(f"At **{selected_d:.1f}%** statewide D. Upper-left quadrant = best emerging opportunities (R-winning but trending D)")

focus = st.radio(
    "Focus",
    ["All districts", "Pickup targets only", "Competitive only (tossup + lean)"],
    horizontal=True,
)

plot_df = df.copy()
if focus == "Pickup targets only":
    plot_df = plot_df[plot_df["pickup_opportunity"] == True]
elif focus == "Competitive only (tossup + lean)":
    plot_df = plot_df[plot_df["tier"].isin(["tossup", "lean_d", "lean_r"])]

fig_scatter = lean_vs_trend_scatter(plot_df, statewide_d=selected_d,
                                     highlight_pickups=(focus != "Pickup targets only"))
st.plotly_chart(fig_scatter, use_container_width=True)

# ── Arrow chart: current → projected ──────────────────────────────────────────

st.subheader("Projected Margin: Current vs. Trend-Adjusted")

years_fwd = st.slider("Years to project forward", min_value=2, max_value=8, value=4, step=2)

arrow_df = pickups.copy()
if not arrow_df.empty:
    fig_arrow = trend_arrow_chart(arrow_df, statewide_d=selected_d, years_forward=years_fwd)
    st.plotly_chart(fig_arrow, use_container_width=True)
    st.caption("* = open seat in 2026. Arrows show direction and magnitude of trend projection.")
else:
    st.info("No pickup targets to display.")

# ── Strategic insight boxes ────────────────────────────────────────────────────

st.subheader("Strategic Highlights")

col_left, col_right = st.columns(2)

with col_left:
    st.markdown("**Best opportunities** (D-winning + trending D)")
    best = df[
        (df["pickup_opportunity"] == True) &
        (df["expected_margin"] > 0) &
        (df["trend_dir"] == "trending_d")
    ].sort_values("trend_slope", ascending=False)
    if not best.empty:
        for _, r in best.iterrows():
            open_tag = " **OPEN**" if r.get("open_seat_2026") else ""
            margin_str = fmt_margin(r["expected_margin"])
            st.markdown(
                f"- **District {int(r['district'])}** "
                f"({margin_str}, "
                f"trend {r['trend_slope'] * 100:+.2f} pts/yr, "
                f"R\u00b2={r['trend_r2']:.2f}){open_tag}"
            )
    else:
        st.write("None found.")

with col_right:
    st.markdown("**Emerging targets** (R-winning but trending D fast)")
    emerging = df[
        (df["pickup_opportunity"] == True) &
        (df["expected_margin"] < 0) &
        (df["trend_dir"] == "trending_d")
    ].sort_values("trend_slope", ascending=False).head(8)
    if not emerging.empty:
        for _, r in emerging.iterrows():
            years_to_even = abs(r["expected_margin"]) / r["trend_slope"] if r["trend_slope"] > 0 else float("inf")
            margin_str = fmt_margin(r["expected_margin"])
            st.markdown(
                f"- **District {int(r['district'])}** "
                f"({margin_str}, "
                f"trend {r['trend_slope'] * 100:+.2f} pts/yr, "
                f"~{years_to_even:.0f}yr to even)"
            )
    else:
        st.write("None found.")

# Warning: eroding targets
st.markdown("---")
st.markdown("**Warning: Pickup targets trending R** (swimming against the current)")
eroding = df[
    (df["pickup_opportunity"] == True) &
    (df["trend_dir"] == "trending_r")
].sort_values("trend_slope").head(8)
if not eroding.empty:
    cols = st.columns(min(4, len(eroding)))
    for i, (_, r) in enumerate(eroding.iterrows()):
        with cols[i % 4]:
            open_tag = " (OPEN)" if r.get("open_seat_2026") else ""
            margin_str = fmt_margin(r["expected_margin"])
            st.metric(
                f"D-{int(r['district'])}{open_tag}",
                margin_str,
                f"{r['trend_slope'] * 100:+.2f} pts/yr",
                delta_color="inverse",
            )

# ── Data table ─────────────────────────────────────────────────────────────────

st.subheader("Full District Trend Table")

show_cols = st.multiselect(
    "Filter by trend direction",
    ["trending_d", "trending_r", "stable"],
    default=["trending_d", "trending_r"],
)

holder_filter = st.radio("Holder", ["All", "R-held only", "D-held only"], horizontal=True)

table_df = df.copy()
table_df["trend_pts_yr"] = table_df["trend_slope"] * 100
table_df["shift_pts"] = table_df["trend_shift"] * 100

if show_cols:
    table_df = table_df[table_df["trend_dir"].isin(show_cols)]
if holder_filter == "R-held only":
    table_df = table_df[table_df["current_holder"] == "R"]
elif holder_filter == "D-held only":
    table_df = table_df[table_df["current_holder"] == "D"]

display_cols = [
    "district", "expected_margin", "tier", "current_holder",
    "trend_pts_yr", "shift_pts", "trend_r2", "trend_dir",
    "pickup_opportunity", "open_seat_2026",
]
display_cols = [c for c in display_cols if c in table_df.columns]

table_out = table_df[display_cols].sort_values("trend_pts_yr", ascending=False)
table_out = table_out.rename(columns={
    "expected_margin": f"Margin @ {selected_d:.0f}%",
    "trend_pts_yr": "Trend (pts/yr)",
    "shift_pts": "14yr Shift (pts)",
    "trend_r2": "R\u00b2",
    "trend_dir": "Direction",
    "pickup_opportunity": "Pickup?",
    "open_seat_2026": "Open 2026?",
})

st.dataframe(
    table_out.style.format({
        f"Margin @ {selected_d:.0f}%": "{:+.3f}",
        "Trend (pts/yr)": "{:+.2f}",
        "14yr Shift (pts)": "{:+.1f}",
        "R\u00b2": "{:.3f}",
    }).background_gradient(
        subset=["Trend (pts/yr)"],
        cmap="RdBu",
        vmin=-2.0, vmax=2.0,
    ),
    use_container_width=True,
    height=600,
)
