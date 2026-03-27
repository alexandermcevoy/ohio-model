"""
Map — Choropleth of Ohio House districts.

Color by composite lean, tier, or win probability.
Requires GeoJSON pre-conversion: python cli.py geojson
"""

import streamlit as st
import pandas as pd

from gui.data_loader import load_targeting, load_win_probs, load_geojson
from gui.charts import district_choropleth
from gui.styles import TIER_LABELS, fmt_pct, lean_to_margin

st.set_page_config(page_title="District Map", page_icon=":world_map:", layout="wide")
st.title("District Map")

# ---------------------------------------------------------------------------
# Shared state
# ---------------------------------------------------------------------------

if "statewide_d" not in st.session_state:
    st.session_state["statewide_d"] = 48.0

selected_d = st.session_state["statewide_d"]

# ---------------------------------------------------------------------------
# Load GeoJSON
# ---------------------------------------------------------------------------

geojson = load_geojson()

if geojson is None:
    st.warning(
        "GeoJSON file not found. Run `python cli.py geojson` to convert the district "
        "shapefile for map rendering.",
        icon="\u26a0\ufe0f",
    )
    st.info(
        "The map view requires a one-time conversion of the district shapefile to GeoJSON format. "
        "This takes a few seconds."
    )
    st.stop()

# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------

targeting = load_targeting()
win_probs = load_win_probs()

# Color mode selector
color_mode = st.radio(
    "Color by:",
    options=["Expected Margin", "Tier", "Win Probability"],
    horizontal=True,
)

# Prepare data for map
map_data = targeting[["district", "composite_lean", "tier", "open_seat_2026",
                       "current_holder"]].copy()
map_data["expected_margin"] = map_data["composite_lean"].apply(
    lambda x: lean_to_margin(x, selected_d)
)

if color_mode == "Win Probability":
    wp_at_sel = win_probs[win_probs["statewide_d_pct"].round(1) == round(selected_d, 1)]
    map_data = map_data.merge(wp_at_sel[["district", "win_prob"]], on="district", how="left")
    color_col = "win_prob"
    st.caption(f"Win probability at {selected_d:.1f}% statewide D")
elif color_mode == "Tier":
    color_col = "tier"
else:
    color_col = "expected_margin"
    st.caption(f"Expected margin at {selected_d:.1f}% statewide D")

# ---------------------------------------------------------------------------
# Render map
# ---------------------------------------------------------------------------

st.plotly_chart(
    district_choropleth(geojson, map_data, color_col, selected_d),
    use_container_width=True,
)

# ---------------------------------------------------------------------------
# Click-to-profile (workaround: dropdown)
# ---------------------------------------------------------------------------

st.divider()
st.markdown("**Explore a district:** Select a district number below to view its full profile.")

district_select = st.selectbox(
    "Jump to district profile",
    options=sorted(targeting["district"].unique()),
    index=None,
    placeholder="Choose a district...",
)

if district_select:
    st.session_state["selected_district"] = int(district_select)
    st.switch_page("pages/3_District_Profiles.py")
