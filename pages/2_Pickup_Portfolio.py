"""
Pickup Portfolio — S-curve win probability chart with Core/Stretch/Long-Shot tiers.

The primary strategic output: which districts are worth investing in across
different statewide environments.
"""

import streamlit as st
import pandas as pd

from gui.data_loader import load_win_probs, load_targeting, load_investment_priority
from gui.compute import classify_portfolio
from gui.charts import scurve_portfolio_chart
from gui.styles import fmt_pct, fmt_lean, fmt_margin, lean_to_margin, TIER_LABELS, PORTFOLIO_COLORS

st.set_page_config(page_title="Pickup Portfolio", page_icon=":dart:", layout="wide")
st.title("Pickup Portfolio")

st.markdown(
    "**The key insight:** you don't need to predict the environment to decide where to invest. "
    "These district rankings are stable across a wide range of plausible 2026 scenarios."
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

win_probs = load_win_probs()
targeting = load_targeting()
investment = load_investment_priority()

# Classify portfolio tiers
portfolio = classify_portfolio(win_probs)

# Filter to R-held pickup targets only
r_held = set(targeting[targeting["current_holder"] == "R"]["district"])
portfolio = portfolio[portfolio["district"].isin(r_held)]

# ---------------------------------------------------------------------------
# Filters
# ---------------------------------------------------------------------------

with st.sidebar:
    st.subheader("Portfolio Filters")
    show_tiers = st.multiselect(
        "Show tiers",
        options=["Core", "Stretch", "Long-Shot"],
        default=["Core", "Stretch"],
    )
    highlight_open = st.checkbox("Highlight open seats", value=True)

portfolio_filtered = portfolio[portfolio["portfolio_tier"].isin(show_tiers)]

# ---------------------------------------------------------------------------
# S-curve chart
# ---------------------------------------------------------------------------

st.plotly_chart(
    scurve_portfolio_chart(win_probs, targeting, portfolio_filtered, selected_d),
    use_container_width=True,
)

# ---------------------------------------------------------------------------
# Portfolio summary tables
# ---------------------------------------------------------------------------

st.divider()

# Win probs at selected environment
wp_at_sel = win_probs[win_probs["statewide_d_pct"].round(1) == round(selected_d, 1)]

for ptier in ["Core", "Stretch", "Long-Shot"]:
    if ptier not in show_tiers:
        continue

    tier_districts = portfolio[portfolio["portfolio_tier"] == ptier]["district"]
    if tier_districts.empty:
        continue

    color = PORTFOLIO_COLORS[ptier]
    st.markdown(f"### <span style='color:{color}'>{ptier} Portfolio</span>", unsafe_allow_html=True)

    if ptier == "Core":
        st.caption("Invest regardless of environment (>25% WP even at 46% D)")
    elif ptier == "Stretch":
        st.caption("Invest if early signals are favorable (viable at 48%+ D)")
    else:
        st.caption("Only in a wave (requires 50%+ D)")

    tier_data = targeting[targeting["district"].isin(tier_districts)].merge(
        wp_at_sel[["district", "win_prob"]], on="district", how="left",
    )

    # Add win probs at 46, 48, 50 for context
    for env in [46.0, 48.0, 50.0]:
        wp_env = win_probs[win_probs["statewide_d_pct"] == env][["district", "win_prob"]].rename(
            columns={"win_prob": f"wp_{env:.0f}"}
        )
        tier_data = tier_data.merge(wp_env, on="district", how="left")

    tier_data = tier_data.sort_values("composite_lean", ascending=False)

    tier_data["expected_margin"] = tier_data["composite_lean"].apply(
        lambda x: lean_to_margin(x, selected_d)
    )
    display = tier_data[["district", "expected_margin", "open_seat_2026",
                          "wp_46", "wp_48", "wp_50"]].copy()
    display.columns = ["District", f"Margin @ {selected_d:.0f}%", "Open 2026?", "WP @ 46%", "WP @ 48%", "WP @ 50%"]
    display[f"Margin @ {selected_d:.0f}%"] = display[f"Margin @ {selected_d:.0f}%"].apply(fmt_margin)
    display["Open 2026?"] = display["Open 2026?"].map({True: "Yes", False: ""})
    for col in ["WP @ 46%", "WP @ 48%", "WP @ 50%"]:
        display[col] = display[col].apply(lambda x: fmt_pct(x) if pd.notna(x) else "n/a")

    st.dataframe(display, use_container_width=True, hide_index=True)

# ---------------------------------------------------------------------------
# Interpretation
# ---------------------------------------------------------------------------

st.divider()
st.markdown("""
**How to read this:**
- **Core** districts are worth funding under any plausible 2026 scenario. If you can only fund 7 races, fund these.
- **Stretch** targets become viable if polling, fundraising, and candidate recruitment suggest a favorable cycle (48%+ environment). Expand here when early signals are positive.
- **Long-Shot** districts only come into play in a historic wave. Don't invest primary resources here unless everything else is breaking your way.
- **Open seats** (marked with stars) climb fastest because the ~6-point incumbency advantage disappears.
""")
