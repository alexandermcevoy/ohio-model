"""
charts.py — Plotly figure builders for the Streamlit GUI.

Each function returns a plotly.graph_objects.Figure ready for st.plotly_chart().
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px

from gui.styles import (
    TIER_COLORS, TIER_LABELS, PORTFOLIO_COLORS, HOLDER_COLORS,
)


# ---------------------------------------------------------------------------
# Scenario Explorer: seat distribution with CI bands
# ---------------------------------------------------------------------------


def seat_distribution_chart(
    scenarios_df: pd.DataFrame,
    selected_d: float,
) -> go.Figure:
    """
    Area chart: mean D seats with p10–p90 shaded band.
    Vertical line at selected statewide D%. Horizontal reference lines at 34/40/50.
    """
    df = scenarios_df.sort_values("statewide_d_pct")

    fig = go.Figure()

    # p10–p90 band
    fig.add_trace(go.Scatter(
        x=df["statewide_d_pct"], y=df["p90_seats"],
        mode="lines", line=dict(width=0),
        showlegend=False, hoverinfo="skip",
    ))
    fig.add_trace(go.Scatter(
        x=df["statewide_d_pct"], y=df["p10_seats"],
        mode="lines", line=dict(width=0),
        fill="tonexty", fillcolor="rgba(21,101,192,0.15)",
        name="80% CI (p10–p90)", hoverinfo="skip",
    ))

    # p25–p75 band
    fig.add_trace(go.Scatter(
        x=df["statewide_d_pct"], y=df["p75_seats"],
        mode="lines", line=dict(width=0),
        showlegend=False, hoverinfo="skip",
    ))
    fig.add_trace(go.Scatter(
        x=df["statewide_d_pct"], y=df["p25_seats"],
        mode="lines", line=dict(width=0),
        fill="tonexty", fillcolor="rgba(21,101,192,0.25)",
        name="50% CI (p25–p75)", hoverinfo="skip",
    ))

    # Mean line
    fig.add_trace(go.Scatter(
        x=df["statewide_d_pct"], y=df["mean_d_seats"],
        mode="lines+markers", line=dict(color="#1565C0", width=3),
        marker=dict(size=4),
        name="Expected D seats",
        hovertemplate="<b>%{x:.1f}% D</b><br>Mean: %{y:.1f} seats<extra></extra>",
    ))

    # Horizontal reference lines
    for seats, label, color, dash in [
        (34, "Current (34)", "#757575", "dot"),
        (40, "Veto-proof (40)", "#E65100", "dash"),
        (50, "Majority (50)", "#1B5E20", "dashdot"),
    ]:
        fig.add_hline(y=seats, line=dict(color=color, width=1, dash=dash),
                       annotation_text=label, annotation_position="top left",
                       annotation_font_size=11)

    # Selected environment vertical line
    fig.add_vline(x=selected_d, line=dict(color="#D32F2F", width=2, dash="dash"),
                   annotation_text=f"{selected_d:.1f}%",
                   annotation_position="top right",
                   annotation_font_color="#D32F2F")

    fig.update_layout(
        title="Projected D Seats by Statewide Environment",
        xaxis_title="Statewide D Two-Party Share (%)",
        yaxis_title="D Seats",
        xaxis=dict(range=[43, 53], dtick=1),
        yaxis=dict(range=[25, 55]),
        legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01),
        height=500,
        margin=dict(l=50, r=30, t=50, b=50),
        hovermode="x unified",
    )
    return fig


# ---------------------------------------------------------------------------
# Pickup Portfolio: S-curve win probability chart
# ---------------------------------------------------------------------------


def scurve_portfolio_chart(
    win_probs_df: pd.DataFrame,
    targeting_df: pd.DataFrame,
    portfolio_df: pd.DataFrame,
    selected_d: float,
) -> go.Figure:
    """
    One sigmoid curve per pickup target district, colored by portfolio tier.
    Open seats get star markers.
    """
    fig = go.Figure()

    # Merge portfolio tier and targeting info
    districts = portfolio_df.merge(
        targeting_df[["district", "composite_lean", "open_seat_2026", "tier"]],
        on="district", how="left",
    )

    # Plot each tier group
    for ptier in ["Core", "Stretch", "Long-Shot"]:
        tier_districts = districts[districts["portfolio_tier"] == ptier]
        color = PORTFOLIO_COLORS.get(ptier, "#757575")

        for _, row in tier_districts.iterrows():
            d = int(row["district"])
            is_open = bool(row.get("open_seat_2026", False))
            dwp = win_probs_df[win_probs_df["district"] == d].sort_values("statewide_d_pct")

            marker_symbol = "star" if is_open else "circle"
            marker_size = 7 if is_open else 4
            open_label = " (OPEN)" if is_open else ""

            fig.add_trace(go.Scatter(
                x=dwp["statewide_d_pct"],
                y=dwp["win_prob"],
                mode="lines+markers",
                line=dict(color=color, width=2),
                marker=dict(symbol=marker_symbol, size=marker_size, color=color),
                name=f"D-{d}{open_label}",
                legendgroup=ptier,
                legendgrouptitle_text=ptier,
                hovertemplate=(
                    f"<b>District {d}</b> ({ptier}){open_label}<br>"
                    "Statewide D: %{x:.1f}%<br>"
                    "Win prob: %{y:.1%}<extra></extra>"
                ),
            ))

    # 50% win prob reference line
    fig.add_hline(y=0.50, line=dict(color="#757575", width=1, dash="dot"),
                   annotation_text="50% WP", annotation_position="right")

    # Selected environment
    fig.add_vline(x=selected_d, line=dict(color="#D32F2F", width=2, dash="dash"),
                   annotation_text=f"{selected_d:.1f}%",
                   annotation_position="top right",
                   annotation_font_color="#D32F2F")

    fig.update_layout(
        title="Win Probability by Statewide Environment — Pickup Targets",
        xaxis_title="Statewide D Two-Party Share (%)",
        yaxis_title="P(D wins district)",
        xaxis=dict(range=[44, 52], dtick=1),
        yaxis=dict(range=[0, 1], tickformat=".0%"),
        legend=dict(yanchor="top", y=0.99, xanchor="right", x=1.15),
        height=550,
        margin=dict(l=50, r=120, t=50, b=50),
    )
    return fig


# ---------------------------------------------------------------------------
# District Profiles: single-district win prob curve
# ---------------------------------------------------------------------------


def district_win_prob_chart(
    win_probs_df: pd.DataFrame,
    district: int,
    selected_d: float,
) -> go.Figure:
    """S-curve for a single district across all environments."""
    dwp = win_probs_df[win_probs_df["district"] == district].sort_values("statewide_d_pct")

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=dwp["statewide_d_pct"], y=dwp["win_prob"],
        mode="lines+markers",
        line=dict(color="#1565C0", width=3),
        marker=dict(size=5),
        hovertemplate="Statewide D: %{x:.1f}%<br>Win prob: %{y:.1%}<extra></extra>",
    ))

    fig.add_hline(y=0.50, line=dict(color="#757575", width=1, dash="dot"))

    # Mark selected environment
    sel_row = dwp[dwp["statewide_d_pct"].round(1) == round(selected_d, 1)]
    if not sel_row.empty:
        wp_val = sel_row["win_prob"].iloc[0]
        fig.add_trace(go.Scatter(
            x=[selected_d], y=[wp_val],
            mode="markers",
            marker=dict(size=12, color="#D32F2F", symbol="diamond"),
            name=f"@ {selected_d:.1f}%: {wp_val:.0%}",
            showlegend=True,
        ))

    fig.update_layout(
        title=f"District {district} Win Probability",
        xaxis_title="Statewide D%",
        yaxis_title="P(D wins)",
        xaxis=dict(range=[43, 53]),
        yaxis=dict(range=[0, 1], tickformat=".0%"),
        height=350,
        margin=dict(l=40, r=20, t=40, b=40),
        showlegend=True,
    )
    return fig


# ---------------------------------------------------------------------------
# District Profiles: race-by-race lean bar chart
# ---------------------------------------------------------------------------


def race_lean_chart(data: dict) -> go.Figure:
    """Horizontal bar chart of individual race leans for a district."""
    races = [
        ("2024 President", data.get("lean_pre_2024")),
        ("2024 Senate", data.get("lean_uss_2024")),
        ("2022 Governor", data.get("lean_gov_2022")),
        ("2022 Senate", data.get("lean_uss_2022")),
        ("2022 SW Avg", data.get("lean_sw_avg_2022")),
        ("2020 President", data.get("lean_pre_2020")),
        ("2018 Governor", data.get("lean_gov_2018")),
        ("2018 Senate", data.get("lean_uss_2018")),
        ("2018 SW Avg", data.get("lean_sw_avg_2018")),
    ]
    labels = [r[0] for r in races if r[1] is not None]
    values = [r[1] for r in races if r[1] is not None]
    colors = ["#1565C0" if v >= 0 else "#D32F2F" for v in values]

    fig = go.Figure(go.Bar(
        x=values, y=labels,
        orientation="h",
        marker_color=colors,
        hovertemplate="%{y}: %{x:+.3f}<extra></extra>",
    ))

    fig.add_vline(x=0, line=dict(color="#757575", width=1))

    # Composite lean reference
    comp = data.get("composite_lean")
    if comp is not None:
        fig.add_vline(x=comp, line=dict(color="#000000", width=2, dash="dash"),
                       annotation_text=f"Composite: {comp:+.3f}",
                       annotation_position="top right")

    fig.update_layout(
        title="Partisan Lean by Race",
        xaxis_title="Lean (D − Statewide)",
        height=320,
        margin=dict(l=100, r=20, t=40, b=40),
    )
    return fig


# ---------------------------------------------------------------------------
# District Profiles: voter universe partisan composition
# ---------------------------------------------------------------------------


def voter_composition_chart(voter_data: dict) -> go.Figure:
    """Stacked horizontal bar showing partisan composition of voter universe."""
    categories = [
        ("Strong D", voter_data.get("pct_strong_d"), "#1565C0"),
        ("Lean D", voter_data.get("pct_lean_d"), "#42A5F5"),
        ("Crossover", voter_data.get("pct_crossover"), "#AB47BC"),
        ("Unaffiliated", voter_data.get("pct_unaffiliated"), "#BDBDBD"),
        ("Lean R", voter_data.get("pct_lean_r"), "#EF5350"),
        ("Strong R", voter_data.get("pct_strong_r"), "#B71C1C"),
    ]

    fig = go.Figure()
    for label, pct, color in categories:
        if pct is not None:
            fig.add_trace(go.Bar(
                x=[pct], y=["Voters"],
                orientation="h",
                name=label,
                marker_color=color,
                hovertemplate=f"{label}: %{{x:.1%}}<extra></extra>",
            ))

    fig.update_layout(
        barmode="stack",
        height=120,
        margin=dict(l=10, r=10, t=10, b=10),
        legend=dict(orientation="h", yanchor="bottom", y=1.0),
        xaxis=dict(tickformat=".0%", range=[0, 1]),
        yaxis=dict(visible=False),
    )
    return fig


# ---------------------------------------------------------------------------
# Investment Priority: marginal WP bar chart
# ---------------------------------------------------------------------------


def marginal_wp_bar_chart(
    investment_df: pd.DataFrame,
    targeting_df: pd.DataFrame,
    n: int = 20,
) -> go.Figure:
    """Horizontal bar chart of top N districts by marginal win probability."""
    df = investment_df.nlargest(n, "marginal_wp").copy()

    # Merge tier/open_seat only if not already present
    if "tier" not in df.columns or "open_seat_2026" not in df.columns:
        merge_cols = ["district"]
        if "tier" not in df.columns:
            merge_cols.append("tier")
        if "open_seat_2026" not in df.columns:
            merge_cols.append("open_seat_2026")
        df = df.merge(targeting_df[merge_cols], on="district", how="left")

    df = df.sort_values("marginal_wp", ascending=True)  # for horizontal bars

    colors = [TIER_COLORS.get(t, "#757575") for t in df["tier"]]
    labels = [
        f"D-{d}" + (" *" if o else "")
        for d, o in zip(df["district"], df["open_seat_2026"].fillna(False))
    ]

    fig = go.Figure(go.Bar(
        x=df["marginal_wp"], y=labels,
        orientation="h",
        marker_color=colors,
        hovertemplate=(
            "<b>District %{customdata[0]}</b><br>"
            "Marginal WP: %{x:.2f}<br>"
            "Win prob: %{customdata[1]:.1%}<extra></extra>"
        ),
        customdata=list(zip(df["district"], df["win_prob"])),
    ))

    fig.update_layout(
        title=f"Top {n} Districts by Marginal Win Probability",
        xaxis_title="Marginal WP (dP/d_lean)",
        height=max(400, n * 25),
        margin=dict(l=80, r=20, t=50, b=40),
    )
    return fig


# ---------------------------------------------------------------------------
# Investment Priority: path-to-target step chart
# ---------------------------------------------------------------------------


def path_to_target_chart(path_df: pd.DataFrame) -> go.Figure:
    """Step chart showing cumulative expected seats as investments are made."""
    df = path_df.sort_values("priority_rank")

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["priority_rank"], y=df["cumulative_expected_seats"],
        mode="lines+markers+text",
        line=dict(color="#1565C0", width=2, shape="hv"),
        marker=dict(size=8, color="#1565C0"),
        text=[f"D-{int(d)}" for d in df["district"]],
        textposition="top center",
        textfont=dict(size=10),
        hovertemplate=(
            "<b>Step %{x}</b><br>"
            "District: D-%{customdata[0]}<br>"
            "Baseline WP: %{customdata[1]:.1%}<br>"
            "After invest: %{customdata[2]:.1%}<br>"
            "Cumulative E[seats]: %{y:.1f}<extra></extra>"
        ),
        customdata=list(zip(
            df["district"].astype(int),
            df["baseline_wp"],
            df["invested_wp"],
        )),
    ))

    # Target line at 40
    fig.add_hline(y=40, line=dict(color="#E65100", width=1, dash="dash"),
                   annotation_text="Target: 40 seats")

    fig.update_layout(
        title="Path to Target: Greedy Investment Optimizer",
        xaxis_title="Investment Priority (step)",
        yaxis_title="Cumulative Expected D Seats",
        height=400,
        margin=dict(l=50, r=20, t=50, b=40),
    )
    return fig


# ---------------------------------------------------------------------------
# Trend Analysis: lean vs. trend scatter
# ---------------------------------------------------------------------------


TREND_DIR_COLORS: dict[str, str] = {
    "trending_d": "#1565C0",
    "trending_r": "#D32F2F",
    "stable":     "#757575",
    "insufficient_data": "#BDBDBD",
}


def lean_vs_trend_scatter(
    df: pd.DataFrame,
    statewide_d: float = 48.0,
    highlight_pickups: bool = True,
) -> go.Figure:
    """
    Scatter: expected margin (x) vs trend_slope (y).

    Each dot is a district. Color by current_holder (D/R).
    Pickup targets get larger markers. Open seats get star markers.
    Quadrant labels indicate strategic meaning.
    """
    from gui.styles import lean_to_margin

    df = df.copy()
    df["expected_margin"] = df["composite_lean"].apply(lambda x: lean_to_margin(x, statewide_d))
    # Convert trend slope to pts/year for readability
    df["trend_pts_yr"] = df["trend_slope"] * 100

    fig = go.Figure()

    # Quadrant shading (now in margin space)
    fig.add_shape(type="rect", x0=0, x1=0.5, y0=0, y1=5,
                  fillcolor="rgba(21,101,192,0.06)", line_width=0, layer="below")
    fig.add_shape(type="rect", x0=-0.5, x1=0, y0=0, y1=5,
                  fillcolor="rgba(21,101,192,0.03)", line_width=0, layer="below")
    fig.add_shape(type="rect", x0=0, x1=0.5, y0=-5, y1=0,
                  fillcolor="rgba(211,47,47,0.03)", line_width=0, layer="below")
    fig.add_shape(type="rect", x0=-0.5, x1=0, y0=-5, y1=0,
                  fillcolor="rgba(211,47,47,0.06)", line_width=0, layer="below")

    # Quadrant annotations
    fig.add_annotation(x=0.12, y=2.8, text="D wins + trending D",
                       showarrow=False, font=dict(size=10, color="#1565C0"), opacity=0.6)
    fig.add_annotation(x=-0.12, y=2.8, text="R wins + trending D",
                       showarrow=False, font=dict(size=10, color="#1565C0"), opacity=0.6)
    fig.add_annotation(x=0.12, y=-2.8, text="D wins + trending R",
                       showarrow=False, font=dict(size=10, color="#D32F2F"), opacity=0.6)
    fig.add_annotation(x=-0.12, y=-2.8, text="R wins + trending R",
                       showarrow=False, font=dict(size=10, color="#D32F2F"), opacity=0.6)

    # Split into groups for different marker treatments
    if highlight_pickups and "pickup_opportunity" in df.columns:
        groups = [
            ("Pickup target (open)", df[(df["pickup_opportunity"] == True) & (df["open_seat_2026"] == True)],
             14, "star", 1.0),
            ("Pickup target", df[(df["pickup_opportunity"] == True) & (df["open_seat_2026"] != True)],
             10, "circle", 0.9),
            ("Other R-held", df[(df["current_holder"] == "R") & (df["pickup_opportunity"] != True)],
             6, "circle", 0.3),
            ("D-held", df[df["current_holder"] == "D"],
             6, "diamond", 0.3),
        ]
    else:
        groups = [
            ("R-held", df[df["current_holder"] == "R"], 8, "circle", 0.7),
            ("D-held", df[df["current_holder"] == "D"], 8, "diamond", 0.7),
        ]

    for name, gdf, size, symbol, opacity in groups:
        if gdf.empty:
            continue
        colors = [HOLDER_COLORS.get(h, "#757575") for h in gdf["current_holder"]]
        fig.add_trace(go.Scatter(
            x=gdf["expected_margin"],
            y=gdf["trend_pts_yr"],
            mode="markers+text",
            marker=dict(size=size, color=colors, symbol=symbol, opacity=opacity,
                        line=dict(width=1, color="white")),
            text=[str(int(d)) for d in gdf["district"]],
            textposition="top center",
            textfont=dict(size=8 if opacity < 0.5 else 9),
            name=name,
            hovertemplate=(
                "<b>District %{customdata[0]:.0f}</b><br>"
                "Exp. margin: %{x:+.3f}<br>"
                "Trend: %{y:+.2f} pts/yr<br>"
                "14yr shift: %{customdata[1]:+.1f} pts<br>"
                "R²: %{customdata[2]:.2f}<br>"
                "Tier: %{customdata[3]}"
                "<extra></extra>"
            ),
            customdata=list(zip(
                gdf["district"], gdf["trend_shift"] * 100,
                gdf["trend_r2"], gdf["tier"],
            )),
        ))

    # Reference lines
    fig.add_hline(y=0, line=dict(color="#757575", width=1))
    fig.add_vline(x=0, line=dict(color="#757575", width=1),
                  annotation_text="Even", annotation_position="top")

    fig.update_layout(
        title=f"Expected Margin vs. Trend @ {statewide_d:.0f}% Statewide D",
        xaxis_title="Expected Margin (positive = D wins)",
        yaxis_title="Trend (pts/year)",
        xaxis=dict(zeroline=False),
        yaxis=dict(zeroline=False, tickformat="+.1f"),
        height=600,
        margin=dict(l=60, r=30, t=50, b=50),
        legend=dict(yanchor="top", y=0.99, xanchor="right", x=0.99),
    )
    return fig


def trend_arrow_chart(
    df: pd.DataFrame,
    statewide_d: float = 48.0,
    years_forward: int = 4,
) -> go.Figure:
    """
    Arrow chart: current margin → projected margin if trend continues.
    One row per district, sorted by projected margin. Shows only pickup targets.
    """
    from gui.styles import lean_to_margin, fmt_margin

    df = df.copy()
    df["current_margin"] = df["composite_lean"].apply(lambda x: lean_to_margin(x, statewide_d))
    df["projected_margin"] = df["current_margin"] + df["trend_slope"] * years_forward
    df = df.sort_values("projected_margin", ascending=True)

    labels = [
        f"D-{int(d)}" + (" *" if o else "")
        for d, o in zip(df["district"], df["open_seat_2026"].fillna(False))
    ]

    fig = go.Figure()

    # Current margin dots
    fig.add_trace(go.Scatter(
        x=df["current_margin"], y=labels,
        mode="markers",
        marker=dict(size=10, color="#757575", symbol="circle"),
        name=f"Current @ {statewide_d:.0f}%",
        hovertemplate="District %{customdata[0]}: %{customdata[1]}<extra>Current</extra>",
        customdata=list(zip(
            df["district"].astype(int),
            df["current_margin"].apply(fmt_margin),
        )),
    ))

    # Projected margin dots
    proj_colors = ["#1565C0" if p > c else "#D32F2F"
                   for p, c in zip(df["projected_margin"], df["current_margin"])]
    fig.add_trace(go.Scatter(
        x=df["projected_margin"], y=labels,
        mode="markers",
        marker=dict(size=10, color=proj_colors, symbol="diamond"),
        name=f"Projected ({years_forward}yr)",
        hovertemplate="District %{customdata[0]}: %{customdata[1]}<extra>Projected</extra>",
        customdata=list(zip(
            df["district"].astype(int),
            df["projected_margin"].apply(fmt_margin),
        )),
    ))

    # Arrows connecting them
    for i, row in df.iterrows():
        color = "#1565C0" if row["projected_margin"] > row["current_margin"] else "#D32F2F"
        label = f"D-{int(row['district'])}" + (" *" if row.get("open_seat_2026") else "")
        fig.add_annotation(
            x=row["projected_margin"], y=label,
            ax=row["current_margin"], ay=label,
            xref="x", yref="y", axref="x", ayref="y",
            showarrow=True, arrowhead=3, arrowsize=1.2,
            arrowwidth=2, arrowcolor=color,
        )

    # Even line
    fig.add_vline(x=0, line=dict(color="#757575", width=1, dash="dot"),
                   annotation_text="Even", annotation_position="top")

    fig.update_layout(
        title=f"Pickup Targets: Current → Projected Margin ({years_forward}-year trend) @ {statewide_d:.0f}%",
        xaxis_title="Expected Margin (positive = D wins)",
        height=max(400, len(df) * 28),
        margin=dict(l=80, r=30, t=50, b=50),
        showlegend=True,
        legend=dict(yanchor="top", y=0.99, xanchor="right", x=0.99),
    )
    return fig


# ---------------------------------------------------------------------------
# Map: choropleth
# ---------------------------------------------------------------------------


def district_choropleth(
    geojson: dict,
    data_df: pd.DataFrame,
    color_col: str = "composite_lean",
    selected_d: float | None = None,
) -> go.Figure:
    """Ohio House district choropleth map."""

    if color_col == "expected_margin":
        # Convert to percentage points for display
        data_df = data_df.copy()
        data_df["margin_pts"] = data_df["expected_margin"] * 100
        fig = px.choropleth_mapbox(
            data_df, geojson=geojson,
            locations="district",
            featureidkey="properties.DISTRICT",
            color="margin_pts",
            color_continuous_scale="RdBu",
            color_continuous_midpoint=0,
            range_color=[-20, 20],
            hover_name="district",
            hover_data={"margin_pts": ":.1f", "tier": True, "expected_margin": False},
            labels={"margin_pts": "Margin (pts)"},
            mapbox_style="carto-positron",
            zoom=6, center={"lat": 40.0, "lon": -82.5},
            opacity=0.7,
        )
        fig.update_coloraxes(
            colorbar_title_text="Margin",
            colorbar_tickvals=[-20, -10, -5, 0, 5, 10, 20],
            colorbar_ticktext=["R+20", "R+10", "R+5", "Even", "D+5", "D+10", "D+20"],
        )
    elif color_col == "composite_lean":
        fig = px.choropleth_mapbox(
            data_df, geojson=geojson,
            locations="district",
            featureidkey="properties.DISTRICT",
            color="composite_lean",
            color_continuous_scale="RdBu",
            color_continuous_midpoint=0,
            range_color=[-0.20, 0.20],
            hover_name="district",
            hover_data={"composite_lean": ":.3f", "tier": True},
            mapbox_style="carto-positron",
            zoom=6, center={"lat": 40.0, "lon": -82.5},
            opacity=0.7,
        )
    elif color_col == "tier":
        tier_order = ["safe_d", "likely_d", "lean_d", "tossup", "lean_r", "likely_r", "safe_r"]
        color_map = {t: TIER_COLORS[t] for t in tier_order}
        fig = px.choropleth_mapbox(
            data_df, geojson=geojson,
            locations="district",
            featureidkey="properties.DISTRICT",
            color="tier",
            color_discrete_map=color_map,
            category_orders={"tier": tier_order},
            hover_name="district",
            hover_data={"composite_lean": ":.3f", "tier": True},
            mapbox_style="carto-positron",
            zoom=6, center={"lat": 40.0, "lon": -82.5},
            opacity=0.7,
        )
    elif color_col == "win_prob":
        fig = px.choropleth_mapbox(
            data_df, geojson=geojson,
            locations="district",
            featureidkey="properties.DISTRICT",
            color="win_prob",
            color_continuous_scale="RdBu",
            color_continuous_midpoint=0.5,
            range_color=[0, 1],
            hover_name="district",
            hover_data={"win_prob": ":.1%", "tier": True},
            mapbox_style="carto-positron",
            zoom=6, center={"lat": 40.0, "lon": -82.5},
            opacity=0.7,
        )
    else:
        # Fallback
        fig = px.choropleth_mapbox(
            data_df, geojson=geojson,
            locations="district",
            featureidkey="properties.DISTRICT",
            color=color_col,
            mapbox_style="carto-positron",
            zoom=6, center={"lat": 40.0, "lon": -82.5},
            opacity=0.7,
        )

    fig.update_layout(
        height=600,
        margin=dict(l=0, r=0, t=30, b=0),
        title="Ohio House Districts",
    )
    return fig
