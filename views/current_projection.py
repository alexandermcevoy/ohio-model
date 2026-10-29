"""Current Projection — every 2026 Ohio race before any votes are counted."""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from gui.projection import (
    OFFICE_LABELS, chamber_win, fmt_lean, model_day, projection_now, office_tables, race_label,
)
from gui.styles import HOLDER_COLORS
from src import election_night as en
from src.office import get_office

result, headlines = projection_now()
races = result.races

st.title("Current projection")
polled = [h for h in headlines if h.polling]
latest_poll = max(h.polling.latest_end for h in polled).date() if polled else None
st.caption(
    f"Pre-election forecast as of {model_day()} · polls through {latest_poll} · 10,000 simulations. "
    "Win likelihood = how often each side wins across the simulations."
)

# --- statewide -------------------------------------------------------------
st.subheader("Statewide races")
cols = st.columns(3) + st.columns(3)
for col, h in zip(cols, headlines):
    d_name = (h.dem_name or "Democrat").split()[-1]
    r_name = (h.rep_name or "Republican").split()[-1]
    leader, p = (d_name, h.win_likelihood) if h.win_likelihood >= 0.5 else (r_name, 1 - h.win_likelihood)
    with col.container(border=True):
        st.markdown(f"**{h.label}**")
        st.metric("Win likelihood", f"{leader} {p:.0%}", label_visibility="collapsed")
        st.progress(float(h.win_likelihood), text=f"{d_name} {h.win_likelihood:.0%} · {r_name} {1 - h.win_likelihood:.0%}")
        if h.polling:
            st.caption(f"Polling average: {h.polling.describe()}  \n{h.polling.band_text()}")
        else:
            st.caption("No public polls: projected from the statewide environment.")

# --- chambers --------------------------------------------------------------
st.subheader("Chambers")
for col, row in zip(st.columns(3), result.chambers.itertuples(index=False)):
    office = row.office
    cfg = get_office(office)
    chamber = result.chambers.set_index("office").loc[office]
    label, p = chamber_win(office, chamber)
    with col.container(border=True):
        st.markdown(f"**{OFFICE_LABELS[office]}** · now {cfg.current_d_seats} D of {cfg.chamber_size}")
        c1, c2 = st.columns(2)
        c1.metric("Expected D seats", f"{row.mean_d_seats:.1f}", f"80%: {row.p10_seats}–{row.p90_seats}",
                  delta_color="off")
        c2.metric("Win likelihood", f"{p:.0%}", label, delta_color="off")
        counts = pd.Series(result.seat_counts[office]).value_counts(normalize=True).sort_index()
        thr = cfg.majority_threshold or cfg.current_d_seats
        colors = [HOLDER_COLORS["D"] if s >= thr else HOLDER_COLORS["R"] for s in counts.index]
        fig = go.Figure(go.Bar(x=counts.index, y=counts.values, marker_color=colors,
                               hovertemplate="%{x} seats: %{y:.1%}<extra></extra>"))
        fig.update_layout(height=180, margin=dict(l=8, r=8, t=8, b=8), yaxis_tickformat=".0%",
                          xaxis_title="D seats", showlegend=False)
        st.plotly_chart(fig, key=f"seats_{office}")
        extra = [f"{lbl}: {chamber[f'P({lbl})']:.0%}" for _, lbl in en.CHAMBER_THRESHOLDS[office]]
        st.caption(" · ".join(extra))
        if office == "senate":
            st.caption("17 odd-numbered seats are on the ballot; the 16 even seats hold over.")

# --- closest races ---------------------------------------------------------
st.subheader("Closest legislative races")
st.caption("Races where neither side is above 80%. Open **Districts & Races** for any race in full.")
leg = races[(races["office"] != "statewide") & (races["fixed_reason"] == "")].copy()
leg = leg[(leg["win_prob"] > 0.2) & (leg["win_prob"] < 0.8)]
leg["Race"] = leg.apply(race_label, axis=1)
leg["Office"] = leg["office"].map(OFFICE_LABELS)
targets = {o: office_tables(o)["targeting"].set_index("district")["composite_lean"] for o in OFFICE_LABELS}
leg["Lean"] = leg.apply(lambda r: fmt_lean(targets[r["office"]].get(int(r["district"]), 0.0)), axis=1)
leg = leg.assign(_d=(leg["win_prob"] - 0.5).abs()).sort_values("_d")
st.dataframe(
    leg[["Race", "Office", "current_holder", "open_seat_2026", "Lean", "proj_d_share", "proj_low80",
         "proj_high80", "win_prob"]],
    hide_index=True, width="stretch",
    column_config={
        "current_holder": "Held by",
        "open_seat_2026": st.column_config.CheckboxColumn("Open seat"),
        "proj_d_share": st.column_config.NumberColumn("Projected D share", format="percent"),
        "proj_low80": st.column_config.NumberColumn("80% low", format="percent"),
        "proj_high80": st.column_config.NumberColumn("80% high", format="percent"),
        "win_prob": st.column_config.ProgressColumn("D win likelihood", min_value=0.0, max_value=1.0, format="percent"),
    },
)
