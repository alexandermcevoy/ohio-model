"""Districts & Races — every 2026 Ohio race in detail."""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from gui.projection import (
    OFFICE_LABELS, fmt_lean, latest_candidates, office_tables, projection_now, race_label,
)
from gui.styles import HOLDER_COLORS
from src.ballot_overrides import get_override
from src.county_layer import county_inputs
from src.join_sos_vest import OHIO_FIPS_TO_COUNTY

result, headlines = projection_now()
races = result.races

RACE_NAMES = {"gov": "Governor", "statewide_avg": "Row offices (avg)", "uss": "US Senate", "pre": "President"}
YEARS = ["2018", "2020", "2022", "2024"]

st.title("Districts & races")
office = st.segmented_control(
    "Office", ["statewide", "house", "senate", "congress"], default="house",
    format_func=lambda o: {"statewide": "Statewide", **OFFICE_LABELS}[o], label_visibility="collapsed",
) or "house"

# ---------------------------------------------------------------------------
# Statewide races
# ---------------------------------------------------------------------------
if office == "statewide":
    labels = [h.label for h in headlines]
    pick = st.selectbox("Race", labels)
    h = next(x for x in headlines if x.label == pick)
    d_name, r_name = h.dem_name or "Democrat", h.rep_name or "Republican"
    st.subheader(h.label)
    st.caption(f"{d_name} (D) vs {r_name} (R)" if h.dem_name else
               "Candidates aren't in the polling data for this race; see the SOS ballot.")
    c1, c2, c3 = st.columns(3)
    c1.metric(f"{d_name.split()[-1]} win likelihood", f"{h.win_likelihood:.0%}")
    c2.metric("Projected D two-party share", f"{h.proj_d_share:.1%}",
              f"80%: {h.proj_low80:.1%}–{h.proj_high80:.1%}", delta_color="off")
    if h.polling:
        lead = (h.dem_name if h.polling.margin >= 0 else h.rep_name).split()[-1]
        c3.metric("Polling average", f"{lead} +{abs(h.polling.margin):.1f}",
                  f"{h.polling.describe().split(' (')[0]} · {h.polling.band_text()}", delta_color="off")
    else:
        c3.metric("Polling average", "No public polls")

    # Where the vote is: projected share by county (statewide projection + county lean).
    ci = county_inputs()

    counties = ci.counties.assign(
        County=lambda d: d["fips"].map(OHIO_FIPS_TO_COUNTY).str.title(),
        projected=lambda d: (h.proj_d_share + d["lean"]).clip(0, 1),
        votes=lambda d: d["base_votes"],
    ).sort_values("votes", ascending=False)
    st.markdown("**Where the vote is.** Projected D share by county (the statewide projection plus each "
                "county's usual lean) and 2022 turnout, largest counties first.")
    st.dataframe(
        counties[["County", "votes", "lean", "projected"]], hide_index=True, width="stretch", height=380,
        column_config={
            "votes": st.column_config.NumberColumn("2022 two-party votes", format="localized"),
            "lean": st.column_config.NumberColumn("County lean", format="%+.3f",
                                                  help="D share relative to the state, composite of 2018–2024"),
            "projected": st.column_config.ProgressColumn("Projected D share", min_value=0.0, max_value=1.0,
                                                         format="percent"),
        },
    )
    st.stop()

# ---------------------------------------------------------------------------
# Legislative offices
# ---------------------------------------------------------------------------
tables = office_tables(office)
targeting, composite = tables["targeting"], tables["composite"]
office_races = races[races["office"] == office].copy()
office_races["Race"] = office_races.apply(race_label, axis=1)
office_races = office_races.merge(targeting[["district", "composite_lean", "current_incumbent_name"]],
                                  on="district", how="left")
office_races["Lean"] = office_races["composite_lean"].map(fmt_lean)
office_races["status"] = office_races["fixed_reason"].replace("", "contested")

st.caption("Every district, sorted by Democratic win likelihood. Pick one below for its full profile.")
st.dataframe(
    office_races.sort_values("win_prob", ascending=False)[
        ["Race", "current_holder", "current_incumbent_name", "open_seat_2026", "Lean", "proj_d_share", "win_prob", "status"]
    ],
    hide_index=True, width="stretch", height=320,
    column_config={
        "current_holder": "Held by",
        "current_incumbent_name": "Incumbent",
        "open_seat_2026": st.column_config.CheckboxColumn("Open"),
        "proj_d_share": st.column_config.NumberColumn("Projected D share", format="percent"),
        "win_prob": st.column_config.ProgressColumn("D win likelihood", min_value=0.0, max_value=1.0, format="percent"),
        "status": "Status",
    },
)

options = office_races.sort_values("district")
default = 52 if office == "house" else int(options.iloc[0]["district"])
labels = {int(r.district): f"{r.Race} · {r.current_holder or '?'}-held · {r.Lean}" + (" · open" if r.open_seat_2026 else "")
          for r in options.itertuples()}
district = st.selectbox("District", list(labels), index=list(labels).index(default), format_func=labels.get)

row = office_races[office_races["district"] == district].iloc[0]
t = targeting[targeting["district"] == district].iloc[0]
c = composite[composite["district"] == district].iloc[0]

st.subheader(f"{OFFICE_LABELS[office]} District {district}")
facts = [f"Held by {t.get('current_holder', '?')}" + (f" ({t['current_incumbent_name']})" if pd.notna(t.get("current_incumbent_name")) else "")]
if bool(t.get("open_seat_2026", False)):
    reason = t.get("open_seat_reason")
    facts.append("open seat" + (f" ({reason.replace('_', ' ')})" if isinstance(reason, str) else ""))
st.caption(" · ".join(facts))

cands = latest_candidates()
if cands is not None and len(cands):
    on_ballot = cands[(cands["office"] == office) & (cands["key"] == district)]
    if len(on_ballot):
        names = " vs ".join(f"{r.name} ({r.party})" for r in on_ballot.sort_values("party").itertuples())
        st.markdown(f"**2026 ballot:** {names}")
elif pd.notna(t.get("dem_candidate_2024")) or pd.notna(t.get("rep_candidate_2024")):
    st.caption(f"2024 candidates: {t.get('dem_candidate_2024', '—')} (D) vs {t.get('rep_candidate_2024', '—')} (R)")

if row["fixed_reason"]:
    ov = get_override(office, district)
    st.info(f"**Not modeled as a contest:** {row['fixed_reason']}." + (f" {ov.note}" if ov else ""))
else:
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("D win likelihood", f"{row['win_prob']:.0%}")
    c2.metric("Projected D share", f"{row['proj_d_share']:.1%}",
              f"80%: {row['proj_low80']:.1%}–{row['proj_high80']:.1%}", delta_color="off")
    c3.metric("Composite lean", fmt_lean(t["composite_lean"]),
              help="District D share minus the statewide share, weighted across 2018–2024 races.")
    if pd.notna(t.get("flip_threshold")):
        c4.metric("Flips D at statewide", f"{t['flip_threshold']:.1%}",
                  help="Statewide reference-race D share at which this district's expected vote is 50/50.")

left, right = st.columns(2)
with left:
    st.markdown("**How the district votes, race by race** (lean vs. the state)")
    lean_cols = [col for col in c.index if col.endswith("_lean") and col != "composite_lean"]
    lean_df = pd.DataFrame({
        "race": [f"{RACE_NAMES.get(col.rsplit('_', 2)[0], col)} {col.rsplit('_', 2)[1]}" for col in lean_cols],
        "lean": [c[col] for col in lean_cols],
    }).dropna()
    fig = go.Figure(go.Bar(
        x=lean_df["lean"] * 100, y=lean_df["race"], orientation="h",
        marker_color=[HOLDER_COLORS["D"] if v >= 0 else HOLDER_COLORS["R"] for v in lean_df["lean"]],
        hovertemplate="%{y}: %{x:+.1f} pts<extra></extra>",
    ))
    fig.add_vline(x=0, line_color="#9e9e9e")
    fig.update_layout(height=300, margin=dict(l=8, r=8, t=8, b=8), xaxis_title="Lean (pts, + = more D)")
    st.plotly_chart(fig, key=f"leans_{office}_{district}")

with right:
    st.markdown("**Past results in this seat**")
    hist = pd.DataFrame([{
        "Year": y,
        "Winner": c.get(f"winner_{y}"),
        "D share": c.get(f"dem_share_{y}"),
        "Contested": c.get(f"contested_{y}"),
        "Candidate effect": c.get(f"candidate_effect_{y}"),
    } for y in YEARS if f"winner_{y}" in c.index])
    st.dataframe(hist, hide_index=True, width="stretch", column_config={
        "D share": st.column_config.NumberColumn(format="percent"),
        "Candidate effect": st.column_config.NumberColumn(format="%+.3f",
                                                          help="Actual D share minus what the district's lean predicted"),
    })
    if office == "house" and tables["overlap"] is not None:
        ov = tables["overlap"].set_index("district")
        if district in ov.index:
            st.caption(f"Years comparable to today's lines: {ov.loc[district, 'years_reliable']}. "
                       "Earlier results come from a differently drawn district with the same number.")
    elif office == "congress":
        st.caption("New map adopted October 2025: earlier results are from differently drawn districts.")
    elif office == "senate":
        st.caption("Senate districts were redrawn for 2022 and 2024; older results may come from different lines.")

if office == "house" and tables["demographics"] is not None:
    demo = tables["demographics"].set_index("district")
    if district in demo.index:
        dm = demo.loc[district]
        st.markdown("**Who lives here** (ACS 2023 5-year)")
        d1, d2, d3, d4, d5 = st.columns(5)
        d1.metric("Population", f"{dm['total_pop']:,.0f}")
        d2.metric("College grads", f"{dm['college_pct']:.0%}")
        d3.metric("Median income", f"${dm['median_income']:,.0f}")
        d4.metric("White / Black / Hispanic", f"{dm['white_pct']:.0%} / {dm['black_pct']:.0%} / {dm['hispanic_pct']:.0%}")
        d5.metric("People per sq mi", f"{dm['total_pop'] / dm['district_land_area_sqmi']:,.0f}")
