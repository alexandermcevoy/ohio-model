"""Polling Averages — Ohio Governor and US Senate polls, plus the national generic ballot."""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from gui.styles import HOLDER_COLORS
from src.forecast_statewide import OHIO_2026_RACES
from src.headlines import polling_average, polling_average_history, race_polls

RACES = {"governor": "Governor", "us_senate": "US Senate (special)"}


@st.cache_data(ttl=3600)
def _history(race: str) -> pd.DataFrame:
    return polling_average_history(race)


@st.cache_data(ttl=3600)
def _polls(race: str) -> pd.DataFrame:
    return race_polls(race)


@st.cache_data(ttl=3600)
def _generic_ballot() -> tuple[pd.DataFrame, pd.DataFrame]:
    from src.fundamentals import load_generic_ballot_polls, national_generic_ballot_average

    polls = load_generic_ballot_polls()
    dates = pd.date_range("2025-06-01", min(pd.Timestamp.now().normalize(), pd.Timestamp("2026-11-03")), freq="14D")
    rows = []
    for d in dates:
        sig = national_generic_ballot_average(as_of=d)
        rows.append({"date": d, "dem_two_party": sig.value})
    return pd.DataFrame(rows), polls


st.title("Polling averages")
st.caption(
    "Weighted by recency, sample size and pollster (the same weights the model uses), shown as the "
    "candidates' raw numbers. The 80% range is where the final margin lands four times in five if the "
    "polls miss the way polls typically do: disagreement among the polls plus a shared statewide miss "
    "of about ±3 points."
)

for key, title in RACES.items():
    dem, rep = OHIO_2026_RACES[key]
    d_last, r_last = dem.split()[-1], rep.split()[-1]
    pa = polling_average(key)
    hist = _history(key)
    polls = _polls(key)
    lo, hi = pa.margin_band80

    st.subheader(title)
    c1, c2, c3, c4 = st.columns(4)
    c1.metric(d_last, f"{pa.dem_pct:.1f}%")
    c2.metric(r_last, f"{pa.rep_pct:.1f}%")
    lead = d_last if pa.margin >= 0 else r_last
    c3.metric("Margin", f"{lead} +{abs(pa.margin):.1f}")
    c4.metric("Polls", pa.n_polls, f"latest ends {pa.latest_end:%b %-d}", delta_color="off")
    st.caption(pa.band_text())

    poll_margin = polls["dem_pct"] - polls["rep_pct"]
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=pd.concat([hist["date"], hist["date"][::-1]]),
        y=pd.concat([hist["band_hi"], hist["band_lo"][::-1]]),
        fill="toself", fillcolor="rgba(117,117,117,0.18)", line=dict(width=0),
        hoverinfo="skip", name="80% range",
    ))
    fig.add_trace(go.Scatter(
        x=polls["end_date"], y=poll_margin, mode="markers", name="Individual polls",
        marker=dict(size=7, color=[HOLDER_COLORS["D"] if m >= 0 else HOLDER_COLORS["R"] for m in poll_margin],
                    opacity=0.55),
        text=polls["pollster"], hovertemplate="%{text}<br>%{x|%b %-d}: %{y:+.1f}<extra></extra>",
    ))
    fig.add_trace(go.Scatter(
        x=hist["date"], y=hist["margin"], mode="lines", name="Polling average",
        line=dict(color="#212121", width=2.5),
        hovertemplate="%{x|%b %-d}: " + f"{d_last} " + "%{y:+.1f}<extra></extra>",
    ))
    fig.add_hline(y=0, line_color="#9e9e9e", line_width=1)
    fig.update_layout(height=330, margin=dict(l=8, r=8, t=8, b=8),
                      yaxis_title=f"Margin ({d_last} − {r_last}, pts)",
                      legend=dict(orientation="h", y=1.08, x=0))
    st.plotly_chart(fig, key=f"poll_chart_{key}")

    with st.expander(f"All {len(polls)} {title} polls"):
        table = polls.assign(margin=poll_margin)[
            ["end_date", "pollster", "sample_size", "population", "dem_pct", "rep_pct", "margin", "partisan", "feed"]
        ]
        st.dataframe(
            table, hide_index=True, width="stretch",
            column_config={
                "end_date": st.column_config.DateColumn("Ended", format="MMM D, YYYY"),
                "pollster": "Pollster",
                "sample_size": st.column_config.NumberColumn("Sample", format="%d"),
                "population": "Pop.",
                "dem_pct": st.column_config.NumberColumn(d_last, format="%.1f"),
                "rep_pct": st.column_config.NumberColumn(r_last, format="%.1f"),
                "margin": st.column_config.NumberColumn("Margin (D)", format="%+.1f"),
                "partisan": "Partisan sponsor",
                "feed": "Source",
            },
        )

st.subheader("National generic ballot")
st.caption(
    "No public polls exist for Ohio's legislative races. The national generic ballot, translated to Ohio, "
    "is one of the inputs to every House, Senate and congressional projection."
)
gb_hist, gb_polls = _generic_ballot()
latest = gb_hist.iloc[-1]["dem_two_party"]
st.metric("Democratic two-party share (national)", f"{latest:.1%}", f"D+{(2 * latest - 1) * 100:.1f}",
          delta_color="off")
fig = go.Figure(go.Scatter(x=gb_hist["date"], y=gb_hist["dem_two_party"], mode="lines",
                           line=dict(color=HOLDER_COLORS["D"], width=2.5),
                           hovertemplate="%{x|%b %-d}: %{y:.1%}<extra></extra>"))
fig.add_hline(y=0.5, line_color="#9e9e9e", line_width=1)
fig.update_layout(height=240, margin=dict(l=8, r=8, t=8, b=8), yaxis_tickformat=".0%",
                  yaxis_title="D two-party share")
st.plotly_chart(fig, key="generic_ballot")
st.caption(f"{len(gb_polls)} polls (Silver Bulletin export plus VoteHub). Refresh with `python cli.py polls-refresh`.")
