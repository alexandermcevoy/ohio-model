"""
Election Night — type in live returns, get a joint projection for every Ohio race.

Same computation as `python cli.py election-night` (src/election_night.py):
returns are saved to data/raw/live_returns_oh_*.csv, so the CLI and this page
always read the same numbers.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from gui.styles import HOLDER_COLORS, fmt_pct
from src import election_night as en
from src.headlines import statewide_headlines
from src.office import get_office

st.set_page_config(page_title="Election Night", page_icon=":ballot_box:", layout="wide")
st.title("Election Night")
st.caption(
    "Enter vote totals as they come in (NYT, AP, VoteHub, county boards), then Save & recompute. "
    "pct_reporting is the source's % of expected vote counted; leave it blank to estimate from 2022 turnout. "
    "Tick final only when every vote is counted. Not a substitute for AP/network calls."
)

OFFICE_TABS = [("statewide", "Statewide"), ("house", "Ohio House"), ("senate", "Ohio Senate"), ("congress", "US House")]

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

with st.sidebar:
    st.subheader("Election night settings")
    early_skew = st.number_input(
        "Early-count skew prior (D pts at 0% in)", value=0.0, step=1.0,
        help="Expected D lean of the first ballots counted (early/absentee), relative to the final "
             "result. The model also estimates this from the returns; this is only its starting point.",
    )
    n_sims = st.select_slider("Simulations", options=[2_000, 5_000, 10_000], value=5_000)
    with_incumbency = st.checkbox("Literature incumbency adjustment", value=False)


@st.cache_resource(show_spinner="Building pre-election model…")
def _model(day: str, skew: float, incumbency: bool) -> en.PriorModel:
    return en.build_prior_model(pd.Timestamp(day), with_incumbency=incumbency, early_skew_mean=skew / 100)


model = _model(str(en.prior_as_of().date()), early_skew, with_incumbency)
en.ensure_return_files()

# ---------------------------------------------------------------------------
# Returns entry
# ---------------------------------------------------------------------------

def _editor_frame(office: str) -> pd.DataFrame:
    raw = en.load_returns(office)
    key = "race" if office == "statewide" else "district"
    df = pd.DataFrame({key: raw[key]})
    if office == "statewide":
        labels = {r.key: r.label for r in en.STATEWIDE_RACES}
        df["label"] = df["race"].map(labels).fillna("")
    else:
        df["district"] = pd.to_numeric(df["district"], errors="coerce").astype("Int64")
        races = {r.district: r for r in model.races if r.office == office}
        df["holder"] = df["district"].map(lambda d: races[d].current_holder if d in races else "")
        df["note"] = df["district"].map(lambda d: races[d].fixed_reason or "" if d in races else "not on ballot")
    for col in ("dem_votes", "rep_votes"):
        df[col] = pd.to_numeric(raw[col].str.replace(",", ""), errors="coerce").astype("Int64")
    df["pct_reporting"] = pd.to_numeric(raw["pct_reporting"].str.rstrip("%"), errors="coerce")
    df["is_final"] = raw["is_final"].str.strip().str.lower().isin(["true", "1", "yes", "y", "t", "x"])
    return df


def _to_file_frame(office: str, edited: pd.DataFrame) -> pd.DataFrame:
    key = "race" if office == "statewide" else "district"
    out = pd.DataFrame({key: edited[key].astype(str)})
    for col in ("dem_votes", "rep_votes"):
        out[col] = edited[col].map(lambda v: "" if pd.isna(v) else str(int(v)))
    out["pct_reporting"] = edited["pct_reporting"].map(lambda v: "" if pd.isna(v) else f"{v:g}")
    out["is_final"] = edited["is_final"].map(lambda v: "True" if v else "False")
    return out


entry, projection = st.tabs(["Enter returns", "Projection"])

# The editors rebuild from the CSVs whenever this changes, so stale typed
# deltas in session state are never re-applied on top of newer file contents.
version = st.session_state.setdefault("en_version", 0)

with entry:
    if st.button("Reload from files", help="Discard unsaved edits and re-read the CSVs (e.g. after editing them by hand)."):
        st.session_state["en_version"] += 1
        st.rerun()
    with st.form("returns"):
        edited: dict[str, pd.DataFrame] = {}
        for (office, label), tab in zip(OFFICE_TABS, st.tabs([t[1] for t in OFFICE_TABS])):
            with tab:
                frame = _editor_frame(office)
                key = "race" if office == "statewide" else "district"
                edited[office] = st.data_editor(
                    frame,
                    key=f"editor_{office}_{version}",
                    hide_index=True,
                    width="stretch",
                    height=min(38 * (len(frame) + 1), 640),
                    disabled=[c for c in frame.columns if c in (key, "label", "holder", "note")],
                    column_config={
                        "dem_votes": st.column_config.NumberColumn("D votes", min_value=0, step=1, format="%d"),
                        "rep_votes": st.column_config.NumberColumn("R votes", min_value=0, step=1, format="%d"),
                        "pct_reporting": st.column_config.NumberColumn("% in", min_value=0.0, max_value=100.0, step=1.0),
                        "is_final": st.column_config.CheckboxColumn("final"),
                    },
                )
        saved = st.form_submit_button("Save & recompute", type="primary")
    if saved:
        for office, df in edited.items():
            en.write_returns(office, _to_file_frame(office, df))
        st.session_state["en_saved"] = True
        st.session_state["en_version"] += 1
        st.rerun()
    if st.session_state.get("en_saved"):
        st.success("Saved. See the Projection tab.")

# ---------------------------------------------------------------------------
# Projection
# ---------------------------------------------------------------------------

try:
    result = en.run_election_night(model, n_sims=n_sims)
except ValueError as exc:
    projection.error(f"Problem in the entered returns: {exc}")
    st.stop()
if st.session_state.pop("en_saved", False):
    en.write_outputs(result)

races = result.races

with projection:
    st.caption(f"As of {result.as_of:%H:%M:%S} · {result.n_observations} race(s) with returns entered · "
               f"{int(races['is_final'].sum())} fully counted")
    for w in result.warnings:
        st.warning(w)

    st.subheader("Statewide races")
    st.caption("Polling average = weighted raw poll toplines; its 80% range is where the final margin lands 4 times "
               "in 5 if polls miss like they typically do. Win likelihood = chance each candidate wins, "
               "updated live as returns are entered.")
    cols = st.columns(3) + st.columns(3)
    for col, h in zip(cols, statewide_headlines(result)):
        d_name = (h.dem_name or "Democrat").split()[-1]
        r_name = (h.rep_name or "Republican").split()[-1]
        leader, p = (d_name, h.win_likelihood) if h.win_likelihood >= 0.5 else (r_name, 1 - h.win_likelihood)
        col.metric(h.label, f"{leader} {p:.0%}", h.call, delta_color="off", help="Win likelihood")
        col.progress(float(h.win_likelihood), text=f"{d_name} {h.win_likelihood:.0%} · {r_name} {1 - h.win_likelihood:.0%}")
        col.caption("Polling average: " + (f"{h.polling.describe()} · {h.polling.band_text()}" if h.polling else "no public polls"))
        if h.live_d_share is not None:
            frac = "final" if h.is_final else ("" if h.live_fraction is None else f"{h.live_fraction:.0%} in")
            col.caption(f"Live: {d_name} {h.live_d_share:.1%} of the two-party vote · {frac}")

    with st.expander("Model internals"):
        e, e_sd = result.environment
        e0, _ = result.environment_prior
        s, s_sd = result.skew
        c1, c2 = st.columns(2)
        c1.metric("Environment (generic statewide race)", f"{e:.1%}",
                  f"±{e_sd * 100:.1f} · {(e - e0) * 100:+.1f} pts vs. pre-election", delta_color="off")
        c2.metric("Early-count skew", f"{s * 100:+.1f} pts", f"±{s_sd * 100:.1f} pts", delta_color="off")
        st.caption("The environment is the Democratic share of a generic statewide race (the average of "
                   "AG/Auditor/SOS/Treasurer) with no candidates attached. Every projection is built on it; "
                   "it is not any real race's vote share. Early-count skew is how far the first-counted "
                   "ballots lean D relative to the final count.")

    st.subheader("Chambers")
    chamber_cols = st.columns(3)
    for col, row in zip(chamber_cols, result.chambers.itertuples(index=False)):
        office = get_office(row.office)
        chamber = result.chambers[result.chambers["office"] == row.office].iloc[0]
        col.metric(office.display_name, f"{row.mean_d_seats:.1f} D seats", f"80%: {row.p10_seats}–{row.p90_seats}", delta_color="off")
        counts = pd.Series(result.seat_counts[row.office]).value_counts(normalize=True).sort_index()
        fig = go.Figure(go.Bar(x=counts.index, y=counts.values, marker_color=HOLDER_COLORS["D"]))
        for thr, _ in en.CHAMBER_THRESHOLDS[row.office]:
            fig.add_vline(x=thr - 0.5, line_dash="dot", line_color="#757575")
        fig.update_layout(height=200, margin=dict(l=10, r=10, t=10, b=10), yaxis_tickformat=".0%",
                          xaxis_title="D seats", showlegend=False)
        col.plotly_chart(fig, key=f"seats_{row.office}")
        for c in result.chambers.columns:
            if c.startswith("P(") and pd.notna(chamber[c]):
                col.write(f"{c}: **{chamber[c]:.0%}**")
        col.caption(f"Decided or ≥99%: D {row.called_d} · R {row.called_r} · reporting {row.n_reporting}")

    st.subheader("Races")
    office_filter = st.radio("Office", [t[1] for t in OFFICE_TABS[1:]], horizontal=True)
    oname = {label: key for key, label in OFFICE_TABS}[office_filter]
    only_close = st.checkbox("Only races between 10% and 90%", value=True)
    view = races[races["office"] == oname].copy()
    if only_close:
        view = view[(view["win_prob"] > 0.10) & (view["win_prob"] < 0.90)]
    view = view.assign(_d=(view["win_prob"] - 0.5).abs()).sort_values("_d")
    st.dataframe(
        view[["district", "current_holder", "open_seat_2026", "live_d_share", "live_fraction",
              "proj_d_share", "proj_low80", "proj_high80", "win_prob", "call", "fixed_reason"]],
        hide_index=True, width="stretch",
        column_config={
            "district": "Dist", "current_holder": "Holder", "open_seat_2026": "Open",
            "live_d_share": st.column_config.NumberColumn("Live D", format="percent"),
            "live_fraction": st.column_config.NumberColumn("% in", format="percent"),
            "proj_d_share": st.column_config.NumberColumn("Proj D", format="percent"),
            "proj_low80": st.column_config.NumberColumn("80% low", format="percent"),
            "proj_high80": st.column_config.NumberColumn("80% high", format="percent"),
            "win_prob": st.column_config.ProgressColumn("P(D)", min_value=0.0, max_value=1.0, format="percent"),
            "call": "Call", "fixed_reason": "Note",
        },
    )

    hist_path = Path(en.OUT_DIR) / "history.csv"
    if hist_path.exists():
        hist = pd.read_csv(hist_path, parse_dates=["timestamp"])
        if len(hist) > 1:
            st.subheader("Over the night")
            seat_cols = [c for c in hist.columns if c.startswith("seats_")]
            st.line_chart(hist.set_index("timestamp")[seat_cols])
            wp_cols = {"wp_governor": "Governor (D win likelihood)", "wp_us_senate": "US Senate (D win likelihood)"}
            have = [c for c in wp_cols if c in hist.columns]
            if have:
                st.line_chart(hist.set_index("timestamp")[have].rename(columns=wp_cols))
