"""
projection.py — the one projection every dashboard page reads.

The pre-election forecast is the election-night model (src/election_night.py)
run with no returns entered, so Current Projection, Districts & Races and the
Election Night page (before 7:30 pm) always show the same numbers as each
other and as `python cli.py forecast` / `election-night`.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

from src import election_night as en
from src.headlines import empty_frames, statewide_headlines
from src.office import get_office

REPO_ROOT = Path(__file__).resolve().parent.parent
OFFICE_LABELS = {"house": "Ohio House", "senate": "Ohio Senate", "congress": "US House"}


def model_day() -> str:
    return str(en.prior_as_of().date())


@st.cache_resource(show_spinner="Building the forecast model…")
def prior_model(day: str, early_skew: float = 0.0, incumbency: bool = False) -> en.PriorModel:
    return en.build_prior_model(pd.Timestamp(day), with_incumbency=incumbency, early_skew_mean=early_skew / 100)


@st.cache_resource(show_spinner="Running 10,000 simulations…")
def current_projection(day: str, ballot_file: str | None = None):
    """
    Pre-election result and statewide headlines: no returns, regardless of
    what's entered or in the feed. The newest SOS feed file contributes only
    its candidate list (races with one major party are marked uncontested).
    """
    from src.enr_feed import ballot_only, parse_feed

    ballot = ballot_only(parse_feed(Path(ballot_file))) if ballot_file else None
    result = en.run_election_night(prior_model(day), empty_frames(), feed=ballot, n_sims=10_000)
    return result, statewide_headlines(result)


def projection_now():
    from src.enr_feed import latest_feed_file

    f = latest_feed_file()
    return current_projection(model_day(), str(f) if f else None)


def chamber_win(office: str, row) -> tuple[str, float]:
    """The chamber's headline Win Likelihood: majority where there is one, else holding current seats."""
    cfg = get_office(office)
    if cfg.majority_threshold:
        return f"Democratic majority ({cfg.majority_threshold}+ of {cfg.chamber_size})", \
            float(row[f"P(majority ({cfg.majority_threshold}))"])
    return f"Democrats hold {cfg.current_d_seats}+ seats", float(row[f"P(hold {cfg.current_d_seats})"])


@st.cache_data
def office_tables(office: str) -> dict[str, pd.DataFrame | None]:
    """Targeting, composite (race leans + past results) and House-only extras for one office."""
    cfg = get_office(office)
    out: dict[str, pd.DataFrame | None] = {
        "targeting": pd.read_csv(REPO_ROOT / f"reports/session3/{cfg.output_prefix}_targeting.csv"),
        "composite": pd.read_csv(REPO_ROOT / f"reports/session2/{cfg.output_prefix}_composite_lean.csv"),
        "demographics": None,
        "overlap": None,
    }
    if office == "house":
        demo = REPO_ROOT / "data/processed/oh_house_demographics.csv"
        if demo.exists():
            out["demographics"] = pd.read_csv(demo).rename(columns={"district_num": "district"})
        ov = REPO_ROOT / "reports/redistricting_overlap.csv"
        if ov.exists():
            out["overlap"] = pd.read_csv(ov)
    return out


@st.cache_data
def feed_candidates(feed_file: str | None) -> pd.DataFrame | None:
    """2026 nominees from the newest saved SOS feed file (its candidate list is the ballot)."""
    if feed_file is None:
        return None
    from src.enr_feed import parse_feed

    return parse_feed(Path(feed_file)).candidates


def latest_candidates() -> pd.DataFrame | None:
    from src.enr_feed import latest_feed_file

    f = latest_feed_file()
    return feed_candidates(str(f) if f else None)


def fmt_lean(lean: float) -> str:
    pts = lean * 100
    return "EVEN" if abs(pts) < 0.05 else (f"D+{pts:.1f}" if pts > 0 else f"R+{-pts:.1f}")


def race_label(row) -> str:
    """Short label for a race row of an ElectionNightResult."""
    if row["office"] == "statewide":
        return row["label"]
    prefix = {"house": "HD", "senate": "SD", "congress": "OH"}[row["office"]]
    return f"{prefix}-{int(row['district'])}"
