"""
data_loader.py — Cached CSV loaders for the Streamlit GUI.

All data is loaded from gui_data/{office}/, which contains pre-computed outputs
from the analytical pipeline. To refresh: python cli.py export-gui

Office is taken from streamlit session_state["office"] (defaults to "house").
House artifacts are also mirrored at gui_data/ root for back-compat.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pandas as pd
import streamlit as st

# ---------------------------------------------------------------------------
# Base paths
# ---------------------------------------------------------------------------

_ROOT = Path(__file__).resolve().parent.parent
_DATA = _ROOT / "gui_data"

VALID_OFFICES = ("house", "senate", "congress")


def _current_office() -> str:
    """Return the active office from session state, defaulting to house."""
    try:
        office = st.session_state.get("office", "house")
    except Exception:
        office = "house"
    if office not in VALID_OFFICES:
        office = "house"
    return office


def _office_dir(office: str | None = None) -> Path:
    """Return gui_data/{office}/ path, falling back to gui_data/ root if missing."""
    o = office or _current_office()
    p = _DATA / o
    if p.exists():
        return p
    # Back-compat: house data may live at gui_data/ root
    return _DATA


# ---------------------------------------------------------------------------
# Cached loaders — all accept optional `office` to bypass session state
# ---------------------------------------------------------------------------


@st.cache_data(ttl=3600)
def load_targeting(office: str | None = None) -> pd.DataFrame:
    return pd.read_csv(_office_dir(office) / "targeting.csv")


@st.cache_data(ttl=3600)
def load_composite_lean(office: str | None = None) -> pd.DataFrame:
    return pd.read_csv(_office_dir(office) / "composite_lean.csv")


@st.cache_data(ttl=3600)
def load_scenarios(office: str | None = None) -> pd.DataFrame:
    return pd.read_csv(_office_dir(office) / "probabilistic_scenarios.csv")


@st.cache_data(ttl=3600)
def load_deterministic_scenarios(office: str | None = None) -> pd.DataFrame:
    return pd.read_csv(_office_dir(office) / "deterministic_scenarios.csv")


@st.cache_data(ttl=3600)
def load_win_probs(office: str | None = None) -> pd.DataFrame:
    return pd.read_csv(_office_dir(office) / "district_win_probs.csv")


@st.cache_data(ttl=3600)
def load_investment_priority(office: str | None = None) -> pd.DataFrame:
    return pd.read_csv(_office_dir(office) / "investment_priority.csv")


@st.cache_data(ttl=3600)
def load_path_optimizer(office: str | None = None) -> pd.DataFrame | None:
    p = _office_dir(office) / "path_optimizer.csv"
    if not p.exists():
        return None
    return pd.read_csv(p)


@st.cache_data(ttl=3600)
def load_defensive(office: str | None = None) -> pd.DataFrame | None:
    p = _office_dir(office) / "defensive_scenarios.csv"
    if not p.exists():
        return None
    return pd.read_csv(p)


@st.cache_data(ttl=3600)
def load_sigma(office: str | None = None) -> pd.DataFrame:
    return pd.read_csv(_office_dir(office) / "district_sigma.csv")


@st.cache_data(ttl=3600)
def load_redistricting(office: str | None = None) -> pd.DataFrame | None:
    p = _office_dir(office) / "redistricting_overlap.csv"
    if not p.exists():
        return None
    return pd.read_csv(p)


@st.cache_data(ttl=3600)
def load_anomaly_flags(office: str | None = None) -> pd.DataFrame | None:
    p = _office_dir(office) / "anomaly_flags.csv"
    if not p.exists():
        return None
    return pd.read_csv(p)


@st.cache_data(ttl=3600)
def load_demographics(office: str | None = None) -> pd.DataFrame | None:
    p = _office_dir(office) / "demographics.csv"
    if not p.exists():
        return None
    df = pd.read_csv(p)
    if "district_num" in df.columns and "district" not in df.columns:
        df = df.rename(columns={"district_num": "district"})
    return df


@st.cache_data(ttl=3600)
def load_voter_universe(office: str | None = None) -> pd.DataFrame | None:
    p = _office_dir(office) / "voter_universe.csv"
    if not p.exists():
        return None
    return pd.read_csv(p)


@st.cache_data(ttl=3600)
def load_trends(office: str | None = None) -> pd.DataFrame | None:
    p = _office_dir(office) / "district_trends.csv"
    if not p.exists():
        return None
    return pd.read_csv(p)


@st.cache_data(ttl=3600)
def load_geojson(office: str | None = None) -> dict | None:
    """Load the pre-converted GeoJSON for the map view."""
    p = _office_dir(office) / "districts.geojson"
    if not p.exists():
        return None
    with open(p) as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Convenience: data freshness timestamp
# ---------------------------------------------------------------------------


def data_refresh_time(office: str | None = None) -> str:
    """Return the modification time of the targeting CSV as a human-readable string."""
    p = _office_dir(office) / "targeting.csv"
    if p.exists():
        mtime = os.path.getmtime(p)
        from datetime import datetime
        return datetime.fromtimestamp(mtime).strftime("%B %d, %Y at %I:%M %p")
    return "unknown"
