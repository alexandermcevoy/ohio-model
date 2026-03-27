"""
data_loader.py — Cached CSV loaders for the Streamlit GUI.

All data is loaded from gui_data/, which contains pre-computed outputs
from the analytical pipeline. To refresh: python cli.py export-gui
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pandas as pd
import streamlit as st

# ---------------------------------------------------------------------------
# Base path — gui_data/ lives at repo root alongside app.py
# ---------------------------------------------------------------------------

_ROOT = Path(__file__).resolve().parent.parent
_DATA = _ROOT / "gui_data"


# ---------------------------------------------------------------------------
# Cached loaders
# ---------------------------------------------------------------------------


@st.cache_data(ttl=3600)
def load_targeting() -> pd.DataFrame:
    return pd.read_csv(_DATA / "targeting.csv")


@st.cache_data(ttl=3600)
def load_composite_lean() -> pd.DataFrame:
    return pd.read_csv(_DATA / "composite_lean.csv")


@st.cache_data(ttl=3600)
def load_scenarios() -> pd.DataFrame:
    return pd.read_csv(_DATA / "probabilistic_scenarios.csv")


@st.cache_data(ttl=3600)
def load_deterministic_scenarios() -> pd.DataFrame:
    return pd.read_csv(_DATA / "deterministic_scenarios.csv")


@st.cache_data(ttl=3600)
def load_win_probs() -> pd.DataFrame:
    return pd.read_csv(_DATA / "district_win_probs.csv")


@st.cache_data(ttl=3600)
def load_investment_priority() -> pd.DataFrame:
    return pd.read_csv(_DATA / "investment_priority.csv")


@st.cache_data(ttl=3600)
def load_path_optimizer() -> pd.DataFrame:
    return pd.read_csv(_DATA / "path_optimizer.csv")


@st.cache_data(ttl=3600)
def load_defensive() -> pd.DataFrame:
    return pd.read_csv(_DATA / "defensive_scenarios.csv")


@st.cache_data(ttl=3600)
def load_sigma() -> pd.DataFrame:
    return pd.read_csv(_DATA / "district_sigma.csv")


@st.cache_data(ttl=3600)
def load_redistricting() -> pd.DataFrame:
    return pd.read_csv(_DATA / "redistricting_overlap.csv")


@st.cache_data(ttl=3600)
def load_anomaly_flags() -> pd.DataFrame:
    return pd.read_csv(_DATA / "anomaly_flags.csv")


@st.cache_data(ttl=3600)
def load_demographics() -> pd.DataFrame:
    df = pd.read_csv(_DATA / "demographics.csv")
    if "district_num" in df.columns and "district" not in df.columns:
        df = df.rename(columns={"district_num": "district"})
    return df


@st.cache_data(ttl=3600)
def load_voter_universe() -> pd.DataFrame | None:
    p = _DATA / "voter_universe.csv"
    if not p.exists():
        return None
    return pd.read_csv(p)


@st.cache_data(ttl=3600)
def load_trends() -> pd.DataFrame | None:
    p = _DATA / "district_trends.csv"
    if not p.exists():
        return None
    return pd.read_csv(p)


@st.cache_data(ttl=3600)
def load_geojson() -> dict | None:
    """Load the pre-converted GeoJSON for the map view."""
    p = _DATA / "districts.geojson"
    if not p.exists():
        return None
    with open(p) as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Convenience: data freshness timestamp
# ---------------------------------------------------------------------------


def data_refresh_time() -> str:
    """Return the modification time of the targeting CSV as a human-readable string."""
    p = _DATA / "targeting.csv"
    if p.exists():
        mtime = os.path.getmtime(p)
        from datetime import datetime
        return datetime.fromtimestamp(mtime).strftime("%B %d, %Y at %I:%M %p")
    return "unknown"
