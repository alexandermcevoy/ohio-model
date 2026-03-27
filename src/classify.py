"""
classify.py — District tier classification and targeting framework.

Tiers are based on composite partisan lean. Swing SD and turnout elasticity
are computed from contested house races only. Districts with fewer than 2
contested races are flagged (n_contested < 2) — one data point isn't enough
for a reliable swing estimate.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from src.constants import LITERATURE_INCUMBENCY_ADVANTAGE  # kept for reference; not wired to calculations

# ---------------------------------------------------------------------------
# Tier thresholds
# ---------------------------------------------------------------------------

TIER_THRESHOLDS: dict[str, tuple[float, float]] = {
    "safe_d":   (+0.15, float("inf")),
    "likely_d": (+0.08, +0.15),
    "lean_d":   (+0.03, +0.08),
    "tossup":   (-0.03, +0.03),
    "lean_r":   (-0.08, -0.03),
    "likely_r": (-0.15, -0.08),
    "safe_r":   (float("-inf"), -0.15),
}

TIER_ORDER = ["safe_d", "likely_d", "lean_d", "tossup", "lean_r", "likely_r", "safe_r"]

# Flip threshold at or below which a pickup is achievable in a strong D midterm year.
# 52% is above any statewide D share Ohio Democrats have hit since 2006; it captures
# roughly "winnable if we have a good environment and run a real candidate."
# Keeps lean_r (53–58% needed) out of the primary pickup ladder.
REALISTIC_TARGET_THRESHOLD = 0.52


# ---------------------------------------------------------------------------
# 2026 open seat tracking
# ---------------------------------------------------------------------------

OPEN_SEATS_2026: dict[int, dict[str, str]] = {
    # R-held open seats (incumbent not running in 2026)
    31: {"incumbent": "Bill Roemer",      "reason": "term_limited"},
    35: {"incumbent": "Steve Demetriou",  "reason": "retiring_state_senate"},
    39: {"incumbent": "Phil Plummer",     "reason": "retiring_state_senate"},
    44: {"incumbent": "Josh Williams",    "reason": "retiring_congress"},
    52: {"incumbent": "Gayle Manning",    "reason": "term_limited"},
    57: {"incumbent": "Jamie Callender",  "reason": "term_limited"},
    81: {"incumbent": "Jim Hoops",        "reason": "term_limited_state_senate"},
    # D-held open seats (incumbent not running in 2026)
     7: {"incumbent": "Allison Russo",    "reason": "term_limited_sos_race"},
    18: {"incumbent": "Juanita Brent",    "reason": "term_limited_cleveland_council"},
}
# Source: Wikipedia "2026 Ohio House of Representatives election" + Ballotpedia
# Last verified: March 2026
# UPDATE THIS as new retirements/filing-deadline announcements arrive.


def assign_tier(lean: float) -> str:
    for tier, (lo, hi) in TIER_THRESHOLDS.items():
        if lo <= lean < hi:
            return tier
    return "safe_r"


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------

def classify_districts(df: pd.DataFrame) -> pd.DataFrame:
    """Add tier, current_holder, holder_matches_tier, pickup_opportunity, defensive_priority."""
    df = df.copy()

    df["tier"] = df["composite_lean"].apply(assign_tier)

    def _holder(winner: object) -> str:
        if pd.isna(winner):
            return "unknown"
        s = str(winner)
        if s.startswith("D"):
            return "D"
        if s.startswith("R"):
            return "R"
        return "unknown"

    df["current_holder"] = df["winner_2024"].apply(_holder) if "winner_2024" in df.columns else "unknown"

    _tier_favors: dict[str, str | None] = {
        "safe_d": "D", "likely_d": "D", "lean_d": "D",
        "tossup": None,
        "lean_r": "R", "likely_r": "R", "safe_r": "R",
    }
    df["holder_matches_tier"] = df.apply(
        lambda r: (
            True
            if _tier_favors[r["tier"]] is None
            else _tier_favors[r["tier"]] == r["current_holder"]
        ),
        axis=1,
    )

    # Pickup: R-held district that the composite says is competitive
    _pickup_tiers = {"lean_d", "tossup", "lean_r", "likely_d"}
    df["pickup_opportunity"] = (df["current_holder"] == "R") & df["tier"].isin(_pickup_tiers)

    # Defensive: D-held tossup/lean_r, or D won by < 5 pts in 2024
    _def_tiers = {"tossup", "lean_r", "lean_d"}
    d_competitive = (df["current_holder"] == "D") & df["tier"].isin(_def_tiers)
    close_win = pd.Series(False, index=df.index)
    if "margin_2024" in df.columns:
        close_win = (df["current_holder"] == "D") & (df["margin_2024"].between(0, 0.05))
    df["defensive_priority"] = d_competitive | close_win

    return df


# ---------------------------------------------------------------------------
# Swing metrics
# ---------------------------------------------------------------------------

def compute_swing_metrics(df: pd.DataFrame, house_long: pd.DataFrame) -> pd.DataFrame:
    """
    Add swing_sd, n_contested, turnout_elasticity, target_mode.

    swing_sd       : std dev of D two-party share across contested races, all years.
    n_contested    : number of contested cycles in 2018–2024.
    turnout_elasticity : mean(pres-year house votes) / mean(gov-year house votes),
                         only when the district was contested in both types.
    target_mode    : persuasion / mobilization / hybrid / structural.
    """
    df = df.copy()
    contested = house_long[house_long["contested"]].copy()

    # Swing SD across all 4 years
    swing = (
        contested.groupby("district")["dem_share"]
        .agg(swing_sd="std", n_contested="count")
        .reset_index()
    )
    df = df.merge(swing, on="district", how="left")
    df["n_contested"] = df["n_contested"].fillna(0).astype(int)

    # Turnout elasticity: pres years (2020, 2024) vs gov years (2018, 2022)
    pres = (
        contested[contested["year"].isin([2020, 2024])]
        .groupby("district")["total_two_party"]
        .mean()
        .rename("pres_votes")
    )
    gov = (
        contested[contested["year"].isin([2018, 2022])]
        .groupby("district")["total_two_party"]
        .mean()
        .rename("gov_votes")
    )
    elasticity = (pres / gov).rename("turnout_elasticity").reset_index()
    df = df.merge(elasticity, on="district", how="left")

    # Target mode (per spec)
    def _mode(row: pd.Series) -> str:
        lean = row["composite_lean"]
        sd = row.get("swing_sd")
        el = row.get("turnout_elasticity")
        competitive = abs(lean) < 0.08  # tossup + lean tiers

        sd_high = pd.notna(sd) and sd > 0.06
        el_high = pd.notna(el) and el > 1.3

        if sd_high and competitive:
            return "persuasion"
        if el_high and lean > -0.03:
            return "mobilization"
        if sd_high and el_high:
            return "hybrid"
        return "structural"

    # Apply target mode only for districts with sufficient data
    def _target_mode_with_quality(row: pd.Series) -> str:
        n = row["n_contested"]
        if n == 0:
            return "no_data"
        if n < 3:
            return "insufficient_data"
        return _mode(row)

    # For n_contested == 0: set swing_sd and turnout_elasticity to NaN
    df.loc[df["n_contested"] == 0, "swing_sd"] = np.nan
    df.loc[df["n_contested"] == 0, "turnout_elasticity"] = np.nan

    df["target_mode"] = df.apply(_target_mode_with_quality, axis=1)

    # Print distribution for calibration
    print("\n  Swing SD distribution (contested districts):")
    for threshold in [0.04, 0.06, 0.08, 0.10]:
        n = (df["swing_sd"] > threshold).sum()
        print(f"    SD > {threshold:.2f}: {n} districts")

    print("\n  Turnout elasticity distribution:")
    for threshold in [1.1, 1.2, 1.3, 1.5]:
        n = (df["turnout_elasticity"] > threshold).sum()
        print(f"    elasticity > {threshold:.1f}: {n} districts")

    print("\n  Target mode counts:")
    print(df["target_mode"].value_counts().to_string())

    print(f"\n  n_contested distribution:")
    print(df["n_contested"].value_counts().sort_index().to_string())

    print(f"\n  Data sufficiency for target mode:")
    print(f"    Sufficient (3+ contested): {(df['n_contested'] >= 3).sum()}")
    print(f"    Insufficient (1-2 contested): {((df['n_contested'] >= 1) & (df['n_contested'] < 3)).sum()}")
    print(f"    No data (0 contested): {(df['n_contested'] == 0).sum()}")
    print(f"\n  Target mode (final, with data quality applied):")
    print(df['target_mode'].value_counts().to_string())

    return df


# ---------------------------------------------------------------------------
# Build targeting DataFrame
# ---------------------------------------------------------------------------

def build_targeting_df(
    composite_df: pd.DataFrame,
    house_long: pd.DataFrame,
    candidate_names_2024: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Full targeting DataFrame: classify + swing metrics + flip threshold +
    2026 open seat intelligence.

    Parameters
    ----------
    composite_df : composite lean DataFrame
    house_long   : long-format house results
    candidate_names_2024 : optional DataFrame with columns
        [district, dem_candidate_2024, rep_candidate_2024] from
        extract_candidate_names(sos_2024). When provided, populates
        current_incumbent_name for non-open-seat districts.
    """
    df = classify_districts(composite_df)
    df = compute_swing_metrics(df, house_long)

    # Flip threshold: statewide D% at which this district becomes majority-D.
    # Fundamentals only — no incumbency adjustment.
    df["flip_threshold"] = (0.50 - df["composite_lean"]).round(4)

    # Realistic target: R-held districts winnable in a strong D environment.
    # flip_threshold <= 0.52 means the district tips D at 52% statewide or better.
    # Excludes lean_r (53–58%) and deeper structural long-shots from the primary
    # pickup ladder. Does not replace tier classification — it filters it.
    df["realistic_target"] = (
        (df["current_holder"] == "R") & (df["flip_threshold"] <= REALISTIC_TARGET_THRESHOLD)
    )

    # ── 2026 open seat intelligence ──────────────────────────────────────────
    df["open_seat_2026"] = df["district"].isin(OPEN_SEATS_2026)
    df["open_seat_reason"] = df["district"].map(
        {k: v["reason"] for k, v in OPEN_SEATS_2026.items()}
    )

    # Incumbent name: start from OPEN_SEATS_2026 (known retirements/TL),
    # then fill remaining seats from 2024 candidate data if provided.
    df["current_incumbent_name"] = df["district"].map(
        {k: v["incumbent"] for k, v in OPEN_SEATS_2026.items()}
    )
    if candidate_names_2024 is not None:
        cand = candidate_names_2024[
            ["district", "dem_candidate_2024", "rep_candidate_2024"]
        ].copy()
        df = df.merge(cand, on="district", how="left")

        # Populate winner name for non-open seats (open seats already have name above)
        def _winner_name(row: pd.Series) -> str | None:
            if pd.notna(row.get("current_incumbent_name")):
                return row["current_incumbent_name"]
            w = str(row.get("winner_2024", ""))
            if w.startswith("D"):
                return row.get("dem_candidate_2024")
            if w.startswith("R"):
                return row.get("rep_candidate_2024")
            return None

        df["current_incumbent_name"] = df.apply(_winner_name, axis=1)

    # Incumbent status for 2026 cycle
    def _inc_status(row: pd.Series) -> str:
        if row["open_seat_2026"]:
            return "open_seat"
        if row["current_holder"] in ("D", "R"):
            return "true_incumbent"
        return "unknown"

    df["incumbent_status_2026"] = df.apply(_inc_status, axis=1)

    # Select and order output columns
    lean_cols = [c for c in composite_df.columns if c.endswith("_lean") and c != "composite_lean"]
    cand_cols = []
    if candidate_names_2024 is not None:
        cand_cols = [c for c in ["dem_candidate_2024", "rep_candidate_2024"] if c in df.columns]
    base_cols = [
        "district", "composite_lean", "tier",
        "current_holder", "holder_matches_tier",
        "pickup_opportunity", "defensive_priority",
        "contested_2024", "margin_2024", "candidate_effect_2024",
        "swing_sd", "n_contested", "turnout_elasticity", "target_mode",
        "flip_threshold", "realistic_target",
        "open_seat_2026", "open_seat_reason",
        "current_incumbent_name", "incumbent_status_2026",
    ] + cand_cols
    base_cols = [c for c in base_cols if c in df.columns]
    out = df[base_cols + lean_cols].copy()
    return out.sort_values("composite_lean", ascending=False).reset_index(drop=True)
