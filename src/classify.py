"""
classify.py — District tier classification and targeting framework.

Tiers are based on **win probability** at reference statewide environments,
following Cook/Sabato convention (Safe D → Safe R). Composite lean is kept
as a raw number — no lean-threshold bucketing.

Swing SD and turnout elasticity are computed from contested house races only.
Districts with fewer than 2 contested races are flagged (n_contested < 2).
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import norm

from src.constants import LITERATURE_INCUMBENCY_ADVANTAGE  # kept for reference; not wired to calculations
from src.office import HOUSE, OfficeConfig

REPO_ROOT = Path(__file__).resolve().parent.parent
PROCESSED = REPO_ROOT / "data" / "processed"

# ---------------------------------------------------------------------------
# Win-probability tier thresholds (Cook-style)
# ---------------------------------------------------------------------------

WP_TIER_THRESHOLDS: list[tuple[str, float, float]] = [
    # (tier_name, wp_lower_bound_inclusive, wp_upper_bound_exclusive)
    ("safe_d",   0.95, float("inf")),
    ("likely_d", 0.75, 0.95),
    ("lean_d",   0.55, 0.75),
    ("tossup",   0.45, 0.55),
    ("lean_r",   0.25, 0.45),
    ("likely_r", 0.05, 0.25),
    ("safe_r",   0.00, 0.05),
]

TIER_ORDER = ["safe_d", "likely_d", "lean_d", "tossup", "lean_r", "likely_r", "safe_r"]

REFERENCE_ENVIRONMENTS: list[float] = [0.46, 0.48, 0.50]

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
#
# This in-memory dict is the seed for the Ohio House and remains the canonical
# source for that office (mirrored to data/processed/open_seats_2026_house.csv).
# For senate / congress, populate per-office CSVs at the same path:
#   data/processed/open_seats_2026_{office.name}.csv  (columns: district, incumbent, reason)
# load_open_seats(office) below handles dispatch.


def load_open_seats(office: OfficeConfig | None = None) -> dict[int, dict[str, str]]:
    """
    Load 2026 open-seat intelligence for the given office.

    For Ohio House, returns the in-memory OPEN_SEATS_2026 seed (preserves the
    no-CSV-required path that has been the convention throughout development).
    For other offices, reads data/processed/open_seats_2026_{office.name}.csv.
    Returns an empty dict if no CSV exists yet (multi-office work in progress).
    """
    if office is None or office.name == "house":
        return OPEN_SEATS_2026

    csv_path = PROCESSED / f"open_seats_2026_{office.name}.csv"
    if not csv_path.exists():
        return {}

    df = pd.read_csv(csv_path)
    return {
        int(row["district"]): {"incumbent": str(row["incumbent"]), "reason": str(row["reason"])}
        for _, row in df.iterrows()
    }


def assign_tier_from_wp(win_prob: float) -> str:
    """Map a D win probability to a Cook-style tier label."""
    for tier, lo, hi in WP_TIER_THRESHOLDS:
        if lo <= win_prob < hi:
            return tier
    return "safe_r"


# Legacy lean-based tier assignment — kept for backward compat in drop-one
# sensitivity (where we care about composite robustness, not projected outcomes).
_LEAN_TIER_THRESHOLDS: dict[str, tuple[float, float]] = {
    "safe_d":   (+0.15, float("inf")),
    "likely_d": (+0.08, +0.15),
    "lean_d":   (+0.03, +0.08),
    "tossup":   (-0.03, +0.03),
    "lean_r":   (-0.08, -0.03),
    "likely_r": (-0.15, -0.08),
    "safe_r":   (float("-inf"), -0.15),
}


def _assign_tier_from_lean(lean: float) -> str:
    """Legacy: map composite lean to tier via fixed thresholds."""
    for tier, (lo, hi) in _LEAN_TIER_THRESHOLDS.items():
        if lo <= lean < hi:
            return tier
    return "safe_r"


# Public alias for backward compatibility (used by composite.py drop-one, backtest.py)
assign_tier = _assign_tier_from_lean


def format_lean(lean: float) -> str:
    """Format composite lean as 'D+3.3' / 'R+5.2' / 'EVEN' (Cook/Sabato style)."""
    pts = lean * 100
    if abs(pts) < 0.05:
        return "EVEN"
    if pts > 0:
        return f"D+{pts:.1f}"
    return f"R+{abs(pts):.1f}"


def compute_wp_at_environment(
    composite_lean: pd.Series,
    sigma_i: pd.Series,
    statewide_d: float,
) -> pd.Series:
    """Compute analytical D win probability at a given statewide environment.

    P(D wins) = Φ((statewide_d + lean - 0.50) / sigma_i)
    """
    margin = statewide_d + composite_lean - 0.50
    return pd.Series(norm.cdf(margin / sigma_i), index=composite_lean.index)


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------

def classify_districts(
    df: pd.DataFrame,
    sigma_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Add tier columns, current_holder, holder_matches_tier, pickup_opportunity, defensive_priority.

    If sigma_df is provided, tiers are based on win probability at reference
    environments (Cook-style). Otherwise falls back to lean-threshold tiers.

    Parameters
    ----------
    df : DataFrame with at least ``composite_lean`` and optionally ``winner_2024``.
    sigma_df : DataFrame with columns ``district``, ``sigma_i`` from
        ``simulate.estimate_district_sigma()``. When provided, enables
        WP-based tier assignment at each reference environment.
    """
    df = df.copy()

    if sigma_df is not None:
        # WP-based tiers at each reference environment
        merged = df[["district", "composite_lean"]].merge(
            sigma_df[["district", "sigma_i"]], on="district", how="left",
        )
        for env in REFERENCE_ENVIRONMENTS:
            env_label = f"{int(env * 100)}"
            wp = compute_wp_at_environment(
                merged["composite_lean"], merged["sigma_i"], env,
            )
            df[f"wp_{env_label}"] = wp.values
            df[f"tier_{env_label}"] = wp.apply(assign_tier_from_wp).values

        # Primary tier = 48% environment (neutral midterm)
        df["tier"] = df["tier_48"]
    else:
        # Legacy fallback: lean-threshold tiers
        import warnings
        warnings.warn(
            "classify_districts() called without sigma_df — using legacy "
            "lean-threshold tiers. Pass sigma_df for WP-based tiers.",
            DeprecationWarning, stacklevel=2,
        )
        df["tier"] = df["composite_lean"].apply(_assign_tier_from_lean)

    def _holder(winner: object) -> str:
        if pd.isna(winner):
            return "unknown"
        s = str(winner)
        if s.startswith("D"):
            return "D"
        if s.startswith("R"):
            return "R"
        return "unknown"

    # Walk most-recent-first across available winner columns. Important for
    # State Senate staggered terms: odd-numbered districts last had elections
    # in 2022, even-numbered in 2024, so winner_2024 alone leaves half the
    # chamber as "unknown".
    winner_cols = [
        c for c in ("winner_2024", "winner_2022", "winner_2020", "winner_2018")
        if c in df.columns
    ]
    if winner_cols:
        holder = pd.Series("unknown", index=df.index, dtype=object)
        winner_year = pd.Series(pd.NA, index=df.index, dtype="Int64")
        for col in winner_cols:
            year = int(col.split("_")[1])
            mask_unknown = holder == "unknown"
            new_holder = df[col].apply(_holder)
            fill_mask = mask_unknown & (new_holder != "unknown")
            holder = holder.where(~fill_mask, new_holder)
            winner_year = winner_year.where(~fill_mask, year)
        df["current_holder"] = holder
        df["current_holder_source_year"] = winner_year
    else:
        df["current_holder"] = "unknown"
        df["current_holder_source_year"] = pd.NA

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

    # Pickup: R-held district in competitive WP tier (at neutral environment)
    _pickup_tiers = {"lean_d", "tossup", "lean_r", "likely_d"}
    df["pickup_opportunity"] = (df["current_holder"] == "R") & df["tier"].isin(_pickup_tiers)

    # Defensive: D-held tossup/lean_r/lean_d, or D won by < 5 pts in 2024
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
    sigma_df: pd.DataFrame | None = None,
    office: OfficeConfig | None = None,
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
        extract_candidate_names(sos_2024, HOUSE). When provided, populates
        current_incumbent_name for non-open-seat districts.
    sigma_df : optional DataFrame with columns [district, sigma_i] from
        estimate_district_sigma(). When provided, tiers are based on
        win probability at reference environments.
    """
    df = classify_districts(composite_df, sigma_df=sigma_df)
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

    # ── Race-result reliability under current map ────────────────────────────
    # The composite lean is built on the *current* map via block reaggregation,
    # so it's clean. But race-result columns (dem_share_{year}, winner_{year},
    # margin_{year}, contested_{year}) are joined by district *number* and
    # therefore valid only for years when the same map+numbering was in effect.
    #
    # Conservative defaults per office:
    #   house    — defer to existing redistricting_overlap.csv (years_reliable
    #              column is added by apply_redistricting_filter / merge upstream).
    #              Only set here if missing entirely.
    #   senate   — current map (Sept 2023) used 2024+; odd-numbered districts
    #              last had elections in 2022 under interim/old maps, so 2022
    #              numbering does NOT match 2026 numbering. 2024 reliable for
    #              even districts only.
    #   congress — Oct 31, 2025 plan is BRAND NEW; no prior CD-numbered
    #              results align with 2026 numbering. years_reliable is empty
    #              for every CD.
    if office is not None and office.name == "senate":
        df["years_reliable_for_race_history"] = df["district"].apply(
            lambda d: "2024" if d % 2 == 0 else ""
        )
    elif office is not None and office.name == "congress":
        df["years_reliable_for_race_history"] = ""
    elif "years_reliable_for_race_history" not in df.columns and "years_reliable" not in df.columns:
        # House default — pre-existing redistricting_overlap merge usually fills
        # `years_reliable`; leave that alone if already present.
        df["years_reliable_for_race_history"] = "2024"

    # ── 2026 open seat intelligence ──────────────────────────────────────────
    open_seats = load_open_seats(office)
    df["open_seat_2026"] = df["district"].isin(open_seats)
    df["open_seat_reason"] = df["district"].map(
        {k: v["reason"] for k, v in open_seats.items()}
    )

    # Incumbent name: start from open-seat data (known retirements/TL),
    # then fill remaining seats from 2024 candidate data if provided.
    df["current_incumbent_name"] = df["district"].map(
        {k: v["incumbent"] for k, v in open_seats.items()}
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

    # WP tier columns at each reference environment
    env_cols = []
    for env in REFERENCE_ENVIRONMENTS:
        env_label = f"{int(env * 100)}"
        for prefix in ["tier_", "wp_"]:
            col = f"{prefix}{env_label}"
            if col in df.columns:
                env_cols.append(col)

    base_cols = [
        "district", "composite_lean", "tier",
    ] + env_cols + [
        "current_holder", "current_holder_source_year", "holder_matches_tier",
        "pickup_opportunity", "defensive_priority",
        "years_reliable_for_race_history",
        "contested_2024", "margin_2024", "candidate_effect_2024",
        "swing_sd", "n_contested", "turnout_elasticity", "target_mode",
        "flip_threshold", "realistic_target",
        "open_seat_2026", "open_seat_reason",
        "current_incumbent_name", "incumbent_status_2026",
    ] + cand_cols
    base_cols = [c for c in base_cols if c in df.columns]
    out = df[base_cols + lean_cols].copy()
    return out.sort_values("composite_lean", ascending=False).reset_index(drop=True)
