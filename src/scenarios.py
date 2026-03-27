"""
scenarios.py — Uniform swing model and path-to-majority analysis.

Model: a district goes D if (statewide_d_share + composite_lean) >= 0.50.
Equivalently, each district's flip threshold = 0.50 - composite_lean.

This is a structural baseline, not a prediction. It abstracts away
incumbency, candidate quality, and spending. Use it to understand which
districts are in range and in what order they become competitive.

Incumbency: the LITERATURE_INCUMBENCY_ADVANTAGE constant (src/constants.py)
is retained for reference and future use once Ohio-specific data is available.
It is NOT applied to any threshold or scenario calculation in this module.
The first opportunity for a valid Ohio cross-sectional incumbency estimate
is after the 2026 election (2024 winners as true incumbents on current maps).
"""

from __future__ import annotations

import pandas as pd

from src.constants import LITERATURE_INCUMBENCY_ADVANTAGE  # reference only

CURRENT_D_SEATS = 34
MAJORITY = 50
VETO_PROOF = 40


# ---------------------------------------------------------------------------
# Scenario table
# ---------------------------------------------------------------------------

def run_scenario_table(
    targeting_df: pd.DataFrame,
    d_range: tuple[float, float] = (0.40, 0.55),
    step: float = 0.005,
) -> pd.DataFrame:
    """
    Run uniform swing scenarios over a range of statewide D shares.

    A district is predicted D if its flip_threshold (= 0.50 - composite_lean)
    is <= the statewide D share. Fundamentals only — no incumbency adjustment.

    Returns
    -------
    DataFrame with columns:
      statewide_d_pct, d_seats, net_change_from_current,
      newly_flipped (districts that cross D at this level vs. previous step),
      cumulative_d_districts (comma-separated)
    """
    steps = round((d_range[1] - d_range[0]) / step) + 1
    statewide_values = [round(d_range[0] + i * step, 4) for i in range(steps)]

    rows = []
    prev_d_set: set[int] = set()

    for sw_d in statewide_values:
        d_mask = targeting_df["flip_threshold"] <= sw_d
        d_districts = set(targeting_df.loc[d_mask, "district"].astype(int).tolist())
        newly_flipped = sorted(d_districts - prev_d_set)
        rows.append(
            {
                "statewide_d_pct": round(sw_d * 100, 1),
                "d_seats": len(d_districts),
                "net_change_from_current": len(d_districts) - CURRENT_D_SEATS,
                "newly_flipped": ",".join(str(d) for d in newly_flipped),
                "cumulative_d_districts": ",".join(
                    str(d) for d in sorted(d_districts)
                ),
            }
        )
        prev_d_set = d_districts

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Pickup ladder
# ---------------------------------------------------------------------------

def build_pickup_ladder(targeting_df: pd.DataFrame) -> pd.DataFrame:
    """
    All R-held districts ranked by how little swing is needed to flip them.

    Sorted by composite_lean descending (most D-leaning R-held district first).
    """
    r_held = targeting_df[targeting_df["current_holder"] == "R"].copy()
    r_held = r_held.sort_values("composite_lean", ascending=False).reset_index(drop=True)
    r_held.insert(0, "ladder_rank", r_held.index + 1)
    return r_held


# ---------------------------------------------------------------------------
# Defensive analysis
# ---------------------------------------------------------------------------

def build_defensive_list(targeting_df: pd.DataFrame) -> pd.DataFrame:
    """
    D-held seats that are at risk.

    Criteria:
    - D-held and tier is tossup, lean_r, or lean_d (competitive range)
    - OR D-held and 2024 margin < 5 pts
    - OR candidate_effect_2024 > 0.08 (significant overperformance — seat may
      depend on candidate quality rather than fundamentals)
    """
    d_held = targeting_df[targeting_df["current_holder"] == "D"].copy()

    competitive = d_held["tier"].isin({"tossup", "lean_r", "lean_d"})

    close = pd.Series(False, index=d_held.index)
    if "margin_2024" in d_held.columns:
        close = d_held["margin_2024"].between(0, 0.05)

    overperforming = pd.Series(False, index=d_held.index)
    if "candidate_effect_2024" in d_held.columns:
        overperforming = d_held["candidate_effect_2024"] > 0.08

    at_risk = d_held[competitive | close | overperforming].copy()
    at_risk["risk_reason"] = ""
    at_risk.loc[competitive, "risk_reason"] = "competitive_tier"
    at_risk.loc[close & ~competitive, "risk_reason"] = "close_margin"
    at_risk.loc[overperforming & ~competitive & ~close, "risk_reason"] = "overperforming"
    at_risk.loc[competitive & close, "risk_reason"] = "competitive_tier+close_margin"
    at_risk.loc[competitive & overperforming, "risk_reason"] += "+overperforming"

    return at_risk.sort_values("composite_lean").reset_index(drop=True)


# ---------------------------------------------------------------------------
# Formatted text report
# ---------------------------------------------------------------------------

def format_pickup_ladder(
    ladder_df: pd.DataFrame,
    scenario_df: pd.DataFrame,
    defensive_df: pd.DataFrame,
) -> str:
    from src.classify import OPEN_SEATS_2026, REALISTIC_TARGET_THRESHOLD

    lines: list[str] = [
        "OHIO HOUSE DEMOCRATIC PICKUP LADDER",
        "=" * 75,
        "Uniform swing model. Flip@ = statewide D% at which composite lean tips D.",
        "Fundamentals only — no incumbency adjustment.",
        f"Current D seats: {CURRENT_D_SEATS}.  "
        f"Majority: {MAJORITY} ({MAJORITY - CURRENT_D_SEATS} net flips).  "
        f"Veto-resistant: {VETO_PROOF} ({VETO_PROOF - CURRENT_D_SEATS} net flips).",
        "",
    ]

    # Key milestones
    lines.append("KEY STATEWIDE THRESHOLDS:")
    for milestone, label in [
        (CURRENT_D_SEATS, f"Hold {CURRENT_D_SEATS} seats (floor)"),
        (VETO_PROOF, f"{VETO_PROOF} seats (veto-resistant)"),
        (MAJORITY, f"{MAJORITY} seats (majority)"),
    ]:
        match = scenario_df[scenario_df["d_seats"] >= milestone]
        if not match.empty:
            pct = match.iloc[0]["statewide_d_pct"]
            lines.append(f"  {label:35s}  statewide D >= {pct:.1f}%")
        else:
            lines.append(f"  {label:35s}  not reached in modeled range")

    hdr = (
        f"{'#':>3}  {'Dist':>4}  {'Lean':>7}  {'Flip@':>6}  {'2026':>6}  "
        f"{'Tier':>8}  {'2024 Margin':>11}  {'Cand Eff':>8}  {'Mode':>12}"
    )
    sep = "-" * 80

    def _format_row(row: pd.Series, rank: int) -> str:
        dist = int(row["district"])
        lean = f"{row['composite_lean']:+.3f}"
        flip_pct = f"{row['flip_threshold'] * 100:.1f}%"
        tier = row.get("tier", "")
        open_flag = "OPEN" if dist in OPEN_SEATS_2026 else ""
        margin = row.get("margin_2024")
        margin_str = (
            f"R+{abs(margin)*100:.1f}" if pd.notna(margin) and margin < 0
            else f"D+{margin*100:.1f}" if pd.notna(margin)
            else "uncontested"
        )
        effect = row.get("candidate_effect_2024")
        eff_str = f"{effect:+.3f}" if pd.notna(effect) else "   n/a"
        mode = str(row.get("target_mode", ""))
        return (
            f"{rank:>3}  {dist:>4}  {lean:>7}  {flip_pct:>6}  {open_flag:>6}  "
            f"{tier:>8}  {margin_str:>11}  {eff_str:>8}  {mode:>12}"
        )

    # Split ladder into realistic targets and structural long-shots
    threshold_pct = REALISTIC_TARGET_THRESHOLD * 100
    realistic = ladder_df[ladder_df["flip_threshold"] <= REALISTIC_TARGET_THRESHOLD].copy()
    longshots = ladder_df[ladder_df["flip_threshold"] > REALISTIC_TARGET_THRESHOLD].copy()

    lines += [
        "",
        "=" * 75,
        f"PICKUP TARGETS — REALISTIC (Flip@ <= {threshold_pct:.0f}%, achievable in a strong D year)",
        "2026: OPEN = confirmed open seat (no incumbent running).",
        "Published research: incumbents typically +5-7 pts vs. open-seat candidates.",
        "=" * 75,
        hdr,
        sep,
    ]
    for rank, (_, row) in enumerate(realistic.iterrows(), 1):
        lines.append(_format_row(row, rank))

    if realistic.empty:
        lines.append("  (none)")

    lines += [
        "",
        "=" * 75,
        f"STRUCTURAL LONG-SHOTS (Flip@ > {threshold_pct:.0f}% — beyond any recent Ohio D environment)",
        f"Shown for completeness. Would require statewide D performance not seen since 2006.",
        "=" * 75,
        hdr,
        sep,
    ]
    for rank, (_, row) in enumerate(longshots.iterrows(), 1):
        lines.append(_format_row(row, rank))

    # Defensive seats
    lines += ["", "=" * 75, "DEFENSIVE PRIORITIES (D-held seats at risk)", "=" * 75]
    if defensive_df.empty:
        lines.append("  No D-held seats flagged as at-risk.")
    else:
        def_hdr = (
            f"{'Dist':>4}  {'Lean':>7}  {'Tier':>8}  {'2024 Margin':>11}  "
            f"{'Cand Eff':>8}  {'2026':>6}  {'Risk Reason'}"
        )
        lines.append(def_hdr)
        lines.append("-" * 70)
        for _, row in defensive_df.iterrows():
            dist = int(row["district"])
            lean = f"{row['composite_lean']:+.3f}"
            tier = row.get("tier", "")
            margin = row.get("margin_2024")
            margin_str = (
                f"D+{margin*100:.1f}" if pd.notna(margin) and margin > 0
                else f"R+{abs(margin)*100:.1f}" if pd.notna(margin)
                else "uncontested"
            )
            effect = row.get("candidate_effect_2024")
            eff_str = f"{effect:+.3f}" if pd.notna(effect) else "   n/a"
            open_flag = "OPEN" if dist in OPEN_SEATS_2026 else ""
            reason = str(row.get("risk_reason", ""))
            lines.append(
                f"{dist:>4}  {lean:>7}  {tier:>8}  {margin_str:>11}  "
                f"{eff_str:>8}  {open_flag:>6}  {reason}"
            )

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 2026 Open Seat Opportunities Report
# ---------------------------------------------------------------------------

def build_2026_opportunities_report(targeting_df: pd.DataFrame) -> str:
    """
    Qualitative 2026 open seat intelligence report.

    Shows fundamentals-only flip thresholds and 2024 actual results for each
    known open seat. Does not apply incumbency adjustments — the qualitative
    note that "published research finds incumbents +5-7 pts" conveys the
    significance without asserting precision we don't have for Ohio.
    """
    from src.classify import OPEN_SEATS_2026

    def _margin_str(row: pd.Series) -> str:
        margin = row.get("margin_2024")
        if pd.isna(margin):
            return "uncontested"
        if margin < 0:
            return f"R+{abs(margin)*100:.1f}"
        return f"D+{margin*100:.1f}"

    lines: list[str] = [
        "2026 OHIO HOUSE OPEN SEAT OPPORTUNITIES",
        "=" * 70,
        "These districts will not have an incumbent running in 2026.",
        "Published research finds state legislative incumbents typically",
        "outperform open-seat candidates of the same party by 5-7 points.",
        "These seats are meaningfully more competitive without an incumbent.",
        "",
        "Flip@ = statewide D% needed to win on fundamentals alone (0.50 - composite_lean).",
        "This is the structural threshold. Absence of an incumbent makes it",
        "operationally achievable in a way it may not be against a true incumbent.",
        "",
    ]

    # R-held open seats
    r_open = {
        k: v for k, v in OPEN_SEATS_2026.items()
        if not targeting_df[targeting_df["district"] == k].empty
        and targeting_df[targeting_df["district"] == k].iloc[0]["current_holder"] == "R"
    }
    r_open_sorted = sorted(
        r_open.items(),
        key=lambda kv: float(targeting_df[targeting_df["district"] == kv[0]]["composite_lean"].iloc[0]),
        reverse=True,
    )

    lines += [
        "R-HELD OPEN SEATS",
        "=" * 70,
        f"{'Dist':>4}  {'Lean':>7}  {'Tier':>8}  {'Incumbent':<20}  {'Reason':<28}  {'Flip@':>6}  {'2024 Result':>11}",
        "-" * 90,
    ]
    for dist_num, info in r_open_sorted:
        row = targeting_df[targeting_df["district"] == dist_num].iloc[0]
        lean = f"{row['composite_lean']:+.3f}"
        tier = str(row.get("tier", ""))
        flip = row.get("flip_threshold")
        flip_str = f"{flip * 100:.1f}%" if pd.notna(flip) else "n/a"
        lines.append(
            f"{dist_num:>4}  {lean:>7}  {tier:>8}  {info['incumbent']:<20}  "
            f"{info['reason']:<28}  {flip_str:>6}  {_margin_str(row):>11}"
        )

    # D-held open seats
    d_open = {
        k: v for k, v in OPEN_SEATS_2026.items()
        if not targeting_df[targeting_df["district"] == k].empty
        and targeting_df[targeting_df["district"] == k].iloc[0]["current_holder"] == "D"
    }
    d_open_sorted = sorted(
        d_open.items(),
        key=lambda kv: float(targeting_df[targeting_df["district"] == kv[0]]["composite_lean"].iloc[0]),
    )

    lines += [
        "",
        "D-HELD OPEN SEATS (defensive priorities)",
        "=" * 70,
        f"{'Dist':>4}  {'Lean':>7}  {'Tier':>8}  {'Incumbent':<20}  {'Reason':<28}  {'Flip@':>6}  {'2024 Result':>11}",
        "-" * 90,
    ]
    for dist_num, info in d_open_sorted:
        row = targeting_df[targeting_df["district"] == dist_num].iloc[0]
        lean = f"{row['composite_lean']:+.3f}"
        tier = str(row.get("tier", ""))
        flip = row.get("flip_threshold")
        flip_str = f"{flip * 100:.1f}%" if pd.notna(flip) else "n/a"
        lines.append(
            f"{dist_num:>4}  {lean:>7}  {tier:>8}  {info['incumbent']:<20}  "
            f"{info['reason']:<28}  {flip_str:>6}  {_margin_str(row):>11}"
        )

    lines += [
        "",
        "Notes:",
        "- Flip@ is the fundamentals-only threshold (0.50 - composite_lean).",
        "- Open seats remove the incumbency advantage but do not change district fundamentals.",
        "- Districts 31, 52, 35 are structurally Lean D — competitive regardless of incumbent.",
        "- Districts 39, 44 are Tossup — open-seat status makes them genuinely winnable.",
        "- Districts 57, 81 are Lean R / Safe R — open seat is necessary but not sufficient.",
        "",
        "Source: OPEN_SEATS_2026 in src/classify.py (Wikipedia + Ballotpedia, March 2026).",
        "Update this dict as new retirements and filing decisions are announced.",
        "Incumbency estimation from Ohio data: possible after 2026 election using",
        "2024 winners (true incumbents on current maps) vs. 2026 open seats.",
    ]

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Summary print
# ---------------------------------------------------------------------------

def print_scenario_summary(scenario_df: pd.DataFrame) -> None:
    """Print scenario table to stdout (condensed — every 1 point)."""
    print(
        f"\n{'Statewide D%':>13}  {'D Seats':>8}  {'Net Chg':>8}  {'Newly Flipped Districts'}"
    )
    print("-" * 75)
    for _, row in scenario_df.iterrows():
        pct = row["statewide_d_pct"]
        if pct % 1.0 != 0:  # print every 1.0 pct point only
            continue
        seats = int(row["d_seats"])
        net = int(row["net_change_from_current"])
        flipped = row["newly_flipped"] if row["newly_flipped"] else "—"
        net_str = f"{net:+d}"
        print(f"{pct:>12.1f}%  {seats:>8}  {net_str:>8}  {flipped}")


# ---------------------------------------------------------------------------
# Combined deterministic + probabilistic summary
# ---------------------------------------------------------------------------

def build_combined_scenario_summary(
    deterministic_df: pd.DataFrame,
    probabilistic_df: pd.DataFrame,
) -> str:
    """
    Format side-by-side comparison of deterministic vs. probabilistic seat projections.

    Parameters
    ----------
    deterministic_df : from run_scenario_table(), columns: statewide_d_pct, d_seats
    probabilistic_df : from run_probabilistic_scenario_table(), columns:
        statewide_d_pct, mean_d_seats, p10_seats, p90_seats, prob_hold_34,
        prob_reach_40, prob_majority
    """
    lines = []
    lines.append("  Deterministic vs. Probabilistic Scenario Comparison")
    lines.append("  " + "=" * 80)
    lines.append(
        f"  {'Statewide D':>11}  │ {'Determ.':>7}  │  "
        f"{'Mean':>5}  {'80% CI':>10}  "
        f"{'P(≥34)':>7}  {'P(≥40)':>7}  {'P(50)':>6}"
    )
    lines.append("  " + "─" * 80)

    merged = deterministic_df.merge(
        probabilistic_df,
        on="statewide_d_pct",
        how="outer",
        suffixes=("_det", "_prob"),
    ).sort_values("statewide_d_pct")

    for _, row in merged.iterrows():
        pct = row["statewide_d_pct"]
        if pct % 1.0 != 0:
            continue

        det_seats = int(row["d_seats"]) if pd.notna(row.get("d_seats")) else "—"
        mean = row.get("mean_d_seats", float("nan"))
        p10 = row.get("p10_seats", float("nan"))
        p90 = row.get("p90_seats", float("nan"))
        ph34 = row.get("prob_hold_34", float("nan"))
        pr40 = row.get("prob_reach_40", float("nan"))
        pmaj = row.get("prob_majority", float("nan"))

        mean_s = f"{mean:>5.1f}" if pd.notna(mean) else "  —  "
        ci_s = f"[{int(p10):>2}, {int(p90):>2}]" if pd.notna(p10) else "    —     "
        ph34_s = f"{ph34:>6.1%}" if pd.notna(ph34) else "   —  "
        pr40_s = f"{pr40:>6.1%}" if pd.notna(pr40) else "   —  "
        pmaj_s = f"{pmaj:>5.1%}" if pd.notna(pmaj) else "  —  "

        lines.append(
            f"  {pct:>10.1f}%  │ {det_seats:>7}  │  "
            f"{mean_s}  {ci_s}   "
            f"{ph34_s}  {pr40_s}  {pmaj_s}"
        )

    return "\n".join(lines)
