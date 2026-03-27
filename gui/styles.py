"""
styles.py — Color maps, formatting helpers, and CSS overrides for the Streamlit GUI.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Tier colors (hex strings for Plotly, mirroring src/export.py TIER_COLORS)
# ---------------------------------------------------------------------------

TIER_COLORS: dict[str, str] = {
    "safe_d":   "#1565C0",
    "likely_d": "#1976D2",
    "lean_d":   "#42A5F5",
    "tossup":   "#757575",
    "lean_r":   "#EF5350",
    "likely_r": "#D32F2F",
    "safe_r":   "#B71C1C",
}

TIER_LABELS: dict[str, str] = {
    "safe_d":   "Safe D",
    "likely_d": "Likely D",
    "lean_d":   "Lean D",
    "tossup":   "Tossup",
    "lean_r":   "Lean R",
    "likely_r": "Likely R",
    "safe_r":   "Safe R",
}

TIER_ORDER: list[str] = [
    "safe_d", "likely_d", "lean_d", "tossup",
    "lean_r", "likely_r", "safe_r",
]

# ---------------------------------------------------------------------------
# Portfolio tier colors
# ---------------------------------------------------------------------------

PORTFOLIO_COLORS: dict[str, str] = {
    "Core":      "#1565C0",   # dark blue
    "Stretch":   "#E65100",   # dark orange
    "Long-Shot": "#9E9E9E",   # gray
    "D-held":    "#42A5F5",   # light blue
}

# ---------------------------------------------------------------------------
# Holder colors
# ---------------------------------------------------------------------------

HOLDER_COLORS: dict[str, str] = {
    "D": "#1976D2",
    "R": "#D32F2F",
}

# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------


def fmt_pct(value: float | None, decimals: int = 1) -> str:
    """Format a 0–1 float as percentage string."""
    if value is None:
        return "n/a"
    return f"{value * 100:.{decimals}f}%"


def fmt_lean(value: float | None) -> str:
    """Format composite lean as 'D+3.3' / 'R+5.2' / 'EVEN' (Cook/Sabato style)."""
    if value is None:
        return "n/a"
    pts = value * 100
    if abs(pts) < 0.05:
        return "EVEN"
    if pts > 0:
        return f"D+{pts:.1f}"
    return f"R+{abs(pts):.1f}"


def lean_to_margin(lean: float, statewide_d: float) -> float:
    """Convert composite lean to expected margin at a given statewide D environment.

    Parameters
    ----------
    lean : partisan lean (positive = more D than state average)
    statewide_d : statewide D two-party share as percentage (e.g. 48.0)

    Returns
    -------
    Expected margin (positive = D wins by that amount, as a 0–1 fraction).
    """
    return lean + (statewide_d / 100.0 - 0.50)


def fmt_margin(margin: float | None) -> str:
    """Format expected margin as 'D+3.2' / 'R-4.1' / 'EVEN'.

    Input is a 0–1 fraction (e.g. 0.032 → 'D+3.2').
    """
    if margin is None:
        return "n/a"
    pts = margin * 100
    if abs(pts) < 0.05:
        return "EVEN"
    if pts > 0:
        return f"D+{pts:.1f}"
    return f"R+{abs(pts):.1f}"


def fmt_dollar(value: float | None) -> str:
    """Format as dollar amount."""
    if value is None:
        return "n/a"
    return f"${value:,.0f}"


def tier_badge(tier: str) -> str:
    """Return a colored markdown badge for a tier."""
    color = TIER_COLORS.get(tier, "#757575")
    label = TIER_LABELS.get(tier, tier)
    return f'<span style="background-color:{color};color:white;padding:2px 8px;border-radius:4px;font-weight:bold">{label}</span>'


# ---------------------------------------------------------------------------
# Wide layout CSS
# ---------------------------------------------------------------------------

WIDE_CSS = """
<style>
    .block-container { max-width: 1200px; }
    [data-testid="stMetric"] { text-align: center; }
</style>
"""
