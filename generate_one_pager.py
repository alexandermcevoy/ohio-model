"""
Generate one-pager for Kathleen Clyde / Ohio Dems.
Outputs: reports/clyde_one_pager.pdf
"""

import csv
import io
import os
import datetime
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.gridspec as gridspec
from matplotlib.patches import FancyBboxPatch
import numpy as np
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image, HRFlowable
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT

# ── Data ────────────────────────────────────────────────────────────────────

SCENARIO_CSV = "reports/session3/oh_house_scenario_table.csv"
TARGETING_CSV = "reports/session3/oh_house_targeting.csv"

scenario_rows = list(csv.DictReader(open(SCENARIO_CSV)))
targeting_rows = list(csv.DictReader(open(TARGETING_CSV)))

# Scenario curve — CSV values are already in percent form (40.0, 40.5, ...)
sv = [float(r["statewide_d_pct"]) for r in scenario_rows]
seats = [int(r["d_seats"]) for r in scenario_rows]

# Key thresholds
CURRENT_SEATS = 34
HOLD_THRESHOLD_SEATS = 34
REACH_SEATS = 40
MAJORITY_SEATS = 50

def pct_for_seats(target):
    for i, s in enumerate(seats):
        if s >= target:
            return sv[i]
    return None

hold_pct = pct_for_seats(HOLD_THRESHOLD_SEATS)
reach_pct = pct_for_seats(REACH_SEATS)
majority_pct = pct_for_seats(MAJORITY_SEATS)

# Priority pickup districts: R-held, lean > -0.15
pickups = [
    r for r in targeting_rows
    if r.get("current_holder", "") == "R"
    and float(r.get("composite_lean", -1)) > -0.15
]
pickups.sort(key=lambda r: float(r["composite_lean"]), reverse=True)

# ── Chart ────────────────────────────────────────────────────────────────────

FIG_PATH = "/tmp/oh_house_seat_chart.png"

OHIO_BLUE = "#003087"
OHIO_RED  = "#BF0A30"
GRAY      = "#666666"
LGRAY     = "#E8ECF0"
ACCENT    = "#F5A623"

fig, ax = plt.subplots(figsize=(8.5, 4.2))
fig.patch.set_facecolor("white")
ax.set_facecolor("white")

# Shaded regions
ax.axhspan(0,  34, xmin=0, xmax=1, color="#fff0f0", zorder=0)
ax.axhspan(34, 40, xmin=0, xmax=1, color="#fff8ee", zorder=0)
ax.axhspan(40, 50, xmin=0, xmax=1, color="#eef4ff", zorder=0)
ax.axhspan(50, 55, xmin=0, xmax=1, color="#d6e8d6", zorder=0)

# Threshold lines
for y, label, clr in [
    (34, "Hold 34", OHIO_RED),
    (40, "Reach 40", ACCENT),
    (50, "Majority", OHIO_BLUE),
]:
    ax.axhline(y, color=clr, lw=1.2, ls="--", alpha=0.8, zorder=1)
    ax.text(54.8, y + 0.4, label, color=clr, fontsize=8, va="bottom", fontweight="bold")

# Main curve
ax.plot(sv, seats, color=OHIO_BLUE, lw=2.8, zorder=3)
ax.fill_between(sv, seats, 0, alpha=0.08, color=OHIO_BLUE, zorder=2)

# Vertical markers
for pct, y_seats, clr in [
    (hold_pct, HOLD_THRESHOLD_SEATS, OHIO_RED),
    (reach_pct, REACH_SEATS, ACCENT),
    (majority_pct, MAJORITY_SEATS, OHIO_BLUE),
]:
    if pct:
        ax.plot([pct, pct], [0, y_seats], color=clr, lw=1.0, ls=":", alpha=0.7, zorder=2)
        ax.plot(pct, y_seats, "o", color=clr, ms=6, zorder=4)
        ax.text(pct, -2.5, f"{pct:.1f}%", color=clr, fontsize=7.5,
                ha="center", va="top", fontweight="bold")

# 2024 baseline annotation (45.3% statewide D in 2024)
baseline_pct = 45.3
ax.axvline(baseline_pct, color=GRAY, lw=1.2, ls="-.", alpha=0.6, zorder=2)
ax.text(baseline_pct + 0.2, 52, "2024 baseline\n(45.3%)", color=GRAY,
        fontsize=7, va="top", ha="left")

ax.set_xlim(40.0, 55.5)
ax.set_ylim(0, 55)
ax.set_xlabel("Statewide Democratic Vote Share", fontsize=9.5, color="#333")
ax.set_ylabel("Democratic House Seats", fontsize=9.5, color="#333")
ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:.0f}%"))
ax.tick_params(labelsize=8.5, color=GRAY)
for spine in ax.spines.values():
    spine.set_edgecolor(LGRAY)
ax.yaxis.grid(True, color=LGRAY, lw=0.8, zorder=0)

ax.set_title("Ohio House Seat Projections by Statewide Democratic Vote Share",
             fontsize=11, fontweight="bold", color=OHIO_BLUE, pad=10)

legend_patches = [
    mpatches.Patch(color="#fff0f0", label="< 34 seats (lose ground)"),
    mpatches.Patch(color="#fff8ee", label="34–39 (hold current caucus)"),
    mpatches.Patch(color="#eef4ff", label="40–49 (reach goal)"),
    mpatches.Patch(color="#d6e8d6", label="50+ (majority)"),
]
ax.legend(handles=legend_patches, fontsize=7, loc="upper left",
          framealpha=0.9, edgecolor=LGRAY, ncol=2)

fig.subplots_adjust(left=0.08, right=0.88, top=0.91, bottom=0.12)
plt.savefig(FIG_PATH, dpi=150, facecolor="white")
plt.close()

# ── PDF ─────────────────────────────────────────────────────────────────────

OUT_PATH = "reports/clyde_one_pager.pdf"
doc = SimpleDocTemplate(
    OUT_PATH,
    pagesize=letter,
    leftMargin=0.65*inch,
    rightMargin=0.65*inch,
    topMargin=0.5*inch,
    bottomMargin=0.5*inch,
)

styles = getSampleStyleSheet()

def style(name, **kw):
    s = ParagraphStyle(name, parent=styles["Normal"], **kw)
    return s

title_style  = style("title",  fontSize=18, textColor=colors.HexColor(OHIO_BLUE),
                     fontName="Helvetica-Bold", spaceAfter=2, leading=22)
sub_style    = style("sub",    fontSize=10, textColor=colors.HexColor(GRAY),
                     fontName="Helvetica", spaceAfter=6)
head2_style  = style("head2",  fontSize=11, textColor=colors.HexColor(OHIO_BLUE),
                     fontName="Helvetica-Bold", spaceBefore=8, spaceAfter=4)
body_style   = style("body",   fontSize=8.5, leading=13, textColor=colors.HexColor("#222"))
note_style   = style("note",   fontSize=7.5, textColor=colors.HexColor(GRAY),
                     fontName="Helvetica-Oblique")
right_style  = style("right",  fontSize=8, textColor=colors.HexColor(GRAY),
                     alignment=TA_RIGHT)

story = []

# Header
story.append(Paragraph("Ohio House 2026: Targeting Overview", title_style))
story.append(Paragraph(
    f"Prepared for Ohio Democratic Party &nbsp;·&nbsp; {datetime.date.today().strftime('%B %d, %Y')}",
    sub_style
))
story.append(HRFlowable(width="100%", thickness=1.5, color=colors.HexColor(OHIO_BLUE),
                         spaceAfter=8))

# Context blurb
story.append(Paragraph(
    "<b>Current standing:</b> Republicans hold 65 seats; Democrats hold 34. "
    "Democrats need to <b>hold all 34</b> and flip a net of <b>6 seats to reach 40</b>, "
    "or 16 to reach a majority. The analysis below is based on composite partisan lean "
    "across nine statewide races (2018–2024), validated against Dave's Redistricting App "
    "(Spearman ρ = 0.999, MAE = 0.8 pts).",
    body_style
))
story.append(Spacer(1, 6))

# Chart
chart_img = Image(FIG_PATH, width=7.2*inch, height=3.5*inch)
story.append(chart_img)
story.append(Spacer(1, 4))
story.append(Paragraph(
    "Chart reads structural district fundamentals only — does not model candidate quality, "
    "fundraising, or ground game. Open-seat adjustments described below.",
    note_style
))

story.append(Spacer(1, 8))
story.append(HRFlowable(width="100%", thickness=0.5, color=colors.HexColor("#CCCCCC"),
                         spaceAfter=6))

# Thresholds callout row
thresh_data = [
    ["Scenario", "D Seat Total", "Statewide D% Needed", "Net Gain"],
    ["Hold 34 (floor)",   "34",  f"{hold_pct:.1f}%",    "0"],
    ["Reach 40 (target)", "40",  f"{reach_pct:.1f}%",   "+6"],
    ["Majority",          "50",  f"{majority_pct:.1f}%","+16"],
]
thresh_table = Table(thresh_data, colWidths=[2.2*inch, 1.4*inch, 2.0*inch, 1.2*inch])
thresh_table.setStyle(TableStyle([
    ("BACKGROUND",  (0,0), (-1,0),  colors.HexColor(OHIO_BLUE)),
    ("TEXTCOLOR",   (0,0), (-1,0),  colors.white),
    ("FONTNAME",    (0,0), (-1,0),  "Helvetica-Bold"),
    ("FONTSIZE",    (0,0), (-1,-1), 8.5),
    ("ALIGN",       (1,0), (-1,-1), "CENTER"),
    ("ROWBACKGROUNDS", (0,1), (-1,-1),
     [colors.HexColor("#F7F9FC"), colors.white]),
    ("GRID",        (0,0), (-1,-1), 0.4, colors.HexColor("#CCCCCC")),
    ("TOPPADDING",  (0,0), (-1,-1), 4),
    ("BOTTOMPADDING",(0,0),(-1,-1), 4),
    # Highlight reach-40 row
    ("BACKGROUND",  (0,2), (-1,2),  colors.HexColor("#FFF3DC")),
    ("FONTNAME",    (0,2), (-1,2),  "Helvetica-Bold"),
]))
story.append(thresh_table)

story.append(Spacer(1, 10))
story.append(HRFlowable(width="100%", thickness=0.5, color=colors.HexColor("#CCCCCC"),
                         spaceAfter=6))

# Pickup targets table
story.append(Paragraph("Priority Pickup Targets — R-Held Competitive Districts", head2_style))
story.append(Paragraph(
    "Sorted by composite partisan lean in percentage points (most D-favorable first). D+5.0 means the district runs 5 points more Democratic than the statewide average. "
    "<b>Open seat</b> = no incumbent running in 2026 (5–7 pt structural advantage). "
    "<b>Flip@</b> = statewide D% needed to win on fundamentals alone.",
    body_style
))
story.append(Spacer(1, 5))

TIER_LABELS = {
    "lean_d": "Lean D", "tossup": "Tossup",
    "lean_r": "Lean R", "likely_r": "Likely R",
}
TIER_COLORS = {
    "lean_d":   "#C8E6C9",
    "tossup":   "#FFF9C4",
    "lean_r":   "#FFCCBC",
    "likely_r": "#FFAB91",
}

hdr = ["Dist", "Tier", "Composite\nLean (pts)", "Flip@", "Open\nSeat?", "2024\nMargin", "District Notes"]
tbl_data = [hdr]

for r in pickups[:14]:
    dist = r["district"]
    lean = float(r["composite_lean"])
    tier = r["tier"]
    flip = float(r["flip_threshold"])
    open_seat = "YES" if r.get("open_seat_2026", "").lower() in ("true", "1", "yes") else "—"
    margin_raw = r.get("margin_2024", "")
    contested = r.get("contested_2024", "").lower() in ("true", "1")
    try:
        margin_val = float(margin_raw)
        if not contested:
            margin = "Uncontested (R)"
        elif margin_val > 0:
            margin = f"D+{margin_val*100:.1f}"
        else:
            margin = f"R+{abs(margin_val)*100:.1f}"
    except (ValueError, TypeError):
        margin = "—"

    inc = r.get("current_incumbent_name", "—")
    reason = r.get("open_seat_reason", "")
    note = inc if open_seat == "—" else f"{inc} ({reason.replace('_',' ')})"

    tbl_data.append([
        dist,
        TIER_LABELS.get(tier, tier),
        f"D+{lean*100:.1f}" if lean >= 0 else f"R+{abs(lean)*100:.1f}",
        f"{flip*100:.1f}%",
        open_seat,
        margin,
        note,
    ])

col_widths = [0.38*inch, 0.72*inch, 0.72*inch, 0.55*inch, 0.52*inch, 0.75*inch, 3.2*inch]
pickup_table = Table(tbl_data, colWidths=col_widths, repeatRows=1)

ts = TableStyle([
    ("BACKGROUND",   (0,0), (-1,0),  colors.HexColor(OHIO_BLUE)),
    ("TEXTCOLOR",    (0,0), (-1,0),  colors.white),
    ("FONTNAME",     (0,0), (-1,0),  "Helvetica-Bold"),
    ("FONTSIZE",     (0,0), (-1,-1), 7.8),
    ("ALIGN",        (0,0), (-1,-1), "CENTER"),
    ("ALIGN",        (6,1), (6,-1),  "LEFT"),
    ("GRID",         (0,0), (-1,-1), 0.3, colors.HexColor("#CCCCCC")),
    ("TOPPADDING",   (0,0), (-1,-1), 3),
    ("BOTTOMPADDING",(0,0), (-1,-1), 3),
    ("ROWBACKGROUNDS", (0,1), (-1,-1),
     [colors.HexColor("#F7F9FC"), colors.white]),
    ("FONTNAME",     (0,1), (-1,-1), "Helvetica"),
])

# Color tier cells
for i, r in enumerate(pickups[:14], start=1):
    tier = r["tier"]
    bg = TIER_COLORS.get(tier, "#FFFFFF")
    ts.add("BACKGROUND", (1, i), (1, i), colors.HexColor(bg))

# Bold open-seat rows
for i, r in enumerate(pickups[:14], start=1):
    if r.get("open_seat_2026", "").lower() in ("true", "1", "yes"):
        ts.add("FONTNAME", (0, i), (-1, i), "Helvetica-Bold")
        ts.add("BACKGROUND", (4, i), (4, i), colors.HexColor("#C8E6C9"))

pickup_table.setStyle(ts)
story.append(pickup_table)

story.append(Spacer(1, 8))
story.append(HRFlowable(width="100%", thickness=0.5, color=colors.HexColor("#CCCCCC"),
                         spaceAfter=4))

# Footer
story.append(Paragraph(
    "Methodology: Area-weighted spatial overlay of VEST 2020 precinct data onto "
    "current Ohio House district boundaries (Unified Bipartisan Plan, Sept 2023). "
    "Composite lean weights 9 races 2018–2024 (presidential, gubernatorial, U.S. Senate, statewide averages). "
    "External validation: DRA 2024 all-party presidential lean, ρ = 0.9985, MAE = 0.79 pts. "
    "Open-seat incumbency adjustment per published state legislative research (5–7 pt range). "
    "Data as of March 2026 — update district list as filing deadlines pass.",
    note_style
))

doc.build(story)
print(f"Written: {OUT_PATH}")
