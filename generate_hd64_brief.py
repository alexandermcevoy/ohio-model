"""
Deep-dive brief: Ohio House District 64.
Outputs: reports/hd64_deep_dive.pdf

Built for one argument: Santucci's 12-point 2024 margin makes HD-64 look like
a safe R seat. The composite lean (built from 9 statewide races 2018-2024,
each compared to the same year's statewide baseline) says otherwise — this
is a structural toss-up where most of that 12-point gap is the 2024
statewide environment, not the district or the candidates. No Democrat
filed for 2026. That's the actual story here.
"""

import json
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image, HRFlowable
)
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT

DISTRICT = 64

OHIO_BLUE = "#003087"
OHIO_RED = "#BF0A30"
GRAY = "#666666"
LGRAY = "#E8ECF0"
ACCENT = "#F5A623"

# ── Load data ────────────────────────────────────────────────────────────────

composite = pd.read_csv("reports/session2/oh_house_composite_lean.csv")
targeting = pd.read_csv("reports/session3/oh_house_targeting.csv")
trends = pd.read_csv("data/processed/oh_house_district_trends.csv")
overlap = pd.read_csv("reports/redistricting_overlap.csv")
demo = pd.read_csv("data/processed/oh_house_demographics.csv").rename(columns={"district_num": "district"})
year_baselines = json.load(open("data/processed/year_baselines.json"))
detail = pd.read_csv("reports/session8/oh_house_forecast_district_detail.csv")

c = composite[composite["district"] == DISTRICT].iloc[0]
t = targeting[targeting["district"] == DISTRICT].iloc[0]
tr = trends[trends["district"] == DISTRICT].iloc[0]
ov = overlap[overlap["district"] == DISTRICT].iloc[0]
d = detail[detail["district"] == DISTRICT].iloc[0]

RACES = [
    ("gov_2018", "2018 Governor\n(Cordray)"),
    ("statewide_avg_2018", "2018 Statewide\nAvg"),
    ("uss_2018", "2018 US Senate\n(Sherrod Brown)"),
    ("pre_2020", "2020 President\n(Biden)"),
    ("gov_2022", "2022 Governor\n(Whaley)"),
    ("statewide_avg_2022", "2022 Statewide\nAvg"),
    ("uss_2022", "2022 US Senate\n(Ryan)"),
    ("pre_2024", "2024 President\n(Harris)"),
    ("uss_2024", "2024 US Senate\n(Sherrod Brown)"),
]
race_leans = [(label, c[f"{key}_lean"] * 100) for key, label in RACES]

# ── Chart: per-race lean vs. state average ──────────────────────────────────

FIG_PATH = "/tmp/hd64_race_leans.png"
fig, ax = plt.subplots(figsize=(9.0, 3.4))
fig.patch.set_facecolor("white")
ax.set_facecolor("white")

labels = [r[0] for r in race_leans]
values = [r[1] for r in race_leans]
colors_bar = [OHIO_BLUE if v >= 0 else OHIO_RED for v in values]
# Highlight the two Sherrod Brown races distinctly
highlight_idx = [i for i, (label, _) in enumerate(race_leans) if "Sherrod" in label]

bars = ax.bar(range(len(values)), values, color=colors_bar, width=0.6, zorder=3)
for i in highlight_idx:
    bars[i].set_edgecolor(ACCENT)
    bars[i].set_linewidth(2.5)

ax.axhline(0, color="#333", lw=1.0, zorder=2)
comp = c["composite_lean"] * 100
ax.axhline(comp, color=GRAY, lw=1.4, ls="--", zorder=2)
ax.text(len(values) - 0.4, comp + 0.4, f"Composite: D+{comp:.1f}", color=GRAY,
        fontsize=8.5, fontweight="bold", ha="right")

for i, v in enumerate(values):
    label = f"D+{v:.1f}" if v >= 0 else f"R+{abs(v):.1f}"
    ax.text(i, v + (0.35 if v >= 0 else -0.35), label, ha="center",
            va="bottom" if v >= 0 else "top", fontsize=8, fontweight="bold",
            color=OHIO_BLUE if v >= 0 else OHIO_RED)

ax.set_xticks(range(len(labels)))
ax.set_xticklabels(labels, fontsize=7.8)
ax.set_ylabel("District lean vs. statewide average (pts)", fontsize=9)
ax.set_ylim(min(values) - 2.5, max(values) + 3)
for spine in ["top", "right"]:
    ax.spines[spine].set_visible(False)
ax.spines["left"].set_color(LGRAY)
ax.spines["bottom"].set_color(LGRAY)
ax.yaxis.grid(True, color=LGRAY, lw=0.8, zorder=0)
ax.set_title("HD-64 has been within a few points of even in every statewide race since 2018",
             fontsize=10.5, fontweight="bold", color=OHIO_BLUE, pad=10)

fig.subplots_adjust(left=0.08, right=0.97, top=0.85, bottom=0.22)
plt.savefig(FIG_PATH, dpi=150, facecolor="white")
plt.close()

# ── PDF ──────────────────────────────────────────────────────────────────────

OUT_PATH = "reports/hd64_deep_dive.pdf"
doc = SimpleDocTemplate(
    OUT_PATH, pagesize=letter,
    leftMargin=0.65 * inch, rightMargin=0.65 * inch,
    topMargin=0.5 * inch, bottomMargin=0.5 * inch,
)
styles = getSampleStyleSheet()
story = []

title_style = ParagraphStyle("TitleC", parent=styles["Title"], fontSize=19,
                              textColor=colors.HexColor(OHIO_BLUE), spaceAfter=2, alignment=TA_LEFT)
subtitle_style = ParagraphStyle("Sub", parent=styles["Normal"], fontSize=12,
                                 textColor=colors.HexColor(GRAY), spaceAfter=10, alignment=TA_LEFT)
h2 = ParagraphStyle("H2", parent=styles["Heading2"], fontSize=12.5,
                     textColor=colors.HexColor(OHIO_BLUE), spaceBefore=12, spaceAfter=4)
body = ParagraphStyle("Body", parent=styles["Normal"], fontSize=9.7, leading=13.5, spaceAfter=6)
small = ParagraphStyle("Small", parent=styles["Normal"], fontSize=8, leading=10.5,
                        textColor=colors.HexColor(GRAY))

story.append(Paragraph("Ohio House District 64: The Case for Investment", title_style))
story.append(Paragraph("Why a 12-point loss doesn't mean what it looks like — and what it cost us in 2026", subtitle_style))
story.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor(LGRAY), spaceAfter=10))

# Key stats row
stat_data = [[
    Paragraph("<b>Composite Lean</b><br/><font size=16 color='%s'>D+1.5</font>" % OHIO_BLUE, body),
    Paragraph("<b>2026 Model Win Prob.</b><br/><font size=16 color='%s'>63%%</font>" % OHIO_BLUE, body),
    Paragraph("<b>2024 Actual Margin</b><br/><font size=16 color='%s'>R+12.5</font>" % OHIO_RED, body),
    Paragraph("<b>2026 Status</b><br/><font size=13 color='%s'>No D Filed</font>" % OHIO_RED, body),
]]
stat_table = Table(stat_data, colWidths=[1.65 * inch] * 4)
stat_table.setStyle(TableStyle([
    ("BOX", (0, 0), (-1, -1), 0.75, colors.HexColor(LGRAY)),
    ("INNERGRID", (0, 0), (-1, -1), 0.75, colors.HexColor(LGRAY)),
    ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#fafbfc")),
    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ("TOPPADDING", (0, 0), (-1, -1), 8),
    ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
    ("LEFTPADDING", (0, 0), (-1, -1), 10),
]))
story.append(stat_table)
story.append(Spacer(1, 12))

story.append(Paragraph("The trap in the raw number", h2))
story.append(Paragraph(
    "Nick Santucci beat Lauren Mathews by 12.5 points in 2024. On its face, that reads as a safe "
    "Republican seat not worth another look. But 2024 was a <b>44.3% statewide Democratic environment</b> "
    "— one of the weakest Ohio cycles in our dataset (2018 was 48.1%, 2020 was 45.9%). Decomposing the "
    "12.5-point margin: fundamentals alone (this district's D+1.5 composite lean applied to the actual "
    "2024 statewide environment) predict roughly an <b>8-point Republican margin</b> here. The remaining "
    "~4 points is the actual 2024 candidate effect — a real but modest underperformance, not a landslide. "
    "Most of what makes this look like a blowout is the year, not the district.",
    body
))

story.append(Image(FIG_PATH, width=6.9 * inch, height=2.6 * inch))
story.append(Paragraph(
    "Composite lean = district's two-party D share minus the statewide two-party D share, for each race, that year. "
    "Positive = district ran more Democratic than the state that cycle. Sherrod Brown's two Senate races are outlined — "
    "even in his losing 2024 statewide campaign, he still ran <b>ahead of his own statewide number</b> in HD-64.",
    small
))
story.append(Spacer(1, 8))

story.append(Paragraph("Track record on the current map: 2022 and 2024", h2))
cell = ParagraphStyle("Cell", parent=styles["Normal"], fontSize=7.6, leading=9.6)
header_cell = ParagraphStyle("HeaderCell", parent=cell, textColor=colors.white, fontName="Helvetica-Bold")
track_data = [
    [Paragraph(x, header_cell) for x in
     ["Year", "D Votes", "R Votes", "D Share", "Margin", "Candidate Effect", "Note"]],
    [Paragraph(x, cell) for x in [
        "2022", "21,072", "21,847", "49.1%", "R+1.8", "+10.1 pts (D over-perf.)",
        "Model flags this as a possible redistricting-boundary artifact — treat with caution, "
        "not as proof of a strong candidate.",
    ]],
    [Paragraph(x, cell) for x in [
        "2024", "23,695", "30,439", "43.8%", "R+12.5", "−4.2 pts margin (mild R lean)",
        "Reliable data point. Most of the margin is the 2024 environment, not the candidates.",
    ]],
]
track_table = Table(track_data, colWidths=[0.4*inch, 0.68*inch, 0.68*inch, 0.6*inch, 0.6*inch, 1.05*inch, 2.84*inch])
track_table.setStyle(TableStyle([
    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor(OHIO_BLUE)),
    ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor(LGRAY)),
    ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ("TOPPADDING", (0, 0), (-1, -1), 5),
    ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
    ("LEFTPADDING", (0, 0), (-1, -1), 4),
    ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#fafbfc")]),
]))
story.append(track_table)
story.append(Paragraph(
    "2018 and 2020 house results aren't shown — the current district boundaries didn't exist on the pre-2022 "
    "maps, so there's no usable current-map comparison for those cycles (not evidence either way about candidate recruitment then).",
    small
))
story.append(Spacer(1, 8))

story.append(Paragraph("The honest trend: a real battleground, not a hidden lock", h2))
story.append(Paragraph(
    f"HD-64 has drifted Republican for over a decade — from roughly D+15 relative to the state in the "
    f"earliest data to essentially D+0 today (trend slope {tr['trend_slope']*100:+.2f} pts/year, "
    f"R²={tr['trend_r2']:.2f} — a strong, consistent drift, not noise). That means the honest framing isn't "
    f"“this is secretly a Democratic seat” — it's a genuine 50/50 battleground that needs real investment "
    f"to win, the same as any other tossup. What it is <i>not</i> is the double-digit Republican lock the 2024 "
    f"result makes it look like.",
    body
))

story.append(Paragraph("What actually happened in 2026", h2))
story.append(Paragraph(
    "No Democrat filed for the 2026 general election; Nick Santucci (R) is on the ballot as filed. An "
    "independent candidate had petitioned; per campaign sources (not independently verified in this analysis), "
    "that candidacy faces a signature-fraud challenge likely to end in disqualification — which doesn't change "
    "the bottom line either way, since an independent win still isn't a Democratic seat. "
    "The current statewide environment estimate is <b>50.1% D</b> (our polls + fundamentals blend), nearly 6 points "
    "better than 2024's actual 44.3% — which is exactly the kind of swing that would have made this district's "
    "structural 63% model win probability a live, winnable race. Instead, it's off the board before a single vote "
    "is cast, contributing roughly <b>0.6 expected seats</b> that the fundamentals said were in play.",
    body
))

story.append(HRFlowable(width="100%", thickness=0.75, color=colors.HexColor(LGRAY), spaceBefore=10, spaceAfter=6))
story.append(Paragraph(
    "Composite lean built from 9 statewide races (2018–2024), each district-vs-state comparison from the "
    "Ohio House Election Model's spatial crosswalk. Candidate effect = actual D share minus (statewide baseline + "
    "composite lean) for that cycle. 2026 win probability from src/simulate.py, blended-environment method "
    "(src/forecast_statewide.py); floored uncertainty per project convention. Redistricting reliability: "
    f"{ov['overlap_category_interim_final']} map-to-map, years reliable: {ov['years_reliable']}. "
    "This is a research-grade estimate, not a guarantee — see the full model documentation for caveats.",
    small
))

doc.build(story)
print(f"Wrote {OUT_PATH}")
