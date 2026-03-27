"""
query.py — Natural language query interface for the Ohio House Election Model.

Wraps the Claude API with a system prompt built from the live data files.
The system prompt includes:
  - Model methodology and conventions (from CLAUDE.md schema section)
  - The full targeting CSV (99 rows, primary analytical file)
  - Year baselines, redistricting summary, anomaly flags

Two entry points:
  ask(question)  — single-shot question, returns answer string
  chat()         — interactive multi-turn session in the terminal
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pandas as pd

# Load .env file from project root if present (before any anthropic import)
_ENV_FILE = Path(__file__).parent.parent / ".env"
if _ENV_FILE.exists():
    for _line in _ENV_FILE.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _, _v = _line.partition("=")
            os.environ[_k.strip()] = _v.strip()

# ---------------------------------------------------------------------------
# Paths (relative to project root, resolved at runtime)
# ---------------------------------------------------------------------------

_ROOT = Path(__file__).parent.parent

_TARGETING_CSV      = _ROOT / "reports/session3/oh_house_targeting.csv"
_COMPOSITE_CSV      = _ROOT / "reports/session2/oh_house_composite_lean.csv"
_REDISTRICTING_CSV  = _ROOT / "reports/redistricting_overlap.csv"
_ANOMALY_CSV        = _ROOT / "reports/anomaly_flags.csv"
_YEAR_BASELINES     = _ROOT / "data/processed/year_baselines.json"
_SCENARIO_CSV       = _ROOT / "reports/session3/oh_house_scenario_table.csv"
_DROP_ONE_CSV       = _ROOT / "data/processed/drop_one_sensitivity.csv"

_DEFAULT_MODEL = "claude-opus-4-6"


# ---------------------------------------------------------------------------
# System prompt construction
# ---------------------------------------------------------------------------

_SYSTEM_PREAMBLE = """\
You are an expert analyst for the Ohio House Election Model, a data-driven tool \
that identifies Democratic pickup targets in Ohio's 99 state house districts. \
Your job is to answer questions from Democratic campaign strategists, researchers, \
and operatives using the live model data provided below.

KEY CONVENTIONS:
- All lean values are expressed from the Democratic perspective. Positive = more Democratic than statewide average.
- "Lean" always means relative to the Ohio statewide average, not absolute vote share.
- "Composite lean" is a weighted average of 9 statewide races (2018–2024). It is the primary model metric.
- "Flip threshold" = 0.50 − composite_lean = the statewide D two-party share at which a district is expected to flip.
- "Realistic target" = R-held district where flip_threshold <= 52% — achievable in a strong D year. Excludes lean_r (53-58%) and deeper long-shots. This is the primary pickup filter; use realistic_target=True, not pickup_opportunity, for the actionable ladder.
- The model is FUNDAMENTALS-ONLY. No incumbency adjustment is applied to flip thresholds or scenario seat counts.
- Ohio currently has 65 Republican seats and 34 Democratic seats (99 total).
- Realistic 2026 goals: hold 34+ (supermajority-proof), push toward 40+ (veto sustainability).
- 2026 open seats (no 2024 incumbent running): Districts 31, 35, 39, 44, 52, 57, 81 (R-held) and 7, 18 (D-held).

DISTRICT TIERS (composite lean thresholds):
  safe_d (>+15 pts): 18 districts
  likely_d (+8 to +15): 11 districts
  lean_d (+3 to +8): 10 districts
  tossup (±3 pts): 10 districts — all R-held
  lean_r (−3 to −8 pts): 18 districts — all R-held
  likely_r (−8 to −15 pts): 14 districts
  safe_r (<−15 pts): 18 districts

REDISTRICTING NOTE:
  Ohio used 3 maps: pre-2022 (old), 2022 interim, 2024 final.
  - 71 districts have zero precinct overlap between old and interim maps.
  - 13 districts have only 2024 as a reliable house election year.
  - 73 districts have 2022+2024 as reliable years.
  - 13 districts have all 4 years (2018–2024) as reliable.
  - The composite lean is UNAFFECTED by redistricting (built from statewide races on consistent geometry).
  - Only house race history is affected.

STATEWIDE YEAR BASELINES (two-party D share in reference race):
  2018 Governor: 48.1%
  2020 President: 45.9%
  2022 Governor: 37.5%
  2024 President: 44.3%

When answering:
- Be direct and specific. Lead with the answer, not the reasoning.
- Cite district numbers, lean values, and thresholds from the data.
- Flag data quality issues (redistricting artifacts, insufficient_data targeting mode) when relevant.
- When a question is about 2026 open seats, note that the ~6-point incumbency premium disappears for those seats.
- If you cannot answer from the data provided, say so clearly.
"""


def _load_targeting_text() -> str:
    """Load targeting CSV as formatted text for the system prompt."""
    df = pd.read_csv(_TARGETING_CSV)
    # Select the most analytically useful columns for the context window
    cols = [
        "district", "composite_lean", "tier", "current_holder",
        "flip_threshold", "realistic_target", "open_seat_2026", "incumbent_status_2026",
        "target_mode", "n_contested", "swing_sd", "turnout_elasticity",
        "pickup_opportunity", "defensive_priority",
        "contested_2024", "margin_2024", "candidate_effect_2024",
        "gov_2018_lean", "pre_2020_lean", "gov_2022_lean",
        "pre_2024_lean", "composite_sensitivity", "most_sensitive_race",
        "dem_candidate_2024", "rep_candidate_2024",
        "open_seat_reason", "current_incumbent_name",
    ]
    cols = [c for c in cols if c in df.columns]
    subset = df[cols].copy()

    # Round floats for readability
    float_cols = subset.select_dtypes("float64").columns
    subset[float_cols] = subset[float_cols].round(4)

    return subset.to_csv(index=False)


def _load_scenario_text() -> str:
    df = pd.read_csv(_SCENARIO_CSV)
    return df.to_csv(index=False)


def _load_redistricting_text() -> str:
    df = pd.read_csv(_REDISTRICTING_CSV)
    cols = ["district", "overlap_category", "jaccard_similarity",
            "overlap_category_interim_final", "jaccard_interim_final", "years_reliable"]
    cols = [c for c in cols if c in df.columns]
    return df[cols].to_csv(index=False)


def _load_anomaly_text() -> str:
    if not _ANOMALY_CSV.exists():
        return "(no anomaly flags file found)"
    df = pd.read_csv(_ANOMALY_CSV)
    return df.to_csv(index=False)


def _load_year_baselines_text() -> str:
    if not _YEAR_BASELINES.exists():
        return "(year baselines not computed yet)"
    with open(_YEAR_BASELINES) as f:
        data = json.load(f)
    return json.dumps(data, indent=2)


def build_system_prompt() -> str:
    """Assemble the full system prompt from preamble + live data."""
    sections = [
        _SYSTEM_PREAMBLE,
        "---\n## TARGETING DATA (99 districts)\n\n```csv\n" + _load_targeting_text() + "```",
        "---\n## SCENARIO TABLE (seats won at each statewide D share)\n\n```csv\n" + _load_scenario_text() + "```",
        "---\n## REDISTRICTING OVERLAP (per-district map reliability)\n\n```csv\n" + _load_redistricting_text() + "```",
        "---\n## ANOMALY FLAGS (outlier house results)\n\n```csv\n" + _load_anomaly_text() + "```",
        "---\n## YEAR BASELINES\n\n```json\n" + _load_year_baselines_text() + "\n```",
    ]
    return "\n\n".join(sections)


# ---------------------------------------------------------------------------
# API calls
# ---------------------------------------------------------------------------

def ask(
    question: str,
    model: str = _DEFAULT_MODEL,
    system_prompt: str | None = None,
) -> str:
    """
    Single-shot question. Returns the answer as a string.

    Parameters
    ----------
    question : str
        The question to ask.
    model : str
        Claude model ID. Defaults to claude-opus-4-6.
    system_prompt : str | None
        Override the auto-built system prompt (useful for testing).
    """
    import anthropic

    client = anthropic.Anthropic()
    prompt = system_prompt if system_prompt is not None else build_system_prompt()

    message = client.messages.create(
        model=model,
        max_tokens=2048,
        system=prompt,
        messages=[{"role": "user", "content": question}],
    )
    return message.content[0].text


def chat(
    model: str = _DEFAULT_MODEL,
    system_prompt: str | None = None,
) -> None:
    """
    Interactive multi-turn chat session in the terminal.

    Type 'quit', 'exit', or Ctrl-C to end the session.
    """
    import anthropic

    client = anthropic.Anthropic()
    prompt = system_prompt if system_prompt is not None else build_system_prompt()

    print("Ohio House Election Model — Query Interface")
    print(f"Model: {model}")
    print("Type your question, or 'quit' to exit.\n")
    print("=" * 60)

    history: list[dict] = []

    while True:
        try:
            user_input = input("\nYou: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nEnding session.")
            break

        if not user_input:
            continue
        if user_input.lower() in {"quit", "exit", "q"}:
            print("Ending session.")
            break

        history.append({"role": "user", "content": user_input})

        response = client.messages.create(
            model=model,
            max_tokens=2048,
            system=prompt,
            messages=history,
        )

        answer = response.content[0].text
        history.append({"role": "assistant", "content": answer})

        print(f"\nAssistant: {answer}")
