"""
ballot_overrides.py — manually curated 2026 general-election ballot facts the
fundamentals-only forecast has no way to know about on its own.

The composite-lean / win-probability model in this repo projects from
historical precinct results, demographics, and polling — it has zero
visibility into who actually filed to run in 2026. That's fine for the
structural question ("how would this district behave under uniform
conditions") but wrong for "who can win in November" the moment a seat goes
uncontested or a candidate is disqualified.

This is a small, dated, sourced override table — same pattern as
OPEN_SEATS_2026 in src/classify.py. It does NOT touch the underlying model
(composite_lean, sigma, the fundamentals-based win_prob are all preserved
and still computed normally) — it's a downstream overlay applied only in
cli.py's `_run_office_forecast`, producing an `effective_win_prob` alongside
the untouched raw one. Update this dict as filing deadlines pass, ballot
challenges resolve, and candidate lists firm up — it will go stale.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BallotOverride:
    office: str  # "house" | "senate" | "congress"
    district: int
    status: str  # "uncontested_no_d" | "uncontested_no_r" | "candidate_at_risk"
    effective_dem_prob: float  # replaces win_prob in the effective forecast
    note: str
    source: str
    verified_date: str  # ISO date this entry was last checked


BALLOT_OVERRIDES_2026: dict[tuple[str, int], BallotOverride] = {
    ("house", 64): BallotOverride(
        office="house",
        district=64,
        status="uncontested_no_d",
        effective_dem_prob=0.0,
        note=(
            "No Democrat filed for the 2026 general election; incumbent Nick "
            "Santucci (R) is on the ballot as filed. An independent candidate "
            "had petitioned; per the analyst (not independently verified by "
            "this pipeline), that candidacy faces a signature-fraud challenge "
            "likely to end in disqualification. Doesn't change the bottom "
            "line either way — an independent win would not be a Democratic "
            "seat, so this district is not a realistic 2026 Democratic "
            "opportunity regardless of that outcome, and the model's "
            "fundamentals-only win probability below overstates it."
        ),
        source="Ballotpedia, Ohio House of Representatives District 64 (accessed 2026-09-11); independent-candidate detail per analyst, unverified",
        verified_date="2026-09-11",
    ),
}


def get_override(office: str, district: int) -> BallotOverride | None:
    """Look up a ballot override for one district, if one exists."""
    return BALLOT_OVERRIDES_2026.get((office, int(district)))


def overrides_for_office(office: str) -> dict[int, BallotOverride]:
    """All overrides for one office, keyed by district number."""
    return {d: ov for (o, d), ov in BALLOT_OVERRIDES_2026.items() if o == office}
