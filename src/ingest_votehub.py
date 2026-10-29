"""
ingest_votehub.py — refresh the poll files from VoteHub's open poll API.

VoteHub (https://api.votehub.com/polls, free, no key) publishes poll-level
toplines for governor, US Senate, generic ballot and approval questions,
including the NYT/Siena polls. This module MERGES its polls into the existing
files rather than replacing them:

- data/polls/governor.csv, senate.csv (538 long schema): Ohio 2026 general
  polls are appended in that schema, one row per answer.
- data/polls/generic_ballot.csv (Silver Bulletin one-row-per-poll schema):
  national generic-ballot polls are appended in that schema.

A VoteHub poll is skipped when the file already has a poll from the same
pollster (name-token match) ending within DUPLICATE_WINDOW_DAYS days, so
re-running is idempotent and the two sources never double-count a poll.
Every row carries a `feed` column ("538", "silver_bulletin" or "votehub");
the 538 files already use `source` for something else.

VoteHub carries no pollster grades. That loses nothing for Ohio (every OH
2026 poll in the 538 files already had numeric_grade empty), and the generic
ballot file never used grades (quality_weight's 1.5 default applies to all).

Each run saves the raw API response under data/polls/votehub/ and copies the
files it changes to data/polls/archive/ first.
"""

from __future__ import annotations

import hashlib
import json
import re
import shutil
import urllib.request
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from src.forecast_statewide import OHIO_2026_RACES
from src.ingest_polls import POLLS

VOTEHUB_URL = "https://api.votehub.com/polls"
DUPLICATE_WINDOW_DAYS = 2
SAME_TOPLINE_WINDOW_DAYS = 10  # same pollster + same toplines this close = same poll, dated differently

# Words that don't identify a pollster on their own ("Marist University" vs
# "Marist College" should still match on "marist").
_GENERIC_TOKENS = {
    "the", "and", "of", "university", "college", "research", "poll", "polls", "polling",
    "group", "strategies", "strategy", "insights", "associates", "company", "co",
    "inc", "llc", "center", "institute", "network", "public", "policy", "opinion",
    "democracy", "state", "data", "analytics", "partners", "survey", "surveys",
}

# VoteHub poll_type -> (538 office_type, OHIO_2026_RACES key, file)
RACE_FEEDS: dict[str, tuple[str, str, str]] = {
    "governor": ("Governor", "governor", "governor.csv"),
    "us-senator": ("U.S. Senate", "us_senate", "senate.csv"),
}


def fetch_votehub(save_dir: Path | None = None, timeout: int = 60) -> list[dict]:
    """Download every poll VoteHub publishes and save the raw response."""
    with urllib.request.urlopen(VOTEHUB_URL, timeout=timeout) as resp:
        polls = json.load(resp)
    if not isinstance(polls, list) or not polls:
        raise ValueError(f"Unexpected VoteHub response: {type(polls).__name__} of length {len(polls) if hasattr(polls, '__len__') else '?'}")
    save_dir = save_dir or POLLS / "votehub"
    save_dir.mkdir(parents=True, exist_ok=True)
    stamp = pd.Timestamp.now().strftime("%Y-%m-%d_%H%M")
    (save_dir / f"votehub_{stamp}.json").write_text(json.dumps(polls))
    return polls


def pollster_tokens(name: str) -> set[str]:
    tokens = set(re.findall(r"[a-z0-9]+", str(name).lower().replace("&", " ")))
    distinctive = {t for t in tokens if t not in _GENERIC_TOKENS and len(t) > 1}
    # Names made only of generic words ("Public Policy Polling") match on the whole name.
    return distinctive or {" ".join(sorted(tokens))}


def _is_duplicate(
    pollster: str, end: pd.Timestamp, existing: pd.DataFrame,
    dem: float | None = None, rep: float | None = None,
) -> bool:
    """
    existing: one row per poll with columns pollster, end_date (and dem, rep
    when toplines are known). A duplicate is the same pollster ending within
    DUPLICATE_WINDOW_DAYS, or within SAME_TOPLINE_WINDOW_DAYS with toplines
    matching to half a point (aggregators sometimes date a poll by release
    rather than fieldwork).
    """
    tokens = pollster_tokens(pollster)
    gap = (existing["end_date"] - end).abs()
    same = existing[existing["pollster"].map(lambda p: bool(tokens & pollster_tokens(p)))]
    if (gap[same.index] <= pd.Timedelta(days=DUPLICATE_WINDOW_DAYS)).any():
        return True
    if dem is None or not {"dem", "rep"} <= set(existing.columns):
        return False
    close = same[gap[same.index] <= pd.Timedelta(days=SAME_TOPLINE_WINDOW_DAYS)]
    return bool(((close["dem"] - dem).abs().le(0.5) & (close["rep"] - rep).abs().le(0.5)).any())


def _int_id(*parts) -> int:
    """Stable integer id from VoteHub fields, in a range the 538/SB ids don't use."""
    digest = hashlib.sha1("|".join(map(str, parts)).encode()).hexdigest()
    return 10**11 + int(digest[:9], 16)


def _mdyy(d: pd.Timestamp) -> str:
    return f"{d.month}/{d.day}/{d:%y}"


def _mdyyyy(d: pd.Timestamp) -> str:
    return f"{d.month}/{d.day}/{d.year}"


_POPULATION_RANK = {"lv": 0, "rv": 1, "a": 2, "v": 3}


def _one_population(polls: list[dict]) -> list[dict]:
    """
    VoteHub sometimes lists the same poll once per population (LV and RV).
    Keep one: likely voters, else registered, else adults, as aggregators do,
    so a poll isn't counted twice.
    """
    best: dict[tuple, int] = {}
    for p in polls:
        k = (p["pollster"], p["start_date"], p["end_date"])
        rank = _POPULATION_RANK.get(str(p.get("population")).lower(), 9)
        best[k] = min(best.get(k, 9), rank)
    return [
        p for p in polls
        if _POPULATION_RANK.get(str(p.get("population")).lower(), 9)
        == best[(p["pollster"], p["start_date"], p["end_date"])]
    ]


def _poll_key(p: dict) -> tuple:
    """VoteHub lists each question of a poll separately; this groups them into one poll."""
    return (p["pollster"], p["start_date"], p["end_date"], p.get("sample_size"), p.get("population"))


@dataclass
class MergeReport:
    file: str
    added_polls: int
    duplicate_polls: int
    added: list[str]   # "pollster end_date" for each added poll


def merge_race_polls(polls: list[dict], poll_type: str, existing: pd.DataFrame) -> tuple[pd.DataFrame, MergeReport]:
    """Append VoteHub Ohio 2026 polls of one type to a 538-schema long frame."""
    office_type, race_key, fname = RACE_FEEDS[poll_type]
    dem_name, rep_name = OHIO_2026_RACES[race_key]

    ex = existing.copy()
    if "feed" not in ex.columns:
        ex["feed"] = "538"
    ex_end = pd.to_datetime(ex["end_date"], format="%m/%d/%y", errors="coerce")
    oh = ex[(ex["state"] == "OH") & (ex["cycle"] == 2026) & (ex["office_type"] == office_type)]
    ex_polls = pd.DataFrame({"pollster": oh["pollster"], "end_date": ex_end[oh.index]}).drop_duplicates()

    vh = _one_population([p for p in polls if p["poll_type"] == poll_type and p.get("subject") == "2026 Ohio"])
    groups: dict[tuple, list[dict]] = {}
    for p in vh:
        groups.setdefault(_poll_key(p), []).append(p)

    rows, added, dup = [], [], 0
    for key, questions in groups.items():
        pollster, start, end = key[0], pd.Timestamp(key[1]), pd.Timestamp(key[2])
        if _is_duplicate(pollster, end, ex_polls):
            dup += 1
            continue
        poll_id = _int_id("votehub", *key)
        q0 = questions[0]
        for q in questions:
            names = {a["choice"] for a in q["answers"]}
            if not {dem_name, rep_name} <= names:
                continue
            for a in q["answers"]:
                party = "DEM" if a["choice"] == dem_name else "REP" if a["choice"] == rep_name else np.nan
                rows.append({
                    "poll_id": poll_id,
                    "question_id": _int_id("votehub-q", q["id"]),
                    "pollster": pollster,
                    "display_name": pollster,
                    "sponsors": ", ".join(q0.get("sponsors") or []) or np.nan,
                    "sample_size": q.get("sample_size"),
                    "population": q.get("population"),
                    "state": "OH",
                    "cycle": 2026,
                    "office_type": office_type,
                    "stage": "general",
                    "election_date": "11/3/26",
                    "start_date": _mdyy(start),
                    "end_date": _mdyy(end),
                    "party": party,
                    "answer": a["choice"].split()[-1],
                    "candidate_name": a["choice"],
                    "pct": a["pct"],
                    "partisan": q.get("partisan") or np.nan,
                    "internal": bool(q.get("internal")),
                    "url": q.get("url"),
                    "feed": "votehub",
                })
        added.append(f"{pollster} {end.date()}")
    new = pd.DataFrame(rows)
    merged = pd.concat([ex, new], ignore_index=True) if len(new) else ex
    return merged, MergeReport(fname, len(added), dup, added)


def merge_generic_ballot(polls: list[dict], existing: pd.DataFrame) -> tuple[pd.DataFrame, MergeReport]:
    """Append VoteHub national generic-ballot polls in the Silver Bulletin schema."""
    ex = existing.copy()
    if "feed" not in ex.columns:
        ex["feed"] = "silver_bulletin"
    ex_polls = pd.DataFrame({
        "pollster": ex["pollster"],
        "end_date": pd.to_datetime(ex["enddate"], format="%m/%d/%Y", errors="coerce"),
        "dem": ex["dem"],
        "rep": ex["rep"],
    }).drop_duplicates()

    rows, added, dup = [], [], 0
    seen: set[tuple] = set()
    for p in _one_population([q for q in polls if q["poll_type"] == "generic-ballot"]):
        key = _poll_key(p)
        if key in seen:
            continue
        seen.add(key)
        end = pd.Timestamp(p["end_date"])
        ans = {a["choice"]: a["pct"] for a in p["answers"]}
        if "Dem" not in ans or "Rep" not in ans:
            continue
        if _is_duplicate(p["pollster"], end, ex_polls, ans["Dem"], ans["Rep"]):
            dup += 1
            continue
        rows.append({
            "subgroup": "All polls",
            "pollster": p["pollster"],
            "sponsors": ", ".join(p.get("sponsors") or []) or np.nan,
            "startdate": _mdyyyy(pd.Timestamp(p["start_date"])),
            "enddate": _mdyyyy(end),
            "samplesize": p.get("sample_size"),
            "population": str(p.get("population") or "").upper() or np.nan,
            "dem": ans["Dem"],
            "rep": ans["Rep"],
            "net": ans["Dem"] - ans["Rep"],
            "url": p.get("url"),
            "createddate": _mdyyyy(pd.Timestamp(p["created_at"])) if p.get("created_at") else np.nan,
            "partisan": p.get("partisan") or np.nan,
            "poll_id": _int_id("votehub", *key),
            "question_id": _int_id("votehub-q", p["id"]),
            "feed": "votehub",
        })
        added.append(f"{p['pollster']} {end.date()}")
    new = pd.DataFrame(rows)
    merged = pd.concat([ex, new], ignore_index=True) if len(new) else ex
    return merged, MergeReport("generic_ballot.csv", len(added), dup, added)


def refresh_polls(polls: list[dict] | None = None, write: bool = True) -> list[MergeReport]:
    """Fetch (unless given) and merge VoteHub polls into the three poll files."""
    polls = polls if polls is not None else fetch_votehub()
    stamp = pd.Timestamp.now().strftime("%Y-%m-%d_%H%M")  # never overwrite an earlier archive
    reports = []
    outputs: dict[Path, pd.DataFrame] = {}
    for poll_type, (_, _, fname) in RACE_FEEDS.items():
        path = POLLS / fname
        merged, rep = merge_race_polls(polls, poll_type, pd.read_csv(path, low_memory=False))
        outputs[path] = merged
        reports.append(rep)
    path = POLLS / "generic_ballot.csv"
    merged, rep = merge_generic_ballot(polls, pd.read_csv(path, low_memory=False))
    outputs[path] = merged
    reports.append(rep)

    if write:
        archive = POLLS / "archive"
        archive.mkdir(exist_ok=True)
        for (path, df), rep in zip(outputs.items(), reports):
            if rep.added_polls == 0:
                continue
            shutil.copy2(path, archive / f"{path.stem}_pre{stamp}.csv")
            df.to_csv(path, index=False)
    return reports
