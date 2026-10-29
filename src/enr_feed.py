"""
enr_feed.py — Ohio Secretary of State election-night results feed.

The SOS live results site (https://liveresults.ohiosos.gov) publishes a NIST
1500-100 XML file ("Ohio-ENR"), refreshed about every three minutes on
election night. Documentation: /Content/PDF/P1622Documentation.pdf on that
site. For every county reporting unit it carries precincts reported / total,
registered voters, ballots cast (total and absentee) and a count status; for
every contest it carries each candidate's vote count per county. That covers
all statewide races, US Senate, US House, Ohio Senate and Ohio House.

What it does NOT carry: a per-candidate split by vote type (absentee vs
Election Day). Only the county-level absentee ballots-cast count, which
src/election_night.py uses to estimate the absentee/Election-Day gap.

Reading rules (from inspecting the 2026 file):
- Counties: the 88 GpUnits with Type="County" AND TotalSubUnits. The other
  88 Type="County" units ("Adams County", IsElectoralDistrict="true") are
  contest scopes for county offices, not reporting units.
- Vote counts appear at several aggregation levels in one contest (state or
  district total plus one per county). Only counts whose GpUnitId is one of
  the 88 county reporting units are used; totals are re-summed from those.
- Parties come from Party/Name text (Abbreviation is "None"). On ticket
  races (Governor/Lt. Governor) a BallotSelection lists the running mate as
  a second CandidateId prefixed "_CO"; only the top candidate is used.
- Two-party only: the Democratic and Republican candidate per contest.
  Write-ins, minor parties and independents are dropped.
- GeneratedDate is written on a 12-hour clock with no AM/PM (a file
  downloaded at 14:15 UTC says "02:15:11Z"). Taken at face value, every file
  after 01:00 UTC on election night (9 pm ET) would look older than the
  evening's earlier files. parse_feed resolves it to whichever of the two
  readings is closest to when the file was downloaded (from its saved name).
"""

from __future__ import annotations

import re
import shutil
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from src.join_sos_vest import OHIO_FIPS_TO_COUNTY

REPO_ROOT = Path(__file__).resolve().parent.parent
FEED_DIR = REPO_ROOT / "data" / "raw" / "enr"
FEED_URL = "https://liveresults.ohiosos.gov/Api/v1/download?filename=VSSC1622XmlFileBlob"

_NS = "{http://grouper.ieee.org/groups/1622/groups/2/V1/1622_2-election_results_reporting.xsd}"
_NAME_TO_FIPS = {name: fips for fips, name in OHIO_FIPS_TO_COUNTY.items()}

# Feed contest name -> (office, district or race key)
STATEWIDE_CONTESTS: dict[str, str] = {
    "Governor and Lieutenant Governor": "governor",
    "U.S. Senator": "us_senate",
    "Attorney General": "attorney_general",
    "Auditor of State": "auditor",
    "Secretary of State": "secretary_of_state",
    "Treasurer of State": "treasurer",
}
_LEGISLATIVE_PATTERNS: tuple[tuple[str, re.Pattern], ...] = (
    ("house", re.compile(r"^State Representative - District (\d+)$")),
    ("senate", re.compile(r"^State Senator - District (\d+)$")),
    ("congress", re.compile(r"^Representative to Congress - District (\d+)$")),
)


@dataclass
class FeedSnapshot:
    generated: pd.Timestamp
    is_test: bool
    notes: str
    status: str
    counties: pd.DataFrame       # fips, name, precincts_reported, precincts_total, registered, ballots_total, ballots_absentee, count_status
    statewide: pd.DataFrame      # race, fips, dem_votes, rep_votes
    legislative: pd.DataFrame    # office, district, fips, dem_votes, rep_votes
    candidates: pd.DataFrame     # office, district/race, party, name
    source: Path | None = None
    warnings: list[str] = field(default_factory=list)


def _download_time(path: Path) -> pd.Timestamp | None:
    """UTC download time from a saved file's name (enr_YYYYmmdd_HHMMSS.xml, local time)."""
    m = re.match(r"enr_(\d{8}_\d{6})", path.name)
    if not m:
        return None
    return pd.to_datetime(m.group(1), format="%Y%m%d_%H%M%S").tz_localize("America/New_York").tz_convert("UTC")


def _resolve_generated(raw: pd.Timestamp, downloaded: pd.Timestamp | None) -> pd.Timestamp:
    if raw.tzinfo is None:
        raw = raw.tz_localize("UTC")
    if downloaded is None:
        return raw
    candidates = [raw, raw + pd.Timedelta(hours=12), raw - pd.Timedelta(hours=12)]
    return min(candidates, key=lambda t: abs(t - downloaded))


def _text(el, path: str) -> str | None:
    node = el.find(path)
    return None if node is None else (node.text or "").strip()


def _party_kind(name: str | None) -> str | None:
    n = (name or "").lower()
    if n.startswith("democrat"):
        return "D"
    if n.startswith("republican"):
        return "R"
    return None


def parse_feed(path: Path | str) -> FeedSnapshot:
    """Parse one Ohio-ENR XML file into tidy county-level tables."""
    path = Path(path)
    root = ET.parse(path).getroot()
    warnings: list[str] = []

    generated = _resolve_generated(pd.Timestamp(root.get("GeneratedDate")), _download_time(path))
    is_test = str(root.get("IsTest", "false")).lower() == "true"
    notes = " ".join((n.text or "").strip() for n in root.iter(f"{_NS}Notes"))
    if "mock" in notes.lower():
        is_test = True

    # --- county reporting units ---
    counties, unit_to_fips = [], {}
    for g in root.iter(f"{_NS}GpUnit"):
        if g.get("Type") != "County" or g.get("TotalSubUnits") is None:
            continue
        name = g.get("Name", "")
        fips = _NAME_TO_FIPS.get(name.upper())
        if fips is None:
            raise KeyError(f"Feed county {name!r} not in OHIO_FIPS_TO_COUNTY")
        unit_to_fips[g.get("ObjectId")] = fips
        summary = {s.get("Type"): s.get("BallotsCast") for s in g.findall(f"{_NS}SummaryCounts")}
        status = g.find(f"{_NS}CountStatus")
        counties.append({
            "fips": fips,
            "name": name,
            "precincts_reported": int(g.get("SubUnitsReported") or 0),
            "precincts_total": int(g.get("TotalSubUnits") or 0),
            "registered": int(g.get("VotersRegistered") or 0),
            "ballots_total": int(summary.get("Total") or 0),
            "ballots_absentee": int(summary.get("Absentee") or 0),
            "count_status": None if status is None else status.get("Status"),
        })
    counties_df = pd.DataFrame(counties).sort_values("fips").reset_index(drop=True)
    if len(counties_df) != 88:
        raise ValueError(f"Expected 88 county reporting units in the feed, found {len(counties_df)}")

    # --- candidates and parties ---
    party_kind = {}
    for p in root.iter(f"{_NS}Party"):
        party_kind[p.get("ObjectId")] = _party_kind(_text(p, f"{_NS}Name/{_NS}LanguageString"))
    cand = {}
    for c in root.iter(f"{_NS}Candidate"):
        cand[c.get("ObjectId")] = (
            _text(c, f"{_NS}BallotName/{_NS}LanguageString"),
            party_kind.get(_text(c, f"{_NS}PartyId")),
        )

    # --- contests ---
    statewide_rows, leg_rows, cand_rows = [], [], []
    for contest in root.iter(f"{_NS}Contest"):
        cname = (contest.get("Name") or "").strip()
        key = None
        if cname in STATEWIDE_CONTESTS:
            office, key = "statewide", STATEWIDE_CONTESTS[cname]
        else:
            for office_name, pat in _LEGISLATIVE_PATTERNS:
                m = pat.match(cname)
                if m:
                    office, key = office_name, int(m.group(1))
                    break
        if key is None:
            continue

        by_party: dict[str, dict[str, int]] = {}
        names: dict[str, str] = {}
        for bs in contest.findall(f"{_NS}BallotSelection"):
            if bs.get("IsWriteIn") == "true":
                continue
            ids = [i.text for i in bs.findall(f"{_NS}CandidateId") if i.text and not i.text.startswith("_CO")]
            if not ids or ids[0] not in cand:
                continue
            name, kind = cand[ids[0]]
            if kind is None:
                continue
            if kind in by_party:
                warnings.append(f"{cname}: more than one {kind} candidate; keeping the first")
                continue
            names[kind] = name
            counts = {}
            for vc in bs.iter(f"{_NS}VoteCounts"):
                fips = unit_to_fips.get(_text(vc, f"{_NS}GpUnitId"))
                if fips is not None and vc.get("Type") == "Total":
                    counts[fips] = counts.get(fips, 0) + int(vc.get("Count") or 0)
            by_party[kind] = counts
        for kind, name in names.items():
            cand_rows.append({"office": office, "key": key, "party": kind, "name": name})
        if "D" not in by_party or "R" not in by_party:
            # Uncontested in the two-party sense; keep whichever side exists.
            if not by_party:
                continue
        fips_all = set().union(*[set(v) for v in by_party.values()])
        for fips in sorted(fips_all):
            row = {
                "fips": fips,
                "dem_votes": by_party.get("D", {}).get(fips, 0),
                "rep_votes": by_party.get("R", {}).get(fips, 0),
                "has_dem": "D" in by_party,
                "has_rep": "R" in by_party,
            }
            if office == "statewide":
                statewide_rows.append({"race": key, **row})
            else:
                leg_rows.append({"office": office, "district": key, **row})

    snap = FeedSnapshot(
        generated=generated,
        is_test=is_test,
        notes=notes,
        status=root.get("Status", ""),
        counties=counties_df,
        statewide=pd.DataFrame(statewide_rows),
        legislative=pd.DataFrame(leg_rows),
        candidates=pd.DataFrame(cand_rows),
        source=path,
        warnings=warnings,
    )
    missing = set(STATEWIDE_CONTESTS.values()) - set(snap.statewide["race"]) if len(snap.statewide) else set(STATEWIDE_CONTESTS.values())
    if missing:
        snap.warnings.append(f"Statewide contests missing from feed: {sorted(missing)}")
    return snap


def ballot_only(snap: FeedSnapshot) -> FeedSnapshot:
    """
    The snapshot's candidate list with every count removed: who is on the
    ballot (and which races have only one major party) without any returns,
    for the pre-election projection.
    """
    counties = snap.counties.copy()
    counties[["precincts_reported", "ballots_total", "ballots_absentee"]] = 0
    counties["count_status"] = "InProcess"
    return FeedSnapshot(
        generated=snap.generated, is_test=False, notes="", status="ballot_only", counties=counties,
        statewide=snap.statewide.iloc[0:0], legislative=snap.legislative.iloc[0:0],
        candidates=snap.candidates, source=snap.source, warnings=[],
    )


def self_check(snap: FeedSnapshot) -> str:
    """One line showing whether the fields the model depends on are populated."""
    c = snap.counties
    statuses = ", ".join(sorted({str(s) for s in c["count_status"].dropna()})) or "none"
    with_votes = int((snap.statewide.groupby("fips")[["dem_votes", "rep_votes"]].sum().sum(axis=1) > 0).sum()) \
        if len(snap.statewide) else 0
    return (f"Feed self-check: {int((c['ballots_total'] > 0).sum())}/88 counties with ballots, "
            f"{int((c['ballots_absentee'] > 0).sum())} with absentee counts, "
            f"{int((c['precincts_reported'] > 0).sum())} with precincts in, {with_votes} with governor votes; "
            f"count statuses: {statuses}")


def fetch_feed(dest_dir: Path = FEED_DIR, timeout: int = 60) -> Path:
    """
    Download the current feed to a timestamped file and return its path.
    Raises on network errors; callers fall back to latest_feed_file().
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    req = urllib.request.Request(FEED_URL, headers={"User-Agent": "ohio-house-model/election-night"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read()
    if not body.lstrip().startswith(b"<?xml"):
        raise ValueError("Feed download did not return XML")
    stamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    tmp = dest_dir / f".enr_{stamp}.xml.tmp"
    tmp.write_bytes(body)
    path = dest_dir / f"enr_{stamp}.xml"
    shutil.move(tmp, path)
    return path


def latest_feed_file(dest_dir: Path = FEED_DIR) -> Path | None:
    files = sorted(dest_dir.glob("enr_*.xml"))
    return files[-1] if files else None


def load_latest(fetch: bool = True, dest_dir: Path = FEED_DIR) -> tuple[FeedSnapshot | None, list[str]]:
    """
    Fetch a new file (if asked) and return the newest valid snapshot.
    A failed fetch, an unparseable file, or a file older than the previous
    good one never replaces the previous good snapshot.
    """
    messages: list[str] = []
    previous = latest_feed_file(dest_dir)
    prev_snap = None
    if previous is not None:
        try:
            prev_snap = parse_feed(previous)
        except Exception as exc:  # noqa: BLE001 — a corrupt cached file shouldn't block a fresh fetch
            messages.append(f"Previous feed file {previous.name} unreadable: {exc}")
    if not fetch:
        return prev_snap, messages
    try:
        new_path = fetch_feed(dest_dir)
        snap = parse_feed(new_path)
    except Exception as exc:  # noqa: BLE001 — network/format failures fall back to the last good file
        messages.append(f"Feed fetch failed ({exc}); using the last good file"
                        + (f" {previous.name}" if previous else " (none)"))
        return prev_snap, messages
    if prev_snap is not None and snap.generated < prev_snap.generated:
        messages.append(f"New feed file is older ({snap.generated}) than the previous one ({prev_snap.generated}); ignored")
        new_path.unlink(missing_ok=True)
        return prev_snap, messages
    return snap, messages
