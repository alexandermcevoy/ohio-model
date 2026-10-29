"""
feed_rehearsal.py — replay a past Ohio election through the real SOS feed
format, one release at a time, to rehearse election night.

Truth comes from real county results in the block vote surfaces (2022: every
statewide race; legislative districts use the 2022 row-office average in
each district x county piece). The feed has no vote-type split per
candidate, so each county's absentee share and the absentee-vs-Election-Day
gap are synthetic: absentee D share = s + (1 - a) * gap, Election Day =
s - a * gap, which leaves the county total at s.

Release order mimics Ohio: absentee batches in the first steps (big urban
counties last), then Election Day precincts in increments, urban counties
slowest. Each step writes a copy of a real Ohio-ENR XML file with the counts
filled in, so the replay goes through enr_feed.parse_feed unchanged.

Not a calibration: the gap, absentee shares and order are scenarios, and the
model's judgment constants must not be tuned to make a replay look good.
"""

from __future__ import annotations

import copy
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from src.county_layer import PROCESSED
from src.enr_feed import _NAME_TO_FIPS, _NS, _LEGISLATIVE_PATTERNS, STATEWIDE_CONTESTS

URBAN = {"035", "049", "061"}  # Cuyahoga, Franklin, Hamilton
RACE_LABELS_2022 = {
    "governor": "gov", "us_senate": "uss", "attorney_general": "atg",
    "auditor": "aud", "secretary_of_state": "sos_off", "treasurer": "tre",
}


@dataclass
class Truth:
    county: pd.DataFrame      # race, fips, share, votes
    pieces: pd.DataFrame      # office, district, fips, share, votes
    absentee_share: pd.Series  # fips -> a
    gap: float


def build_truth(year: str = "2022", turnout_factor: float = 1.08, gap: float = 0.10, seed: int = 0,
                target_gov_share: float | None = None) -> Truth:
    """
    target_gov_share shifts every county and district share by one constant so
    the statewide governor result lands there (a close race instead of 2022's
    landslide); geography and race-to-race differences are kept.
    """
    from src.office import get_office

    rng = np.random.default_rng(seed)
    bv = pd.read_parquet(PROCESSED / f"block_votes_{year}.parquet")
    cm = pd.read_parquet(PROCESSED / "block_county_map.parquet").rename(columns={"county_fips": "fips"})
    m = bv.merge(cm, on="block_geoid")
    m["race"] = m["race"].str.rsplit("_", n=1).str[0]
    rows = []
    for key, label in RACE_LABELS_2022.items():
        g = m[m["race"] == label].groupby("fips")[["d_votes", "r_votes"]].sum()
        rows.append(pd.DataFrame({"race": key, "fips": g.index, "share": g["d_votes"] / g.sum(axis=1),
                                  "votes": g.sum(axis=1) * turnout_factor}))
    county = pd.concat(rows, ignore_index=True)

    ref = m[m["race"].isin(["atg", "aud", "sos_off", "tre"])]
    pieces = []
    for oname in ("house", "senate", "congress"):
        bmap = pd.read_parquet(get_office(oname).block_district_map_path)
        p = ref.merge(bmap, on="block_geoid").groupby(["district_num", "fips"])[["d_votes", "r_votes"]].sum() / 4
        p = p.reset_index().rename(columns={"district_num": "district"})
        pieces.append(pd.DataFrame({"office": oname, "district": p["district"], "fips": p["fips"],
                                    "share": p["d_votes"] / (p["d_votes"] + p["r_votes"]),
                                    "votes": (p["d_votes"] + p["r_votes"]) * turnout_factor}))
    pieces = pd.concat(pieces, ignore_index=True)
    if target_gov_share is not None:
        gv = county[county["race"] == "governor"]
        shift = target_gov_share - float((gv["share"] * gv["votes"]).sum() / gv["votes"].sum())
        county["share"] = (county["share"] + shift).clip(0.01, 0.99)
        pieces["share"] = (pieces["share"] + shift).clip(0.01, 0.99)
    a = pd.Series(rng.uniform(0.25, 0.45, size=88), index=sorted(cm["fips"].unique()))
    return Truth(county, pieces, a, gap)


def release_schedule(n_steps: int = 10, seed: int = 0) -> pd.DataFrame:
    """Per county and step: absentee released (0/1) and Election Day fraction counted."""
    rng = np.random.default_rng(seed + 1)
    fips = sorted(_NAME_TO_FIPS.values())
    out = []
    for f in fips:
        urban = f in URBAN
        abs_step = rng.integers(1, 3) if urban else rng.integers(0, 2)
        start = rng.integers(2, 4) if urban else rng.integers(1, 3)
        pace = rng.uniform(0.12, 0.2) if urban else rng.uniform(0.2, 0.45)
        for step in range(n_steps):
            ed = 0.0 if step < start else min(1.0, (step - start + 1) * pace)
            out.append({"fips": f, "step": step, "absentee_in": step >= abs_step, "ed_frac": ed})
    return pd.DataFrame(out)


def counts_at(truth: Truth, sched: pd.DataFrame, step: int) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """County statewide counts, legislative piece counts and county ballot totals at one step."""
    st = sched[sched["step"] == step].set_index("fips")
    a = truth.absentee_share
    g = truth.gap

    def counted(share, votes, fips):
        aa = a.reindex(fips).to_numpy()
        ab_in = st.loc[fips, "absentee_in"].to_numpy().astype(float)
        ed = st.loc[fips, "ed_frac"].to_numpy()
        s_abs = np.clip(share + (1 - aa) * g, 0, 1)
        s_ed = np.clip(share - aa * g, 0, 1)
        n_abs, n_ed = votes * aa * ab_in, votes * (1 - aa) * ed
        dem = n_abs * s_abs + n_ed * s_ed
        return np.round(dem), np.round(n_abs + n_ed - dem), n_abs, n_ed

    c = truth.county.copy()
    c["dem"], c["rep"], _, _ = counted(c["share"].to_numpy(), c["votes"].to_numpy(), c["fips"].to_numpy())
    p = truth.pieces.copy()
    p["dem"], p["rep"], _, _ = counted(p["share"].to_numpy(), p["votes"].to_numpy(), p["fips"].to_numpy())
    gov = truth.county[truth.county["race"] == "governor"].set_index("fips")
    _, _, n_abs, n_ed = counted(gov["share"].to_numpy(), gov["votes"].to_numpy(), gov.index.to_numpy())
    ballots = pd.DataFrame({"fips": gov.index, "ballots_total": np.round((n_abs + n_ed) * 1.03),
                            "ballots_absentee": np.round(n_abs * 1.03),
                            "precincts_frac": st.loc[gov.index, "ed_frac"].to_numpy(),
                            "complete": (st.loc[gov.index, "ed_frac"].to_numpy() >= 1)
                            & st.loc[gov.index, "absentee_in"].to_numpy()})
    return c, p, ballots


def write_step_xml(template: Path, out: Path, county_counts: pd.DataFrame, piece_counts: pd.DataFrame,
                   ballots: pd.DataFrame, generated: pd.Timestamp) -> Path:
    ET.register_namespace("", _NS.strip("{}"))
    ET.register_namespace("xsi", "http://www.w3.org/2001/XMLSchema-instance")
    tree = ET.parse(template)
    root = tree.getroot()
    root.set("GeneratedDate", generated.strftime("%Y-%m-%dT%H:%M:%SZ"))
    b = ballots.set_index("fips")
    unit_to_fips = {}
    for gp in root.iter(f"{_NS}GpUnit"):
        if gp.get("Type") != "County" or gp.get("TotalSubUnits") is None:
            continue
        f = _NAME_TO_FIPS[gp.get("Name").upper()]
        unit_to_fips[gp.get("ObjectId")] = f
        total = int(gp.get("TotalSubUnits"))
        gp.set("SubUnitsReported", str(int(round(b.loc[f, "precincts_frac"] * total))))
        for sc in gp.findall(f"{_NS}SummaryCounts"):
            if sc.get("Type") == "Total":
                sc.set("BallotsCast", str(int(b.loc[f, "ballots_total"])))
            elif sc.get("Type") == "Absentee":
                sc.set("BallotsCast", str(int(b.loc[f, "ballots_absentee"])))
        cs = gp.find(f"{_NS}CountStatus")
        if cs is not None:
            cs.set("Status", "Completed" if b.loc[f, "complete"] else "InProcess")

    party_kind, cand_kind = {}, {}
    for p in root.iter(f"{_NS}Party"):
        nm = p.find(f"{_NS}Name/{_NS}LanguageString")
        txt = (nm.text or "").lower() if nm is not None else ""
        party_kind[p.get("ObjectId")] = "D" if txt.startswith("democrat") else "R" if txt.startswith("republican") else None
    for cnd in root.iter(f"{_NS}Candidate"):
        pid = cnd.find(f"{_NS}PartyId")
        cand_kind[cnd.get("ObjectId")] = party_kind.get(pid.text if pid is not None else None)

    cc = county_counts.set_index(["race", "fips"])
    pc = piece_counts.set_index(["office", "district", "fips"])
    for contest in root.iter(f"{_NS}Contest"):
        name = (contest.get("Name") or "").strip()
        key = STATEWIDE_CONTESTS.get(name)
        leg = None
        if key is None:
            for office, pat in _LEGISLATIVE_PATTERNS:
                mm = pat.match(name)
                if mm:
                    leg = (office, int(mm.group(1)))
        if key is None and leg is None:
            continue
        for bs in contest.findall(f"{_NS}BallotSelection"):
            ids = [i.text for i in bs.findall(f"{_NS}CandidateId") if i.text and not i.text.startswith("_CO")]
            kind = cand_kind.get(ids[0]) if ids else None
            col = "dem" if kind == "D" else "rep" if kind == "R" else None
            for vc in bs.iter(f"{_NS}VoteCounts"):
                gid = vc.find(f"{_NS}GpUnitId")
                f = unit_to_fips.get(gid.text if gid is not None else None)
                if f is None or col is None:
                    vc.set("Count", "0")
                    continue
                if key is not None:
                    val = cc[col].get((key, f), 0)
                else:
                    val = pc[col].get((leg[0], leg[1], f), 0)
                vc.set("Count", str(int(val)))
    out.parent.mkdir(parents=True, exist_ok=True)
    tree.write(out, xml_declaration=True, encoding="utf-8")
    return out
