"""
county_layer.py — county-level inputs for the election-night model.

The Ohio SOS feed reports every race county by county (src/enr_feed.py). To
read a partial count the way the user described ("Acton is ahead, but is
that Franklin's whole vote or just its first absentee batch?") the model
needs, for each county:

- lean: the county's composite partisan lean, built exactly like the
  district composite (backbone.build_composite_from_blocks with
  DEFAULT_WEIGHTS on 2018-2024 block surfaces), so counties and districts
  share one scale. Centered so the turnout-weighted average is 0, which keeps
  the statewide projection with no votes counted equal to the published
  forecast.
- base_votes: 2022 governor two-party votes, the turnout baseline.
- sigma_u: how far a county strays from its composite lean in a given cycle
  (shared by every race in that county), and sigma_v[race]: race-specific
  county noise (e.g. Brown's coalition is geographically different from the
  ticket). Both measured from 2018-2024 county results, vote-weighted.

Plus the 2022 governor two-party vote in every district x county piece, so a
district's expected vote can be built from the feed's district-by-county
counts.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
PROCESSED = REPO_ROOT / "data" / "processed"
CACHE = PROCESSED / "county_layer.parquet"
PIECES_CACHE = PROCESSED / "county_district_pieces.parquet"

COMPOSITE_YEARS = ["2018", "2020", "2022", "2024"]
REFERENCE_RACES = ["atg", "aud", "sos_off", "tre"]

# Feed race key -> block-surface race label used to measure race x county noise
RACE_LABEL = {
    "governor": "gov",
    "us_senate": "uss",
    "attorney_general": "atg",
    "auditor": "aud",
    "secretary_of_state": "sos_off",
    "treasurer": "tre",
}


@dataclass(frozen=True)
class CountyInputs:
    counties: pd.DataFrame          # fips, lean, base_votes, weight
    sigma_u: float
    sigma_v: dict[str, float]       # feed race key -> sd
    pieces: pd.DataFrame            # office, district, fips, base_votes


def _county_map() -> pd.DataFrame:
    cm = pd.read_parquet(PROCESSED / "block_county_map.parquet")
    return pd.DataFrame({"block_geoid": cm["block_geoid"], "district_num": cm["county_fips"].astype(int)})


def _year_leans(cm: pd.DataFrame) -> tuple[dict[str, pd.DataFrame], dict[str, pd.DataFrame]]:
    from src.backbone import compute_lean_from_blocks

    leans, totals = {}, {}
    for y in COMPOSITE_YEARS:
        lean = compute_lean_from_blocks(pd.read_parquet(PROCESSED / f"block_votes_{y}.parquet"), cm)
        lean["race"] = lean["race"].str.rsplit("_", n=1).str[0]
        leans[y] = lean.pivot(index="district_num", columns="race", values="lean")
        totals[y] = lean.pivot(index="district_num", columns="race", values="d_votes") + \
            lean.pivot(index="district_num", columns="race", values="r_votes")
    return leans, totals


def _measure(leans: dict, w: pd.Series) -> tuple[float, dict[str, float]]:
    """Vote-weighted county noise: shared (sigma_u) and race-specific (sigma_v)."""
    ref = pd.DataFrame({
        y: (L["pre"] if int(y) % 4 == 0 else L[REFERENCE_RACES].mean(axis=1)) for y, L in leans.items()
    })
    k = ref.shape[1]
    sq = []
    for y in ref.columns:
        d = ref[y] - ref.drop(columns=y).mean(axis=1)
        sq.append(np.average(d**2, weights=w) * (k - 1) / k)
    sigma_u = float(np.sqrt(np.mean(sq)))

    def wsd(diffs: list[pd.Series], scale: float = 1.0) -> float:
        return float(np.sqrt(np.mean([np.average(d**2, weights=w) for d in diffs])) * scale)

    L18, L22, L24 = leans["2018"], leans["2022"], leans["2024"]
    sa18, sa22 = L18[REFERENCE_RACES].mean(axis=1), L22[REFERENCE_RACES].mean(axis=1)
    row = wsd([L[r] - sa for L, sa in ((L18, sa18), (L22, sa22)) for r in REFERENCE_RACES], np.sqrt(4 / 3))
    sigma_v = {
        "governor": wsd([L18["gov"] - sa18, L22["gov"] - sa22]),
        "us_senate": wsd([L18["uss"] - sa18, L24["uss"] - L24["pre"]]),
        **{k_: row for k_ in ("attorney_general", "auditor", "secretary_of_state", "treasurer")},
    }
    return sigma_u, sigma_v


def _pieces() -> pd.DataFrame:
    from src.office import get_office

    votes = pd.read_parquet(PROCESSED / "block_votes_2022.parquet")
    votes = votes[votes["race"] == "gov_2022"]
    cm = pd.read_parquet(PROCESSED / "block_county_map.parquet").rename(columns={"county_fips": "fips"})
    out = []
    for oname in ("house", "senate", "congress"):
        bmap = pd.read_parquet(get_office(oname).block_district_map_path)
        m = votes.merge(bmap, on="block_geoid").merge(cm, on="block_geoid")
        g = m.groupby(["district_num", "fips"])[["d_votes", "r_votes"]].sum().sum(axis=1).reset_index(name="base_votes")
        g.insert(0, "office", oname)
        out.append(g.rename(columns={"district_num": "district"}))
    return pd.concat(out, ignore_index=True)


@lru_cache(maxsize=1)
def county_inputs(force: bool = False) -> CountyInputs:
    """County leans, turnout baselines, measured noise, and district pieces (cached to parquet)."""
    if CACHE.exists() and PIECES_CACHE.exists() and not force:
        c = pd.read_parquet(CACHE)
        if "sigma_u" in c.columns:
            return CountyInputs(
                counties=c[["fips", "lean", "base_votes", "weight"]].copy(),
                sigma_u=float(c["sigma_u"].iloc[0]),
                sigma_v={k: float(c[f"sigma_v_{k}"].iloc[0]) for k in RACE_LABEL},
                pieces=pd.read_parquet(PIECES_CACHE),
            )

    import contextlib
    import io

    from src.backbone import build_composite_from_blocks

    cm = _county_map()
    with contextlib.redirect_stdout(io.StringIO()):
        comp = build_composite_from_blocks(COMPOSITE_YEARS, cm)
        leans, totals = _year_leans(cm)
    base = totals["2022"]["gov"]
    df = pd.DataFrame({"county": comp["district"].astype(int), "lean": comp["composite_lean"].to_numpy()})
    df["base_votes"] = df["county"].map(base)
    df["weight"] = df["base_votes"] / df["base_votes"].sum()
    df["lean"] = df["lean"] - np.average(df["lean"], weights=df["weight"])
    df["fips"] = df["county"].map(lambda x: f"{x:03d}")
    sigma_u, sigma_v = _measure(leans, base.reindex(df["county"]).to_numpy())

    out = df[["fips", "lean", "base_votes", "weight"]].sort_values("fips").reset_index(drop=True)
    cached = out.copy()
    cached["sigma_u"] = sigma_u
    for k, v in sigma_v.items():
        cached[f"sigma_v_{k}"] = v
    cached.to_parquet(CACHE, index=False)
    pieces = _pieces()
    pieces.to_parquet(PIECES_CACHE, index=False)
    return CountyInputs(counties=out, sigma_u=sigma_u, sigma_v=sigma_v, pieces=pieces)
