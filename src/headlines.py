"""
headlines.py — the two numbers shown for each statewide race: Polling Average
and Win Likelihood.

Every display surface (`forecast`, `election-night`, the Election Night page,
the published forecast page) calls statewide_headlines() so they always agree.

The model's blended "environment" (forecast_statewide.build_forecast) is
deliberately not a headline. It is the Democratic two-party share of a
generic statewide race (the average of AG/Auditor/SOS/Treasurer) with no
candidates attached: an input to every projection, but neither any real
race's vote share nor a probability.

- Polling Average: the race's own polls, as raw candidate toplines (what a
  reader would see on any poll tracker), weighted exactly as the model
  weights them (ingest_polls.poll_weights). Never the Brown-adjusted value,
  which is a model input, not a polling average. Its 80% band on the
  margin combines disagreement among the polls (their weighted spread, the
  same dispersion the model treats as poll uncertainty) with the shared
  statewide polling miss (election_night.STATEWIDE_POLL_ERROR_SD) that the
  Win Likelihood also assumes.
- Win Likelihood: P(Democrat wins) from the joint election-night model
  (src/election_night.py). Before any returns are entered it is the
  pre-election forecast; with returns it is the live posterior.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from src.forecast_statewide import OHIO_2026_RACES
from src.ingest_polls import POLLS, load_polls, poll_weights, two_party_share
from src.constants import POLL_PARTISAN_DISCOUNT, POLL_RECENCY_HALF_LIFE_DAYS

POLL_FILES: dict[str, str] = {"governor": "governor.csv", "us_senate": "senate.csv"}


Z80 = 1.2816  # half-width of a central 80% normal interval, in sd


@dataclass(frozen=True)
class PollingAverage:
    dem_name: str
    rep_name: str
    dem_pct: float          # weighted topline, percent
    rep_pct: float
    n_polls: int
    latest_end: pd.Timestamp
    spread_sd: float        # weighted sd of the polls' two-party D share (disagreement among polls)
    error_sd: float         # spread_sd combined with the shared statewide polling miss

    @property
    def margin(self) -> float:
        return self.dem_pct - self.rep_pct

    @property
    def margin_band80(self) -> tuple[float, float]:
        """
        80% range for the topline margin, in points. The two-party sd is
        doubled for a margin and scaled by the decided share so the band is
        on the same footing as the toplines it's shown next to.
        """
        half = Z80 * 2 * self.error_sd * (self.dem_pct + self.rep_pct)
        return self.margin - half, self.margin + half

    def describe(self) -> str:
        lead = self.dem_name if self.margin >= 0 else self.rep_name
        return (f"{self.dem_name.split()[-1]} {self.dem_pct:.1f} / {self.rep_name.split()[-1]} {self.rep_pct:.1f}"
                f" ({lead.split()[-1]} +{abs(self.margin):.1f})")

    def band_text(self) -> str:
        lo, hi = self.margin_band80
        d, r = self.dem_name.split()[-1], self.rep_name.split()[-1]
        fmt = lambda m: f"{d} +{m:.0f}" if m >= 0.5 else (f"{r} +{-m:.0f}" if m <= -0.5 else "even")
        return f"80% range {fmt(lo)} to {fmt(hi)}"


@dataclass(frozen=True)
class RaceHeadline:
    key: str
    label: str
    dem_name: str | None
    rep_name: str | None
    polling: PollingAverage | None
    win_likelihood: float          # P(Democrat wins)
    proj_d_share: float            # two-party; secondary, for detail views
    proj_low80: float
    proj_high80: float
    live_d_share: float | None = None
    live_fraction: float | None = None
    is_final: bool = False
    call: str = ""

    def win_text(self) -> str:
        d = (self.dem_name or "Democrat").split()[-1]
        r = (self.rep_name or "Republican").split()[-1]
        return f"{d} {self.win_likelihood:.0%} · {r} {1 - self.win_likelihood:.0%}"


def polling_average(race_key: str, as_of: pd.Timestamp | None = None) -> PollingAverage:
    """Weighted raw toplines for one polled statewide race, same weights as the model."""
    dem_name, rep_name = OHIO_2026_RACES[race_key]
    df = load_polls(POLLS / POLL_FILES[race_key], state="OH", cycle=2026, stage="general")
    polls = two_party_share(df, dem_name, rep_name)
    used, w = poll_weights(
        polls, "dem_two_party", as_of=as_of or pd.Timestamp.now().normalize(),
        half_life_days=POLL_RECENCY_HALF_LIFE_DAYS, partisan_discount=POLL_PARTISAN_DISCOUNT,
    )
    from src.election_night import STATEWIDE_POLL_ERROR_SD
    from src.forecast_statewide import SINGLE_POLL_FALLBACK_SIGMA

    tp = used["dem_two_party"].to_numpy()
    if len(used) > 1:
        spread = float(np.sqrt(np.average((tp - np.average(tp, weights=w)) ** 2, weights=w)))
    else:
        spread = SINGLE_POLL_FALLBACK_SIGMA
    return PollingAverage(
        dem_name=dem_name,
        rep_name=rep_name,
        dem_pct=float(np.average(used["dem_pct"], weights=w)),
        rep_pct=float(np.average(used["rep_pct"], weights=w)),
        n_polls=len(used),
        latest_end=used["end_date"].max(),
        spread_sd=spread,
        error_sd=float(np.sqrt(spread**2 + STATEWIDE_POLL_ERROR_SD**2)),
    )


def empty_frames() -> dict[str, pd.DataFrame]:
    """No returns entered: the pre-election forecast, regardless of what's in data/raw/."""
    from src.election_night import LEGISLATIVE_OFFICES, RETURN_COLUMNS

    return {
        o: pd.DataFrame(columns=["race" if o == "statewide" else "district", *RETURN_COLUMNS])
        for o in ("statewide", *LEGISLATIVE_OFFICES)
    }


def statewide_headlines(result=None, *, as_of: pd.Timestamp | None = None) -> list[RaceHeadline]:
    """
    Headlines for every statewide race. Pass an ElectionNightResult for live
    numbers; with none, the pre-election forecast is computed (live returns
    files are ignored, so `forecast` never shows a posterior by accident).
    """
    from src import election_night as en

    if result is None:
        model = en.build_prior_model(as_of)
        result = en.run_election_night(model, empty_frames(), n_sims=2_000)
    poll_as_of = min(as_of or pd.Timestamp.now().normalize(), en.ELECTION_DAY)

    races = result.races.set_index("race_id")
    out = []
    for sr in en.STATEWIDE_RACES:
        row = races.loc[f"statewide:{sr.key}"]
        names = OHIO_2026_RACES.get(sr.key, (None, None))
        polling = polling_average(sr.key, poll_as_of) if sr.key in POLL_FILES else None
        out.append(RaceHeadline(
            key=sr.key,
            label=sr.label,
            dem_name=names[0],
            rep_name=names[1],
            polling=polling,
            win_likelihood=float(row["win_prob"]),
            proj_d_share=float(row["proj_d_share"]),
            proj_low80=float(row["proj_low80"]),
            proj_high80=float(row["proj_high80"]),
            live_d_share=None if pd.isna(row["live_d_share"]) else float(row["live_d_share"]),
            live_fraction=None if pd.isna(row["live_fraction"]) else float(row["live_fraction"]),
            is_final=bool(row["is_final"]),
            call=str(row["call"]),
        ))
    return out
