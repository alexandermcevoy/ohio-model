"""
election_night.py — joint live projection for every Ohio race on election
night: Governor, US Senate, the four statewide row offices, Ohio House, Ohio
Senate (the 17 seats up) and US House.

Supersedes the per-office src/nowcast.py. That module conditioned each
chamber separately and treated every partial count as an unbiased,
independent reading. Both assumptions break on the night:

1. Offices share one environment. A strong Acton county count, an Attorney
   General statewide total and fifty House districts are all readings of the
   same latent statewide reference share E. One model, one posterior.

2. Partial counts are biased the same way everywhere. Ohio releases
   early/absentee ballots first, and those lean one way. Fifty districts at
   30% reporting all carry that lean; with independent noise they would
   average down into a confident but wrong environment. Here every partial
   observation loads on ONE shared skew term, scaled by the fraction not yet
   counted (1 - f), so a uniform early lean is absorbed by the skew term
   instead of being read as the environment.

Generative model (all two-party D shares; matches simulate.run_simulations
and forecast_statewide.build_forecast exactly when nothing has reported):

    governor        = E + dev_gov
    us_senate       = E + brown_offset + dev_uss
    row office k    = E + dev_k              (sum_k dev_k = 0: the four row
                                              offices ARE the reference)
    legislative i   = E + lean_i + inc_i + eta_office + eps_i

    observed share  = true share + (1 - f) * skew + N(0, tau(f)^2)

The prior on (E, dev_gov, dev_uss) is the polls+fundamentals blend: E starts
from the non-poll signals and is conditioned on the governor and Senate poll
averages, so E's marginal equals build_forecast()'s blended environment and
the two race deviations carry what the polls say about each race.

Races that are not uncertain are kept out of the Gaussian entirely and given a
fixed outcome: ballot overrides (src/ballot_overrides.py), Ohio Senate
holdovers (even districts, not on the 2026 ballot), and uncontested races
detected from the entered returns (one party at 0 votes).

Input is manual by design: the analyst types totals read off NYT, VoteHub,
AP or county boards into the CSVs under data/raw/. No scraping.
"""

from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import norm

REPO_ROOT = Path(__file__).resolve().parent.parent
RAW = REPO_ROOT / "data" / "raw"
PROCESSED = REPO_ROOT / "data" / "processed"
OUT_DIR = REPO_ROOT / "reports" / "election_night"

LEGISLATIVE_OFFICES: tuple[str, ...] = ("house", "senate", "congress")

ELECTION_DAY = pd.Timestamp("2026-11-03")


def prior_as_of() -> pd.Timestamp:
    """
    Date the pre-election prior is built at: today, but never past election
    day, so the prior (and the "vs. pre-election" comparison) doesn't shift
    at midnight while the count runs on.
    """
    return min(pd.Timestamp.now().normalize(), ELECTION_DAY)

# ---------------------------------------------------------------------------
# Statewide races
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class StatewideRace:
    key: str          # row key in live_returns_oh_statewide.csv
    label: str
    dev_state: str    # name of this race's deviation term in the state vector


STATEWIDE_RACES: tuple[StatewideRace, ...] = (
    StatewideRace("governor", "Governor", "dev_gov"),
    StatewideRace("us_senate", "US Senate (special)", "dev_uss"),
    StatewideRace("attorney_general", "Attorney General", "dev_atg"),
    StatewideRace("auditor", "Auditor", "dev_aud"),
    StatewideRace("secretary_of_state", "Secretary of State", "dev_sos_off"),
    StatewideRace("treasurer", "Treasurer", "dev_tre"),
)
ROW_OFFICE_RACES: tuple[str, ...] = ("atg", "aud", "sos_off", "tre")  # = REFERENCE_MIDTERM_RACES
ROW_OFFICE_CYCLES: tuple[int, ...] = (2010, 2014, 2018, 2022)

# ---------------------------------------------------------------------------
# Judgment calls — documented defaults, NOT fitted. No archive of
# minute-by-minute Ohio reporting exists to calibrate any of these against.
# Do not tune them against a single replay.
# ---------------------------------------------------------------------------

TAU_FLOOR: float = 0.01
"""Observation sd that remains even at ~100% reporting (provisional and late ballots)."""

TAU_SCALE: float = 0.10
"""
Independent observation sd at 0% reporting (which precincts/counties happen
to be in); shrinks linearly to 0 as f -> 1. src/nowcast.py used 0.175, but
that had to absorb vote-type bias too, which EARLY_COUNT_SKEW_SD now carries
as a shared term. At 0%: sqrt(0.10^2 + 0.06^2) = 11.7 pts total; at 50%:
about 5.8 pts.
"""

EARLY_COUNT_SKEW_MEAN: float = 0.0
"""
Prior mean of the shared partial-count skew: how far a 0%-complete count's D
share sits from the final share, applied as (1 - f) * skew. Ohio's
early/absentee-first release order makes early counts lean D in recent
cycles, but there is no measured Ohio value in this repo, so the default is 0
with a wide sd and the analyst can set it (`--early-skew`) once the first
counties in are compared against their expected share.
"""

EARLY_COUNT_SKEW_SD: float = 0.06

ETA_CROSS_OFFICE_CORR: float = 0.0
"""
Correlation between the House, Ohio Senate and US House offsets from the
reference race (eta). 0 matches the pre-election model, which estimates and
simulates each office's sigma_shared independently. A positive value would
let a House over-performance move the other chambers beyond what flows
through E.
"""

STATEWIDE_POLL_ERROR_SD: float = 0.03
"""
Shared error of this cycle's Ohio statewide polling averages (two-party
share), common to the governor and Senate polls. Without it the poll averages
pin each race to their cross-poll dispersion (~1.2 pts), which is far tighter
than how Ohio polls have actually done: the final averages missed toward
Democrats by roughly 2-3.5 pts of two-party share in 2018 (governor), 2020
(president), 2022 and 2024 (Senate). Those misses are from public reporting,
not data in this repo, so the mean is left at 0 and only the size informs the
sd. A judgment call. It widens the two polled races only; the environment
E keeps the published blend's sd (MIN_OHIO_SIGMA floor).
"""

CALL_THRESHOLD: float = 0.99

# --- County layer / SOS feed (src/enr_feed.py, src/county_layer.py) ---------
# Judgment calls like the ones above; none is fitted to Ohio reporting data.

ABSENTEE_GAP_MEAN: float = 0.10
ABSENTEE_GAP_SD: float = 0.08
"""
Prior on Delta: D two-party share of absentee (early + mail) ballots minus
that of Election Day ballots, shared statewide. The feed reports absentee
ballots cast per county but not who they voted for, so Delta is estimated
from how counted shares move as each county's mix of absentee and Election
Day ballots changes. The prior is wide; the returns do most of the work.
"""

COUNTY_GAP_SD: float = 0.045
"""
How much a county's own absentee-vs-Election-Day gap can differ from the
statewide Delta. Without it the model would read cross-county differences in
absentee mix as if every county had the same gap, and pin Delta (and the
environment) far too early. Added to each county's composition noise as
(Delta loading x COUNTY_GAP_SD)^2. Chosen before looking at rehearsal
coverage; do not tune it to a replay.
"""

COUNTY_TAU_FLOOR: float = 0.004
COUNTY_TAU_SCALE: float = 0.05
"""
How far a county's counted ballots can differ from its eventual whole
(which precincts are in), after the absentee/Election-Day mix is accounted
for: sqrt(floor^2 + scale^2 (1 - f)^2). Shared by every race counted in that
county, since they are tallies of the same ballots.
"""

ABSENTEE_PRIOR_SHARE: float = 0.35   # absentee share of a county's ballots before it reports any
RESIDUAL_FRACTION: float = 0.02      # late absentee + provisional ballots still out when precincts hit 100%
BALLOTS_PER_VOTE: float = 1.03       # ballots per two-party governor vote before a county reports
TURNOUT_PRIOR_MEAN: float = 1.0      # 2026 two-party turnout relative to 2022 governor
TURNOUT_PRIOR_SD: float = 0.12
COUNTY_TURNOUT_SD: float = 0.06      # county turnout noise around the statewide factor
PRECINCT_EXTRAPOLATION_MIN: float = 0.5   # precinct share at which a county's own turnout takes over
CALL_GUARD: float = 0.30
"""
No CALL while the counted margin is smaller than CALL_GUARD x the expected
outstanding vote, whatever the posterior says: the remaining ballots would
only need to break 65/35 against the leader to flip it.
"""

FINAL_TAU: float = 1e-4  # sd for a fully counted race; tiny but keeps the solve well-conditioned

# Seat thresholds worth a probability on the dashboard, per office.
CHAMBER_THRESHOLDS: dict[str, tuple[tuple[int, str], ...]] = {
    "house": ((34, "hold 34"), (40, "block supermajority (40)"), (50, "majority (50)")),
    "senate": ((9, "hold 9"), (14, "block supermajority (14)"), (17, "majority (17)")),
    "congress": ((5, "hold 5"), (6, "gain 1 (6)"), (7, "gain 2 (7)")),
}


# ---------------------------------------------------------------------------
# Prior model
# ---------------------------------------------------------------------------

@dataclass
class RaceRow:
    """One race the model projects (or holds fixed)."""

    race_id: str          # "statewide:governor", "house:52", ...
    office: str           # "statewide" | "house" | "senate" | "congress"
    district: int | None
    label: str
    const: float          # share = const + loading . x
    loading: dict[str, float]
    fixed_winner: str | None = None   # "D" | "R" when not uncertain
    fixed_reason: str | None = None
    current_holder: str | None = None
    open_seat: bool = False


@dataclass
class PriorModel:
    state_names: list[str]
    mean: np.ndarray
    cov: np.ndarray
    races: list[RaceRow]
    expected_total: dict[str, float]  # race_id -> expected two-party total
    environment_prior: tuple[float, float]
    as_of: pd.Timestamp
    notes: list[str] = field(default_factory=list)
    county: object | None = None        # county_layer.CountyInputs
    race_const: dict[str, float] = field(default_factory=dict)  # statewide race key -> const

    def index(self, name: str) -> int:
        return self.state_names.index(name)


def _row_office_sd() -> float:
    """Pooled sd of each row office's deviation from the four-office mean (3 df per cycle)."""
    from src.forecast_statewide import _statewide_shares
    from src.composite import reference_share

    sq, df = 0.0, 0
    for year in ROW_OFFICE_CYCLES:
        shares = _statewide_shares(year)
        present = [r for r in ROW_OFFICE_RACES if r in shares]
        if len(present) < 2:
            continue
        ref = reference_share(shares, year)
        sq += sum((shares[r] - ref) ** 2 for r in present)
        df += len(present) - 1
    if df == 0:
        raise ValueError("No cycles with row-office results to measure their spread.")
    return float(np.sqrt(sq / df))


@lru_cache(maxsize=None)
def _district_turnout_2022(office_name: str) -> dict[int, float]:
    """
    2022 governor two-party total per CURRENT district, from block vote
    surfaces through the current map. Joining 2022 house totals by district
    number would be wrong for relocated House districts, Ohio Senate odd
    districts and every district on the Oct 2025 congressional map.
    """
    from src.office import get_office

    office = get_office(office_name)
    votes = pd.read_parquet(PROCESSED / "block_votes_2022.parquet")
    votes = votes[votes["race"] == "gov_2022"]
    bmap = pd.read_parquet(office.block_district_map_path)
    merged = votes.merge(bmap, on="block_geoid", how="inner")
    tot = merged.groupby("district_num")[["d_votes", "r_votes"]].sum().sum(axis=1)
    return {int(d): float(v) for d, v in tot.items()}


@lru_cache(maxsize=None)
def _statewide_turnout_2022() -> float:
    votes = pd.read_parquet(PROCESSED / "block_votes_2022.parquet")
    votes = votes[votes["race"] == "gov_2022"]
    return float(votes["d_votes"].sum() + votes["r_votes"].sum())


def _environment_block(as_of: pd.Timestamp) -> tuple[np.ndarray, np.ndarray, float, dict]:
    """
    Joint prior over (E, dev_gov, dev_uss), conditioned on the governor and
    Senate poll averages. Reuses build_forecast()'s signals so the inputs are
    identical; E's marginal mean equals summary.ohio_value.
    """
    from src.forecast_statewide import (
        CANDIDATE_IDIOSYNCRASY_SIGMA, MIN_OHIO_SIGMA, build_forecast,
    )

    summary, _ = build_forecast(as_of=as_of, n_samples=10)
    comps = {c.label: c for c in summary.components}
    devs = {d.race: d for d in summary.race_deviations}

    prior_signals = [c for c in summary.components if c.label in ("national_fundamentals", "historical_governor")]
    prec = sum(1 / s.sigma**2 for s in prior_signals)
    e0 = sum(s.mean / s.sigma**2 for s in prior_signals) / prec

    race_sd = {
        key: max(devs[race].predictive_sd, CANDIDATE_IDIOSYNCRASY_SIGMA)
        for key, race in (("gov", "gov"), ("uss", "uss"))
    }
    m = np.array([e0, 0.0, 0.0])
    P = np.diag([1 / prec, race_sd["gov"] ** 2, race_sd["uss"] ** 2])

    # Poll observations: raw average = E + offset + dev + poll dispersion.
    H, y, R = [], [], []
    for key, label, col in (("gov", "governor_polls", 1), ("uss", "senate_polls", 2)):
        sig = comps[label]
        poll_var = max(sig.sigma**2 - race_sd[key] ** 2, 1e-8)
        h = np.zeros(3)
        h[0], h[col] = 1.0, 1.0
        H.append(h)
        y.append(sig.raw_mean - devs[key].applied_offset)
        R.append(poll_var)
    H, y, R = np.array(H), np.array(y), np.diag(R)
    S = H @ P @ H.T + R
    K = P @ H.T @ np.linalg.inv(S)
    m = m + K @ (y - H @ m)
    P = P - K @ H @ P

    # Same floor build_forecast applies: covers correlated Ohio polling error.
    if P[0, 0] < MIN_OHIO_SIGMA**2:
        P[0, 0] = MIN_OHIO_SIGMA**2

    # Shared polling error on the two polled races. Added after conditioning
    # so E stays identical to the published blend (whose MIN_OHIO_SIGMA floor
    # already stands in for correlated poll error on the environment).
    P[1:3, 1:3] += STATEWIDE_POLL_ERROR_SD**2
    P = (P + P.T) / 2

    info = {
        "ohio_value": summary.ohio_value,
        "ohio_sigma": summary.ohio_sigma,
        "uss_offset": devs["uss"].applied_offset,
        "gov_offset": devs["gov"].applied_offset,
    }
    return m, P, info["uss_offset"], info


def build_prior_model(
    as_of: pd.Timestamp | None = None,
    *,
    with_incumbency: bool = False,
    early_skew_mean: float = EARLY_COUNT_SKEW_MEAN,
) -> PriorModel:
    """Assemble the joint pre-election prior over every Ohio race."""
    from src.ballot_overrides import overrides_for_office
    from src.office import get_office
    from src.simulate import (
        SimConfig, compute_incumbency_shifts, compute_shared_sigma, compute_sigma_prior,
        estimate_district_sigma,
    )

    as_of = as_of or prior_as_of()
    notes: list[str] = []

    env_m, env_P, uss_offset, env_info = _environment_block(as_of)
    if abs(env_m[0] - env_info["ohio_value"]) > 1e-6 or abs(env_P[0, 0] ** 0.5 - env_info["ohio_sigma"]) > 1e-6:
        raise AssertionError(
            f"Election-night prior environment {env_m[0]:.4f} ± {env_P[0, 0] ** 0.5:.4f} does not "
            f"match build_forecast() {env_info['ohio_value']:.4f} ± {env_info['ohio_sigma']:.4f}"
        )

    names: list[str] = ["E", "dev_gov", "dev_uss", "skew"]
    means: list[float] = [*env_m, early_skew_mean]
    blocks: list[np.ndarray] = [env_P, np.array([[EARLY_COUNT_SKEW_SD**2]])]

    row_sd = _row_office_sd()
    k = len(ROW_OFFICE_RACES)
    names += [f"dev_{r}" for r in ROW_OFFICE_RACES]
    means += [0.0] * k
    blocks.append(row_sd**2 * k / (k - 1) * (np.eye(k) - np.ones((k, k)) / k))

    # County layer: each statewide race in county c is
    #   E + dev_race + const + lean_c + u_c + v_{c,race}.
    # Leans are relative to the state, so the county deviations u (shared by
    # every race in the county) and v (race-specific) are centered: their
    # turnout-weighted sum is 0 by construction (cov = s^2 M M', M = I - 1 w').
    # Counties then only redistribute a statewide result geographically, and
    # with nothing counted every statewide race keeps exactly its published
    # mean and variance.
    from src.county_layer import county_inputs

    ci = county_inputs()
    w = ci.counties["weight"].to_numpy()
    fips = ci.counties["fips"].tolist()
    centered = np.eye(len(fips)) - np.outer(np.ones(len(fips)), w)
    centered_cov = centered @ centered.T
    names += ["delta"]
    means += [ABSENTEE_GAP_MEAN]
    blocks.append(np.array([[ABSENTEE_GAP_SD**2]]))
    names += [f"u_{f}" for f in fips]
    means += [0.0] * len(fips)
    blocks.append(centered_cov * ci.sigma_u**2)
    for sr in STATEWIDE_RACES:
        names += [f"v_{sr.key}_{f}" for f in fips]
        means += [0.0] * len(fips)
        blocks.append(centered_cov * ci.sigma_v[sr.key] ** 2)

    races: list[RaceRow] = []
    expected: dict[str, float] = {}
    race_const: dict[str, float] = {}
    sw_total = _statewide_turnout_2022()
    for sr in STATEWIDE_RACES:
        const = uss_offset if sr.key == "us_senate" else 0.0
        race_const[sr.key] = const
        rid = f"statewide:{sr.key}"
        races.append(RaceRow(rid, "statewide", None, sr.label, const, {"E": 1.0, sr.dev_state: 1.0}))
        expected[rid] = sw_total

    # Office offsets (eta), possibly correlated across offices.
    eta_sd: dict[str, float] = {}
    office_data: dict[str, tuple] = {}
    for oname in LEGISLATIVE_OFFICES:
        office = get_office(oname)
        targeting = pd.read_csv(REPO_ROOT / f"reports/session3/{office.output_prefix}_targeting.csv")
        composite = pd.read_csv(REPO_ROOT / f"reports/session2/{office.output_prefix}_composite_lean.csv")
        cfg = SimConfig(include_incumbency=with_incumbency)
        sigma_shared = compute_shared_sigma(composite)
        sigma_df = estimate_district_sigma(
            targeting, compute_sigma_prior(composite), cfg, sigma_shared=sigma_shared,
        )
        eta_sd[oname] = sigma_shared
        office_data[oname] = (office, targeting, sigma_df, cfg)

    eta_names = [f"eta_{o}" for o in LEGISLATIVE_OFFICES]
    sds = np.array([eta_sd[o] for o in LEGISLATIVE_OFFICES])
    corr = np.full((len(sds), len(sds)), ETA_CROSS_OFFICE_CORR)
    np.fill_diagonal(corr, 1.0)
    names += eta_names
    means += [0.0] * len(sds)
    blocks.append(np.outer(sds, sds) * corr)

    eps_var: list[float] = []
    for oname in LEGISLATIVE_OFFICES:
        office, targeting, sigma_df, cfg = office_data[oname]
        overrides = overrides_for_office(oname)
        inc = compute_incumbency_shifts(targeting, cfg)
        turnout = _district_turnout_2022(oname)
        sig = sigma_df.set_index("district")["sigma_i"]
        seats_up = set(office.seats_up_2026)
        for _, row in targeting.sort_values("district").iterrows():
            d = int(row["district"])
            rid = f"{oname}:{d}"
            holder = row.get("current_holder")
            holder = holder if holder in ("D", "R") else None
            race = RaceRow(
                rid, oname, d, f"{office.display_name} {d}", 0.0, {},
                current_holder=holder,
                open_seat=bool(row.get("open_seat_2026", False)) if not pd.isna(row.get("open_seat_2026", False)) else False,
            )
            if d not in seats_up:
                if holder is None:
                    raise ValueError(f"{rid} is a holdover with no current_holder")
                race.fixed_winner, race.fixed_reason = holder, "holdover (not up in 2026)"
            elif d in overrides:
                ov = overrides[d]
                race.fixed_winner = "D" if ov.effective_dem_prob >= 0.5 else "R"
                race.fixed_reason = f"ballot override: {ov.status}"
            else:
                eps = f"eps_{oname}_{d}"
                names.append(eps)
                means.append(0.0)
                var = float(sig[d]) ** 2
                if inc[d] != 0:
                    var += cfg.incumbency_sd**2
                eps_var.append(var)
                race.const = float(row["composite_lean"]) + float(inc[d])
                race.loading = {"E": 1.0, f"eta_{oname}": 1.0, eps: 1.0}
            races.append(race)
            expected[rid] = turnout.get(d, np.nan)
    blocks.append(np.diag(eps_var))

    n = sum(b.shape[0] for b in blocks)
    cov = np.zeros((n, n))
    i = 0
    for b in blocks:
        j = i + b.shape[0]
        cov[i:j, i:j] = b
        i = j

    return PriorModel(
        state_names=names,
        mean=np.array(means, dtype=float),
        cov=cov,
        races=races,
        expected_total=expected,
        environment_prior=(float(env_m[0]), float(env_P[0, 0] ** 0.5)),
        as_of=as_of,
        notes=notes,
        county=ci,
        race_const=race_const,
    )


# ---------------------------------------------------------------------------
# Live returns: files, parsing, validation
# ---------------------------------------------------------------------------

RETURN_COLUMNS = ["dem_votes", "rep_votes", "pct_reporting", "is_final"]


def returns_path(office: str) -> Path:
    prefix = "oh_statewide" if office == "statewide" else f"oh_{office}"
    return RAW / f"live_returns_{prefix}.csv"


def _template_keys(office: str) -> tuple[str, list]:
    if office == "statewide":
        return "race", [r.key for r in STATEWIDE_RACES]
    from src.office import get_office

    return "district", list(get_office(office).seats_up_2026)


def ensure_return_files() -> list[Path]:
    """
    Create any missing returns CSV, and add any missing column or race row to
    an existing one without touching what is already typed in. Returns the
    files that were created or changed.
    """
    changed = []
    for office in ("statewide", *LEGISLATIVE_OFFICES):
        path = returns_path(office)
        key, keys = _template_keys(office)
        if path.exists():
            df = pd.read_csv(path, dtype=str, keep_default_na=False)
        else:
            df = pd.DataFrame(columns=[key, *RETURN_COLUMNS])
        before = df.copy()
        for col in RETURN_COLUMNS:
            if col not in df.columns:
                df[col] = ""
        have = set(df[key].astype(str))
        missing = [k for k in keys if str(k) not in have]
        if missing:
            add = pd.DataFrame({key: [str(k) for k in missing]})
            df = pd.concat([df, add], ignore_index=True).fillna("")
        df["is_final"] = df["is_final"].replace("", "False")
        if not path.exists() or not df.equals(before):
            write_returns(office, df)
            changed.append(path)
    return changed


def load_returns(office: str) -> pd.DataFrame:
    path = returns_path(office)
    if not path.exists():
        ensure_return_files()
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def write_returns(office: str, df: pd.DataFrame) -> Path:
    """Atomic write, so a crash mid-save never leaves a half-written file."""
    path = returns_path(office)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    os.close(fd)
    df.to_csv(tmp, index=False)
    os.replace(tmp, path)
    return path


_TRUE = {"true", "1", "yes", "y", "t", "x"}
_FALSE = {"false", "0", "no", "n", "f", ""}


def _parse_bool(val, where: str) -> bool:
    s = str(val).strip().lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    raise ValueError(f"{where}: is_final must be True/False, got {val!r}")


def _parse_count(val, where: str) -> int:
    s = str(val).strip().replace(",", "")
    if s in ("", "nan"):
        return 0
    try:
        v = float(s)
    except ValueError:
        raise ValueError(f"{where}: vote count must be a number, got {val!r}") from None
    if v < 0:
        raise ValueError(f"{where}: negative vote count {val!r}")
    return int(round(v))


def _parse_pct(val, where: str) -> float | None:
    s = str(val).strip().rstrip("%").strip()
    if s in ("", "nan"):
        return None
    try:
        v = float(s)
    except ValueError:
        raise ValueError(f"{where}: pct_reporting must be a number, got {val!r}") from None
    return v


@dataclass(frozen=True)
class Observation:
    race_id: str
    dem_votes: int
    rep_votes: int
    fraction: float | None      # fraction of expected vote counted; None = unknown
    fraction_source: str        # "entered" | "turnout_2022" | "unknown" | "final"
    is_final: bool

    @property
    def share(self) -> float:
        return self.dem_votes / (self.dem_votes + self.rep_votes)


def parse_returns(
    model: PriorModel,
    frames: dict[str, pd.DataFrame] | None = None,
) -> tuple[list[Observation], dict[str, str], list[str]]:
    """
    Read every returns file (or the given frames) into observations.

    Returns (observations, uncontested {race_id: winner}, warnings).
    """
    frames = frames or {o: load_returns(o) for o in ("statewide", *LEGISLATIVE_OFFICES)}
    known = {r.race_id: r for r in model.races}
    obs: list[Observation] = []
    uncontested: dict[str, str] = {}
    warnings: list[str] = []

    for office, df in frames.items():
        key = "race" if office == "statewide" else "district"
        for _, row in df.iterrows():
            k = str(row.get(key, "")).strip()
            if not k:
                continue
            if office != "statewide":
                try:
                    k = str(int(float(k)))
                except ValueError:
                    raise ValueError(f"{returns_path(office).name}: bad district {k!r}") from None
            rid = f"{office}:{k}"
            where = f"{returns_path(office).name} [{rid}]"
            dem = _parse_count(row.get("dem_votes", ""), where)
            rep = _parse_count(row.get("rep_votes", ""), where)
            pct = _parse_pct(row.get("pct_reporting", ""), where)
            final = _parse_bool(row.get("is_final", ""), where)
            if dem + rep == 0:
                if final:
                    warnings.append(f"{where}: marked final with no votes entered; ignored")
                continue
            if rid not in known:
                warnings.append(f"{where}: not a race on the 2026 ballot in this model; ignored")
                continue
            race = known[rid]
            if race.fixed_winner is not None:
                warnings.append(f"{where}: votes entered for a fixed race ({race.fixed_reason}); ignored")
                continue
            if dem == 0 or rep == 0:
                uncontested[rid] = "D" if rep == 0 else "R"
                warnings.append(
                    f"{where}: treated as uncontested ({uncontested[rid]}) because one party has 0 votes; "
                    "if that's a data-entry gap, fill in both columns"
                )
                continue

            exp_total = model.expected_total.get(rid, np.nan)
            if pct is not None and not (0 < pct <= 100):
                warnings.append(f"{where}: pct_reporting {pct} outside (0, 100]; using turnout estimate instead")
                pct = None
            if final:
                f, src = 1.0, "final"
            elif pct is not None:
                f, src = pct / 100, "entered"
            elif np.isfinite(exp_total) and exp_total > 0:
                f, src = (dem + rep) / exp_total, "turnout_2022"
                if f > 1.5:
                    warnings.append(
                        f"{where}: {dem + rep:,} votes is {f:.1f}x the 2022 total; check the entry"
                    )
            else:
                f, src = None, "unknown"
            if f is not None:
                f = min(f, 0.999) if not final else 1.0
            obs.append(Observation(rid, dem, rep, f, src, final))

    return obs, uncontested, warnings



# ---------------------------------------------------------------------------
# SOS feed: turnout, outstanding vote, observations
# ---------------------------------------------------------------------------

def estimate_turnout(model: PriorModel, snap) -> tuple[pd.DataFrame, float, float]:
    """
    Expected final two-party (governor) vote per county and how the counted
    ballots are composed.

    Before a county's precincts are substantially in, its expected vote is
    2022 turnout x a statewide factor g. g starts at TURNOUT_PRIOR_MEAN and
    is learned from counties with at least PRECINCT_EXTRAPOLATION_MIN of
    precincts reported (absentee + Election-Day ballots extrapolated by the
    precinct share). The first absentee batch arrives with 0 precincts
    reported, so precinct share alone would read it as a whole county.

    Returns (per-county frame, g mean, g sd).
    """
    ci = model.county
    c = ci.counties.merge(snap.counties, on="fips", how="left")
    gov = snap.statewide[snap.statewide["race"] == "governor"].set_index("fips") if len(snap.statewide) else None
    t_gov = (gov["dem_votes"] + gov["rep_votes"]).reindex(c["fips"]).fillna(0).to_numpy() if gov is not None \
        else np.zeros(len(c))
    c["t_gov"] = t_gov
    B = c["ballots_total"].fillna(0).to_numpy(float)
    A = c["ballots_absentee"].fillna(0).to_numpy(float)
    p = (c["precincts_reported"] / c["precincts_total"].replace(0, np.nan)).fillna(0).to_numpy()
    complete = c["count_status"].fillna("").str.lower().isin(["completed", "complete", "final"]).to_numpy()
    kappa = np.where(t_gov > 0, B / np.maximum(t_gov, 1), BALLOTS_PER_VOTE)
    kappa = np.clip(kappa, 1.0, 1.25)

    # County's own projection of its final ballots, where precincts allow it.
    own = np.full(len(c), np.nan)
    ok = (p >= PRECINCT_EXTRAPOLATION_MIN) & (B > A)
    own[ok] = A[ok] + (B[ok] - A[ok]) / p[ok]
    own[complete] = B[complete] * (1 + RESIDUAL_FRACTION)
    own_votes = own / kappa
    base = c["base_votes"].to_numpy()

    has_own = np.isfinite(own_votes)
    share_known = float(base[has_own].sum() / base.sum()) if has_own.any() else 0.0
    if has_own.any():
        ratio = float(np.average(own_votes[has_own] / base[has_own], weights=base[has_own]))
        g = (1 - share_known) * TURNOUT_PRIOR_MEAN + share_known * ratio
    else:
        g = TURNOUT_PRIOR_MEAN
    g_sd = TURNOUT_PRIOR_SD * (1 - share_known) + 0.02 * share_known

    n_hat = np.where(has_own, own_votes, base * g)
    n_sd = np.where(has_own, own_votes * 0.10 * (1 - np.clip(p, 0, 1)), base * COUNTY_TURNOUT_SD)
    n_sd[complete] = (B[complete] / kappa[complete]) * RESIDUAL_FRACTION / 2
    floor = np.maximum(t_gov, B / kappa) * (1 + RESIDUAL_FRACTION)
    n_hat = np.maximum(n_hat, floor)

    nb = n_hat * kappa                                   # expected final ballots
    a_final = np.where(complete, A / np.maximum(B, 1),
                       np.where(A > 0, A * (1 + RESIDUAL_FRACTION) / nb, ABSENTEE_PRIOR_SHARE))
    a_final = np.clip(a_final, 0, 1)
    f = np.clip(B / nb, 0, 1 - RESIDUAL_FRACTION / 2)
    counted_abs = np.where(B > 0, A / np.maximum(B, 1), 0.0)
    loading = np.where(B > 0, counted_abs - a_final, 0.0)  # Delta loading of the counted share
    tau = np.sqrt(COUNTY_TAU_FLOOR**2 + COUNTY_TAU_SCALE**2 * (1 - f) ** 2 + (loading * COUNTY_GAP_SD) ** 2)

    c = c.assign(precinct_frac=p, complete=complete, kappa=kappa, n_hat=n_hat, n_sd=n_sd, has_own=has_own,
                 absentee_final=a_final, fraction=f, delta_loading=loading, tau=tau, ballots=B, absentee=A)
    return c, float(g), float(g_sd)


def absentee_missing(snap) -> bool:
    """Ballots are being counted but the feed reports no absentee ballots anywhere."""
    return bool(snap.counties["ballots_total"].sum() > 0 and snap.counties["ballots_absentee"].sum() == 0)


@dataclass
class FeedObservations:
    H: np.ndarray
    y: np.ndarray
    R: np.ndarray
    records: list[dict]          # one per observation, for reporting
    race_ids: set[str]           # races with any feed data
    uncontested: dict[str, str]  # race_id -> winner, from the feed's candidate list
    turnout: pd.DataFrame
    g: tuple[float, float]


def feed_observations(model: PriorModel, snap) -> FeedObservations:
    """County observations for statewide races and district observations for legislative ones."""
    turnout, g, g_sd = estimate_turnout(model, snap)
    no_absentee = absentee_missing(snap)
    if no_absentee:
        # Without absentee counts the mix of counted ballots is unknown; fall
        # back to the manual-entry early-count skew, (1 - fraction) x skew.
        turnout["delta_loading"] = 0.0
        turnout["tau"] = np.sqrt(COUNTY_TAU_FLOOR**2 + COUNTY_TAU_SCALE**2 * (1 - turnout["fraction"]) ** 2)
    idx = {n: i for i, n in enumerate(model.state_names)}
    races = {r.race_id: r for r in model.races}
    t_by_fips = turnout.set_index("fips")
    lean = t_by_fips["lean"]
    rows_h, rows_y, records = [], [], []
    noise_groups: list[str] = []   # observations sharing a group share composition noise
    noise_sd: list[float] = []
    race_ids: set[str] = set()
    uncontested: dict[str, str] = {}

    # Statewide: one observation per county x race, composition noise shared within county.
    for r in (snap.statewide.itertuples() if len(snap.statewide) else []):
        tot = r.dem_votes + r.rep_votes
        rid = f"statewide:{r.race}"
        if tot <= 0 or not (r.has_dem and r.has_rep) or rid not in races:
            continue
        race_ids.add(rid)
        tc = t_by_fips.loc[r.fips]
        h = np.zeros(len(idx))
        sr = next(x for x in STATEWIDE_RACES if x.key == r.race)
        h[idx["E"]] = 1.0
        h[idx[sr.dev_state]] = 1.0
        h[idx[f"u_{r.fips}"]] = 1.0
        h[idx[f"v_{r.race}_{r.fips}"]] = 1.0
        h[idx["delta"]] = tc["delta_loading"]
        if no_absentee:
            h[idx["skew"]] = 1 - tc["fraction"]
        rows_h.append(h)
        rows_y.append(r.dem_votes / tot - model.race_const[r.race] - lean[r.fips])
        noise_groups.append(f"county:{r.fips}")
        noise_sd.append(float(tc["tau"]))
        records.append({"race_id": rid, "fips": r.fips, "dem": r.dem_votes, "rep": r.rep_votes})

    # Legislative: one observation per district, summed over its county pieces.
    pieces = model.county.pieces
    if len(snap.legislative):
        ratio = (t_by_fips["n_hat"] / t_by_fips["base_votes"]).to_dict()
        for (office, district), grp in snap.legislative.groupby(["office", "district"]):
            rid = f"{office}:{int(district)}"
            race = races.get(rid)
            if race is None or race.fixed_winner is not None:
                continue
            has_dem, has_rep = bool(grp["has_dem"].iloc[0]), bool(grp["has_rep"].iloc[0])
            if not (has_dem and has_rep):
                if has_dem or has_rep:
                    uncontested[rid] = "D" if has_dem else "R"
                continue
            dem, rep = int(grp["dem_votes"].sum()), int(grp["rep_votes"].sum())
            if dem + rep <= 0:
                continue
            race_ids.add(rid)
            pc = pieces[(pieces["office"] == office) & (pieces["district"] == district)].set_index("fips")
            n_pieces = pc["base_votes"] * pc.index.map(ratio)
            n_i = float(n_pieces.sum())
            counted = grp.set_index("fips")[["dem_votes", "rep_votes"]].sum(axis=1)
            t_i = float(counted.sum())
            # Each piece's counted ballots are assumed to have its county's absentee mix.
            abs_frac = (t_by_fips["absentee"] / t_by_fips["ballots"].replace(0, np.nan)).fillna(0)
            comp_counted = float((counted * abs_frac.reindex(counted.index)).sum() / t_i)
            comp_final = float((n_pieces * t_by_fips.loc[n_pieces.index, "absentee_final"]).sum() / max(n_i, 1))
            frac = min(t_i / max(n_i, 1), 1 - RESIDUAL_FRACTION / 2)
            h = _loading_vector(model, race)
            d_load = 0.0 if no_absentee else comp_counted - comp_final
            h[idx["delta"]] = d_load
            if no_absentee:
                h[idx["skew"]] = 1 - frac
            rows_h.append(h)
            rows_y.append(dem / (dem + rep) - race.const)
            noise_groups.append(rid)
            noise_sd.append(float(np.sqrt(TAU_FLOOR**2 + TAU_SCALE**2 * (1 - frac) ** 2 + (d_load * COUNTY_GAP_SD) ** 2)))
            records.append({"race_id": rid, "fips": None, "dem": dem, "rep": rep,
                            "expected": n_i, "fraction": frac})

    # Contests on the ballot with only one major party, from the candidate list.
    cands = snap.candidates
    if len(cands):
        for (office, key), grp in cands.groupby(["office", "key"]):
            rid = f"{office}:{key}" if office != "statewide" else f"statewide:{key}"
            race = races.get(rid)
            parties = set(grp["party"])
            if race is not None and race.fixed_winner is None and parties in ({"D"}, {"R"}):
                uncontested[rid] = parties.pop()

    n = len(rows_y)
    R = np.zeros((n, n))
    groups = pd.Series(noise_groups)
    for g_name, members in groups.groupby(groups).groups.items():
        mi = np.array(list(members))
        sd = noise_sd[mi[0]]
        R[np.ix_(mi, mi)] = sd**2
    R += np.eye(n) * 1e-8
    return FeedObservations(
        H=np.array(rows_h) if n else np.zeros((0, len(idx))),
        y=np.array(rows_y),
        R=R,
        records=records,
        race_ids=race_ids,
        uncontested=uncontested,
        turnout=turnout,
        g=(g, g_sd),
    )


# ---------------------------------------------------------------------------
# Posterior
# ---------------------------------------------------------------------------

def _loading_vector(model: PriorModel, race: RaceRow) -> np.ndarray:
    h = np.zeros(len(model.state_names))
    for name, w in race.loading.items():
        h[model.index(name)] = w
    return h


def condition(
    model: PriorModel,
    observations: list[Observation],
    *,
    tau_floor: float = TAU_FLOOR,
    tau_scale: float = TAU_SCALE,
    extra: "FeedObservations | None" = None,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Gaussian conditioning of the state on every current observation, from the
    original prior: manual entries (diagonal noise, early-count skew) plus
    feed observations (correlated composition noise, absentee gap).
    """
    if not observations and (extra is None or len(extra.y) == 0):
        return model.mean.copy(), model.cov.copy()
    if not observations:
        H, y, R = extra.H, extra.y, extra.R
        return _gaussian_update(model.mean, model.cov, H, y, R)

    races = {r.race_id: r for r in model.races}
    skew_i = model.index("skew")
    H = np.zeros((len(observations), len(model.state_names)))
    y = np.empty(len(observations))
    r = np.empty(len(observations))
    for i, o in enumerate(observations):
        race = races[o.race_id]
        H[i] = _loading_vector(model, race)
        y[i] = o.share - race.const
        if o.is_final:
            r[i] = FINAL_TAU**2
            continue
        f = 0.0 if o.fraction is None else o.fraction
        H[i, skew_i] = 1 - f
        r[i] = tau_floor**2 + tau_scale**2 * (1 - f) ** 2

    R = np.diag(r)
    if extra is not None and len(extra.y):
        H = np.vstack([H, extra.H])
        y = np.concatenate([y, extra.y])
        R = np.block([[R, np.zeros((len(r), len(extra.y)))], [np.zeros((len(extra.y), len(r))), extra.R]])
    return _gaussian_update(model.mean, model.cov, H, y, R)


def _gaussian_update(m0, P, H, y, R) -> tuple[np.ndarray, np.ndarray]:
    S = H @ P @ H.T + R
    K = np.linalg.solve(S, H @ P).T
    m = m0 + K @ (y - H @ m0)
    P_post = P - K @ H @ P
    return m, (P_post + P_post.T) / 2


@dataclass
class ElectionNightResult:
    as_of: pd.Timestamp
    environment: tuple[float, float]           # posterior (mean, sd) of E
    environment_prior: tuple[float, float]
    skew: tuple[float, float]
    races: pd.DataFrame
    chambers: pd.DataFrame
    seat_counts: dict[str, np.ndarray]
    n_observations: int
    warnings: list[str]
    absentee_gap: tuple[float, float] = (ABSENTEE_GAP_MEAN, ABSENTEE_GAP_SD)
    outstanding: pd.DataFrame | None = None    # per county, governor race
    feed: dict | None = None                   # generated time, test flag, file


def _call_label(row) -> str:
    if row["fixed_reason"]:
        reason = row["fixed_reason"]
        if reason.startswith("holdover"):
            return f"HOLDOVER {row['winner']}"
        if reason.startswith("uncontested"):
            return f"UNCONTESTED {row['winner']}"
        return f"BALLOT {row['winner']}"
    if row["is_final"]:
        return f"FINAL {'D' if row['win_prob'] > 0.5 else 'R'}"
    guarded = bool(row.get("call_guard", False))
    if row["win_prob"] >= CALL_THRESHOLD and not guarded:
        return "CALL D"
    if row["win_prob"] <= 1 - CALL_THRESHOLD and not guarded:
        return "CALL R"
    if row["win_prob"] >= 0.75:
        return "likely D"
    if row["win_prob"] <= 0.25:
        return "likely R"
    return "too close"


def _statewide_simulation(model: PriorModel, draws: np.ndarray, fo: "FeedObservations | None",
                          rng: np.random.Generator) -> dict[str, np.ndarray]:
    """
    Final two-party D share of each statewide race, per draw: sum over
    counties of N_c * share_c / sum N_c, with N_c drawn around the expected
    turnout (statewide factor g plus county noise). Counted votes are in each
    county's share through the conditioning; what is left is how the
    outstanding vote breaks and how much of it there is.
    """
    ci = model.county
    fips = ci.counties["fips"].tolist()
    idx = {n: i for i, n in enumerate(model.state_names)}
    lean = ci.counties["lean"].to_numpy()
    base = ci.counties["base_votes"].to_numpy()
    n = draws.shape[0]
    if fo is not None:
        t = fo.turnout.set_index("fips").reindex(fips)
        g = rng.normal(fo.g[0], fo.g[1], size=(n, 1))
        own = t["has_own"].to_numpy()
        n_hat = t["n_hat"].to_numpy()
        noise = rng.standard_normal((n, len(fips)))
        n_draw = np.where(own, n_hat + noise * t["n_sd"].to_numpy(),
                          base * g * (1 + COUNTY_TURNOUT_SD * noise))
        n_draw = np.maximum(n_draw, t["t_gov"].to_numpy())
    else:
        g = rng.normal(TURNOUT_PRIOR_MEAN, TURNOUT_PRIOR_SD, size=(n, 1))
        n_draw = base * g * (1 + COUNTY_TURNOUT_SD * rng.standard_normal((n, len(fips))))
    n_draw = np.maximum(n_draw, 1.0)
    u = draws[:, [idx[f"u_{f}"] for f in fips]]
    out = {}
    for sr in STATEWIDE_RACES:
        v = draws[:, [idx[f"v_{sr.key}_{f}"] for f in fips]]
        share = (draws[:, [idx["E"]]] + draws[:, [idx[sr.dev_state]]] + model.race_const[sr.key]
                 + lean + u + v)
        out[sr.key] = (share * n_draw).sum(axis=1) / n_draw.sum(axis=1)
    out["_county_gov_share"] = (draws[:, [idx["E"]]] + draws[:, [idx["dev_gov"]]] + lean + u
                                + draws[:, [idx[f"v_governor_{f}"] for f in fips]])
    out["_n_draw"] = n_draw
    return out


def _outstanding_table(model: PriorModel, fo: "FeedObservations", sim: dict, snap) -> pd.DataFrame:
    """Per county: counted vs expected governor vote and how the outstanding vote should break."""
    t = fo.turnout.copy()
    gov = snap.statewide[snap.statewide["race"] == "governor"].set_index("fips")
    t["dem_counted"] = gov["dem_votes"].reindex(t["fips"]).fillna(0).to_numpy()
    t["rep_counted"] = gov["rep_votes"].reindex(t["fips"]).fillna(0).to_numpy()
    share_mean = sim["_county_gov_share"].mean(axis=0)
    n_mean = sim["_n_draw"].mean(axis=0)
    counted = t["dem_counted"] + t["rep_counted"]
    remaining = np.maximum(n_mean - counted, 0)
    dem_final = share_mean * n_mean
    dem_remaining = np.clip(dem_final - t["dem_counted"], 0, remaining)
    t = t.assign(
        expected_votes=n_mean,
        counted_votes=counted,
        remaining_votes=remaining,
        counted_d_share=np.where(counted > 0, t["dem_counted"] / counted.replace(0, np.nan), np.nan),
        remaining_d_share=np.where(remaining > 0, dem_remaining / np.maximum(remaining, 1), np.nan),
        remaining_net_d=2 * dem_remaining - remaining,
        projected_d_share=share_mean,
    )
    cols = ["fips", "name", "precinct_frac", "ballots", "absentee", "expected_votes", "counted_votes",
            "remaining_votes", "counted_d_share", "remaining_d_share", "remaining_net_d", "projected_d_share",
            "complete"]
    return t[cols].sort_values("remaining_votes", ascending=False).reset_index(drop=True)


def run_election_night(
    model: PriorModel,
    frames: dict[str, pd.DataFrame] | None = None,
    *,
    feed=None,
    n_sims: int = 10_000,
    seed: int = 42,
) -> ElectionNightResult:
    """
    Condition the prior on the current returns and summarize every race and
    chamber. `feed` is an enr_feed.FeedSnapshot; when given, it is the source
    for every race it covers and manual entries for those races are ignored.
    """
    observations, uncontested, warnings = parse_returns(model, frames)
    fo = None
    feed_meta = None
    if feed is not None:
        fo = feed_observations(model, feed)
        overlap = sorted({o.race_id for o in observations} & fo.race_ids)
        if overlap:
            warnings.append(f"Manual entries ignored where the SOS feed has data: {', '.join(overlap)}")
            observations = [o for o in observations if o.race_id not in fo.race_ids]
        for rid, winner in fo.uncontested.items():
            uncontested.setdefault(rid, winner)
        if feed.is_test:
            warnings.append("SOS FEED IS TEST/MOCK DATA (IsTest or mock-election notes). Not real results.")
        if absentee_missing(feed):
            warnings.append("SOS feed reports NO absentee ballots in any county while ballots are being counted; "
                            "using the generic early-count skew instead of the absentee mix (--early-skew sets its prior).")
        warnings.extend(feed.warnings)
        from src.enr_feed import self_check
        feed_meta = {"generated": feed.generated, "is_test": feed.is_test,
                     "file": str(feed.source) if feed.source else None, "g": fo.g,
                     "self_check": self_check(feed)}
    m, P = condition(model, observations, extra=fo)

    obs_by = {o.race_id: o for o in observations}
    feed_by: dict[str, dict] = {}
    if fo is not None:
        for rec in fo.records:
            agg = feed_by.setdefault(rec["race_id"], {"dem": 0, "rep": 0, "expected": rec.get("expected"),
                                                      "fraction": rec.get("fraction")})
            agg["dem"] += rec["dem"]
            agg["rep"] += rec["rep"]

    uncertain = [r for r in model.races if r.fixed_winner is None and r.race_id not in uncontested]
    H = np.array([_loading_vector(model, r) for r in uncertain])
    c = np.array([r.const for r in uncertain])
    share_mean = c + H @ m
    share_var = np.einsum("ij,jk,ik->i", H, P, H)
    share_sd = np.sqrt(np.clip(share_var, 1e-12, None))
    win_prob = 1 - norm.cdf((0.5 - share_mean) / share_sd)
    stats = {r.race_id: (mu, sd, wp, mu - 1.2816 * sd, mu + 1.2816 * sd)
             for r, mu, sd, wp in zip(uncertain, share_mean, share_sd, win_prob)}

    rng = np.random.default_rng(seed)
    L = np.linalg.cholesky(P + np.eye(len(m)) * 1e-10)
    draws = m + rng.standard_normal((n_sims, len(m))) @ L.T

    # Statewide races: simulate turnout-weighted county results.
    sim = _statewide_simulation(model, draws, fo, rng)
    sw_total_expected = float(sim["_n_draw"].mean(axis=0).sum())
    for sr in STATEWIDE_RACES:
        f = sim[sr.key]
        stats[f"statewide:{sr.key}"] = (float(f.mean()), float(f.std()), float((f > 0.5).mean()),
                                        float(np.quantile(f, 0.1)), float(np.quantile(f, 0.9)))

    rows = []
    for race in model.races:
        o = obs_by.get(race.race_id)
        fd = feed_by.get(race.race_id)
        fixed = race.fixed_winner or uncontested.get(race.race_id)
        reason = race.fixed_reason or ("uncontested" if race.race_id in uncontested else None)
        if fixed:
            mu, sd, wp, lo, hi = (np.nan, np.nan, 1.0 if fixed == "D" else 0.0, np.nan, np.nan)
        else:
            mu, sd, wp, lo, hi = stats[race.race_id]
        live_dem = live_rep = live_frac = np.nan
        source = ""
        guard = False
        if fd is not None:
            live_dem, live_rep = fd["dem"], fd["rep"]
            expected = sw_total_expected if race.office == "statewide" else fd["expected"]
            live_frac = min((live_dem + live_rep) / expected, 1.0) if expected else np.nan
            source = "sos_feed"
            outstanding = max(expected - (live_dem + live_rep), RESIDUAL_FRACTION * expected) if expected else 0
            guard = abs(live_dem - live_rep) < CALL_GUARD * outstanding
        elif o is not None:
            live_dem, live_rep, live_frac, source = o.dem_votes, o.rep_votes, o.fraction, o.fraction_source
        elif race.office == "statewide" and fo is not None:
            guard = True  # statewide race with no counted votes yet
        rows.append({
            "race_id": race.race_id,
            "office": race.office,
            "district": race.district,
            "label": race.label,
            "current_holder": race.current_holder,
            "open_seat_2026": race.open_seat,
            "proj_d_share": mu,
            "proj_sd": sd,
            "proj_low80": lo,
            "proj_high80": hi,
            "win_prob": wp,
            "winner": fixed or ("D" if wp > 0.5 else "R"),
            "fixed_reason": reason or "",
            "live_dem_votes": live_dem,
            "live_rep_votes": live_rep,
            "live_d_share": live_dem / (live_dem + live_rep) if (live_dem + live_rep) > 0 else np.nan,
            "live_fraction": live_frac,
            "fraction_source": source,
            "is_final": bool(o.is_final) if o else False,
            "call_guard": guard,
        })
    races_df = pd.DataFrame(rows)
    races_df["call"] = races_df.apply(_call_label, axis=1)

    # Seat distributions: joint draws of the uncertain legislative races plus fixed outcomes.
    shares = c + draws @ H.T
    seat_counts: dict[str, np.ndarray] = {}
    chamber_rows = []
    for oname in LEGISLATIVE_OFFICES:
        cols = [i for i, r in enumerate(uncertain) if r.office == oname]
        sim_wins = (shares[:, cols] >= 0.5).sum(axis=1)
        fixed_d = int((races_df[(races_df["office"] == oname) & (races_df["fixed_reason"] != "")]["winner"] == "D").sum())
        counts = sim_wins + fixed_d
        seat_counts[oname] = counts
        office_races = races_df[races_df["office"] == oname]
        row = {
            "office": oname,
            "mean_d_seats": float(counts.mean()),
            "median_d_seats": int(np.median(counts)),
            "p10_seats": int(np.percentile(counts, 10)),
            "p90_seats": int(np.percentile(counts, 90)),
            "called_d": int(office_races["call"].str.match(r"(CALL|FINAL|HOLDOVER|UNCONTESTED|BALLOT) D").sum()),
            "called_r": int(office_races["call"].str.match(r"(CALL|FINAL|HOLDOVER|UNCONTESTED|BALLOT) R").sum()),
            "n_reporting": int(office_races["live_dem_votes"].notna().sum()),
        }
        for thr, label in CHAMBER_THRESHOLDS[oname]:
            row[f"P({label})"] = float((counts >= thr).mean())
        chamber_rows.append(row)

    e_i, s_i, d_i = model.index("E"), model.index("skew"), model.index("delta")
    return ElectionNightResult(
        as_of=pd.Timestamp.now(),
        environment=(float(m[e_i]), float(np.sqrt(P[e_i, e_i]))),
        environment_prior=model.environment_prior,
        skew=(float(m[s_i]), float(np.sqrt(P[s_i, s_i]))),
        races=races_df,
        chambers=pd.DataFrame(chamber_rows),
        seat_counts=seat_counts,
        n_observations=len(observations) + (len(fo.y) if fo is not None else 0),
        warnings=warnings,
        absentee_gap=(float(m[d_i]), float(np.sqrt(P[d_i, d_i]))),
        outstanding=_outstanding_table(model, fo, sim, feed) if fo is not None else None,
        feed=feed_meta,
    )


# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

def write_outputs(result: ElectionNightResult, out_dir: Path = OUT_DIR) -> dict[str, Path]:
    """Latest race and chamber tables, plus one appended history row per run."""
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "races": out_dir / "latest_races.csv",
        "chambers": out_dir / "latest_chambers.csv",
        "history": out_dir / "history.csv",
    }
    result.races.to_csv(paths["races"], index=False, float_format="%.4f")
    result.chambers.to_csv(paths["chambers"], index=False, float_format="%.4f")
    if result.outstanding is not None:
        paths["outstanding"] = out_dir / "latest_outstanding.csv"
        result.outstanding.to_csv(paths["outstanding"], index=False, float_format="%.4f")

    sw = result.races[result.races["office"] == "statewide"].set_index("race_id")
    hist = {
        "timestamp": result.as_of.isoformat(timespec="seconds"),
        "n_observations": result.n_observations,
        "environment": result.environment[0],
        "environment_sd": result.environment[1],
        "skew": result.skew[0],
        "absentee_gap": result.absentee_gap[0],
        "feed_generated": result.feed["generated"].isoformat() if result.feed else "",
        **{f"wp_{rid.split(':')[1]}": sw.loc[rid, "win_prob"] for rid in sw.index},
        **{f"seats_{r.office}": r.mean_d_seats for r in result.chambers.itertuples()},
    }
    hist_df = pd.DataFrame([hist])
    hist_df.to_csv(paths["history"], mode="a", header=not paths["history"].exists(),
                   index=False, float_format="%.4f")
    return paths
