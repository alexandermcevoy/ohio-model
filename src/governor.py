"""
governor.py — degenerate "office" treatment for the statewide Governor race.

The Governor is one statewide race — there are no 99 districts to score, no
composite lean to compute per-district, no MC over a chamber. The OfficeConfig
abstraction still applies (so that CLI commands and downstream code can
dispatch uniformly across the four offices), but most operations collapse to
trivial single-number outputs.

For Phase A.10 (close-out), this module provides a *minimal baseline*:
- `compute_statewide_history(...)` — pull statewide D% from each gov cycle in
  the block backbone (2010, 2014, 2018, 2022).
- `build_baseline_distribution(...)` — naive mean + sigma over those cycles.
  Used as a placeholder posterior over 2026 statewide D%.

Phase B will replace `build_baseline_distribution` with a polls + fundamentals
nowcast. The shape of the output (a sample of statewide D% draws, plus
summary stats) stays the same so that downstream consumers don't need to
change.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
PROCESSED = REPO_ROOT / "data" / "processed"


# ---------------------------------------------------------------------------
# Historical statewide D% from block backbone
# ---------------------------------------------------------------------------

GOVERNOR_CYCLES = (2010, 2014, 2018, 2022)


def compute_statewide_history(
    block_votes_dir: Path | None = None,
) -> pd.DataFrame:
    """
    Read each `block_votes_{year}.parquet` for gov years and compute
    statewide two-party D share.

    Returns
    -------
    DataFrame with columns: year, race, d_votes, r_votes, total_two_party,
                            statewide_d_share
    """
    if block_votes_dir is None:
        block_votes_dir = PROCESSED

    rows = []
    for year in GOVERNOR_CYCLES:
        path = block_votes_dir / f"block_votes_{year}.parquet"
        if not path.exists():
            continue
        df = pd.read_parquet(path)
        race_name = f"gov_{year}"
        sub = df[df["race"] == race_name]
        if sub.empty:
            continue
        d = float(sub["d_votes"].sum())
        r = float(sub["r_votes"].sum())
        tp = d + r
        rows.append({
            "year": year,
            "race": race_name,
            "d_votes": d,
            "r_votes": r,
            "total_two_party": tp,
            "statewide_d_share": d / tp if tp > 0 else float("nan"),
        })

    return pd.DataFrame(rows).sort_values("year").reset_index(drop=True)


# ---------------------------------------------------------------------------
# Baseline distribution (Phase A placeholder; Phase B will replace)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class StatewideBaseline:
    """Naive mean + sigma over historical statewide D%."""
    n_cycles: int
    mean_d_share: float
    std_d_share: float
    cycles: tuple[int, ...]
    method: str  # "historical_mean" for now; Phase B: "poll_average", "bayes_nowcast", etc.


def build_baseline_distribution(
    history_df: pd.DataFrame,
    n_samples: int = 10_000,
    seed: int = 42,
) -> tuple[StatewideBaseline, np.ndarray]:
    """
    Build a baseline posterior over 2026 statewide D% from prior gov cycles.

    PHASE A PLACEHOLDER: assumes draws are i.i.d. normal with mean and sigma
    estimated from the historical cycles. This is deliberately wide
    (sigma ~7 pts with only 4 cycles, including the 2014 Strickland landslide
    loss). Phase B replaces this with a polls + fundamentals model that will
    shrink the posterior substantially as the election approaches.

    Returns
    -------
    StatewideBaseline summary, and an array of sampled statewide D% draws.
    """
    if history_df.empty:
        raise ValueError("compute_statewide_history returned no rows")

    shares = history_df["statewide_d_share"].to_numpy()
    mu = float(np.mean(shares))
    sd = float(np.std(shares, ddof=1)) if len(shares) > 1 else 0.05

    summary = StatewideBaseline(
        n_cycles=len(shares),
        mean_d_share=mu,
        std_d_share=sd,
        cycles=tuple(int(y) for y in history_df["year"].tolist()),
        method="historical_mean",
    )

    rng = np.random.default_rng(seed)
    samples = rng.normal(loc=mu, scale=sd, size=n_samples)
    # Clip to feasible range
    samples = np.clip(samples, 0.20, 0.80)
    return summary, samples


def summarize_distribution(samples: np.ndarray) -> dict[str, float]:
    """Standard summary stats for a posterior over statewide D%."""
    return {
        "mean": float(np.mean(samples)),
        "median": float(np.median(samples)),
        "std": float(np.std(samples)),
        "p10": float(np.quantile(samples, 0.10)),
        "p25": float(np.quantile(samples, 0.25)),
        "p75": float(np.quantile(samples, 0.75)),
        "p90": float(np.quantile(samples, 0.90)),
        "prob_d_win": float(np.mean(samples > 0.50)),
    }
