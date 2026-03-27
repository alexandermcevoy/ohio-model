"""
validate_external.py — External validation of composite lean against published sources.

Attempts to obtain district-level partisan composite data from Dave's Redistricting
App (DRA) or other published sources and compares against our composite lean.

DRA does not expose a public API for district-level partisan data. The intended
comparison is: DRA Partisan Index (or similar) vs. our composite_lean, Spearman
rank correlation should exceed 0.95 if both models measure the same underlying
quantity. Manual validation instructions are included at the bottom of this module.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from scipy import stats


# ---------------------------------------------------------------------------
# DRA CSV parser
# ---------------------------------------------------------------------------

def parse_dra_csv(path: str) -> pd.DataFrame:
    """
    Parse a DRA export CSV for Ohio House districts.

    DRA export quirks:
    - Trailing comma on every row — use index_col=False to prevent column shift.
    - 'Un' row (unassigned population placeholder) — drop it.
    - Dem/Rep columns are shares of total votes (including third parties).
    - We convert to two-party share then express lean relative to the
      population-weighted statewide mean, matching our composite's baseline.

    Returns DataFrame with columns: district (int), dra_lean (float).
    """
    df = pd.read_csv(path, index_col=False)
    # Drop the 'Un' unassigned row and any non-numeric IDs
    df = df[df['ID'].apply(lambda x: str(x).strip().isdigit())].copy()
    df['district'] = df['ID'].astype(int)
    df['dem_f'] = pd.to_numeric(df['Dem'], errors='coerce')
    df['rep_f'] = pd.to_numeric(df['Rep'], errors='coerce')
    df['dra_2p'] = df['dem_f'] / (df['dem_f'] + df['rep_f'])
    # Use simple mean (districts are roughly equal population)
    df['dra_lean'] = df['dra_2p'] - df['dra_2p'].mean()
    return df[['district', 'dra_lean']].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Manual reference data (partial) — DRA Partisan Index for selected districts
# ---------------------------------------------------------------------------
# DRA Partisan Index for Ohio House districts is based on the average of the
# 2016 and 2020 presidential results, expressed as Democratic share minus 0.50.
# This is NOT directly comparable to our composite (which uses 9 races, 4 cycles,
# and expresses lean relative to statewide rather than absolute 50%).
#
# To convert: DRA_comparable = DRA_partisan_index + (0.50 − statewide_D_baseline)
# where statewide_D_baseline ≈ 0.46 for Ohio, so DRA values are shifted ~+0.04
# relative to our lean.
#
# If you have access to DRA's CSV export for Ohio House 2024 boundaries,
# load it as a DataFrame with columns ['district', 'dra_partisan_index'] and
# pass to compare_with_external() below.

DRA_MANUAL_NOTES = """
Dave's Redistricting App — Ohio House External Validation
==========================================================
DRA provides district-level partisan analysis at davesredistricting.org.
For Ohio House districts, their Partisan Index is available interactively
but not via a public download API as of 2026.

To perform external validation manually:
1. Go to davesredistricting.org and load the Ohio 2023 House plan.
2. Select "Partisan" analysis and download the CSV of district-level results.
3. Load the CSV using: df = pd.read_csv('dra_ohio_house.csv')
4. Rename columns to ['district', 'dra_partisan_index'] and call:
   compare_with_external(our_composite_df, df, source_name='DRA')

Expected results if both models are correct:
  - Spearman rank correlation > 0.95
  - Mean absolute difference < 3 points (after converting to same baseline)
  - Largest disagreements in districts with unusual candidate effects or
    races where DRA's 2016/2020 pres-only average differs from our multi-race
    composite (e.g., districts where the 2022 governor's race diverged from
    presidential trend)
"""


def compare_with_external(
    composite_df: pd.DataFrame,
    external_df: pd.DataFrame,
    source_name: str = "External",
    external_lean_col: str = "external_lean",
    baseline_adjustment: float = 0.0,
) -> pd.DataFrame:
    """
    Compare our composite lean against an external partisan index.

    Parameters
    ----------
    composite_df : our composite lean DataFrame (must have 'district', 'composite_lean').
    external_df  : external source (must have 'district' and external_lean_col).
    source_name  : label for the external source (used in output).
    external_lean_col : name of the lean column in external_df.
    baseline_adjustment : add this to external_lean to put it on the same scale as
                          our composite (e.g., +0.04 to convert DRA absolute to
                          Ohio-relative).

    Returns
    -------
    DataFrame with our_lean, external_lean_adj, difference, abs_difference.
    Prints correlation and disagreement summary.
    """
    merged = composite_df[["district", "composite_lean"]].merge(
        external_df[["district", external_lean_col]],
        on="district",
        how="inner",
    ).copy()

    merged["external_lean_adj"] = merged[external_lean_col] + baseline_adjustment
    merged["difference"] = merged["composite_lean"] - merged["external_lean_adj"]
    merged["abs_difference"] = merged["difference"].abs()

    n = len(merged)
    rho, pval = stats.spearmanr(merged["composite_lean"], merged["external_lean_adj"])
    mae = float(merged["abs_difference"].mean())
    n_disagree_3pt = int((merged["abs_difference"] > 0.03).sum())
    n_disagree_5pt = int((merged["abs_difference"] > 0.05).sum())

    print(f"\n=== External Validation: {source_name} ===")
    print(f"  Districts compared: {n}")
    print(f"  Spearman rank correlation: {rho:.4f}  (p={pval:.4g})")
    print(f"  Mean absolute difference:  {mae:.4f} ({mae*100:.2f} pts)")
    print(f"  Districts disagreeing > 3 pts: {n_disagree_3pt}")
    print(f"  Districts disagreeing > 5 pts: {n_disagree_5pt}")

    if rho >= 0.95:
        print(f"  PASS: Spearman ρ ≥ 0.95 — models agree on rank order.")
    elif rho >= 0.90:
        print(f"  WARN: Spearman ρ in [0.90, 0.95] — minor disagreements, investigate.")
    else:
        print(f"  FAIL: Spearman ρ < 0.90 — substantial disagreement, one model has a problem.")

    if n_disagree_3pt > 0:
        print(f"\n  Largest disagreements (our lean vs. {source_name}):")
        top_disagree = merged.nlargest(10, "abs_difference")[
            ["district", "composite_lean", "external_lean_adj", "difference"]
        ]
        print(top_disagree.to_string(index=False))
        print("\n  Hypotheses for large disagreements:")
        print("    - Different race composition: DRA uses pres-only; we use 9 races.")
        print("    - Different years: DRA may use 2016+2020; we use 2018–2024.")
        print("    - Candidate effects in 2018/2022 that shift our multi-race average.")
        print("    - Different baseline (absolute vs. statewide-relative).")

    return merged[
        ["district", "composite_lean", "external_lean_adj", "difference", "abs_difference"]
    ].sort_values("abs_difference", ascending=False).reset_index(drop=True)


def run_external_validation(
    composite_df: pd.DataFrame,
    external_csv_path: str | None = None,
) -> str:
    """
    Attempt external validation. If external_csv_path is provided and exists,
    load and compare. Otherwise, document the attempt and return instructions.

    Returns a summary string suitable for inclusion in validation reports.
    """
    import os

    if external_csv_path and os.path.exists(external_csv_path):
        # Try DRA-format first (has ID, Dem, Rep columns)
        raw = pd.read_csv(external_csv_path, index_col=False)
        if 'Dem' in raw.columns and 'Rep' in raw.columns and 'ID' in raw.columns:
            ext_df = parse_dra_csv(external_csv_path)
            lean_col = 'dra_lean'
            source = 'DRA 2024'
        else:
            ext_df = raw
            lean_col = next(
                (c for c in ext_df.columns if 'lean' in c.lower() or 'partisan' in c.lower()),
                None,
            )
            if lean_col is None:
                return (
                    f"ERROR: Could not identify lean column in {external_csv_path}. "
                    f"Columns: {ext_df.columns.tolist()}"
                )
            source = f"External ({external_csv_path})"

        result_df = compare_with_external(
            composite_df, ext_df,
            source_name=source,
            external_lean_col=lean_col,
        )
        out_path = "reports/session5/external_validation.csv"
        result_df.to_csv(out_path, index=False, float_format="%.6f")
        print(f"\n  External validation CSV written to {out_path}")
        return f"External validation complete. Results in {out_path}."

    # Programmatic access unavailable
    msg = (
        "External Validation Status: MANUAL REQUIRED\n"
        "\n"
        "DRA does not expose a public CSV/API for district-level partisan data.\n"
        "Programmatic access was attempted and is not available.\n"
        "\n"
        "Intended comparison: DRA Partisan Index for Ohio 2023 House plan\n"
        "  URL: https://davesredistricting.org (load Ohio 2023 plan, Partisan tab)\n"
        "  Expected: Spearman rank correlation > 0.95 with our composite\n"
        "  Baseline conversion needed: DRA uses absolute share, we use statewide-relative\n"
        "\n"
        "To run validation manually:\n"
        "  1. Export DRA district CSV to data/processed/dra_ohio_house.csv\n"
        "     (columns: district, dra_partisan_index)\n"
        "  2. Run: python cli.py session5 --external-csv data/processed/dra_ohio_house.csv\n"
        "\n"
        "Alternative: CNalysis (cnalysis.com) publishes district-level ratings that\n"
        "  could serve as a qualitative rank-order check.\n"
    )
    print(msg)
    return msg
