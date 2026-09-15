# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Running the pipeline

```bash
# Session 1 — VEST 2020 presidential lean only
python cli.py run \
  --precincts data/shapefiles/oh_2020/oh_2020.shp \
  --districts "data/shapefiles/Corrected Sept 29 2023 Unified Bipartisan Redistricting Plan HD SHP.shp" \
  --output reports/session1/oh_house_partisan_lean_2020.csv

# Session 2 — multi-race composite lean (all SOS years)
python cli.py composite \
  --precincts data/shapefiles/oh_2020/oh_2020.shp \
  --districts "data/shapefiles/Corrected Sept 29 2023 Unified Bipartisan Redistricting Plan HD SHP.shp"

# Session 3 — district classification, targeting, scenarios
python cli.py classify

# Session 4 — ACS demographics, pop-weighted crosswalk, regression, classification update
python cli.py session4

# Session 8 — voter file ingestion, scoring, district universe, targeting merge
python cli.py voters --build                       # full pipeline (~10 min, ~2–3 GB RAM)
python cli.py voters --build --force               # force rebuild of parquet

# Voter file queries (after --build)
python cli.py voters                               # statewide summary
python cli.py voters --targets-only                # all pickup targets with mob/pers counts
python cli.py voters --district 52                 # single district voter universe
python cli.py voters --district 52 --target mobilization --export contacts_d52.csv

# Session 8 — probabilistic scenarios + resource allocation
python cli.py session8                             # full pipeline (all outputs)
python cli.py simulate                             # probabilistic sweep (40–55%)
python cli.py simulate --statewide-d 48.0          # single-point summary
python cli.py simulate --with-incumbency           # apply literature incumbency adjustment
python cli.py invest --statewide-d 48.0 --target 40  # path-to-target optimizer
python cli.py win-prob --district 52               # single district win prob curve
python cli.py win-prob --statewide-d 48.0          # all districts at one environment

# Session 10 — Census Block Backbone (2010–2024 historical depth)
python cli.py backbone --full                  # all steps: geometry, maps, surfaces, validate
python cli.py backbone --build-geometry        # step 1: load Census 2020 blocks (~220k)
python cli.py backbone --build-maps            # step 2: block-precinct + block-district maps
python cli.py backbone --build-surfaces        # step 3: disaggregate votes to blocks (8 years)
python cli.py backbone --validate              # step 4: round-trip validation vs existing composite
python cli.py backbone --full --force          # rebuild all cached parquets from scratch

# Trend analysis — per-district partisan trend from 8 election cycles
python cli.py trends                           # compute + merge into targeting CSV
python cli.py trends --force                   # recompute from block vote surfaces

# Session 12 — historical backtest (pre-2024 → predict 2024)
python cli.py backtest                         # full out-of-sample backtest

# Session 14 — multi-office expansion (Phase A: Senate, US House)
# Top-level commands continue to default to House. Each office also has its
# own subcommand group; `house <cmd>` is an alias of `<cmd>` for symmetry.
python cli.py house composite                  # = python cli.py composite
python cli.py senate backbone --build-map      # build block→senate-district map
python cli.py senate composite                 # composite lean for 33 senate districts
python cli.py senate classify                  # tier + targeting + scenarios
python cli.py senate simulate                  # probabilistic scenarios + investment priority
python cli.py congress backbone --build-map    # build block→CD map (Oct 2025 plan)
python cli.py congress composite               # composite lean for 15 CDs
python cli.py congress classify                # tier + targeting + scenarios
python cli.py congress simulate                # probabilistic scenarios + investment priority
python cli.py multi backbone --build-all-maps  # build block-district maps for all 3 offices
python cli.py senate report                    # one-page office summary PDF (senate)
python cli.py congress report                  # one-page office summary PDF (congress)
python cli.py multi report                     # generate summary PDFs for all 3 offices

# Individual modules can be imported and called independently:
python -c "from src.ingest_sos import load_sos_file; f = load_sos_file('data/raw/statewide-races-by-precinct.xlsx')"
```

## Installing dependencies

```bash
pip install -r requirements.txt
```

geopandas depends on GDAL. On macOS, install via conda (`conda install geopandas`) or brew (`brew install gdal`) before pip-installing.

## Architecture

The pipeline is strictly linear: **ingest → crosswalk → partisan → output**. Each step is a separate module with pure functions; `cli.py` wires them together.

- `src/ingest.py` — loads both shapefiles, reprojects to **EPSG:3735** (Ohio State Plane South), detects VEST column names dynamically via regex (no hardcoding of column names).
- `src/crosswalk.py` — runs `geopandas.overlay(how='intersection')` to split precincts at district boundaries, computes area fractions, allocates votes proportionally. This is the slowest and most error-prone step; four validation checks run automatically after.
- `src/partisan.py` — computes two-party D share, partisan lean (district minus statewide), and raw margin for every race with both D and R columns. `build_output()` renames PRE columns to friendly names (`biden_votes`, `trump_votes`, etc.) for the CSV.
- `src/validate.py` — collects validation messages from the other modules and writes `reports/session1/validation_summary.txt`.
- `cli.py` — thin Typer CLI stub; all logic lives in `src/`.

## Data conventions

- **VEST column pattern**: `G{YY}{RACE}{PARTY}{CANDIDATE}` — e.g. `G20PREDBID` = 2020 general, president, Democrat, Biden. Detected by `VEST_RACE_PATTERN` regex in `ingest.py` and `partisan.py`.
- **CRS**: everything is reprojected to EPSG:3735 before any area calculation. Never compute areas in EPSG:4326.
- **No synthetic data**: if a required file or column is missing, modules raise `FileNotFoundError` or `KeyError` immediately.
- Raw source files live in `data/raw/` and `data/shapefiles/` (gitignored). Processed outputs go to `data/processed/` and `reports/`.

## Known limitations (by design, session 1)

- Area-weighting assumes uniform voter distribution within precincts. Precincts on the state boundary will have area-fraction sums slightly < 1.0 — this is expected and flagged in the validation summary, not an error.
- CLI is a stub; demographic overlay, composite index, and visualization are deferred to later sessions.

# Ohio House Election Model

## What This Is

An analytical platform for projecting Ohio State House election outcomes and identifying Democratic pickup targets. Designed for a single analyst (me) to generate reports for Democratic campaign operatives.

Three layers:
1. **Data warehouse** — precinct-level election results mapped to current district geography, with demographic overlays. SQLite or parquet. Grows as new election cycles are ingested.
2. **Analytical models** — composite partisan lean, district classification tiers, candidate effect estimates, turnout elasticity, regression model.
3. **Reporting interface** — CLI that generates PDFs, CSVs, and district profiles. Eventually a natural language query layer via Claude API.

## Strategic Context

Ohio House is 99 seats. Current split: 65R–34D. Democrats need 50 for a majority (net +16, a heavy lift). More realistic 2026 goals: hold above supermajority-proof threshold (34+), push toward 40+ for veto sustainability. The tool identifies where marginal campaign investment is most efficient.

## Architecture

```
ohio-house-model/
├── data/
│   ├── raw/              # source files (gitignored)
│   ├── processed/        # cleaned intermediate data
│   └── shapefiles/       # geographic files
├── src/
│   ├── __init__.py
│   ├── ingest.py         # VEST shapefile loading
│   ├── ingest_sos.py     # Ohio SOS XLSX parsing
│   ├── ingest_house_results.py  # actual state house race results
│   ├── join_sos_vest.py  # SOS-to-VEST precinct matching
│   ├── crosswalk.py      # precinct-to-district spatial overlay
│   ├── partisan.py       # partisan lean computation
│   ├── composite.py      # weighted multi-race composite index
│   ├── classify.py       # district tiering and targeting (Session 3)
│   ├── demographics.py   # ACS overlay (Session 4)
│   ├── model.py          # regression model (Session 4)
│   ├── export.py         # PDF/CSV report generation (Session 5)
│   ├── query.py          # natural language interface (Session 6)
│   ├── simulate.py       # probabilistic scenarios + resource allocation (Session 8)
│   ├── scenarios.py      # uniform swing model + formatting (Session 7)
│   └── validate.py       # validation checks
├── reports/              # generated outputs
├── cli.py                # typer CLI entry point
├── requirements.txt
├── CLAUDE.md             # this file
└── README.md
```

## Vision and Roadmap

This tool is being built to become durable analytical infrastructure for Ohio Democratic state legislative targeting — not a one-off analysis. 

**Full vision and positioning:** See `VISION.md` for the strategic case, 
intended audience, and pitch framing. See `roadmap.md` for execution plan.

**Current state (v3.0 in progress):** Composite partisan lean validated against DRA (ρ = 0.9985), win-probability tiers at 3 reference environments (Cook-style), fundamentals-only uniform swing scenarios + probabilistic Monte Carlo scenarios with district-level uncertainty, GLM regression (exploratory), ACS demographics, voter file integration, resource allocation optimizer, PDF district profiles with probabilistic outlook, Streamlit GUI, historical backtest (Brier skill 0.89 vs. a coin-flip baseline; binary accuracy is not a meaningful headline — see Session 12), block backbone architecture (8 election cycles 2010–2024), CLI with multi-office subcommand groups, anomaly detection, drop-one sensitivity, full three-map redistricting filter, CLAUDE.md data schema. **Phase A multi-office expansion in progress** (Session 14): composite/classify/simulate pipeline working end-to-end for State Senate (33 districts) and US House (15 CDs, NEW Oct 2025 plan). Validation, PDF profiles, and GUI extension still pending.

**In flight (v3.0): Public-facing four-office forecast.** Strategic pivot from House-only conditional projection model (where the user supplies statewide D% via a slider) to a published forecast covering Governor 2026, State House, State Senate, and US House. Audience pivot: from private analytical tool pitched to ODP leadership → public forecast for political analytics consulting credibility + indirect ODP pressure via being the de facto Ohio forecast that reporters cite. Four phases:
- **Phase A — Office expansion foundation** (Session 14, ~80% complete): same conditional-projection model, but for 4 offices instead of 1. `OfficeConfig` abstraction, SOS parser extension for senate/congress, block-district maps for new offices, CLI subcommand groups per office.
- **Phase B — Statewide environment forecast** (governor first, MVP regression nowcast). Replace the user-supplied statewide D% slider with a probability distribution from polls + fundamentals. Polls and approval data acquired (`data/polls/`).
- **Phase C — Multi-race correlation + heterogeneous swing.** Joint posterior across all four offices with internally-consistent (correlated) outcomes; PCA-based swing decomposition; lean drift forward-projection.
- **Phase D — Public launch layer.** Brand, domain, polished public Streamlit deployment, methodology page, weekly cadence, post-election scorecard.

Full plan at `~/.claude/plans/iterative-coalescing-candle.md`.

**Deferred:** Natural language query interface (Claude API integration) — unnecessary for current stakeholder needs. Further resource allocation model refinements — current optimizer is sufficient; diminishing returns on additional complexity.

**Future (post-2026):** Campaign finance overlay, post-election scorecard publishing, 2028 cycle ingestion.

**Design principles across all sessions:**
- The composite lean (statewide race pipeline) is the anchor. It's geometry-aware, externally validated, and unaffected by house race data quality issues.
- No synthetic data. Every number traces to a public source.
- Surface uncertainty honestly. `insufficient_data` over false confidence. Confidence intervals over point estimates. Caveats on the face of the output, not in footnotes.
- The crosswalk is the moat. It's the technical infrastructure that makes every downstream analysis defensible.
- Build for durability. New election data should ingest cleanly. Redistricting should be a reaggregation, not a rebuild.

**Redistricting context:** Ohio used three different district maps in our data window. Pre-2020 (old maps), 2022 (interim maps struck down by Ohio Supreme Court but used for elections), 2024 (final maps adopted September 2023). Any cross-year comparison of house race results must account for which map was in effect. See `reports/redistricting_overlap.csv` for per-district reliability flags. The composite lean is unaffected because it routes through the spatial crosswalk, not district numbers.

**2026 targeting intelligence:** Multiple top pickup targets are open seats in 2026 due to term limits and retirements (Districts 31, 35, 39, 44, 52 confirmed). Open seat status is tracked in the targeting CSV and should be prominently surfaced in all targeting outputs. The incumbency advantage (~6-8 points) disappearing from these seats is the single largest shift in win probability available. The probabilistic model classifies targets into a **pickup portfolio** (`gui/compute.py::classify_portfolio`): Core (6 districts: 31, 35, 36, 49, 52, 64), Stretch (5: 17, 32, 39, 60, 72), and Long-Shot (6: 29, 37, 44, 73, 75, 89), as of the 2026-09-11 σ recalibration (before it: 7 Core incl. 17, 5 Stretch, 7 Long-Shot). HD-64 has no Democratic nominee in 2026 (`src/ballot_overrides.py`); that override is only applied inside `forecast` so far. `reports/session8/README.md` has the original portfolio write-up; its numbers predate the recalibration.

## Data Sources

### Election Results
- **VEST precinct shapefiles** — precinct polygons with election results in attribute table. Free for 2016–2020, paid subscription for 2022–2024. Source: UF Election Lab. VEST column naming convention: `G20PREDBID` = 2020 General, President, Democrat, Biden.
- **Ohio SOS precinct-level XLSX** — official results for all races, downloaded from ohiosos.gov. Covers 2016–2024. Contains statewide races (president, governor, senator, AG, SOS, auditor, treasurer) and state house races. Format varies by year — always inspect before parsing.
- **Ohio SOS state house results** — extracted from the same XLSX files. These are reported by precinct within districts, so they aggregate directly to districts without a spatial crosswalk.

### Geographic
- **Census TIGER/Line SLDL** — current Ohio House district polygons (99 districts), reflecting 2024-cycle boundaries. Downloaded from Census Bureau.
- **VEST precinct shapefiles** — also serve as the precinct geometry for the spatial crosswalk. Precinct boundaries shift between elections; each election year needs its own geometry.

### Demographic (Session 4)
- **ACS 5-year estimates** — Census API, block group level. Key variables: college attainment, median household income, racial composition, housing tenure, population density. Aggregated to districts via spatial overlay.

## Core Methodology

### Spatial Crosswalk
Precincts and state house districts are drawn independently. A crosswalk maps precincts to districts.

**Population-weighted (current implementation):** For each precinct, Census 2020 blocks are centroid-assigned to districts; the allocation fraction is the share of the precinct's block population in that district. Largest composite lean change vs. area-weighting: 0.003 points. Population weights are used in all final outputs.

All area calculations use EPSG:3735 (Ohio State Plane South). Never compute areas in geographic coordinates (EPSG:4326).

### Partisan Lean
For each district and each statewide race:
- Two-party D share = dem_votes / (dem_votes + rep_votes). Third parties excluded from denominator.
- Statewide two-party D share = same formula, statewide totals.
- Partisan lean = district two-party D share − statewide two-party D share. Positive = more D than state average.

### Composite Partisan Lean Index
Weighted average of individual race leans across multiple years and offices. Default weights:

```
2024 president:       0.20
2022 governor:        0.25
2022 statewide avg:   0.15  (mean of senator, AG, SOS, auditor, treasurer)
2020 president:       0.15
2018 governor:        0.15
2018 statewide avg:   0.10
```

Gubernatorial years weighted more heavily than presidential because turnout composition is closer to a state house election. If a race/year is unavailable, its weight is redistributed proportionally among available races. The effective weights are documented in every output.

### Candidate Effect
For contested state house races: actual D share minus (reference statewide D share for that cycle + composite lean). The reference race is the president in presidential years and the unweighted mean of AG/Auditor/SOS/Treasurer in midterms (`REFERENCE_MIDTERM_RACES` in `src/composite.py`; the governor was the midterm reference until 2026-09-11). Measures how much a candidate over/underperformed district fundamentals. Only meaningful for contested races; uncontested races are flagged but excluded from this calculation.

## Data Schema

### File Inventory

All primary analytical outputs live in `reports/` and `data/processed/`. Source files (raw XLSX, shapefiles, ACS parquets) are in `data/raw/`, `data/shapefiles/`, and `data/processed/` (parquet cache files).

| File | Type | Rows | Key join column |
|---|---|---|---|
| `reports/session3/oh_house_targeting.csv` | wide, one row/district | 99 | `district` |
| `reports/session2/oh_house_composite_lean.csv` | wide, one row/district | 99 | `district` |
| `reports/session2/oh_house_actual_results.csv` | long, one row/district-year | ~396 | `district`, `year` |
| `reports/redistricting_overlap.csv` | wide, one row/district | 99 | `district` |
| `reports/anomaly_flags.csv` | long, one row/flagged district-year | variable | `district`, `year` |
| `data/processed/drop_one_sensitivity.csv` | wide, one row/district | 99 | `district` |
| `data/processed/oh_house_demographics.csv` | wide, one row/district | 99 | `district_num` |
| `data/processed/year_baselines.json` | dict | 4 keys | year string |
| `reports/session5/external_validation.csv` | wide, one row/district | 99 | `district` |
| `reports/session3/oh_house_scenario_table.csv` | wide, one row/statewide-pct | 31 | `statewide_d_pct` |
| `data/processed/oh_house_voter_universe.csv` | wide, one row/district | 99 | `district` |
| `data/processed/voter_file_clean.parquet` | one row/active voter | ~5–6M | — |
| `data/voterfiles/SWVF_*.txt` | raw SOS voter file (4 batches) | ~7.9M total | — |
| `reports/session8/oh_house_probabilistic_scenarios.csv` | wide, one row/statewide-pct | 31 | `statewide_d_pct` |
| `reports/session8/oh_house_district_win_probs.csv` | long, one row/district-pct | 3,069 | `district`, `statewide_d_pct` |
| `reports/session8/oh_house_investment_priority.csv` | wide, one row/district | 99 | `district` |
| `reports/session8/oh_house_path_optimizer.csv` | long, one row/investment-step | variable | `priority_rank` |
| `reports/session8/oh_house_defensive_scenarios.csv` | long, one row/district-pct | variable | `district`, `statewide_d_pct` |
| `reports/session8/oh_house_district_sigma.csv` | wide, one row/district | 99 | `district` |
| `reports/session8/oh_house_forecast_seat_distribution.csv` | long, one row/simulation | 10,000 | `sim_index` |
| `reports/session8/oh_house_forecast_district_detail.csv` | wide, one row/district | 99 | `district` |
| `data/processed/block_geometry.parquet` | one row/block | ~220k | `block_geoid` |
| `data/processed/block_county_map.parquet` | one row/block | ~220k | `block_geoid` |
| `data/processed/block_precinct_map_{year}.parquet` | one row/block | ~220k | `block_geoid` |
| `data/processed/block_district_map_house_2024.parquet` | one row/block | ~220k | `block_geoid` |
| `data/processed/block_votes_{year}.parquet` | long, block × race | variable | `block_geoid`, `race` |
| `data/processed/oh_house_district_trends.csv` | wide, one row/district | 99 | `district` |
| `reports/session12/backtest_district_predictions.csv` | wide, one row/district | 99 | `district` |
| `reports/session12/backtest_composite_comparison.csv` | wide, one row/district | 99 | `district` |
| `reports/session2/oh_senate_composite_lean.csv` | wide, one row/senate district | 33 | `district` |
| `reports/session2/oh_senate_actual_results.csv` | long, one row/district-year | variable | `district`, `year` |
| `reports/session3/oh_senate_targeting.csv` | wide, one row/senate district | 33 | `district` |
| `reports/session3/oh_senate_scenario_table.csv` | wide, one row/statewide-pct | 31 | `statewide_d_pct` |
| `reports/session3/oh_senate_pickup_ladder.txt` | plain text report | — | — |
| `reports/session8/oh_senate_probabilistic_scenarios.csv` | wide, one row/statewide-pct | 31 | `statewide_d_pct` |
| `reports/session8/oh_senate_district_win_probs.csv` | long, district × pct | 1,023 | `district`, `statewide_d_pct` |
| `reports/session8/oh_senate_investment_priority.csv` | wide, one row/senate district | 33 | `district` |
| `reports/session8/oh_senate_district_sigma.csv` | wide, one row/senate district | 33 | `district` |
| `reports/session2/oh_congress_composite_lean.csv` | wide, one row/CD | 15 | `district` |
| `reports/session2/oh_congress_actual_results.csv` | long, one row/district-year | variable | `district`, `year` |
| `reports/session3/oh_congress_targeting.csv` | wide, one row/CD | 15 | `district` |
| `reports/session3/oh_congress_scenario_table.csv` | wide, one row/statewide-pct | 31 | `statewide_d_pct` |
| `reports/session3/oh_congress_pickup_ladder.txt` | plain text report | — | — |
| `reports/session8/oh_congress_probabilistic_scenarios.csv` | wide, one row/statewide-pct | 31 | `statewide_d_pct` |
| `reports/session8/oh_congress_district_win_probs.csv` | long, district × pct | 465 | `district`, `statewide_d_pct` |
| `reports/session8/oh_congress_investment_priority.csv` | wide, one row/CD | 15 | `district` |
| `reports/session8/oh_congress_district_sigma.csv` | wide, one row/CD | 15 | `district` |
| `data/processed/block_district_map_senate_2024.parquet` | one row/block | ~220k | `block_geoid` |
| `data/processed/block_district_map_congress_2026.parquet` | one row/block | ~220k | `block_geoid` |
| `data/processed/open_seats_2026_house.csv` | long, one row/open seat | 9 | `district` |
| `data/polls/governor.csv` | FiveThirtyEight long, one row/candidate-poll | 5,332 | `poll_id` |
| `data/polls/senate.csv` | FiveThirtyEight long, one row/candidate-poll | 3,990 | `poll_id` |
| `data/polls/approval_polls.csv` | FiveThirtyEight long, one row/answer-poll | 1,221 | `poll_id` |
| `data/polls/president-averages.csv` | FiveThirtyEight long, one row/topic-date-answer | 890 | — |
| `data/polls/generic_ballot.csv` | Silver Bulletin, one row/poll | 581 | `poll_id` |

The **primary analytical join** is `targeting.csv ← composite_lean.csv ← redistricting_overlap.csv`, all on `district`. The targeting CSV already contains the most-used columns from composite lean.

After running `voters --build`, the targeting CSV also contains voter-file-derived columns (see `oh_house_voter_universe.csv` schema below). The old aggregate-based `target_mode` is retained as `target_mode_aggregate`; `target_mode_voterfile` is the authoritative mode going forward.

---

### `reports/session3/oh_house_targeting.csv` — primary analytical file

| Column | Type | Description |
|---|---|---|
| `district` | int | Ohio House district number, 1–99 |
| `composite_lean` | float | Weighted composite partisan lean (D − statewide). Positive = more D. Range: −0.20 to +0.43 |
| `tier` | str | Win-probability tier at 48% statewide D (neutral midterm). Cook-style: safe_d (>95% WP) / likely_d (75–95%) / lean_d (55–75%) / tossup (45–55%) / lean_r (25–45%) / likely_r (5–25%) / safe_r (<5%) |
| `tier_46` | str | Win-probability tier at 46% statewide D (bad cycle) |
| `wp_46` | float | D win probability at 46% statewide D |
| `tier_48` | str | Win-probability tier at 48% statewide D (= `tier` column) |
| `wp_48` | float | D win probability at 48% statewide D |
| `tier_50` | str | Win-probability tier at 50% statewide D (good cycle) |
| `wp_50` | float | D win probability at 50% statewide D |
| `current_holder` | str | D or R (2024 winner) |
| `holder_matches_tier` | bool | Whether 2024 winner matches tier prediction |
| `pickup_opportunity` | bool | R-held district in competitive WP tier at 48% statewide D |
| `defensive_priority` | bool | D-held district in competitive tier |
| `flip_threshold` | float | Statewide D share needed to flip = 0.50 − composite_lean |
| `realistic_target` | bool | R-held district where flip_threshold ≤ 0.52 — achievable in a strong D year. Excludes lean_r and deeper structural long-shots from the primary pickup ladder |
| `open_seat_2026` | bool | Known open seat in 2026 (no 2024 incumbent running) |
| `open_seat_reason` | str | Why it's open, e.g. term_limited, retired |
| `incumbent_status_2026` | str | open_seat / true_incumbent / unknown |
| `current_incumbent_name` | str | Name of 2024 winner |
| `swing_sd` | float | Std dev of D two-party share across contested cycles (NaN if < 2 cycles) |
| `n_contested` | int | Number of contested house cycles in 2018–2024 (0–4) |
| `turnout_elasticity` | float | Mean pres-year house votes / mean gov-year house votes |
| `target_mode` | str | persuasion / mobilization / hybrid / structural / insufficient_data / no_data |
| `contested_2024` | bool | Was the 2024 house race contested? |
| `margin_2024` | float | D share − R share in 2024 house race (contested only) |
| `candidate_effect_2024` | float | Actual D share − (statewide baseline + composite_lean) for 2024 |
| `dem_candidate_2024` | str | 2024 Democratic candidate name from SOS |
| `rep_candidate_2024` | str | 2024 Republican candidate name from SOS |
| `gov_2018_lean` | float | 2018 governor district lean |
| `statewide_avg_2018_lean` | float | Mean of 2018 AG/Aud/SOS/Tre lean |
| `uss_2018_lean` | float | 2018 U.S. Senate lean |
| `pre_2020_lean` | float | 2020 presidential lean |
| `gov_2022_lean` | float | 2022 governor lean |
| `statewide_avg_2022_lean` | float | Mean of 2022 AG/Aud/SOS/Tre lean |
| `uss_2022_lean` | float | 2022 U.S. Senate lean |
| `pre_2024_lean` | float | 2024 presidential lean |
| `uss_2024_lean` | float | 2024 U.S. Senate lean |
| `composite_sensitivity` | float | Max lean change when any single race is dropped (from drop-one analysis) |
| `most_sensitive_race` | str | Which race causes the largest change when dropped |

---

### `reports/session2/oh_house_composite_lean.csv` — composite lean + house results, wide

| Column | Type | Description |
|---|---|---|
| `district` | int | District number |
| `composite_lean` | float | Same as targeting CSV |
| `{race}_{year}_lean` | float | One column per race, e.g. `gov_2022_lean`. Nine races total |
| `dem_votes_{year}` | int | D votes in house race (2018–2024 for each year) |
| `rep_votes_{year}` | int | R votes in house race |
| `dem_share_{year}` | float | D two-party share in house race (NaN if NaN-ed out by redistricting filter) |
| `margin_{year}` | float | D share − R share |
| `winner_{year}` | str | D / R / D_uncontested / R_uncontested / no_data |
| `contested_{year}` | bool | Was the race contested? False for redistricting-filtered years |
| `candidate_effect_{year}` | float | Actual D share − expected D share; NaN if uncontested or no baseline |

---

### `reports/session2/oh_house_actual_results.csv` — long format house results

One row per (district, year). Pre-redistricting-filter version; use `composite_lean.csv` for filtered wide format.

| Column | Type | Description |
|---|---|---|
| `year` | int | 2018, 2020, 2022, or 2024 |
| `district` | int | District number |
| `dem_votes` | int | D votes cast |
| `rep_votes` | int | R votes cast |
| `total_two_party` | int | dem_votes + rep_votes |
| `dem_share` | float | D two-party share |
| `margin` | float | D share − R share |
| `winner` | str | D / R / D_uncontested / R_uncontested |
| `contested` | bool | Both D and R candidates present |

---

### `reports/redistricting_overlap.csv` — per-district redistricting flags

| Column | Type | Description |
|---|---|---|
| `district` | int | District number |
| `jaccard_similarity` | float | Jaccard overlap between 2020 and 2022 precinct sets (old→interim) |
| `overlap_category` | str | same (≥0.70) / redrawn (0.30–0.70) / relocated (<0.30), old→interim |
| `n_precincts_2020` | int | Precinct count in 2020 SOS filing |
| `n_precincts_2022` | int | Precinct count in 2022 SOS filing |
| `n_precincts_shared` | int | Precinct names matching between 2020 and 2022 |
| `jaccard_interim_final` | float | Jaccard overlap between 2022 and 2024 precinct sets (interim→final) |
| `overlap_category_interim_final` | str | same / redrawn / relocated, interim→final |
| `n_precincts_2024` | int | Precinct count in 2024 SOS filing |
| `n_precincts_shared_interim_final` | int | Precinct names matching between 2022 and 2024 |
| `years_reliable` | str | Comma-separated reliable house years, e.g. "2022,2024" or "2024" |

Summary: old→interim — 71 relocated, 15 redrawn, 13 same. Interim→final — 13 relocated, 34 redrawn, 52 same. Years reliable: 13 districts have all 4 years; 73 have 2022+2024; 13 have only 2024.

---

### `reports/anomaly_flags.csv` — flagged house result outliers

Populated by `src/validate.py::detect_anomalies()`. Currently 2 rows (both 2022, both redistricting_artifact). There were 10 before the 2026-09-11 reference-race change, which moved the 2022 baseline up 3.2 pts and shrank 2022 residuals accordingly.

| Column | Type | Description |
|---|---|---|
| `district` | int | District number |
| `year` | int | Election year of the flagged observation |
| `composite_lean` | float | District composite lean |
| `expected_d_share` | float | statewide_baseline + composite_lean |
| `actual_d_share` | float | Actual house D two-party share |
| `residual` | float | actual − expected (positive = over-performed) |
| `abs_residual` | float | |residual| |
| `severity` | str | high (>15 pts) or moderate (>10 pts) |
| `auto_explanation` | str | redistricting_artifact / nominal_candidate / possible_open_seat / unexplained |
| `contested` | bool | Was the race contested? |

---

### `data/processed/drop_one_sensitivity.csv` — composite robustness by district

| Column | Type | Description |
|---|---|---|
| `district` | int | District number |
| `composite_lean_full` | float | Full 9-race composite lean |
| `composite_lean_drop_{race}{year}` | float | Composite recomputed without that race (9 columns) |
| `max_change` | float | Max |Δ| across all race exclusions |
| `most_sensitive_to` | str | Race whose exclusion causes the largest change, e.g. "gov2022" |

---

### `data/processed/year_baselines.json`

```json
{"2018": 0.4772, "2020": 0.4592, "2022": 0.4067, "2024": 0.4434}
```

Reference statewide two-party D share each cycle: president in presidential years, mean of AG/Auditor/SOS/Treasurer in midterms (`src/composite.py::compute_year_baselines`). Used to compute expected D share in anomaly detection and candidate effects. Until 2026-09-11 midterms used the governor (2018: 0.4807, 2022: 0.3746).

---

### `data/processed/oh_house_demographics.csv` — ACS district-level demographics

Key columns (raw ACS alloc columns also present but rarely needed directly):

| Column | Type | Description |
|---|---|---|
| `district_num` | int | District number (join key) |
| `total_pop` | float | ACS 2023 total population |
| `white_pct` | float | White non-Hispanic share |
| `black_pct` | float | Black alone share |
| `hispanic_pct` | float | Hispanic/Latino share |
| `college_pct` | float | Adults 25+ with bachelor's or higher |
| `owner_occ_pct` | float | Owner-occupied housing share |
| `median_income` | float | Pop-weighted mean of block-group median household incomes ($) |
| `median_age` | float | Pop-weighted mean of block-group median ages (years) |
| `pop_density` | float | Population per square mile |

Note: join key is `district_num`, not `district` — rename before merging with other files.

---

### `reports/session3/oh_house_scenario_table.csv` — seat counts at each statewide environment

| Column | Type | Description |
|---|---|---|
| `statewide_d_pct` | float | Statewide D two-party share (40.0%–55.0%, 0.5pt steps) |
| `d_seats` | int | Predicted D seats won at this environment |
| `net_change_from_current` | int | Change from current 34 D seats |
| `newly_flipped` | str | Comma-separated district numbers flipping D at this step |

---

### `data/processed/oh_house_voter_universe.csv` — voter-file district profiles (Session 8+)

One row per district. Produced by `python cli.py voters --build`.

| Column | Type | Description |
|---|---|---|
| `district` | int | District number |
| `total_active_voters` | int | Active registered voters in district |
| `inactive_voters` | int | Inactive registered voters (metadata only) |
| `n_strong_d` | int | Voters who pulled D primary 3+ times, never R |
| `n_lean_d` | int | Voters who pulled D primary at least once, never R |
| `n_strong_r` | int | Voters who pulled R primary 3+ times, never D |
| `n_lean_r` | int | Voters who pulled R primary at least once, never D |
| `n_crossover` | int | Voters who pulled both D and R primaries in different cycles |
| `n_unaffiliated` | int | Voters who never pulled a partisan primary ballot |
| `pct_strong_d` … `pct_unaffiliated` | float | Partisan shares of active voters |
| `partisan_advantage` | float | (n_strong_d + n_lean_d − n_strong_r − n_lean_r) / total_active |
| `n_high_propensity` … `n_very_low_propensity` | int | Turnout propensity tier counts |
| `n_presidential_only` | int | Voters who voted 2020/2024 generals but not 2018/2022 |
| `pct_presidential_only` | float | Share of active voters who are presidential-only |
| `turnout_2024` … `turnout_2018` | float | Share of active voters who voted in each November general |
| `turnout_dropoff` | float | turnout_2022 / turnout_2024 (< 1 = big dropoff = more mobilization opportunity) |
| `n_mobilization_targets` | int | D-leaning (strong_d or lean_d) voters who are presidential_only or low/very_low propensity |
| `pct_mobilization_targets` | float | Mobilization universe as share of active voters |
| `n_persuasion_targets` | int | Crossover/unaffiliated voters with medium or high turnout propensity |
| `pct_persuasion_targets` | float | Persuasion universe as share of active voters |
| `target_mode_voterfile` | str | mobilization / persuasion / hybrid / low_opportunity |

**Notes:**
- Partisan lean classification uses primary history 2016+ (5 even-year + odd-year primaries). Ohio has open primaries — any voter can pull any party's ballot. See Known Limitations for caveats.
- Turnout rates are computed from voter file history (who participated), not official totals. They should be close to SOS-reported turnout but not identical.
- After `voters --build`, the targeting CSV also contains these columns (voter-file prefix). The old `target_mode` is retained as `target_mode_aggregate`.

---

### `reports/session8/oh_house_probabilistic_scenarios.csv` — probabilistic scenario table (Session 9)

| Column | Type | Description |
|---|---|---|
| `statewide_d_pct` | float | Statewide D two-party share (40.0%–55.0%, 0.5pt steps) |
| `mean_d_seats` | float | Mean D seats across 10,000 simulations |
| `median_d_seats` | int | Median D seats |
| `p10_seats` | int | 10th percentile (pessimistic) |
| `p25_seats` | int | 25th percentile |
| `p75_seats` | int | 75th percentile |
| `p90_seats` | int | 90th percentile (optimistic) |
| `std_seats` | float | Standard deviation of seat distribution |
| `prob_hold_34` | float | P(D seats ≥ 34) — hold current count |
| `prob_reach_40` | float | P(D seats ≥ 40) — veto-proof threshold |
| `prob_majority` | float | P(D seats ≥ 50) — majority |

---

### `reports/session8/oh_house_investment_priority.csv` — resource allocation ranking (Session 9)

One row per district (99 rows), at a reference statewide environment (default 48%).

| Column | Type | Description |
|---|---|---|
| `district` | int | District number |
| `composite_lean` | float | For reference |
| `margin` | float | Expected margin at reference statewide D (lean + statewide - 0.50) |
| `sigma_i` | float | Independent district-level outcome noise (EB-shrunk) |
| `win_prob` | float | P(D wins) at reference environment |
| `marginal_wp` | float | dP(win)/d(lean) — "bang for buck": how much a small D lean improvement increases win probability. Highest near 50% WP (tossups), near zero for safe seats. Formula: φ(margin/σ)/σ with σ = √(σ_i² + σ_shared²) |
| `investment_rank` | int | Priority rank (1 = highest return) |
| `tier` | str | District tier for reference |
| `open_seat_2026` | bool | Open seat flag |

---

### `reports/session8/{office}_district_sigma.csv` — district outcome uncertainty

Written by `session8` / `<office> simulate`; mirrored to `gui_data/{office}/district_sigma.csv`. Every win-probability consumer (`run_simulations`, `compute_analytical_win_probs`, `classify_districts`) raises if `sigma_shared` is missing, so a stale file fails loudly instead of producing overconfident numbers.

| Column | Type | Description |
|---|---|---|
| `district` | int | District number |
| `sigma_i` | float | Independent district noise: EB-shrunk blend of `swing_sd` and the within-year candidate-effect prior (`compute_sigma_prior`) |
| `sigma_source` | str | `prior_only` or `shrunk_{n}pt` |
| `sigma_shared` | float | Year-level shift shared by every district, drawn once per simulation (`compute_shared_sigma`). Same value on every row |
| `sigma_total` | float | √(σ_i² + σ_shared²) — a district's marginal uncertainty at a known statewide environment |

---

### `data/processed/oh_house_district_trends.csv` — per-district partisan trend (2010–2024)

| Column | Type | Description |
|---|---|---|
| `district` | int | District number |
| `trend_slope` | float | Lean change per year (positive = trending D relative to state). Units: partisan lean points/year |
| `trend_r2` | float | R² of the linear fit. High = consistent trend, low = noisy/non-linear |
| `trend_shift` | float | Total lean change over the data span (slope × 14 years, 2010–2024) |
| `trend_dir` | str | trending_d / trending_r / stable (threshold: ±0.05 pts/yr) |
| `n_years` | int | Number of election years with data (max 8) |
| `lean_earliest` | float | Mean statewide-race lean in earliest year |
| `lean_latest` | float | Mean statewide-race lean in latest year |

Note: trend is computed from the *average lean across all statewide races* each year, not from the composite. This avoids circular dependency with composite weights. After running `python cli.py trends`, trend columns are also merged into the targeting CSV.

---

### File relationships

```
targeting.csv ──────────────────────────────── primary analytical file
    │ district (1:1)
    ├── composite_lean.csv ──────────────────── race-by-race leans + house results (wide)
    │       │ district (1:many)
    │       └── actual_results.csv ──────────── long-format house results
    │
    ├── redistricting_overlap.csv ───────────── per-district map reliability flags
    │       │ district (1:1)
    │       └── years_reliable → filters actual_results
    │
    ├── anomaly_flags.csv ───────────────────── flagged outliers (district + year)
    ├── drop_one_sensitivity.csv ────────────── composite robustness (district)
    ├── demographics.csv ────────────────────── ACS demographics (district_num)
    ├── oh_house_voter_universe.csv ─────────── voter-file district profiles (district)
    └── oh_house_district_trends.csv ────────── partisan trend 2010–2024 (district)

scenario_table.csv ──────────────────────────── standalone; no join key
probabilistic_scenarios.csv ─────────────────── standalone; statewide_d_pct
district_win_probs.csv ──────────────────────── district × statewide_d_pct
investment_priority.csv ─────────────────────── district (1:1)
path_optimizer.csv ──────────────────────────── standalone; investment steps
defensive_scenarios.csv ─────────────────────── D-held district × statewide_d_pct
year_baselines.json ─────────────────────────── used by anomaly detection
voter_file_clean.parquet ────────────────────── one row per active voter (no join)
```

---

### Example query patterns

These illustrate how to answer common questions from the data. All use pandas; file paths are relative to repo root.

```python
import pandas as pd, json

t  = pd.read_csv("reports/session3/oh_house_targeting.csv")
c  = pd.read_csv("reports/session2/oh_house_composite_lean.csv")
r  = pd.read_csv("reports/redistricting_overlap.csv")
a  = pd.read_csv("reports/anomaly_flags.csv")
d  = pd.read_csv("data/processed/oh_house_demographics.csv").rename(
     columns={"district_num": "district"})
vu = pd.read_csv("data/processed/oh_house_voter_universe.csv")  # Session 8+
```

**1. Top 10 realistic R-held pickup targets by composite lean**
```python
t[t["realistic_target"]].nlargest(10, "composite_lean")[
    ["district","composite_lean","tier","flip_threshold","open_seat_2026","target_mode"]]
```

**2. All tossup and lean_r districts with open seats in 2026**
```python
t[(t["tier"].isin(["tossup","lean_r"])) & (t["open_seat_2026"])][
    ["district","composite_lean","tier","flip_threshold","open_seat_reason"]]
```

**3. How many seats does D win at 48.5% statewide?**
```python
s = pd.read_csv("reports/session3/oh_house_scenario_table.csv")
s[s["statewide_d_pct"] == 48.5][["d_seats","net_change_from_current","newly_flipped"]]
```

**4. At what statewide D share does the 40th seat flip?**
```python
s = pd.read_csv("reports/session3/oh_house_scenario_table.csv")
s[s["d_seats"] >= 40].iloc[0][["statewide_d_pct","newly_flipped"]]
```

**5. Which districts have persuasion as their target mode?**
```python
t[t["target_mode"] == "persuasion"][
    ["district","composite_lean","tier","flip_threshold","swing_sd"]]
```

**6. Which districts have unreliable 2022 house data?**
```python
r[r["overlap_category_interim_final"].isin(["relocated","redrawn"])][
    ["district","jaccard_interim_final","overlap_category_interim_final","years_reliable"]]
```

**7. Which districts have only 2024 as a reliable house year?**
```python
r[r["years_reliable"] == "2024"][["district","overlap_category","overlap_category_interim_final"]]
```

**8. What were the anomaly flags and their explanations?**
```python
a[["district","year","residual","severity","auto_explanation"]].sort_values("abs_residual",ascending=False)
```

**9. Candidate effects in 2024 — who most over- and underperformed?**
```python
(t[t["contested_2024"].fillna(False)]
   [["district","composite_lean","tier","dem_candidate_2024","rep_candidate_2024","candidate_effect_2024"]]
   .sort_values("candidate_effect_2024", ascending=False))
```

**10. Defensive priorities — D-held seats at risk**
```python
t[t["defensive_priority"]][
    ["district","composite_lean","tier","flip_threshold","contested_2024","margin_2024"]]
```

**11. Most demographically educated competitive districts (college share)**
```python
competitive = t[t["pickup_opportunity"]].merge(d, on="district")
competitive.nlargest(10, "college_pct")[
    ["district","composite_lean","tier","college_pct","median_income","target_mode"]]
```

**12. Districts where the composite is most sensitive to any single race**
```python
t.nlargest(10, "composite_sensitivity")[
    ["district","composite_lean","tier","composite_sensitivity","most_sensitive_race"]]
```

**13. Compare 2022 vs 2024 house performance in districts that have both years**
```python
reliable_both = r[r["years_reliable"].str.contains("2022")]["district"]
c[c["district"].isin(reliable_both)][
    ["district","dem_share_2022","dem_share_2024","candidate_effect_2022","candidate_effect_2024",
     "contested_2022","contested_2024"]].dropna(subset=["dem_share_2022","dem_share_2024"])
```

**14. Which race is each district most sensitive to losing from the composite?**
```python
drops = pd.read_csv("data/processed/drop_one_sensitivity.csv")
t.merge(drops[["district","max_change","most_sensitive_to"]], on="district")\
 [["district","composite_lean","tier","max_change","most_sensitive_to"]]\
 .sort_values("max_change", ascending=False).head(20)
```

**15. Full district profile: everything about District 52**
```python
dist = 52
profile = t[t["district"] == dist].iloc[0]
redist = r[r["district"] == dist].iloc[0]
demo = d[d["district"] == dist].iloc[0]
print(profile.to_string())
print(f"Redistricting: {redist['overlap_category']} → {redist['overlap_category_interim_final']}")
print(f"Reliable years: {redist['years_reliable']}")
print(f"College: {demo['college_pct']:.1%}, Median income: ${demo['median_income']:,.0f}")
```

**16. Mobilization targets in a specific district (Session 8+)**
```python
vu[vu["district"] == 52][["total_active_voters","n_mobilization_targets",
    "pct_mobilization_targets","n_persuasion_targets","target_mode_voterfile"]]
```

**17. Which pickup targets have the largest mobilization universe?**
```python
t[t["realistic_target"]].merge(vu[["district","n_mobilization_targets",
    "pct_mobilization_targets","partisan_advantage","target_mode_voterfile"]],
    on="district").nlargest(10, "n_mobilization_targets")[
    ["district","composite_lean","tier","open_seat_2026","n_mobilization_targets","pct_mobilization_targets"]]
```

**18. Total mobilization universe across all open-seat targets**
```python
open_targets = t[t["open_seat_2026"] & t["pickup_opportunity"]]["district"]
vu[vu["district"].isin(open_targets)][["district","n_mobilization_targets",
    "n_persuasion_targets","partisan_advantage"]].assign(
    total=lambda x: x["n_mobilization_targets"] + x["n_persuasion_targets"])
```

**19. Districts with highest turnout dropoff (best mobilization opportunity)**
```python
vu.merge(t[["district","tier","pickup_opportunity","open_seat_2026"]], on="district")\
  [vu["pickup_opportunity"]].nsmallest(10, "turnout_dropoff")[
  ["district","turnout_2024","turnout_2022","turnout_dropoff","n_mobilization_targets"]]
```

**20. Compare partisan composition of two districts**
```python
vu[vu["district"].isin([31, 36])][["district","total_active_voters",
    "pct_strong_d","pct_lean_d","pct_unaffiliated","pct_lean_r","pct_strong_r",
    "partisan_advantage","n_mobilization_targets","target_mode_voterfile"]]
```

**21. District win probability at a specific statewide environment (Session 9+)**
```python
wp = pd.read_csv("reports/session8/oh_house_district_win_probs.csv")
wp[wp["statewide_d_pct"] == 48.0].sort_values("win_prob", ascending=False).head(20)[
    ["district","win_prob","sigma_total","sigma_i","sigma_source"]]  # win_prob uses sigma_total
```

**22. Probabilistic scenario summary — how many seats at each statewide D?**
```python
ps = pd.read_csv("reports/session8/oh_house_probabilistic_scenarios.csv")
ps[["statewide_d_pct","mean_d_seats","p10_seats","p90_seats","prob_hold_34","prob_reach_40","prob_majority"]]
```

**23. Top investment priority targets (highest marginal win probability)**
```python
inv = pd.read_csv("reports/session8/oh_house_investment_priority.csv")
inv.head(15)[["district","win_prob","marginal_wp","investment_rank","tier","open_seat_2026"]]
```

**24. Path-to-40 optimizer — which districts to invest in and in what order**
```python
path = pd.read_csv("reports/session8/oh_house_path_optimizer.csv")
path[["priority_rank","district","baseline_wp","invested_wp","marginal_gain","cumulative_expected_seats"]]
```

**25. Defensive risk — which D-held seats are in danger at 46% statewide?**
```python
defense = pd.read_csv("reports/session8/oh_house_defensive_scenarios.csv")
defense[(defense["statewide_d_pct"] == 46.0) & (defense["risk_level"] == "high")][
    ["district","prob_hold","risk_level"]]
```

**26. Which pickup targets are trending D? (best long-term investments)**
```python
tr = pd.read_csv("data/processed/oh_house_district_trends.csv")
t.merge(tr, on="district")[
    (t["pickup_opportunity"]) & (tr["trend_dir"] == "trending_d")
].sort_values("trend_slope", ascending=False)[
    ["district","composite_lean","tier","trend_slope","trend_shift","trend_r2","open_seat_2026"]]
```

**27. Combine lean + trend for opportunity scoring**
```python
tr = pd.read_csv("data/processed/oh_house_district_trends.csv")
m = t[t["pickup_opportunity"]].merge(tr, on="district")
# D-leaning AND trending D = best opportunities
m[(m["composite_lean"] > 0) & (m["trend_dir"] == "trending_d")][
    ["district","composite_lean","trend_slope","trend_shift","flip_threshold","open_seat_2026"]]
```

**28. Defensive seats trending R — erosion risk**
```python
tr = pd.read_csv("data/processed/oh_house_district_trends.csv")
m = t[t["current_holder"] == "D"].merge(tr, on="district")
m[m["trend_dir"] == "trending_r"].sort_values("trend_slope")[
    ["district","composite_lean","tier","trend_slope","trend_shift","trend_r2"]]
```

---

## Session Log

### Session 1: Proof of Concept ✅
- Ingested VEST 2020 precinct shapefile and TIGER/Line SLDL districts.
- Built area-weighted crosswalk (8,941 precincts → 99 districts, 35% split rate).
- Computed presidential partisan lean for all 99 districts.
- Validation passed: vote totals reconcile perfectly, statewide D two-party share = 45.92% (expected ~45.9%).
- Output: `reports/session1/oh_house_partisan_lean_2020.csv`, `reports/session1/validation_summary.txt`.
- Finding: 44 D-leaning districts, 55 R-leaning. Median lean −0.0425. D vote is packed into fewer, more heavily D districts (most D: +44 pts, most R: −28 pts).

### Session 2: Multi-Race Composite Lean ✅
- Parsed all four Ohio SOS XLSX files (2018/2020/2022/2024) via `src/ingest_sos.py`.
- Built county FIPS → county name lookup programmatically; 88/88 counties mapped.
- Joined all statewide races to VEST 2020 geometry; 90%+ match rate on all races.
- VEST/SOS 2020 presidential cross-check: exact match (diff=0).
- Computed district lean for 9 statewide races across 4 years.
- Ingested actual house results for 2018/2020/2022/2024 (no crosswalk needed; precincts already scoped to house districts).
- Built weighted composite index; effective weights sum to 1.0 after normalization.
- Output: `reports/session2/oh_house_composite_lean.csv`, `reports/session2/oh_house_actual_results.csv`, `reports/session2/validation_summary_session2.txt`.
- **Key findings:** 43 D-leaning districts, 56 R-leaning. Median lean −0.023. Most D: District 18 (+0.43), Most R: District 80 (−0.20). 18 R-held districts with composite lean > −0.05 are the primary pickup targets.
- **Known limitation:** SOS vote capture shows 101–103% for 2022/2024 races. Lean calculations appear correct; root cause is precinct boundary changes between 2020 VEST geometry and later SOS files. Investigate before Session 4.

### Session 3: District Classification & Targeting ✅
- New modules: `src/classify.py`, `src/scenarios.py`; new CLI command: `python cli.py classify`
- Tiered all 99 districts (safe_d → safe_r). Originally used ±3/8/15-pt composite lean thresholds; updated in Session 13 to win-probability tiers at three reference environments (46/48/50% statewide D).
- Computed swing SD and turnout elasticity per district from contested house races; assigned target modes (persuasion/mobilization/hybrid/structural).
- Uniform swing model: flip_threshold = 0.50 − composite_lean for each district.
- Output: `reports/session3/oh_house_targeting.csv`, `reports/session3/oh_house_scenario_table.csv`, `reports/session3/oh_house_pickup_ladder.txt`
- **Key findings:**
  - 18 safe_d, 11 likely_d, 10 lean_d (5D/5R), 10 tossup (all R), 18 lean_r (all R), 14 likely_r, 18 safe_r
  - 33 R-held districts in competitive tiers (pickup opportunities)
  - 6 D-held seats flagged as defensive priorities; District 10 (D+3.0) most vulnerable
  - Hold 34 seats: statewide D ≥ 45.5%. Reach 40: statewide D ≥ 48.5%. Majority: statewide D ≥ 53.5%
  - Top 5 pickup targets (composite lean): 31 (+0.050, structural), 36 (+0.038, mobilization), 49 (+0.037, mobilization), 52 (+0.034, mobilization), 35 (+0.033, persuasion)

### Session 4: ACS Demographics, Pop-Weighted Crosswalk & Regression ✅
- New modules: `src/demographics.py`, `src/model.py`; new CLI command: `python cli.py session4`
- Extended `src/crosswalk.py` with `build_pop_weight_table` and `build_crosswalk_pop_weighted`.
- Sub-task A: ACS 2023 5-year estimates pulled block-group level via Census API, overlaid on districts using area-fraction intersection. Outputs `data/processed/oh_house_demographics.csv`.
- Sub-task B: Census 2020 blocks loaded county-by-county via pygris; block centroids assigned to precincts and districts; `data/processed/pop_weight_table.parquet` cached. Composite re-run with pop-weighted allocation; before/after comparison logged.
- Sub-task C: GLM binomial/logit (primary) + OLS (comparison), clustered SEs by district. Formula: `dem_share ~ composite_lean + college_pct_c + log_income_c + white_pct_c + log_density_c + C(incumbent_party, Treatment('open')) + presidential_year + composite_lean:presidential_year`. Key results: deviance explained = 0.627, OLS R² = 0.643, composite_lean log-odds β = 2.59, ME at lean=0 = 0.64, D incumbency AME = +6.7 pts, R incumbency AME = −6.1 pts. McFadden pseudo-R² (0.075) is unreliable for fractional outcomes — use deviance explained. Outputs `reports/session4/regression_summary.txt`.
- Sub-task D: Districts with n_contested < 3 flagged as `insufficient_data`; n_contested == 0 flagged as `no_data`. Updated `reports/session3/oh_house_targeting.csv`.
- Validation: `reports/session4/validation_summary_session4.txt`.

### Session 5: Methodology Document, External Validation & Reporting Layer ✅
- New modules: `src/export.py` (reportlab PDF), `src/validate_external.py` (DRA comparison)
- New CLI commands: `report`, `targets`, `scenario`, `defense`, `export`, `methodology`, `session5`
- **Sub-task A:** `reports/session5/methodology.md` (3,518 words) — dual-track plain-English + technical. Covers all 10 sections including full data dictionary, sensitivity analysis, and limitations.
- **Sub-task B:** DRA 2024 Ohio House CSV obtained manually; `parse_dra_csv()` handles trailing-comma/column-shift quirk. **Validated: Spearman ρ = 0.9985 (p=3e-124), MAE = 0.79 pts, 0 districts disagree by >3 pts — PASS.** Largest gaps (~1.6 pts) in Districts 62, 29, 63 — consistent with DRA using pres-only vs. our 9-race composite. `reports/session5/external_validation.csv` written. `python cli.py session5 --external-csv data/processed/dra_ohio_house.csv` works end-to-end. Sensitivity analysis: Spearman ρ > 0.998 vs. 3 alternative weighting schemes; max district change < 1.2 pts.
- **Sub-task C:** 99 PDF district profiles in `reports/session5/district_profiles/`; `reports/session5/methodology.pdf`. Profiles are one-page, data-dense: banner (tier color), lean by race, house results 2018–2024 with GLM candidate effects, ACS demographics, model estimates, targeting metadata.
- CLI usage:
  ```bash
  python cli.py report --district 12         # single PDF
  python cli.py report --all                 # all 99 PDFs
  python cli.py targets --tier tossup        # pickup targets by tier
  python cli.py targets --mode persuasion    # targets by mode
  python cli.py scenario --statewide-d 48.5  # seat projection
  python cli.py defense                      # D-held at-risk seats
  python cli.py export                       # copy CSVs to exports/
  python cli.py methodology --pdf            # print summary + gen PDF
  python cli.py session5                     # run full session
  ```

### Redistricting Fix (post-Session 5 bugfix) ✅
- New CLI command: `python cli.py redistricting-fix`
- New files: `src/constants.py`, `reports/redistricting_overlap.csv`
- **Problem:** Pre-2022 house race results were joined by district number only (geometry-blind). Ohio's 2022 redistricting relocated 71/99 districts (Jaccard = 0). Joining 2018/2020 results for e.g. District 52 (moved Butler County → Lorain County) compared completely different electorates.
- **Fix:** `src/validate.py::check_precinct_redistricting_overlap()` computes Jaccard similarity between SOS precinct sets. Extended to compute both old→interim (2020 vs. 2022) and interim→final (2022 vs. 2024) comparisons. `src/ingest_house_results.py::apply_redistricting_filter()` drops contaminated house rows. Composite lean unaffected.
- **Results:** old→interim: 71 relocated, 15 redrawn, 13 same. Interim→final: 13 relocated, 34 redrawn, 52 same. Years reliable: 13 districts have all 4 years; 73 have 2022+2024; 13 have only 2024.
- **Regression:** With only 2022/2024 data, R² = 0.954. Incumbency coefficient near-collinear with composite lean — regression is exploratory only.
- **`LITERATURE_INCUMBENCY_ADVANTAGE = 0.06`** in `src/constants.py`: 6-point literature prior, kept for reference. Not wired to any primary model calculation.

### Session 6: 2026 Open Seat Tracking ✅
- New CLI command: `python cli.py open-seats`
- **OPEN_SEATS_2026** dict in `src/classify.py` (hardcoded, easy to update): 7 R-held open seats (Roemer 31, Demetriou 35, Plummer 39, Williams 44, Manning 52, Callender 57, Hoops 81) and 2 D-held (Russo 7, Brent 18). Source: Wikipedia + Ballotpedia, verified March 2026. **UPDATE this dict as new retirements/filing decisions are announced.**
- **New targeting CSV columns:** `open_seat_2026`, `open_seat_reason`, `current_incumbent_name`, `incumbent_status_2026`
- **Candidate name extraction**: `src/ingest_sos.py` stores `d_candidate_names` / `r_candidate_names` in `RaceSpec`. `extract_candidate_names(sos)` in `src/ingest_house_results.py`.

### Session 7: Incumbency Strip + Validation Suite ✅
- **Incumbency stripped from all quantitative outputs.** `flip_threshold = 0.50 − composite_lean` is the only threshold. No `flip_threshold_inc_adj` columns. Scenarios and pickup ladder are fundamentals-only.
- **Anomaly detection** (`src/validate.py::detect_anomalies()`): flags contested district-years where |residual| > 10/15 pts; auto-explains as redistricting_artifact / nominal_candidate / possible_open_seat / unexplained. Current: 10 flags, all 2022, all redistricting_artifact. Zero unexplained. Output: `reports/anomaly_flags.csv`.
- **Drop-one sensitivity** (`src/composite.py::drop_one_sensitivity()`): recomputes composite lean without each of 9 races in turn. Result: no district changes > 2 pts; 9 change tier; gov2022 most influential (mean |Δ| = 0.004 pts). Output: `data/processed/drop_one_sensitivity.csv`.
- `composite_sensitivity` and `most_sensitive_race` columns added to targeting CSV.
- Year baselines stored to `data/processed/year_baselines.json` after each composite run.
- `reports/methodology.md` rewritten from scratch (v2.0, 14 sections) to reflect current model state accurately.
- `CLAUDE.md` updated with `## Data Schema` section including file inventory, column docs, file relationships, and 15 example query patterns.
- **`realistic_target` flag** added to targeting CSV: R-held districts where `flip_threshold ≤ 0.52`. Pickup ladder now splits into "Realistic Targets" and "Structural Long-shots" sections. Fixes misleading presentation where lean_r districts (requiring 53–58% statewide D) appeared alongside genuinely competitive tossups. Tier thresholds unchanged; `realistic_target` is a separate filter.

### Session 8: Voter File Integration ✅
- New module: `src/voterfile.py`; new CLI command: `python cli.py voters`
- Data source: Ohio SOS statewide voter file, ~7.9M records, 4 batch files in `data/voterfiles/`.
- `load_voter_file()`: reads all 4 batches in chunks, drops PII immediately, recodes election history to int8, scores each voter, writes `data/processed/voter_file_clean.parquet`.
- Per-voter scores: `turnout_propensity` (high/medium/low/very_low, based on last 4 November generals), `presidential_only` (voted 2020/2024 but not 2018/2022), `partisan_lean` (strong_d/lean_d/crossover/unaffiliated/lean_r/strong_r, based on primary history 2016+).
- `build_voter_universe()`: aggregates to 99 districts; computes partisan composition, turnout rates, turnout dropoff, mobilization universe, persuasion universe, voter-file targeting mode. Writes `data/processed/oh_house_voter_universe.csv`.
- `merge_voter_universe_into_targeting()`: merges voter universe into targeting CSV; renames old `target_mode` → `target_mode_aggregate`; adds `target_mode_voterfile` as authoritative mode.
- `export_contact_universe()`: exports voter_id + scores (no PII) for a district to CSV for field use.
- PDF district profiles extended with VOTER UNIVERSE section when data is present.
- Validation: statewide active voter count, partisan composition check (D share of primary participants 35–45%), turnout rates vs. SOS-reported, spot-checks for Districts 18/52/80.

### Session 9: Probabilistic Scenarios + Resource Allocation ✅
- New module: `src/simulate.py`; new CLI commands: `simulate`, `invest`, `win-prob`, `session8`
- **Empirical Bayes district uncertainty**: σ_prior = 0.0462 (pooled candidate effect std). Per-district σ_i shrunk toward prior based on n_contested (shrinkage weights: 0/1→0.0, 2→0.3, 3→0.7, 4→0.85). Range: 0.020–0.077.
- **Monte Carlo simulation engine**: 10,000 sims per statewide environment. Partial correlation model (shared statewide shift + independent district noise).
- **Optional incumbency adjustment** (`--with-incumbency`): applies literature prior (6 pts ± 1pt noise) to confirmed-status districts only. Off by default.
- **Probabilistic scenario table**: mean/median/p10/p25/p75/p90 seats + P(≥34)/P(≥40)/P(majority) at each statewide D% from 40–55%.
- **District-level win probabilities**: analytical (normal CDF) + MC verification. 99 districts × 31 environments.
- **Marginal win probability**: analytical derivative — ranks districts by "bang for buck" (sensitivity of P(win) to small D share improvement).
- **Path-to-target optimizer**: greedy algorithm selects districts that most increase expected seats. Investment = abstract +1pt lean shift.
- **Defensive scenarios**: P(hold) for D-held seats at each statewide environment (43–48%).
- **PDF profiles extended** with PROBABILISTIC OUTLOOK section (win probs at 46/48/50%, investment rank, σ_i).
- **Pickup portfolio framing**: districts classified into Core (7 districts, >25% WP even at 46%), Stretch (5 districts, viable at 48%+), and Long-Shot (6 districts, only in a wave). Core portfolio: Districts 31, 36, 49, 52, 35, 64, 17. Three open seats (31, 52, 35) are highest-value opportunities.
- **Key findings at 48% statewide D**: mean 39.9 seats [37–43 80% CI], P(≥40) = 57%, top investment targets by marginal WP: Districts 52, 39, 49, 36, 64.
- **Validation**: σ_prior in expected range (0.046), monotonicity (0 violations), 2024 backtest (mean 32.8, 34 in 80% CI), tossup districts have highest marginal WP, seat std = 2.17.
- **Superseded 2026-09-11** (see "Reference race and district-noise split" below): σ is now 2.98 pts independent + 2.35 pts shared (was 4.62 pooled, all independent). At 48%: mean 39.7 seats [33–46 80% CI], P(≥40) = 52%, seat std ≈ 5.0. Top investment targets at 48%: 52, 39, 17, 49, 60. Core portfolio: 31, 35, 36, 49, 52, 64.

### Session 10: Census Block Backbone Architecture ✅
- New modules: `src/backbone.py`, `src/ingest_historical.py`; new CLI command: `python cli.py backbone`
- **Canonical data model**: Census 2020 blocks (~220k populated) as the fundamental storage unit. Precinct votes disaggregated to blocks proportional to population, then reaggregated to any district map via `merge + groupby` (instant, no geometry needed).
- **Block geometry cache**: 219,669 populated blocks across 88 counties, loaded via `pygris.blocks()`. Cached to `data/processed/block_geometry.parquet` (5.7 MB).
- **Block-to-precinct maps**: Centroid-assigned for VEST 2016, 2018, 2020. Only 7 blocks unassigned per year (edge cases).
- **Block-to-district map**: 99 districts covered, 630–5,345 blocks per district.
- **Block vote surfaces (8 election years)**:
  - VEST precinct disaggregation (2016, 2018, 2020): highest precision, max lean diff < 0.005 pts vs existing crosswalk.
  - SOS→VEST precinct join (2022, 2024): 98–99% match rate to VEST 2020 precincts, comparable precision.
  - County-level disaggregation (2010, 2012, 2014): coarser (88 counties vs ~9k precincts), but acceptable for composite lean. 2012 limited to President + USS (no statewide offices).
- **SOS parser extension**: `src/ingest_historical.py` handles 2010 (flat AllCounties sheet, no party tags — candidate name lookup), 2012 (multi-sheet, party tags, missing statewide offices), 2014 (single sheet, party tags).
- **Validation A — backbone vs existing composite (2016–2024)**: max diff 0.0028 pts, mean 0.0002 pts, 0 districts > 0.005, 0 tier changes. **PASS.**
- **Validation B — extended composite (2010–2024)**: 34 race-year combinations across 8 elections. Correlation with current composite: 0.997. Mean shift from historical data: 0.015 pts. No tier changes from adding history alone.
- **Data store**: 14 parquet files, ~110 MB total. `python cli.py backbone --full` builds everything end-to-end.
- **Backward compatible**: existing crosswalk pipeline untouched; backbone is an alternative path producing the same outputs.

### Session 11: Stakeholder GUI ✅
- Streamlit multi-page dashboard in `gui/` package + `pages/` directory.
- 5 pages: Scenario Explorer, Pickup Portfolio, District Profiles, Map (choropleth), Investment Priority.
- Live recomputation via `src/simulate.py` when slider differs from reference environment.
- `streamlit run app.py` launches the dashboard.

### Session 12: Historical Backtest ✅
- New module: `src/backtest.py`; new CLI command: `python cli.py backtest`
- **Out-of-sample test**: builds composite lean from 2016–2022 block vote surfaces only (no 2024 data), then predicts 2024 house outcomes at the actual 2024 statewide D share (44.3%).
- **Pre-2024 weights**: DEFAULT_WEIGHTS with 2024 keys removed; `build_composite()` redistributes proportionally. Effective weights: gov2022 29.4%, statewide_avg2022 17.6%, pre2020 17.6%, gov2018 17.6%, statewide_avg2018 11.8%, uss 2.9% each.
- **Composite stability**: pre-2024 vs full composite correlation = 0.9996, mean |Δ lean| = 0.004, max = 0.013, 6 tier changes. The composite is highly stable without 2024 data.
- **Binary accuracy**: 98.0% overall (97/99), 97.6% contested-only (81/83). Only 2 misclassifications: Districts 23 and 10 (both lean_d, predicted R, actually D — candidate overperformance).
- **Competitive district accuracy**: the originally recorded 94.9% (37/39) does not reproduce — `backtest_summary.txt` reported 13/15 (86.7%) for contested tossup/lean districts. After the 2026-09-11 σ change: 11/12 (91.7%); the set shifts as tiers move, so treat it as a small-n diagnostic, not a headline.
- **Seat count**: actual 34 D seats within MC 80% CI [30, 35] and 50% CI [31, 34]. MC mean = 32.5.
- **Probabilistic quality**: Brier score 0.026 (all), Brier skill score 0.878 vs naive baseline. Spearman ρ = 0.970 (win_prob vs actual dem_share).
- **Calibration**: model is slightly conservative in the 0.3–0.8 WP range (underpredicts D wins), consistent with D candidates outperforming fundamentals in some competitive districts.
- Output: `reports/session12/backtest_summary.txt`, `reports/session12/backtest_district_predictions.csv`, `reports/session12/backtest_composite_comparison.csv`.
- **Credibility framing**: lead with the probabilistic metrics (Brier skill, log loss, Spearman ρ), not binary accuracy — a naive "2022 winner's party holds" baseline scores 98.8% (86 districts with both years), higher than the model's 98.0%. The backtest is also conditional on the actual 2024 statewide result, so it tests the composite and district noise, not the environment forecast. The 2 misses were D candidates overperforming lean_d fundamentals — the kind of candidate effect a fundamentals-only model cannot predict in advance.
- **Rerun 2026-09-11** (new reference race + σ split): Brier 0.0239 (was 0.0257), Brier skill 0.886, log loss 0.090 (was 0.097), Spearman ρ 0.978, E[seats] 32.3 vs. actual 34, 80% CI [25, 40] (was [30, 35]). The wider interval is intentional: it now carries the year-level shift. 2024's shift happened to be ≈0, so this one year can't identify σ_shared — don't tune it against the backtest.

### Session 13: Tier Terminology Overhaul ✅
- **Problem**: tier labels (Lean D, Lean R, etc.) were based on composite lean thresholds (±3/8/15 pts), but in political jargon "Lean D" implies a projected outcome (~55% chance D wins). District 52 was labeled "Lean D" but predicted R at the 2024 statewide environment — confusing.
- **Fix**: tiers are now **win-probability based** (Cook-style) at three reference environments:
  - 46% statewide D (bad cycle), 48% (neutral midterm), 50% (good cycle)
  - Safe D (>95% WP), Likely D (75–95%), Lean D (55–75%), Tossup (45–55%), Lean R (25–45%), Likely R (5–25%), Safe R (<5%)
- **Primary `tier` column** = tier at 48% (neutral midterm). Also: `tier_46`, `tier_48`, `tier_50`, `wp_46`, `wp_48`, `wp_50`.
- **Composite lean** remains as a raw number — no more bucketing into lean-threshold tiers. Displayed as D+3.3 / R+5.2 (Cook/Sabato style) in human-readable contexts.
- **Implementation**: `classify_districts()` now accepts optional `sigma_df`; if provided, computes WP tiers. Legacy lean-based fallback with deprecation warning.
- `format_lean()` helper in `src/classify.py` and `_fmt_lean_cook()` in `src/export.py` for D+/R+ display.
- GUI `fmt_lean()` updated to Cook format.
- **Key dependency**: classify now requires sigma (district uncertainty) for WP tiers. The `classify` CLI command computes sigma from composite_lean_df before classification.

### Session 14: Multi-Office Expansion (Phase A — partial) ✅
- **Strategic shift**: pivot from House-only conditional projection model toward a public-facing four-office forecast (Governor, State House, State Senate, US House). Full plan at `~/.claude/plans/iterative-coalescing-candle.md`.
- **A.2 — `src/office.py`**: `OfficeConfig` dataclass + `OFFICES` registry (`HOUSE`, `SENATE`, `CONGRESS`, `GOVERNOR`). Single source of truth for n_districts, district_range, seats_up_2026, shapefile paths, output prefixes, chamber thresholds, and SOS race attributes. `get_office(name)` and `all_offices()` helpers.
- **A.1 — Shapefiles acquired**: `data/shapefiles/sldu_2026/` (Sept 2023 unified bipartisan SD plan, in effect 2024+2026), `data/shapefiles/cd_2026/` (NEW Oct 31, 2025 congressional plan).
- **A.3 — SOS parser extension**: `src/ingest_sos.py` now extracts `senate` and `congressional` race dicts in addition to `house` and statewide. `_SENATE_RE`, `_CONGRESS_RE` regexes added.
- **A.4 — Race results parser generalized**: `src/ingest_house_results.py` → `src/ingest_race_results.py`. `parse_race_results(sos, office)` dispatches on `office.sos_race_attr`. `extract_candidate_names(sos, office)` likewise office-aware.
- **A.5 — Block-district maps for new offices**: `python cli.py senate backbone --build-map`, `congress backbone --build-map`. `build_block_district_map_for_office()` threads `office.n_districts` through validation. Outputs `block_district_map_senate_2024.parquet`, `block_district_map_congress_2026.parquet`.
- **A.6 — Block votes for US House**: `build_composite_from_blocks` extended to handle congressional races; backbone disaggregates congressional vote totals to blocks alongside statewide races.
- **A.7 — Hardcoded constants generalized**:
  - `src/scenarios.py` — `CURRENT_D_SEATS` and `MAJORITY` derived from `HOUSE` constants. `run_scenario_table` and `format_pickup_ladder` accept `office: OfficeConfig | None` and substitute office-specific seat thresholds.
  - `src/classify.py` — `OPEN_SEATS_2026` becomes `data/processed/open_seats_2026_{office}.csv`. `load_open_seats(office)` helper. `build_targeting_df(..., office=office)` accepts office context.
  - `src/backbone.py` — `build_block_district_map(..., expected_n=N)` parameterized; default 99 for back-compat.
- **A.8 — CLI restructure**: typer subcommand groups (`house_app`, `senate_app`, `congress_app`, `governor_app`, `multi_app`). All top-level commands auto-aliased into `house <cmd>` via `_alias_top_level_into_house()` for backwards compat. New: `<office> backbone`, `<office> composite`, `<office> classify`, `<office> simulate`; `multi backbone --build-all-maps`.
- **A.9 — End-to-end pipeline for senate + congress**: composite, classify, simulate all working. Outputs at `reports/session2/{office.output_prefix}_*.csv`, `reports/session3/{office.output_prefix}_*.csv`, `reports/session8/{office.output_prefix}_*.csv`.
- **Senate sanity check** (33 districts, current 9D-24R): SD-21 D+38 (Cleveland) safest D, SD-12 R+23 safest R. Neutral 48% statewide D → mean 12.4 senate seats, P(hold ≥9) = 99.9%, P(majority ≥17) requires ~53% statewide D.
- **Congress sanity check** (15 CDs, NEW Oct 2025 map, current 5D-10R): CD-11 D+33 (safe D), CD-1/9/10/15 all near-tossups under new map. Neutral 48% statewide D → mean 5.1 seats, P(hold ≥5) = 68%, P(+1 = 6 seats) = 37%.
- **Senate caveat**: 4-year staggered terms put only odd-numbered districts on 2026 ballot (17 of 33). Composite lean computed for all 33 for chamber narrative; investment_priority CSV flags `seat_up_2026`. The probabilistic scenario table is chamber-wide context, not a 2026 forecast.
- **Phase B preliminary — polls data acquired and refreshed 2026-09-11**: `data/polls/governor.csv` (5,332 rows nationwide, 719 unique polls; 22 unique Ohio 2026 governor general polls, 16 pollsters incl. NYT/Siena). `data/polls/senate.csv` (3,990 rows nationwide; 19 unique Ohio 2026 US Senate special polls — Brown vs. Husted, 13 pollsters incl. NYT/Siena). `data/polls/approval_polls.csv` (1,221 rows, poll-level national Trump approval, supersedes `president-averages.csv` as the primary approval source). Two-party D share computed at ingestion (toplines include don't-know/other): OH governor mean 49.7%, OH Senate mean 50.5% — see `## Polling Data` below. Still no generic ballot time series; the two OH statewide races partially substitute. Prior `governor.csv` (2,663 rows) archived at `data/polls/archive/`.
- **Outstanding A.9 follow-ups (non-blocking)**: senate `current_holder` needs to derive from `winner_2022` for odd districts; congress redistricting reliability flagging for Oct 2025 map vs prior maps; canonicalize HOUSE block-district map filename. Polishing the existing two-cycle senate and pre-2026 congressional history is on hold until Phase B.

### Phase A scope (still in progress)
- **Done**: A.1 (shapefiles), A.2 (OfficeConfig), A.3 (SOS extension), A.4 (race results generalize), A.5 (block-district maps), A.6 (block-vote disaggregation for congress), A.7 (constants generalized), A.8 (CLI restructure), A.9 (composite/classify/simulate for senate + congress), A.10 partial (per-office summary PDF for senate + congress via `generate_office_summary` in `src/export.py`; new `<office> report` CLI command and `multi report` aggregator).
- **Not done**: external validation for senate vs DRA equivalent, US House composite vs DRA congressional CSV, regression test for byte-exact unchanged house outputs (spot-checks pass), backtest extension to senate/congress, GUI multi-office support, governor "office" treatment (statewide degenerate case).

### Per-office summary PDFs (A.10)
- One PDF per office at `reports/session14/{office.output_prefix}_office_summary.pdf` (e.g. `oh_senate_office_summary.pdf`, `oh_congress_office_summary.pdf`).
- Sections: header banner with current chamber split + majority threshold, probabilistic seat outlook at 46/48/50% statewide D, tier distribution at the 3 reference environments, top 10 R-held pickup opportunities (sorted by investment_rank when available), defensive priorities (D-held in competitive tiers), full per-district lean+tier+WP@48 table.
- Senate PDFs flag districts on the 2026 ballot (odd-numbered districts only); congress PDFs omit that column since all 15 are up.
- Per the plan ("Per-CD US House district profile PDFs for all 15. Build for the ~5 competitive ones; bulk-template the rest."), this is intentionally a single per-office summary, not 48 sparse individual district PDFs.

### Reference race and district-noise split (2026-09-11)

**Problem.** Two biases were pushing the forecast up and making district noise look bigger than it is:
1. **Midterm reference race.** `year_baselines.json` used the governor as each midterm's "statewide D%". In 2022 Whaley (37.5%) ran 2.4–4.0 pts behind every down-ballot statewide race, and House Democrats beat her by 6.8 pts on average. That one-year gap went into the pooled candidate-effect σ as if it were district noise (4.62 pooled vs. 2.95 year-demeaned vs. 2.81 realized 2024 district scatter).
2. **Senate polls as an environment reading.** Brown ran ahead of the rest of the D ticket in all three of his Ohio races (+1.6 in 2012, +5.7 in 2018, +3.8 in 2024, vs. the reference race). The blend treated that as symmetric ±2-pt noise, so a 53% Brown average read as a 53% environment. The premium is Brown's, not the office's: Fisher (2010, −4.8) and Strickland (2016, −6.7) ran well behind the ticket.

**Changes.**
- `src/composite.py`: `REFERENCE_MIDTERM_RACES` (AG/Aud/SOS/Treas), `reference_share()`, `compute_year_baselines()` (replaces three copy-pasted baseline loops in `cli.py`), `add_candidate_effects()`.
- `src/simulate.py`: `compute_sigma_prior()` now returns the **within-year** pooled sd. New `compute_shared_sigma()` returns the between-year sd of the House's mean offset from the reference, net of each year's sampling noise: τ² = var(year means) − mean(σ_within²/n_year). `estimate_district_sigma(..., sigma_shared=...)` is keyword-required. `run_simulations` draws one shared shift per simulation. `compute_analytical_win_probs` and `classify_districts` use √(σ_i² + σ_shared²) (+ env σ). `shared_sigma_of()` raises on a sigma_df without the column.
- `src/forecast_statewide.py`: `RACE_ADJUSTMENTS` / `race_deviation()` measure each race's deviation from the reference race in past cycles, using the block vote surfaces. The Senate uses Brown's three races: mean +3.7 subtracted, 2.3-pt predictive sd. The governor uses 2010–2022 (+3.3, −5.7, +0.4, −3.2): **no mean shift** (Acton has no record) but a 4.4-pt predictive sd. `CANDIDATE_IDIOSYNCRASY_SIGMA` is now only a floor. `forecast-trend` adds `governor_polls_raw_pct` / `senate_polls_raw_pct`.
- House composite CSV candidate effects migrated in place. Recomputing them with the old baselines reproduced the existing columns exactly before the new ones were written. Senate/congress composites were rerun through the new code path; their effects shifted by exactly the baseline deltas.

**Numbers (House unless noted).**

| | Before | After |
|---|---|---|
| σ (district) | 4.62 pooled, independent | 2.98 independent + 2.35 shared |
| At 48% fixed: mean / 80% CI / P(≥40) | 39.9 / [37, 43] / 57% | 39.7 / [33, 46] / 52% |
| Blended environment (2026-09-11) | 50.0% | 48.2% |
| Unconditional forecast: mean / 80% CI | 44.1 / [38, 50] | 39.8 / [32, 47] |
| P(≥34) / P(≥40) / P(majority) | — / 84.1% / 12.7% | 85.4% / 51.7% / 5.3% |
| Senate (unconditional) | — | 12.7 seats, P(≥9) 95.2%, P(majority) 10.0% |
| Congress (unconditional) | — | 5.3 seats, P(≥5) 64.2%, P(≥6) 41.9% |

"Before" figures come from outputs generated before this change. Separately, uncommitted edits to `src/ingest_polls.py` / `src/fundamentals.py` (effective-sample-size poll weighting, `question_id` grouping) landed during the same session; they are not part of this change. All "after" numbers use them. On their own they move the old-method blend only 50.1% → 50.0%. Blend variants at the same as-of date, with the same poll code: old method 50.0%, Senate adjustment only 48.7%, Senate + governor spread 48.2%. **The governor treatment is a judgment call.** Applying the measured 4.4-pt spread drops the governor signal's weight from 35% to 13% and moves weight to the generic ballot (44%) and adjusted Senate polls (39%). Reverting to the old governor treatment would mean letting the governor signal fall back to the 2-pt `CANDIDATE_IDIOSYNCRASY_SIGMA` floor instead of its measured spread; there is no switch for that yet.

**Verified.** With `sigma_shared=0` and the old pooled σ, the new engine reproduces the old scenario table exactly. Analytic Σ win_prob (39.7) matches the MC mean (39.8). The CLI (`classify`, `session8`, `simulate`, `invest`, `win-prob`, `scenario`, `forecast`, `forecast-trend`, `backtest`, office variants), `report --all`, and the senate/congress office PDFs all run. All Streamlit pages except Pickup Portfolio pass `AppTest`; that page fails identically on the pre-change commit (pre-existing KeyError on `wp_46/48/50`).

**Still open.**
- τ (σ_shared) rests mostly on 2022 vs. 2024. The 2018/2020 means come from ~13 same-geography districts, and 2022's +3.6-pt House offset vs. the down-ballot reference drives most of it. Revisit after 2026.
- `swing_sd` (raw D-share sd across years) still contains year-level shifts. It feeds σ_i for the 60 districts with ≥2 contested cycles. Small effect today (2022 and 2024 House environments were nearly identical), but conceptually it should be a residual sd.
- `multi report` crashes on the House office summary (ReportLab LayoutError on the 99-row table). This predates the change; senate/congress reports run individually.
- **The national fundamentals signal is now the largest blend weight (44%), and it sits on a presidential-anchored scale.** `OHIO_VS_NATIONAL_OFFSET_MEAN` (−5.63) comes from Ohio-vs-national *presidential* vote in 2020/2024, but it now feeds an estimate of the midterm down-ballot reference. Ohio's midterm reference moved from 47.7% (2018) to 40.7% (2022), so carrying the presidential offset over is an unvalidated assumption. Midterm national vote share is still uncollected (see Known gaps).
- **Senate and congress σ_shared come from race history the repo already flags as unreliable.** Congress `years_reliable_for_race_history` is empty for every CD (new Oct 2025 map), and senate odd-district 2022 results are joined across maps by district number. `compute_shared_sigma` still reads their `candidate_effect_{year}` columns (senate τ = 2.57, congress τ = 1.38), so those forecasts' widths inherit that contamination.
- **`reports/methodology.md` is the document meant for publication**, and it now misdescribes both the reference race and σ. Rewrite it before anything public. `reports/session8/README.md` and `reports/session5/methodology.md` also quote pre-change σ and seat numbers.

### Deferred: Natural Language Query Interface
- Claude API integration — pushed back; unnecessary for current stakeholder needs.
- May revisit if stakeholders need freeform querying beyond what the GUI provides.

## Polling Data (Phase B preliminary)

Polling data feeds the Phase B statewide forecast through `python cli.py forecast` / `<office> forecast` / `forecast-trend` (see below); the GUI does not use it yet. Files live in `data/polls/`. All three raw-poll files (`governor.csv`, `senate.csv`, `approval_polls.csv`) share the same schema — the standard 538/ABC-style raw polls feed, which several outlets (including the NYT/Upshot poll tracker) source from or contribute to. `The New York Times/Siena University` shows up as one pollster among many within this feed, not as a separate source — "NYT polling" is already inside these files, not a distinct integration.

Refreshed 2026-09-11 (previous `governor.csv` archived at `data/polls/archive/governor_2663rows_pre2026-09.csv`; confirmed the new pull is a strict superset by `poll_id` before replacing).

### `data/polls/governor.csv` and `data/polls/senate.csv` — FiveThirtyEight-schema, all-state polls

Long format: one row per (poll, candidate, party). For Ohio 2026 general specifically, filter on `state == "OH"` AND `cycle == 2026` AND `stage == "general"` (`office_type == "Governor"` or `"U.S. Senate"`).

| Column | Notes |
|---|---|
| `poll_id`, `pollster_id`, `pollster` | Identity. Use `display_name` for human labels. |
| `pollster_rating_id`, `numeric_grade`, `pollscore`, `transparency_score` | 538 quality metrics. May be empty for newer pollsters. |
| `state`, `cycle`, `office_type`, `seat_name`, `election_date` | Race identifiers. |
| `start_date`, `end_date` | Field period (`m/d/yy` strings, **not zero-padded** — parse with `pd.to_datetime(..., format="%m/%d/%y")`; naive string min/max sorts these wrong). |
| `sample_size`, `population` | LV / RV / A. `lv` = likely voters. |
| `methodology` | "Live phone", "IVR/online", "Text", etc. |
| `stage` | `primary` or `general` — important filter. |
| `party`, `pct`, `candidate_name`, `answer` | The actual response. One row per candidate-option per poll, **including non-candidate rows** ("Don't know", "Someone else", "Would not vote") — always filter to the two named candidates before averaging. |
| `partisan`, `internal`, `sponsor_*` | Sponsorship/internal-poll flags. Filter or weight accordingly. |

**`governor.csv`** — 719 unique polls nationwide (all-state, 2023-09 to 2026-09). Ohio 2026 general: 22 unique polls (Acton D vs. Ramaswamy R), 16 pollsters including NYT/Siena (one poll, 6/28/26). **Two-party D share must be computed at ingestion, not read off the topline** — toplines include "don't know"/"someone else," e.g. the NYT/Siena poll reads Acton 46.67 / Ramaswamy 46.85 topline, which is 49.9% two-party D, not 46.7%. Mean two-party D across all 22 OH polls (as of acquisition): **49.7%** (range 44.7–55.2%).

**`senate.csv`** — Ohio 2026 general (Husted special): 19 unique polls, 13 pollsters including NYT/Siena. Mean two-party D share (Brown vs. Husted): **50.5%** (range 46.9–54.1%). This is the second independent Ohio statewide race this cycle and partially fills the missing-generic-ballot gap below — it also hedges the governor race's Ramaswamy problem (a celebrity candidate whose polling likely carries a large candidate-specific effect on top of the underlying environment).

### `data/polls/president-averages.csv` — FiveThirtyEight *pre-aggregated* approval averages

Long format: one row per (topic, date, answer). Kept for reference; superseded as the primary approval source by `approval_polls.csv` below.

| Column | Notes |
|---|---|
| `topic` | E.g. `2025 Approval - Trump`. As of acquisition: only Trump 2nd-term approval. |
| `date` | YYYY-MM-DD. |
| `answer` | `Approve` or `Disapprove`. |
| `pct` | Percentage (0–100). |

### `data/polls/approval_polls.csv` — raw national approval/favorability polls (poll-level)

Same 538-schema family as `governor.csv`/`senate.csv` but national approval questions (no `state` column; `politician`, `yes`, `no` in place of `office_type`/`candidate_name`/`party`). 1,221 rows, 2025-01 to 2026-09, Trump approval only. Poll-level — carries `sample_size`, `population` (lv/rv), `methodology`, and pollster quality ratings, so a custom recency/quality-weighted average can be built instead of relying on `president-averages.csv`'s fixed one. Not yet consumed by any model code.

### `data/polls/generic_ballot.csv` — Silver Bulletin national generic-ballot polls (closes the generic-ballot gap)

Different schema from the other three files (Silver Bulletin's own export, not the 538/ABC raw-polls schema): one row per poll, `dem`/`rep` as direct raw topline columns (not long-format candidate rows), dates as full-4-digit-year `m/d/YYYY` strings (`enddate`, `startdate` — **not** the 2-digit `m/d/yy` used in `governor.csv`/`senate.csv`/`approval_polls.csv`; using the wrong format string silently produces `NaT`). Also carries Silver Bulletin's own `weight`/`influence`/`adjusted_dem`/`adjusted_rep` columns — deliberately not used by this project's ingestion (see `src/fundamentals.py`), which two-party-normalizes the raw topline and applies its own transparent recency/quality weighting instead, consistent with how `governor.csv`/`senate.csv` are handled.

581 polls, 2024-12-11 to 2026-09-08. As of acquisition (2026-09-11): recency-weighted national two-party D share ≈ 53.3% (D+6.6), consistent with independently-reported ~D+6.3 trackers for the same period.

### Modules built on this data (2026-09-11)

- **`src/ingest_polls.py`** — shared utilities for the 538-schema files (`governor.csv`, `senate.csv`, `approval_polls.csv`): `load_polls()` (state/cycle/office_type/stage filters, correct `%m/%d/%y` date parsing), `two_party_share()` (drops "don't know"/"someone else" rows before normalizing — confirmed this matters: an OH governor NYT/Siena poll reads 46.67/46.85 topline, which is 49.9% two-party D, not 46.7%), `recency_weight()`, `quality_weight()`, `weighted_average()`. Regression-checked against this session's ad hoc OH governor/Senate two-party means (49.74% / 50.55% — exact match).
- **`src/fundamentals.py`** — national aggregates only, built on `ingest_polls.py` plus a dedicated loader for `generic_ballot.csv`'s different schema: `national_generic_ballot_average()` (primary signal) and `national_approval_average()` (secondary/fallback — see below for why).
- **`src/national_to_state.py`** — translates the national generic-ballot average into an Ohio-equivalent statewide two-party D% via `OHIO_VS_NATIONAL_OFFSET_MEAN` (−5.63 pts, built from Ohio's 2020/2024 presidential vote vs. certified national two-party vote — 2 data points, not a regression, deliberately wide sigma). `build_environment_prior()` returns a `(summary, samples)` pair mirroring `governor.py::build_baseline_distribution()`'s output shape. Prefers the generic ballot over approval because research on midterm forecasting models finds presidential approval adds little predictive power once the generic ballot is known (adding it produces small, non-significant coefficients) — approval is used only as a crude fallback (`_approval_fallback_signal`, 1.5x sigma inflation) if `generic_ballot.csv` is ever unavailable.
  - As of 2026-09-11: translated OH environment = 47.7% D (±2.5 pts), vs. direct OH polling averages of 49.7% (governor) / 50.5% (Senate) — a 2-3 pt gap, roughly 1 sigma. This is a **prior**, not a final forecast, for governor/Senate specifically (candidate effects and offset noise both plausibly explain the gap); it remains the *sole* environment estimate for State House, State Senate, and US House, which have no direct race-level polling.

### `simulate.py` distribution mode (2026-09-11)

`run_simulations()` now accepts either a scalar `statewide_d` (unchanged point-simulation behavior — every simulation uses the same fixed environment) or a 1-D array of pre-drawn environment samples (e.g. from `national_to_state.build_environment_prior()`), one draw per simulation, producing an UNCONDITIONAL seat distribution that integrates over environment uncertainty rather than answering "if the environment is exactly X%." `config.n_sims` is ignored in array mode — the sample count is `len(statewide_d)`. `SimResult` gained an `environment_samples` field (`None` for point mode, the raw array otherwise); `SimResult.statewide_d` is always a float (the mean, for point mode trivially itself).

Verified: scalar-mode output is byte-identical to pre-change behavior (mean_seats=39.93 at 48% matches the Session 9 finding exactly); all downstream consumers (`run_probabilistic_scenario_table`, `compute_analytical_win_probs`, `build_investment_priority`, `run_defensive_scenarios`) smoke-tested with no regressions, since they only ever pass scalars. Distribution mode tested end-to-end with `national_to_state.build_environment_prior()`'s Ohio House samples: mean 39.4 seats (close to the point estimate at the prior's mean, 47.7%), but std 5.37 vs. 2.17 for the equivalent point simulation — correctly wider, since it now carries both environment and district-level uncertainty. P(≥40 seats) drops from 57.5% (conditional on exactly 48%) to 48.4% (unconditional, using the current fundamentals-only environment prior).

**Wired 2026-09-11**: `python cli.py forecast` (top-level, defaults to House; also `house forecast`), `senate forecast`, `congress forecast`. Each runs the full chain end to end — `forecast_statewide.build_forecast()` → `simulate.run_simulations()` distribution mode → prints the signal breakdown and unconditional seat forecast → writes `reports/session8/{office}_forecast_seat_distribution.csv` (one row per simulation: `sim_index`, `d_seats`, `statewide_d_pct`) and `reports/session8/{office}_forecast_district_detail.csv` (one row per district — see below). Flags: `--n-sims`, `--with-incumbency`, `--include-historical/--no-historical`, `--seed`.

### `forecast-trend` — this cycle's forecast reconstructed over time (2026-09-11)

`python cli.py forecast-trend [--office house|senate|congress] [--start YYYY-MM-DD] [--freq-days 7]` reruns `forecast_statewide.build_forecast()` at each of a sequence of past dates, using **only polls that existed on or before that date** — "what would this model have said on 6/1, 6/9, ...?" This is *not* a backtest against a past election (no historical 2018/2022 polling archive exists — the official FiveThirtyEight/ABC historical polls endpoints are dead, redirecting to abcnews.com; no working mirror covers pre-2024 cycles either, confirmed by checking). It replays this cycle's own polls forward in time. Writes `reports/session8/{office}_forecast_trend.csv`.

**This surfaced two real bugs, both now fixed:**

1. **Look-ahead bias in `ingest_polls.weighted_average()`.** `recency_weight()`'s age calculation clips negative ages to 0, so a poll dated *after* `as_of` was being treated as maximally fresh instead of excluded — asking for `as_of=2026-06-01` was silently still pulling in every poll through September. Fixed: `weighted_average()` now filters `df = df[df["end_date"] <= as_of]` before computing any weights, and raises if that leaves nothing (per the "no synthetic data" convention — fail loudly, don't fabricate a point-in-time estimate from data that didn't exist yet).
2. **NaN poisoning the blend when a race has only one poll.** `weighted_average()` correctly returns `NaN` for a single-poll's std (undefined, not zero) — but `_oh_race_signal()` was feeding that `NaN` straight into the precision-weighted blend, and `NaN` poisons a weighted sum: the entire `build_forecast()` output silently went `NaN`, which downstream became `mean_seats=0`. This is exactly the kind of early-cycle case `forecast-trend` exercises (only 1 OH governor/Senate poll existed in Feb–Apr 2025) that a "today only" workflow never would. Fixed: added `SINGLE_POLL_FALLBACK_SIGMA = 0.06` in `src/forecast_statewide.py` (matching this project's existing "insufficient data" convention, e.g. `compute_sigma_prior()`'s own 0.06 default) — used only when a race's own poll-spread is undefined.

**Result, House, Feb 2025–present**: the blended environment has drifted from 43.5% D (Feb 2025) to 48.2% D (2026-09-11), after the Senate/governor race adjustments (44.0% → 50.1% before them) — a real ~5-point Democratic drift over the cycle, not noise.

**Per-signal breakdown (2026-09-11)**: `forecast-trend`'s CSV also carries each individual component's own value at each date (`national_fundamentals_pct`, `governor_polls_pct`, `senate_polls_pct`, `historical_governor_pct` — the last one is a flat reference line since it's built from fixed past cycles, not live polls). Visualized as 4 lines on the artifact (national fundamentals, governor polls, senate polls, blended final) plus the historical baseline as a dashed flat reference — crosshair tooltip shows all values at once. This surfaced a real finding: governor and Senate polls **spiked together** in Feb–Mar 2026 while national fundamentals stayed flat, and by September Senate polls are running noticeably hotter (52.8% D) than governor polls (50.1%) and national fundamentals (47.7%) — worth investigating what drove the Feb–Mar spike before trusting it blindly. Since 2026-09-11, `senate_polls_pct` is the adjusted value used in the blend (raw − 3.7 pts, Brown's measured premium); `senate_polls_raw_pct` / `governor_polls_raw_pct` carry the unadjusted averages.

### Per-district detail at the blended environment (2026-09-11)

`compute_analytical_win_probs()` (`src/simulate.py`) gained an `environment_sigma: float = 0.0` parameter. At the default (every existing caller — scenario tables, win-prob curves, `build_investment_priority`, defensive scenarios), behavior and output schema are byte-identical to before. Passed nonzero, it treats `statewide_d` as the *mean* of an uncertain environment rather than a known point: district margin = environment + composite_lean + inc_shift + district noise, and since environment ~ N(statewide_d, environment_sigma) is independent of district noise ~ N(0, sigma_i), their sum is exactly Normal with `sigma = sqrt(environment_sigma² + sigma_i²)` — no per-simulation arrays needed, this is exact under the model. The returned `sigma_i` column is unchanged (always district-only); the combined sigma is used internally for `win_prob`/`marginal_wp` only.

`_run_office_forecast` (cli.py) uses this to build `{office}_forecast_district_detail.csv` — columns `district, composite_lean, tier, current_holder, open_seat_2026, margin_mean, margin_sigma, win_prob` — at the same blended environment (mean + sigma) and same `--with-incumbency` config as the seat distribution it explains, sorted by win_prob descending.

**Verified**: analytic per-district win_prob matches the Monte Carlo `run_simulations(..., env_samples)` path within 0.64 pts max / 0.14 pts mean (House, 10,000 sims each way), and both sum to ≈44.14–44.16, matching the published mean_seats exactly (E[seats] = Σ P(win_i) holds). `cli.py invest` output confirmed byte-identical before/after this change (via `git stash`), so no existing consumer's ranking or CSV schema changed.

**A framing trap worth flagging explicitly**: the count of districts with `win_prob > 0.5` (39 for House as of 2026-09-11) is *not* the same number as the sum of win probabilities / expected seats (39.7) — they're usually close but not identical, and neither is a claim about which specific districts will flip. The CLI and the visualization both print/display both numbers side by side rather than picking one and implying it's "the" answer.

**Bug found and fixed 2026-09-11**: the first version of `{office}_forecast_district_detail.csv` reused targeting.csv's `tier` column as-is next to the freshly-computed `win_prob`/`margin_mean`. Those two things are computed at different reference environments — `tier` is a snapshot at the fixed 48% reference from Session 13, while `win_prob` here is computed at whatever the blended environment actually is (currently 50.1%). For districts whose fundamentals sit between those two points (District 60: `flip_threshold` 49.19%, squarely between 48% and 50.1%), this produced rows that looked self-contradictory — "Lean R" tier sitting next to a 57% Democratic win probability — even though every individual number was correct. Fixed: `tier` in this file is now computed fresh from the file's own `win_prob` via `classify.assign_tier_from_wp()` (the same WP_TIER_THRESHOLDS bucketing used everywhere else); the old 48%-reference tier is kept alongside as `tier_at_48` for comparison, since the shift itself is informative, not something to hide. 34 of 99 House districts shifted tier between the two reference points, all in the same (more Democratic) direction — consistent with the environment estimate itself moving from 48% to 50.1%, not evidence of a further bug. The visualization surfaces this shift directly in each district's detail panel when it applies.

### `src/forecast_statewide.py` — the final poll+fundamentals blend (2026-09-11)

Combines four independent estimates of the shared latent "2026 Ohio generic partisan environment" via inverse-variance ("precision") weighting — `national_to_state.build_environment_prior()`, the direct OH governor polling average, the direct OH US Senate polling average, and `governor.py`'s historical 4-cycle baseline (included by default, `include_historical_governor_baseline=True`). Each race's poll average is first put on the reference scale using that race's measured deviation from the reference race in past Ohio cycles (`RACE_ADJUSTMENTS`, see "Reference race and district-noise split" above). Senate polls are shifted −3.7 pts (Brown's mean premium) with a 2.3-pt predictive sd. Governor polls get no shift but carry the 4.4-pt predictive sd of past governor-vs-reference gaps. `CANDIDATE_IDIOSYNCRASY_SIGMA` (0.02) is now only a floor under those measured spreads.

As of 2026-09-11 (`build_forecast()`):

| Signal | Raw D% | Adj | Mean D% | Sigma | N | Weight |
|---|---|---|---|---|---|---|
| national_fundamentals | 47.4% | — | 47.4% | 2.41 pts | 581 polls | 43.9% |
| governor_polls | 50.0% | 0 | 50.0% | 4.49 pts | 19 polls | 12.6% |
| senate_polls | 53.0% | −3.7 | 49.3% | 2.56 pts | 19 polls | 38.9% |
| historical_governor | 42.2% | — | 42.2% | 7.46 pts | 4 cycles | 4.6% |
| **BLENDED** | | | **48.2%** | **2.00 pts (floored)** | — | — |

Before the race adjustments, the same inputs blended to 50.0% (governor 34.7% weight, Senate 33.3% at 53.0%).

**Sigma is floored at `MIN_OHIO_SIGMA` (0.02)** rather than using the naive precision-weighted combination (1.60 pts after the race adjustments; 1.31 before). The floor covers error in estimating the statewide reference itself; the House's offset from the reference is a separate term (`simulate.compute_shared_sigma`). The naive combination treats all four signals as independent; they aren't fully independent — a systematic Ohio-wide polling bias this cycle would move the governor and Senate polling averages together, and that correlated-error component isn't modeled (full correlated multi-office posteriors are Phase C's job, not this pass's). The floor is a documented guard against the resulting overconfidence, not a fitted correction. Effect: flooring widened the Ohio House unconditional forecast from P(majority)=6.3% to a more defensible 12.7%, and the 80% seat CI from [40,49] to [38,50].

Verified end-to-end: `build_forecast()`'s samples feed directly into `simulate.py::run_simulations()`'s distribution mode with no adapter needed. Unconditional Ohio House forecast as of 2026-09-11: mean 39.8 seats, 80% CI [32, 47], P(≥34)=85.4%, P(≥40)=51.7%, P(majority)=5.3% (before the reference-race and σ changes: 44.1, [38, 50], 84.1%, 12.7%).

**Explicitly out of scope for this module** (see module docstring): a governor- or Senate-race-*specific* point forecast (e.g. "Acton's predicted vote share") — that would require separating each race's own idiosyncratic deviation from the shared environment used to estimate it, which isn't a clean thing to do without double-counting. This module only produces the shared generic-environment posterior that feeds down-ballot (House/State Senate/Congress) projections; `governor.py`'s own machinery is untouched.

### Known gaps
- **The GUI doesn't use the Phase B forecast yet.** The CLI does (`forecast`, `<office> forecast`, `forecast-trend`); the dashboard still only exposes the manual statewide-D% slider.
- **No pre-2025 national generic-ballot or approval time series exists in the repo.** A self-calibrated "national environment → Ohio environment" regression (the roadmap's originally-stated MVP approach) can't be fit from Ohio's own history without it — `OHIO_VS_NATIONAL_OFFSET_MEAN` is a 2-point cited reference instead, not a fit coefficient. No historical backtest of `national_to_state.py` is possible until this is acquired.
- **No state-level polling for State Senate, State House, or US House races**. Forecast for those offices is fundamentals-only by necessity (`national_to_state.py`'s translated environment + composite-lean projection). Be transparent about this in the public methodology.
- **Approval data is Trump-only** — no Biden retrospective averages in either approval file. Adequate for current cycle, but limits historical calibration, and approval is now a secondary/fallback signal rather than primary (see above).

## Validation Standards

- **Vote reconciliation:** precinct totals must equal district totals within floating-point tolerance after every crosswalk.
- **Statewide benchmarks:** computed statewide two-party D share must match known election results within 0.5 points.
- **Match rates:** SOS-to-VEST precinct join must exceed 90% by vote count. Below that, stop and investigate.
- **No synthetic data.** Every number traces to a source file. If data is missing, document the gap — never fabricate.

## Known Limitations

- Area-weighted crosswalk assumes uniform voter distribution within precincts. Weakest for large rural precincts.
- Precinct boundaries shift between elections. Each year needs its own geometry; crosswalking across years requires either VEST shapefiles for that year or a precinct-to-block assignment from the voter file.
- Composite weights are analyst judgments, not empirically optimized. Session 4 regression can inform weight calibration.
- Uncontested races (~30-40% of Ohio House seats in a typical cycle) are information voids for candidate effect estimation.
- The shared year-level σ (`compute_shared_sigma`, 2.35 pts for the House) is estimated from four cycles, and effectively from 2022 vs. 2024. It widens every published interval; treat it as provisional until 2026 results are in.
- `swing_sd` is the raw sd of House D share across years, so it still contains year-level shifts. It feeds σ_i via EB shrinkage.

### Voter file limitations (Session 8)

- **Ohio has open primaries.** Any voter can pull any party's ballot. A voter pulling an R ballot doesn't make them a Republican — they may be a Democrat voting in a competitive R primary. The `partisan_lean` score is a signal, not a definitive party ID. Treat `crossover` and `unaffiliated` classifications conservatively.
- **Primary participation is sparse.** Many voters never vote in any primary. The `unaffiliated` category will be large (typically 40–60% of active voters). These voters are genuinely unknown partisans, not centrists.
- **Voter file is a snapshot.** The file reflects registrations as of the download date. Voters register, move, and die continuously. The structural metrics (partisan composition, turnout patterns) are stable; the contact universe needs refreshing closer to November 2026.
- **No vote choice data.** The voter file confirms who voted in each election. It does NOT reveal who they voted for. Partisan lean is inferred from primary history, not general election behavior.
- **INACTIVE voters are a wildcard.** INACTIVE status means the voter has not confirmed registration after being flagged (usually for not voting and not responding to mailings). Some are genuinely gone; some are reachable. Contact universe uses ACTIVE voters only; inactive counts are metadata.
- **PII policy.** The SOS voter file is public record, but this tool never writes names, addresses, or DOB to any output. `--export` writes voter_id + classification scores only. Operatives join on voter_id to their own SOS/VAN copy for contact details.

## Conventions

- All partisan metrics are expressed from the Democratic perspective. Positive = more Democratic.
- "Lean" always means relative to statewide average, not absolute vote share.
- District numbers are 1–99 (Ohio House). When discussing Ohio Senate, always specify "Senate District X" to avoid ambiguity.
- Election years refer to November general elections unless otherwise specified.
- CRS: EPSG:3735 for all spatial operations.
