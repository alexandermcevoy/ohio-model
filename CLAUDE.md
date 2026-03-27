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

**Current state (v1.6):** Composite partisan lean validated against DRA (ρ = 0.9985), 7-tier district classification, fundamentals-only uniform swing scenarios + probabilistic Monte Carlo scenarios with district-level uncertainty, GLM regression (exploratory), ACS demographics, voter file integration, resource allocation optimizer, PDF district profiles with probabilistic outlook, CLI, anomaly detection, drop-one sensitivity, full three-map redistricting filter, CLAUDE.md data schema.

**Next (v2.0): Block Backbone Architecture** — Census blocks become the canonical storage unit, eliminating redistricting contamination structurally and enabling instant reaggregation to any district map. Also unlocks ingestion of county-level 2000–2016 historical data for long-term trend analysis. This is the infrastructure foundation that makes everything else durable.

**Next (v2.1): Stakeholder GUI** — a web-based interface for campaign operatives to explore the model's outputs interactively: scenario explorer, district profiles, pickup portfolio visualization, win probability curves. Turns CLI-only analytical infrastructure into a tool stakeholders can actually use without technical assistance.

**Deferred:** Natural language query interface (Claude API integration) — unnecessary for current stakeholder needs. Further resource allocation model refinements — current optimizer is sufficient; diminishing returns on additional complexity.

**Future (v3.0+):** Ohio Senate extension, campaign finance overlay, 2026 results ingestion + backtest validation.

**Design principles across all sessions:**
- The composite lean (statewide race pipeline) is the anchor. It's geometry-aware, externally validated, and unaffected by house race data quality issues.
- No synthetic data. Every number traces to a public source.
- Surface uncertainty honestly. `insufficient_data` over false confidence. Confidence intervals over point estimates. Caveats on the face of the output, not in footnotes.
- The crosswalk is the moat. It's the technical infrastructure that makes every downstream analysis defensible.
- Build for durability. New election data should ingest cleanly. Redistricting should be a reaggregation, not a rebuild.

**Redistricting context:** Ohio used three different district maps in our data window. Pre-2020 (old maps), 2022 (interim maps struck down by Ohio Supreme Court but used for elections), 2024 (final maps adopted September 2023). Any cross-year comparison of house race results must account for which map was in effect. See `reports/redistricting_overlap.csv` for per-district reliability flags. The composite lean is unaffected because it routes through the spatial crosswalk, not district numbers.

**2026 targeting intelligence:** Multiple top pickup targets are open seats in 2026 due to term limits and retirements (Districts 31, 35, 39, 44, 52 confirmed). Open seat status is tracked in the targeting CSV and should be prominently surfaced in all targeting outputs. The incumbency advantage (~6-8 points) disappearing from these seats is the single largest shift in win probability available. The probabilistic model classifies targets into a **pickup portfolio**: Core (7 districts viable across all plausible environments), Stretch (5 districts viable in good cycles), and Long-Shot (6 districts, wave only). See `reports/session8/README.md` for the full portfolio analysis.

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
For contested state house races: actual D share minus composite partisan lean. Measures how much a candidate over/underperformed district fundamentals. Only meaningful for contested races; uncontested races are flagged but excluded from this calculation.

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
| `data/processed/block_geometry.parquet` | one row/block | ~220k | `block_geoid` |
| `data/processed/block_county_map.parquet` | one row/block | ~220k | `block_geoid` |
| `data/processed/block_precinct_map_{year}.parquet` | one row/block | ~220k | `block_geoid` |
| `data/processed/block_district_map_2024.parquet` | one row/block | ~220k | `block_geoid` |
| `data/processed/block_votes_{year}.parquet` | long, block × race | variable | `block_geoid`, `race` |
| `data/processed/oh_house_district_trends.csv` | wide, one row/district | 99 | `district` |

The **primary analytical join** is `targeting.csv ← composite_lean.csv ← redistricting_overlap.csv`, all on `district`. The targeting CSV already contains the most-used columns from composite lean.

After running `voters --build`, the targeting CSV also contains voter-file-derived columns (see `oh_house_voter_universe.csv` schema below). The old aggregate-based `target_mode` is retained as `target_mode_aggregate`; `target_mode_voterfile` is the authoritative mode going forward.

---

### `reports/session3/oh_house_targeting.csv` — primary analytical file

| Column | Type | Description |
|---|---|---|
| `district` | int | Ohio House district number, 1–99 |
| `composite_lean` | float | Weighted composite partisan lean (D − statewide). Positive = more D. Range: −0.20 to +0.43 |
| `tier` | str | safe_d / likely_d / lean_d / tossup / lean_r / likely_r / safe_r |
| `current_holder` | str | D or R (2024 winner) |
| `holder_matches_tier` | bool | Whether 2024 winner matches tier prediction |
| `pickup_opportunity` | bool | R-held district in tossup, lean_r, or likely_r tier |
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

Populated by `src/validate.py::detect_anomalies()`. Currently 10 rows (all 2022, all redistricting_artifact).

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
{"2018": 0.4807, "2020": 0.4592, "2022": 0.3746, "2024": 0.4434}
```

Statewide two-party D share for the reference race each cycle (governor for midterms, president for presidential years). Used to compute expected D share in anomaly detection and candidate effects.

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
| `sigma_i` | float | District-level outcome noise (EB-shrunk) |
| `win_prob` | float | P(D wins) at reference environment |
| `marginal_wp` | float | dP(win)/d(lean) — "bang for buck": how much a small D lean improvement increases win probability. Highest near 50% WP (tossups), near zero for safe seats. Formula: φ(margin/σ_i)/σ_i |
| `investment_rank` | int | Priority rank (1 = highest return) |
| `tier` | str | District tier for reference |
| `open_seat_2026` | bool | Open seat flag |

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
    ["district","win_prob","sigma_i","sigma_source"]]
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
- Tiered all 99 districts (safe_d → safe_r) using ±3/8/15-pt composite lean thresholds.
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

### Deferred: Natural Language Query Interface
- Claude API integration — pushed back; unnecessary for current stakeholder needs.
- May revisit if stakeholders need freeform querying beyond what the GUI provides.

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
