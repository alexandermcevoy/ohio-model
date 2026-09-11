# Ohio House Election Model — Roadmap

*Last updated: April 16, 2026*

---

## Vision

A durable analytical platform for Ohio state legislative elections that any Democrat — from a county party chair to a statewide campaign director — can use to make smarter resource allocation decisions. The platform should survive redistricting cycles, ingest new election data cleanly, and produce outputs that are auditable, defensible, and honest about uncertainty.

---

## Current State: v2.3 (Sessions 1–13 complete)

The analytical engine is operational, validated, and producing actionable outputs — now with a block-level data backbone and a stakeholder-facing web GUI. What's built:

- **Composite partisan lean** for all 99 Ohio House districts, validated against DRA (Spearman ρ = 0.9985). Built from 9 statewide races across 4 election cycles (2018–2024), crosswalked to current district geography via population-weighted precinct-to-district overlay.
- **Census Block Backbone** (Session 10): 219,669 populated Census 2020 blocks as the canonical storage unit. Precinct votes disaggregated to blocks, then reaggregated to any district map on demand. 8 election years (2010–2024) ingested. Block-level vote surfaces validated against existing composite (max diff 0.003 pts). Redistricting contamination eliminated structurally.
- **Per-district partisan trend** from 8 election cycles (2010–2024), computed from block vote surfaces. Trend slope, R², direction merged into targeting CSV.
- **Stakeholder GUI** (Session 11): Streamlit multi-page dashboard with 5 views — Scenario Explorer, Pickup Portfolio, District Profiles, Map (choropleth), Investment Priority. Live recomputation via Monte Carlo when slider changes. Deployed on Streamlit Cloud.
- **Three-map redistricting filter**: two-transition Jaccard overlap check (old→interim and interim→final) identifies which house election years are reliable per district. 13 districts have all 4 years; 73 have 2022+2024; 13 have only 2024.
- **District classification** into 7 win-probability tiers (Cook-style: Safe D → Safe R) at three reference environments (46/48/50% statewide D). Composite lean kept as raw number (D+/R+ format). Session 13 overhauled terminology to align with political convention.
- **Probabilistic Monte Carlo scenario model** (10,000 sims per environment) with Empirical Bayes district-level uncertainty, analytical win probabilities, marginal win probability for investment prioritization, path-to-target optimizer, and defensive scenarios.
- **Pickup portfolio classification**: Core (7 districts viable across all plausible 2026 environments), Stretch (5 districts viable in good cycles), Long-Shot (6 districts, wave only). Three open seats (31, 52, 35) are highest-value opportunities.
- **Voter file integration**: Ohio SOS voter file (~7.9M records) scored for partisan lean, turnout propensity, and presidential-only status. Per-district mobilization and persuasion universes. Contact export for field use.
- **2026 open seat tracking**: 9 known open seats (7 R-held, 2 D-held) flagged in all targeting outputs.
- **GLM regression** (exploratory only — 2-cycle post-redistricting panel). R² = 0.954. Used for candidate effect residuals, not primary projections.
- **ACS demographic overlay** at the district level (college attainment, income, race, density, etc.).
- **PDF district profiles** for all 99 districts — includes probabilistic outlook, voter universe, open seat status, redistricting status, anomaly flags.
- **Validation suite**: anomaly detection, drop-one sensitivity, external DRA validation, 2024 backtest, monotonicity checks.
- **Methodology document v3.0** (`reports/methodology.md`, 15 sections) — fully current.
- **CLI** with simulate, invest, win-prob, session8, voters, report, targets, scenario, defense, open-seats, backbone, trends, and more.

**Known limitations:**
- County-level historical data (2010–2014) is coarser than precinct-level (88 counties vs ~9k precincts). Acceptable for trend analysis but not for fine-grained district profiling.
- Incumbency cannot be estimated from Ohio data (2-cycle post-redistricting panel); literature prior (6 pts) used optionally.
- VEST 2022/2024 shapefiles not yet acquired (subscription required). Currently using SOS→VEST 2020 precinct join for those years (98–99% match rate).
- Voter file not yet geocoded to blocks — still uses district assignment from voter registration, not centroid mapping.

---

## Completed: v2.0 — Block Backbone Architecture (Session 10) ✅

Census 2020 blocks (~220k populated) as the canonical storage unit. 8 election years (2010–2024) disaggregated to blocks. Validated against existing composite (max diff 0.003 pts). Per-district partisan trend (2010–2024) computed and merged into targeting. See CLAUDE.md Session 10 log for details.

---

## Completed: v2.1 — Stakeholder GUI (Session 11) ✅

Streamlit multi-page dashboard with 5 views: Scenario Explorer, Pickup Portfolio, District Profiles, Map (choropleth), Investment Priority. Live Monte Carlo recomputation. Deployed on Streamlit Cloud. See CLAUDE.md Session 11 log for details.

---

## Completed: v2.2 — Historical Backtest (Session 12) ✅

Out-of-sample validation: pre-2024 composite (2016–2022 only) predicts 2024 house outcomes. 98% binary accuracy, 94.9% competitive district accuracy, Brier skill 0.878, actual 34 seats in MC 80% CI [30, 35]. Only 2 misclassifications (both D candidate overperformance in lean_d districts). See CLAUDE.md Session 12 log for details.

---

## Completed: v2.3 — Tier Terminology Overhaul (Session 13) ✅

Tiers changed from composite-lean-threshold-based to win-probability-based (Cook-style). Tiers shown at three reference environments: 46% (bad cycle), 48% (neutral midterm), 50% (good cycle). Composite lean kept as raw number with D+/R+ display format. Eliminates confusion where "Lean D" implied a projected win but actually described relative partisan position. See CLAUDE.md Session 13 log for details.

---

## In Progress: v3.0 — Public-Facing Four-Office Forecast

Strategic pivot from House-only conditional projection model to a published forecast covering Governor 2026, State House, State Senate, and US House. The conditional-projection layer (existing infrastructure) becomes the analytical depth backing a marginal forecast (new). Audience shifts from private analytical tool pitched to ODP leadership → public forecast that reporters cite.

Four phases. Full plan at `~/.claude/plans/iterative-coalescing-candle.md`.

### Phase A — Office expansion foundation (Session 14, ~80% complete)

Generalize the existing pipeline from 99 House districts to four offices.

- ✅ A.1 — Shapefiles acquired: `data/shapefiles/sldu_2026/` (Sept 2023 Senate plan), `data/shapefiles/cd_2026/` (NEW Oct 31, 2025 congressional plan).
- ✅ A.2 — `src/office.py`: `OfficeConfig` dataclass + `OFFICES` registry. Single source of truth for n_districts, paths, chamber thresholds, SOS race attributes.
- ✅ A.3 — SOS parser extended to extract `senate` and `congressional` race dicts.
- ✅ A.4 — `src/ingest_house_results.py` → `src/ingest_race_results.py`, office-aware.
- ✅ A.5 — Block-district maps for senate (`block_district_map_senate_2024.parquet`) and congress (`block_district_map_congress_2026.parquet`).
- ✅ A.6 — Block votes for US House: `build_composite_from_blocks` extended.
- ✅ A.7 — Hardcoded constants generalized: `scenarios.py`, `classify.py`, `backbone.py` all accept `OfficeConfig`.
- ✅ A.8 — CLI restructure: typer subcommand groups (`house`, `senate`, `congress`, `governor`, `multi`); top-level commands auto-aliased into `house <cmd>` for back-compat.
- ✅ A.9 — End-to-end pipeline: `<office> composite → classify → simulate` working for senate and congress.
- ◐ A.10 — In progress.
  - ✅ Per-office summary PDFs (`<office> report` CLI; outputs at `reports/session14/{prefix}_office_summary.pdf`). One summary PDF per office (lighter than 48 sparse per-district PDFs) per plan guidance.
  - ⏳ External validation (DRA congressional CSV for US House analog of Session 5; senate has no DRA equivalent — needs alternative).
  - ⏳ Backtest extension to senate/congress.
  - ⏳ GUI multi-office support.
  - ⏳ Governor "office" treatment (statewide degenerate case).

**Sanity checks**:
- Senate: SD-21 D+38 (Cleveland) safest D, SD-12 R+23 safest R. At 48% statewide D, mean 12.4 of 33 senate seats (current 9D); majority requires ~53%.
- Congress (NEW Oct 2025 map): CD-11 D+33 safest D; CDs 1, 9, 10, 15 all near-tossups. At 48% statewide D, mean 5.1 of 15 (current 5D); P(+1) = 37%, P(+2) = 13%.

### Phase B — Statewide environment forecast (governor first, MVP nowcast)

Replace the user-supplied statewide D% slider with a probability distribution derived from polls + fundamentals. Pilot on governor (highest-profile, most polling, simplest single race).

**Data refreshed 2026-09-11**:
- `data/polls/governor.csv` — 5,332 rows nationwide; 22 unique Ohio 2026 governor general polls, 16 pollsters incl. NYT/Siena. Direct OH two-party D mean: 49.7%.
- `data/polls/senate.csv` — new; 19 unique Ohio 2026 US Senate special polls (Brown vs. Husted), 13 pollsters incl. NYT/Siena. Direct OH two-party D mean: 50.5%.
- `data/polls/approval_polls.csv` — new; poll-level national Trump approval (1,221 rows), supersedes `president-averages.csv` as the primary approval source.
- `data/polls/generic_ballot.csv` — new; Silver Bulletin national generic-ballot polls (581 rows, Dec 2024–Sept 2026). **Closes the generic-ballot gap** flagged below — this is the primary national signal `national_to_state.py` now uses, not presidential approval (research on midterm forecasting models finds approval adds little once the generic ballot is known).

**Built and verified 2026-09-11**: `src/ingest_polls.py` (shared 538-schema loading, two-party normalization, recency/quality-weighted averaging — reused across all three race-level poll files), `src/fundamentals.py` (national generic-ballot + approval aggregates), `src/national_to_state.py` (translates the national generic-ballot average to an Ohio-equivalent statewide D% via a small cited offset, `OHIO_VS_NATIONAL_OFFSET_MEAN` in `src/national_to_state.py`, built from Ohio's 2020/2024 presidential vote vs. certified national two-party vote — 2 data points, deliberately wide sigma). As of 2026-09-11: national generic ballot D+6.6, translated OH environment 47.7% D (±2.5 pts) — about 2-3 pts below the two direct OH polling averages, a plausible gap (candidate effects, offset noise) that `forecast_statewide.py` will need to reconcile, not evidence of a bug.

**Built and verified 2026-09-11**: `simulate.py::run_simulations()` now accepts either a scalar `statewide_d` (unchanged) or a 1-D array of pre-drawn environment samples, producing an unconditional seat distribution (verified byte-identical scalar-mode output + no regressions in `run_probabilistic_scenario_table`/`compute_analytical_win_probs`/`build_investment_priority`/`run_defensive_scenarios`; distribution-mode smoke test with `national_to_state.build_environment_prior()`'s OH House samples: mean 39.4 seats, std 5.37 vs. 2.17 for the equivalent point simulation, P(≥40) 48.4% unconditional vs. 57.5% conditional on exactly 48%).

**Built and verified 2026-09-11**: `src/forecast_statewide.py` blends the national fundamentals prior, direct OH governor polls, direct OH Senate polls, and `governor.py`'s historical 4-cycle baseline via inverse-variance weighting (governor/Senate poll sigmas inflated by a documented `CANDIDATE_IDIOSYNCRASY_SIGMA`; final sigma floored at `MIN_OHIO_SIGMA`=0.02 to guard against the naive combination's overconfidence from treating all signals as independent when Ohio-wide correlated polling error isn't modeled — that's Phase C's job). As of 2026-09-11: blended environment 50.1% D ± 2.0 pts (governor polls 35.5% weight, Senate polls 33.8%, national fundamentals 27.6%, historical baseline 3.1%). Feeds `simulate.py` distribution mode directly with no adapter: unconditional Ohio House forecast — mean 44.1 seats, 80% CI [38,50], P(≥40)=84.1%, P(majority)=12.7%.

To do: wire this into `cli.py` (no command uses the Phase B chain yet — only the manual `--statewide-d` scalar workflow exists) and GUI. Forecast snapshot versioning to `forecasts/YYYY-MM-DD/{office}/`. Phase C's correlated multi-office posterior would also let `MIN_OHIO_SIGMA`'s blunt floor be replaced with an actual modeled correlated-error term.

### Phase C — Multi-race correlation + heterogeneous swing

Extend the forecast from one race to all four offices, with internally consistent (correlated) outcomes. Replace uniform-swing-plus-noise with a structured swing model.

To do: `src/swing_decomposition.py` (PCA on residuals from uniform swing, 2 components max), `src/lean_drift.py` (forward-project composite lean to 2026 using `trend_slope`, demographic change, generational replacement). Joint multivariate posterior across (gov, USS-baseline, generic, statehouse-baseline).

### Phase D — Public launch layer

Brand, domain, polished public Streamlit deployment, methodology page, weekly cadence, post-election scorecard.

---

## Data Acquisition (ongoing)
- **County-level results 2000–2008** (free, Ohio SOS) — extends trend analysis to 25 years. The backbone already handles county-level disaggregation.
- **VEST 2022/2024 shapefiles** (subscription required) — enables geometry-aware block disaggregation for post-redistricting cycles.
- ~~**Generic ballot time series**~~ — done 2026-09-11, `data/polls/generic_ballot.csv` (Silver Bulletin, 581 polls).

---

## Deferred

These items were previously on the near-term roadmap but have been deprioritized:

### Natural Language Query Interface (was Session 10)
Claude API integration for freeform queries (`ohio-house ask "Which districts..."`). **Deprioritized because:** the GUI provides structured exploration that covers the most common stakeholder questions more reliably than freeform NL. May revisit post-GUI if stakeholders need query patterns the GUI doesn't cover.

### Further Resource Allocation Model Refinements
The current greedy optimizer and marginal WP ranking are sufficient for strategic guidance. Further refinements (multi-objective optimization, spending elasticity curves, diminishing returns modeling) would add complexity without proportional strategic value. The core insight — invest in tossup districts with high marginal WP, especially open seats — doesn't change with a fancier optimizer.

---

## Data Acquisition Priorities

| Data | Source | Cost | Priority | Unlocks |
|------|--------|------|----------|---------|
| County-level results 2000–2008 | Ohio SOS / Dave Leip's Atlas | Free | **High** | Extend trend analysis to 25 years (2010–2016 already ingested) |
| VEST 2022/2024 shapefiles | UF Election Lab (subscription) | TBD | **High** | Geometry-aware block disaggregation for post-redistricting cycles |
| ~~VEST 2018 shapefile~~ | ~~UF Election Lab (free)~~ | ~~Free~~ | ~~Done~~ | ~~Ingested in Session 10~~ |
| ~~Census 2020 block shapefile~~ | ~~Census Bureau~~ | ~~Free~~ | ~~Done~~ | ~~219,669 blocks loaded in Session 10~~ |
| Follow the Money campaign finance | followthemoney.org | Free | Medium | Spending data for regression, disentangle candidate quality |
| L2 modeled voter file | L2 Inc. | $$$ | Low | Pre-modeled partisan scores — less needed now that we have SOS voter file scored |
| Census 2030 blocks | Census Bureau | Free (2031) | Future | Required for post-2030 redistricting |

---

## Future Sessions (Post-v2.2)

### Ohio Senate Extension
- Same methodology applied to Ohio Senate (33 districts, 4-year staggered terms).
- Reaggregate block vote surface to Senate districts — the Senate is just a different aggregation of the same underlying data.
- Separate composite lean, targeting, scenario, and probabilistic outputs.

### Historical Backtest
- Use 2016–2020 data to "predict" 2022 results; use 2018–2022 to "predict" 2024.
- Out-of-sample validation of composite index predictive power.
- Strong credibility claim for 2028 if the model would have correctly identified 2024 targets using only pre-2024 data.

### Campaign Finance Overlay
- Ingest Follow the Money / FEC data for Ohio House races.
- Add spending as a predictor in the regression.
- Model diminishing returns to spending per district.

### 2026 Post-Election Ingest
- After November 2026 results are certified:
  - Download SOS precinct-level XLSX.
  - Disaggregate to blocks via block backbone, reaggregate to districts.
  - Update composite lean (add 2026 races, adjust weights to favor recency).
  - Backtest: how well did the model predict 2026 outcomes?
  - Generate post-election report comparing projections to results.
  - This is the credibility test. If the pickup portfolio correctly identified the competitive races, that's the foundation for 2028 resource conversations.

---

## Architecture Evolution

```
v1.6 (Sessions 1–9)    v2.0 (Session 10)         v2.1 (Session 11)         v2.2+ (next)
──────────────────      ─────────────────         ─────────────────         ────────────
Precincts → Districts   Precincts → Blocks →      Same backbone +           Backtest validation,
                        Districts                  Streamlit GUI             deeper history

Population-weighted     Same crosswalk,            GUI reads from            Out-of-sample
block centroid join     generalized to             backbone outputs          predictive test
                        any district map

House results by        House results via          Interactive district      Pre-2024 composite
district number         block reaggregation        profiles, scenario        predicts 2024
(redistricting-         (structurally              explorer, maps
filtered)               contamination-free)

4 election cycles       8 election cycles          Stakeholder self-         25 years of history
(2018–2024)             (2010–2024)                service access            (2000–2024)
```

---

## Recurring Maintenance

**After each election cycle (every 2 years):**
1. Download SOS precinct-level results.
2. Obtain precinct shapefile for that year (VEST, SOS, or county-by-county).
3. Disaggregate to blocks, reaggregate to districts.
4. Update composite lean (adjust weights to favor recency).
5. Rerun regression and probabilistic model.
6. Regenerate all reports, scenario tables, and portfolio classification.
7. Post-election backtest.

**After redistricting (every 10 years):**
1. Obtain new district shapefile (TIGER/Line SLDL).
2. Reaggregate all historical block vote surfaces to new districts.
3. Recalibrate tier thresholds if district count or population targets change.
4. All historical data is immediately available on new maps — no rebuild required.

**After Census (every 10 years):**
1. Obtain new Census block shapefile with updated population.
2. Build block-to-block crosswalk (Census Bureau publishes these).
3. Migrate historical block vote surfaces to new block geography.
4. Update ACS demographics.

---

## Success Metrics

The tool is successful if:

1. **Accuracy.** Post-2026, the composite lean correctly rank-ordered the competitive districts. The probabilistic model's predicted seat distribution at the actual statewide environment contains the actual result within the 80% CI.

2. **Adoption.** At least one Ohio Democratic campaign operative or county party uses the tool's outputs to inform resource allocation in 2026. The GUI makes this possible without technical intermediation.

3. **Durability.** The 2028 election can be ingested and analyzed without architectural changes. The 2032 redistricting can be handled with a single reaggregation command.

4. **Credibility.** No one finds a number that doesn't trace to a source. The methodology doc answers every challenge. The external validation holds.

---

## Current Execution Order

1. ☑ Sessions 1–5 — core pipeline (ingest, crosswalk, composite, classification, reporting)
2. ☑ Redistricting bugfix — three-map Jaccard check, contamination filter
3. ☑ Session 6 — 2026 open seat tracking, candidate name extraction
4. ☑ Session 7 — validation suite, methodology v2.0, CLAUDE.md data schema
5. ☑ Session 8 — voter file integration
6. ☑ Session 9 — probabilistic scenarios, pickup portfolio, resource allocation
7. ☑ Session 10 — block backbone architecture (2010–2024, 8 election years)
8. ☑ Session 11 — stakeholder GUI (Streamlit, deployed to Cloud)
9. ☑ Session 12 — historical backtest (98% accuracy, Brier skill 0.878)
10. ☑ Session 13 — tier terminology overhaul (WP-based tiers, Cook-style)
11. ◐ Session 14 — Phase A multi-office expansion (composite/classify/simulate done for senate + congress; validation, PDFs, GUI extension pending)
12. ◐ A.10 — Phase A finishing work: per-office summary PDFs ✅; external validation (DRA congressional), backtest extension, GUI multi-office support, governor degenerate case all still ⏳
13. ◐ Phase B — governor MVP nowcast: full chain built and verified (2026-09-11) — `src/ingest_polls.py`, `src/fundamentals.py`, `src/national_to_state.py`, `src/forecast_statewide.py`, and `simulate.py`'s scalar→distribution change all done. Blended Ohio environment: 50.1% D ± 2.0 pts. Only `cli.py`/GUI wiring remains ⏳.
14. ☐ Phase D partial — brand, domain, public launch with governor forecast + state house conditional surface
15. ☐ Phase C — multivariate posterior, heterogeneous swing, lean drift, four-office joint forecast
16. ☐ Phase D full — multi-office public launch, weekly cadence, post-election scorecard
17. ☐ Acquire county-level results 2000–2008
18. ☐ Check VEST subscription pricing for 2022/2024 shapefiles
19. ☑ Acquire/curate generic ballot time series for Phase B `national_to_state.py` — `data/polls/generic_ballot.csv` (Silver Bulletin, 581 polls, Dec 2024–Sept 2026)

---

*This roadmap is a living document. Update after each session with findings, revised priorities, and new items.*
