# Ohio House Election Model — Roadmap

*Last updated: March 26, 2026*

---

## Vision

A durable analytical platform for Ohio state legislative elections that any Democrat — from a county party chair to a statewide campaign director — can use to make smarter resource allocation decisions. The platform should survive redistricting cycles, ingest new election data cleanly, and produce outputs that are auditable, defensible, and honest about uncertainty.

---

## Current State: v1.6 (Sessions 1–9 complete)

The analytical engine is operational, validated, and producing actionable outputs. What's built:

- **Composite partisan lean** for all 99 Ohio House districts, validated against DRA (Spearman ρ = 0.9985). Built from 9 statewide races across 4 election cycles (2018–2024), crosswalked to current district geography via population-weighted precinct-to-district overlay.
- **Three-map redistricting filter**: two-transition Jaccard overlap check (old→interim and interim→final) identifies which house election years are reliable per district. 13 districts have all 4 years; 73 have 2022+2024; 13 have only 2024.
- **District classification** into 7 tiers (safe D → safe R) with targeting mode (persuasion / mobilization / hybrid / structural / insufficient_data).
- **Probabilistic Monte Carlo scenario model** (10,000 sims per environment) with Empirical Bayes district-level uncertainty, analytical win probabilities, marginal win probability for investment prioritization, path-to-target optimizer, and defensive scenarios.
- **Pickup portfolio classification**: Core (7 districts viable across all plausible 2026 environments), Stretch (5 districts viable in good cycles), Long-Shot (6 districts, wave only). Three open seats (31, 52, 35) are highest-value opportunities.
- **Voter file integration**: Ohio SOS voter file (~7.9M records) scored for partisan lean, turnout propensity, and presidential-only status. Per-district mobilization and persuasion universes. Contact export for field use.
- **2026 open seat tracking**: 9 known open seats (7 R-held, 2 D-held) flagged in all targeting outputs.
- **GLM regression** (exploratory only — 2-cycle post-redistricting panel). R² = 0.954. Used for candidate effect residuals, not primary projections.
- **ACS demographic overlay** at the district level (college attainment, income, race, density, etc.).
- **PDF district profiles** for all 99 districts — includes probabilistic outlook, voter universe, open seat status, redistricting status, anomaly flags.
- **Validation suite**: anomaly detection, drop-one sensitivity, external DRA validation, 2024 backtest, monotonicity checks.
- **Methodology document v3.0** (`reports/methodology.md`, 15 sections) — fully current.
- **CLI** with simulate, invest, win-prob, session8, voters, report, targets, scenario, defense, open-seats, and more.

**Known limitations:**
- House race results joined by district number, not geometry — redistricting filter mitigates contamination but is a bandaid, not architectural. Block backbone solves this structurally.
- Historical data limited to 2018–2024. No long-term trend context (2000–2016) to assess whether current competitive leans are durable or transient.
- Incumbency cannot be estimated from Ohio data (2-cycle post-redistricting panel); literature prior (6 pts) used optionally.
- All outputs are CLI-only. Stakeholders need technical assistance to explore the model.

---

## Next: v2.0 — Block Backbone Architecture (Session 10)

The foundational infrastructure upgrade. Makes Census blocks the canonical storage unit. Every election's precinct results are disaggregated to blocks, then reaggregated to any district map on demand.

**What it solves:**
- **Redistricting contamination eliminated structurally** — historical data maps to current geography through blocks, not district numbers. The Jaccard filter becomes unnecessary.
- **Future redistricting (2032) is a one-command reaggregation**, not a rebuild. All historical data is instantly available on new maps.
- **Long-term trend analysis unlocked** — county-level 2000–2016 results can be ingested and disaggregated to blocks, giving 25+ years of partisan trajectory context at the district level.
- **Adding new election years is a single ingest step**, not a new crosswalk.
- **Voter file plugs directly into the block layer** — geocoded addresses map to blocks, enabling exact (not population-weighted) vote allocation.

**What it requires:**
- Census 2020 block shapefile with population (~300k Ohio blocks).
- VEST precinct shapefiles for each election year (have 2020; 2018 free from VEST; 2022/2024 require subscription).
- County-level results for 2000–2016 (freely available from Ohio SOS / Dave Leip's Atlas).
- Computational resources for block-precinct spatial joins (manageable with county-by-county chunking — already proven in pop-weight table build).

**What it produces:**
- `data/processed/block_vote_surface_{year}.parquet` — one row per (block, race), canonical storage format.
- `src/backbone.py` — block disaggregation, reaggregation engine, county-level historical ingest.
- Updated composite lean pipeline routing through block layer instead of direct precinct-to-district crosswalk.
- County-level trend analysis outputs: 25-year partisan trajectories for all 88 Ohio counties.

---

## Next: v2.1 — Stakeholder GUI (Session 11)

The analytical layer is mature. The bottleneck is access — stakeholders can't explore the model without running CLI commands and reading CSVs. A web-based GUI turns the platform into a tool operatives can actually use.

**Core views:**
- **Scenario Explorer** — slider for statewide D%, live-updating seat distribution chart, probability thresholds (P(≥34), P(≥40), P(majority)), and the pickup portfolio at that environment.
- **District Profiles** — interactive version of the PDF profiles. Click a district to see composite lean, win probability curve, voter universe, demographics, house race history, candidate effects.
- **Pickup Portfolio** — the S-curve chart from `pickup_portfolio.png`, interactive. Hover for district details. Filter by core/stretch/long-shot.
- **Map View** — choropleth of composite lean, tier, win probability, or demographic variables. Click-to-drill into district profiles.
- **Investment Priority** — ranked list with marginal WP, sortable/filterable. Path-to-target optimizer visualization.

**Framework considerations:**
- Streamlit is the most likely choice — lightweight, Python-native, reads directly from existing pandas DataFrames and CSVs. No frontend build step. Deploys easily for a single-user or small-team context.
- Alternatives: Panel/HoloViz (more flexibility, steeper learning curve), Dash (Plotly-native, heavier). Streamlit wins on speed-to-useful.
- The GUI reads from the same CSV/parquet outputs the CLI produces. No separate data pipeline — run `session8` or `simulate` to refresh, then the GUI picks up the latest files.

**What it does NOT do (intentionally):**
- No user accounts or auth (single-analyst tool shared with a small team).
- No data editing through the GUI — all data flows through the CLI pipeline.
- No real-time updates — refresh by re-running the pipeline, not by polling.

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
| County-level results 2000–2016 | Ohio SOS / Dave Leip's Atlas | Free | **High** | Long-term trend analysis via block backbone — 25 years of partisan realignment |
| VEST 2022/2024 shapefiles | UF Election Lab (subscription) | TBD | **High** | Block backbone for post-redistricting cycles, geometry-aware house results |
| VEST 2018 shapefile | UF Election Lab (free) | Free | **High** | Block backbone for 2018 cycle |
| Census 2020 block shapefile | Census Bureau | Free | **High** | Block backbone foundation (~300k Ohio blocks) |
| Follow the Money campaign finance | followthemoney.org | Free | Medium | Spending data for regression, disentangle candidate quality |
| L2 modeled voter file | L2 Inc. | $$$ | Low | Pre-modeled partisan scores — less needed now that we have SOS voter file scored |
| Census 2030 blocks | Census Bureau | Free (2031) | Future | Required for post-2030 redistricting |

---

## Future Sessions (Post-v2.1)

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
v1.6 (current)          v2.0 (block backbone)     v2.1+ (GUI + extensions)
──────────────          ─────────────────────     ──────────────────────────
Precincts → Districts   Precincts → Blocks →      Same backbone +
                        Districts                  web-based exploration

Population-weighted     Same crosswalk,            Streamlit GUI reads
block centroid join     generalized to             from backbone outputs
                        any district map

House results by        House results via          Interactive district
district number         block reaggregation        profiles, scenario
(redistricting-         (structurally              explorer, maps
filtered)               contamination-free)

Single district map     Any district map           Any district map
                        on demand                  on demand

4 election cycles       25 years of history        25 years + stakeholder
(2018–2024)             (2000–2024)                self-service access

Voter file scored       Voter file geocoded        Voter universe visible
but not geocoded        to blocks                  in GUI district profiles
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
7. ☐ Acquire Census 2020 block shapefile + VEST 2018 shapefile
8. ☐ Check VEST subscription pricing for 2022/2024 shapefiles
9. ☐ Acquire county-level results 2000–2016
10. ☐ **Session 10 — block backbone architecture**
11. ☐ **Session 11 — stakeholder GUI**
12. ☐ Future sessions as priorities dictate

---

*This roadmap is a living document. Update after each session with findings, revised priorities, and new items.*
