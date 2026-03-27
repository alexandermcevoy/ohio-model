# Ohio House Election Model — Roadmap

*Last updated: March 27, 2026*

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

## Next: Data Acquisition + Extensions

### Data Acquisition
- **County-level results 2000–2008** (free, Ohio SOS) — extends trend analysis to 25 years. The backbone already handles county-level disaggregation (proven with 2010–2014).
- **VEST 2022/2024 shapefiles** (subscription required) — enables geometry-aware block disaggregation for post-redistricting cycles, replacing the current SOS→VEST 2020 precinct join.

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
11. ☐ Acquire county-level results 2000–2008
11. ☐ Check VEST subscription pricing for 2022/2024 shapefiles
12. ☐ Future sessions as priorities dictate

---

*This roadmap is a living document. Update after each session with findings, revised priorities, and new items.*
