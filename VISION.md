# Ohio House Election Model — Vision

*March 2026*

---

## The Problem

Ohio Democrats make resource allocation decisions for state legislative races based on a combination of national party data (VAN, DCCC targeting), conventional wisdom, and intuition. None of these are wrong, but none of them are built for Ohio's specific landscape — a state where the structural partisan geography, the redistricting history, and the turnout dynamics are distinct from any other state.

The result is predictable: money goes to districts that "feel" competitive based on the last election's margin, ignoring that a 14-point Republican win can mask a district that leans Democratic on fundamentals when you strip away the statewide environment and incumbency. Winnable seats go unfunded. Unwinnable seats get attention because someone knows the candidate. The party's scarce resources get spread by instinct rather than evidence.

Meanwhile, Republicans hold a 65-34 supermajority in the Ohio House. Democrats haven't held a majority since 2008. The structural disadvantage is real — Democratic voters are packed into fewer, more heavily D districts — but the current seat deficit exceeds what the geography dictates. Democrats are underperforming their own fundamentals.

---

## The Thesis

A single analyst with the right methodology, the right data infrastructure, and the right relationships can measurably improve Democratic performance in Ohio state legislative races by replacing intuition-based targeting with evidence-based targeting.

This is not a forecasting project. It does not predict who will win. It answers a narrower, more useful question: **given scarce resources, where should Democrats invest to maximize seats gained?**

The answer requires:
- Knowing the true partisan lean of every district, independent of who ran and when.
- Knowing which districts respond to persuasion vs. mobilization.
- Knowing how many seats are realistically achievable at different statewide environments.
- Being honest about what the data can and cannot tell you.

---

## What We're Building

A durable analytical platform for Ohio state legislative elections. Not a one-time report. Not a dashboard. A living system that ingests new data, updates its estimates, survives redistricting, and produces actionable intelligence for campaign operatives.

**Core capability:** For any district, at any point in the cycle, answer: What are the partisan fundamentals? What would it take to flip this seat? Is the opportunity persuasion or mobilization? How does this district compare to others competing for the same resources?

**Core principle:** Every number traces to a public data source. Every methodological choice is documented. Every limitation is stated. The tool is as transparent about what it doesn't know as what it does.

---

## What Success Looks Like

**2026 cycle:**
- The Ohio Democratic Party uses this tool's district rankings to inform resource allocation for at least 5 targeted House races.
- Post-election, the model correctly identified 7 of the 10 closest races (rank-order accuracy in the competitive tier).
- At least one district that conventional wisdom wrote off but the model flagged as competitive receives investment and outperforms expectations.

**2028 cycle:**
- The model incorporates 2026 results and a voter file, enabling individual-level mobilization targeting.
- Ohio Senate targeting is operational on the same infrastructure.
- The backtest from 2026 provides empirical validation: "the model correctly predicted X of Y competitive districts." That becomes the credibility foundation for 2028 resource conversations.

**2030 redistricting:**
- When new district boundaries are drawn, the block backbone reaggregates 20+ years of historical election data to the new maps in a single command. Every other targeting operation in the state starts from scratch. This one doesn't.

**Long-term:**
- The Ohio Democratic Party has a permanent, science-based targeting infrastructure that survives staff turnover, leadership transitions, and redistricting cycles. Resource allocation decisions are grounded in validated methodology rather than relationships and intuition.

---

## What This Is Not

**Not a substitute for political judgment.** The model identifies where fundamentals are favorable. It cannot tell you whether a specific candidate is strong, whether a local issue will dominate, or whether a national wave will materialize. Those are human judgments. The model's job is to make sure those judgments are applied to the right districts.

**Not a forecasting model.** This is not 538 for Ohio state races. It does not produce win probabilities or call races. It identifies structural opportunities and quantifies what it would take to realize them. The distinction matters — a forecast says "Democrats will win 38 seats." This tool says "if the statewide environment is 48% D, these 6 districts flip, and here's what each one needs."

**Not a product for sale.** This is infrastructure for Ohio Democrats, built by someone embedded in Ohio's political landscape. It could theoretically be replicated for other states, but the value comes from deep familiarity with Ohio's data, geography, and political dynamics — not from a generalizable software product.

**Not AI-generated analysis.** AI tools (Claude Code, Claude) are used to accelerate development — writing code, processing data, stress-testing methodology. Every analytical decision, every methodological choice, and every strategic interpretation is made by a human analyst who can defend every number at a microphone. The tool was built with AI. The judgment is human.

---

## Why Me

I'm a content designer at Meta with a political science background, deep roots in Cleveland Heights civic policy, and an established analytical practice in Ohio housing and zoning reform. I built Open Heights from the same foundation — taking publicly available data, applying rigorous methodology, and producing analysis that changes how institutions make decisions.

The relevant skills:
- **Data infrastructure.** I built the precinct-to-district crosswalk, the composite partisan index, the regression model, and the validation suite from public data sources using Python, geopandas, and statsmodels.
- **Institutional credibility.** I serve on the Cleveland Heights Technical Advisory Group for zoning reform, the FutureHeights board, and have published analytical work in the Heights Observer. I know how to present data to decision-makers who aren't data people.
- **Ohio-specific knowledge.** I live here. I understand the political geography — why Cuyahoga County suburbs behave differently from Franklin County suburbs, why Appalachian Ohio is a different universe from the Western Reserve, why the Mahoning Valley is realigning. That context is baked into every methodological choice.
- **Commitment to rigor.** The adversarial review, the external validation, the methodology document, the known limitations section — these exist because I'd rather be challenged on an honest finding than trusted on a fabricated one.

---

## The Ask

Fifteen minutes with Kathleen Clyde to show what this tool can do. Not to sell anything. To demonstrate that Ohio Democrats have access to analytical infrastructure that most state parties don't — and that it's ready to inform 2026 targeting decisions.

The deliverable for that meeting:
- A one-page summary: current seat count, realistic 2026 targets, the 18 flippable districts ranked by difficulty, and what statewide environment each tier requires.
- One district deep-dive showing how a 14-point Republican win masks a fundamentally Democratic-leaning district — the math, the decomposition, the validation.
- The DRA external validation proving the methodology matches the gold standard.
- A clear statement of what the tool needs to become fully operational for 2026: voter file access, a relationship with the party's data team, and a seat at the targeting table.

---

*Ohio House Election Model. Built in Ohio, for Ohio, by someone who plans to be here for the long haul.*
