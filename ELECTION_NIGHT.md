# Election night checklist — Tuesday, November 3, 2026

Polls close 7:30 pm ET. The model reads the Ohio Secretary of State's live
results feed (https://liveresults.ohiosos.gov, refreshed about every 3
minutes): every race, county by county, with precincts reported and absentee
ballots counted. You don't type statewide or legislative results.

## Before Tuesday

Use `/usr/bin/python3` for everything below: on this machine `python` doesn't exist and plain `python3` (Homebrew) lacks the project's packages.


- [ ] `/usr/bin/python3 cli.py election-night-rehearsal --target 0.505 --gap 15` — watch a
      close race resolve. Try a few `--target`, `--gap` and `--seed` values.
- [ ] `/usr/bin/python3 cli.py election-night --feed` once a day through Monday. The SOS
      runs mock elections into this same feed; any file with non-zero votes is
      the best real-format test there is. Look for parser warnings.
- [ ] `/usr/bin/python3 cli.py polls-refresh` Monday night, then `/usr/bin/python3 cli.py forecast`,
      so the pre-election starting point has the last polls.
- [ ] Code frozen Monday. No model changes on the night.

## On the night

1. **7:15 pm** — start the dashboard and check it loads with the feed:

   ```bash
   /usr/bin/python3 -m streamlit run app.py
   ```

   Election Night page → Results source: **SOS live feed** → **Refresh feed
   now**. The red TEST / MOCK banner must be **gone** after 7:30; if the feed
   still says test data, the numbers are not real. Or in a terminal:

   ```bash
   /usr/bin/python3 cli.py election-night --feed --watch 180
   ```

2. **7:30–8:30** — absentee batches arrive first (early + mail votes, which
   lean D). Expect Democrats to lead the early count. The model knows each
   county's absentee share and discounts it, but with only absentee ballots
   in it can't tell a strong environment from a big absentee lean, so it
   leans on the pre-election forecast. If the night is landing near the
   forecast, its first-hour ranges hold up (84% coverage in rehearsal); if
   the night is far from it, they're too narrow until Election Day votes
   arrive (usually the second big batch). Trust the Call column, not the
   first-hour ranges.

   Check the **feed self-check** line: if it says 0 counties with absentee
   counts while ballots are being counted, the model has switched to a
   weaker fallback (a warning says so). Calls stay safe, but projections run
   ~2 pts off mid-count.

3. **Every few minutes** — Refresh. Read, in order:
   - Statewide cards: Win Likelihood, Call, Polling Average for reference.
   - **Outstanding vote — Governor**: which counties still have votes and how
     they should break. This is the "Acton is behind, but Franklin, Cuyahoga
     and Hamilton are still out" view.
   - Chambers, then the Races table (default: 10–90% races).

4. **Calls** — a CALL needs a ≥99% win likelihood **and** a counted margin of
   at least 30% of the expected outstanding vote. "likely D/R" is not a call.
   Precincts at 100% is not final: late absentee (up to 4 days) and
   provisional ballots are still treated as outstanding.

5. **If the feed breaks** — the model keeps the last good file and says so.
   Fall back to manual entry (Enter returns tab, or the CSVs in `data/raw/`)
   from AP or NYT; the feed wins for any race it has data for, so
   remove `--feed` / switch to "Manual entry only" if you need to override it.

## What the model assumes (judgment calls, in `src/election_night.py`)

- Absentee ballots lean D by **+10 pts** vs Election Day to start (sd 8);
  the feed's absentee counts update it. `--details` / "Model internals"
  shows the current estimate.
- Turnout starts at 2022 levels (sd 12%) and is learned from counties with
  half their precincts in.
- 2% of a county's vote is treated as still out after it reports 100%.
- Outputs: `reports/election_night/latest_*.csv`, `history.csv` (one row per
  refresh), raw feed files in `data/raw/enr/`.
