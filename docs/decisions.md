# Design decisions

Short records of the choices in this project and why they were made. Open decisions are at the end.

## Edge

**1. Eventstream custom endpoint, not MQTT.** The custom endpoint (Event Hubs protocol) is GA, needs no public
broker and has no traffic quota. Eventstream's MQTT source is in preview, takes one topic per source and needs a
publicly reachable broker. In production the edge would be OPC Publisher or Azure IoT Operations, sending to Event
Hubs over private networking with a managed identity instead of a SAS key.

**2. One OPC UA server for machine state and meter values (lab shortcut).** In a plant the state comes from the 3D
printer's controller and the electrical values from a separate IP meter. One simulator keeps the lab small; the
gateway already treats them as separate variables.

**3. Readings are reassembled by source timestamp.** OPC UA notifies per variable. The simulator writes all variables
of a reading with one sourceTimestamp, and the gateway groups by meter and timestamp. The monitored items use the
StatusValueTimestamp trigger so an unchanged value (a 3D printer standing idle) is still reported.

**4. Store-and-forward buffer on SQLite.** Events and per-meter sequence numbers survive restarts and outages. Built
on `node:sqlite`, so there is no native dependency.

**5. Live data first, backlog behind it.** After an outage the gateway sends the newest events first and replays the
backlog in the background. The dashboard is current at once; the price is late, out-of-order arrival, which the
pipeline is built to handle anyway.

**6. Partition key = meter id.** Events of one meter stay in order inside one Event Hubs partition.

## Pipeline

**7. Natural key (meter_id, ts_source) for deduplication; first arrival wins.** A meter can't take two readings at
the same instant. `seq` is kept to detect loss within a gateway session but isn't part of the key, because it
restarts when a gateway is replaced (the live gateway and the history both start at 0).

**8. Silver is written with an insert-only MERGE.** A measurement already in silver is never overwritten, so replays,
duplicates across batches and job re-runs are harmless (idempotent).

**9. Energy from counter deltas, not from integrating power.** The meter's kWh counter keeps counting during a
dropout, so the energy over a gap is still known. A counter that goes down means a meter swap; the new counter value
is then the energy since the swap. Instantaneous power is only used for power statistics and spike detection.

**10. Proportional allocation to 15-minute buckets.** An interval that spans buckets (a boundary, or a 3-hour
dropout) is split in proportion to time instead of being booked at its end.

**11. Gaps are state "unknown", not the last known state.** Nobody saw what the 3D printer did during a dropout. Idle
share excludes unknown time, and the data quality page shows it.

**12. job_id only while building; heat-ups are matched to jobs by time.** That's what the machine reports. Gold links
each heat-up to the build that starts right after it.

**13. Heat-up overrun = longer than 1.3 × a 2 h baseline (2.6 h).** On two months of history it found 15 of 16
injected overruns with no false alarm. The missed one was a short heat-up stretched only a little. That is within
the range of a slow normal heat-up, so no fixed threshold can catch it without false alarms. A per-machine baseline
would be the next step.

**14. UTC in storage, local time for reporting.** `date_key`, `slot_of_day` and the time-of-use tariff use
Europe/Berlin local time; daylight saving is covered by a test.

**15. Transformations as a tested Python package.** `spark/` holds pure DataFrame functions, tested locally with the
Spark and Delta versions of Fabric Runtime 2.0. In Fabric they run as a wheel attached to an Environment. The
alternative, code pasted into notebooks, can't be unit tested.

**16. Fabric Runtime 2.0 (Spark 4.1, Delta 4.2), not the workspace default 1.3.** 1.3 has its end of support
announced; 2.0 is GA.

**17. dim_machine is SCD type 2, built from monthly master data snapshots.** Facts carry the version valid at their
timestamp, so energy before a hall move stays with the old hall, and row-level security by hall stays historically
correct.

**22. Silver is a notebook with Spark Structured Streaming (`availableNow`), not a materialized lake view.** Both
bronze tables are read as streams; the checkpoint records which Delta versions were processed, so each run handles
exactly the new rows. `foreachBatch` checks each batch with neighbouring rows from earlier batches (the spike rule
needs both neighbours) and writes with the insert-only MERGE (8). An MLV can't express MERGE or cross-batch context.
The same code runs as a continuous stream by changing only the trigger.

**23. Gold is rebuilt in full on each run (overwrite), not updated incrementally.** Late silver rows change
energy deltas, state intervals and day totals around them, so an incremental gold would have to find and recompute
every affected day. At 5M rows a full rebuild takes minutes and is trivially correct and idempotent. If runtime
grows, the next step is recomputing only the local days touched by new silver rows (`replaceWhere` on `date_key`).

**24. Facts carry the dim_machine version valid at the fact's time (SCD type 2).** 15-minute and event facts use
their own timestamp; day-level facts use noon of the day, since versions change at midnight. Energy before LS05's
hall move stays with hall 1, which row-level security by hall relies on.

**25. A heat-up whose end falls in a data gap is not flagged as an overrun.** The gap hides when the heat-up
really ended, so its measured length is an upper bound. Over the full year this removed the only 2 false alarms;
those heat-ups show on the data quality page instead (`has_gap`).

**26. One daily pipeline (`pl_daily`): silver → gold → maintenance, chained on success.** Gold never runs on
half-processed silver. Each notebook activity retries once after 2 minutes (transient Spark errors), and the three
share one Spark session through high concurrency for pipelines, which saves two session starts per run. The workspace
ID is passed as `@pipeline().DataFactory`, so the pipeline runs unchanged in another workspace.

**27. Table maintenance: OPTIMIZE (with V-Order) for append-heavy tables, VACUUM with 7 days' retention for all.**
Eventstream commits a small file every minute or two; compaction brought `bronze.stream_meter_events` from about 400
files to 1 in the current version. Gold is rewritten in full, so it only needs VACUUM. Seven days keeps time travel
for a week and protects the silver stream, which must read bronze's appended files before they may be vacuumed.

**28. Counter jumps are checked for plausibility; an impossible one is a discontinuity, not energy.** A counter
that goes down is a meter swap only if the new value is at most what a new meter (≤ 5 kWh) plus the 3D printer
could show after the interval; a counter that rises faster than the maximum power allows is impossible too. Such an
interval's energy is unknown (not booked) and counted on the data-quality page. Found when the live simulator
restarted every meter at its initial value after the 12-month history: the old rule booked each restart as a meter
swap and invented about 217,000 kWh in October.

**29. The semantic model is reframed once, explicitly, at the end of the pipeline.** Direct Lake reads a frozen
version (frame) of each Delta table; a refresh only moves the frame to the latest versions, in seconds, without
copying data. Automatic reframing is off because `nb_gold` writes its ten tables one after another, and a reframe in
the middle would mix new facts with old dimensions. `pl_daily` reframes with a Semantic model refresh activity after
gold succeeds. Its connection uses OAuth with my user, fine for a portfolio; production would use a service principal
or the workspace identity.

**30. The data-quality fact is a coverage grid: one row per machine version and day, with expected_minutes.**
Completeness divides what arrived by what was expected, and the expectation lives in the fact rather than in DAX.
A day belongs to the dim_machine version valid at its noon, so a 3D printer that moved halls is expected in each hall
only while it was there, and a silent meter shows up as a 0 % row instead of disappearing. Found when the first DAX
version (days in filter × machines) put hall 1 at 84 % and hall 2 at 92 % with identical fault rates: LS05 was
expected in both halls for the whole year.

**31. Report pages can be written as code (PBIR JSON) and deployed through Git.** The report is stored in the
PBIR format: one `visual.json` per visual, with a published JSON schema. Pages 1 and 2 were first built by hand in the
browser; then all five pages were generated as JSON in one consistent layout (header with title, subtitle and
slicers, a KPI row, a two-column grid), validated against Microsoft's schemas before committing, and pulled into the
workspace with Update from Git. Page ids stay stable, and the pages remain editable in the browser afterwards.

**32. Jobs and heat-ups that span a data gap are filtered out at page level, not deleted.** Their length is an upper
bound, so averages and the scatter of hours vs energy would be distorted (94 of 1,716 jobs, 6 of 1,719 heat-ups).
They stay in gold with `has_gap`, and the filter is visible in the report's filter pane.

## Real-time path

**33. A hot path in an Eventhouse next to the Lakehouse, fed by the same Eventstream.** The Eventstream has two
destinations: the Lakehouse (bronze, for the history and the semantic model) and the KQL database `eh_am_energy`
(table `meter_events`, events queryable within seconds). The Eventhouse answers "what is happening now" for the
Real-Time Dashboard and the alerts and keeps 30 days, 7 of them in hot cache; the Lakehouse keeps everything. Reading
live data from bronze instead would mean waiting for files to land, the pipeline to run and the model to be
reframed: minutes or hours instead of seconds.

**34. The real-time path deduplicates like silver, with a materialized view.** `readings_dedup` keeps one row per
(meter, measurement time) with `take_any(*)` and is maintained by the Eventhouse as data arrives. Dashboard tiles and
alerts read the view; the raw table keeps the duplicates, which the freshness tile counts (the gateway resends a share
of events on purpose). The view is created with `.create async ... with (backfill=true)` because the table already
held data.

**35. Real-time logic lives in stored KQL functions, versioned in `kql/setup.kql`.** `CurrentState`, `StateRuns`,
`HeatupRuns`, `OverrunStatus`, `ParkPower` and `Freshness`: tiles and the alert call a function instead of carrying
their own copy of the logic, the script can be re-run, and `scripts/kql.py` runs the same queries from the terminal.
State runs are found with window functions (`prev`, `row_cumsum`) ordered by measurement time, so late and replayed
events land in the right run. Exact distinct counts use `count_distinct`: `dcount` is a HyperLogLog estimate, and in
the freshness tile it once reported more distinct measurements than rows.

**36. A demo mode that never reaches the history.** For a live demo the simulator runs 30 times faster with a 50 %
overrun probability (environment overrides, validated and unit-tested), so an alert happens within minutes instead of
every two days or so. Meanwhile the Eventstream's Lakehouse destination is paused, so no demo event reaches
silver or gold; it is resumed afterwards with the start option "Now", which skips what arrived during the pause. The
order matters: pause the destination before the simulator switches to demo mode. On the first demo it was paused
14 s late, and 23 demo events went through to gold. Only 0.1 kWh, but they stretched the data-quality grid over six
days the lab was switched off (completeness 96.1 % instead of 97.7 %). They were deleted from silver with a Delta
DELETE and gold rebuilt; bronze keeps them, because silver reads bronze as an append-only stream. The KQL functions
take a `scale` argument that converts demo time back to plant hours.

**37. The heat-up alert reads a KQL query and fires when `overrun` changes to true.** The first rule watched the raw
stream with "state is equal to heat_up". That is a state condition, so it fires on every matching event: 91 e-mails
in a few minutes, until Activator throttled the recipient. "When it has been true for 5 minutes" still fires on every
event once the five minutes have passed (13 e-mails for a single overrun in Activator's preview). The rule now runs
`OverrunStatus()` every minute, one row per 3D printer with the current heat-up duration in plant hours, and fires
when `overrun` changes to true for a `machine_id`: one e-mail per overrun, with the exact 2.6 h threshold, on
deduplicated data and measurement time. The price is up to a minute of extra latency and a small query per minute,
negligible next to a 2.6-hour threshold. Firing once per episode always comes from a transition condition ("becomes
true", "changes to", "increases above"), never from a state condition. One more trap: the alert wizard typed every
column of the query as text, so the boolean arrives as "0"/"1" and a logical "becomes true" never matched. The rule
uses the text condition "changes to 1"; Activator's projected activations on the past hour (zero before the fix,
exactly one per overrun after) showed the difference before any e-mail was due. And a polled condition can be
missed: of the first three live overruns, two sent their e-mail and one did not, because that heat-up ended 30 s
after crossing the threshold, between two queries. `OverrunStatus()` now keeps the flag up for three minutes after
an overrun heat-up ends, longer than two polling intervals, which still gives one transition per overrun.

**38. Pipeline failures are alerted through Fabric job events.** An Activator rule receives `ItemJobFailed` events
for `pl_daily` from the Real-Time hub and sends an e-mail. Job events also cover cancelled runs and runs that fail
before their first activity, which an "on failure" branch inside the pipeline would miss. It was tested by cancelling
a run: the e-mail went out the same second. Activator runs on the capacity, so failures while the capacity is paused
(the nightly schedule) cannot be alerted; a production capacity runs continuously.

## Market prices: Dataflow Gen2 and a Warehouse

**39. Real public market prices, loaded by Dataflow Gen2.** Hourly day-ahead prices of the Swiss bidding zone come
from the Energy-Charts API (data Bundesnetzagentur | SMARD.de, CC BY 4.0), the only real data in the project: public
and unrelated to any company. Dataflow Gen2 fits a small REST source that needs shaping rather than code: Power Query
turns two parallel JSON arrays into rows, converts Unix seconds to UTC and types the columns. The base URL is static,
with `RelativePath` and `Query`, so the service can refresh it; a URL built by string concatenation is a dynamic
data source and cannot be refreshed. Full refresh, because about 9,000 rows load in seconds. Writing to a Warehouse
requires staging, a fixed schema and the connection option "Navigate using full hierarchy" (to reach the `ref`
schema); the Warehouse has no DateTimeZone type, so every time is UTC.

**40. The mart is built in a Warehouse, with T-SQL.** `mart.usp_refresh_energy_cost` reads gold through a
cross-database query (three-part names on the Lakehouse's SQL analytics endpoint, no copy) and rebuilds
`mart.energy_cost_hourly` in one transaction: the DELETE and the INSERT commit together, so readers see the old or
the new mart, never half of it. A Warehouse suits SQL-first consumers such as controlling, and multi-table
transactions; Spark in the Lakehouse could have produced the same table. In Git the Warehouse is a SQL database
project with one `.sql` file per object. Its default collation is case-sensitive (`Latin1_General_100_BIN2_UTF8`).

**41. A OneLake schema shortcut brings the mart into the Lakehouse.** `lh_energy.mart` points at `wh_am_energy.mart`:
nothing is copied, and the Direct Lake model keeps one source for all its tables. The new table relates to
`dim_machine` through `machine_sk`, so the hall roles reach it too.

**42. Prices are compared in shape, not in totals.** The day-ahead price is the energy component only; the fixed
time-of-use tariff includes grid fees and levies. So the page compares the park's load-weighted price with the
market's time-weighted average: +0.2 %, because builds run around the clock and the park simply pays the average.
It also shows where the cheap hours are. The market is cheapest at 13:00 (solar) and dearest at 19:00, while the
fixed tariff counts 07:00 to 20:00 on weekdays as peak: its window charges the market's cheapest hours as peak and
leaves the dearest evening hours off-peak. 239 hours had negative prices, mostly in spring.

**43. `pl_daily`: prices in parallel with silver, the mart after gold and prices, the reframe last.** The model is
still reframed once (29). If the price API is down, the dataflow fails after two retries, the mart and the reframe
are skipped, the run fails and the failure alert fires; the report keeps yesterday's consistent frame. The
alternative, a reframe after gold plus a second one after the mart, would keep the core pages fresh during an API
outage at the price of two reframes per run.

## Platform

**21. One Lakehouse (`lh_energy`) with bronze, silver and gold schemas, not three Lakehouses.** One team owns all
layers, so one SQL analytics endpoint, one set of permissions and one Direct Lake source are simpler. Three Lakehouses
would pay off with different owners, retention or access per layer; workspace roles plus OneLake security on the
schemas cover that here if it ever comes up.

**18. Fabric in the user's own Microsoft 365 tenant.** Signing up to Fabric with a work address created an
unmanaged tenant; it was taken over by verifying the domain with a DNS TXT record. The tenant switch "Users can sync
workspace items with GitHub repositories" is enabled for the whole organization because there is one user; in a
company it would be limited to a security group of developers.

**19. A paid F4 capacity, paused when idle, instead of the free trial.** Microsoft refused the 60-day Fabric trial for
the brand-new tenant (community reports put the minimum tenant age at about 90 days). The capacity is an Azure
pay-as-you-go F4 in the tenant's home region (Sweden Central, €0.67/h at list price), created and paused from the
terminal with `scripts/capacity.sh`, under a monthly budget with e-mail alerts. Paused, it costs nothing for compute.
Sizing, pausing and costing a capacity is also part of running Fabric in production.

**20. Synthetic data, every parameter an assumption.** No real machine, customer or company data. All parameters are
in `config/assumptions.yaml`.

**44. CI checks what Fabric can't, and doesn't deploy yet.** GitHub Actions runs on every push and pull request: the
TypeScript typecheck and tests; the Python unit tests plus the end-to-end tests on a regenerated 12-month history
(fixed seed, 16 s; `REQUIRE_HISTORY` turns a missing history into a failure instead of a silent skip); ruff at a
pinned version; and `scripts/check_fabric.py`, which resolves every table, column and measure the report uses
against the semantic model's TMDL. The report pages are edited as code, and a renamed measure would otherwise only
show up as a broken visual in Fabric. Deployment stays with Fabric Git integration: deploying from CI (fabric-cicd)
needs a service principal allowed to use the Fabric APIs and a running capacity, which this project pauses. It's
the next step for a dev/test/prod setup.

## Open

- **RLS by hall is defined but not yet enforced end to end.** Roles `Hall H1` and `Hall H2` filter `dim_machine`
  and reach every fact through `machine_sk`. The model still reads OneLake with SSO (the default), so report users
  would need Lakehouse access, which bypasses model RLS, and "Test as role" is unavailable. Enforcing it means binding
  the model to a fixed-identity cloud connection with SSO off (or using OneLake security). Skipped for now: the
  portfolio has a single user.

- **Which gold steps become materialized lake views?** Silver is decided (22). For gold, MLVs suit declarative
  aggregates over silver; window-heavy logic (energy deltas, state intervals) stays in notebooks.
