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

## Open

- **Which gold steps become materialized lake views?** Silver is decided (22). For gold, MLVs suit declarative
  aggregates over silver; window-heavy logic (energy deltas, state intervals) stays in notebooks.
