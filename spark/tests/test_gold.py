from __future__ import annotations

from datetime import date, datetime, timezone

import pytest
from pyspark.sql import functions as F

from am_energy import PipelineConfig
from am_energy import gold
from am_energy.silver import to_silver
from conftest import bronze_df, event, ts

CFG = PipelineConfig()


def silver_of(spark, rows):
    s, q = to_silver(bronze_df(spark, rows), CFG)
    assert q.count() == 0
    return s


def test_counter_reset_and_gap_keep_their_energy(spark):
    rows = [
        event(0, energy_kwh_total=500.0),
        event(1, energy_kwh_total=500.1),
        event(61, energy_kwh_total=502.0),  # 60 min dropout: 1.9 kWh still counted, state unknown
        event(62, energy_kwh_total=0.05),   # meter replaced: counter restarts
        event(63, energy_kwh_total=0.1),
    ]
    d = {r["seq"]: r for r in gold.with_energy_deltas(silver_of(spark, rows), CFG).collect()}
    assert d[0]["delta_kwh"] is None
    assert d[61]["is_gap"] and d[61]["interval_state"] == "unknown" and d[61]["delta_kwh"] == pytest.approx(1.9)
    assert d[62]["counter_reset"] and d[62]["delta_kwh"] == pytest.approx(0.05)
    assert d[63]["delta_kwh"] == pytest.approx(0.05)


def test_bucket_allocation_is_proportional_and_conserves_energy(spark):
    # 08:00 → 08:40 in one interval (a dropout): 40 minutes over buckets 08:00, 08:15, 08:30 → 15/15/10 minutes.
    rows = [event(0, energy_kwh_total=100.0), event(40, energy_kwh_total=104.0)]
    b = gold.allocate_to_buckets(gold.with_energy_deltas(silver_of(spark, rows), CFG)).orderBy("bucket_start").collect()
    assert [r["kwh"] for r in b] == pytest.approx([1.5, 1.5, 1.0])
    assert sum(r["seconds"] for r in b) == pytest.approx(2400)


def test_fact_energy_15min_completeness_and_tariff(spark):
    # Monday 2026-03-02 08:00 UTC = 09:00 local → peak. One reading per minute for 15 min, minus 5 missing minutes.
    rows = [event(m) for m in range(0, 16) if m not in (3, 4, 5, 6, 7)]
    f = gold.fact_energy_15min(silver_of(spark, rows), CFG).where(F.col("bucket_start") == datetime(2026, 3, 2, 8, tzinfo=timezone.utc)).first()
    assert f["minutes_covered"] == 10 and f["completeness"] == pytest.approx(10 / 15)
    assert f["tariff_band"] == "peak" and f["date_key"] == 20260302 and f["slot_of_day"] == 36
    assert f["kwh"] == pytest.approx(15 * 1.5 / 60)
    assert f["cost"] == pytest.approx(f["kwh"] * 0.24)


def test_tariff_uses_local_time_across_daylight_saving(spark):
    df = spark.createDataFrame(
        [(datetime(2026, 1, 12, 5, 30, tzinfo=timezone.utc),),   # Mon 06:30 CET  → off-peak
         (datetime(2026, 1, 12, 6, 30, tzinfo=timezone.utc),),   # Mon 07:30 CET  → peak
         (datetime(2026, 7, 13, 5, 30, tzinfo=timezone.utc),),   # Mon 07:30 CEST → peak
         (datetime(2026, 7, 11, 10, 0, tzinfo=timezone.utc),)],  # Saturday       → off-peak
        "t timestamp",
    )
    bands = [r[0] for r in df.select(gold.tariff_band(gold.local(F.col("t"), CFG), CFG)).collect()]
    assert bands == ["off_peak", "peak", "peak", "off_peak"]


def job_cycle_rows(heatup_minutes: int):
    rows, m = [], 0
    for state, length, kw, job in [("idle", 30, 1.5, None), ("heat_up", heatup_minutes, 7.0, None),
                                   ("building", 300, 4.0, "LS01-20260302T1000"), ("cool_down", 60, 1.5, None),
                                   ("idle", 30, 1.5, None)]:
        for _ in range(0, length, 5):
            rows.append(dict(minutes=m, state=state, power_kw=kw, job_id=job))
            m += 5
    energy, out = 1000.0, []
    for i, r in enumerate(rows):
        if i:
            energy += rows[i - 1]["power_kw"] * 5 / 60
        out.append(event(r["minutes"], state=r["state"], power_kw=r["power_kw"], job_id=r["job_id"], energy_kwh_total=energy))
    return out


@pytest.mark.parametrize("heatup_minutes, overrun", [(120, False), (180, True)])
def test_heatup_matched_to_its_job_and_flagged(spark, heatup_minutes, overrun):
    intervals = gold.state_intervals(silver_of(spark, job_cycle_rows(heatup_minutes)), CFG)
    h = gold.fact_heatup(intervals, CFG).collect()
    assert len(h) == 1
    assert h[0]["job_id"] == "LS01-20260302T1000"
    assert h[0]["duration_h"] == pytest.approx(heatup_minutes / 60)
    assert h[0]["is_overrun"] is overrun
    assert h[0]["kwh"] == pytest.approx(7.0 * heatup_minutes / 60)


def test_a_gap_at_the_end_of_a_heatup_is_not_an_overrun(spark):
    rows = [r for r in job_cycle_rows(120) if not 100 <= (r["ts_source"] - ts(0)).total_seconds() / 60 < 200]
    h = gold.fact_heatup(gold.state_intervals(silver_of(spark, rows), CFG), CFG).collect()
    assert len(h) == 1 and h[0]["duration_h"] > CFG.heatup_threshold_h  # looks too long because the end is hidden
    assert h[0]["has_gap"] and not h[0]["is_overrun"]


def test_build_job_energy_includes_heatup_and_cooldown(spark):
    intervals = gold.state_intervals(silver_of(spark, job_cycle_rows(120)), CFG)
    j = gold.fact_build_job(intervals, CFG).collect()
    assert len(j) == 1
    assert j[0]["build_kwh"] == pytest.approx(4.0 * 5)
    assert j[0]["heatup_kwh"] == pytest.approx(7.0 * 2)
    assert j[0]["cooldown_kwh"] == pytest.approx(1.5 * 1)
    assert j[0]["kwh_per_build_hour"] == pytest.approx(4.0)


def test_state_energy_adds_up_to_total(spark):
    s = silver_of(spark, job_cycle_rows(120))
    by_state = {r["state"]: r for r in gold.fact_state_energy(s, CFG).collect()}
    total = s.agg(F.max("energy_kwh_total") - F.min("energy_kwh_total")).first()[0]
    assert sum(r["kwh"] for r in by_state.values()) == pytest.approx(total)
    assert by_state["building"]["hours"] == pytest.approx(5.0)


def test_scd2_from_snapshots_and_merge(spark):
    snap = lambda d, hall, serial: (d, "LS05", hall, "EM05", serial)  # noqa: E731
    schema = "snapshot_date string, machine_id string, hall_id string, meter_id string, meter_serial string"
    snapshots = spark.createDataFrame(
        [snap("2026-01-01", "H1", "A"), snap("2026-02-01", "H1", "A"), snap("2026-03-01", "H2", "A")], schema
    )
    dim = gold.scd2_rows(snapshots)
    rows = dim.orderBy("valid_from").collect()
    assert [(r["hall_id"], r["is_current"]) for r in rows] == [("H1", False), ("H2", True)]
    assert rows[0]["valid_to"] == rows[1]["valid_from"]

    dim.write.format("delta").saveAsTable("dim_machine_test")
    gold.apply_scd2(spark, "dim_machine_test", spark.createDataFrame([snap("2026-04-01", "H2", "B")], schema), "2026-04-01")
    gold.apply_scd2(spark, "dim_machine_test", spark.createDataFrame([snap("2026-05-01", "H2", "B")], schema), "2026-05-01")
    t = spark.table("dim_machine_test").orderBy("valid_from").collect()
    assert [(r["hall_id"], r["meter_serial"], r["is_current"]) for r in t] == [("H1", "A", False), ("H2", "A", False), ("H2", "B", True)]

    fact = spark.createDataFrame([("LS05", datetime(2026, 1, 15, tzinfo=timezone.utc)), ("LS05", datetime(2026, 3, 15, tzinfo=timezone.utc))], "machine_id string, t timestamp")
    keyed = {r["t"].month: r["machine_sk"] for r in gold.attach_machine_sk(fact, spark.table("dim_machine_test"), "t").collect()}
    assert keyed == {1: "LS05_20260101", 3: "LS05_20260301"}


def test_dim_date(spark):
    d = gold.dim_date(spark, date(2026, 3, 1), date(2026, 3, 31))
    assert d.count() == 31
    sunday = d.where("date_key = 20260301").first()
    assert sunday["iso_weekday"] == 7 and sunday["is_weekend"]
