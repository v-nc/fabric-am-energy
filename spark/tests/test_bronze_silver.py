from __future__ import annotations

from datetime import timedelta

import pytest
from pyspark.sql import functions as F

from am_energy import PipelineConfig
from am_energy.bronze import BRONZE_SCHEMA, to_bronze
from am_energy.silver import deduplicate, merge_into, to_silver
from conftest import HISTORY, bronze_df, event, ts

CFG = PipelineConfig()


@pytest.mark.skipif(not (HISTORY / "meter_events_2026-07.parquet").exists(), reason="run `npm run history` in edge/")
def test_history_files_of_both_schema_versions_land_in_one_bronze_schema(spark):
    raw = spark.read.option("mergeSchema", "true").parquet(
        str(HISTORY / "meter_events_2026-06.parquet"), str(HISTORY / "meter_events_2026-07.parquet")
    )
    bronze = to_bronze(raw, "history")
    assert bronze.schema.simpleString() == BRONZE_SCHEMA.simpleString()
    by_version = {r["schema_version"]: r for r in bronze.groupBy("schema_version").agg(F.count("power_factor").alias("pf")).collect()}
    assert by_version[1]["pf"] == 0 and by_version[2]["pf"] > 0
    assert bronze.where("_source_file IS NULL OR _source_file_modified IS NULL").count() == 0


def test_eventstream_rows_with_string_timestamps_are_typed(spark):
    df = spark.createDataFrame(
        [("1", "LS01", "EM01", "7", "2026-10-01T12:00:00.000Z", "2026-10-01T12:00:01.500Z", "idle", None, 2.0, 2.0, 2.0,
          400.0, 1.4, 1000.5, "Good", "2026-10-01T12:00:02Z")],
        "schema_version string, machine_id string, meter_id string, seq string, ts_source string, ts_edge string, "
        "state string, job_id string, l1_a double, l2_a double, l3_a double, voltage_v double, power_kw double, "
        "energy_kwh_total double, opc_status string, EventEnqueuedUtcTime string",
    )
    row = to_bronze(df, "eventstream").first()
    assert row["seq"] == 7 and row["schema_version"] == 1 and row["power_factor"] is None
    assert row["ts_source"].isoformat().startswith("2026-10-01T12:00:00")
    assert row["_enqueued_at"] is not None and row["_source"] == "eventstream"


def test_deduplicate_keeps_first_arrival(spark):
    rows = [event(0), event(1), event(1, ts_edge=ts(1) + timedelta(seconds=30), power_kw=9.9)]
    out = deduplicate(bronze_df(spark, rows)).collect()
    assert len(out) == 2
    assert {r["power_kw"] for r in out} == {1.5}


def test_quality_rules_and_quarantine(spark):
    rows = [
        event(0), event(1),
        event(2, opc_status="Bad", power_kw=0.0),
        event(3, power_kw=25.0),                          # above max_power_kw
        event(4),
        event(5, power_kw=12.0),                          # spike vs neighbours 1.5
        event(6),
        event(7, opc_status="Uncertain"),
        event(8, ts_edge=ts(8) + timedelta(minutes=40)),  # late
        event(9, state=None),
    ]
    silver, quarantine = to_silver(bronze_df(spark, rows), CFG)
    reasons = {r["seq"]: r["dq_reason"] for r in quarantine.collect()}
    assert reasons == {2: "bad_status", 3: "implausible_power", 5: "spike", 9: "missing_value"}
    s = {r["seq"]: r for r in silver.collect()}
    assert s[7]["is_uncertain"] and not s[6]["is_uncertain"]
    assert s[8]["is_late"] and not s[6]["is_late"]


def test_a_real_level_change_is_not_a_spike(spark):
    rows = [event(0), event(1), event(2, power_kw=7.5, state="heat_up"), event(3, power_kw=7.8, state="heat_up")]
    _, quarantine = to_silver(bronze_df(spark, rows), CFG)
    assert quarantine.count() == 0


def test_merge_is_insert_only_and_idempotent(spark):
    silver, _ = to_silver(bronze_df(spark, [event(0), event(1)]), CFG)
    silver.limit(0).write.format("delta").saveAsTable("silver_merge_test")
    merge_into(spark, "silver_merge_test", silver)
    merge_into(spark, "silver_merge_test", silver)  # replay
    late_dup, _ = to_silver(bronze_df(spark, [event(1, power_kw=1.6), event(2)]), CFG)
    merge_into(spark, "silver_merge_test", late_dup)
    t = {r["seq"]: r["power_kw"] for r in spark.table("silver_merge_test").collect()}
    assert t == {0: 1.5, 1: 1.5, 2: 1.5}
