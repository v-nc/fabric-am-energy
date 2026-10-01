"""Runs bronze → silver → gold over two months of generated history and checks the results against the ground truth
the generator recorded. Skipped until `npm run history` has been run in edge/."""

from __future__ import annotations

import json
from datetime import datetime, timedelta

import pytest
from pyspark.sql import functions as F

from am_energy import PipelineConfig, gold
from am_energy.bronze import to_bronze
from am_energy.silver import to_silver
from conftest import HISTORY

MONTHS = ["2026-04", "2026-05"]  # the EM07 meter swap happens on 2026-05-01
pytestmark = pytest.mark.skipif(
    not all((HISTORY / f"meter_events_{m}.parquet").exists() for m in MONTHS), reason="run `npm run history` in edge/"
)
CFG = PipelineConfig()


@pytest.fixture(scope="module")
def run(spark):
    raw = spark.read.option("mergeSchema", "true").parquet(*[str(HISTORY / f"meter_events_{m}.parquet") for m in MONTHS])
    bronze = to_bronze(raw, "history").cache()
    silver, quarantine = to_silver(bronze, CFG)
    silver, quarantine = silver.cache(), quarantine.cache()
    intervals = gold.state_intervals(silver, CFG).cache()
    return {
        "raw": raw,
        "bronze": bronze,
        "silver": silver,
        "quarantine": quarantine,
        "intervals": intervals,
        "truth": json.loads((HISTORY / "ground_truth.json").read_text()),
    }


def test_every_measurement_survives_exactly_once(run):
    distinct = run["raw"].select("meter_id", "seq").distinct().count()
    assert run["silver"].count() + run["quarantine"].count() == distinct
    assert run["bronze"].count() > distinct  # duplicates were present


def test_energy_is_conserved_per_meter(spark, run):
    fact = gold.fact_energy_15min(run["silver"], CFG).groupBy("machine_id").agg(F.sum("kwh").alias("kwh"))
    counters = run["silver"].groupBy("machine_id").agg(
        (F.max_by("energy_kwh_total", "ts_source") - F.min_by("energy_kwh_total", "ts_source")).alias("span")
    )
    for r in fact.join(counters, "machine_id").collect():
        if r["machine_id"] == "LS07":
            continue  # counter reset: checked separately
        assert r["kwh"] == pytest.approx(r["span"], rel=1e-9), r["machine_id"]


def test_meter_swap_is_detected_once_at_the_right_time(run):
    resets = gold.with_energy_deltas(run["silver"], CFG).where("counter_reset").select("meter_id", "ts_source").collect()
    assert [r["meter_id"] for r in resets] == ["EM07"]
    assert abs(resets[0]["ts_source"] - datetime(2026, 5, 1)) < timedelta(hours=4)


def test_heatup_overrun_detection_against_ground_truth(run, capsys):
    detected = gold.fact_heatup(run["intervals"], CFG).collect()
    truth = [
        h for h in run["truth"]["heatups"]
        if MONTHS[0] <= h["start"][:7] <= MONTHS[-1] and h["end"][:7] <= MONTHS[-1]
    ]

    def match(d):
        for h in truth:
            if h["machine_id"] == d["machine_id"] and abs(datetime.fromisoformat(h["start"][:19]) - d["start_ts"]) < timedelta(minutes=5):
                return h
        return None

    pairs = [(d, match(d)) for d in detected]
    matched = [(d, h) for d, h in pairs if h]
    assert len(matched) >= 0.9 * len(detected)
    tp = sum(1 for d, h in matched if d["is_overrun"] and h["overrun"])
    fp = sum(1 for d, h in matched if d["is_overrun"] and not h["overrun"])
    fn = sum(1 for d, h in matched if not d["is_overrun"] and h["overrun"])
    with capsys.disabled():
        print(f"\nheat-ups: {len(detected)} detected, {len(matched)} matched; overruns TP={tp} FP={fp} FN={fn}")
    assert fp == 0
    assert tp >= 0.7 * (tp + fn)


def test_every_build_job_has_its_heatup_and_cost_is_positive(spark, run):
    jobs = gold.fact_build_job(run["intervals"], CFG)
    assert jobs.where("heatup_kwh = 0").count() <= 0.05 * jobs.count()
    assert jobs.where("job_id IS NULL").count() == 0
    fact = gold.fact_energy_15min(run["silver"], CFG)
    assert fact.where("cost < 0 OR kwh < 0").count() == 0
    assert fact.agg(F.avg("completeness")).first()[0] > 0.95


def test_data_quality_report_accounts_for_everything(run):
    dq = gold.fact_data_quality_daily(run["bronze"], run["silver"], run["quarantine"], CFG)
    totals = dq.agg(*[F.sum(c).alias(c) for c in ["events_received", "duplicates_removed", "valid_readings", "rejected_bad_status", "rejected_spike", "gaps", "counter_resets"]]).first()
    assert totals["events_received"] == run["bronze"].count()
    assert totals["duplicates_removed"] > 0 and totals["rejected_bad_status"] > 0 and totals["rejected_spike"] > 0
    assert totals["gaps"] > 0 and totals["counter_resets"] == 1
