from __future__ import annotations

import os
import time

# PySpark converts timestamps to the Python process's local time zone when collecting; pin it so tests are stable.
os.environ["TZ"] = "UTC"
time.tzset()

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession

from am_energy.bronze import BRONZE_SCHEMA

REPO = Path(__file__).resolve().parents[2]
HISTORY = REPO / "data" / "history"


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    warehouse = tmp_path_factory.mktemp("warehouse")
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("am-energy-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "3g")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


T0 = datetime(2026, 3, 2, 8, 0, tzinfo=timezone.utc)  # a Monday


def ts(minutes: float) -> datetime:
    return T0 + timedelta(minutes=minutes)


def event(minutes: float, **kw) -> dict:
    """A valid bronze row at T0 + minutes; override any field with keyword arguments."""
    row = {
        "schema_version": 1,
        "machine_id": "LS01",
        "meter_id": "EM01",
        "seq": int(minutes),
        "ts_source": ts(minutes),
        "ts_edge": ts(minutes) + timedelta(seconds=1),
        "state": "idle",
        "job_id": None,
        "l1_a": 2.0,
        "l2_a": 2.0,
        "l3_a": 2.0,
        "voltage_v": 400.0,
        "power_kw": 1.5,
        "energy_kwh_total": 1000.0 + minutes * 1.5 / 60,
        "opc_status": "Good",
        "power_factor": None,
        "_source": "test",
        "_source_file": None,
        "_enqueued_at": None,
        "_ingested_at": ts(minutes) + timedelta(seconds=2),
    }
    row.update(kw)
    return row


def bronze_df(spark: SparkSession, rows: list[dict]) -> DataFrame:
    return spark.createDataFrame([tuple(r[f.name] for f in BRONZE_SCHEMA.fields) for r in rows], BRONZE_SCHEMA)
