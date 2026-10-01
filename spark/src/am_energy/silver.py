"""Silver: one row per real measurement. Deduplicated, checked, typed; rejected rows go to quarantine with a reason."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window, functions as F

from .config import PipelineConfig

# A meter cannot take two readings at the same instant, so (meter_id, ts_source) identifies a measurement. seq is kept
# to detect loss within a gateway session; it restarts when a gateway is replaced, so it is not part of the key.
NATURAL_KEY = ["meter_id", "ts_source"]

REQUIRED = ["machine_id", "meter_id", "ts_source", "state", "power_kw", "energy_kwh_total", "opc_status"]


def deduplicate(bronze: DataFrame) -> DataFrame:
    """Keeps the first arrival of each measurement. Eventstream is at-least-once and the gateway resends after lost
    acknowledgements, so duplicates are expected, not exceptional."""
    w = Window.partitionBy(*NATURAL_KEY).orderBy(F.col("ts_edge").asc_nulls_last(), F.col("_ingested_at").asc())
    return bronze.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def flag_quality(df: DataFrame, cfg: PipelineConfig) -> DataFrame:
    """Adds dq_reason (null = valid) plus is_uncertain and is_late flags. The first matching rule wins."""
    missing = F.lit(False)
    for c in REQUIRED:
        missing = missing | F.col(c).isNull()

    # Spike: one reading far above both neighbours of the same meter. Bad readings don't count as neighbours.
    w = Window.partitionBy("meter_id").orderBy("ts_source")
    usable = F.col("opc_status") != "Bad"
    prev_kw = F.lag(F.when(usable, F.col("power_kw"))).over(w)
    next_kw = F.lead(F.when(usable, F.col("power_kw"))).over(w)
    spike = (
        (F.col("power_kw") > cfg.spike_min_kw)
        & prev_kw.isNotNull()
        & next_kw.isNotNull()
        & (F.col("power_kw") > F.greatest(prev_kw, next_kw) * cfg.spike_ratio)
    )

    reason = (
        F.when(missing, "missing_value")
        .when(F.col("opc_status") == "Bad", "bad_status")
        .when((F.col("power_kw") < 0) | (F.col("energy_kwh_total") < 0), "negative_value")
        .when(F.col("power_kw") > cfg.max_power_kw, "implausible_power")
        .when(spike, "spike")
    )
    late_s = F.col("ts_edge").cast("double") - F.col("ts_source").cast("double")
    return df.select(
        "*",
        reason.alias("dq_reason"),
        (F.col("opc_status") == "Uncertain").alias("is_uncertain"),
        (late_s > cfg.late_threshold_s).alias("is_late"),
    )


def to_silver(bronze: DataFrame, cfg: PipelineConfig) -> tuple[DataFrame, DataFrame]:
    """Bronze → (silver readings, quarantine)."""
    checked = flag_quality(deduplicate(bronze), cfg)
    silver = checked.where(F.col("dq_reason").isNull()).drop("dq_reason")
    quarantine = checked.where(F.col("dq_reason").isNotNull())
    return silver, quarantine


def merge_into(spark: SparkSession, table: str, updates: DataFrame) -> None:
    """Insert-only MERGE on the natural key: a measurement that is already in silver is never overwritten, so replays
    and duplicates across batches are harmless and the job can be re-run safely (idempotent)."""
    from delta.tables import DeltaTable

    on = " AND ".join(f"t.{k} = s.{k}" for k in NATURAL_KEY)
    (
        DeltaTable.forName(spark, table)
        .alias("t")
        .merge(updates.alias("s"), on)
        .whenNotMatchedInsertAll()
        .execute()
    )
