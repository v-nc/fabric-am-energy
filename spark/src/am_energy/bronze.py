"""Bronze: every event as received, typed, append-only. Duplicates, late events and bad readings stay in."""

from __future__ import annotations

from pyspark.sql import DataFrame, functions as F, types as T

EVENT_COLUMNS: list[tuple[str, T.DataType]] = [
    ("schema_version", T.IntegerType()),
    ("machine_id", T.StringType()),
    ("meter_id", T.StringType()),
    ("seq", T.LongType()),
    ("ts_source", T.TimestampType()),
    ("ts_edge", T.TimestampType()),
    ("state", T.StringType()),
    ("job_id", T.StringType()),
    ("l1_a", T.DoubleType()),
    ("l2_a", T.DoubleType()),
    ("l3_a", T.DoubleType()),
    ("voltage_v", T.DoubleType()),
    ("power_kw", T.DoubleType()),
    ("energy_kwh_total", T.DoubleType()),
    ("opc_status", T.StringType()),
    ("power_factor", T.DoubleType()),  # schema_version 2 only
]

METADATA_COLUMNS: list[tuple[str, T.DataType]] = [
    ("_source", T.StringType()),  # "history" or "eventstream"
    ("_source_file", T.StringType()),
    ("_source_file_modified", T.TimestampType()),  # watermark for incremental file loads
    ("_enqueued_at", T.TimestampType()),  # Eventstream enqueue time; null for history
    ("_ingested_at", T.TimestampType()),
]

BRONZE_SCHEMA = T.StructType([T.StructField(n, t, True) for n, t in EVENT_COLUMNS + METADATA_COLUMNS])


def to_bronze(df: DataFrame, source: str) -> DataFrame:
    """Casts any input (history Parquet of either schema version, or Eventstream rows with string timestamps) to the
    bronze schema. Missing columns become null, so version 1 and version 2 events land in one table."""
    present = set(df.columns)
    cols = []
    for name, dtype in EVENT_COLUMNS:
        if name not in present:
            cols.append(F.lit(None).cast(dtype).alias(name))
        elif isinstance(dtype, T.TimestampType):
            cols.append(F.to_timestamp(F.col(name)).alias(name))
        else:
            cols.append(F.col(name).cast(dtype).alias(name))
    enqueued = F.to_timestamp(F.col("EventEnqueuedUtcTime")) if "EventEnqueuedUtcTime" in present else F.lit(None)
    from_files = source == "history"
    source_file = F.col("_metadata.file_path") if from_files else F.lit(None)
    source_modified = F.col("_metadata.file_modification_time") if from_files else F.lit(None)
    return df.select(
        *cols,
        F.lit(source).alias("_source"),
        source_file.cast("string").alias("_source_file"),
        source_modified.cast("timestamp").alias("_source_file_modified"),
        enqueued.cast("timestamp").alias("_enqueued_at"),
        F.current_timestamp().alias("_ingested_at"),
    )
