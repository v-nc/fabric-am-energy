"""Gold: star schema for the Direct Lake semantic model.

Energy comes from the meter's kWh counter, not from integrating instantaneous power: the counter keeps counting
through dropouts, so the energy over a gap is still known even when the readings in between are missing.
"""

from __future__ import annotations

from datetime import date, timedelta

from pyspark.sql import Column, DataFrame, SparkSession, Window, functions as F

from .config import PipelineConfig

BUCKET_S = 15 * 60


# --- readings → intervals ------------------------------------------------------------------------------------------


def with_energy_deltas(silver: DataFrame, cfg: PipelineConfig) -> DataFrame:
    """Adds, per reading, the interval since the previous reading of the same meter and the energy drawn in it.

    A counter that goes down means the meter was replaced: the new counter started near zero, so the energy since the
    previous reading is the new counter value itself. A gap longer than gap_threshold_s keeps its energy but gets the
    state "unknown", because nobody saw what the 3D printer did meanwhile.
    """
    w = Window.partitionBy("meter_id").orderBy("ts_source")
    prev_energy = F.lag("energy_kwh_total").over(w)
    raw = F.col("energy_kwh_total") - prev_energy
    gap_s = F.col("ts_source").cast("double") - F.col("prev_ts").cast("double")
    return (
        silver.withColumn("prev_ts", F.lag("ts_source").over(w))
        .withColumn("prev_state", F.lag("state").over(w))
        .withColumn("counter_reset", F.coalesce(raw < 0, F.lit(False)))
        .withColumn("delta_kwh", F.when(raw < 0, F.col("energy_kwh_total")).otherwise(raw))
        .withColumn("interval_s", gap_s)
        .withColumn("is_gap", F.coalesce(gap_s > cfg.gap_threshold_s, F.lit(False)))
        .withColumn("interval_state", F.when(F.col("is_gap"), F.lit("unknown")).otherwise(F.col("prev_state")))
    )


def allocate_to_buckets(deltas: DataFrame) -> DataFrame:
    """Spreads each interval's energy over the 15-minute buckets it overlaps, in proportion to time. An interval that
    crosses a bucket boundary (or a 3-hour dropout) is split instead of being booked entirely at its end."""
    start = F.col("prev_ts").cast("double")
    end = F.col("ts_source").cast("double")
    first = (F.floor(start / BUCKET_S) * BUCKET_S).cast("long")
    last = (F.floor((end - 0.001) / BUCKET_S) * BUCKET_S).cast("long")
    b = F.col("bucket_epoch")
    overlap = F.least(end, b + BUCKET_S) - F.greatest(start, b)
    return (
        deltas.where(F.col("delta_kwh").isNotNull() & (F.col("interval_s") > 0))
        .withColumn("bucket_epoch", F.explode(F.sequence(first, last, F.lit(BUCKET_S).cast("long"))))
        .select(
            "machine_id",
            "meter_id",
            F.timestamp_seconds("bucket_epoch").alias("bucket_start"),
            F.col("interval_state").alias("state"),
            (F.col("delta_kwh") * overlap / F.col("interval_s")).alias("kwh"),
            overlap.alias("seconds"),
        )
    )


# --- local time and tariff -----------------------------------------------------------------------------------------


def local(ts: Column, cfg: PipelineConfig) -> Column:
    return F.from_utc_timestamp(ts, cfg.timezone)


def date_key(local_ts: Column) -> Column:
    return F.date_format(local_ts, "yyyyMMdd").cast("int")


def iso_weekday(local_ts: Column) -> Column:
    return ((F.dayofweek(local_ts) + 5) % 7) + 1  # Spark: 1 = Sunday; ISO: 1 = Monday


def tariff_band(local_ts: Column, cfg: PipelineConfig) -> Column:
    hour = F.hour(local_ts)
    peak = iso_weekday(local_ts).isin(list(cfg.peak_weekdays)) & (hour >= cfg.peak_from_hour) & (hour < cfg.peak_to_hour)
    return F.when(peak, F.lit("peak")).otherwise(F.lit("off_peak"))


def price(band: Column, cfg: PipelineConfig) -> Column:
    return F.when(band == "peak", F.lit(cfg.peak_price_per_kwh)).otherwise(F.lit(cfg.off_peak_price_per_kwh))


# --- facts ---------------------------------------------------------------------------------------------------------


def fact_energy_15min(silver: DataFrame, cfg: PipelineConfig) -> DataFrame:
    """Energy, power and data completeness per 3D printer and 15-minute bucket, priced with the time-of-use tariff.
    Completeness = minutes of the bucket that have at least one valid reading."""
    deltas = with_energy_deltas(silver, cfg)
    energy = (
        allocate_to_buckets(deltas)
        .groupBy("machine_id", "bucket_start")
        .agg(F.sum("kwh").alias("kwh"))
    )
    bucket = F.timestamp_seconds((F.floor(F.col("ts_source").cast("double") / BUCKET_S) * BUCKET_S).cast("long"))
    readings = (
        silver.withColumn("bucket_start", bucket)
        .groupBy("machine_id", "bucket_start")
        .agg(
            F.avg("power_kw").alias("avg_power_kw"),
            F.max("power_kw").alias("max_power_kw"),
            F.count("*").alias("readings"),
            F.countDistinct(F.date_trunc("minute", "ts_source")).alias("minutes_covered"),
        )
    )
    lt = local(F.col("bucket_start"), cfg)
    band = tariff_band(lt, cfg)
    return (
        energy.join(readings, ["machine_id", "bucket_start"], "full_outer")
        .fillna({"kwh": 0.0, "readings": 0, "minutes_covered": 0})
        .select(
            "machine_id",
            "bucket_start",
            lt.alias("bucket_start_local"),
            date_key(lt).alias("date_key"),
            (F.hour(lt) * 4 + F.floor(F.minute(lt) / 15)).cast("int").alias("slot_of_day"),
            "kwh",
            "avg_power_kw",
            "max_power_kw",
            "readings",
            "minutes_covered",
            (F.col("minutes_covered") / 15.0).alias("completeness"),
            band.alias("tariff_band"),
            (F.col("kwh") * price(band, cfg)).alias("cost"),
        )
    )


def fact_state_energy(silver: DataFrame, cfg: PipelineConfig) -> DataFrame:
    """Hours and energy per 3D printer, local day and state (including "unknown" for gaps). Idle share is a DAX
    measure on top of this."""
    lt = local(F.col("bucket_start"), cfg)
    return (
        allocate_to_buckets(with_energy_deltas(silver, cfg))
        .groupBy("machine_id", date_key(lt).alias("date_key"), "state")
        .agg((F.sum("seconds") / 3600.0).alias("hours"), F.sum("kwh").alias("kwh"))
    )


def state_intervals(silver: DataFrame, cfg: PipelineConfig) -> DataFrame:
    """Contiguous runs of one state per 3D printer. An interval ends where the next one starts; the last interval
    in the data is open (is_open) because it may still be running. Energy comes from the deltas inside it."""
    deltas = with_energy_deltas(silver, cfg)
    w = Window.partitionBy("machine_id").orderBy("ts_source")
    changed = F.coalesce(F.col("state") != F.lag("state").over(w), F.lit(True)).cast("int")
    runs = deltas.withColumn("run_id", F.sum(changed).over(w))
    # The delta of a reading belongs to the interval its previous reading was in.
    energy = (
        runs.withColumn("prev_run_id", F.lag("run_id").over(w))
        .where(F.col("prev_run_id").isNotNull())
        .groupBy("machine_id", F.col("prev_run_id").alias("run_id"))
        .agg(F.sum("delta_kwh").alias("kwh"), F.max(F.col("is_gap").cast("int")).cast("boolean").alias("has_gap"))
    )
    intervals = runs.groupBy("machine_id", "run_id").agg(
        F.first("state").alias("state"),
        F.min("ts_source").alias("start_ts"),
        F.max("ts_source").alias("last_ts"),
        F.max("job_id").alias("job_id"),
    )
    wi = Window.partitionBy("machine_id").orderBy("run_id")
    next_start = F.lead("start_ts").over(wi)
    return (
        intervals.join(energy, ["machine_id", "run_id"], "left")
        .withColumn("end_ts", F.coalesce(next_start, F.col("last_ts")))
        .withColumn("is_open", next_start.isNull())
        .withColumn("is_first", F.lag("run_id").over(wi).isNull())
        .withColumn("duration_h", (F.col("end_ts").cast("double") - F.col("start_ts").cast("double")) / 3600.0)
        .withColumn("prev_state", F.lag("state").over(wi))
        .withColumn("next_state", F.lead("state").over(wi))
        .withColumn("prev_kwh", F.lag("kwh").over(wi))
        .withColumn("next_kwh", F.lead("kwh").over(wi))
        .withColumn("next_job_id", F.lead("job_id").over(wi))
        .fillna({"kwh": 0.0, "has_gap": False})
    )


def fact_heatup(intervals: DataFrame, cfg: PipelineConfig) -> DataFrame:
    """One row per complete heat-up. The job is the build that starts right after it (matched by time, since job_id
    is only reported while building). Heat-ups cut off at the start or end of the data are left out."""
    return intervals.where(
        (F.col("state") == "heat_up") & ~F.col("is_open") & ~F.col("is_first")
    ).select(
        "machine_id",
        F.when(F.col("next_state") == "building", F.col("next_job_id")).alias("job_id"),
        "start_ts",
        "end_ts",
        date_key(local(F.col("start_ts"), cfg)).alias("date_key"),
        "duration_h",
        F.lit(cfg.heatup_baseline_h).alias("baseline_h"),
        F.lit(cfg.heatup_threshold_h).alias("threshold_h"),
        # A gap hides when the heat-up really ended, so it can't be judged; it shows on the data quality page instead.
        ((F.col("duration_h") > cfg.heatup_threshold_h) & ~F.col("has_gap")).alias("is_overrun"),
        "kwh",
        "has_gap",
    )


def fact_build_job(intervals: DataFrame, cfg: PipelineConfig) -> DataFrame:
    """One row per complete build job, with the energy of the heat-up before it and the cool-down after it."""
    builds = intervals.where((F.col("state") == "building") & ~F.col("is_open") & ~F.col("is_first"))
    heatup_kwh = F.when(F.col("prev_state") == "heat_up", F.col("prev_kwh")).otherwise(F.lit(0.0))
    cooldown_kwh = F.when(F.col("next_state") == "cool_down", F.col("next_kwh")).otherwise(F.lit(0.0))
    return builds.select(
        "job_id",
        "machine_id",
        "start_ts",
        "end_ts",
        date_key(local(F.col("start_ts"), cfg)).alias("date_key"),
        F.col("duration_h").alias("build_h"),
        F.col("kwh").alias("build_kwh"),
        heatup_kwh.alias("heatup_kwh"),
        cooldown_kwh.alias("cooldown_kwh"),
        (F.col("kwh") + heatup_kwh + cooldown_kwh).alias("job_kwh"),
        (F.col("kwh") / F.col("duration_h")).alias("kwh_per_build_hour"),
        "has_gap",
    )


def fact_data_quality_daily(bronze: DataFrame, silver: DataFrame, quarantine: DataFrame, cfg: PipelineConfig) -> DataFrame:
    """Per meter and local day: what arrived, what was removed and why, and how complete the day is."""
    def day(ts: str) -> Column:
        return date_key(local(F.col(ts), cfg)).alias("date_key")

    received = bronze.groupBy("meter_id", day("ts_source")).agg(F.count("*").alias("events_received"))
    measurements = (
        bronze.dropDuplicates(["meter_id", "ts_source"]).groupBy("meter_id", day("ts_source")).agg(F.count("*").alias("measurements"))
    )
    quarantined = (
        quarantine.groupBy("meter_id", day("ts_source"))
        .pivot("dq_reason", ["missing_value", "bad_status", "negative_value", "implausible_power", "spike"])
        .agg(F.count(F.lit(1)))
    )
    for reason in ["missing_value", "bad_status", "negative_value", "implausible_power", "spike"]:
        quarantined = quarantined.withColumnRenamed(reason, f"rejected_{reason}")
    deltas = with_energy_deltas(silver, cfg)
    valid = deltas.groupBy("meter_id", day("ts_source")).agg(
        F.count("*").alias("valid_readings"),
        F.sum(F.col("is_uncertain").cast("int")).alias("uncertain_readings"),
        F.sum(F.col("is_late").cast("int")).alias("late_events"),
        F.sum(F.col("is_gap").cast("int")).alias("gaps"),
        (F.sum(F.when(F.col("is_gap"), F.col("interval_s"))) / 60.0).alias("gap_minutes"),
        F.sum(F.col("counter_reset").cast("int")).alias("counter_resets"),
        F.countDistinct(F.date_trunc("minute", "ts_source")).alias("minutes_covered"),
    )
    return (
        received.join(measurements, ["meter_id", "date_key"], "left")
        .join(valid, ["meter_id", "date_key"], "left")
        .join(quarantined, ["meter_id", "date_key"], "left")
        .fillna(0)
        .withColumn("duplicates_removed", F.col("events_received") - F.col("measurements"))
        .withColumn("completeness", F.col("minutes_covered") / 1440.0)
    )


# --- dimensions ----------------------------------------------------------------------------------------------------


def dim_date(spark: SparkSession, start: date, end: date) -> DataFrame:
    d = F.col("date")
    return spark.sql(f"SELECT explode(sequence(DATE'{start}', DATE'{end}', INTERVAL 1 DAY)) AS date").select(
        F.date_format(d, "yyyyMMdd").cast("int").alias("date_key"),
        "date",
        F.year(d).alias("year"),
        F.quarter(d).alias("quarter"),
        F.month(d).alias("month"),
        F.date_format(d, "MMMM").alias("month_name"),
        F.date_format(d, "yyyy-MM").alias("year_month"),
        F.weekofyear(d).alias("iso_week"),
        iso_weekday(d).alias("iso_weekday"),
        F.date_format(d, "EEEE").alias("weekday_name"),
        (iso_weekday(d) >= 6).alias("is_weekend"),
    )


def dim_tariff(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    hours = f"{cfg.peak_from_hour:02d}:00–{cfg.peak_to_hour:02d}:00"
    return spark.createDataFrame(
        [
            ("peak", f"Weekdays {list(cfg.peak_weekdays)} {hours} local time", cfg.peak_price_per_kwh, cfg.currency),
            ("off_peak", "All other times", cfg.off_peak_price_per_kwh, cfg.currency),
        ],
        "tariff_band string, description string, price_per_kwh double, currency string",
    )


MACHINE_TRACKED = ["hall_id", "meter_id", "meter_serial"]


def scd2_rows(snapshots: DataFrame) -> DataFrame:
    """Builds SCD type 2 rows from dated master data snapshots: a new version whenever a tracked attribute changes.
    Each version is valid from its first snapshot until the next version starts (valid_to exclusive, null = current)."""
    w = Window.partitionBy("machine_id").orderBy("snapshot_date")
    attrs = F.concat_ws("|", *[F.col(c) for c in MACHINE_TRACKED])
    changed = F.coalesce(attrs != F.lag(attrs).over(w), F.lit(True))
    versions = (
        snapshots.withColumn("_changed", changed)
        .where("_changed")
        .withColumn("valid_from", F.to_timestamp("snapshot_date"))
    )
    wv = Window.partitionBy("machine_id").orderBy("valid_from")
    return versions.select(
        F.concat_ws("_", "machine_id", F.date_format("valid_from", "yyyyMMdd")).alias("machine_sk"),
        "machine_id",
        *MACHINE_TRACKED,
        "valid_from",
        F.lead("valid_from").over(wv).alias("valid_to"),
        F.lead("valid_from").over(wv).isNull().alias("is_current"),
    )


def apply_scd2(spark: SparkSession, table: str, snapshot: DataFrame, as_of: str) -> None:
    """Merges one master data snapshot into an SCD type 2 Delta table: a changed machine gets its current version
    closed (valid_to = as_of) and a new current version inserted, in one MERGE. Unchanged machines are untouched."""
    from delta.tables import DeltaTable

    target = DeltaTable.forName(spark, table)
    current = target.toDF().where("is_current")
    changed = (
        snapshot.alias("s")
        .join(current.alias("c"), "machine_id", "left")
        .where(" OR ".join([f"c.machine_id IS NULL"] + [f"NOT (s.{c} <=> c.{c})" for c in MACHINE_TRACKED]))
        .select("s.*", F.col("c.machine_id").isNotNull().alias("_existing"))
    )
    # Changed machines appear twice: once to close the old version (matched on machine_id), once to insert the new
    # one (merge_key null never matches).
    staged = changed.select(F.col("machine_id").alias("merge_key"), "*").where("_existing").unionByName(
        changed.select(F.lit(None).cast("string").alias("merge_key"), "*")
    )
    ts = F.to_timestamp(F.lit(as_of))
    (
        target.alias("t")
        .merge(staged.alias("s"), "t.machine_id = s.merge_key AND t.is_current")
        .whenMatchedUpdate(set={"valid_to": ts, "is_current": F.lit(False)})
        .whenNotMatchedInsert(
            values={
                "machine_sk": F.concat_ws("_", F.col("s.machine_id"), F.date_format(ts, "yyyyMMdd")),
                "machine_id": "s.machine_id",
                **{c: f"s.{c}" for c in MACHINE_TRACKED},
                "valid_from": ts,
                "valid_to": F.lit(None).cast("timestamp"),
                "is_current": F.lit(True),
            }
        )
        .execute()
    )


def attach_machine_sk(fact: DataFrame, dim_machine: DataFrame, ts_col: str) -> DataFrame:
    """Gives each fact row the dim_machine version that was valid at its timestamp, so energy before a hall move
    stays with the old hall."""
    d = dim_machine.select("machine_sk", F.col("machine_id").alias("_mid"), "valid_from", "valid_to")
    cond = (
        (F.col("machine_id") == F.col("_mid"))
        & (F.col(ts_col) >= F.col("valid_from"))
        & (F.col("valid_to").isNull() | (F.col(ts_col) < F.col("valid_to")))
    )
    return fact.join(F.broadcast(d), cond, "left").drop("_mid", "valid_from", "valid_to")


STATES = [
    ("off", "Off", 1),
    ("idle", "Idle", 2),
    ("heat_up", "Heat-up", 3),
    ("building", "Building", 4),
    ("cool_down", "Cool-down", 5),
    ("unpacking", "Unpacking", 6),
    ("unknown", "Unknown (gap)", 7),
]


def dim_state(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(STATES, "state string, state_name string, sort_order int")


def dim_time_slot(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    """96 local 15-minute slots of a day. The tariff band depends on the weekday too, so it lives on the fact."""
    rows = [(s, f"{s // 4:02d}:{s % 4 * 15:02d}", s // 4, cfg.peak_from_hour <= s // 4 < cfg.peak_to_hour) for s in range(96)]
    return spark.createDataFrame(rows, "slot_of_day int, slot_label string, hour int, is_peak_hours boolean")


def _day_noon_utc(date_key_col: str) -> Column:
    """Day-level facts take the dim_machine version valid at noon UTC of their day (versions change at midnight)."""
    return F.to_timestamp(F.concat(F.col(date_key_col).cast("string"), F.lit(" 12")), "yyyyMMdd HH")


def build_gold(
    spark: SparkSession, bronze: DataFrame, silver: DataFrame, quarantine: DataFrame, snapshots: DataFrame,
    cfg: PipelineConfig,
) -> dict[str, DataFrame]:
    """All gold tables from silver and the master data snapshots. Facts carry machine_sk, the dim_machine version
    valid at the fact's time, so energy before a hall move stays with the old hall."""
    dim_machine = scd2_rows(snapshots).cache()
    intervals = state_intervals(silver, cfg).cache()
    meters = dim_machine.select("meter_id", "machine_id").distinct()
    span = silver.agg(F.min("ts_source").alias("lo"), F.max("ts_source").alias("hi")).first()
    keyed = lambda fact, ts: attach_machine_sk(fact, dim_machine, ts)  # noqa: E731
    day_keyed = lambda fact: (  # noqa: E731
        keyed(fact.withColumn("_noon", _day_noon_utc("date_key")), "_noon").drop("_noon")
    )
    return {
        "dim_machine": dim_machine,
        "dim_date": dim_date(spark, span["lo"].date() - timedelta(days=1), span["hi"].date() + timedelta(days=1)),
        "dim_tariff": dim_tariff(spark, cfg),
        "dim_state": dim_state(spark),
        "dim_time_slot": dim_time_slot(spark, cfg),
        "fact_energy_15min": keyed(fact_energy_15min(silver, cfg), "bucket_start"),
        "fact_state_energy": day_keyed(fact_state_energy(silver, cfg)),
        "fact_heatup": keyed(fact_heatup(intervals, cfg), "start_ts"),
        "fact_build_job": keyed(fact_build_job(intervals, cfg), "start_ts"),
        "fact_data_quality_daily": day_keyed(
            fact_data_quality_daily(bronze, silver, quarantine, cfg).join(meters, "meter_id", "left")
        ),
    }
