# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************

workspace_id = "1a1fb8c0-f461-4025-ab89-a04bcfe30756"
lakehouse_id = "9d7ad7e7-c954-4865-8034-3986cdd9db39"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from am_energy import PipelineConfig, gold
from am_energy.bronze import BRONZE_SCHEMA, to_bronze

# V-Order: a Fabric write-time sort and compression of Parquet; Direct Lake and the SQL endpoint read it much faster.
spark.conf.set("spark.sql.parquet.vorder.default", "true")

root = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}"
tbl = lambda name: f"{root}/Tables/{name}"
cols = [f.name for f in BRONZE_SCHEMA]
cfg = PipelineConfig()

bronze = (spark.read.format("delta").load(tbl("bronze/history_meter_events")).select(cols)
          .unionByName(to_bronze(spark.read.format("delta").load(tbl("bronze/stream_meter_events")), "eventstream").select(cols)))
silver = spark.read.format("delta").load(tbl("silver/meter_readings")).cache()
quarantine = spark.read.format("delta").load(tbl("silver/quarantine"))
snapshots = spark.read.option("header", "true").csv(f"{root}/Files/landing/history/master/*.csv")

tables = gold.build_gold(spark, bronze, silver, quarantine, snapshots, cfg)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import time
for name, df in tables.items():
    t0 = time.time()
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(tbl(f"gold/{name}"))
    rows = spark.read.format("delta").load(tbl(f"gold/{name}")).count()
    print(f"gold.{name:26} {rows:>9} rows  {time.time() - t0:5.0f} s")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

g = lambda n: spark.read.format("delta").load(tbl(f"gold/{n}"))
g("fact_heatup").groupBy("is_overrun").count().show()
(g("fact_energy_15min").join(g("dim_machine").select("machine_sk", "hall_id"), "machine_sk")
    .groupBy("hall_id").agg(F.round(F.sum("kwh")).alias("kwh"), F.round(F.sum("cost")).alias("cost_eur")).show())
g("dim_machine").orderBy("machine_id", "valid_from").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

