# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "8b4a3c41-5b44-8519-4851-71b587a14929",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     },
# META     "warehouse": {
# META       "default_warehouse": "bf4113f4-52fe-4263-b0bc-f49755656c12",
# META       "known_warehouses": [
# META         {
# META           "id": "bf4113f4-52fe-4263-b0bc-f49755656c12",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************


# PARAMETERS CELL ********************

full_load = True
workspace_id = "1a1fb8c0-f461-4025-ab89-a04bcfe30756"
lakehouse_id = "9d7ad7e7-c954-4865-8034-3986cdd9db39"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import notebookutils
from pyspark.sql import functions as F
from am_energy.bronze import to_bronze

root = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}"
landing = f"{root}/Files/landing/history"
target = f"{root}/Tables/bronze/history_meter_events"

files = [f for f in notebookutils.fs.ls(landing) if f.name.startswith("meter_events_") and f.name.endswith(".parquet")]

if not full_load and notebookutils.fs.exists(f"{target}/_delta_log"):
    # Watermark: the newest source file already loaded. Only files changed since then are read.
    watermark_ms = (spark.read.format("delta").load(target)
                    .agg(F.max(F.unix_millis("_source_file_modified"))).first()[0] or 0)
    files = [f for f in files if f.modifyTime > watermark_ms]

print(f"{len(files)} file(s) to load")
if files:
    raw = spark.read.option("mergeSchema", "true").parquet(*[f.path for f in files])
    (to_bronze(raw, "history").write.format("delta")
        .mode("overwrite" if full_load else "append")
        .option("overwriteSchema", str(full_load).lower())
        .save(target))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(spark.read.format("delta").load(target)
    .groupBy("schema_version")
    .agg(F.count("*").alias("rows"), F.count("power_factor").alias("with_pf"))
    .show())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
