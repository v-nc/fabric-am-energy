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
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

workspace_id = "1a1fb8c0-f461-4025-ab89-a04bcfe30756"
lakehouse_id = "9d7ad7e7-c954-4865-8034-3986cdd9db39"
reset = False   # True: drop silver + checkpoint and rebuild from all of bronze


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import notebookutils
from pyspark.sql import functions as F
from am_energy import PipelineConfig
from am_energy.bronze import BRONZE_SCHEMA, to_bronze
from am_energy.silver import silver_increment, merge_into

root = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}"
BRONZE_HISTORY = f"{root}/Tables/bronze/history_meter_events"
BRONZE_STREAM  = f"{root}/Tables/bronze/stream_meter_events"
SILVER         = f"{root}/Tables/silver/meter_readings"
QUARANTINE     = f"{root}/Tables/silver/quarantine"
CHECKPOINT     = f"{root}/Files/_checkpoints/silver_meter_readings"
cfg = PipelineConfig()
cols = [f.name for f in BRONZE_SCHEMA]

if reset:
    for p in (SILVER, QUARANTINE, CHECKPOINT):
        if notebookutils.fs.exists(p):
            notebookutils.fs.rm(p, True)

def typed_stream(df):   # raw Eventstream rows → bronze schema
    return to_bronze(df, "eventstream").select(cols)

def bronze_context():   # all of bronze, read as a batch, for neighbour rows
    return (spark.read.format("delta").load(BRONZE_HISTORY).select(cols)
            .unionByName(typed_stream(spark.read.format("delta").load(BRONZE_STREAM))))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def process(batch, batch_id):
    batch.persist()
    silver, quarantine = silver_increment(batch, bronze_context(), cfg)
    merge_into(spark, SILVER, silver)
    merge_into(spark, QUARANTINE, quarantine)
    batch.unpersist()

new_rows = (spark.readStream.format("delta").load(BRONZE_HISTORY).select(cols)
            .unionByName(typed_stream(spark.readStream.format("delta").load(BRONZE_STREAM))))

query = (new_rows.writeStream
         .foreachBatch(process)
         .option("checkpointLocation", CHECKPOINT)
         .trigger(availableNow=True)   # process everything new, then stop
         .start())
query.awaitTermination()
print("rows in this run:", sum(p["numInputRows"] for p in query.recentProgress))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

s = spark.read.format("delta").load(SILVER)
q = spark.read.format("delta").load(QUARANTINE)
print("silver", s.count(), "| quarantine", q.count())
q.groupBy("dq_reason").count().orderBy("dq_reason").show()
s.groupBy("_source").agg(F.count("*").alias("rows"), F.sum(F.col("is_late").cast("int")).alias("late")).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
