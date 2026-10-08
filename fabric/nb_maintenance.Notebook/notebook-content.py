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
retain_hours = 168   # VACUUM keeps 7 days of old files


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

root = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables"
appended = ["bronze/stream_meter_events", "bronze/history_meter_events", "silver/meter_readings", "silver/quarantine"]
rewritten = [f"gold/{t}" for t in ["fact_energy_15min", "fact_state_energy", "fact_heatup", "fact_build_job",
                                    "fact_data_quality_daily", "dim_machine", "dim_date", "dim_tariff", "dim_state", "dim_time_slot"]]

def files(path):
    return spark.sql(f"DESCRIBE DETAIL delta.`{path}`").first()["numFiles"]

for t in appended + rewritten:
    path = f"{root}/{t}"
    before = files(path)
    if t in appended:                          # many small appends → compact them (and V-Order the result)
        spark.sql(f"OPTIMIZE delta.`{path}` VORDER")
    spark.sql(f"VACUUM delta.`{path}` RETAIN {retain_hours} HOURS")   # delete files no longer referenced
    print(f"{t:32} files {before:>4} → {files(path):>4}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
