# Databricks notebook source
# MAGIC %md
# MAGIC # Run one streaming query once
# MAGIC
# MAGIC A helper, started by `run_stream_once(...)` from `_config`, never by hand. It runs one streaming query with
# MAGIC `availableNow` and returns the number of rows it read:
# MAGIC
# MAGIC - with `folder`: Auto Loader on that folder (JSON files, schema location and checkpoint under `state`)
# MAGIC - with `source_table`: a stream on that Delta table
# MAGIC
# MAGIC Why a notebook of its own? When a stream fails, Databricks marks the whole notebook cell as failed, even if
# MAGIC your Python code catches the exception. In a run of its own, the failure comes back to the calling notebook as
# MAGIC a normal exception, the way a failed job task would. So the demos can show a stream that stops on purpose,
# MAGIC and carry on.

# COMMAND ----------

import json

from pyspark.sql import functions as F

for name in ["target_table", "state", "folder", "source_table", "options"]:
    dbutils.widgets.text(name, "")

target_table = dbutils.widgets.get("target_table")
state = dbutils.widgets.get("state")
folder = dbutils.widgets.get("folder")
source_table = dbutils.widgets.get("source_table")
options = json.loads(dbutils.widgets.get("options") or "{}")
error_file = f"{state}/_last_error.txt"

# COMMAND ----------

try:
    dbutils.fs.rm(error_file)
    if folder:
        reader = (spark.readStream.format("cloudFiles")
                  .option("cloudFiles.format", "json")
                  .option("cloudFiles.schemaLocation", f"{state}/_schema")
                  .option("cloudFiles.inferColumnTypes", "true"))
        for key, value in options.items():
            reader = reader.option(key, value)
        df = (reader.load(folder)
              .withColumn("_source_file", F.col("_metadata.file_path"))
              .withColumn("_ingested_at", F.current_timestamp()))
    else:
        reader = spark.readStream
        for key, value in options.items():
            reader = reader.option(key, value)
        df = reader.table(source_table)
    query = (df.writeStream
             .option("checkpointLocation", f"{state}/_checkpoint")
             .option("mergeSchema", "true")
             .trigger(availableNow=True)
             .toTable(target_table))
    query.awaitTermination()
except Exception as e:
    dbutils.fs.put(error_file, str(e), overwrite=True)
    raise

rows = sum((p if isinstance(p, dict) else json.loads(p.json)).get("numInputRows") or 0 for p in query.recentProgress)
dbutils.notebook.exit(str(rows))
