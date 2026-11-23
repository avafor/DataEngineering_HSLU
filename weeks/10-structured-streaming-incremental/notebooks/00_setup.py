# Databricks notebook source
# MAGIC %md
# MAGIC # 00 Setup
# MAGIC
# MAGIC Run this once before you start. It creates the schemas and volumes of this week in the catalog
# MAGIC `denghs26` from week 8. If you skipped week 8, it creates the catalog as well.
# MAGIC
# MAGIC | Where | What for |
# MAGIC |---|---|
# MAGIC | `landing.orders_raw` | the folder where the delivery app drops its order files |
# MAGIC | `landing.stream_state` | checkpoints and schema locations of the pipeline |
# MAGIC | `bronze`, `silver`, `gold` | the tables of the pipeline, built step by step from notebook 01 on |
# MAGIC | `playground` and `playground.w10_practice` | a place to try things out, so the pipeline stays clean |
# MAGIC
# MAGIC Want to start over? Set `reset` to `yes` at the top and run it again. That drops the tables of this
# MAGIC week and deletes the files and checkpoints. The catalog and your week 8 tables stay.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

dbutils.widgets.dropdown("reset", "no", ["no", "yes"], "Reset this week")
RESET = dbutils.widgets.get("reset") == "yes"

# COMMAND ----------

# Exists already if you did week 8. SQL, because Free Edition can't create a catalog through the API
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")

for schema in ["landing", "bronze", "silver", "gold", "playground"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.landing.orders_raw")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.landing.stream_state")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.playground.w10_practice")

# COMMAND ----------

if RESET:
    for table in [GOLD_TABLE, SILVER_TABLE, BRONZE_TABLE]:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
    for row in spark.sql(f"SHOW TABLES IN {PLAYGROUND} LIKE 'w10_*'").collect():
        spark.sql(f"DROP TABLE IF EXISTS {PLAYGROUND}.{row.tableName}")
    for folder in [INCOMING_DIR, STATE_VOLUME, PRACTICE_VOLUME]:
        try:
            content = dbutils.fs.ls(folder)
        except Exception:
            content = []
        for f in content:
            dbutils.fs.rm(f.path, recurse=True)
    print("Reset done: tables of this week dropped, files and checkpoints deleted.")

# COMMAND ----------

dbutils.fs.mkdirs(INCOMING_DIR)
print("Landing folder :", INCOMING_DIR)
print("Practice folder:", PRACTICE_VOLUME)
print("Ready. Go on with 01_auto_loader/1_demo_auto_loader.")
