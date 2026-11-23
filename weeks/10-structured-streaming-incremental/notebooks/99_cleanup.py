# Databricks notebook source
# MAGIC %md
# MAGIC # 99 Clean up week 10
# MAGIC
# MAGIC Run this when you're done with the week. It deletes:
# MAGIC
# MAGIC - the jobs of notebook 08 (everything whose name starts with `w10_pipeline`)
# MAGIC - the tables of this week: `bronze.orders`, `bronze.orders_kafka`, `silver.orders`, `gold.restaurant_revenue` and
# MAGIC   every `playground.w10_*` table
# MAGIC - the files, checkpoints and schema locations in the volumes `landing.orders_raw`, `landing.stream_state` and
# MAGIC   `playground.w10_practice`
# MAGIC
# MAGIC It keeps the catalog, your week 8 tables, the secret scope `aiven` and `ca.pem` (in case you want to try Kafka again).
# MAGIC
# MAGIC Your Kafka service on Aiven powers itself off after 24 hours without traffic. If you don't need it anymore, delete
# MAGIC it in the Aiven Console: open the service, **⋯** → **Delete service**.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
for job in w.jobs.list():
    if job.settings and job.settings.name and job.settings.name.startswith("w10_pipeline"):
        w.jobs.delete(job.job_id)
        print("job deleted:", job.settings.name)

for table in [GOLD_TABLE, SILVER_TABLE, BRONZE_TABLE, f"{CATALOG}.bronze.orders_kafka"]:
    spark.sql(f"DROP TABLE IF EXISTS {table}")
for row in spark.sql(f"SHOW TABLES IN {PLAYGROUND} LIKE 'w10_*'").collect():
    spark.sql(f"DROP TABLE IF EXISTS {PLAYGROUND}.{row.tableName}")
print("tables dropped")

for folder in [LANDING_VOLUME, STATE_VOLUME, PRACTICE_VOLUME]:
    try:
        content = dbutils.fs.ls(folder)
    except Exception:
        content = []
    for f in content:
        dbutils.fs.rm(f.path, recurse=True)
print("files and checkpoints deleted")
