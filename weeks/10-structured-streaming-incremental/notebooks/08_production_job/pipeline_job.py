# Databricks notebook source
# MAGIC %md
# MAGIC # The pipeline as one job
# MAGIC
# MAGIC This notebook is what the job of notebook 08 starts. It runs bronze, silver and gold once, each layer only for
# MAGIC what's new since the last run, and then checks gold against a full recompute from silver. If they don't match,
# MAGIC it fails, so the job turns red and somebody looks.
# MAGIC
# MAGIC You can run it by hand too. It doesn't matter how often: a run without new files does nothing.

# COMMAND ----------

# MAGIC %run ../_config

# COMMAND ----------

# MAGIC %run ../_generator

# COMMAND ----------

# MAGIC %run ../_pipeline

# COMMAND ----------

import json


def version(table):
    return spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first().version


def merged_since(table, after_version):
    # Rows inserted and updated by all MERGE commits after `after_version`, from the table's history
    rows = (spark.sql(f"DESCRIBE HISTORY {table}")
            .where(f"version > {after_version} AND operation = 'MERGE'")
            .selectExpr("cast(operationMetrics['numTargetRowsInserted'] AS BIGINT) AS ins",
                        "cast(operationMetrics['numTargetRowsUpdated'] AS BIGINT) AS upd")
            .agg(F.coalesce(F.sum("ins"), F.lit(0)).alias("ins"), F.coalesce(F.sum("upd"), F.lit(0)).alias("upd"))
            .first())
    return int(rows.ins), int(rows.upd)


# The numbers come from the tables, not from the streaming queries' progress reports: in a job those can be empty
bronze_before = spark.table(BRONZE_TABLE).count()
silver_version, gold_version = version(SILVER_TABLE), version(GOLD_TABLE)

run_bronze_inline()     # a new column stops this once: the job's retry runs the notebook again
run_silver()
run_gold()

silver_ins, silver_upd = merged_since(SILVER_TABLE, silver_version)
gold_ins, gold_upd = merged_since(GOLD_TABLE, gold_version)
summary = {
    "bronze_new_rows": spark.table(BRONZE_TABLE).count() - bronze_before,
    "silver_new_orders": silver_ins,
    "silver_updated_orders": silver_upd,
    "gold_restaurants_changed": gold_ins + gold_upd,
}

# COMMAND ----------

wrong = gold_diffs().where("diff_orders != 0 OR diff_revenue != 0")
if wrong.count() > 0:
    display(wrong)
    raise AssertionError("gold doesn't match a full recompute from silver")
summary["check"] = "gold matches silver"
print(summary)
dbutils.notebook.exit(json.dumps(summary))
