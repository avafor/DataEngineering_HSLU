# Databricks notebook source
# MAGIC %md
# MAGIC # 99 – Remove the week 8 catalog
# MAGIC
# MAGIC Drops the catalog **with everything in it**: the schemas `nyc_taxi` and `playground`, all tables, the volumes
# MAGIC and their files (830 MB of taxi data). Cannot be undone. The notebooks themselves are not touched.
# MAGIC
# MAGIC Two confirmations are required: `confirm` must be `yes`, and `confirm_catalog` must repeat the catalog name.

# COMMAND ----------

dbutils.widgets.text("confirm", "", "Type yes to confirm")
dbutils.widgets.text("confirm_catalog", "", "Repeat the catalog name")

# COMMAND ----------

import importlib

import config

importlib.reload(config)
catalog = config.CATALOG   # the catalog to drop, from config.py
confirm = dbutils.widgets.get("confirm").strip().lower()
confirm_catalog = dbutils.widgets.get("confirm_catalog").strip()

if catalog in ("workspace", "system", "samples", "main", "hive_metastore"):
    raise ValueError(f"Refusing to drop the built-in catalog {catalog!r}.")
if confirm != "yes" or confirm_catalog != catalog:
    raise ValueError(
        f"Not confirmed: set confirm=yes and confirm_catalog={catalog} to drop the catalog {catalog!r}."
    )

# COMMAND ----------

existing = [r.catalog for r in spark.sql("SHOW CATALOGS").collect()]
if catalog not in existing:
    print(f"Catalog {catalog!r} does not exist – nothing to do.")
else:
    for row in spark.sql(f"SHOW SCHEMAS IN `{catalog}`").collect():
        print("dropping schema", row[0])
    spark.sql(f"DROP CATALOG `{catalog}` CASCADE")
    print(f"Catalog {catalog!r} dropped.")
