# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 00 – Setup: catalog, schemas, volumes and the NYC taxi data
# MAGIC
# MAGIC Run this notebook **once** before the others. It creates – if they do not exist yet:
# MAGIC
# MAGIC | Object | Purpose |
# MAGIC |---|---|
# MAGIC | catalog `denghs26` | everything of week 8 in one place |
# MAGIC | schema `nyc_taxi` | the case study: volumes and the `bronze_*`, `silver_*`, `gold_*` tables |
# MAGIC | volume `nyc_taxi.raw` | the **raw layer**: the files exactly as delivered |
# MAGIC | volume `nyc_taxi.landing` | the **landing zone**: where external suppliers would drop files |
# MAGIC | schema `playground` + volume `playground.files` | small demo tables and files for notebooks 01–05 |
# MAGIC
# MAGIC Then it downloads NYC yellow taxi 2025 (12 Parquet files, ≈ 49 M rows, 830 MB) and the zone lookup
# MAGIC into `nyc_taxi.raw`. Free Edition cannot reach the NYC TLC website, but it can reach GitHub, so the files
# MAGIC come from the release of [stefanko-ch/practice-datasets](https://github.com/stefanko-ch/practice-datasets/releases/tag/v1).
# MAGIC
# MAGIC Uses SQL because Free Edition cannot create a catalog through the API. Safe to run again.

# COMMAND ----------

import importlib

import config

importlib.reload(config)   # picks up a change to config.py without restarting Python
catalog = config.CATALOG


def q(*parts: str) -> str:
    """Backtick-quote each part of a Unity Catalog name."""
    return ".".join(f"`{p}`" for p in parts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 – Catalog, schemas and volumes

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {q(catalog)} COMMENT 'DENG HS26 week 8: Spark and lakehouse'")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {q(catalog, 'nyc_taxi')} COMMENT 'NYC taxi case study: raw files and medallion tables'")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {q(catalog, 'playground')} COMMENT 'Small demo tables for the intro notebooks'")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {q(catalog, 'nyc_taxi', 'raw')} COMMENT 'Raw layer: files exactly as delivered'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {q(catalog, 'nyc_taxi', 'landing')} COMMENT 'Landing zone: temporary drop zone for deliveries'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {q(catalog, 'playground', 'files')} COMMENT 'Demo files written by the intro notebooks'")

display(spark.sql(f"SHOW SCHEMAS IN {q(catalog)}"))
display(spark.sql(f"SHOW VOLUMES IN {q(catalog, 'nyc_taxi')}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 – Download the files into the raw volume
# MAGIC
# MAGIC The download runs on the driver and streams every file straight into the volume, cloud to cloud – no
# MAGIC laptop and no upload involved. Each file is written to `<name>.part` first and renamed afterwards, so a
# MAGIC broken download never looks complete. Files already in the volume are skipped: re-run the cell after an
# MAGIC interruption.

# COMMAND ----------

import shutil
import urllib.request

RAW = f"/Volumes/{catalog}/nyc_taxi/raw"
RELEASE = "https://github.com/stefanko-ch/practice-datasets/releases/download/v1"
files = [f"yellow_tripdata_2025-{m:02d}.parquet" for m in range(1, 13)] + ["taxi_zone_lookup.csv"]
present = {f.name: f.size for f in dbutils.fs.ls(RAW)}

for name in files:
    if present.get(name, 0) > 0:
        print(f"{name:35} already there ({present[name] / 1e6:,.1f} MB)")
        continue
    tmp = f"{RAW}/{name}.part"
    with urllib.request.urlopen(f"{RELEASE}/{name}") as response, open(tmp, "wb") as out:
        shutil.copyfileobj(response, out, length=16 * 1024 * 1024)
    dbutils.fs.mv(tmp, f"{RAW}/{name}")
    print(f"{name:35} downloaded")

# COMMAND ----------

# MAGIC %md
# MAGIC **If the download fails** (e.g. `URLError` or a timeout): download the files on your laptop from the
# MAGIC [release page](https://github.com/stefanko-ch/practice-datasets/releases/tag/v1) and upload them via
# MAGIC **Catalog** → `denghs26` → `nyc_taxi` → **Volumes** → `raw` → **Upload to this volume**. Two or three months
# MAGIC are enough for the exercises – every notebook reads whatever is there.

# COMMAND ----------

# MAGIC %md
# MAGIC The payment-type codes are not in the trip files; they come from the TLC data dictionary. We write them as
# MAGIC a small **JSON Lines** file into the raw volume, so that bronze has a JSON source too.

# COMMAND ----------

payment_types = [
    (0, "Flex Fare trip"), (1, "Credit card"), (2, "Cash"), (3, "No charge"),
    (4, "Dispute"), (5, "Unknown"), (6, "Voided trip"),
]
json_lines = "\n".join(f'{{"payment_type": {code}, "description": "{desc}"}}' for code, desc in payment_types)
dbutils.fs.put(f"{RAW}/payment_types.json", json_lines, overwrite=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 – Check

# COMMAND ----------

rows = [(f.name, round(f.size / 1e6, 1)) for f in dbutils.fs.ls(RAW) if not f.name.endswith(".part")]
display(spark.createDataFrame(rows, "file string, size_mb double").orderBy("file"))
print(f"{len(rows)} files, {sum(r[1] for r in rows):,.0f} MB in {RAW}")
