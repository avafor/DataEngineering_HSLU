# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 04 – From a folder of Parquet files to a Delta table
# MAGIC
# MAGIC **Slides:** *Why Parquet alone is not enough* · *What is a table format?* · *A table format: Parquet files + a log* ·
# MAGIC *What table formats add* · *Delta Lake: what one commit contains* · *Delta Lake: one log, many versions*
# MAGIC
# MAGIC **Goal (≈ 30 min):**
# MAGIC
# MAGIC | Section | Idea |
# MAGIC |---|---|
# MAGIC | 1 | Plain Parquet: fixing one row means rewriting, a stray file is read as data, the old state is gone |
# MAGIC | 2 | The same table in Delta: v0 … v4 exactly as on the slide (write, append, MERGE, DELETE, new column) |
# MAGIC | 3 | Inside `_delta_log/`: what one commit contains |
# MAGIC | 4 | Files are never changed, only added and marked as removed – time travel and VACUUM |
# MAGIC | 5 | Exercises |
# MAGIC
# MAGIC Works with six made-up trips in the playground volume, so every file and every commit can be looked at.
# MAGIC The Delta table is written to a **path** (not registered in the catalog), because Unity Catalog does not let us
# MAGIC browse the storage of a managed table.

# COMMAND ----------

import importlib

from pyspark.sql import functions as F

import config

importlib.reload(config)   # picks up a change to config.py without restarting Python
catalog = config.CATALOG

FILES = f"/Volumes/{catalog}/playground/files"
PLAIN = f"{FILES}/trips_plain_parquet"
DELTA = f"{FILES}/trips_delta"
for path in (PLAIN, DELTA):
    dbutils.fs.rm(path, recurse=True)      # start clean, so the versions below always start at 0

SCHEMA = "trip_id int, pickup string, zone string, fare double"
trips_v0 = spark.createDataFrame(
    [(1, "08:12", "JFK", 12.5), (2, "08:40", "JFK", 8.0), (3, "09:05", "SoHo", 23.0)], SCHEMA)


def part_files(path: str) -> list:
    return sorted(f.name for f in dbutils.fs.ls(path) if f.name.endswith(".parquet"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 – Plain Parquet: just files
# MAGIC
# MAGIC A folder of Parquet files has no list of which files belong to the table. Whatever is in the folder is the table.

# COMMAND ----------

trips_v0.write.mode("overwrite").parquet(PLAIN)
print(part_files(PLAIN))
spark.read.parquet(PLAIN).orderBy("trip_id").display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Fix one wrong row** – the fare of trip 2 should be 9.0. Parquet files are immutable, there is no `UPDATE`:
# MAGIC read everything, change it in memory, and **overwrite the whole folder**. The old state is gone – no history.

# COMMAND ----------

rows = (spark.read.parquet(PLAIN)
        .withColumn("fare", F.when(F.col("trip_id") == 2, F.lit(9.0)).otherwise(F.col("fare")))
        .collect())                   # materialise first: Spark must not read the folder it is overwriting
spark.createDataFrame(rows, SCHEMA).write.mode("overwrite").parquet(PLAIN)
spark.read.parquet(PLAIN).orderBy("trip_id").display()

# COMMAND ----------

# MAGIC %md
# MAGIC **A half-written file is visible.** Imagine a job that crashed while writing `part-0004`: the file is in the folder,
# MAGIC so every reader picks it up. We simulate it by copying one part file under a new name – its trip is now counted twice.

# COMMAND ----------

first = part_files(PLAIN)[0]
dbutils.fs.cp(f"{PLAIN}/{first}", f"{PLAIN}/part-0004-crashed-job.parquet")
print(f"{spark.read.parquet(PLAIN).count()} rows – but the table has 3 trips")
dbutils.fs.rm(f"{PLAIN}/part-0004-crashed-job.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 – The same table in Delta: one log, many versions
# MAGIC
# MAGIC Delta Lake = the same Parquet files **plus a transaction log** in `_delta_log/`. Every write is one commit, one new
# MAGIC version. We replay the slide *Delta Lake: one log, many versions*:
# MAGIC
# MAGIC | Version | Operation |
# MAGIC |---|---|
# MAGIC | v0 | write – create the table |
# MAGIC | v1 | append – new trips arrive |
# MAGIC | v2 | MERGE – update + insert |
# MAGIC | v3 | DELETE – one customer (GDPR) |
# MAGIC | v4 | new column – schema evolution |

# COMMAND ----------

# v0: write
trips_v0.write.format("delta").save(DELTA)

# v1: append
spark.createDataFrame([(4, "10:15", "JFK", 61.0)], SCHEMA).write.format("delta").mode("append").save(DELTA)

# v2: MERGE – trip 2 gets the corrected fare, trips 5 and 6 are new
spark.createDataFrame([(2, "08:40", "JFK", 9.0), (5, "10:30", "SoHo", 5.5), (6, "11:02", "SoHo", 14.0)], SCHEMA) \
    .createOrReplaceTempView("changes")
spark.sql(f"""
    MERGE INTO delta.`{DELTA}` AS t
    USING changes AS s ON t.trip_id = s.trip_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

# v3: DELETE – the customer of trip 4 asked to be forgotten
spark.sql(f"DELETE FROM delta.`{DELTA}` WHERE trip_id = 4")

# v4: new column
spark.sql(f"ALTER TABLE delta.`{DELTA}` ADD COLUMNS (cbd_fee double)")

spark.read.format("delta").load(DELTA).orderBy("trip_id").display()

# COMMAND ----------

# MAGIC %md
# MAGIC `DESCRIBE HISTORY` lists the commits. Look at `operation` and `operationMetrics` (files added/removed, rows updated):

# COMMAND ----------

history = spark.sql(f"DESCRIBE HISTORY delta.`{DELTA}`").orderBy("version")
history.select("version", "operation", "operationMetrics").display()

# COMMAND ----------

# MAGIC %md
# MAGIC Compare with the slide. Two things Databricks does on its own:
# MAGIC
# MAGIC - The **MERGE** and **DELETE** may use **deletion vectors**: instead of rewriting a data file, Delta adds the same
# MAGIC   file again with a small vector that marks some of its rows as deleted.
# MAGIC - **Auto compaction** can run right after a write and merge small files into one – an extra `OPTIMIZE` commit, so
# MAGIC   the version numbers can shift by one. That is why the cells below look the versions up by operation.

# COMMAND ----------

versions = {}
for row in history.collect():
    versions.setdefault(row.operation, row.version)     # first version of each operation
MERGE_V, DELETE_V = versions["MERGE"], versions["DELETE"]
LATEST = history.agg(F.max("version")).first()[0]
print(versions, "latest:", LATEST)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 – What one commit contains
# MAGIC
# MAGIC The log is a folder of numbered JSON files – one per version. We read them all at once and take the version from
# MAGIC the file name:

# COMMAND ----------

log = (spark.read.json(f"{DELTA}/_delta_log/*.json")
       .withColumn("version", F.regexp_extract(F.col("_metadata.file_name"), r"^(\d+)\.json$", 1).cast("int")))

actions = log.select(
    "version",
    F.when(F.col("commitInfo").isNotNull(), "commitInfo")
     .when(F.col("metaData").isNotNull(), "metaData")
     .when(F.col("protocol").isNotNull(), "protocol")
     .when(F.col("add").isNotNull(), "add")
     .when(F.col("remove").isNotNull(), "remove")
     .otherwise("other").alias("action"),
    F.coalesce(F.col("commitInfo.operation"), F.col("add.path"), F.col("remove.path")).alias("operation_or_file"),
    F.col("add.deletionVector").isNotNull().alias("with_deletion_vector"),
    F.col("add.stats").alias("stats"),
)
print(sorted(r.version for r in actions.select("version").distinct().collect()))

# COMMAND ----------

# MAGIC %md
# MAGIC Each line of a commit file is one **action**: `commitInfo` (who, when, which operation), `metaData` (schema),
# MAGIC `protocol` (which features readers need), `add` (a Parquet file that belongs to the table now, with row count and
# MAGIC min/max statistics) or `remove` (a file that no longer belongs to it). The MERGE commit:

# COMMAND ----------

actions.where(F.col("version") == MERGE_V).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 – Files are never changed, only added and marked as removed
# MAGIC
# MAGIC Which Parquet files make up each version? The hidden `_metadata.file_name` column tells us:

# COMMAND ----------

def files_of(version: int) -> list:
    df = spark.read.format("delta").option("versionAsOf", version).load(DELTA)
    return sorted(r.f for r in df.select(F.col("_metadata.file_name").alias("f")).distinct().collect())

for row in history.collect():
    print(f"v{row.version} {row.operation:12} {files_of(row.version)}")

on_storage = part_files(DELTA)
print(f"\n{len(on_storage)} Parquet files on storage, {len(files_of(LATEST))} of them belong to the latest version")

# COMMAND ----------

# MAGIC %md
# MAGIC The removed files are still there – that is why **time travel** works: the version right after the MERGE is still
# MAGIC readable, with trip 4 and without the new column.

# COMMAND ----------

spark.read.format("delta").option("versionAsOf", MERGE_V).load(DELTA).orderBy("trip_id").display()

# COMMAND ----------

# MAGIC %md
# MAGIC `VACUUM` deletes files that have been removed from the table for longer than the retention period (default 7 days).
# MAGIC After that, time travel to those old versions is no longer possible. `DRY RUN` only lists what would be deleted:

# COMMAND ----------

spark.sql(f"VACUUM delta.`{DELTA}` DRY RUN").display()   # empty today: the removed files are younger than 7 days

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 – Exercises

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4.1 – Which files make up version 1?
# MAGIC
# MAGIC Read version **1** of the Delta table with `versionAsOf` and list the distinct `_metadata.file_name` values.
# MAGIC Compare with the slide: v1 = files of v0 plus one new file.

# COMMAND ----------

# TODO Task 4.1 – files of version 1
raise NotImplementedError("Task 4.1")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'><code>spark.read.format(&quot;delta&quot;).option(&quot;versionAsOf&quot;, 1).load(DELTA)</code>, then select <code>_metadata.file_name</code> and <code>distinct()</code>.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>v1_files = (spark.read.format(&quot;delta&quot;).option(&quot;versionAsOf&quot;, 1).load(DELTA)
# MAGIC             .select(F.col(&quot;_metadata.file_name&quot;).alias(&quot;file&quot;)).distinct())
# MAGIC v1_files.display()</code></pre></details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4.2 – Read the DELETE commit
# MAGIC
# MAGIC Use `actions` and `DELETE_V` to show which files the **DELETE** commit removed and which it added. Why does the
# MAGIC DELETE of trip 4 not need to add a file?

# COMMAND ----------

# TODO Task 4.2 – remove and add actions of the DELETE commit
raise NotImplementedError("Task 4.2")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'><code>actions</code> already has one row per action with its <code>version</code>. Filter on <code>version == DELETE_V</code> and on the action types <code>add</code> and <code>remove</code>.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>actions.where((F.col(&quot;version&quot;) == DELETE_V) &amp; F.col(&quot;action&quot;).isin(&quot;add&quot;, &quot;remove&quot;)).display()
# MAGIC # Trip 4 was alone in the file written by the append, so the whole file is removed and nothing has to be rewritten.
# MAGIC # If the file had held other rows too, Delta would add it again with a deletion vector (or rewrite it).</code></pre></details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4.3 – Undo the DELETE
# MAGIC
# MAGIC Restore the table to the version **right after the MERGE** (`MERGE_V`) with
# MAGIC `RESTORE TABLE delta.`<path>` TO VERSION AS OF …` and check that trip 4 is back. Which version number does the
# MAGIC restore create?

# COMMAND ----------

# TODO Task 4.3 – restore the version after the MERGE
raise NotImplementedError("Task 4.3")
# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'><code>RESTORE TABLE delta.</code>&lt;path&gt;<code> TO VERSION AS OF {MERGE_V}</code> in an f-string – then read the table and <code>DESCRIBE HISTORY</code>.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>spark.sql(f&quot;RESTORE TABLE delta.`{DELTA}` TO VERSION AS OF {MERGE_V}&quot;)
# MAGIC spark.read.format(&quot;delta&quot;).load(DELTA).orderBy(&quot;trip_id&quot;).display()
# MAGIC spark.sql(f&quot;DESCRIBE HISTORY delta.`{DELTA}`&quot;).select(&quot;version&quot;, &quot;operation&quot;).orderBy(&quot;version&quot;).display()
# MAGIC # RESTORE adds a new version on top – the history is never rewritten</code></pre></details>

