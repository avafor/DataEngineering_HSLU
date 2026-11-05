# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 01 – Why Spark? The problem with one machine
# MAGIC
# MAGIC **Slides:** *From your laptop PoC to production* · *The problem with one machine: memory / time / reading
# MAGIC and writing* · *The way out: split, distribute, combine*
# MAGIC
# MAGIC **Goal (≈ 20 min):** measure the limits of one machine on real data, then let Spark do the same work split
# MAGIC into partitions.
# MAGIC
# MAGIC | Section | Question |
# MAGIC |---|---|
# MAGIC | 1 | How much RAM does pandas need for one month – and for the whole year? |
# MAGIC | 2 | How does Spark answer the same question on all twelve months? |
# MAGIC | 3 | Why does Spark write a folder of part files instead of one file? |
# MAGIC | 4 | Exercises |
# MAGIC
# MAGIC Needs the files from `00_setup`.

# COMMAND ----------

import importlib
import time

from pyspark.sql import functions as F

import config

importlib.reload(config)   # picks up a change to config.py without restarting Python
catalog = config.CATALOG

RAW = f"/Volumes/{catalog}/nyc_taxi/raw"
FILES = f"/Volumes/{catalog}/playground/files"
YEAR_ROWS = 48_722_602   # rows of all twelve monthly files 2025, counted once with Spark

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 – The PoC: one month with pandas
# MAGIC
# MAGIC The Parquet file of January is 59 MB on disk. pandas decompresses it and keeps every value as a Python
# MAGIC object or a NumPy array – how big is it in memory?

# COMMAND ----------

import pandas as pd

pdf = pd.read_parquet(f"{RAW}/yellow_tripdata_2025-01.parquet")
mem_bytes = pdf.memory_usage(deep=True).sum()
bytes_per_row = mem_bytes / len(pdf)

print(f"rows in January          : {len(pdf):>12,}")
print(f"pandas memory            : {mem_bytes / 1e9:>12.2f} GB")
print(f"bytes per row            : {bytes_per_row:>12.0f}")
print(f"whole year, extrapolated : {YEAR_ROWS * bytes_per_row / 1e9:>12.1f} GB  (for {YEAR_ROWS:,} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC 150–190 bytes per row, depending on the pandas version: the whole year needs 7–9 GB of RAM **before** the first
# MAGIC `groupBy` copies anything. That is more than many laptops have free – and production data is years, not one year.
# MAGIC
# MAGIC The PoC question: *average fare per pickup zone*. On one month pandas answers it quickly.

# COMMAND ----------

start = time.time()
pdf_answer = (pdf[pdf["trip_distance"] > 0]
              .groupby("PULocationID")["fare_amount"].mean()
              .sort_values(ascending=False))
print(f"pandas, one month: {time.time() - start:.1f} s")
pdf_answer.head(5)

# COMMAND ----------

del pdf   # free the driver's memory before Spark starts

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 – The same question on the whole year with Spark
# MAGIC
# MAGIC The code looks like pandas. The difference is underneath: Spark splits the twelve files into partitions,
# MAGIC every partition is read and aggregated by its own task, and only the small partial results are combined.

# COMMAND ----------

trips = spark.read.parquet(f"{RAW}/yellow_tripdata_2025-*.parquet")

start = time.time()
answer = (trips
          .filter(F.col("trip_distance") > 0)
          .groupBy("PULocationID")
          .agg(F.avg("fare_amount").alias("avg_fare"), F.count("*").alias("trips"))
          .orderBy(F.desc("avg_fare")))
rows = answer.collect()          # the action: only now does Spark read the files
print(f"Spark, twelve months: {time.time() - start:.1f} s for {sum(r.trips for r in rows):,} trips")
display(answer.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC **Split → distribute → combine.** How many pieces did Spark cut the year into? Every partition becomes one
# MAGIC task; the tasks run in parallel on the available cores.

# COMMAND ----------

(trips
 .groupBy(F.spark_partition_id().alias("partition"))
 .count()
 .agg(F.count("*").alias("partitions"),
      F.min("count").alias("min_rows"),
      F.max("count").alias("max_rows"))
 .display())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 – Reading and writing in parallel
# MAGIC
# MAGIC One process writes one file, one stream at a time. Spark lets every task write **its own part file**, so the
# MAGIC throughput of all tasks adds up. The result is a **folder**, not a file. We write the first quarter:

# COMMAND ----------

target = f"{FILES}/trips_q1_parquet"
start = time.time()
(spark.read.parquet(*[f"{RAW}/yellow_tripdata_2025-{m:02d}.parquet" for m in (1, 2, 3)])
 .write.mode("overwrite").parquet(target))
print(f"written in {time.time() - start:.1f} s")

parts = [f for f in dbutils.fs.ls(target) if f.name.startswith("part-")]
print(f"{len(parts)} part files, {sum(f.size for f in parts) / 1e6:,.0f} MB in {target}")
display(spark.createDataFrame([(f.name, round(f.size / 1e6, 1)) for f in parts[:10]], "file string, size_mb double"))

# COMMAND ----------

# MAGIC %md
# MAGIC Reading the folder back is the same call as reading one file – Spark lists the folder and reads the part files
# MAGIC in parallel. The `_SUCCESS` / `_committed_*` files are written last: a reader can see whether the write finished.

# COMMAND ----------

print(f"{spark.read.parquet(target).count():,} rows in the folder")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 – Exercises

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1.1 – How much RAM would pandas need for your month?
# MAGIC
# MAGIC Load **July** (`yellow_tripdata_2025-07.parquet`) with pandas, compute the bytes per row and extrapolate to the
# MAGIC whole year (`YEAR_ROWS`). Print both numbers. Does the estimate change compared to January?

# COMMAND ----------

# TODO Task 1.1 – bytes per row of July and the extrapolated year
raise NotImplementedError("Task 1.1")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>Same as section 1, only the file changes. <code>memory_usage(deep=True).sum()</code> counts the bytes of every column including the strings; divide by the number of rows and multiply by <code>YEAR_ROWS</code>.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>pdf_jul = pd.read_parquet(f&quot;{RAW}/yellow_tripdata_2025-___.parquet&quot;)
# MAGIC per_row = pdf_jul.memory_usage(deep=___).sum() / len(pdf_jul)
# MAGIC print(per_row, YEAR_ROWS * per_row / 1e9)</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>pdf_jul = pd.read_parquet(f&quot;{RAW}/yellow_tripdata_2025-07.parquet&quot;)
# MAGIC jul_bytes_per_row = pdf_jul.memory_usage(deep=True).sum() / len(pdf_jul)
# MAGIC print(f&quot;bytes per row (July)     : {jul_bytes_per_row:.0f}&quot;)
# MAGIC print(f&quot;whole year, extrapolated : {YEAR_ROWS * jul_bytes_per_row / 1e9:.1f} GB&quot;)
# MAGIC del pdf_jul</code></pre></details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1.2 – Part files
# MAGIC
# MAGIC Every partition is written by one task into one part file. Write **January** twice and count the part files:
# MAGIC
# MAGIC 1. split into 8 partitions with `.repartition(8)` → `f"{FILES}/trips_jan_8_parts"`
# MAGIC 2. merged into one partition with `.coalesce(1)` → `f"{FILES}/trips_jan_one_file"`
# MAGIC
# MAGIC What do you gain with 8 parts, and what do you give up with one file?

# COMMAND ----------

# TODO Task 1.2 – write January with 8 partitions and with 1
raise NotImplementedError("Task 1.2")
# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'><code>repartition(n)</code> / <code>coalesce(n)</code> change the number of partitions before <code>.write</code>. Count the entries of <code>dbutils.fs.ls(path)</code> whose name starts with <code>part-</code>.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>jan = spark.read.parquet(f&quot;{RAW}/yellow_tripdata_2025-01.parquet&quot;)
# MAGIC jan.___(8).write.mode(&quot;overwrite&quot;).parquet(f&quot;{FILES}/trips_jan_8_parts&quot;)
# MAGIC jan.___(1).write.mode(&quot;overwrite&quot;).parquet(f&quot;{FILES}/trips_jan_one_file&quot;)</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>jan = spark.read.parquet(f&quot;{RAW}/yellow_tripdata_2025-01.parquet&quot;)
# MAGIC 
# MAGIC jan.repartition(8).write.mode(&quot;overwrite&quot;).parquet(f&quot;{FILES}/trips_jan_8_parts&quot;)
# MAGIC jan.coalesce(1).write.mode(&quot;overwrite&quot;).parquet(f&quot;{FILES}/trips_jan_one_file&quot;)
# MAGIC 
# MAGIC for path in (f&quot;{FILES}/trips_jan_8_parts&quot;, f&quot;{FILES}/trips_jan_one_file&quot;):
# MAGIC     n = len([f for f in dbutils.fs.ls(path) if f.name.startswith(&quot;part-&quot;)])
# MAGIC     print(f&quot;{n:3} part files in {path}&quot;)
# MAGIC # 8 parts: written by 8 tasks in parallel, read back in parallel too.
# MAGIC # 1 file: written by one single task – no parallelism left (repartition costs a shuffle, coalesce does not).</code></pre></details>

