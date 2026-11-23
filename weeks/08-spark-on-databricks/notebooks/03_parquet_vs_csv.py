# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 03 – Parquet vs. CSV: how a table becomes a file
# MAGIC
# MAGIC **Slides:** *Apache Parquet* (how a table becomes a file) · *Reading: CSV vs. Parquet*
# MAGIC
# MAGIC **Goal (≈ 25 min):**
# MAGIC
# MAGIC | Section | Idea |
# MAGIC |---|---|
# MAGIC | 1 | The same month as CSV and as Parquet – size on disk |
# MAGIC | 2 | Inside a Parquet file: row groups, column chunks, footer with min/max |
# MAGIC | 3 | `SELECT avg(fare)` on CSV and Parquet – what is read? |
# MAGIC | 4 | min/max only helps if the data is sorted |
# MAGIC | 5 | Exercises |
# MAGIC
# MAGIC Needs the files from `00_setup`.

# COMMAND ----------

import importlib
import time

import pyarrow.parquet as pq
from pyspark.sql import functions as F

import config

importlib.reload(config)   # picks up a change to config.py without restarting Python
catalog = config.CATALOG

RAW = f"/Volumes/{catalog}/nyc_taxi/raw"
FILES = f"/Volumes/{catalog}/playground/files"
JAN = f"{RAW}/yellow_tripdata_2025-01.parquet"


def folder_mb(path: str) -> float:
    return sum(f.size for f in dbutils.fs.ls(path)) / 1e6

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 – The same month as CSV and as Parquet
# MAGIC
# MAGIC CSV stores every value as **text**, row by row. Parquet stores every column separately, typed and compressed.

# COMMAND ----------

jan = spark.read.parquet(JAN)
CSV_DIR = f"{FILES}/trips_jan_csv"
jan.write.mode("overwrite").option("header", "true").csv(CSV_DIR)

parquet_mb = dbutils.fs.ls(JAN)[0].size / 1e6
csv_mb = folder_mb(CSV_DIR)
print(f"Parquet : {parquet_mb:7.0f} MB")
print(f"CSV     : {csv_mb:7.0f} MB   ({csv_mb / parquet_mb:.1f}× bigger)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 – Inside a Parquet file
# MAGIC
# MAGIC A Parquet file is cut into **row groups**. Inside each row group the values of one column lie together in a
# MAGIC **column chunk**. The **footer** at the end of the file holds the schema and, per row group and column, the
# MAGIC min/max statistics. `pyarrow` reads only the footer – not the data. (The TLC files were written by pyarrow with
# MAGIC 1 Mi rows per row group; Spark's writer cuts row groups at 128 MB instead.)

# COMMAND ----------

meta = pq.ParquetFile(JAN).metadata
print(f"rows: {meta.num_rows:,}   row groups: {meta.num_row_groups}   columns: {meta.num_columns}")
print(meta.schema)

# COMMAND ----------

fare_idx = meta.schema.names.index("fare_amount")
rows = []
for rg in range(meta.num_row_groups):
    col = meta.row_group(rg).column(fare_idx)
    rows.append((rg, meta.row_group(rg).num_rows, col.compression, round(col.total_compressed_size / 1e6, 2),
                 float(col.statistics.min), float(col.statistics.max)))
display(spark.createDataFrame(rows, "row_group int, rows long, compression string, fare_chunk_mb double, fare_min double, fare_max double"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 – `SELECT avg(fare_amount)`: what is read?
# MAGIC
# MAGIC - **CSV:** `fare_amount` is the 11th field of every line. The reader has to parse every line completely to find it.
# MAGIC - **Parquet:** the reader reads the footer, then jumps straight to the `fare_amount` chunks and skips the other columns.
# MAGIC
# MAGIC Compare the `ReadSchema` in both plans, then the time.

# COMMAND ----------

csv_df = spark.read.option("header", "true").csv(CSV_DIR)          # all columns as strings
parquet_df = spark.read.parquet(JAN)

q_csv = csv_df.agg(F.avg(F.col("fare_amount").cast("double")).alias("avg_fare"))
q_parquet = parquet_df.agg(F.avg("fare_amount").alias("avg_fare"))

q_csv.explain()
q_parquet.explain()

# COMMAND ----------

for label, query in (("CSV", q_csv), ("Parquet", q_parquet)):
    start = time.time()
    value = query.first()["avg_fare"]
    print(f"{label:8} avg fare {value:6.2f}   {time.time() - start:5.1f} s")

# COMMAND ----------

# MAGIC %md
# MAGIC With 20 columns, one column is about 5 % of the data. That is **column pruning**. The time difference on one
# MAGIC month is small on a fast cluster; on terabytes it decides whether a query costs seconds or hours.
# MAGIC
# MAGIC ## 4 – min/max only helps if the data is sorted
# MAGIC
# MAGIC With the footer statistics a reader can skip whole row groups: `WHERE fare_amount > 50` does not need a row group
# MAGIC whose maximum fare is 40. Look at the table in section 2 – the raw file was written in pickup order, so almost
# MAGIC every row group contains some expensive trip, and nothing can be skipped.
# MAGIC
# MAGIC Write the same month **sorted by fare**, with smaller row groups, and look again:

# COMMAND ----------

SORTED = f"{FILES}/trips_jan_sorted_by_fare.parquet"
table = pq.read_table(JAN, columns=["tpep_pickup_datetime", "PULocationID", "fare_amount", "total_amount"])
pq.write_table(table.sort_by("fare_amount"), SORTED, row_group_size=250_000)

sorted_meta = pq.ParquetFile(SORTED).metadata
idx = sorted_meta.schema.names.index("fare_amount")
sorted_rows = [(rg, float(sorted_meta.row_group(rg).column(idx).statistics.min),
                float(sorted_meta.row_group(rg).column(idx).statistics.max))
               for rg in range(sorted_meta.num_row_groups)]
display(spark.createDataFrame(sorted_rows, "row_group int, fare_min double, fare_max double"))

# COMMAND ----------

def skippable(stats, threshold):
    """Row groups whose maximum is not above the threshold – a reader never has to open them."""
    return sum(1 for _, _, fare_max in stats if fare_max <= threshold)

print(f"raw file    : {skippable([(r[0], r[4], r[5]) for r in rows], 50)} of {len(rows)} row groups skipped for fare > 50")
print(f"sorted file : {skippable(sorted_rows, 50)} of {len(sorted_rows)} row groups skipped for fare > 50")

# COMMAND ----------

# MAGIC %md
# MAGIC This is why Delta tables are **clustered** (`CLUSTER BY`) – notebook 06 does it for `pickup_date`.
# MAGIC
# MAGIC ## 5 – Exercises

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.1 – Read a footer yourself
# MAGIC
# MAGIC Open the footer of **February** (`yellow_tripdata_2025-02.parquet`) with `pq.ParquetFile(...).metadata` and print,
# MAGIC for every row group, the number of rows and the min/max of `trip_distance`.

# COMMAND ----------

# TODO Task 3.1 – row groups of February with min/max of trip_distance
raise NotImplementedError("Task 3.1")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'><code>pq.ParquetFile(path).metadata</code> reads only the footer. <code>meta.row_group(i).column(j).statistics</code> has <code>min</code> and <code>max</code>; find <code>j</code> with <code>meta.schema.names.index(&quot;trip_distance&quot;)</code>.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>meta = pq.ParquetFile(f&quot;{RAW}/yellow_tripdata_2025-02.parquet&quot;).metadata
# MAGIC j = meta.schema.names.index(&quot;___&quot;)
# MAGIC for rg in range(meta.___):
# MAGIC     s = meta.row_group(rg).column(j).statistics
# MAGIC     print(rg, s.min, s.max)</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>feb_meta = pq.ParquetFile(f&quot;{RAW}/yellow_tripdata_2025-02.parquet&quot;).metadata
# MAGIC dist_idx = feb_meta.schema.names.index(&quot;trip_distance&quot;)
# MAGIC for rg in range(feb_meta.num_row_groups):
# MAGIC     stats = feb_meta.row_group(rg).column(dist_idx).statistics
# MAGIC     print(f&quot;row group {rg}: {feb_meta.row_group(rg).num_rows:&gt;9,} rows, trip_distance {stats.min} … {stats.max}&quot;)</code></pre></details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.2 – Which row groups can be skipped?
# MAGIC
# MAGIC Use `sorted_rows` (row group, min, max of `fare_amount` in the sorted file). How many row groups does a reader have
# MAGIC to open for `WHERE fare_amount BETWEEN 20 AND 30`? Count the row groups whose range **overlaps** 20–30.

# COMMAND ----------

# TODO Task 3.2 – row groups overlapping the range 20–30
raise NotImplementedError("Task 3.2")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>A row group must be read when its range [min, max] overlaps [20, 30]: max ≥ 20 <b>and</b> min ≤ 30.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>needed = [rg for rg, fare_min, fare_max in sorted_rows if fare_max &gt;= 20 and fare_min &lt;= 30]
# MAGIC print(f&quot;{len(needed)} of {len(sorted_rows)} row groups must be read: {needed}&quot;)</code></pre></details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.3 – Column pruning in SQL
# MAGIC
# MAGIC Write a SQL query on the Parquet file that returns the number of trips and the average tip per `payment_type`.
# MAGIC Use ``parquet.`<path>` `` as the table name. Look at `ReadSchema` in `EXPLAIN`: how many columns are read?

# COMMAND ----------

# TODO Task 3.3 – aggregate on the Parquet file and check ReadSchema
raise NotImplementedError("Task 3.3")
# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>Spark SQL can query a file directly: `<code>FROM parquet.</code>/Volumes/…/file.parquet<code> </code><code>. Then </code>spark.sql(query).explain()<code> and look for </code>ReadSchema`.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>query = f&quot;&quot;&quot;
# MAGIC     SELECT payment_type, COUNT(*) AS trips, ROUND(AVG(tip_amount), 2) AS avg_tip
# MAGIC     FROM parquet.`{JAN}`
# MAGIC     GROUP BY payment_type
# MAGIC     ORDER BY trips DESC
# MAGIC &quot;&quot;&quot;
# MAGIC spark.sql(query).display()
# MAGIC spark.sql(query).explain()   # ReadSchema: payment_type, tip_amount – 2 of 20 columns</code></pre></details>

