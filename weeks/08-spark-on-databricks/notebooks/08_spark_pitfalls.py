# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 08 – Spark pitfalls 1–6
# MAGIC
# MAGIC **Slides (appendix):** *Spark pitfalls: overview* · *Pitfall 1 · Lazy evaluation* · *Pitfall 2 · toPandas()* ·
# MAGIC *Pitfall 3 · Shuffle* · *Pitfall 4 · Big joins* · *Pitfall 5 · Data skew* · *Pitfall 6 · cache()*
# MAGIC
# MAGIC **Goal (≈ 30 min):** see each pitfall in a plan or a measurement, and the fix.
# MAGIC
# MAGIC Everything runs on `samples.nyctaxi.trips`, which exists in every workspace – no setup needed. Attach the notebook
# MAGIC to **Serverless** and run it cell by cell.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Before you start: why the pitfalls are hard to see here
# MAGIC
# MAGIC **Databricks catches many of these pitfalls for you.** Adaptive Query Execution (AQE) is on by default and
# MAGIC re-plans a query while it runs, using the real sizes of the intermediate results:
# MAGIC
# MAGIC | Pitfall | What Databricks does on its own |
# MAGIC |---|---|
# MAGIC | 3 · Shuffle | AQE merges many small shuffle partitions into fewer, larger ones |
# MAGIC | 4 · Big joins | AQE switches a sort-merge join to a broadcast join when one side turns out to be small |
# MAGIC | 5 · Data skew | AQE splits an oversized partition of a join into several tasks |
# MAGIC | 2, 6 · Memory | Serverless sizes and scales the compute; Photon and the disk cache speed up repeated reads |
# MAGIC
# MAGIC **And the data in this notebook is small.** `samples.nyctaxi.trips` has about 22,000 rows – it fits in the memory
# MAGIC of one machine many times over. On data this size, every variant finishes in about a second, and the time you
# MAGIC measure is mostly overhead: starting the job, sending the plan, returning the result. A "slow" and a "fast"
# MAGIC version look the same on the stopwatch.
# MAGIC
# MAGIC **So this notebook looks at plans, not at runtimes.** `explain()` shows what Spark *would* do – an
# MAGIC `Exchange` (shuffle), a `BroadcastHashJoin` – no matter how much data there is. That plan is the same on
# MAGIC 22,000 rows and on 2 billion; only on 2 billion does it decide whether a job takes minutes or hours.
# MAGIC
# MAGIC **Why learn the pitfalls anyway?** The automatic fixes have limits. AQE cannot help when you pull a large result
# MAGIC to the driver with `collect()` or `toPandas()`, when a single key holds half of all rows in a `groupBy`, or when
# MAGIC you run the same expensive query ten times without caching it. And outside Databricks – open-source Spark,
# MAGIC older runtimes, other platforms – these defaults may be switched off or missing. Knowing the pitfalls means you
# MAGIC recognise them in a plan before they show up in the bill.

# COMMAND ----------

from pyspark.sql import functions as F

TRIPS = "samples.nyctaxi.trips"
df = spark.table(TRIPS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pitfall 1 – Lazy evaluation: the work happens at the action
# MAGIC
# MAGIC `filter`, `select`, `withColumn`, `groupBy` are **transformations**: they only extend the plan.
# MAGIC `count`, `show`, `display`, `collect`, `write` are **actions**: they run a job. The next cell returns immediately,
# MAGIC no matter how big the table is.

# COMMAND ----------

long_trips = (
    df
    .filter(F.col("trip_distance") > 10)
    .withColumn("duration_min",
                (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60)
    .select("tpep_pickup_datetime", "trip_distance", "duration_min", "fare_amount")
)
long_trips   # just a plan – no job ran

# COMMAND ----------

# MAGIC %md
# MAGIC `explain()` shows the plan Spark *would* run. Read it bottom-up: the scan, the filter pushed into the scan
# MAGIC (`RequiredDataFilters` with Photon, `PushedFilters` in open-source Spark), then the projection. Only the action
# MAGIC triggers the work.

# COMMAND ----------

long_trips.explain(mode="formatted")
print(f"Trips longer than 10 miles: {long_trips.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pitfall 2 – `collect()` / `toPandas()`: all rows to one machine
# MAGIC
# MAGIC Both actions copy the **whole** result to the driver. On a large DataFrame this ends with an out-of-memory error.
# MAGIC Reduce first, then collect.

# COMMAND ----------

# Do NOT do this on a large table:
# pdf = df.toPandas()

# Aggregate first, then collect the small result
pdf_hourly = (
    df
    .groupBy(F.hour("tpep_pickup_datetime").alias("hour"))
    .agg(F.count("*").alias("trips"))
    .orderBy("hour")
    .toPandas()
)
pdf_hourly.head(24)

# COMMAND ----------

# MAGIC %md
# MAGIC To look at a few rows, use `limit()` / `display()` – only those rows reach the driver. To keep a big result, write it
# MAGIC as a table: it stays distributed.

# COMMAND ----------

df.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pitfall 3 – Shuffle: rows travel across the cluster
# MAGIC
# MAGIC - **Narrow** (`filter`, `select`, `withColumn`): each output partition needs exactly one input partition.
# MAGIC - **Wide** (`groupBy`, `join`, `orderBy`, `distinct`): rows with the same key must end up in the same partition, so
# MAGIC   Spark **shuffles** them – to disk and over the network. A shuffle is a stage boundary and usually the most expensive
# MAGIC   part of a job.
# MAGIC
# MAGIC Look for `Exchange` in the plan – that is the shuffle.

# COMMAND ----------

per_zip = (
    df
    .groupBy("pickup_zip")
    .agg(F.count("*").alias("trips"), F.round(F.avg("fare_amount"), 2).alias("avg_fare"))
    .orderBy(F.desc("trips"))
)
per_zip.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC Note the `HashAggregate` **before and after** the `Exchange`: Spark first aggregates inside every partition (partial
# MAGIC aggregation) and only shuffles the small intermediate result. Filter rows and select columns **before** the shuffle.

# COMMAND ----------

print("shuffle partitions:", spark.conf.get("spark.sql.shuffle.partitions"))   # 'auto' on serverless: AQE decides

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pitfall 4 – Big joins: copy the small side
# MAGIC
# MAGIC A join of two big tables shuffles **both** sides (sort-merge join). If one side is small – a lookup table – Spark sends
# MAGIC a copy of it to every executor and joins locally: a **broadcast hash join**, no shuffle of the big side.

# COMMAND ----------

zips = (
    df.select(F.col("pickup_zip").alias("zip")).distinct()
    .withColumn("zip_label", F.concat(F.lit("ZIP "), F.col("zip").cast("string")))
)

joined = df.join(F.broadcast(zips), df.pickup_zip == zips.zip, "left")
joined.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC Look for `BroadcastHashJoin` and `BroadcastExchange`. The same hint in SQL: `SELECT /*+ BROADCAST(z) */ …`.
# MAGIC Spark broadcasts small tables automatically below `spark.sql.autoBroadcastJoinThreshold`; the hint is for cases where
# MAGIC its size estimate is wrong.
# MAGIC
# MAGIC ## Pitfall 5 – Data skew: one key, one slow task
# MAGIC
# MAGIC A few pickup zip codes account for a large share of all trips. In a `groupBy("pickup_zip")` the task that receives the
# MAGIC busiest zip code works much longer than the others – a stage is only as fast as its slowest task.

# COMMAND ----------

total = df.count()
(per_zip
 .withColumn("share_pct", F.round(F.col("trips") / F.lit(total) * 100, 2))
 .limit(10)
 .display())

# COMMAND ----------

# MAGIC %md
# MAGIC **Adaptive Query Execution (AQE)** re-optimises a query while it runs: it coalesces tiny shuffle partitions, switches
# MAGIC to a broadcast join when one side turns out small, and splits skewed join partitions. It is on by default. On
# MAGIC serverless the setting is managed by Databricks – reading it fails with `CONFIG_NOT_AVAILABLE`:

# COMMAND ----------

try:
    print("AQE enabled:", spark.conf.get("spark.sql.adaptive.enabled"))
except Exception as e:  # some configs are not readable on serverless
    print("Could not read AQE setting:", type(e).__name__, e)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pitfall 6 – `cache()`: only for results you reuse
# MAGIC
# MAGIC Without cache, every action recomputes the DataFrame from the source. `df.cache()` keeps it in executor memory after the
# MAGIC first action. It is not free: it costs memory, and the first action pays for filling the cache.
# MAGIC
# MAGIC On serverless, `cache()` needs **environment version 5 or higher** – pinned in the header of this notebook. On an older
# MAGIC version the cell prints `CACHE_REQUIRES_NEWER_CLIENT_ON_SERVERLESS_GC`.

# COMMAND ----------

try:
    cached = long_trips.cache()
    print("rows:", cached.count())
    print("rows again (from cache):", cached.count())
    cached.unpersist()
except Exception as e:
    print("Caching not available on this compute:", type(e).__name__)
    print(str(e)[:300])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Where is the Spark UI?
# MAGIC
# MAGIC On serverless compute there is no classic Spark UI. Click **See performance** below a cell that ran a job, then open the
# MAGIC **query profile**: it shows the stages, the rows read, the time spent per operator – and the shuffles (`Exchange`).
# MAGIC
# MAGIC ## Exercises

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 8.1 – Pitfall 2: bring back only what you need
# MAGIC
# MAGIC A colleague wrote `pdf = df.toPandas(); pdf.groupby("pickup_zip")["fare_amount"].mean()`. Rewrite it so that Spark does
# MAGIC the aggregation and only the result reaches pandas. How many rows reach the driver?

# COMMAND ----------

# TODO Task 8.1 – aggregate in Spark, then toPandas
raise NotImplementedError("Task 8.1")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>Move the <code>groupBy</code> + <code>avg</code> into Spark and call <code>toPandas()</code> at the very end.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>pdf_fare = (df.groupBy(&quot;pickup_zip&quot;)
# MAGIC               .agg(F.avg(&quot;fare_amount&quot;).alias(&quot;avg_fare&quot;))
# MAGIC               .toPandas())
# MAGIC print(f&quot;{len(pdf_fare)} rows reach the driver instead of {total:,}&quot;)</code></pre></details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 8.2 – Pitfall 4: broadcast a lookup
# MAGIC
# MAGIC Build a small lookup `hours` with the 24 hours and a label (`"night"` for 0–5, `"day"` otherwise). Join it to `df`
# MAGIC on the pickup hour **with a broadcast hint** and check that the plan shows `BroadcastHashJoin`.

# COMMAND ----------

# TODO Task 8.2 – broadcast join with an hour lookup
raise NotImplementedError("Task 8.2")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'><code>spark.createDataFrame([(h, ...) for h in range(24)], &quot;hour int, part_of_day string&quot;)</code>, add an <code>hour</code> column to <code>df</code> with <code>F.hour(...)</code>, then <code>df.join(F.broadcast(hours), &quot;hour&quot;)</code>.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>hours = spark.createDataFrame([(h, &quot;night&quot; if h &lt; 6 else &quot;day&quot;) for h in range(24)], &quot;hour int, part_of_day string&quot;)
# MAGIC with_hour = df.withColumn(&quot;hour&quot;, F.hour(&quot;tpep_pickup_datetime&quot;))
# MAGIC by_part = with_hour.join(F.broadcast(hours), &quot;hour&quot;).groupBy(&quot;part_of_day&quot;).count()
# MAGIC by_part.explain()
# MAGIC by_part.display()</code></pre></details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 8.3 – Pitfall 5: find the hot keys
# MAGIC
# MAGIC Which **dropoff** zip codes hold the most trips? Show the top 3 with their share of all trips in percent. Would a
# MAGIC `groupBy("dropoff_zip")` be skewed?

# COMMAND ----------

# TODO Task 8.3 – top 3 dropoff zips and their share
raise NotImplementedError("Task 8.3")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'><code>groupBy(&quot;dropoff_zip&quot;).count()</code>, divide by <code>total</code>, order descending, <code>limit(3)</code>.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>(df.groupBy(&quot;dropoff_zip&quot;).count()
# MAGIC    .withColumn(&quot;share_pct&quot;, F.round(F.col(&quot;count&quot;) / F.lit(total) * 100, 2))
# MAGIC    .orderBy(F.desc(&quot;count&quot;))
# MAGIC    .limit(3)
# MAGIC    .display())</code></pre></details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 8.4 – Pitfall 6: cache only what you reuse
# MAGIC
# MAGIC `airport` (trips from zip 11430, JFK) is used by **two** actions below. Cache it before the first action and release it
# MAGIC after the second.

# COMMAND ----------

# TODO Task 8.4 – cache a reused DataFrame
raise NotImplementedError("Task 8.4")
# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>Call <code>.cache()</code> on the filtered DataFrame, run both actions, then <code>.unpersist()</code>.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>airport = df.filter(F.col(&quot;pickup_zip&quot;) == 11430).cache()
# MAGIC print(&quot;trips from JFK      :&quot;, airport.count())
# MAGIC print(&quot;avg fare from JFK   :&quot;, round(airport.agg(F.avg(&quot;fare_amount&quot;)).first()[0], 2))
# MAGIC airport.unpersist()</code></pre></details>

