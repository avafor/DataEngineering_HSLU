# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 02 – Spark basics: processing model, lazy evaluation, DataFrames
# MAGIC
# MAGIC **Slides:** *Spark: processing model* · *Autoscaling* · *Spark: APIs & engines* · *Lazy evaluation: the plan
# MAGIC grows, the work waits* · *DataFrame = one table, many partitions* · *Pandas vs. Spark DataFrame*
# MAGIC
# MAGIC **Goal (≈ 25 min):**
# MAGIC
# MAGIC | Section | Idea |
# MAGIC |---|---|
# MAGIC | 1 | Driver, executors, cluster manager – and what you see of them on serverless |
# MAGIC | 2 | One engine, several APIs: Python and SQL produce the same plan |
# MAGIC | 3 | Lazy evaluation: transformations cost 0 s, the action does the work |
# MAGIC | 4 | A DataFrame is one table split into partitions |
# MAGIC | 5 | pandas vs. Spark DataFrame |
# MAGIC | 6 | Exercises |
# MAGIC
# MAGIC Needs the files from `00_setup`. Reads only – nothing is written.

# COMMAND ----------

import importlib
import time

from pyspark.sql import functions as F

import config

importlib.reload(config)   # picks up a change to config.py without restarting Python
catalog = config.CATALOG

RAW = f"/Volumes/{catalog}/nyc_taxi/raw"
trips = spark.read.parquet(f"{RAW}/yellow_tripdata_2025-*.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 – The processing model
# MAGIC
# MAGIC - The **driver** runs your notebook code, builds the plan and splits it into **tasks** – one per partition.
# MAGIC - The **executors** (worker VMs or containers) run the tasks in parallel and hold the data.
# MAGIC - The **cluster manager** is a shared service that starts executors and gives them to the driver. On Databricks
# MAGIC   it lives in the control plane.
# MAGIC
# MAGIC On **serverless** compute Databricks manages all of this: you see neither the VMs nor a classic Spark UI, and the
# MAGIC number of executors grows and shrinks with the load (*autoscaling*). What you can still see is how the work was
# MAGIC split – the partitions – and the plan. Below a cell that ran a job, **See performance → query profile** shows the
# MAGIC stages and tasks.
# MAGIC
# MAGIC The `SparkSession` is already there as `spark`:

# COMMAND ----------

print("Spark version :", spark.version)
print("Session type  :", type(spark).__module__)   # pyspark.sql.connect… = Spark Connect, the serverless client

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 – One engine, several APIs
# MAGIC
# MAGIC The DataFrame API and SQL are two ways to write the same plan. The Catalyst optimiser turns both into the same
# MAGIC physical plan – there is no "faster language" here.

# COMMAND ----------

trips.createOrReplaceTempView("trips")

by_python = (trips.filter(F.col("passenger_count") >= 3)
             .groupBy("payment_type").agg(F.count("*").alias("trips")))
by_sql = spark.sql("SELECT payment_type, COUNT(*) AS trips FROM trips WHERE passenger_count >= 3 GROUP BY payment_type")

by_python.explain()
by_sql.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 – Lazy evaluation: the plan grows, the work waits
# MAGIC
# MAGIC `filter`, `select`, `withColumn`, `groupBy` are **transformations**: they only add a step to the plan.
# MAGIC `count`, `show`, `display`, `collect`, `toPandas`, `write` are **actions**: they run a job.
# MAGIC
# MAGIC Time every step:

# COMMAND ----------

def timed(label, fn):
    start = time.time()
    result = fn()
    print(f"{label:38} {time.time() - start:6.2f} s")
    return result

step1 = timed("read (transformation)",      lambda: spark.read.parquet(f"{RAW}/yellow_tripdata_2025-*.parquet"))
step2 = timed("filter (transformation)",    lambda: step1.filter(F.col("PULocationID") == 132))
step3 = timed("groupBy+sum (transformation)", lambda: step2.groupBy("payment_type").agg(F.sum("total_amount").alias("revenue")))
_     = timed("show() (action)",            lambda: step3.show())

# COMMAND ----------

# MAGIC %md
# MAGIC The time appears **at the action**, not on the line that is slow. Waiting pays off: Spark sees the whole plan
# MAGIC and pushes the filter into the scan. In the plan below, look at the scan node (`PhotonScan parquet` – Databricks'
# MAGIC engine Photon runs the plan): the filter appears there as `RequiredDataFilters` / `DictionaryFilters` (open-source
# MAGIC Spark calls it `PushedFilters`), and `ReadSchema` lists only the three columns the query needs.

# COMMAND ----------

step3.explain(mode="formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC Every action runs the plan **again from the start** – two actions, two jobs:

# COMMAND ----------

timed("count() (action 1)", lambda: step2.count())
timed("count() (action 2, same plan)", lambda: step2.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 – A DataFrame: one table, many partitions
# MAGIC
# MAGIC Spark cut the twelve files into partitions; each is processed by one task on one core. On serverless the RDD API
# MAGIC (`df.rdd.getNumPartitions()`) is not available, so we ask Spark which partition every row lives in:

# COMMAND ----------

(trips
 .groupBy(F.spark_partition_id().alias("partition"))
 .count()
 .orderBy("partition")
 .display())

# COMMAND ----------

# MAGIC %md
# MAGIC Every DataFrame has a **schema**, known before anything runs. Parquet carries it inside the file:

# COMMAND ----------

trips.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 – pandas vs. Spark DataFrame
# MAGIC
# MAGIC | | pandas | Spark |
# MAGIC |---|---|---|
# MAGIC | Where | one process on one machine | partitions on many machines |
# MAGIC | When | every line runs immediately | lazy: plan first, run at the action |
# MAGIC | Size | must fit into RAM | spills to disk, or add machines |
# MAGIC | Schema | types inferred, can drift | fixed schema, known before running |
# MAGIC | Speed-up | none, runs as written | the optimiser rewrites the plan |
# MAGIC
# MAGIC They work well together: aggregate in Spark, then bring the **small** result to pandas for a plot or a quick check.

# COMMAND ----------

monthly = (trips
           .groupBy(F.date_trunc("month", "tpep_pickup_datetime").alias("month"))
           .agg(F.count("*").alias("trips"))
           .filter(F.year("month") == 2025)
           .orderBy("month")
           .toPandas())          # 12 rows reach the driver, not 49 million
monthly.plot(x="month", y="trips", kind="bar", legend=False, title="Trips per month 2025")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 – Exercises

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.1 – Transformation or action?
# MAGIC
# MAGIC Fill the dictionary with `"T"` for transformation or `"A"` for action. The check cell below tells you if you are right.

# COMMAND ----------

# TODO Task 2.1 – classify each call
answers = {
    "filter": "_", "count": "_", "withColumn": "_", "write": "_",
    "groupBy": "_", "toPandas": "_", "join": "_", "show": "_",
}

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>A transformation returns a new DataFrame and runs nothing. An action returns a result to Python or writes data – that needs a job.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>answers = {
# MAGIC     &quot;filter&quot;: &quot;T&quot;, &quot;count&quot;: &quot;A&quot;, &quot;withColumn&quot;: &quot;T&quot;, &quot;write&quot;: &quot;A&quot;,
# MAGIC     &quot;groupBy&quot;: &quot;T&quot;, &quot;toPandas&quot;: &quot;A&quot;, &quot;join&quot;: &quot;T&quot;, &quot;show&quot;: &quot;A&quot;,
# MAGIC }</code></pre></details>

# COMMAND ----------

expected = {"filter": "T", "count": "A", "withColumn": "T", "write": "A",
            "groupBy": "T", "toPandas": "A", "join": "T", "show": "A"}
wrong = sorted(k for k in expected if answers.get(k) != expected[k])
print("All correct!" if not wrong else f"Not yet right: {wrong}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.2 – Read the plan
# MAGIC
# MAGIC Build a DataFrame `airport_trips` with all trips that **start at JFK** (`PULocationID == 132`) and are **longer than
# MAGIC 20 miles**, keeping only `tpep_pickup_datetime`, `trip_distance` and `total_amount`. Do **not** run an action – call
# MAGIC `explain(mode="formatted")` and find in the scan node: (a) the filters pushed into the scan (`RequiredDataFilters`),
# MAGIC (b) the columns in `ReadSchema`.

# COMMAND ----------

# TODO Task 2.2 – filter, select and explain
raise NotImplementedError("Task 2.2")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>Combine both conditions in one <code>filter</code> with <code>&amp;</code>, each condition in brackets. Then <code>select</code> the three columns and call <code>explain(mode=&quot;formatted&quot;)</code> – no <code>show()</code> or <code>count()</code>.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>airport_trips = (trips
# MAGIC     .filter((F.col(&quot;PULocationID&quot;) == ___) &amp; (F.col(&quot;trip_distance&quot;) &gt; ___))
# MAGIC     .select(___))
# MAGIC airport_trips.explain(mode=&quot;formatted&quot;)</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>airport_trips = (trips
# MAGIC                  .filter((F.col(&quot;PULocationID&quot;) == 132) &amp; (F.col(&quot;trip_distance&quot;) &gt; 20))
# MAGIC                  .select(&quot;tpep_pickup_datetime&quot;, &quot;trip_distance&quot;, &quot;total_amount&quot;))
# MAGIC airport_trips.explain(mode=&quot;formatted&quot;)
# MAGIC # (a) RequiredDataFilters: isnotnull(PULocationID), isnotnull(trip_distance), PULocationID = 132, trip_distance &gt; 20.0
# MAGIC # (b) ReadSchema: tpep_pickup_datetime, trip_distance, PULocationID, total_amount – 4 of 20 columns</code></pre></details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.3 – Same question in SQL
# MAGIC
# MAGIC Write the query of Task 2.2 in SQL against the temp view `trips` and compare its plan with the Python version.

# COMMAND ----------

# TODO Task 2.3 – the same query in SQL
raise NotImplementedError("Task 2.3")
# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'><code>spark.sql(&quot;SELECT … FROM trips WHERE …&quot;)</code> returns a DataFrame – call <code>explain</code> on it like on any other.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>spark.sql(&quot;&quot;&quot;
# MAGIC     SELECT tpep_pickup_datetime, trip_distance, total_amount
# MAGIC     FROM trips
# MAGIC     WHERE PULocationID = 132 AND trip_distance &gt; 20
# MAGIC &quot;&quot;&quot;).explain(mode=&quot;formatted&quot;)</code></pre></details>

