# Databricks notebook source
# MAGIC %md
# MAGIC # 02 The streaming engine (exercise, short)
# MAGIC
# MAGIC About 10 minutes. You write two streaming queries yourself: one that just copies the new orders into a
# MAGIC table, and one that keeps a running count per restaurant. Same rules as before: replace
# MAGIC `raise NotImplementedError`, run, then run the check.

# COMMAND ----------

# MAGIC %run ../_config

# COMMAND ----------

# MAGIC %run ../_generator

# COMMAND ----------

from pyspark.sql import functions as F

EX_DIR = f"{PRACTICE_VOLUME}/ex02_short"
STATE = f"{PRACTICE_VOLUME}/ex02_short_state"
EX_EVENTS = f"{PLAYGROUND}.w10_ex02_events"
EX_COUNTS = f"{PLAYGROUND}.w10_ex02_per_restaurant"

for table in [EX_EVENTS, EX_COUNTS]:
    spark.sql(f"DROP TABLE IF EXISTS {table}")
for folder in [EX_DIR, STATE]:
    dbutils.fs.rm(folder, recurse=True)
dbutils.fs.mkdirs(EX_DIR)

ORDER_SCHEMA = ("order_id STRING, customer STRING, restaurant STRING, city STRING, items INT, "
                "amount_chf DOUBLE, status STRING, event_ts STRING")

write_batch(1, EX_DIR)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.1 – Copy the new orders ★
# MAGIC
# MAGIC Complete `copy_events()`: a streaming query that reads the JSON files in `EX_DIR` with `ORDER_SCHEMA`
# MAGIC (plain file source, not Auto Loader) and appends them to `EX_EVENTS`. Checkpoint `f"{STATE}/events"`,
# MAGIC trigger `availableNow`. It returns the number of rows it read. Call it once and store the result in `n_1`.

# COMMAND ----------

# TODO Task 2.1
def copy_events():
    query = (
        spark.readStream
        .schema(___)
        .json(___)
        .writeStream
        .option("checkpointLocation", ___)
        .trigger(availableNow=True)
        .toTable(___)
    )
    query.awaitTermination()
    return input_rows(query)


n_1 = copy_events()
print(n_1)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>A plain file source: <code>spark.readStream.schema(ORDER_SCHEMA).json(EX_DIR)</code>. On the write side the checkpoint, the trigger and <code>toTable(EX_EVENTS)</code>. No output mode needed.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>spark.readStream.schema(ORDER_SCHEMA).json(EX_DIR)
# MAGIC     .writeStream
# MAGIC     .option(&quot;checkpointLocation&quot;, f&quot;{STATE}/___&quot;)
# MAGIC     .trigger(availableNow=True)
# MAGIC     .toTable(___)</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>def copy_events():
# MAGIC     query = (
# MAGIC         spark.readStream
# MAGIC         .schema(ORDER_SCHEMA)
# MAGIC         .json(EX_DIR)
# MAGIC         .writeStream
# MAGIC         .option(&quot;checkpointLocation&quot;, f&quot;{STATE}/events&quot;)
# MAGIC         .trigger(availableNow=True)
# MAGIC         .toTable(EX_EVENTS)
# MAGIC     )
# MAGIC     query.awaitTermination()
# MAGIC     return input_rows(query)
# MAGIC 
# MAGIC 
# MAGIC n_1 = copy_events()
# MAGIC print(n_1)</code></pre></details>

# COMMAND ----------

print("Correct!" if n_1 == 200 else f"Not yet – expected 200, got {n_1}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.2 – The second file ★
# MAGIC
# MAGIC Tuesday's file is written below. Run `copy_events()` again and store the result in `n_2`. Which output
# MAGIC mode did your query use, without you choosing one?

# COMMAND ----------

write_batch(2, EX_DIR)

# TODO Task 2.2
raise NotImplementedError("Task 2.2")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>Just call <code>copy_events()</code> again. The default output mode is <code>append</code>.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>n_2 = ___()</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>n_2 = copy_events()
# MAGIC print(n_2)</code></pre></details>

# COMMAND ----------

ok = n_2 == 350 and spark.table(EX_EVENTS).count() == 550
print("Correct!" if ok else f"Not yet – expected 350 new and 550 in total, got {n_2} and {spark.table(EX_EVENTS).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.3 – A running count per restaurant ★★
# MAGIC
# MAGIC Write `count_per_restaurant()`: the same source, `groupBy("restaurant").count()`, written to `EX_COUNTS`
# MAGIC with checkpoint `f"{STATE}/counts"`. Pick the output mode that works for a count (the demo, part 6). Return
# MAGIC the finished query. Call it once and store it in `q_counts`.

# COMMAND ----------

# TODO Task 2.3
raise NotImplementedError("Task 2.3")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>Between source and <code>writeStream</code> comes <code>.groupBy(&quot;restaurant&quot;).count()</code>. A count changes with every file, so <code>append</code> won&#x27;t work: use <code>complete</code>.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>spark.readStream.schema(ORDER_SCHEMA).json(EX_DIR)
# MAGIC     .groupBy(&quot;___&quot;).count()
# MAGIC     .writeStream.outputMode(&quot;___&quot;)
# MAGIC     .option(&quot;checkpointLocation&quot;, f&quot;{STATE}/counts&quot;)
# MAGIC     .trigger(availableNow=True)
# MAGIC     .toTable(EX_COUNTS)</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>def count_per_restaurant():
# MAGIC     query = (
# MAGIC         spark.readStream.schema(ORDER_SCHEMA).json(EX_DIR)
# MAGIC         .groupBy(&quot;restaurant&quot;).count()
# MAGIC         .writeStream
# MAGIC         .outputMode(&quot;complete&quot;)
# MAGIC         .option(&quot;checkpointLocation&quot;, f&quot;{STATE}/counts&quot;)
# MAGIC         .trigger(availableNow=True)
# MAGIC         .toTable(EX_COUNTS)
# MAGIC     )
# MAGIC     query.awaitTermination()
# MAGIC     return query
# MAGIC 
# MAGIC 
# MAGIC q_counts = count_per_restaurant()
# MAGIC display(spark.table(EX_COUNTS))</code></pre></details>

# COMMAND ----------

want = {r.restaurant: r["count"] for r in spark.table(EX_EVENTS).groupBy("restaurant").count().collect()}
got = {r.restaurant: r["count"] for r in spark.table(EX_COUNTS).collect()}
print("Correct!" if got == want else f"Not yet – expected {want}, got {got}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.4 – What does the state hold? ★★
# MAGIC
# MAGIC The next file arrives (cell below). Run `count_per_restaurant()` again. From its progress report
# MAGIC (`last_progress(query)` from `_config`), store how many rows it read in `n_read` and how many rows its
# MAGIC state holds in `n_state` (`stateOperators`, `numRowsTotal`).
# MAGIC
# MAGIC Why is `n_state` so much smaller than the number of orders?

# COMMAND ----------

write_batch(4, EX_DIR)

# TODO Task 2.4
raise NotImplementedError("Task 2.4")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'><code>last_progress(query)</code> returns a dict. <code>numInputRows</code> is at the top, the state size is in <code>stateOperators</code>, a list with one entry per stateful step.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>p = last_progress(count_per_restaurant())
# MAGIC n_read = p[&quot;___&quot;]
# MAGIC n_state = p[&quot;stateOperators&quot;][0][&quot;___&quot;]</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>p = last_progress(count_per_restaurant())
# MAGIC n_read = p[&quot;numInputRows&quot;]
# MAGIC n_state = p[&quot;stateOperators&quot;][0][&quot;numRowsTotal&quot;]
# MAGIC print(n_read, n_state)</code></pre></details>

# COMMAND ----------

n_restaurants = spark.read.schema(ORDER_SCHEMA).json(EX_DIR).select("restaurant").distinct().count()
ok = n_read == 21 and n_state == n_restaurants
print("Correct!" if ok else f"Not yet – expected 21 and {n_restaurants}, got {n_read} and {n_state}")

# COMMAND ----------

# MAGIC %md
# MAGIC The state only holds one row per restaurant: the count so far. Not the orders themselves. That's why a
# MAGIC running count stays cheap however many orders come in.
# MAGIC
# MAGIC Done. More in `3_exercise_extended_streaming_engine`, or go on with notebook 03.
