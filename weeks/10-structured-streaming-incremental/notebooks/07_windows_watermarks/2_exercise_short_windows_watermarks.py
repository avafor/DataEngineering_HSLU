# Databricks notebook source
# MAGIC %md
# MAGIC # 07 Orders per 15 minutes (exercise, short)
# MAGIC
# MAGIC About 10 minutes. You count orders per restaurant and quarter of an hour, with a watermark, and watch what a late
# MAGIC event and a new batch do. Everything in the playground.

# COMMAND ----------

# MAGIC %run ../_config

# COMMAND ----------

# MAGIC %run ../_generator

# COMMAND ----------

from pyspark.sql import functions as F

EX_DIR = f"{PRACTICE_VOLUME}/ex07_short"
STATE = f"{PRACTICE_VOLUME}/ex07_short_state"
EX_TABLE = f"{PLAYGROUND}.w10_ex07_windows"
spark.sql(f"DROP TABLE IF EXISTS {EX_TABLE}")
for folder in [EX_DIR, STATE]:
    dbutils.fs.rm(folder, recurse=True)
for batch in (1, 2, 3):
    write_batch(batch, EX_DIR)

ORDER_SCHEMA = ("order_id STRING, customer STRING, restaurant STRING, city STRING, items INT, "
                "amount_chf DOUBLE, status STRING, event_ts TIMESTAMP")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 7.1 – Orders per restaurant and 15 minutes ★
# MAGIC
# MAGIC Complete `run_windows()`: only `placed` orders, a watermark of 10 minutes on `event_ts`, grouped by a 15-minute
# MAGIC window of `event_ts` **and** `restaurant`, counted. Output mode `append` into `EX_TABLE`, checkpoint
# MAGIC `f"{STATE}/windows"`. It returns the finished query. Call it once.

# COMMAND ----------

# TODO Task 7.1
def run_windows():
    q = (spark.readStream.schema(ORDER_SCHEMA).json(EX_DIR)
         .where("status = 'placed'")
         .withWatermark("event_ts", "___")
         .groupBy(F.window("event_ts", "___"), "restaurant").count()
         .select(F.col("window.start").alias("window_start"), "restaurant", "count")
         .writeStream.outputMode("___")
         .option("checkpointLocation", f"{STATE}/windows")
         .trigger(availableNow=True)
         .toTable(EX_TABLE))
    q.awaitTermination()
    return q


run_windows()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'><code>withWatermark(&quot;event_ts&quot;, &quot;10 minutes&quot;)</code> before the <code>groupBy</code>, the window is <code>F.window(&quot;event_ts&quot;, &quot;15 minutes&quot;)</code>, the output mode that writes a window once is <code>append</code>.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>.withWatermark(&quot;event_ts&quot;, &quot;10 minutes&quot;)
# MAGIC .groupBy(F.window(&quot;event_ts&quot;, &quot;___&quot;), &quot;restaurant&quot;).count()
# MAGIC ...
# MAGIC .writeStream.outputMode(&quot;___&quot;)</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>def run_windows():
# MAGIC     q = (spark.readStream.schema(ORDER_SCHEMA).json(EX_DIR)
# MAGIC          .where(&quot;status = &#x27;placed&#x27;&quot;)
# MAGIC          .withWatermark(&quot;event_ts&quot;, &quot;10 minutes&quot;)
# MAGIC          .groupBy(F.window(&quot;event_ts&quot;, &quot;15 minutes&quot;), &quot;restaurant&quot;).count()
# MAGIC          .select(F.col(&quot;window.start&quot;).alias(&quot;window_start&quot;), &quot;restaurant&quot;, &quot;count&quot;)
# MAGIC          .writeStream.outputMode(&quot;append&quot;)
# MAGIC          .option(&quot;checkpointLocation&quot;, f&quot;{STATE}/windows&quot;)
# MAGIC          .trigger(availableNow=True)
# MAGIC          .toTable(EX_TABLE))
# MAGIC     q.awaitTermination()
# MAGIC     return q
# MAGIC 
# MAGIC 
# MAGIC run_windows()</code></pre></details>

# COMMAND ----------

starts = sorted({r.s for r in spark.table(EX_TABLE).select(F.date_format("window_start", "HH:mm").alias("s")).collect()})
want = ["17:00", "17:15", "17:30", "17:45", "18:00", "18:15", "18:30"]
print("Correct!" if starts == want else f"Not yet – expected the windows {want}, got {starts}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 7.2 – A late order ★
# MAGIC
# MAGIC An order placed at 17:20 arrives now (cell below). Run `run_windows()` again. From `last_progress(...)` store how
# MAGIC many events the watermark dropped in `n_dropped` (`stateOperators`, `numRowsDroppedByWatermark`).

# COMMAND ----------

write_late_order(EX_DIR, "2026-11-20T17:20:00")

# TODO Task 7.2
raise NotImplementedError("Task 7.2")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'><code>last_progress(run_windows())</code> is a dict. The dropped events are in <code>stateOperators</code>, the first entry, key <code>numRowsDroppedByWatermark</code>.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>p = last_progress(run_windows())
# MAGIC n_dropped = p[&quot;stateOperators&quot;][0][&quot;___&quot;]</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>p = last_progress(run_windows())
# MAGIC n_dropped = p[&quot;stateOperators&quot;][0][&quot;numRowsDroppedByWatermark&quot;]
# MAGIC print(n_dropped)</code></pre></details>

# COMMAND ----------

print("Correct!" if n_dropped == 1 else f"Not yet – expected 1, got {n_dropped}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 7.3 – The watermark moves ★★
# MAGIC
# MAGIC Batch 5 arrives (cell below), with orders from 20:35 on. Run `run_windows()` again and store the start times of the
# MAGIC windows that are **new** in `EX_TABLE` as a sorted list of `"HH:mm"` strings in `new_windows`.
# MAGIC
# MAGIC Why are they final now, and why isn't 20:30 among them?

# COMMAND ----------

write_batch(5, EX_DIR)
before = {r.s for r in spark.table(EX_TABLE).select(F.date_format("window_start", "HH:mm").alias("s")).collect()}

# TODO Task 7.3
raise NotImplementedError("Task 7.3")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'><code>before</code> holds the window starts before the run. Run, read the starts again the same way, and take the difference of the two sets.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>run_windows()
# MAGIC after = {r.s for r in spark.table(EX_TABLE).select(F.date_format(&quot;window_start&quot;, &quot;HH:mm&quot;).alias(&quot;s&quot;)).collect()}
# MAGIC new_windows = sorted(after ___ before)</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>run_windows()
# MAGIC after = {r.s for r in spark.table(EX_TABLE).select(F.date_format(&quot;window_start&quot;, &quot;HH:mm&quot;).alias(&quot;s&quot;)).collect()}
# MAGIC new_windows = sorted(after - before)
# MAGIC print(new_windows)</code></pre></details>

# COMMAND ----------

print("Correct!" if new_windows == ["18:45", "19:00"] else f"Not yet – expected ['18:45', '19:00'], got {new_windows}")

# COMMAND ----------

# MAGIC %md
# MAGIC The newest event is from 20:50 now, so the watermark is at 20:40: every window that ends before that is final.
# MAGIC 20:30–20:45 ends after 20:40, it could still get an event from 20:44.
# MAGIC
# MAGIC Done. More in `3_exercise_extended_windows_watermarks`, or go on with notebook 08.
