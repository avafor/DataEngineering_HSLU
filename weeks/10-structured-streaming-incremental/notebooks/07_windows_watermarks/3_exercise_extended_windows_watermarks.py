# Databricks notebook source
# MAGIC %md
# MAGIC # 07 Orders per 15 minutes (exercise, extended)
# MAGIC
# MAGIC For at home. Other kinds of windows, a more patient watermark, and windows that may change.
# MAGIC
# MAGIC | Part | Question |
# MAGIC |---|---|
# MAGIC | 1 | «Orders in the last 15 minutes, every 5 minutes»: how many windows is that? |
# MAGIC | 2 | What does a longer watermark cost, and what does it buy? |
# MAGIC | 3 | The dashboard wants the current quarter of an hour too, before it is final |

# COMMAND ----------

# MAGIC %run ../_config

# COMMAND ----------

# MAGIC %run ../_generator

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F

BASE = f"{PRACTICE_VOLUME}/ex07_extended"
for row in spark.sql(f"SHOW TABLES IN {PLAYGROUND} LIKE 'w10_ex07x_*'").collect():
    spark.sql(f"DROP TABLE IF EXISTS {PLAYGROUND}.{row.tableName}")
dbutils.fs.rm(BASE, recurse=True)

ORDER_SCHEMA = ("order_id STRING, customer STRING, restaurant STRING, city STRING, items INT, "
                "amount_chf DOUBLE, status STRING, event_ts TIMESTAMP")


def placed(folder):
    return spark.readStream.schema(ORDER_SCHEMA).json(folder).where("status = 'placed'")


def window_starts(table):
    return sorted({r.s for r in spark.table(table).select(F.date_format("window_start", "HH:mm").alias("s")).collect()})

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Sliding windows
# MAGIC
# MAGIC ### What goes wrong
# MAGIC
# MAGIC «Orders in the last 15 minutes» on a dashboard that refreshes every 5 minutes. With tumbling windows the number
# MAGIC jumps every 15 minutes and says nothing in between.
# MAGIC
# MAGIC ### How to do it properly
# MAGIC
# MAGIC A **sliding** window (Flink: `HOP`): 15 minutes long, a new one every 5 minutes. Every event is then in three
# MAGIC windows. `F.window("event_ts", "15 minutes", "5 minutes")`.
# MAGIC
# MAGIC ### Task 7.4 – Sliding windows ★★
# MAGIC
# MAGIC Count placed orders in sliding windows (15 minutes, every 5 minutes) with a watermark of 10 minutes, `append`, into
# MAGIC `f"{PLAYGROUND}.w10_ex07x_sliding"` (columns `window_start`, `count`), checkpoint `f"{BASE}/sliding_state"`. The
# MAGIC files are in `f"{BASE}/sliding"`, batches 1 to 3. Store the number of windows in the table in `n_sliding`.

# COMMAND ----------

for batch in (1, 2, 3):
    write_batch(batch, f"{BASE}/sliding")

# TODO Task 7.4
raise NotImplementedError("Task 7.4")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'><code>F.window(&quot;event_ts&quot;, &quot;15 minutes&quot;, &quot;5 minutes&quot;)</code>: the third argument is the slide. The rest is the query of the short exercise without <code>restaurant</code>.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>placed(f&quot;{BASE}/sliding&quot;).withWatermark(&quot;event_ts&quot;, &quot;10 minutes&quot;)
# MAGIC     .groupBy(F.window(&quot;event_ts&quot;, &quot;15 minutes&quot;, &quot;___&quot;)).count()</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>q = (placed(f&quot;{BASE}/sliding&quot;)
# MAGIC      .withWatermark(&quot;event_ts&quot;, &quot;10 minutes&quot;)
# MAGIC      .groupBy(F.window(&quot;event_ts&quot;, &quot;15 minutes&quot;, &quot;5 minutes&quot;)).count()
# MAGIC      .select(F.col(&quot;window.start&quot;).alias(&quot;window_start&quot;), &quot;count&quot;)
# MAGIC      .writeStream.outputMode(&quot;append&quot;)
# MAGIC      .option(&quot;checkpointLocation&quot;, f&quot;{BASE}/sliding_state&quot;)
# MAGIC      .trigger(availableNow=True)
# MAGIC      .toTable(f&quot;{PLAYGROUND}.w10_ex07x_sliding&quot;))
# MAGIC q.awaitTermination()
# MAGIC n_sliding = spark.table(f&quot;{PLAYGROUND}.w10_ex07x_sliding&quot;).count()
# MAGIC print(n_sliding, window_starts(f&quot;{PLAYGROUND}.w10_ex07x_sliding&quot;)[:4])</code></pre></details>

# COMMAND ----------

print("Correct!" if n_sliding == 23 else f"Not yet – expected 23 windows, got {n_sliding}")

# COMMAND ----------

# MAGIC %md
# MAGIC Why 23? The first window that contains the order from 17:00 starts at 16:50 (16:50–17:05). A window is final
# MAGIC when it ends before the watermark at 18:55, so the last final one starts at 18:40. From 16:50 to 18:40 in steps of
# MAGIC 5 minutes: 23 windows. And every order is counted in three of them.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. A more patient watermark
# MAGIC
# MAGIC ### What goes wrong
# MAGIC
# MAGIC In the demo, the order from 17:20 was dropped. Maybe your app really sends events up to an hour late (phones
# MAGIC offline in the mountains). Then a watermark of 10 minutes drops real orders.
# MAGIC
# MAGIC ### Why, and how to do it properly
# MAGIC
# MAGIC A watermark of 1 hour keeps windows open for an hour: late events are counted, but windows appear an hour later,
# MAGIC and the state holds an hour of windows. Look at your data first: how late are events really?
# MAGIC
# MAGIC ### Task 7.5 – An hour of patience ★★
# MAGIC
# MAGIC In `f"{BASE}/patient"` are batches 1 to 3, then an order placed at **18:20** arrives late, then batch 5. With 10
# MAGIC minutes it would be dropped (the watermark is at 18:55 by then). Write `run_patient()`: tumbling windows of 15
# MAGIC minutes, watermark **1 hour**, `append`, into `f"{PLAYGROUND}.w10_ex07x_patient"`, checkpoint
# MAGIC `f"{BASE}/patient_state"`. The cell below it calls it after each of the three steps. Store the count of the 18:15
# MAGIC window in `count_1815`.

# COMMAND ----------

PATIENT = f"{PLAYGROUND}.w10_ex07x_patient"

# TODO Task 7.5
raise NotImplementedError("Task 7.5")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>The same query as in 7.4, tumbling windows, but <code>withWatermark(&quot;event_ts&quot;, &quot;1 hour&quot;)</code>, its own table and checkpoint. Put it in <code>run_patient()</code>; the cell below calls it three times.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>def run_patient():
# MAGIC     q = (placed(f&quot;{BASE}/patient&quot;).withWatermark(&quot;event_ts&quot;, &quot;___&quot;)
# MAGIC          .groupBy(F.window(&quot;event_ts&quot;, &quot;15 minutes&quot;)).count() ...)
# MAGIC     q.awaitTermination()</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>def run_patient():
# MAGIC     q = (placed(f&quot;{BASE}/patient&quot;)
# MAGIC          .withWatermark(&quot;event_ts&quot;, &quot;1 hour&quot;)
# MAGIC          .groupBy(F.window(&quot;event_ts&quot;, &quot;15 minutes&quot;)).count()
# MAGIC          .select(F.col(&quot;window.start&quot;).alias(&quot;window_start&quot;), &quot;count&quot;)
# MAGIC          .writeStream.outputMode(&quot;append&quot;)
# MAGIC          .option(&quot;checkpointLocation&quot;, f&quot;{BASE}/patient_state&quot;)
# MAGIC          .trigger(availableNow=True)
# MAGIC          .toTable(PATIENT))
# MAGIC     q.awaitTermination()</code></pre></details>

# COMMAND ----------

for batch in (1, 2, 3):
    write_batch(batch, f"{BASE}/patient")
run_patient()
print("after batch 3:", window_starts(PATIENT))
write_late_order(f"{BASE}/patient", "2026-11-20T18:20:00")
run_patient()
write_batch(5, f"{BASE}/patient")
run_patient()
print("after batch 5:", window_starts(PATIENT))
row = spark.table(PATIENT).where("date_format(window_start, 'HH:mm') = '18:15'").first()
count_1815 = row["count"] if row else None

# COMMAND ----------

batch_count = (spark.read.schema(ORDER_SCHEMA).json(f"{BASE}/patient").where("status = 'placed'")
               .where("event_ts >= '2026-11-20 18:15:00' AND event_ts < '2026-11-20 18:30:00'").count())
print("Correct!" if count_1815 == batch_count else f"Not yet – expected {batch_count} (the late order included), got {count_1815}")

# COMMAND ----------

# MAGIC %md
# MAGIC After batch 3 the watermark is at 18:05: only the windows up to 17:45 are final. The late order from 18:20 still
# MAGIC finds its window open. After batch 5 (latest event 20:50, watermark 19:50) all windows up to 19:00 are final, and
# MAGIC 18:15 has the late order in it.
# MAGIC
# MAGIC ## 3. Windows that may change
# MAGIC
# MAGIC ### What goes wrong
# MAGIC
# MAGIC `append` writes a window only when it's final. The dashboard wants to see the current quarter of an hour growing,
# MAGIC now, not in 10 minutes.
# MAGIC
# MAGIC ### How to do it properly
# MAGIC
# MAGIC Output mode `update`: every run writes the windows that changed, including open ones. A Delta table can't take
# MAGIC `update` directly, so we `MERGE` each micro-batch on the window (notebook 04's pattern). The watermark still keeps
# MAGIC the state small, it just doesn't hold back the output.
# MAGIC
# MAGIC ### Task 7.6 – Open windows in a table ★★★
# MAGIC
# MAGIC Create `f"{PLAYGROUND}.w10_ex07x_live"` (`window_start TIMESTAMP, count BIGINT`). Write `upsert_windows(batch_df,
# MAGIC batch_id)`: MERGE on `window_start`, update `count`, insert new windows. Stream the placed orders of
# MAGIC `f"{BASE}/live"` (batches 1 to 3) with a watermark of 10 minutes and 15-minute windows, **`outputMode("update")`**,
# MAGIC `foreachBatch(upsert_windows)`, checkpoint `f"{BASE}/live_state"`.

# COMMAND ----------

LIVE = f"{PLAYGROUND}.w10_ex07x_live"
for batch in (1, 2, 3):
    write_batch(batch, f"{BASE}/live")

# TODO Task 7.6
raise NotImplementedError("Task 7.6")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'><code>outputMode(&quot;update&quot;)</code> together with <code>foreachBatch(upsert_windows)</code>. In <code>upsert_windows</code> a MERGE on <code>window_start</code> that sets <code>count</code> when matched and inserts otherwise.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>.merge(batch_df.alias(&quot;s&quot;), &quot;t.window_start = s.window_start&quot;)
# MAGIC .whenMatchedUpdate(set={&quot;count&quot;: &quot;s.count&quot;}).whenNotMatchedInsertAll().execute()
# MAGIC ...
# MAGIC .writeStream.outputMode(&quot;___&quot;).foreachBatch(upsert_windows)</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>spark.sql(f&quot;CREATE TABLE {LIVE} (window_start TIMESTAMP, count BIGINT)&quot;)
# MAGIC 
# MAGIC 
# MAGIC def upsert_windows(batch_df, batch_id):
# MAGIC     (DeltaTable.forName(batch_df.sparkSession, LIVE).alias(&quot;t&quot;)
# MAGIC      .merge(batch_df.alias(&quot;s&quot;), &quot;t.window_start = s.window_start&quot;)
# MAGIC      .whenMatchedUpdate(set={&quot;count&quot;: &quot;s.count&quot;})
# MAGIC      .whenNotMatchedInsertAll()
# MAGIC      .execute())
# MAGIC 
# MAGIC 
# MAGIC q = (placed(f&quot;{BASE}/live&quot;)
# MAGIC      .withWatermark(&quot;event_ts&quot;, &quot;10 minutes&quot;)
# MAGIC      .groupBy(F.window(&quot;event_ts&quot;, &quot;15 minutes&quot;)).count()
# MAGIC      .select(F.col(&quot;window.start&quot;).alias(&quot;window_start&quot;), &quot;count&quot;)
# MAGIC      .writeStream.outputMode(&quot;update&quot;)
# MAGIC      .foreachBatch(upsert_windows)
# MAGIC      .option(&quot;checkpointLocation&quot;, f&quot;{BASE}/live_state&quot;)
# MAGIC      .trigger(availableNow=True)
# MAGIC      .start())
# MAGIC q.awaitTermination()
# MAGIC display(spark.table(LIVE).orderBy(&quot;window_start&quot;))</code></pre></details>

# COMMAND ----------

got = window_starts(LIVE)
ok = "18:45" in got and "19:00" in got and len(got) == 9
print("Correct!" if ok else f"Not yet – expected 9 windows including the open ones 18:45 and 19:00, got {got}")

# COMMAND ----------

# MAGIC %md
# MAGIC All nine windows, the open ones included. Their counts may still change with the next file, and the `MERGE` will
# MAGIC overwrite them. A consumer of this table has to know that the latest rows aren't final yet.
# MAGIC
# MAGIC ## Clean up (optional)

# COMMAND ----------

for row in spark.sql(f"SHOW TABLES IN {PLAYGROUND} LIKE 'w10_ex07x_*'").collect():
    spark.sql(f"DROP TABLE IF EXISTS {PLAYGROUND}.{row.tableName}")
dbutils.fs.rm(BASE, recurse=True)
