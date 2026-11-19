# Databricks notebook source
# MAGIC %md
# MAGIC # 07 Orders per 15 minutes: when is a window finished? (demo)
# MAGIC
# MAGIC The restaurants want a chart: **how many orders came in per quarter of an hour**. In week 9 you did that with
# MAGIC Flink SQL, with tumbling windows and a watermark. Spark has the same two ideas. The question is the same too:
# MAGIC events can arrive late, so **when may we say «17:15 to 17:30 had 59 orders, and that's final»?**
# MAGIC
# MAGIC This notebook works on its own folder in the playground, the pipeline isn't touched.

# COMMAND ----------

# MAGIC %run ../_config

# COMMAND ----------

# MAGIC %run ../_generator

# COMMAND ----------

from pyspark.sql import functions as F

DEMO_DIR = f"{PRACTICE_VOLUME}/demo_07"
STATE = f"{PRACTICE_VOLUME}/demo_07_state"
BATCH_TABLE = f"{PLAYGROUND}.w10_demo07_batch"
NO_WM_TABLE = f"{PLAYGROUND}.w10_demo07_no_watermark"
WINDOWS_TABLE = f"{PLAYGROUND}.w10_demo07_windows"
for row in spark.sql(f"SHOW TABLES IN {PLAYGROUND} LIKE 'w10_demo07_*'").collect():
    spark.sql(f"DROP TABLE IF EXISTS {PLAYGROUND}.{row.tableName}")
for folder in [DEMO_DIR, STATE]:
    dbutils.fs.rm(folder, recurse=True)
for batch in (1, 2, 3):
    write_batch(batch, DEMO_DIR)

# event_ts as a real timestamp this time: windows need a time column
ORDER_SCHEMA = ("order_id STRING, customer STRING, restaurant STRING, city STRING, items INT, "
                "amount_chf DOUBLE, status STRING, event_ts TIMESTAMP")


def placed_orders(df):
    return df.where("status = 'placed'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Event time
# MAGIC
# MAGIC Every order has an `event_ts`: when it was placed in the app. That's the time we count by, not the moment the file
# MAGIC reached us. (Week 9: event time vs. processing time.) The placed orders of batches 1 to 3 cover Friday from 17:00
# MAGIC to 19:05.
# MAGIC
# MAGIC `F.window("event_ts", "15 minutes")` puts every row into its quarter of an hour, a *tumbling* window:

# COMMAND ----------

per_window = (placed_orders(spark.read.schema(ORDER_SCHEMA).json(DEMO_DIR))
              .groupBy(F.window("event_ts", "15 minutes")).count()
              .select(F.date_format("window.start", "HH:mm").alias("from"),
                      F.date_format("window.end", "HH:mm").alias("to"), "count")
              .orderBy("from"))
display(per_window)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Without a watermark: is 17:15 finished?
# MAGIC
# MAGIC The batch query above is right for the data it saw. Now an order arrives that was placed at 17:20, but reached us
# MAGIC only now (a phone without signal, a retry in the app). Count again:

# COMMAND ----------

write_late_order(DEMO_DIR, "2026-11-20T17:20:00")
display(per_window.where("`from` = '17:15'"))

# COMMAND ----------

# MAGIC %md
# MAGIC One more in the 17:15 window. If we had sent «17:15 to 17:30: 60 orders» to a report, it would be wrong now. In a
# MAGIC batch job no window is ever really finished, because any file could still bring an old event. You can only
# MAGIC recompute everything, every time, and accept that numbers change.
# MAGIC
# MAGIC A streaming query without a watermark has the same problem, and one more: it has to keep **every** window in its
# MAGIC state forever, just in case.

# COMMAND ----------

def run_no_watermark():
    q = (placed_orders(spark.readStream.schema(ORDER_SCHEMA).json(DEMO_DIR))
         .groupBy(F.window("event_ts", "15 minutes")).count()
         .writeStream.outputMode("complete")
         .option("checkpointLocation", f"{STATE}/no_watermark")
         .trigger(availableNow=True)
         .toTable(NO_WM_TABLE))
    q.awaitTermination()
    print("windows kept in the state:", last_progress(q)["stateOperators"][0]["numRowsTotal"])


run_no_watermark()

# COMMAND ----------

# MAGIC %md
# MAGIC 9 windows in the state after two hours. After a year of orders, 35 000, and every one of them is rewritten with
# MAGIC `complete` on every run. That's not going to work.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. The watermark
# MAGIC
# MAGIC A **watermark** is a promise we make to Spark: «events are at most 10 minutes late. Anything older, drop it.»
# MAGIC Spark tracks the latest event time it has seen and subtracts 10 minutes. That's the watermark. A window whose end
# MAGIC is below the watermark can't get new events anymore, so:
# MAGIC
# MAGIC - with `outputMode("append")` Spark writes the window **once**, when it's final, and never touches it again
# MAGIC - and it removes the window from the state
# MAGIC
# MAGIC ![Windows of 15 minutes from 17:00 to 19:00. The watermark is at 18:55, the latest event 19:05 minus 10 minutes. Windows up to 18:30–18:45 are final and written, 18:45 and 19:00 are still open. A late order placed at 17:20 is dropped](./assets/watermark_windows.png)
# MAGIC
# MAGIC Let's start over with a fresh folder, without the late order for now:

# COMMAND ----------

dbutils.fs.rm(DEMO_DIR, recurse=True)
for batch in (1, 2, 3):
    write_batch(batch, DEMO_DIR)


def run_windows():
    q = (placed_orders(spark.readStream.schema(ORDER_SCHEMA).json(DEMO_DIR))
         .withWatermark("event_ts", "10 minutes")                       # events are at most 10 minutes late
         .groupBy(F.window("event_ts", "15 minutes")).count()
         .select(F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"), "count")
         .writeStream.outputMode("append")                              # write a window once, when it is final
         .option("checkpointLocation", f"{STATE}/windows")
         .trigger(availableNow=True)
         .toTable(WINDOWS_TABLE))
    q.awaitTermination()
    reports = progress_reports(q)            # one report per micro-batch; the last one has the newest watermark
    print("watermark now:", reports[-1].get("eventTime", {}).get("watermark"),
          "| windows still open:", reports[-1]["stateOperators"][0]["numRowsTotal"],
          "| events dropped as too late:", sum(r["stateOperators"][0].get("numRowsDroppedByWatermark", 0) for r in reports))
    display(spark.table(WINDOWS_TABLE).orderBy("window_start"))
    return q


run_windows()

# COMMAND ----------

# MAGIC %md
# MAGIC Seven windows, 17:00 to 18:45. The two latest ones, 18:45 and 19:00, are not in the table yet: the newest event was
# MAGIC at 19:05, the watermark is at 18:55, and the window 18:45–19:00 could still get an event from 18:58. Those two are
# MAGIC the «windows still open» in the state.
# MAGIC
# MAGIC By the way, this run had two micro-batches: one that read the files, and one without data that only moved the
# MAGIC watermark and wrote the windows that had become final. Spark adds that second one by itself.
# MAGIC
# MAGIC (The watermark shows as UTC, `2026-11-20T18:55:00.000Z`. Our event times have no time zone, Spark reads them as UTC.)
# MAGIC
# MAGIC Now the late order from 17:20:

# COMMAND ----------

write_late_order(DEMO_DIR, "2026-11-20T17:20:00")
run_windows()

# COMMAND ----------

# MAGIC %md
# MAGIC Dropped, the table didn't change. That's the deal: windows that are final, at the price of losing what comes later
# MAGIC than promised. Choose the delay by looking at your data: how late do events really arrive? Week 9 had the same
# MAGIC trade-off in Flink.
# MAGIC
# MAGIC Saturday: batch 5 brings orders from 20:35 on. The watermark moves, and the windows that were open are now final:

# COMMAND ----------

write_batch(5, DEMO_DIR)
run_windows()

# COMMAND ----------

# MAGIC %md
# MAGIC 18:45 and 19:00 are there now, and nothing else changed. No window from 19:15 to 20:30 appears: there were no orders
# MAGIC then, and a window without rows doesn't exist.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Under the hood
# MAGIC
# MAGIC The open windows live in the state of the checkpoint, the watermark too. A watermark of 10 minutes keeps the state
# MAGIC small: only the windows of the last few minutes are in it.

# COMMAND ----------

display(dbutils.fs.ls(f"{STATE}/windows/state/0"))

# COMMAND ----------

# MAGIC %md
# MAGIC | | Flink SQL (week 9) | Spark |
# MAGIC |---|---|---|
# MAGIC | tumbling window | `TUMBLE(TABLE t, DESCRIPTOR(ts), INTERVAL '15' MINUTE)` | `F.window("event_ts", "15 minutes")` |
# MAGIC | sliding window | `HOP(...)` | `F.window("event_ts", "15 minutes", "5 minutes")` |
# MAGIC | session window | `SESSION(...)` | `F.session_window("event_ts", "10 minutes")` |
# MAGIC | watermark | `WATERMARK FOR ts AS ts - INTERVAL '10' MINUTE` | `.withWatermark("event_ts", "10 minutes")` |
# MAGIC
# MAGIC Same ideas, different engine.

# COMMAND ----------

# MAGIC %md
# MAGIC ## What you saw
# MAGIC
# MAGIC - In a batch query no window is ever final: any late event changes it.
# MAGIC - A stream without a watermark keeps every window in its state, forever.
# MAGIC - With a watermark Spark knows when a window is final. In `append` mode it writes it once and forgets it. Events
# MAGIC   that come later than the watermark allows are dropped.
# MAGIC
# MAGIC All our queries ran because we pressed *Run*. In real life a job starts them every few minutes. That's notebook 08.
# MAGIC First: `2_exercise_short_windows_watermarks`.
