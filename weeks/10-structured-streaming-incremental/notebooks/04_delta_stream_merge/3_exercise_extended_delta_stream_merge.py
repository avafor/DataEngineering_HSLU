# Databricks notebook source
# MAGIC %md
# MAGIC # 04 From events to one row per order (exercise, extended)
# MAGIC
# MAGIC For at home. Silver looks simple once it works. These are the ways it quietly goes wrong.
# MAGIC
# MAGIC | Part | Question |
# MAGIC |---|---|
# MAGIC | 1 | What if the MERGE updates without asking «is it newer»? |
# MAGIC | 2 | Isn't `dropDuplicates` the same as «newest per order»? |
# MAGIC | 3 | What happens when the source table gets an UPDATE? |
# MAGIC | 4 | Can silver keep a value from the first event, like when the order was placed? |
# MAGIC
# MAGIC Everything happens on copies in the playground. The pipeline is only read.

# COMMAND ----------

# MAGIC %run ../_config

# COMMAND ----------

# MAGIC %run ../_generator

# COMMAND ----------

# MAGIC %run ../_pipeline

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F

if not spark.catalog.tableExists(BRONZE_TABLE) or spark.table(BRONZE_TABLE).count() != 801:
    catch_up(3)

BASE = f"{PRACTICE_VOLUME}/ex04_extended"
for row in spark.sql(f"SHOW TABLES IN {PLAYGROUND} LIKE 'w10_ex04x_*'").collect():
    spark.sql(f"DROP TABLE IF EXISTS {PLAYGROUND}.{row.tableName}")
dbutils.fs.rm(BASE, recurse=True)

newest_first = Window.partitionBy("order_id").orderBy(F.col("event_ts").desc())
bronze = spark.table(BRONZE_TABLE).where("_rescued_data IS NULL").select(
    "order_id", "restaurant", "amount_chf", "status", F.to_timestamp("event_ts").alias("event_ts"), "_source_file")


def newest(df):
    return df.withColumn("_rn", F.row_number().over(newest_first)).where("_rn = 1").drop("_rn", "_source_file")


def fresh_silver(name):
    # An empty small silver table in the playground, filled with the newest event per order from bronze
    table = f"{PLAYGROUND}.w10_ex04x_{name}"
    newest(bronze).write.mode("overwrite").saveAsTable(table)
    return table


def delivered(table):
    return spark.table(table).where("status = 'delivered'").count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. A MERGE without «is it newer?»
# MAGIC
# MAGIC ### What goes wrong
# MAGIC
# MAGIC Many MERGEs you find online look like this: matched → update, not matched → insert. No condition. It works on
# MAGIC the first run. Then a batch comes again (a crash, a backfill, a file delivered twice), and old events overwrite
# MAGIC newer ones.

# COMMAND ----------

monday = bronze.where("_source_file LIKE '%orders_batch_1.json'")
print("Monday's rows:", monday.count(), "all", [r.status for r in monday.select("status").distinct().collect()])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4.4 – Both MERGEs, side by side ★★
# MAGIC
# MAGIC Two silver copies are prepared below, both with 300 delivered orders. Replay `newest(monday)` into each:
# MAGIC
# MAGIC - into `t_plain` with a MERGE **without** condition on the update
# MAGIC - into `t_safe` with the condition `s.event_ts > t.event_ts`
# MAGIC
# MAGIC Store the delivered orders afterwards in `delivered_plain` and `delivered_safe`.

# COMMAND ----------

t_plain, t_safe = fresh_silver("plain"), fresh_silver("safe")

# TODO Task 4.4
raise NotImplementedError("Task 4.4")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>One MERGE per table, the same except for <code>condition</code> in <code>whenMatchedUpdateAll</code>: <code>None</code> for the plain one. A loop over <code>[(t_plain, None), (t_safe, &quot;s.event_ts &gt; t.event_ts&quot;)]</code> keeps it short.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>for table, condition in [(t_plain, None), (t_safe, &quot;___&quot;)]:
# MAGIC     (DeltaTable.forName(spark, table).alias(&quot;t&quot;)
# MAGIC      .merge(newest(monday).alias(&quot;s&quot;), &quot;t.order_id = s.order_id&quot;)
# MAGIC      .whenMatchedUpdateAll(condition=___)
# MAGIC      .whenNotMatchedInsertAll().execute())</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>for table, condition in [(t_plain, None), (t_safe, &quot;s.event_ts &gt; t.event_ts&quot;)]:
# MAGIC     (DeltaTable.forName(spark, table).alias(&quot;t&quot;)
# MAGIC      .merge(newest(monday).alias(&quot;s&quot;), &quot;t.order_id = s.order_id&quot;)
# MAGIC      .whenMatchedUpdateAll(condition=condition)
# MAGIC      .whenNotMatchedInsertAll()
# MAGIC      .execute())
# MAGIC delivered_plain, delivered_safe = delivered(t_plain), delivered(t_safe)
# MAGIC print(delivered_plain, delivered_safe)</code></pre></details>

# COMMAND ----------

ok = (delivered_plain, delivered_safe) == (100, 300)
print("Correct!" if ok else f"Not yet – expected 100 and 300, got {delivered_plain} and {delivered_safe}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Why
# MAGIC
# MAGIC Orders 1 to 200 were placed on Monday and 1 to 150 (and 151 to 300) delivered later. Without the condition,
# MAGIC Monday's `placed` overwrote the newer `delivered` for orders 1 to 200: 200 deliveries undone. With the
# MAGIC condition, an older event can't win. Use a time that comes **from the event** (`event_ts`), not the time of
# MAGIC loading: a replayed batch is loaded later, but its events are just as old as before.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. `dropDuplicates` is not «newest per order»
# MAGIC
# MAGIC ### What goes wrong
# MAGIC
# MAGIC `dropDuplicates(["order_id"])` looks like the short way to one row per order. It keeps *a* row per order, but
# MAGIC which one isn't defined. Compare it with the window:

# COMMAND ----------

by_window = newest(bronze).select("order_id", F.col("status").alias("by_window"))
by_drop = bronze.dropDuplicates(["order_id"]).select("order_id", F.col("status").alias("by_drop"))
display(by_window.join(by_drop, "order_id").groupBy("by_window", "by_drop").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Why, and how to do it properly
# MAGIC
# MAGIC `dropDuplicates` keeps whatever row it meets first, and that depends on how the data is split up. Some orders
# MAGIC come out as `placed` although they were delivered. It's the right tool for **exact** duplicates (the same event
# MAGIC twice, like the double event in batch 4), not for «newest». For «newest» you need an order: the window, or
# MAGIC `max_by`.
# MAGIC
# MAGIC ### Task 4.5 – Newest per order with `max_by` ★★
# MAGIC
# MAGIC Without a window: group `bronze` by `order_id` and take the `status` of the row with the largest `event_ts`
# MAGIC (`F.max_by("status", "event_ts")`). Store the result, with the columns `order_id` and `status`, in
# MAGIC `by_max_by`.

# COMMAND ----------

# TODO Task 4.5
raise NotImplementedError("Task 4.5")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'><code>F.max_by(col, ord)</code> returns the value of <code>col</code> in the row where <code>ord</code> is largest, per group.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>by_max_by = bronze.groupBy(&quot;___&quot;).agg(F.max_by(&quot;status&quot;, &quot;___&quot;).alias(&quot;status&quot;))</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>by_max_by = bronze.groupBy(&quot;order_id&quot;).agg(F.max_by(&quot;status&quot;, &quot;event_ts&quot;).alias(&quot;status&quot;))</code></pre></details>

# COMMAND ----------

diff = (by_max_by.join(by_window, "order_id").where("status != by_window").count())
print("Correct!" if diff == 0 and by_max_by.count() == 500 else f"Not yet – {diff} orders differ from the window, {by_max_by.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. An UPDATE in the source
# MAGIC
# MAGIC ### What goes wrong
# MAGIC
# MAGIC Somebody fixes a wrong amount directly in a table that is the source of a stream. Plain Delta streaming only
# MAGIC understands appends. An `UPDATE` rewrites a file, and the stream doesn't know what to do with the old and the
# MAGIC new version of those rows.

# COMMAND ----------

SOURCE = f"{PLAYGROUND}.w10_ex04x_source"
TARGET = f"{PLAYGROUND}.w10_ex04x_copy"
bronze.write.mode("overwrite").saveAsTable(SOURCE)


def copy(state, **options):
    # A plain stream SOURCE → TARGET. Through run_stream_once (from _config), so that a failing stream comes back
    # as a RuntimeError instead of failing the whole cell
    return run_stream_once(TARGET, state, source_table=SOURCE, **options)


print("first run:", copy(f"{BASE}/copy"))
spark.sql(f"UPDATE {SOURCE} SET amount_chf = amount_chf + 1 WHERE order_id = 'o-00001'")
try:
    copy(f"{BASE}/copy")
except RuntimeError as e:
    print(str(e)[:250])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Why
# MAGIC
# MAGIC The `UPDATE` wrote a new file with the changed rows, and removed the old one. To the stream that's «rows that
# MAGIC appear again», and it refuses to guess. That's a feature: it would rather stop than give you duplicates.
# MAGIC
# MAGIC ### Task 4.6 – `skipChangeCommits` ★★
# MAGIC
# MAGIC The option `skipChangeCommits` tells the stream to skip commits that update or delete. Run `copy` again with the
# MAGIC same state folder and that option set to `"true"`. Store the rows read in `n_skipped`. Then check the amount of
# MAGIC `o-00001` in `TARGET`: did the fix arrive?

# COMMAND ----------

# TODO Task 4.6
raise NotImplementedError("Task 4.6")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'><code>copy</code> passes keyword arguments on as reader options. Same state folder as before.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>n_skipped = copy(f&quot;{BASE}/copy&quot;, skipChangeCommits=&quot;___&quot;)</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>n_skipped = copy(f&quot;{BASE}/copy&quot;, skipChangeCommits=&quot;true&quot;)
# MAGIC display(spark.table(TARGET).where(&quot;order_id = &#x27;o-00001&#x27;&quot;).select(&quot;order_id&quot;, &quot;status&quot;, &quot;amount_chf&quot;))</code></pre></details>

# COMMAND ----------

print("Correct!" if n_skipped == 0 else f"Not yet – expected 0, got {n_skipped}")

# COMMAND ----------

# MAGIC %md
# MAGIC The stream runs again, and the fix is lost: `TARGET` still has the old amount. `skipChangeCommits` is fine
# MAGIC when updates in the source don't matter downstream (a cleanup, a GDPR delete you handle elsewhere). When they
# MAGIC do matter, as in silver → gold when Anna cancels, you need the **Change Data Feed**. Notebook 05.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Keep the first, update the rest
# MAGIC
# MAGIC ### What goes wrong
# MAGIC
# MAGIC `whenMatchedUpdateAll` overwrites every column. Once an order is delivered, silver no longer knows when it was
# MAGIC placed, and «time from order to delivery» can't be computed anymore.
# MAGIC
# MAGIC ### How to do it properly
# MAGIC
# MAGIC Update only the columns that should change, and set a column like `placed_at` only on insert.
# MAGIC
# MAGIC ### Task 4.7 – `placed_at` ★★★
# MAGIC
# MAGIC Create `f"{PLAYGROUND}.w10_ex04x_with_placed"` with the columns of `newest(bronze)` plus `placed_at TIMESTAMP`
# MAGIC (empty). Write `upsert_keep_placed(batch_df, batch_id)`: newest event per order, MERGE on `order_id`;
# MAGIC on insert set every column and `placed_at = event_ts`; on update (only if newer) set `status`, `amount_chf`,
# MAGIC `event_ts`, but **not** `placed_at`. Then stream bronze through it in two steps, so that orders are placed in one
# MAGIC run and delivered in a later one: first a micro-batch with only Monday's and Tuesday's files, then the rest.
# MAGIC
# MAGIC Hint for the two steps: call `upsert_keep_placed` directly with a filtered DataFrame for step 1, and for
# MAGIC step 2 with the rows of `orders_batch_3.json`.

# COMMAND ----------

WITH_PLACED = f"{PLAYGROUND}.w10_ex04x_with_placed"

# TODO Task 4.7
raise NotImplementedError("Task 4.7")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'><code>whenMatchedUpdate(condition=..., set={...})</code> with only the columns that may change. <code>whenNotMatchedInsert(values={...})</code> with all columns, <code>placed_at</code> from <code>s.event_ts</code>.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>.whenMatchedUpdate(condition=&quot;s.event_ts &gt; t.event_ts&quot;,
# MAGIC                    set={&quot;status&quot;: &quot;s.status&quot;, &quot;amount_chf&quot;: &quot;s.amount_chf&quot;, &quot;event_ts&quot;: &quot;s.event_ts&quot;})
# MAGIC .whenNotMatchedInsert(values={..., &quot;placed_at&quot;: &quot;___&quot;})</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>spark.sql(f&quot;&quot;&quot;CREATE OR REPLACE TABLE {WITH_PLACED} (
# MAGIC     order_id STRING, restaurant STRING, amount_chf DOUBLE, status STRING, event_ts TIMESTAMP, placed_at TIMESTAMP)&quot;&quot;&quot;)
# MAGIC 
# MAGIC 
# MAGIC def upsert_keep_placed(batch_df, batch_id):
# MAGIC     latest = newest(batch_df)
# MAGIC     (DeltaTable.forName(spark, WITH_PLACED).alias(&quot;t&quot;)
# MAGIC      .merge(latest.alias(&quot;s&quot;), &quot;t.order_id = s.order_id&quot;)
# MAGIC      .whenMatchedUpdate(condition=&quot;s.event_ts &gt; t.event_ts&quot;,
# MAGIC                         set={&quot;status&quot;: &quot;s.status&quot;, &quot;amount_chf&quot;: &quot;s.amount_chf&quot;, &quot;event_ts&quot;: &quot;s.event_ts&quot;})
# MAGIC      .whenNotMatchedInsert(values={&quot;order_id&quot;: &quot;s.order_id&quot;, &quot;restaurant&quot;: &quot;s.restaurant&quot;,
# MAGIC                                    &quot;amount_chf&quot;: &quot;s.amount_chf&quot;, &quot;status&quot;: &quot;s.status&quot;,
# MAGIC                                    &quot;event_ts&quot;: &quot;s.event_ts&quot;, &quot;placed_at&quot;: &quot;s.event_ts&quot;})
# MAGIC      .execute())
# MAGIC 
# MAGIC 
# MAGIC upsert_keep_placed(bronze.where(&quot;_source_file NOT LIKE &#x27;%orders_batch_3.json&#x27;&quot;), 0)
# MAGIC upsert_keep_placed(bronze.where(&quot;_source_file LIKE &#x27;%orders_batch_3.json&#x27;&quot;), 1)</code></pre></details>

# COMMAND ----------

r = spark.table(WITH_PLACED).where("order_id = 'o-00151'").first()
placed = bronze.where("order_id = 'o-00151' AND status = 'placed'").first().event_ts
ok = r is not None and r.status == "delivered" and r.placed_at == placed and r.event_ts > r.placed_at
print("Correct!" if ok else f"Not yet – o-00151 should be delivered with placed_at = {placed}, got {r}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean up (optional)

# COMMAND ----------

for row in spark.sql(f"SHOW TABLES IN {PLAYGROUND} LIKE 'w10_ex04x_*'").collect():
    spark.sql(f"DROP TABLE IF EXISTS {PLAYGROUND}.{row.tableName}")
dbutils.fs.rm(BASE, recurse=True)
