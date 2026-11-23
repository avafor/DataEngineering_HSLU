# Databricks notebook source
# MAGIC %md
# MAGIC # 05 Anna cancels (exercise, extended)
# MAGIC
# MAGIC For at home. Three situations where the Change Data Feed helps, and one where our gold logic has a weak spot.
# MAGIC
# MAGIC | Part | Question |
# MAGIC |---|---|
# MAGIC | 1 | Lea wants her data deleted. Does gold notice, and is she really gone? |
# MAGIC | 2 | Marketing wants revenue per city. Do we need a second silver? |
# MAGIC | 3 | What if a micro-batch reaches gold twice? |
# MAGIC
# MAGIC Part 1 works on a copy of silver in the playground. The pipeline is only read.

# COMMAND ----------

# MAGIC %run ../_config

# COMMAND ----------

# MAGIC %run ../_generator

# COMMAND ----------

# MAGIC %run ../_pipeline

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F

if not spark.catalog.tableExists(GOLD_TABLE) or spark.table(BRONZE_TABLE).count() != 921:
    catch_up(5)

BASE = f"{PRACTICE_VOLUME}/ex05_extended"
for row in spark.sql(f"SHOW TABLES IN {PLAYGROUND} LIKE 'w10_ex05x_*'").collect():
    spark.sql(f"DROP TABLE IF EXISTS {PLAYGROUND}.{row.tableName}")
dbutils.fs.rm(BASE, recurse=True)

COPY = f"{PLAYGROUND}.w10_ex05x_silver"
COPY_GOLD = f"{PLAYGROUND}.w10_ex05x_gold"


def deltas(changes, key="restaurant"):
    sign = (F.when(F.col("_change_type").isin("insert", "update_postimage"), 1)
             .when(F.col("_change_type").isin("update_preimage", "delete"), -1)
             .otherwise(0))
    counts = F.col("status") != "cancelled"
    return changes.groupBy(key).agg(
        F.sum(F.when(counts, sign).otherwise(0)).alias("d_orders"),
        F.sum(F.when(counts, sign * F.col("amount_chf")).otherwise(0)).alias("d_revenue"))


def merge_deltas(d, gold, key="restaurant"):
    (DeltaTable.forName(spark, gold).alias("t")
     .merge(d.alias("s"), f"t.{key} = s.{key}")
     .whenMatchedUpdate(set={"active_orders": "t.active_orders + s.d_orders",
                             "revenue_chf": "t.revenue_chf + s.d_revenue"})
     .whenNotMatchedInsert(values={key: f"s.{key}", "active_orders": "s.d_orders", "revenue_chf": "s.d_revenue"})
     .execute())


def stream_changes(source, gold, checkpoint, key="restaurant"):
    # The gold pattern of the demo, from any table with a Change Data Feed into any gold table
    q = (spark.readStream.option("readChangeFeed", "true").table(source)
         .writeStream.foreachBatch(lambda df, _: merge_deltas(deltas(df, key), gold, key))
         .option("checkpointLocation", checkpoint)
         .trigger(availableNow=True)
         .start())
    q.awaitTermination()
    return input_rows(q)


def diffs(silver, gold, key="restaurant"):
    # gold compared with a full recompute from silver: rows where something differs
    full = (spark.table(silver).groupBy(key)
            .agg(F.count_if(F.col("status") != "cancelled").alias("n"),
                 F.sum(F.when(F.col("status") != "cancelled", F.col("amount_chf")).otherwise(0)).alias("rev")))
    return (full.join(spark.table(gold), key, "full_outer")
            .where("coalesce(n, 0) != coalesce(active_orders, 0) OR abs(coalesce(rev, 0) - coalesce(revenue_chf, 0)) > 0.005")
            .count())


# A copy of silver with its own Change Data Feed, and a gold built from it
spark.sql(f"CREATE TABLE {COPY} TBLPROPERTIES (delta.enableChangeDataFeed = true) AS SELECT * FROM {SILVER_TABLE}")
spark.sql(f"CREATE TABLE {COPY_GOLD} (restaurant STRING, active_orders BIGINT, revenue_chf DOUBLE)")
print(stream_changes(COPY, COPY_GOLD, f"{BASE}/copy_gold"), "change rows, diffs:", diffs(COPY, COPY_GOLD))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Lea wants her data deleted
# MAGIC
# MAGIC ### What goes wrong
# MAGIC
# MAGIC Lea writes to support: she wants all her orders removed (GDPR, the right to erasure). Someone runs a `DELETE` on
# MAGIC silver. Gold still counts her orders, unless something tells it. And the `DELETE` on silver doesn't touch bronze
# MAGIC at all.
# MAGIC
# MAGIC ### Why
# MAGIC
# MAGIC Every layer is a copy. A delete in one layer is just that: a delete in one layer.
# MAGIC
# MAGIC ### How to do it properly
# MAGIC
# MAGIC For the layers *after* silver, the Change Data Feed carries the delete along: a row with `_change_type = delete`,
# MAGIC which our gold logic counts with −1. For the layers *before* and for the history, you need more (see the end of
# MAGIC this part).
# MAGIC
# MAGIC ### Task 5.4 – Delete Lea, update gold ★★
# MAGIC
# MAGIC Count Lea's orders in `COPY` and store the number in `n_lea`. Delete them. Then run `stream_changes(COPY, COPY_GOLD,
# MAGIC f"{BASE}/copy_gold")` again and store the change rows it read in `n_changes`.

# COMMAND ----------

# TODO Task 5.4
raise NotImplementedError("Task 5.4")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>First the count, then <code>DELETE FROM ... WHERE customer = &#x27;Lea&#x27;</code>, then the same <code>stream_changes</code> call as in the setup cell. Its return value is the number of change rows.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>n_lea = spark.table(COPY).where(&quot;customer = &#x27;Lea&#x27;&quot;).count()
# MAGIC spark.sql(f&quot;DELETE FROM {COPY} WHERE ___&quot;)
# MAGIC n_changes = stream_changes(COPY, COPY_GOLD, f&quot;{BASE}/copy_gold&quot;)</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>n_lea = spark.table(COPY).where(&quot;customer = &#x27;Lea&#x27;&quot;).count()
# MAGIC spark.sql(f&quot;DELETE FROM {COPY} WHERE customer = &#x27;Lea&#x27;&quot;)
# MAGIC n_changes = stream_changes(COPY, COPY_GOLD, f&quot;{BASE}/copy_gold&quot;)
# MAGIC print(n_lea, n_changes)</code></pre></details>

# COMMAND ----------

ok = n_lea > 0 and n_changes == n_lea and diffs(COPY, COPY_GOLD) == 0
print("Correct!" if ok else f"Not yet – expected as many change rows as Lea had orders ({n_lea}) and no diffs, got {n_changes} and {diffs(COPY, COPY_GOLD)} diffs")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 5.5 – Is she really gone? ★
# MAGIC
# MAGIC Store in `n_in_bronze` how many rows of Lea are still in the pipeline's `BRONZE_TABLE`, and in `n_in_history` how
# MAGIC many rows of Lea `COPY` still has in the version **before** the delete (`DESCRIBE HISTORY`, `VERSION AS OF`).

# COMMAND ----------

# TODO Task 5.5
raise NotImplementedError("Task 5.5")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>Bronze: a simple count. History: find the version of the DELETE in <code>DESCRIBE HISTORY</code>, then read the table <code>VERSION AS OF</code> one version earlier.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>delete_version = (spark.sql(f&quot;DESCRIBE HISTORY {COPY}&quot;).where(&quot;operation = &#x27;DELETE&#x27;&quot;)
# MAGIC                   .agg(F.max(&quot;version&quot;)).first()[0])
# MAGIC n_in_history = spark.sql(f&quot;SELECT * FROM {COPY} VERSION AS OF {delete_version - ___}&quot;).where(...).count()</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>n_in_bronze = spark.table(BRONZE_TABLE).where(&quot;customer = &#x27;Lea&#x27;&quot;).count()
# MAGIC delete_version = (spark.sql(f&quot;DESCRIBE HISTORY {COPY}&quot;).where(&quot;operation = &#x27;DELETE&#x27;&quot;)
# MAGIC                   .agg(F.max(&quot;version&quot;)).first()[0])
# MAGIC n_in_history = spark.sql(f&quot;SELECT * FROM {COPY} VERSION AS OF {delete_version - 1}&quot;).where(&quot;customer = &#x27;Lea&#x27;&quot;).count()
# MAGIC print(n_in_bronze, n_in_history)</code></pre></details>

# COMMAND ----------

ok = n_in_bronze > n_lea and n_in_history == n_lea
print("Correct!" if ok else f"Not yet – got {n_in_bronze} in bronze and {n_in_history} in the history")

# COMMAND ----------

# MAGIC %md
# MAGIC Gold is right, and Lea is still everywhere else: in bronze (every event, so more rows than orders), in the landing
# MAGIC files, in the old versions of silver, and in its Change Data Feed. And the next file with a late event of hers
# MAGIC would bring her back into silver through the `MERGE`. A real erasure needs a list of erased customers that every
# MAGIC layer filters on, a delete in every layer including the files, and a `VACUUM` so the old versions are gone too.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. A second gold, per city
# MAGIC
# MAGIC ### What goes wrong
# MAGIC
# MAGIC Marketing wants revenue per city. The quick way is a second pipeline from bronze, with its own silver. Two silvers
# MAGIC that will slowly disagree.
# MAGIC
# MAGIC ### How to do it properly
# MAGIC
# MAGIC Silver stays as it is. A second query reads the same Change Data Feed, with its **own checkpoint**, into its own
# MAGIC gold table. Each consumer keeps its own bookmark. And because the checkpoint is new, its first run starts with all
# MAGIC of silver: the history comes for free.
# MAGIC
# MAGIC ### Task 5.6 – Revenue per city ★★
# MAGIC
# MAGIC Create `f"{PLAYGROUND}.w10_ex05x_city"` with the columns `city STRING, active_orders BIGINT, revenue_chf DOUBLE`.
# MAGIC Fill it with `stream_changes(...)` from the pipeline's `SILVER_TABLE`, key `city`, checkpoint `f"{BASE}/city"`.

# COMMAND ----------

CITY = f"{PLAYGROUND}.w10_ex05x_city"

# TODO Task 5.6
raise NotImplementedError("Task 5.6")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>A <code>CREATE TABLE</code> with a <code>city</code> column instead of <code>restaurant</code>, then <code>stream_changes</code> with <code>key=&quot;city&quot;</code> and a checkpoint folder of its own.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>spark.sql(f&quot;CREATE TABLE {CITY} (city STRING, active_orders BIGINT, revenue_chf DOUBLE)&quot;)
# MAGIC stream_changes(SILVER_TABLE, CITY, f&quot;{BASE}/___&quot;, key=&quot;___&quot;)</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>spark.sql(f&quot;CREATE TABLE {CITY} (city STRING, active_orders BIGINT, revenue_chf DOUBLE)&quot;)
# MAGIC stream_changes(SILVER_TABLE, CITY, f&quot;{BASE}/city&quot;, key=&quot;city&quot;)
# MAGIC display(spark.table(CITY).orderBy(&quot;city&quot;))</code></pre></details>

# COMMAND ----------

print("Correct!" if diffs(SILVER_TABLE, CITY, "city") == 0 and spark.table(CITY).count() == 4
      else f"Not yet – {diffs(SILVER_TABLE, CITY, 'city')} cities differ from a full recompute")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. The same micro-batch twice
# MAGIC
# MAGIC ### What goes wrong
# MAGIC
# MAGIC `foreachBatch` is *at least once*: if a run dies after the `MERGE` into gold but before the checkpoint says
# MAGIC «done», Spark hands over the same batch again. Our silver `MERGE` didn't mind (notebook 04). Gold adds:

# COMMAND ----------

REPLAY_GOLD = f"{PLAYGROUND}.w10_ex05x_replay"
last = spark.sql(f"DESCRIBE HISTORY {COPY}").agg(F.max("version")).first()[0]
one_batch = spark.sql(f"SELECT * FROM table_changes('{COPY}', {last}, {last})")   # the DELETE of Lea, as one batch


def gold_before_the_delete():
    # REPLAY_GOLD as it was before Lea's delete: a full recompute from the version before it
    (spark.sql(f"SELECT * FROM {COPY} VERSION AS OF {last - 1}").groupBy("restaurant")
     .agg(F.count_if(F.col("status") != "cancelled").alias("active_orders"),
          F.sum(F.when(F.col("status") != "cancelled", F.col("amount_chf")).otherwise(0)).alias("revenue_chf"))
     .write.mode("overwrite").saveAsTable(REPLAY_GOLD))


gold_before_the_delete()
merge_deltas(deltas(one_batch), REPLAY_GOLD)    # Spark hands over the batch once ...
print("diffs after one run:  ", diffs(COPY, REPLAY_GOLD))
merge_deltas(deltas(one_batch), REPLAY_GOLD)    # ... and, after a crash, again
print("diffs after the replay:", diffs(COPY, REPLAY_GOLD))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Why
# MAGIC
# MAGIC «Add the deltas» isn't safe to repeat: doing it twice subtracts Lea twice. A `MERGE` that *sets* a value can be
# MAGIC repeated, a `MERGE` that *adds* can't.
# MAGIC
# MAGIC ### How to do it properly
# MAGIC
# MAGIC Remember which batches you already applied, in a small table, and skip a batch you have seen. The id of a batch
# MAGIC is `batch_id`, the second argument of the `foreachBatch` function. (Delta has a built-in way for this too, the
# MAGIC writer options `txnAppId` and `txnVersion`, but they work for plain writes, not for `MERGE`.)
# MAGIC
# MAGIC ### Task 5.7 – Apply each batch only once ★★★
# MAGIC
# MAGIC Create `f"{PLAYGROUND}.w10_ex05x_applied"` with the columns `consumer STRING, batch_id BIGINT`. Write
# MAGIC `apply_once(batch_df, batch_id, consumer, gold)`: if the pair `(consumer, batch_id)` is already in the table, print
# MAGIC a note and do nothing. Otherwise merge the deltas into `gold` and add the pair to the table.
# MAGIC
# MAGIC The check rebuilds `REPLAY_GOLD` as it was before the replay and calls your function twice with the same batch.

# COMMAND ----------

APPLIED = f"{PLAYGROUND}.w10_ex05x_applied"

# TODO Task 5.7
raise NotImplementedError("Task 5.7")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>Look the pair up in <code>APPLIED</code> first. If it&#x27;s there: print and <code>return</code>. If not: <code>merge_deltas(deltas(batch_df), gold)</code>, then append one row <code>(consumer, batch_id)</code> to <code>APPLIED</code>.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>def apply_once(batch_df, batch_id, consumer, gold):
# MAGIC     if spark.table(APPLIED).where(f&quot;consumer = &#x27;{consumer}&#x27; AND batch_id = {batch_id}&quot;).count() &gt; 0:
# MAGIC         return
# MAGIC     merge_deltas(deltas(batch_df), gold)
# MAGIC     spark.createDataFrame([(consumer, batch_id)], &quot;consumer STRING, batch_id BIGINT&quot;).write.mode(&quot;___&quot;).saveAsTable(APPLIED)</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>spark.sql(f&quot;CREATE TABLE IF NOT EXISTS {APPLIED} (consumer STRING, batch_id BIGINT)&quot;)
# MAGIC 
# MAGIC 
# MAGIC def apply_once(batch_df, batch_id, consumer, gold):
# MAGIC     seen = spark.table(APPLIED).where(f&quot;consumer = &#x27;{consumer}&#x27; AND batch_id = {batch_id}&quot;).count() &gt; 0
# MAGIC     if seen:
# MAGIC         print(f&quot;batch {batch_id} of {consumer} was applied already, skipping it&quot;)
# MAGIC         return
# MAGIC     merge_deltas(deltas(batch_df), gold)
# MAGIC     spark.createDataFrame([(consumer, batch_id)], &quot;consumer STRING, batch_id BIGINT&quot;).write.mode(&quot;append&quot;).saveAsTable(APPLIED)</code></pre></details>

# COMMAND ----------

gold_before_the_delete()
apply_once(one_batch, 7, "replay_test", REPLAY_GOLD)
apply_once(one_batch, 7, "replay_test", REPLAY_GOLD)
print("Correct!" if diffs(COPY, REPLAY_GOLD) == 0 else f"Not yet – {diffs(COPY, REPLAY_GOLD)} restaurants are off")

# COMMAND ----------

# MAGIC %md
# MAGIC In the stream you'd call it as `foreachBatch(lambda df, batch_id: apply_once(df, batch_id, "gold_per_restaurant",
# MAGIC GOLD_TABLE))`. One more subtlety for real pipelines: the `MERGE` and the insert into `APPLIED` are two writes again
# MAGIC (notebook 01, part 2). If the job dies between them, the batch is applied but not noted. Storing the last
# MAGIC `batch_id` *in the gold table itself*, in the same `MERGE`, closes that gap. Try it if you want a real challenge.
# MAGIC
# MAGIC ## Clean up (optional)

# COMMAND ----------

for row in spark.sql(f"SHOW TABLES IN {PLAYGROUND} LIKE 'w10_ex05x_*'").collect():
    spark.sql(f"DROP TABLE IF EXISTS {PLAYGROUND}.{row.tableName}")
dbutils.fs.rm(BASE, recurse=True)
