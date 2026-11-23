# Databricks notebook source
# MAGIC %md
# MAGIC # 05 Anna cancels (exercise, short)
# MAGIC
# MAGIC About 10 minutes. You read the Change Data Feed of silver yourself, turn it into plus and minus, and build your
# MAGIC own small gold table in the playground. Silver is only read.

# COMMAND ----------

# MAGIC %run ../_config

# COMMAND ----------

# MAGIC %run ../_generator

# COMMAND ----------

# MAGIC %run ../_pipeline

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F

# This exercise needs the pipeline as notebook 05 leaves it (batches 1 to 5, gold built)
if not spark.catalog.tableExists(GOLD_TABLE) or spark.table(BRONZE_TABLE).count() != 921:
    catch_up(5)

EX_GOLD = f"{PLAYGROUND}.w10_ex05_gold"
STATE = f"{PRACTICE_VOLUME}/ex05_short_state"
spark.sql(f"DROP TABLE IF EXISTS {EX_GOLD}")
dbutils.fs.rm(STATE, recurse=True)
spark.sql(f"CREATE TABLE {EX_GOLD} (restaurant STRING, active_orders BIGINT, revenue_chf DOUBLE)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 5.1 – Anna's history ★
# MAGIC
# MAGIC Use `table_changes` on `SILVER_TABLE` from version 0 and keep the rows of Anna's order (`ANNA_ORDER`). Store the
# MAGIC change types in the order they happened (by `_commit_version`, then `_change_type`) as a list in `anna_changes`.

# COMMAND ----------

# TODO Task 5.1
raise NotImplementedError("Task 5.1")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'><code>table_changes(&#x27;&lt;table&gt;&#x27;, 0)</code> in SQL, filtered on <code>order_id</code>, ordered by <code>_commit_version</code> and <code>_change_type</code>. Collect the <code>_change_type</code> values into a list.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>anna_changes = [r._change_type for r in spark.sql(
# MAGIC     f&quot;SELECT _change_type FROM table_changes(&#x27;{SILVER_TABLE}&#x27;, 0) WHERE order_id = &#x27;{ANNA_ORDER}&#x27; &quot;
# MAGIC     f&quot;ORDER BY ___, ___&quot;).collect()]</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>anna_changes = [r._change_type for r in spark.sql(
# MAGIC     f&quot;SELECT _change_type FROM table_changes(&#x27;{SILVER_TABLE}&#x27;, 0) WHERE order_id = &#x27;{ANNA_ORDER}&#x27; &quot;
# MAGIC     f&quot;ORDER BY _commit_version, _change_type&quot;).collect()]
# MAGIC print(anna_changes)</code></pre></details>

# COMMAND ----------

want = ["insert", "update_postimage", "update_preimage"]
print("Correct!" if anna_changes == want else f"Not yet – expected {want}, got {anna_changes}")

# COMMAND ----------

# MAGIC %md
# MAGIC (Within one version, `update_postimage` sorts before `update_preimage` only because of the alphabet. The two
# MAGIC belong to the same update.)
# MAGIC
# MAGIC ### Task 5.2 – Plus and minus ★
# MAGIC
# MAGIC Complete `deltas(changes)`: a column `sign` that is `1` for `insert` and `update_postimage`, `-1` for
# MAGIC `update_preimage` and `delete`; then per restaurant `d_orders` (sum of `sign`, but only where the status isn't
# MAGIC `cancelled`) and `d_revenue` (the same with `sign * amount_chf`). The check uses it on the changes of Friday's
# MAGIC `MERGE`.

# COMMAND ----------

# TODO Task 5.2
def deltas(changes):
    sign = (F.when(F.col("_change_type").isin("___", "___"), 1)
             .when(F.col("_change_type").isin("___", "___"), -1)
             .otherwise(0))
    counts = F.col("status") != "cancelled"
    return changes.groupBy("restaurant").agg(
        F.sum(F.when(counts, sign).otherwise(0)).alias("d_orders"),
        F.sum(F.when(counts, ___).otherwise(0)).alias("d_revenue"))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'><code>insert</code> and <code>update_postimage</code> are the rows as they are now (+1), <code>update_preimage</code> and <code>delete</code> the rows as they were (−1). The revenue is the same sum, with <code>sign * amount_chf</code>.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>sign = (F.when(F.col(&quot;_change_type&quot;).isin(&quot;insert&quot;, &quot;update_postimage&quot;), 1)
# MAGIC          .when(F.col(&quot;_change_type&quot;).isin(&quot;___&quot;, &quot;___&quot;), -1).otherwise(0))
# MAGIC ...F.sum(F.when(counts, sign * F.col(&quot;___&quot;)).otherwise(0)).alias(&quot;d_revenue&quot;)</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>def deltas(changes):
# MAGIC     sign = (F.when(F.col(&quot;_change_type&quot;).isin(&quot;insert&quot;, &quot;update_postimage&quot;), 1)
# MAGIC              .when(F.col(&quot;_change_type&quot;).isin(&quot;update_preimage&quot;, &quot;delete&quot;), -1)
# MAGIC              .otherwise(0))
# MAGIC     counts = F.col(&quot;status&quot;) != &quot;cancelled&quot;
# MAGIC     return changes.groupBy(&quot;restaurant&quot;).agg(
# MAGIC         F.sum(F.when(counts, sign).otherwise(0)).alias(&quot;d_orders&quot;),
# MAGIC         F.sum(F.when(counts, sign * F.col(&quot;amount_chf&quot;)).otherwise(0)).alias(&quot;d_revenue&quot;))</code></pre></details>

# COMMAND ----------

friday = spark.sql(f"SELECT max(_commit_version) AS v FROM table_changes('{SILVER_TABLE}', 0) WHERE order_id = '{ANNA_ORDER}'").first().v
got = {r.restaurant: r.d_orders for r in deltas(spark.sql(f"SELECT * FROM table_changes('{SILVER_TABLE}', {friday}, {friday})")).collect()}
want = {r.restaurant: -r.n for r in spark.table(SILVER_TABLE).where("status = 'cancelled'").groupBy("restaurant").agg(F.count("*").alias("n")).collect()}
print("Correct!" if got == want else f"Not yet – expected {want}, got {got}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 5.3 – Your own gold ★★
# MAGIC
# MAGIC Write `apply(batch_df, batch_id)`: compute `deltas(batch_df)` and `MERGE` them into `EX_GOLD` on `restaurant`.
# MAGIC Matched: add `d_orders` to `active_orders` and `d_revenue` to `revenue_chf`. Not matched: insert them. Then start a
# MAGIC stream on the Change Data Feed of `SILVER_TABLE` with `foreachBatch(apply)`, checkpoint `f"{STATE}/gold"`,
# MAGIC `availableNow`.

# COMMAND ----------

# TODO Task 5.3
raise NotImplementedError("Task 5.3")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>In <code>apply</code>, a MERGE on <code>restaurant</code>: <code>whenMatchedUpdate</code> adds, <code>whenNotMatchedInsert</code> inserts. The stream reads <code>readStream.option(&quot;readChangeFeed&quot;, &quot;true&quot;).table(SILVER_TABLE)</code>.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>.whenMatchedUpdate(set={&quot;active_orders&quot;: &quot;t.active_orders + s.d_orders&quot;,
# MAGIC                         &quot;revenue_chf&quot;: &quot;t.revenue_chf + s.___&quot;})
# MAGIC ...
# MAGIC q = (spark.readStream.option(&quot;readChangeFeed&quot;, &quot;___&quot;).table(SILVER_TABLE)
# MAGIC      .writeStream.foreachBatch(apply) ...)</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>def apply(batch_df, batch_id):
# MAGIC     (DeltaTable.forName(batch_df.sparkSession, EX_GOLD).alias(&quot;t&quot;)
# MAGIC      .merge(deltas(batch_df).alias(&quot;s&quot;), &quot;t.restaurant = s.restaurant&quot;)
# MAGIC      .whenMatchedUpdate(set={&quot;active_orders&quot;: &quot;t.active_orders + s.d_orders&quot;,
# MAGIC                              &quot;revenue_chf&quot;: &quot;t.revenue_chf + s.d_revenue&quot;})
# MAGIC      .whenNotMatchedInsert(values={&quot;restaurant&quot;: &quot;s.restaurant&quot;, &quot;active_orders&quot;: &quot;s.d_orders&quot;,
# MAGIC                                    &quot;revenue_chf&quot;: &quot;s.d_revenue&quot;})
# MAGIC      .execute())
# MAGIC 
# MAGIC 
# MAGIC q = (spark.readStream.option(&quot;readChangeFeed&quot;, &quot;true&quot;).table(SILVER_TABLE)
# MAGIC      .writeStream.foreachBatch(apply)
# MAGIC      .option(&quot;checkpointLocation&quot;, f&quot;{STATE}/gold&quot;)
# MAGIC      .trigger(availableNow=True)
# MAGIC      .start())
# MAGIC q.awaitTermination()
# MAGIC display(spark.table(EX_GOLD).orderBy(&quot;restaurant&quot;))</code></pre></details>

# COMMAND ----------

full = {r.restaurant: (r.n, round(r.rev, 2)) for r in spark.table(SILVER_TABLE).groupBy("restaurant").agg(
    F.count_if(F.col("status") != "cancelled").alias("n"),
    F.sum(F.when(F.col("status") != "cancelled", F.col("amount_chf")).otherwise(0)).alias("rev")).collect()}
mine = {r.restaurant: (r.active_orders, round(r.revenue_chf, 2)) for r in spark.table(EX_GOLD).collect()}
print("Correct!" if mine == full else f"Not yet – expected {full}, got {mine}")

# COMMAND ----------

# MAGIC %md
# MAGIC Your stream started without a bookmark, so its first run read silver as it is now, as inserts. If you run the
# MAGIC cell again, it reads 0 rows. Done. More in `3_exercise_extended_change_data_feed`, or go on with notebook 06.
