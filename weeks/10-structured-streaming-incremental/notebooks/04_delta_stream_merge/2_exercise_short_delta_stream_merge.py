# Databricks notebook source
# MAGIC %md
# MAGIC # 04 From events to one row per order (exercise, short)
# MAGIC
# MAGIC About 10 minutes. You build your own silver table in the playground: newest event per order, then a
# MAGIC streaming `MERGE` from bronze, then a replay to prove it's safe. Bronze is only read, not changed.

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

# This exercise reads bronze with batches 1 to 3 (801 rows). If it isn't there, rebuild it.
if not spark.catalog.tableExists(BRONZE_TABLE) or spark.table(BRONZE_TABLE).count() != 801:
    catch_up(3)

EX_SILVER = f"{PLAYGROUND}.w10_ex04_silver"
STATE = f"{PRACTICE_VOLUME}/ex04_short_state"
spark.sql(f"DROP TABLE IF EXISTS {EX_SILVER}")
dbutils.fs.rm(STATE, recurse=True)
spark.sql(f"""
    CREATE TABLE {EX_SILVER} (
        order_id STRING, restaurant STRING, amount_chf DOUBLE, status STRING, event_ts TIMESTAMP
    )
""")


def shape(df):
    # The columns of our small silver, with event_ts as a real timestamp
    return df.select("order_id", "restaurant", "amount_chf", "status", F.to_timestamp("event_ts").alias("event_ts"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4.1 – The newest event per order ★
# MAGIC
# MAGIC Write `newest(df)`: from `df`, drop rows with something in `_rescued_data`, then keep for each `order_id` only
# MAGIC the row with the latest `event_ts` (window + `row_number`, like in the demo). Return it through `shape(...)`.
# MAGIC The check runs it on all of bronze.

# COMMAND ----------

# TODO Task 4.1
def newest(df):
    w = Window.partitionBy("___").orderBy(F.col("___").desc())
    return shape(df.where("_rescued_data IS NULL")
                 .withColumn("_rn", F.row_number().over(w))
                 .where("_rn = ___").drop("_rn"))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>A window partitioned by <code>order_id</code>, ordered by <code>event_ts</code> descending. <code>row_number()</code> over it, keep 1.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>w = Window.partitionBy(&quot;order_id&quot;).orderBy(F.col(&quot;event_ts&quot;).___())
# MAGIC ... .withColumn(&quot;_rn&quot;, F.row_number().over(w)).where(&quot;_rn = ___&quot;).drop(&quot;_rn&quot;)</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>def newest(df):
# MAGIC     w = Window.partitionBy(&quot;order_id&quot;).orderBy(F.col(&quot;event_ts&quot;).desc())
# MAGIC     return shape(df.where(&quot;_rescued_data IS NULL&quot;)
# MAGIC                  .withColumn(&quot;_rn&quot;, F.row_number().over(w))
# MAGIC                  .where(&quot;_rn = 1&quot;).drop(&quot;_rn&quot;))</code></pre></details>

# COMMAND ----------

check = newest(spark.table(BRONZE_TABLE))
n, n_ids = check.count(), check.select("order_id").distinct().count()
print("Correct!" if (n, n_ids) == (500, 500) else f"Not yet – expected 500 rows for 500 orders, got {n} rows for {n_ids} orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4.2 – The streaming MERGE ★★
# MAGIC
# MAGIC Complete `upsert(batch_df, batch_id)`: take `newest(batch_df)` and `MERGE` it into `EX_SILVER` on `order_id`.
# MAGIC Update a matched row **only if the incoming event is newer**, insert new orders. Then stream bronze through
# MAGIC it with `foreachBatch`, checkpoint `f"{STATE}/silver"`, `availableNow`.

# COMMAND ----------

# TODO Task 4.2
def upsert(batch_df, batch_id):
    (DeltaTable.forName(batch_df.sparkSession, EX_SILVER).alias("t")
     .merge(newest(batch_df).alias("s"), "t.order_id = s.order_id")
     .whenMatchedUpdateAll(condition="___")
     .whenNotMatchedInsertAll()
     .execute())


q = (spark.readStream.table(BRONZE_TABLE)
     .writeStream.foreachBatch(___)
     .option("checkpointLocation", f"{STATE}/silver")
     .trigger(availableNow=True)
     .start())
q.awaitTermination()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>In the MERGE, <code>s</code> is the incoming row and <code>t</code> the row in silver. Update only if <code>s.event_ts</code> is later than <code>t.event_ts</code>. Then <code>foreachBatch(upsert)</code>.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>.whenMatchedUpdateAll(condition=&quot;s.event_ts ___ t.event_ts&quot;)
# MAGIC ...
# MAGIC .writeStream.foreachBatch(___)</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>def upsert(batch_df, batch_id):
# MAGIC     (DeltaTable.forName(batch_df.sparkSession, EX_SILVER).alias(&quot;t&quot;)
# MAGIC      .merge(newest(batch_df).alias(&quot;s&quot;), &quot;t.order_id = s.order_id&quot;)
# MAGIC      .whenMatchedUpdateAll(condition=&quot;s.event_ts &gt; t.event_ts&quot;)
# MAGIC      .whenNotMatchedInsertAll()
# MAGIC      .execute())
# MAGIC 
# MAGIC 
# MAGIC q = (spark.readStream.table(BRONZE_TABLE)
# MAGIC      .writeStream.foreachBatch(upsert)
# MAGIC      .option(&quot;checkpointLocation&quot;, f&quot;{STATE}/silver&quot;)
# MAGIC      .trigger(availableNow=True)
# MAGIC      .start())
# MAGIC q.awaitTermination()</code></pre></details>

# COMMAND ----------

counts = {r.status: r["count"] for r in spark.table(EX_SILVER).groupBy("status").count().collect()}
print("Correct!" if counts == {"placed": 200, "delivered": 300} else f"Not yet – expected 200 placed and 300 delivered, got {counts}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4.3 – Replay ★
# MAGIC
# MAGIC Pretend Spark hands you an old micro-batch again: call `upsert` yourself with **only Monday's rows** from
# MAGIC bronze (`_source_file` ends with `orders_batch_1.json`) and any `batch_id`. Then store the number of delivered
# MAGIC orders in `EX_SILVER` in `n_delivered`.
# MAGIC
# MAGIC Before you run it: Monday's rows are all `placed`. Will some orders go back from delivered to placed?

# COMMAND ----------

# TODO Task 4.3
raise NotImplementedError("Task 4.3")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>Call <code>upsert</code> like Spark would, with a DataFrame and a number: bronze filtered on <code>_source_file</code>.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>upsert(spark.table(BRONZE_TABLE).where(&quot;_source_file LIKE &#x27;%___&#x27;&quot;), batch_id=999)</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>upsert(spark.table(BRONZE_TABLE).where(&quot;_source_file LIKE &#x27;%orders_batch_1.json&#x27;&quot;), batch_id=999)
# MAGIC n_delivered = spark.table(EX_SILVER).where(&quot;status = &#x27;delivered&#x27;&quot;).count()
# MAGIC print(n_delivered)</code></pre></details>

# COMMAND ----------

print("Correct!" if n_delivered == 300 else f"Not yet – expected 300, got {n_delivered}")

# COMMAND ----------

# MAGIC %md
# MAGIC Still 300: the old events are older than what silver has, so the condition skips them. The extended exercise
# MAGIC shows what happens without it.
# MAGIC
# MAGIC Done. More in `3_exercise_extended_delta_stream_merge`, or go on with notebook 05.
