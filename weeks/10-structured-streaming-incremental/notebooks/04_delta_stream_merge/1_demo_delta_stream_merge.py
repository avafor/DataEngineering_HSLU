# Databricks notebook source
# MAGIC %md
# MAGIC # 04 From events to one row per order (demo)
# MAGIC
# MAGIC `bronze.orders` keeps every event as it arrived: an order shows up as `placed`, later again as `delivered`.
# MAGIC That's right for bronze. The restaurant dashboard wants something else: **one row per order, with its
# MAGIC latest status**. That's `silver.orders`.
# MAGIC
# MAGIC The question for this notebook: how do we keep silver up to date while bronze keeps growing, without
# MAGIC reading all of bronze every time?
# MAGIC
# MAGIC ![The pipeline: this notebook is the step from bronze to silver](./assets/pipeline_delta.png)

# COMMAND ----------

# MAGIC %run ../_config

# COMMAND ----------

# MAGIC %run ../_generator

# COMMAND ----------

# MAGIC %run ../_pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC The pipeline as notebook 03 left it: batches 1 to 3 in bronze, 801 rows. Takes about a minute.

# COMMAND ----------

catch_up(3)

FULL_TABLE = f"{PLAYGROUND}.w10_demo04_full_recompute"
APPEND_TABLE = f"{PLAYGROUND}.w10_demo04_append"
NAIVE_MERGE_TABLE = f"{PLAYGROUND}.w10_demo04_naive_merge"
STATE = f"{PRACTICE_VOLUME}/demo_04_state"
for row in spark.sql(f"SHOW TABLES IN {PLAYGROUND} LIKE 'w10_demo04_*'").collect():
    spark.sql(f"DROP TABLE IF EXISTS {PLAYGROUND}.{row.tableName}")
dbutils.fs.rm(STATE, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. One order, several rows
# MAGIC
# MAGIC Order `o-00151` was placed on Monday and delivered on Wednesday. In bronze that's two rows:

# COMMAND ----------

display(spark.table(BRONZE_TABLE).where("order_id = 'o-00151'")
        .select("order_id", "status", "event_ts", "amount_chf", "tip_chf", "_source_file"))

# COMMAND ----------

# MAGIC %md
# MAGIC In silver we want one row: `o-00151`, delivered, with the tip.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Without streaming: compute silver from all of bronze
# MAGIC
# MAGIC Take all of bronze, keep the newest event per order, overwrite silver. The «newest event per order» is a
# MAGIC window function: number the events of each order from newest to oldest, keep number 1.

# COMMAND ----------

from pyspark.sql import Window

newest_first = Window.partitionBy("order_id").orderBy(F.col("event_ts").desc())


def latest_per_order(df):
    return (df.withColumn("_rn", F.row_number().over(newest_first))
              .where("_rn = 1").drop("_rn"))


(latest_per_order(spark.table(BRONZE_TABLE).where("_rescued_data IS NULL"))
 .write.mode("overwrite").saveAsTable(FULL_TABLE))
print(spark.table(FULL_TABLE).count(), "orders, read", spark.table(BRONZE_TABLE).count(), "bronze rows for it")
display(spark.table(FULL_TABLE).groupBy("status").count())

# COMMAND ----------

# MAGIC %md
# MAGIC Correct: 500 orders, 300 delivered, 200 still on their way. (`o-00501`, the one with `"12,50"`, stays out:
# MAGIC its amount is broken. It's still in bronze.) But it's notebook 01 all over again, one layer up: every run
# MAGIC reads all of bronze, and all of silver is rewritten, to change a few rows.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. A Delta table is a stream, too
# MAGIC
# MAGIC Every write to a Delta table is a **commit**: a new version in its transaction log (week 8). Bronze got one
# MAGIC per Auto Loader run:

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {BRONZE_TABLE}")
        .selectExpr("version", "timestamp", "operation", "operationMetrics['numOutputRows'] AS rows_written")
        .orderBy("version"))

# COMMAND ----------

# MAGIC %md
# MAGIC So a Delta table can be a streaming source: `spark.readStream.table(...)`. The checkpoint remembers the
# MAGIC last version it read, and the next run reads only the commits after it.
# MAGIC
# MAGIC ![The Delta log of bronze with commits v0 to v4. The checkpoint of the silver query is at v2, the next run reads only v3 and v4. An UPDATE as v5 would stop a plain stream](./assets/delta_stream_log.png)
# MAGIC
# MAGIC Let's try the obvious: stream bronze into a table, append.

# COMMAND ----------

q = (spark.readStream.table(BRONZE_TABLE)
     .writeStream.option("checkpointLocation", f"{STATE}/append")
     .trigger(availableNow=True)
     .toTable(APPEND_TABLE))
q.awaitTermination()
print(input_rows(q), "rows read")
display(spark.table(APPEND_TABLE).where("order_id = 'o-00151'").select("order_id", "status", "event_ts"))

# COMMAND ----------

# MAGIC %md
# MAGIC Incremental, yes. But it's a copy of bronze: `o-00151` twice. `append` can only add rows. To get *one row
# MAGIC per order*, an existing row has to be **updated** when a newer event comes in. That's a `MERGE`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. `foreachBatch`: a MERGE per micro-batch
# MAGIC
# MAGIC A streaming query can't `MERGE` by itself. But it can hand every micro-batch to a function of ours, as a
# MAGIC normal DataFrame: `foreachBatch`. Inside, we can do whatever works in batch, including `MERGE`.
# MAGIC
# MAGIC First, the obvious version: MERGE the micro-batch as it is.

# COMMAND ----------

from delta.tables import DeltaTable

spark.sql(f"CREATE TABLE {NAIVE_MERGE_TABLE} (order_id STRING, status STRING, event_ts STRING)")


def merge_as_is(batch_df, batch_id):
    (DeltaTable.forName(batch_df.sparkSession, NAIVE_MERGE_TABLE).alias("t")
     .merge(batch_df.select("order_id", "status", "event_ts").alias("s"), "t.order_id = s.order_id")
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute())


q = (spark.readStream.table(BRONZE_TABLE)
     .writeStream.foreachBatch(merge_as_is)
     .option("checkpointLocation", f"{STATE}/naive_merge")
     .trigger(availableNow=True)
     .start())
q.awaitTermination()
print(spark.table(NAIVE_MERGE_TABLE).count(), "rows")
display(spark.table(NAIVE_MERGE_TABLE).where("order_id = 'o-00151'"))

# COMMAND ----------

# MAGIC %md
# MAGIC Still two rows for `o-00151`. The micro-batch contained both events. Silver was empty, so neither matched,
# MAGIC and both were inserted. And the next time such a batch comes in, it gets worse. Let's feed the same rows
# MAGIC once more, the way a replay after a crash would:

# COMMAND ----------

try:
    merge_as_is(spark.table(BRONZE_TABLE), batch_id=999)
except Exception as e:
    message = str(e)
    print(message[message.find("[DELTA"):][:300] if "[DELTA" in message else message[:300])

# COMMAND ----------

# MAGIC %md
# MAGIC `MERGE` refuses when two incoming rows want to change the same row: which one should win? It can't know. So
# MAGIC we have to decide **before** the MERGE, inside the micro-batch: keep only the newest event per order.
# MAGIC That's our window function from part 2, now on the micro-batch instead of all of bronze.
# MAGIC
# MAGIC ![A micro-batch with several events per order. Step 1 keeps the newest event per order, step 2 MERGEs into silver: newer events update, new orders are inserted. Without step 1 the MERGE fails](./assets/silver_dedupe_merge.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. The real thing: bronze → silver
# MAGIC
# MAGIC Here's the function the pipeline uses. Three steps per micro-batch:
# MAGIC
# MAGIC 1. drop rows we can't use (`_rescued_data`), bring the columns into shape
# MAGIC 2. keep the newest event per order
# MAGIC 3. `MERGE` into silver. Update only if the incoming event is **newer** than what silver has
# MAGIC
# MAGIC The condition in step 3 is the safety net. If a batch is ever processed twice, or an old event shows up
# MAGIC late, it can't overwrite a newer status.

# COMMAND ----------

def upsert_latest(batch_df, batch_id):
    clean = (batch_df.where("_rescued_data IS NULL")                                  # 1
             .select("order_id", "customer", "restaurant", "city",
                     F.col("items").cast("int").alias("items"),
                     F.col("amount_chf").cast("double").alias("amount_chf"),
                     F.col("tip_chf").cast("double").alias("tip_chf"),
                     "status",
                     F.to_timestamp("event_ts").alias("event_ts")))
    latest = latest_per_order(clean).withColumn("updated_at", F.current_timestamp())   # 2
    (DeltaTable.forName(batch_df.sparkSession, SILVER_TABLE).alias("t")               # 3
     .merge(latest.alias("s"), "t.order_id = s.order_id")
     .whenMatchedUpdateAll(condition="s.event_ts > t.event_ts")
     .whenNotMatchedInsertAll()
     .execute())


create_silver_table()     # from _pipeline: the silver columns, with the Change Data Feed on (notebook 05)


def run_silver_demo():
    query = (spark.readStream.table(BRONZE_TABLE)
             .writeStream.foreachBatch(upsert_latest)
             .option("checkpointLocation", SILVER_CHECKPOINT)
             .trigger(availableNow=True)
             .start())
    query.awaitTermination()
    print(f"{input_rows(query)} bronze rows read")


run_silver_demo()
display(spark.table(SILVER_TABLE).groupBy("status").count())
display(spark.table(SILVER_TABLE).where("order_id = 'o-00151'"))

# COMMAND ----------

# MAGIC %md
# MAGIC 500 orders, 300 delivered, `o-00151` once, delivered, with its tip. The same result as the full recompute in
# MAGIC part 2. A second run, nothing new in bronze:

# COMMAND ----------

run_silver_demo()

# COMMAND ----------

# MAGIC %md
# MAGIC 0 rows read. And the replay that broke the naive MERGE? Feed all of bronze through the function once more:

# COMMAND ----------

before = {r.status: r["count"] for r in spark.table(SILVER_TABLE).groupBy("status").count().collect()}
upsert_latest(spark.table(BRONZE_TABLE), batch_id=999)
after = {r.status: r["count"] for r in spark.table(SILVER_TABLE).groupBy("status").count().collect()}
print("before:", before)
print("after: ", after)

# COMMAND ----------

# MAGIC %md
# MAGIC Nothing changed. Duplicates are removed in step 2, and step 3 only updates when something newer comes in. A
# MAGIC `MERGE` like this can run twice without harm. That matters, because `foreachBatch` gives you **at least
# MAGIC once**: after a crash, Spark may hand you the same micro-batch again.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Under the hood
# MAGIC
# MAGIC The offset in the silver checkpoint is a version of bronze:

# COMMAND ----------

offset_files = sorted(dbutils.fs.ls(f"{SILVER_CHECKPOINT}/offsets"), key=lambda f: int(f.name))
print(dbutils.fs.head(offset_files[-1].path).splitlines()[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC `reservoirVersion` is the bronze version the next run will start at. And silver's own history shows the
# MAGIC `MERGE` commits:

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {SILVER_TABLE}")
        .selectExpr("version", "operation", "operationMetrics['numTargetRowsInserted'] AS inserted",
                    "operationMetrics['numTargetRowsUpdated'] AS updated")
        .orderBy("version"))

# COMMAND ----------

# MAGIC %md
# MAGIC One catch, and it leads straight to the next notebook. A plain stream on a Delta table can only read
# MAGIC **appends**. Bronze only ever gets appends, so this works. Silver gets `MERGE`s, which update rows in place.
# MAGIC Try to stream from silver the same way, and the query stops at the first update. The extended exercise
# MAGIC shows it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## What you saw
# MAGIC
# MAGIC - Recomputing silver from all of bronze is correct, and reads everything every time.
# MAGIC - A Delta table is a streaming source: `readStream.table(...)` reads only the new commits.
# MAGIC - `append` can't turn events into one row per order. `foreachBatch` + `MERGE` can.
# MAGIC - Inside the batch: first keep the newest event per order, then `MERGE` with «only if newer». Then a replay
# MAGIC   or a duplicate does no harm.
# MAGIC
# MAGIC Silver now has one row per order. Friday, 19:09: Anna cancels her pizza. Silver will update her row, and the
# MAGIC revenue in gold has to go down. How does gold notice an update, if a stream can only read appends? That's
# MAGIC notebook 05. First: `2_exercise_short_delta_stream_merge`.
