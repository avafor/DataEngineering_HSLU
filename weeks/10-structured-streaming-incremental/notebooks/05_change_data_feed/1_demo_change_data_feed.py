# Databricks notebook source
# MAGIC %md
# MAGIC # 05 Anna cancels: how does gold notice? (demo)
# MAGIC
# MAGIC Gold answers the restaurant owners' question: **how many active orders, and how much revenue, per
# MAGIC restaurant?** Cancelled orders don't count.
# MAGIC
# MAGIC Friday, 19:09. Anna cancels her pizza (`o-00342`, Pizzeria Bella, 34.50 CHF), and 19 other customers cancel
# MAGIC too. Silver updates their rows to `cancelled`. Gold has to go down by 20 orders and their revenue. The question
# MAGIC for this notebook: **how does gold find out which rows of silver changed?**
# MAGIC
# MAGIC ![The pipeline: this notebook is the step from silver to gold](./assets/pipeline_cdf.png)

# COMMAND ----------

# MAGIC %run ../_config

# COMMAND ----------

# MAGIC %run ../_generator

# COMMAND ----------

# MAGIC %run ../_pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC The pipeline as notebook 04 left it: bronze with batches 1 to 3, silver with 500 orders, no gold yet.

# COMMAND ----------

catch_up(4)

FULL_GOLD = f"{PLAYGROUND}.w10_demo05_gold_full"
PLAIN_COPY = f"{PLAYGROUND}.w10_demo05_plain_stream"
STATE = f"{PRACTICE_VOLUME}/demo_05_state"
for table in [FULL_GOLD, PLAIN_COPY]:
    spark.sql(f"DROP TABLE IF EXISTS {table}")
dbutils.fs.rm(STATE, recurse=True)


def gold_full_recompute():
    # Gold from all of silver, the batch way
    (spark.table(SILVER_TABLE).groupBy("restaurant")
     .agg(F.count_if(F.col("status") != "cancelled").alias("active_orders"),
          F.round(F.sum(F.when(F.col("status") != "cancelled", F.col("amount_chf")).otherwise(0)), 2).alias("revenue_chf"))
     .write.mode("overwrite").saveAsTable(FULL_GOLD))
    print("silver rows read:", spark.table(SILVER_TABLE).count())
    display(spark.table(FULL_GOLD).orderBy("restaurant"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Before Friday evening
# MAGIC
# MAGIC Two ways to build gold that we already know. First the batch way: compute it from all of silver.

# COMMAND ----------

gold_full_recompute()

# COMMAND ----------

# MAGIC %md
# MAGIC And the streaming way from notebook 04: a plain stream on silver, here just copying silver into a table so we
# MAGIC can see what it reads. Through `run_stream_once` again, because in a moment it is going to fail.

# COMMAND ----------

print(run_stream_once(PLAIN_COPY, f"{STATE}/plain", source_table=SILVER_TABLE), "rows read")

# COMMAND ----------

# MAGIC %md
# MAGIC 500 rows, all fine so far. Now Friday evening.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Friday, 19:09
# MAGIC
# MAGIC Batch 4 arrives: 20 cancellations, one of them Anna's, and one event that was sent twice. Bronze and silver run
# MAGIC as in notebooks 01 and 04.

# COMMAND ----------

write_batch(4)
run_bronze()
run_silver()
display(spark.table(SILVER_TABLE).where(f"order_id = '{ANNA_ORDER}'"))

# COMMAND ----------

# MAGIC %md
# MAGIC Anna's row in silver now says `cancelled`. The `MERGE` **updated** it in place. That's the whole difficulty of
# MAGIC this notebook: silver doesn't get new rows for the cancellations, it changes existing ones.
# MAGIC
# MAGIC ## 3. Without the Change Data Feed
# MAGIC
# MAGIC ### The plain stream

# COMMAND ----------

try:
    run_stream_once(PLAIN_COPY, f"{STATE}/plain", source_table=SILVER_TABLE)
except RuntimeError as e:
    print(str(e)[:350])

# COMMAND ----------

# MAGIC %md
# MAGIC A plain Delta stream reads appends. The `MERGE` rewrote files, and the stream refuses to guess what that means.
# MAGIC (`skipChangeCommits` would let it continue, but then it would simply skip the cancellations, so gold would be
# MAGIC wrong. The extended exercise of notebook 04 shows it.)
# MAGIC
# MAGIC ### The full recompute

# COMMAND ----------

gold_full_recompute()

# COMMAND ----------

# MAGIC %md
# MAGIC Correct: Pizzeria Bella has fewer active orders now. But it read all 500 silver rows to process 20 changes. With
# MAGIC a year of orders that's millions of rows, every 10 minutes.
# MAGIC
# MAGIC ### By hand: compare two versions
# MAGIC
# MAGIC Delta keeps old versions (week 8, time travel). So we could compare silver before and after the last `MERGE`:

# COMMAND ----------

history = spark.sql(f"DESCRIBE HISTORY {SILVER_TABLE}").select("version", "operation").orderBy("version")
display(history)
now = history.agg(F.max("version")).first()[0]
before = now - 1

changed_now = spark.sql(f"SELECT * FROM {SILVER_TABLE} EXCEPT SELECT * FROM {SILVER_TABLE} VERSION AS OF {before}")
changed_before = spark.sql(f"SELECT * FROM {SILVER_TABLE} VERSION AS OF {before} EXCEPT SELECT * FROM {SILVER_TABLE}")
print(changed_now.count(), "rows look different now,", changed_before.count(), "rows looked like this before")
display(changed_now.where(f"order_id = '{ANNA_ORDER}'").union(changed_before.where(f"order_id = '{ANNA_ORDER}'")))

# COMMAND ----------

# MAGIC %md
# MAGIC It works: 20 rows before, 20 rows after, Anna's included. Then you'd subtract the «before» rows from gold and
# MAGIC add the «after» rows. But:
# MAGIC
# MAGIC - you have to remember which version you compared last time, a bookmark of your own again
# MAGIC - both `EXCEPT`s read the whole table, twice
# MAGIC - deleted rows only show up in one of the two, new rows only in the other, you have to sort that out
# MAGIC - old versions disappear after a `VACUUM`, and with them your «before»
# MAGIC
# MAGIC Delta can do this for us.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. The Change Data Feed
# MAGIC
# MAGIC With the table property `delta.enableChangeDataFeed = true`, Delta writes down every change to every row:
# MAGIC inserts, deletes, and for updates the row before and after. Our silver table has it on since notebook 04
# MAGIC (`create_silver_table()` in `_pipeline`):

# COMMAND ----------

display(spark.sql(f"SHOW TBLPROPERTIES {SILVER_TABLE}").where("key = 'delta.enableChangeDataFeed'"))

# COMMAND ----------

# MAGIC %md
# MAGIC `table_changes(table, from_version)` shows what it recorded. Anna's order, from the start:

# COMMAND ----------

display(spark.sql(f"SELECT _change_type, _commit_version, order_id, status, amount_chf FROM table_changes('{SILVER_TABLE}', 0) "
                  f"WHERE order_id = '{ANNA_ORDER}' ORDER BY _commit_version, _change_type"))

# COMMAND ----------

# MAGIC %md
# MAGIC ![Anna's order in the Change Data Feed: insert when it was placed, then update_preimage with status placed and update_postimage with status cancelled](./assets/cdf_anna.png)
# MAGIC
# MAGIC | `_change_type` | means |
# MAGIC |---|---|
# MAGIC | `insert` | a new row |
# MAGIC | `update_preimage` | the row as it was **before** an update |
# MAGIC | `update_postimage` | the row as it is **after** the update |
# MAGIC | `delete` | the row was deleted |
# MAGIC
# MAGIC Only the rows that changed are in there. The last `MERGE` produced exactly these:

# COMMAND ----------

display(spark.sql(f"SELECT _change_type, count(*) AS rows FROM table_changes('{SILVER_TABLE}', {now}) GROUP BY _change_type"))

# COMMAND ----------

# MAGIC %md
# MAGIC 20 rows before, 20 after. The duplicated event from batch 4 isn't there: the `MERGE` didn't change anything for
# MAGIC it (notebook 04, «only if newer»).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Gold from the changes
# MAGIC
# MAGIC Now gold doesn't need to look at all of silver. It reads the Change Data Feed as a stream
# MAGIC (`readStream.option("readChangeFeed", "true").table(silver)`), and turns every change row into a small plus or
# MAGIC minus:
# MAGIC
# MAGIC ![The change rows of Anna's order: update_preimage placed counts minus one order and minus 34.50, update_postimage cancelled counts zero; summed per restaurant and merged into gold](./assets/cdf_gold_deltas.png)
# MAGIC
# MAGIC - `insert` and `update_postimage` add (+1), `update_preimage` and `delete` subtract (−1)
# MAGIC - a cancelled order counts 0
# MAGIC
# MAGIC So Anna's old row comes off (−1 order, −34.50) and her new row adds nothing. Summed per restaurant, then a
# MAGIC `MERGE` into gold adds the result to what's there:

# COMMAND ----------

def apply_changes(batch_df, batch_id):
    sign = (F.when(F.col("_change_type").isin("insert", "update_postimage"), 1)
             .when(F.col("_change_type").isin("update_preimage", "delete"), -1)
             .otherwise(0))
    counts = F.col("status") != "cancelled"
    deltas = (batch_df.groupBy("restaurant").agg(
        F.sum(F.when(counts, sign).otherwise(0)).alias("d_orders"),
        F.sum(F.when(counts, sign * F.col("amount_chf")).otherwise(0)).alias("d_revenue")))
    (DeltaTable.forName(batch_df.sparkSession, GOLD_TABLE).alias("t")
     .merge(deltas.alias("s"), "t.restaurant = s.restaurant")
     .whenMatchedUpdate(set={"active_orders": "t.active_orders + s.d_orders",
                             "revenue_chf": "round(t.revenue_chf + s.d_revenue, 2)",
                             "updated_at": "current_timestamp()"})
     .whenNotMatchedInsert(values={"restaurant": "s.restaurant", "active_orders": "s.d_orders",
                                   "revenue_chf": "round(s.d_revenue, 2)", "updated_at": "current_timestamp()"})
     .execute())


def run_gold_demo():
    create_gold_table()
    query = (spark.readStream.option("readChangeFeed", "true").table(SILVER_TABLE)
             .writeStream.foreachBatch(apply_changes)
             .option("checkpointLocation", GOLD_CHECKPOINT)
             .trigger(availableNow=True)
             .start())
    query.awaitTermination()
    print(f"{input_rows(query)} change rows read")


run_gold_demo()
display(spark.table(GOLD_TABLE).orderBy("restaurant"))

# COMMAND ----------

# MAGIC %md
# MAGIC The first run of a new gold query has no bookmark yet. It starts with the current content of silver, delivered
# MAGIC as inserts: 500 rows, Anna's already cancelled. That's the backfill, for free. Is gold right? Compare it with a
# MAGIC full recompute, every difference should be 0:

# COMMAND ----------

display(gold_diffs())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. The next change
# MAGIC
# MAGIC Saturday: batch 5 arrives, 59 new orders and 40 deliveries. Through the whole pipeline:

# COMMAND ----------

write_batch(5)
run_bronze()
run_silver()
run_gold_demo()
display(gold_diffs())

# COMMAND ----------

# MAGIC %md
# MAGIC This time gold read **139 change rows**: 59 inserts for the new orders, and a before and an after for each of the
# MAGIC 40 deliveries. Not 559 rows of silver. And the diffs are still 0. The work now grows with the number of
# MAGIC changes, not with the size of the table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Under the hood
# MAGIC
# MAGIC The gold query's bookmark is a version of silver, like in notebook 04:

# COMMAND ----------

offset_files = sorted(dbutils.fs.ls(f"{GOLD_CHECKPOINT}/offsets"), key=lambda f: int(f.name))
print(dbutils.fs.head(offset_files[-1].path).splitlines()[-1])
display(spark.sql(f"DESCRIBE HISTORY {SILVER_TABLE}").select("version", "operation").orderBy("version"))

# COMMAND ----------

# MAGIC %md
# MAGIC The changes themselves are stored next to the table's data files, in a folder `_change_data`, and only for
# MAGIC commits that update or delete (inserts can be read from the data files directly). They cost some storage, and
# MAGIC they are cleaned up with the table's old files by `VACUUM`. A consumer that is switched off for longer than that
# MAGIC can't catch up from the feed anymore, only with a full recompute.
# MAGIC
# MAGIC Two things to keep in mind about our gold logic:
# MAGIC
# MAGIC - `foreachBatch` is **at least once**. If a run dies right after the `MERGE` and the batch is processed again,
# MAGIC   gold adds the same deltas twice. Our silver `MERGE` was safe to repeat, this one isn't. The extended exercise
# MAGIC   fixes it.
# MAGIC - That's why the check against a full recompute (`gold_diffs()`) belongs into the pipeline, not only into this
# MAGIC   notebook. Notebook 08 makes the job fail when it isn't 0.

# COMMAND ----------

# MAGIC %md
# MAGIC ## What you saw
# MAGIC
# MAGIC - Silver gets updates, not appends. A plain stream stops at the first one.
# MAGIC - A full recompute is right and expensive. Comparing versions by hand works and is fragile.
# MAGIC - The Change Data Feed records every change of every row: insert, delete, before and after of an update.
# MAGIC   `table_changes` shows it, `readChangeFeed` streams it.
# MAGIC - Gold turns changes into plus and minus per restaurant and `MERGE`s them. Only the changes are read.
# MAGIC
# MAGIC The whole pipeline runs now: files → bronze → silver → gold. But the events took a detour through files. The app
# MAGIC sends them to Kafka anyway, so why not read them from there? That's notebook 06. First:
# MAGIC `2_exercise_short_change_data_feed`.
