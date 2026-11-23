# Databricks notebook source
# MAGIC %md
# MAGIC # 02 The streaming engine behind Auto Loader (demo)
# MAGIC
# MAGIC In notebook 01, Auto Loader was a `readStream` with a checkpoint and `trigger(availableNow=True)`. We used
# MAGIC it like a batch job that remembers what it has read. That's half of the story. Spark's streaming engine,
# MAGIC **Structured Streaming**, can do more than remember files: it can keep a running result up to date.
# MAGIC
# MAGIC The question for this notebook: the restaurant dashboard wants to know **how many orders are in each
# MAGIC status**, always up to date. New files keep arriving. How do we keep that number correct without
# MAGIC counting everything again every time?
# MAGIC
# MAGIC This notebook doesn't touch the pipeline. Everything happens in the playground.

# COMMAND ----------

# MAGIC %run ../_config

# COMMAND ----------

# MAGIC %run ../_generator

# COMMAND ----------

import json

from delta.tables import DeltaTable
from pyspark.sql import functions as F

DEMO_DIR = f"{PRACTICE_VOLUME}/demo_02"
STATE = f"{PRACTICE_VOLUME}/demo_02_state"
FULL_TABLE = f"{PLAYGROUND}.w10_demo02_counts_full"
HAND_TABLE = f"{PLAYGROUND}.w10_demo02_counts_by_hand"
STREAM_TABLE = f"{PLAYGROUND}.w10_demo02_counts_stream"

for row in spark.sql(f"SHOW TABLES IN {PLAYGROUND} LIKE 'w10_demo02_*'").collect():
    spark.sql(f"DROP TABLE IF EXISTS {PLAYGROUND}.{row.tableName}")
for folder in [DEMO_DIR, STATE]:
    dbutils.fs.rm(folder, recurse=True)
dbutils.fs.mkdirs(DEMO_DIR)

# A plain file source needs the schema up front (Auto Loader inferred it for us)
ORDER_SCHEMA = ("order_id STRING, customer STRING, restaurant STRING, city STRING, items INT, "
                "amount_chf DOUBLE, status STRING, event_ts STRING")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Without streaming: count everything, every time
# MAGIC
# MAGIC Monday's file is there. Count per status, overwrite the result table.

# COMMAND ----------

write_batch(1, DEMO_DIR)


def count_everything():
    (spark.read.schema(ORDER_SCHEMA).json(DEMO_DIR)
     .groupBy("status").count()
     .write.mode("overwrite").saveAsTable(FULL_TABLE))
    print("files read:", len(dbutils.fs.ls(DEMO_DIR)))
    display(spark.table(FULL_TABLE))


count_everything()

# COMMAND ----------

write_batch(2, DEMO_DIR)
count_everything()

# COMMAND ----------

# MAGIC %md
# MAGIC Correct, and simple. But it's notebook 01 again: every run reads every file, to add the few rows that
# MAGIC are new. For a count that's annoying. For a sum over a year of orders it's expensive.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Without streaming, but clever: add up the new rows yourself
# MAGIC
# MAGIC We know from notebook 01 how to find the new files. So: count only the new file, and add the result to
# MAGIC the counts we already have. That's a `MERGE`: existing status → add, new status → insert.

# COMMAND ----------

spark.sql(f"CREATE TABLE {HAND_TABLE} (status STRING, count BIGINT)")


def add_counts_by_hand(new_file):
    new_counts = spark.read.schema(ORDER_SCHEMA).json(new_file).groupBy("status").count()
    (DeltaTable.forName(spark, HAND_TABLE).alias("t")
     .merge(new_counts.alias("s"), "t.status = s.status")
     .whenMatchedUpdate(set={"count": "t.count + s.count"})
     .whenNotMatchedInsertAll()
     .execute())
    display(spark.table(HAND_TABLE))


add_counts_by_hand(f"{DEMO_DIR}/orders_batch_1.json")
add_counts_by_hand(f"{DEMO_DIR}/orders_batch_2.json")

# COMMAND ----------

# MAGIC %md
# MAGIC Same numbers, and each run only read one file. Now we are responsible for two things:
# MAGIC
# MAGIC - **which files are done** (notebook 01, with all its traps), and
# MAGIC - **the running result**, the counts so far. That's called *state*.
# MAGIC
# MAGIC And the state is fragile. Add the same file twice by mistake, and the counts are wrong forever:

# COMMAND ----------

add_counts_by_hand(f"{DEMO_DIR}/orders_batch_2.json")   # oops, ran twice

# COMMAND ----------

# MAGIC %md
# MAGIC 300 delivered, but there are only 150. To repair it you have to count everything again, which is what we
# MAGIC wanted to avoid. A sum is still easy. An average, a distinct count or «orders per 15 minutes» by hand is
# MAGIC where it gets really messy.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Structured Streaming: write the batch query, Spark keeps the state
# MAGIC
# MAGIC Here's the idea of Structured Streaming. You don't think of the data as files that arrive. You think of
# MAGIC it as **one table that keeps growing**. New files are new rows at the bottom of that table.
# MAGIC
# MAGIC ![A data stream as an unbounded table: every new piece of data is a new row appended to the table](./assets/spark_stream_as_a_table.png)
# MAGIC
# MAGIC *Image: [Apache Spark, Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/streaming/index.html)*
# MAGIC
# MAGIC You write your query as if it were a normal batch query on that table: `groupBy("status").count()`. Spark
# MAGIC runs it **incrementally**: each run reads only the new rows and updates the result. The counts so far are
# MAGIC kept in the checkpoint. That's the state from part 2, but Spark looks after it.
# MAGIC
# MAGIC Same files, one more time, in a fresh folder so we can watch them arrive:

# COMMAND ----------

STREAM_DIR = f"{DEMO_DIR}/stream"
write_batch(1, STREAM_DIR)


def run_counts():
    counts = (
        spark.readStream                       # streaming instead of read
        .schema(ORDER_SCHEMA)
        .json(STREAM_DIR)                      # source: a plain folder of JSON files
        .groupBy("status").count()             # the same query as in part 1
    )
    query = (
        counts.writeStream
        .outputMode("complete")                # write the whole result table every time (more on that below)
        .option("checkpointLocation", f"{STATE}/counts")
        .trigger(availableNow=True)
        .toTable(STREAM_TABLE)
    )
    query.awaitTermination()
    print(f"rows read in this run: {input_rows(query)}")
    display(spark.table(STREAM_TABLE))
    return query


run_counts()

# COMMAND ----------

write_batch(2, STREAM_DIR)
q = run_counts()

# COMMAND ----------

# MAGIC %md
# MAGIC This run read **350 rows**, only the new file, and still the result covers both files: 400 placed, 150
# MAGIC delivered. Spark took the counts from the last run out of the checkpoint and added the new ones. Exactly
# MAGIC what we did by hand in part 2, without the code.
# MAGIC
# MAGIC And the trap from part 2? Run the cell once more. 0 rows read, counts unchanged. A file is never counted
# MAGIC twice, because the list of files and the state live in the same checkpoint.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. How a run works: micro-batches
# MAGIC
# MAGIC Spark doesn't process row by row. Each run collects what's new into a **micro-batch**, runs the query on
# MAGIC it, updates the result, and writes down how far it got.
# MAGIC
# MAGIC ![Every trigger: the new rows of the input table are processed, the result table is updated and written to the output](./assets/spark_model.png)
# MAGIC
# MAGIC *Image: [Apache Spark, Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/streaming/index.html)*
# MAGIC
# MAGIC Every streaming query has the same five parts. Look at `run_counts()` again:
# MAGIC
# MAGIC | Part | In our code | Could also be |
# MAGIC |---|---|---|
# MAGIC | source | `readStream.schema(...).json(folder)` | Auto Loader (`cloudFiles`), a Delta table, Kafka |
# MAGIC | transformation | `groupBy("status").count()` | anything you'd do on a DataFrame: filter, join, window |
# MAGIC | sink | `toTable(...)` | a Delta table, Kafka, or your own function (`foreachBatch`, notebook 04) |
# MAGIC | checkpoint | `checkpointLocation` | always a folder of its own, on a volume |
# MAGIC | trigger | `availableNow=True` | see below |
# MAGIC
# MAGIC Auto Loader in notebook 01 was the same thing with a different source.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Triggers: when does the next micro-batch run?
# MAGIC
# MAGIC | Trigger | What it does |
# MAGIC |---|---|
# MAGIC | none (default) | the query keeps running, a new micro-batch as soon as the last one is done |
# MAGIC | `processingTime="1 minute"` | keeps running, a micro-batch every minute |
# MAGIC | `availableNow=True` | reads everything that is there now, in one or more micro-batches, then **stops** |
# MAGIC
# MAGIC The first two are «real» streaming: a query that never ends and needs a cluster that never stops. On
# MAGIC Free Edition that isn't allowed. Try it:

# COMMAND ----------

try:
    (spark.readStream.schema(ORDER_SCHEMA).json(STREAM_DIR)
     .writeStream.option("checkpointLocation", f"{STATE}/forever")
     .trigger(processingTime="1 minute")
     .toTable(f"{PLAYGROUND}.w10_demo02_forever"))
except Exception as e:
    print(str(e).splitlines()[0][:300])

# COMMAND ----------

# MAGIC %md
# MAGIC That's fine for us, and honestly for many real pipelines too. `availableNow` started by a job every
# MAGIC 10 minutes (notebook 08) is cheaper than a cluster that runs all day, and the result is the same: every
# MAGIC row is processed once. You only need a query that runs forever when seconds matter.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Output modes: what gets written?
# MAGIC
# MAGIC We used `outputMode("complete")`. Why not the default, `append`?

# COMMAND ----------

try:
    (spark.readStream.schema(ORDER_SCHEMA).json(STREAM_DIR)
     .groupBy("status").count()
     .writeStream.outputMode("append")
     .option("checkpointLocation", f"{STATE}/append")
     .trigger(availableNow=True)
     .toTable(f"{PLAYGROUND}.w10_demo02_append"))
except Exception as e:
    print(str(e).splitlines()[0][:300])

# COMMAND ----------

# MAGIC %md
# MAGIC `append` means: a row, once written, never changes. But the count for «placed» changes with every new
# MAGIC file. Spark can't append it, because it would be wrong a minute later. So:
# MAGIC
# MAGIC | Output mode | Writes | Good for |
# MAGIC |---|---|---|
# MAGIC | `append` | only new rows, never changed again | events as they are, e.g. files into bronze |
# MAGIC | `complete` | the whole result table, every run | small aggregates like our counts |
# MAGIC | `update` | only the result rows that changed | bigger aggregates, usually with `foreachBatch` |
# MAGIC
# MAGIC Bronze in notebook 01 used `append`, the default, because every order event is final once it arrived.
# MAGIC Aggregations with a time window can use `append` too, once Spark knows a window is closed. That's
# MAGIC notebook 07.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Under the hood: the checkpoint of a query with state
# MAGIC
# MAGIC Our count query has a checkpoint like Auto Loader's, plus one more folder: `state/`.

# COMMAND ----------

display(dbutils.fs.ls(f"{STATE}/counts"))

# COMMAND ----------

# MAGIC %md
# MAGIC ![The checkpoint folder with metadata, offsets, commits, sources and state; each batch writes offsets first, then the result, then the commit. After a crash Spark reruns the batch that has offsets but no commit](./assets/checkpoint_folder.png)
# MAGIC
# MAGIC - `offsets/` and `commits/`: the plan of each micro-batch and its «done», like in notebook 01
# MAGIC - `sources/`: for a file source, the list of files already read
# MAGIC - `state/`: the counts so far. This is what part 2 kept in a table by hand
# MAGIC
# MAGIC The offset of a plain file source is just a number: how many files it has listed so far.

# COMMAND ----------

offset_files = sorted(dbutils.fs.ls(f"{STATE}/counts/offsets"), key=lambda f: int(f.name))
print(dbutils.fs.head(offset_files[-1].path).splitlines()[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC Every finished query also reports what it did, one report per micro-batch. The most useful fields:

# COMMAND ----------

p = last_progress(q)
print(json.dumps({
    "batchId": p["batchId"],
    "numInputRows": p["numInputRows"],
    "rows in the state": p["stateOperators"][0]["numRowsTotal"],
    "source": p["sources"][0]["description"][:80],
}, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC `numInputRows` is how much was new. The state has 2 rows: «placed» and «delivered». If this number grows
# MAGIC and grows, your state is getting too big. Notebook 07 shows how a watermark keeps it small.

# COMMAND ----------

# MAGIC %md
# MAGIC ## What you saw
# MAGIC
# MAGIC - Counting everything is correct but reads everything. Adding up by hand reads little, but you own the
# MAGIC   state, and one mistake breaks it for good.
# MAGIC - Structured Streaming: you write the query as if the data were one big table. Spark runs it on the new
# MAGIC   rows only and keeps the running result in the checkpoint.
# MAGIC - Source, transformation, sink, checkpoint, trigger. On Free Edition the trigger is `availableNow`.
# MAGIC - `append` for things that never change, `complete` or `update` for results that do.
# MAGIC
# MAGIC Our plain file source needed the schema up front. What happens when the app adds a field tomorrow? That's
# MAGIC notebook 03. First: `2_exercise_short_streaming_engine`.
