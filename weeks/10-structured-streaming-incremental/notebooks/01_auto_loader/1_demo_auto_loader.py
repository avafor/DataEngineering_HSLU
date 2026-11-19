# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Auto Loader: every file exactly once (demo)
# MAGIC
# MAGIC Last week Anna's pizza order was an event in a Kafka topic. Someone has to get those events into the
# MAGIC lakehouse. In this first step the delivery app (or a Kafka connector) writes them as JSON files into a
# MAGIC landing folder, a new file every few minutes. Our job is to bring every file into the bronze table.
# MAGIC Once. Not zero times, not twice.
# MAGIC
# MAGIC ![The pipeline of this week: app, landing folder, bronze, silver, gold. This notebook is the step from the landing folder to bronze](./assets/pipeline_autoloader.png)
# MAGIC
# MAGIC Sounds trivial. We'll try it the obvious way first, then the careful way, and only then with Auto Loader.
# MAGIC Run the cells one by one and read the output, that's where the story is.

# COMMAND ----------

# MAGIC %run ../_config

# COMMAND ----------

# MAGIC %run ../_generator

# COMMAND ----------

from pyspark.sql import functions as F

# A practice folder and practice tables for parts 1 and 2. The pipeline itself starts in part 3.
DEMO_DIR = f"{PRACTICE_VOLUME}/demo_01"
NAIVE_TABLE = f"{PLAYGROUND}.w10_demo01_naive"
HAND_TABLE = f"{PLAYGROUND}.w10_demo01_by_hand"
LOG_TABLE = f"{PLAYGROUND}.w10_demo01_processed_files"

# Start from scratch every time you run this notebook
for table in [NAIVE_TABLE, HAND_TABLE, LOG_TABLE]:
    spark.sql(f"DROP TABLE IF EXISTS {table}")
dbutils.fs.rm(DEMO_DIR, recurse=True)
dbutils.fs.mkdirs(DEMO_DIR)


def duplicates(table):
    # Rows that exist more than once: same order, same status, same time
    return (spark.table(table).groupBy("order_id", "status", "event_ts").count()
            .where("count > 1").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The obvious way: read the folder, append
# MAGIC
# MAGIC Monday evening. The first file is in the folder.

# COMMAND ----------

write_batch(1, DEMO_DIR)
display(dbutils.fs.ls(DEMO_DIR))

# COMMAND ----------

# MAGIC %md
# MAGIC We read everything in the folder and append it to a table. Two lines, done.

# COMMAND ----------

spark.read.json(DEMO_DIR).write.mode("append").saveAsTable(NAIVE_TABLE)
print(spark.table(NAIVE_TABLE).count(), "rows in the table")

# COMMAND ----------

# MAGIC %md
# MAGIC 200 rows, correct. On Tuesday the second file arrives, and the job runs again. Same code.

# COMMAND ----------

write_batch(2, DEMO_DIR)
spark.read.json(DEMO_DIR).write.mode("append").saveAsTable(NAIVE_TABLE)
print(spark.table(NAIVE_TABLE).count(), "rows in the table")
print(duplicates(NAIVE_TABLE), "rows are in there twice")

# COMMAND ----------

# MAGIC %md
# MAGIC 750 instead of 550. The job read Monday's file again and appended it a second time. Nobody notices
# MAGIC right away, the job didn't fail. You notice it a week later, when the revenue in the dashboard is
# MAGIC too high.
# MAGIC
# MAGIC ![Monday: one file, 200 rows, correct. Tuesday: the same code reads both files and appends 550 rows, the table has 750 rows. Overwrite would be correct, but every run reads every file](./assets/folder_loaded_twice.png)
# MAGIC
# MAGIC The quick fix is `overwrite` instead of `append`:

# COMMAND ----------

spark.read.json(DEMO_DIR).write.mode("overwrite").saveAsTable(NAIVE_TABLE)
print(spark.table(NAIVE_TABLE).count(), "rows,", duplicates(NAIVE_TABLE), "duplicates")
print("files read in this run:", len(dbutils.fs.ls(DEMO_DIR)))

# COMMAND ----------

# MAGIC %md
# MAGIC Correct again. But every run now reads every file that ever arrived. With one file per day, the job
# MAGIC reads 100 files on day 100 and 1000 on day 1000, to add one. And bronze is rewritten every time, so its
# MAGIC history (week 8, time travel) is no longer «what arrived when», just «the last full load».
# MAGIC
# MAGIC What we actually want: **read only the files that are new**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. The careful way: remember which files are done
# MAGIC
# MAGIC So we keep a list. A small table `processed_files` with one row per file we have loaded. Each run
# MAGIC lists the folder, takes the files that are not in the list yet, loads them, and adds them to the list.

# COMMAND ----------

spark.sql(f"CREATE TABLE IF NOT EXISTS {LOG_TABLE} (path STRING, loaded_at TIMESTAMP)")


def load_new_files(crash_before_log=False):
    all_files = [f.path for f in dbutils.fs.ls(DEMO_DIR)]                  # 1. list the folder
    done = {row.path for row in spark.table(LOG_TABLE).collect()}           # 2. what is done already?
    new = [p for p in all_files if p not in done]
    if not new:
        print("Nothing new.")
        return
    spark.read.json(new).write.mode("append").saveAsTable(HAND_TABLE)       # 3. + 4. load only the new ones
    if crash_before_log:
        raise RuntimeError("The job died right here: after the append, before the list was updated")
    (spark.createDataFrame([(p,) for p in new], "path STRING")              # 5. remember them
        .withColumn("loaded_at", F.current_timestamp())
        .write.mode("append").saveAsTable(LOG_TABLE))
    print(f"Loaded {len(new)} file(s):", [p.split("/")[-1] for p in new])

# COMMAND ----------

# MAGIC %md
# MAGIC The practice folder still has both files. We start the list empty, so the first run loads both:

# COMMAND ----------

load_new_files()
print(spark.table(HAND_TABLE).count(), "rows,", duplicates(HAND_TABLE), "duplicates")

# COMMAND ----------

# MAGIC %md
# MAGIC And a second run, without a new file:

# COMMAND ----------

load_new_files()
display(spark.table(LOG_TABLE))

# COMMAND ----------

# MAGIC %md
# MAGIC That works. Now let's be unlucky once. The next file arrives, and the job dies after the append,
# MAGIC before it writes the file name into the list. Serverless restarts, a timeout, someone cancels the
# MAGIC run. It happens more often than you'd think.

# COMMAND ----------

write_batch(4, DEMO_DIR)
try:
    load_new_files(crash_before_log=True)
except RuntimeError as e:
    print("Error:", e)

# COMMAND ----------

# MAGIC %md
# MAGIC The next scheduled run comes along and does what it is told: the file is not in the list, so it's new.

# COMMAND ----------

load_new_files()
print(spark.table(HAND_TABLE).count(), "rows,", duplicates(HAND_TABLE), "duplicates")

# COMMAND ----------

# MAGIC %md
# MAGIC 21 rows twice. And that's only one of the ways this goes wrong:
# MAGIC
# MAGIC ![The five steps of the hand-made loader, and where it breaks: a crash between append and list, a file sent again under the same name, two runs at the same time, and listing the whole folder on every run](./assets/file_log_by_hand.png)
# MAGIC
# MAGIC You can fix each of these. Write the list and the data in one transaction, lock the job so it never
# MAGIC runs twice, store file sizes and dates, find a faster way to list millions of files. Then you have
# MAGIC written your own little ingestion framework, and you get to maintain it.
# MAGIC
# MAGIC Somebody did that already.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Auto Loader
# MAGIC
# MAGIC **Auto Loader** is Databricks' answer to exactly this problem. You point it at a folder and it loads
# MAGIC every new file once. It keeps the list of files itself, in a **checkpoint** folder, and it writes the
# MAGIC data and its progress in a way that survives a crash.
# MAGIC
# MAGIC From here on we build the real pipeline: the landing folder of the app and the table `bronze.orders`.
# MAGIC This cell empties both, so the numbers below are the same every time you run the notebook.

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {BRONZE_TABLE}")
for folder in [INCOMING_DIR, f"{STATE_VOLUME}/bronze_orders"]:
    dbutils.fs.rm(folder, recurse=True)
dbutils.fs.mkdirs(INCOMING_DIR)

write_batch(1)

# COMMAND ----------

def run_bronze():
    query = (
        spark.readStream.format("cloudFiles")                          # cloudFiles = Auto Loader
        .option("cloudFiles.format", "json")                           # what kind of files
        .option("cloudFiles.schemaLocation", BRONZE_SCHEMA_LOCATION)   # where it keeps the schema (notebook 03)
        .option("cloudFiles.inferColumnTypes", "true")                 # numbers as numbers, not strings
        .load(INCOMING_DIR)
        .withColumn("_source_file", F.col("_metadata.file_path"))     # which file a row came from
        .withColumn("_ingested_at", F.current_timestamp())
        .writeStream
        .option("checkpointLocation", BRONZE_CHECKPOINT)               # where it keeps the list of files
        .trigger(availableNow=True)                                    # read what's new, then stop
        .toTable(BRONZE_TABLE)
    )
    query.awaitTermination()
    print(f"{input_rows(query)} new rows read")


run_bronze()

# COMMAND ----------

# MAGIC %md
# MAGIC `readStream` and `writeStream` instead of `read` and `write`: Auto Loader is a **streaming** query. Don't
# MAGIC let the word scare you. With `trigger(availableNow=True)` it reads everything that is new, writes it and
# MAGIC stops, like a normal batch job. Notebook 02 looks at the streaming part in detail.
# MAGIC
# MAGIC Now the same test as before. A second run, no new file:

# COMMAND ----------

run_bronze()

# COMMAND ----------

# MAGIC %md
# MAGIC 0 rows. Tuesday's file arrives:

# COMMAND ----------

write_batch(2)
run_bronze()

display(spark.table(BRONZE_TABLE).groupBy("_source_file").count())
print(spark.table(BRONZE_TABLE).count(), "rows,", duplicates(BRONZE_TABLE), "duplicates")

# COMMAND ----------

# MAGIC %md
# MAGIC 350 new rows, 550 in total, no duplicates. Same code on every run, and no list of our own.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Under the hood: where is the list?
# MAGIC
# MAGIC Auto Loader keeps it in the checkpoint folder. You can ask for it with `cloud_files_state`:

# COMMAND ----------

display(spark.sql(f"""
    SELECT regexp_extract(path, '[^/]+$', 0) AS file, size, commit_time, path
    FROM cloud_files_state('{BRONZE_CHECKPOINT}')
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC That's our `processed_files` table, kept by Auto Loader. Don't be surprised by the long `path`: that's
# MAGIC where the volume really lives in cloud storage. `/Volumes/...` is just the name Unity Catalog gives it.
# MAGIC
# MAGIC The checkpoint folder itself looks like this:

# COMMAND ----------

display(dbutils.fs.ls(BRONZE_CHECKPOINT))

# COMMAND ----------

# MAGIC %md
# MAGIC | Folder | What's in it |
# MAGIC |---|---|
# MAGIC | `sources/` | the list of files Auto Loader has seen (a small RocksDB database) |
# MAGIC | `offsets/` | one file per run: *what this run is going to read*, written **before** it reads |
# MAGIC | `commits/` | one file per run: *done*, written **after** the data is in the table |
# MAGIC | `metadata` | the id of the query |
# MAGIC
# MAGIC `offsets` before, `commits` after: that's the answer to our crash from part 2. If the job dies in the
# MAGIC middle, there is an `offsets` file without a `commits` file. The next run sees that, reads the same files
# MAGIC again, and the Delta table knows that this run was already written once and doesn't write it twice.
# MAGIC
# MAGIC Here is the newest `offsets` file:

# COMMAND ----------

offset_files = sorted(dbutils.fs.ls(f"{BRONZE_CHECKPOINT}/offsets"), key=lambda f: int(f.name))
print(dbutils.fs.head(offset_files[-1].path))

# COMMAND ----------

# MAGIC %md
# MAGIC Not very readable, and it doesn't have to be. Two things to remember:
# MAGIC
# MAGIC ![The checkpoint is a bookmark: files already read stay behind it, the next run only reads the files after it. Without a checkpoint every run starts from scratch](./assets/checkpoint_bookmark.png)
# MAGIC
# MAGIC - **One checkpoint belongs to one query and one table.** Never share it, never copy it.
# MAGIC - **Delete the checkpoint and the list is gone.** The next run loads every file again, and you are back
# MAGIC   at part 1. If you want to start over, delete the table *and* the checkpoint together.
# MAGIC
# MAGIC One more thing, to be fair: if the app sends a corrected file **under the same name**, Auto Loader skips it
# MAGIC too, like our list did. That's on purpose, files in a landing folder should not change. If yours do,
# MAGIC there's an option for it (`cloudFiles.allowOverwrites`). The extended exercise tries it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## What you saw
# MAGIC
# MAGIC | | read the folder + append | your own list | Auto Loader |
# MAGIC |---|---|---|---|
# MAGIC | duplicates after a rerun | yes | no | no |
# MAGIC | duplicates after a crash | yes | yes | no |
# MAGIC | reads per run | everything | only new files | only new files |
# MAGIC | code you maintain | 1 line | a small framework | 10 lines |
# MAGIC
# MAGIC `bronze.orders` now has Monday's and Tuesday's orders, 550 rows. The rest of the week builds on it.
# MAGIC
# MAGIC Auto Loader is a `readStream`. So what is a streaming query, what does `availableNow` really do, and what
# MAGIC else can you stream from? That's notebook 02. First try it yourself: `2_exercise_short_auto_loader`.
