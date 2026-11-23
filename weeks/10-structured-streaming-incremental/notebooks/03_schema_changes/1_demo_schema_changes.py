# Databricks notebook source
# MAGIC %md
# MAGIC # 03 When the files change shape (demo)
# MAGIC
# MAGIC Wednesday. The delivery app got an update: customers can now give the rider a tip, and the app sends it as
# MAGIC a new field `tip_chf`. Nobody told us. And somewhere a restaurant typed an amount with a comma, `"12,50"`.
# MAGIC
# MAGIC Both happen all the time in real pipelines. The question for this notebook: **does our pipeline notice,
# MAGIC and does it lose anything?**
# MAGIC
# MAGIC ![The pipeline: this notebook is again the step from the landing folder to bronze](./assets/pipeline_autoloader.png)

# COMMAND ----------

# MAGIC %run ../_config

# COMMAND ----------

# MAGIC %run ../_generator

# COMMAND ----------

# MAGIC %run ../_pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC First we bring the pipeline to where notebook 01 left it: batches 1 and 2 in `bronze.orders`. `catch_up`
# MAGIC rebuilds it from scratch, so it doesn't matter whether you did notebook 01 or not. Takes about a minute.

# COMMAND ----------

catch_up(2)

DEMO_DIR = f"{PRACTICE_VOLUME}/demo_03"
STATE = f"{PRACTICE_VOLUME}/demo_03_state"
FIXED_TABLE = f"{PLAYGROUND}.w10_demo03_fixed_schema"
for folder in [DEMO_DIR, STATE]:
    dbutils.fs.rm(folder, recurse=True)
spark.sql(f"DROP TABLE IF EXISTS {FIXED_TABLE}")
for batch in (1, 2, 3):
    write_batch(batch, DEMO_DIR)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. What's in Wednesday's file?
# MAGIC
# MAGIC Let's look at the raw file, the way it arrived. Two lines are interesting:

# COMMAND ----------

lines = dbutils.fs.head(f"{DEMO_DIR}/orders_batch_3.json", 200_000).splitlines()
print(next(l for l in lines if "tip_chf" in l))
print(next(l for l in lines if '"12,50"' in l))

# COMMAND ----------

# MAGIC %md
# MAGIC A delivered order with a tip, and order `o-00501` with an amount that isn't a number.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Without Auto Loader: a fixed schema
# MAGIC
# MAGIC In notebook 02 the plain file source needed a schema, so we wrote one down. That schema knows nothing about
# MAGIC tips. Here is the same stream on the practice folder with all three files:

# COMMAND ----------

ORDER_SCHEMA = ("order_id STRING, customer STRING, restaurant STRING, city STRING, items INT, "
                "amount_chf DOUBLE, status STRING, event_ts STRING")

q = (spark.readStream.schema(ORDER_SCHEMA).json(DEMO_DIR)
     .writeStream.option("checkpointLocation", f"{STATE}/fixed")
     .trigger(availableNow=True)
     .toTable(FIXED_TABLE))
q.awaitTermination()
print(input_rows(q), "rows loaded, no error")
print("columns:", spark.table(FIXED_TABLE).columns)
display(spark.table(FIXED_TABLE).where("order_id = 'o-00501'"))

# COMMAND ----------

# MAGIC %md
# MAGIC The job is green. And it lost two things without a word:
# MAGIC
# MAGIC - `tip_chf` is gone. 150 tips, not in the table, not in any log.
# MAGIC - `o-00501` has `amount_chf = null`. The `"12,50"` is gone too, nobody can fix it later.
# MAGIC
# MAGIC The bad part isn't the missing column. It's that nothing tells you.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Without Auto Loader, careful: check every file first
# MAGIC
# MAGIC We could check each new file before we load it: read it on its own, compare its columns with the table,
# MAGIC and add the missing ones.

# COMMAND ----------

def new_columns(path, table):
    in_file = set(spark.read.json(path).columns)
    in_table = set(spark.table(table).columns)
    return sorted(in_file - in_table)


print(new_columns(f"{DEMO_DIR}/orders_batch_3.json", FIXED_TABLE))

# COMMAND ----------

# MAGIC %md
# MAGIC That finds the tip. Then you'd `ALTER TABLE ... ADD COLUMNS`, change the schema in the code, restart the
# MAGIC stream with a new checkpoint (notebook 02, part 3), and load the file again. Every time the app changes.
# MAGIC
# MAGIC And the `"12,50"`? Comparing columns doesn't find it, the column is the same. You'd have to check every
# MAGIC value of every row against its type. It's doable, and it's a lot of code nobody wants to own.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Auto Loader: it notices, stops once, and carries on
# MAGIC
# MAGIC Now the real pipeline. Wednesday's file lands in the landing folder, and `run_bronze()` runs. It's the
# MAGIC Auto Loader query from notebook 01, with two options added:
# MAGIC
# MAGIC | Option | Side | What it does |
# MAGIC |---|---|---|
# MAGIC | `cloudFiles.schemaEvolutionMode = addNewColumns` | source | a new field in a file: add it to the schema (that's the default) |
# MAGIC | `mergeSchema = true` | table | allow the Delta table to get the new column |
# MAGIC
# MAGIC One technical detail before we start. When a stream fails, Databricks marks the whole cell as failed, even
# MAGIC inside `try`/`except`. That's why `run_bronze()` starts the query in a notebook run of its own (the helper
# MAGIC `_stream_once`, look at it if you like), the way a job task runs. If it fails, we get a normal exception back.
# MAGIC
# MAGIC Run it. It will fail, and that's the point:

# COMMAND ----------

write_batch(3)
try:
    run_bronze()
except RuntimeError as e:
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC Auto Loader saw `tip_chf`, a field its schema doesn't have. It didn't drop it like the fixed schema did, and it
# MAGIC didn't guess. It **stopped**, after writing a new version of the schema with `tip_chf` in it. Nothing from
# MAGIC this file was written to bronze yet.
# MAGIC
# MAGIC ![Run 1 reads batch 3 with the old schema and stops on purpose after writing schema version 1. Run 2 reads batch 3 with the new schema, bronze gets the column tip_chf. A wrong value like 12,50 goes into _rescued_data](./assets/schema_evolution_restart.png)
# MAGIC
# MAGIC Why stop instead of just carrying on? Because a running query can't change its schema in the middle. The
# MAGIC stop is the moment to switch. So: run it again. In a job, a retry does this for you (notebook 08).

# COMMAND ----------

run_bronze()

bronze = spark.table(BRONZE_TABLE)
print("columns:", bronze.columns)
print(bronze.count(), "rows,", bronze.where("tip_chf IS NOT NULL").count(), "with a tip")

# COMMAND ----------

# MAGIC %md
# MAGIC 801 rows, the new column is there, 150 tips kept. The 550 older rows simply have `null` as tip.
# MAGIC
# MAGIC And `o-00501`?

# COMMAND ----------

display(bronze.where("_rescued_data IS NOT NULL").select("order_id", "amount_chf", "_rescued_data", "_source_file"))

# COMMAND ----------

# MAGIC %md
# MAGIC The row is in bronze. `amount_chf` is `null`, but the original value is not lost: it's in the column
# MAGIC `_rescued_data`, as JSON, together with the file it came from. Auto Loader adds that column itself when it
# MAGIC infers the schema. Bronze keeps everything, and silver (notebook 04) decides what to do with such rows.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Under the hood: the schema location
# MAGIC
# MAGIC The schema lives in the folder we gave as `cloudFiles.schemaLocation`. Every change is a new numbered file:

# COMMAND ----------

import json

schema_files = sorted(dbutils.fs.ls(f"{BRONZE_SCHEMA_LOCATION}/_schemas"), key=lambda f: int(f.name))
for f in schema_files:
    # line 1 is the format version, line 2 a JSON object; the schema itself is a JSON string inside it
    fields = json.loads(json.loads(dbutils.fs.head(f.path).splitlines()[-1])["dataSchemaJson"])["fields"]
    print(f.name, "→", [field["name"] for field in fields])

# COMMAND ----------

# MAGIC %md
# MAGIC Version 0 from Monday, version 1 since Wednesday, with `tip_chf`. That's how Auto Loader knows what to expect
# MAGIC from the next file, and how you can see later when a source changed. Like the checkpoint: **don't delete it**,
# MAGIC and don't share it between queries.
# MAGIC
# MAGIC `addNewColumns` isn't the only choice:
# MAGIC
# MAGIC | `schemaEvolutionMode` | A new field … |
# MAGIC |---|---|
# MAGIC | `addNewColumns` (default) | stops the stream once, then it's a column |
# MAGIC | `rescue` | never stops, the field goes into `_rescued_data` |
# MAGIC | `failOnNewColumns` | stops, and keeps stopping until you change the schema yourself |
# MAGIC | `none` | is ignored. Like our fixed schema, so only if you really don't care |
# MAGIC
# MAGIC And if you know better than the inference, you can tell it: `cloudFiles.schemaHints = "amount_chf DECIMAL(10,2)"`.
# MAGIC The extended exercise tries the modes side by side.

# COMMAND ----------

# MAGIC %md
# MAGIC ## What you saw
# MAGIC
# MAGIC - A fixed schema loses new fields and broken values silently. The job stays green.
# MAGIC - Checking every file by hand finds new columns, but not broken values, and it's a lot of code.
# MAGIC - Auto Loader with `addNewColumns`: stops once, writes a new schema version, the retry carries on. Nothing is
# MAGIC   lost. Broken values end up in `_rescued_data`.
# MAGIC - The schema location is part of the query's memory, like the checkpoint.
# MAGIC
# MAGIC `bronze.orders` now has 801 rows: every event of every order, as it came. For the restaurant dashboard we
# MAGIC need something else: **one row per order, with its latest status**. That's notebook 04. First:
# MAGIC `2_exercise_short_schema_changes`.
