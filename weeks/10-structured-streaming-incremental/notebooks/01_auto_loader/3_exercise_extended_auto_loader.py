# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Auto Loader (exercise, extended)
# MAGIC
# MAGIC For at home, or if you finished the short one early. Four topics, each in the same order: something that
# MAGIC goes wrong, why it goes wrong, and how to do it properly. Then a task. The topics don't build on each
# MAGIC other, so you can start with the one that interests you.
# MAGIC
# MAGIC | Part | Question |
# MAGIC |---|---|
# MAGIC | 1 | Can a hand-made file list survive a crash? |
# MAGIC | 2 | Which file did this row come from? |
# MAGIC | 3 | What does Auto Loader do with 1000 new files at once? |
# MAGIC | 4 | What happens when someone deletes the checkpoint, or overwrites a file? |
# MAGIC
# MAGIC Everything happens in the playground, the pipeline tables are not touched.

# COMMAND ----------

# MAGIC %run ../_config

# COMMAND ----------

# MAGIC %run ../_generator

# COMMAND ----------

from pyspark.sql import functions as F

BASE = f"{PRACTICE_VOLUME}/ex01_extended"
for row in spark.sql(f"SHOW TABLES IN {PLAYGROUND} LIKE 'w10_ex01x_*'").collect():
    spark.sql(f"DROP TABLE IF EXISTS {PLAYGROUND}.{row.tableName}")
dbutils.fs.rm(BASE, recurse=True)


def auto_loader(folder, table, state, **options):
    # Auto Loader from `folder` into `table`, checkpoint and schema under `state`. Returns the finished query.
    reader = (spark.readStream.format("cloudFiles")
              .option("cloudFiles.format", "json")
              .option("cloudFiles.schemaLocation", f"{state}/_schema")
              .option("cloudFiles.inferColumnTypes", "true"))
    for key, value in options.items():
        reader = reader.option(key, value)
    query = (reader.load(folder)
             .withColumn("_source_file", F.col("_metadata.file_path"))
             .withColumn("_ingested_at", F.current_timestamp())
             .writeStream
             .option("checkpointLocation", f"{state}/_checkpoint")
             .trigger(availableNow=True)
             .toTable(table))
    query.awaitTermination()
    return query

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. A file list that survives a crash
# MAGIC
# MAGIC ### What goes wrong
# MAGIC
# MAGIC In the demo, our loader wrote the data first and the list of files second. Two writes. If the job dies
# MAGIC between them, the data is there but the list doesn't know it, and the next run loads the file again.
# MAGIC
# MAGIC ### Why
# MAGIC
# MAGIC Each write on its own is safe, Delta makes sure of that. The *pair* is not. There is no transaction
# MAGIC that covers two tables.
# MAGIC
# MAGIC ### How to do it properly
# MAGIC
# MAGIC Don't keep the list in a second table. Keep it **in the data**: every row gets the name of the file it
# MAGIC came from (`_source_file`). The files that are done are then simply the distinct `_source_file` values of
# MAGIC the table. One write, one transaction. Either the file is in the table, completely, or it isn't.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1.6 – A crash-safe loader of your own ★★
# MAGIC
# MAGIC Write `load_new_files_safe(folder, table)`:
# MAGIC
# MAGIC 1. list the files in `folder`
# MAGIC 2. if the table exists, take the distinct `_source_file` values from it, otherwise nothing is done yet
# MAGIC 3. read only the files that are not done yet, add the column `_source_file` (from `_metadata.file_path`)
# MAGIC 4. append them to `table` and return the number of rows you appended
# MAGIC
# MAGIC Careful: `dbutils.fs.ls` returns paths that start with `dbfs:/Volumes/...`, `_metadata.file_path` has
# MAGIC `dbfs:` too. Compare like with like. The check runs your loader three times, with a new file in between.

# COMMAND ----------

SAFE_DIR = f"{BASE}/safe"
SAFE_TABLE = f"{PLAYGROUND}.w10_ex01x_safe"
dbutils.fs.mkdirs(SAFE_DIR)

# TODO Task 1.6
raise NotImplementedError("Task 1.6")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'><code>spark.catalog.tableExists(table)</code> tells you whether there is a table yet. The files that are done are the distinct values of <code>_source_file</code> in it. Read only the others with <code>spark.read.json(list)</code>, add <code>_source_file</code> from <code>_metadata.file_path</code>, append, return the count.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>def load_new_files_safe(folder, table):
# MAGIC     all_files = [f.path for f in dbutils.fs.ls(folder)]
# MAGIC     done = set()
# MAGIC     if spark.catalog.tableExists(table):
# MAGIC         done = {r._source_file for r in spark.table(table).select(&quot;___&quot;).distinct().collect()}
# MAGIC     new = [p for p in all_files if p ___ done]
# MAGIC     if not new:
# MAGIC         return 0
# MAGIC     df = spark.read.json(new).withColumn(&quot;_source_file&quot;, F.col(&quot;___&quot;))
# MAGIC     df.write.mode(&quot;append&quot;).saveAsTable(table)
# MAGIC     return df.count()</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>def load_new_files_safe(folder, table):
# MAGIC     all_files = [f.path for f in dbutils.fs.ls(folder)]
# MAGIC     if spark.catalog.tableExists(table):
# MAGIC         done = {r._source_file for r in spark.table(table).select(&quot;_source_file&quot;).distinct().collect()}
# MAGIC     else:
# MAGIC         done = set()
# MAGIC     new = [p for p in all_files if p not in done]
# MAGIC     if not new:
# MAGIC         return 0
# MAGIC     df = spark.read.json(new).withColumn(&quot;_source_file&quot;, F.col(&quot;_metadata.file_path&quot;))
# MAGIC     df.write.mode(&quot;append&quot;).option(&quot;mergeSchema&quot;, &quot;true&quot;).saveAsTable(table)
# MAGIC     return df.count()</code></pre></details>

# COMMAND ----------

write_batch(1, SAFE_DIR)
runs = [load_new_files_safe(SAFE_DIR, SAFE_TABLE), load_new_files_safe(SAFE_DIR, SAFE_TABLE)]
write_batch(2, SAFE_DIR)
runs.append(load_new_files_safe(SAFE_DIR, SAFE_TABLE))
total = spark.table(SAFE_TABLE).count()
print("Correct!" if runs == [200, 0, 350] and total == 550 else f"Not yet – expected runs [200, 0, 350] and 550 rows, got {runs} and {total}")

# COMMAND ----------

# MAGIC %md
# MAGIC Better. But step 1 still lists the whole folder on every run, and step 2 reads a column of the whole
# MAGIC table. With a few files, fine. With a year of files every five minutes, that's 100 000 files to list and
# MAGIC compare, every run. Auto Loader keeps its list in a small database in the checkpoint and, if you set it
# MAGIC up, gets notified about new files instead of listing (see the slide on file notification).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Which file did this row come from?
# MAGIC
# MAGIC ### What goes wrong
# MAGIC
# MAGIC A wrong amount shows up in the dashboard. Which delivery did it come from? If bronze only has the
# MAGIC columns of the JSON, you can't tell anymore.
# MAGIC
# MAGIC ### How to do it properly
# MAGIC
# MAGIC Every file source in Spark has a hidden column `_metadata` with `file_path`, `file_name`, `file_size` and
# MAGIC `file_modification_time`. Keep at least the path in bronze. It costs nothing and saves you hours later.

# COMMAND ----------

META_DIR = f"{BASE}/meta"
META_TABLE = f"{PLAYGROUND}.w10_ex01x_meta"
write_batch(1, META_DIR)
write_batch(2, META_DIR)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1.7 – Rows and bytes per file ★
# MAGIC
# MAGIC Read `META_DIR` with a plain `spark.read.json` and select `_metadata.file_name` and `_metadata.file_size`
# MAGIC next to the data. Then build `per_file`: one row per file name with the number of rows (`rows`) and the
# MAGIC file size (`bytes`).

# COMMAND ----------

# TODO Task 1.7
raise NotImplementedError("Task 1.7")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'><code>_metadata</code> is a struct column you can select like any other: <code>F.col(&quot;_metadata.file_name&quot;)</code>. Group by name and size and count.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>per_file = (spark.read.json(META_DIR)
# MAGIC     .select(F.col(&quot;_metadata.___&quot;).alias(&quot;file_name&quot;), F.col(&quot;_metadata.___&quot;).alias(&quot;bytes&quot;))
# MAGIC     .groupBy(&quot;file_name&quot;, &quot;bytes&quot;).agg(F.count(&quot;*&quot;).alias(&quot;rows&quot;)))</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>per_file = (spark.read.json(META_DIR)
# MAGIC             .select(F.col(&quot;_metadata.file_name&quot;).alias(&quot;file_name&quot;), F.col(&quot;_metadata.file_size&quot;).alias(&quot;bytes&quot;))
# MAGIC             .groupBy(&quot;file_name&quot;, &quot;bytes&quot;).agg(F.count(&quot;*&quot;).alias(&quot;rows&quot;)))
# MAGIC display(per_file)</code></pre></details>

# COMMAND ----------

got = {r.file_name: r.rows for r in per_file.collect()}
want = {"orders_batch_1.json": 200, "orders_batch_2.json": 350}
print("Correct!" if got == want and "bytes" in per_file.columns else f"Not yet – expected {want} and a column bytes, got {got}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. A thousand files at once
# MAGIC
# MAGIC ### What goes wrong
# MAGIC
# MAGIC The job was paused for a week. Now 2000 files are waiting. `availableNow` wants to read all of them, and
# MAGIC one huge micro-batch can run out of memory or take so long that the job times out. Then nothing is
# MAGIC written and the next run tries the same huge batch again.
# MAGIC
# MAGIC ### How to do it properly
# MAGIC
# MAGIC `cloudFiles.maxFilesPerTrigger` limits how many files go into one micro-batch. `availableNow` still reads
# MAGIC everything that is there, but in several smaller batches, and each finished batch is committed. If the
# MAGIC job dies after batch 3 of 5, the next run starts at batch 4.

# COMMAND ----------

MANY_DIR = f"{BASE}/many"
MANY_TABLE = f"{PLAYGROUND}.w10_ex01x_many"
write_batch_in_parts(1, 4, MANY_DIR)     # 200 orders in 4 files of 50

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1.8 – One file per micro-batch ★★
# MAGIC
# MAGIC Load `MANY_DIR` into `MANY_TABLE` with `auto_loader(...)` and the option `cloudFiles.maxFilesPerTrigger`
# MAGIC set to `1`. The state folder is `f"{BASE}/many_state"`. Store the finished query in `q_many`, then count the
# MAGIC micro-batches that actually read rows and store the number in `n_batches`. `batch_sizes(q_many)` from
# MAGIC `_config` gives you the rows of every micro-batch.

# COMMAND ----------

# TODO Task 1.8
raise NotImplementedError("Task 1.8")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'><code>auto_loader</code> passes extra options on as keyword arguments. Option names with a dot don&#x27;t work as Python names, so pass them as a dict with <code>**</code>. Then count the entries of <code>batch_sizes(q_many)</code> that are above 0.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>q_many = auto_loader(MANY_DIR, MANY_TABLE, f&quot;{BASE}/many_state&quot;, **{&quot;cloudFiles.___&quot;: &quot;1&quot;})
# MAGIC n_batches = sum(1 for rows in batch_sizes(q_many) if rows ___ 0)</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>q_many = auto_loader(MANY_DIR, MANY_TABLE, f&quot;{BASE}/many_state&quot;, **{&quot;cloudFiles.maxFilesPerTrigger&quot;: &quot;1&quot;})
# MAGIC n_batches = sum(1 for rows in batch_sizes(q_many) if rows &gt; 0)
# MAGIC print(n_batches, &quot;micro-batches&quot;)</code></pre></details>

# COMMAND ----------

ok = n_batches == 4 and spark.table(MANY_TABLE).count() == 200
print("Correct!" if ok else f"Not yet – expected 4 batches and 200 rows, got {n_batches} and {spark.table(MANY_TABLE).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. The checkpoint is gone, a file is overwritten
# MAGIC
# MAGIC ### What goes wrong
# MAGIC
# MAGIC Someone tidies up the volume and deletes a folder called `_checkpoint`, it looked unimportant. The next
# MAGIC run of the job starts without a list, sees every file as new, and loads all of them again. The job
# MAGIC doesn't fail. Bronze just has everything twice.
# MAGIC
# MAGIC ### Why
# MAGIC
# MAGIC The checkpoint *is* the memory of the query. The table doesn't know which files it was built from, only
# MAGIC the checkpoint does. (Unless you kept `_source_file`, see part 2: that's how you notice it.)
# MAGIC
# MAGIC ### How to do it properly
# MAGIC
# MAGIC Treat table and checkpoint as one thing. Starting over means dropping both. Moving the table means
# MAGIC moving both. And keep `_source_file` and `_ingested_at` in bronze, so a double load is easy to find.

# COMMAND ----------

LOST_DIR = f"{BASE}/lost"
LOST_TABLE = f"{PLAYGROUND}.w10_ex01x_lost"
LOST_STATE = f"{BASE}/lost_state"
write_batch(1, LOST_DIR)
write_batch(2, LOST_DIR)
auto_loader(LOST_DIR, LOST_TABLE, LOST_STATE)

dbutils.fs.rm(f"{LOST_STATE}/_checkpoint", recurse=True)   # someone tidies up
auto_loader(LOST_DIR, LOST_TABLE, LOST_STATE)
print(spark.table(LOST_TABLE).count(), "rows in the table")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1.9 – Find the files that were loaded twice ★★
# MAGIC
# MAGIC Using only the table `LOST_TABLE`: which files were loaded in more than one run? A run is one value of
# MAGIC `_ingested_at` (all rows of one micro-batch get the same timestamp). Store the file names in the list
# MAGIC `loaded_twice`, sorted.

# COMMAND ----------

# TODO Task 1.9
raise NotImplementedError("Task 1.9")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>Group by <code>_source_file</code> and count the distinct <code>_ingested_at</code> values. Files with more than one were loaded in more than one run. Keep only the file name: the part after the last <code>/</code>.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>rows = (spark.table(LOST_TABLE).groupBy(&quot;_source_file&quot;)
# MAGIC         .agg(F.countDistinct(&quot;___&quot;).alias(&quot;runs&quot;)).where(&quot;runs &gt; 1&quot;).collect())
# MAGIC loaded_twice = sorted(r._source_file.split(&quot;/&quot;)[___] for r in rows)</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>loaded_twice = sorted(
# MAGIC     r._source_file.split(&quot;/&quot;)[-1]
# MAGIC     for r in (spark.table(LOST_TABLE).groupBy(&quot;_source_file&quot;)
# MAGIC               .agg(F.countDistinct(&quot;_ingested_at&quot;).alias(&quot;runs&quot;))
# MAGIC               .where(&quot;runs &gt; 1&quot;).collect())
# MAGIC )
# MAGIC print(loaded_twice)</code></pre></details>

# COMMAND ----------

want = ["orders_batch_1.json", "orders_batch_2.json"]
print("Correct!" if loaded_twice == want else f"Not yet – expected {want}, got {loaded_twice}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1.10 – A corrected file under the same name ★★★
# MAGIC
# MAGIC The app sends `orders_batch_1.json` again, with corrected amounts, **under the same name**. By default
# MAGIC Auto Loader ignores it: it has seen that file. With `cloudFiles.allowOverwrites` set to `true` it loads a
# MAGIC file again when it changed.
# MAGIC
# MAGIC 1. Load `OVER_DIR` into `OVER_TABLE` with `auto_loader(...)` and `allowOverwrites` (state `OVER_STATE`).
# MAGIC 2. Overwrite the file (cell given below), then run your loader again.
# MAGIC 3. Store the number of rows in `OVER_TABLE` that came from `orders_batch_1.json` in `n_batch_1`.
# MAGIC
# MAGIC 200 or 400? And what does that mean for the next layer, silver?

# COMMAND ----------

import json

OVER_DIR = f"{BASE}/over"
OVER_TABLE = f"{PLAYGROUND}.w10_ex01x_over"
OVER_STATE = f"{BASE}/over_state"
write_batch(1, OVER_DIR)


def send_corrected_batch_1():
    # Same orders, every amount 1 CHF higher, same file name
    rows = [dict(r, amount_chf=r["amount_chf"] + 1) for r in _rows(1)]
    dbutils.fs.put(f"{OVER_DIR}/orders_batch_1.json", "\n".join(json.dumps(r) for r in rows), overwrite=True)

# COMMAND ----------

# TODO Task 1.10
raise NotImplementedError("Task 1.10")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>Three steps: load once with the option, call <code>send_corrected_batch_1()</code>, load again with the same option and the same state folder. Then count the rows whose <code>_source_file</code> ends with <code>orders_batch_1.json</code>.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>opts = {&quot;cloudFiles.allowOverwrites&quot;: &quot;true&quot;}
# MAGIC auto_loader(OVER_DIR, OVER_TABLE, OVER_STATE, **opts)
# MAGIC send_corrected_batch_1()
# MAGIC auto_loader(___, ___, ___, **opts)
# MAGIC n_batch_1 = spark.table(OVER_TABLE).where(&quot;_source_file LIKE &#x27;%___&#x27;&quot;).count()</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>auto_loader(OVER_DIR, OVER_TABLE, OVER_STATE, **{&quot;cloudFiles.allowOverwrites&quot;: &quot;true&quot;})
# MAGIC send_corrected_batch_1()
# MAGIC auto_loader(OVER_DIR, OVER_TABLE, OVER_STATE, **{&quot;cloudFiles.allowOverwrites&quot;: &quot;true&quot;})
# MAGIC n_batch_1 = spark.table(OVER_TABLE).where(&quot;_source_file LIKE &#x27;%orders_batch_1.json&#x27;&quot;).count()
# MAGIC print(n_batch_1)</code></pre></details>

# COMMAND ----------

print("Correct!" if n_batch_1 == 400 else f"Not yet – expected 400, got {n_batch_1}")

# COMMAND ----------

# MAGIC %md
# MAGIC 400: the old and the corrected version, both in bronze. That's fine for bronze, it keeps what arrived.
# MAGIC But silver must then pick the newer version of each order, and that is exactly what notebook 04 does with
# MAGIC `MERGE`. Keep `allowOverwrites` off unless your source really overwrites files.
# MAGIC
# MAGIC ## Clean up (optional)
# MAGIC
# MAGIC The next cell removes the tables and files of this notebook.

# COMMAND ----------

for row in spark.sql(f"SHOW TABLES IN {PLAYGROUND} LIKE 'w10_ex01x_*'").collect():
    spark.sql(f"DROP TABLE IF EXISTS {PLAYGROUND}.{row.tableName}")
dbutils.fs.rm(BASE, recurse=True)
