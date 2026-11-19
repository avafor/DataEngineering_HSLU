# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Auto Loader (exercise, short)
# MAGIC
# MAGIC About 10 minutes. You do the same as in the demo, on your own folder in the playground: first see the
# MAGIC duplicates with your own eyes, then load the files with Auto Loader, then look at its list of files.
# MAGIC
# MAGIC Every task has a cell with `raise NotImplementedError`. Replace that line with your code, run it, then
# MAGIC run the check below it. Stuck? Open the hints under the cell. The solution is there too, but try first.

# COMMAND ----------

# MAGIC %run ../_config

# COMMAND ----------

# MAGIC %run ../_generator

# COMMAND ----------

from pyspark.sql import functions as F

EX_DIR = f"{PRACTICE_VOLUME}/ex01_short"
EX_NAIVE = f"{PLAYGROUND}.w10_ex01_naive"
EX_BRONZE = f"{PLAYGROUND}.w10_ex01_bronze"
EX_CHECKPOINT = f"{PRACTICE_VOLUME}/ex01_short_state/_checkpoint"
EX_SCHEMA = f"{PRACTICE_VOLUME}/ex01_short_state/_schema"

# A fresh start every time you run the notebook
for table in [EX_NAIVE, EX_BRONZE]:
    spark.sql(f"DROP TABLE IF EXISTS {table}")
for folder in [EX_DIR, f"{PRACTICE_VOLUME}/ex01_short_state"]:
    dbutils.fs.rm(folder, recurse=True)
dbutils.fs.mkdirs(EX_DIR)

# Monday: the first file arrives and the naive job loads it
write_batch(1, EX_DIR)
spark.read.json(EX_DIR).write.mode("append").saveAsTable(EX_NAIVE)
print(spark.table(EX_NAIVE).count(), "rows after Monday")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1.1 – How many rows are in there twice? ★
# MAGIC
# MAGIC Tuesday's file is written below. Run the naive job once more (read `EX_DIR`, append to `EX_NAIVE`), then
# MAGIC count the rows that now exist **more than once**. Store the number in `n_twice`.
# MAGIC
# MAGIC Before you run it: what number do you expect?

# COMMAND ----------

write_batch(2, EX_DIR)

# TODO Task 1.1
raise NotImplementedError("Task 1.1")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>First the same append as on Monday. Then group the table by <code>order_id</code>, <code>status</code> and <code>event_ts</code>, count, and keep the groups with a count above 1. How many groups is that?</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>spark.read.json(EX_DIR).write.mode(&quot;___&quot;).saveAsTable(EX_NAIVE)
# MAGIC n_twice = (spark.table(EX_NAIVE).groupBy(&quot;order_id&quot;, &quot;status&quot;, &quot;event_ts&quot;).count()
# MAGIC            .where(&quot;count ___ 1&quot;).count())</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>spark.read.json(EX_DIR).write.mode(&quot;append&quot;).saveAsTable(EX_NAIVE)
# MAGIC n_twice = (spark.table(EX_NAIVE).groupBy(&quot;order_id&quot;, &quot;status&quot;, &quot;event_ts&quot;).count()
# MAGIC            .where(&quot;count &gt; 1&quot;).count())
# MAGIC print(n_twice)</code></pre></details>

# COMMAND ----------

print("Correct!" if n_twice == 200 else f"Not yet – expected 200, got {n_twice}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1.2 – Load the folder with Auto Loader ★
# MAGIC
# MAGIC Complete `load_with_auto_loader()`: Auto Loader on `EX_DIR`, JSON files, schema location `EX_SCHEMA`,
# MAGIC checkpoint `EX_CHECKPOINT`, trigger `availableNow`, into the table `EX_BRONZE`. The function returns the
# MAGIC number of rows it read. Then call it once and store the result in `n_first`.
# MAGIC
# MAGIC The demo has the same code, part 3. Copy it if you like, but read every line you copy.

# COMMAND ----------

# TODO Task 1.2
def load_with_auto_loader():
    query = (
        spark.readStream.format("___")
        .option("cloudFiles.format", "___")
        .option("cloudFiles.schemaLocation", ___)
        .option("cloudFiles.inferColumnTypes", "true")
        .load(___)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .writeStream
        .option("checkpointLocation", ___)
        .trigger(availableNow=True)
        .toTable(___)
    )
    query.awaitTermination()
    return input_rows(query)


n_first = load_with_auto_loader()
print(n_first, "rows read")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>The format is <code>cloudFiles</code>, the files are <code>json</code>. The schema location goes into <code>cloudFiles.schemaLocation</code>, the checkpoint into <code>checkpointLocation</code> on the write side. <code>load()</code> gets the folder, <code>toTable()</code> the table.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>spark.readStream.format(&quot;cloudFiles&quot;)
# MAGIC     .option(&quot;cloudFiles.format&quot;, &quot;json&quot;)
# MAGIC     .option(&quot;cloudFiles.schemaLocation&quot;, ___)
# MAGIC     ...
# MAGIC     .load(___)
# MAGIC     ...
# MAGIC     .option(&quot;checkpointLocation&quot;, ___)
# MAGIC     .trigger(availableNow=True)
# MAGIC     .toTable(___)</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>def load_with_auto_loader():
# MAGIC     query = (
# MAGIC         spark.readStream.format(&quot;cloudFiles&quot;)
# MAGIC         .option(&quot;cloudFiles.format&quot;, &quot;json&quot;)
# MAGIC         .option(&quot;cloudFiles.schemaLocation&quot;, EX_SCHEMA)
# MAGIC         .option(&quot;cloudFiles.inferColumnTypes&quot;, &quot;true&quot;)
# MAGIC         .load(EX_DIR)
# MAGIC         .withColumn(&quot;_source_file&quot;, F.col(&quot;_metadata.file_path&quot;))
# MAGIC         .writeStream
# MAGIC         .option(&quot;checkpointLocation&quot;, EX_CHECKPOINT)
# MAGIC         .trigger(availableNow=True)
# MAGIC         .toTable(EX_BRONZE)
# MAGIC     )
# MAGIC     query.awaitTermination()
# MAGIC     return input_rows(query)
# MAGIC 
# MAGIC 
# MAGIC n_first = load_with_auto_loader()
# MAGIC print(n_first, &quot;rows read&quot;)</code></pre></details>

# COMMAND ----------

ok = n_first == 550 and spark.table(EX_BRONZE).count() == 550
print("Correct!" if ok else f"Not yet – expected 550 rows read and 550 in {EX_BRONZE}, got {n_first} and {spark.table(EX_BRONZE).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1.3 – Run it again ★
# MAGIC
# MAGIC Call `load_with_auto_loader()` once more, without a new file. Store the result in `n_again`.

# COMMAND ----------

# TODO Task 1.3
raise NotImplementedError("Task 1.3")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>Nothing to change: call the same function once more. The checkpoint knows both files already.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>n_again = ___()</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>n_again = load_with_auto_loader()
# MAGIC print(n_again, &quot;rows read&quot;)</code></pre></details>

# COMMAND ----------

print("Correct!" if n_again == 0 else f"Not yet – expected 0, got {n_again}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1.4 – The next file ★
# MAGIC
# MAGIC Wednesday's file is written below. Call `load_with_auto_loader()` again and store the result in `n_new`.
# MAGIC How many rows would the naive job have read this time?

# COMMAND ----------

write_batch(4, EX_DIR)

# TODO Task 1.4
raise NotImplementedError("Task 1.4")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>The new file is in the folder now. The same call reads only that one.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>n_new = load_with_auto_loader()</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>n_new = load_with_auto_loader()
# MAGIC print(n_new, &quot;rows read&quot;)</code></pre></details>

# COMMAND ----------

ok = n_new == 21 and spark.table(EX_BRONZE).count() == 571
print("Correct!" if ok else f"Not yet – expected 21 new rows and 571 in total, got {n_new} and {spark.table(EX_BRONZE).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1.5 – Read Auto Loader's list of files ★★
# MAGIC
# MAGIC Use `cloud_files_state` on your checkpoint to see which files Auto Loader has recorded. Store the number of
# MAGIC files in `n_files` and display the list.
# MAGIC
# MAGIC Which column tells you when a file was written to the table? What would this list look like after you
# MAGIC delete `EX_CHECKPOINT`?

# COMMAND ----------

# TODO Task 1.5
raise NotImplementedError("Task 1.5")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'><code>cloud_files_state</code> is a table function in SQL. It takes the path of the checkpoint as a string and returns one row per file.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>files = spark.sql(f&quot;SELECT * FROM cloud_files_state(&#x27;{___}&#x27;)&quot;)
# MAGIC n_files = files.___()</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>files = spark.sql(f&quot;SELECT * FROM cloud_files_state(&#x27;{EX_CHECKPOINT}&#x27;)&quot;)
# MAGIC display(files)
# MAGIC n_files = files.count()</code></pre></details>

# COMMAND ----------

print("Correct!" if n_files == 3 else f"Not yet – expected 3 files, got {n_files}")

# COMMAND ----------

# MAGIC %md
# MAGIC Done. If you have time left, start with `3_exercise_extended_auto_loader`, or go on with notebook 02.
