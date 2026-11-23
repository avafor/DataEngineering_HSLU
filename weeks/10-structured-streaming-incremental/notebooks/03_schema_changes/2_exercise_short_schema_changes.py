# Databricks notebook source
# MAGIC %md
# MAGIC # 03 When the files change shape (exercise, short)
# MAGIC
# MAGIC About 10 minutes. You build your own Auto Loader that survives the new column, with a retry, and then find
# MAGIC the rescued value and the schema versions. Everything in the playground.

# COMMAND ----------

# MAGIC %run ../_config

# COMMAND ----------

# MAGIC %run ../_generator

# COMMAND ----------

import json

from pyspark.sql import functions as F

EX_DIR = f"{PRACTICE_VOLUME}/ex03_short"
STATE = f"{PRACTICE_VOLUME}/ex03_short_state"   # schema in STATE/_schema, checkpoint in STATE/_checkpoint
EX_BRONZE = f"{PLAYGROUND}.w10_ex03_bronze"

spark.sql(f"DROP TABLE IF EXISTS {EX_BRONZE}")
for folder in [EX_DIR, STATE]:
    dbutils.fs.rm(folder, recurse=True)
write_batch(1, EX_DIR)
write_batch(2, EX_DIR)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.1 – An Auto Loader ready for new columns ★
# MAGIC
# MAGIC As in the demo, we start the query with `run_stream_once` from `_config`, so that a stop comes back as a
# MAGIC normal exception. It runs Auto Loader on `folder` into `target_table`, with schema location and checkpoint
# MAGIC under `state`, and it already sets `mergeSchema` on the table side. You give it the source-side option.
# MAGIC
# MAGIC Write `load()`: it calls `run_stream_once` for `EX_BRONZE`, state `STATE`, folder `EX_DIR`, with
# MAGIC `cloudFiles.schemaEvolutionMode` set so that new columns are added. It returns the rows read. Call it once and
# MAGIC store the result in `n_first`.

# COMMAND ----------

# TODO Task 3.1
def load():
    return run_stream_once(EX_BRONZE, STATE, folder=EX_DIR, **{"cloudFiles.schemaEvolutionMode": "___"})


n_first = load()
print(n_first)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>On the source side the option is <code>cloudFiles.schemaEvolutionMode</code>, the value that adds new fields as columns is <code>addNewColumns</code>. The table side (<code>mergeSchema</code>) is done by the helper.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>def load():
# MAGIC     return run_stream_once(EX_BRONZE, STATE, folder=EX_DIR,
# MAGIC                            **{&quot;cloudFiles.schemaEvolutionMode&quot;: &quot;___&quot;})</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>def load():
# MAGIC     return run_stream_once(EX_BRONZE, STATE, folder=EX_DIR, **{&quot;cloudFiles.schemaEvolutionMode&quot;: &quot;addNewColumns&quot;})
# MAGIC 
# MAGIC 
# MAGIC n_first = load()
# MAGIC print(n_first)</code></pre></details>

# COMMAND ----------

print("Correct!" if n_first == 550 else f"Not yet – expected 550, got {n_first}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.2 – A retry ★
# MAGIC
# MAGIC Wednesday's file with `tip_chf` arrives below. The first `load()` will stop with a `RuntimeError`. Write
# MAGIC `load_with_retry()`: it calls `load()`, and if that raises a `RuntimeError`, prints the reason and calls `load()`
# MAGIC once more. Then call it. Afterwards `EX_BRONZE` must have 801 rows and a column `tip_chf`.
# MAGIC
# MAGIC Why only one retry, and not «retry until it works»?

# COMMAND ----------

write_batch(3, EX_DIR)

# TODO Task 3.2
raise NotImplementedError("Task 3.2")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>A <code>try</code> around <code>load()</code>. In <code>except RuntimeError as e:</code> print the reason and call <code>load()</code> again. Don&#x27;t catch around the second call: if that fails too, you want to see it.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>def load_with_retry():
# MAGIC     try:
# MAGIC         return load()
# MAGIC     except RuntimeError as e:
# MAGIC         print(&quot;stopped once:&quot;, e)
# MAGIC         return ___()</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>def load_with_retry():
# MAGIC     try:
# MAGIC         return load()
# MAGIC     except RuntimeError as e:
# MAGIC         print(&quot;stopped once:&quot;, str(e)[:150])
# MAGIC         return load()
# MAGIC 
# MAGIC 
# MAGIC load_with_retry()</code></pre></details>

# COMMAND ----------

table = spark.table(EX_BRONZE)
ok = table.count() == 801 and "tip_chf" in table.columns
print("Correct!" if ok else f"Not yet – expected 801 rows with tip_chf, got {table.count()} rows, columns {table.columns}")

# COMMAND ----------

# MAGIC %md
# MAGIC (A second stop would mean something else is wrong. Retrying forever would hide it.)
# MAGIC
# MAGIC ### Task 3.3 – The rescued value ★★
# MAGIC
# MAGIC Find the row with something in `_rescued_data`. Store its `order_id` in `rescued_order` and the original
# MAGIC amount, as the text it was, in `rescued_amount`. `_rescued_data` is a JSON string: `F.from_json` with the
# MAGIC schema `"amount_chf STRING"` gets you the value.

# COMMAND ----------

# TODO Task 3.3
raise NotImplementedError("Task 3.3")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'><code>where(&quot;_rescued_data IS NOT NULL&quot;)</code>, then <code>F.from_json(&quot;_rescued_data&quot;, &quot;amount_chf STRING&quot;)</code> gives a struct with the field <code>amount_chf</code>. <code>.first()</code> returns one row.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>row = (spark.table(EX_BRONZE).where(&quot;_rescued_data IS NOT NULL&quot;)
# MAGIC        .select(&quot;order_id&quot;, F.from_json(&quot;_rescued_data&quot;, &quot;___&quot;).alias(&quot;r&quot;)).first())
# MAGIC rescued_order, rescued_amount = row.order_id, row.r.___</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>row = (spark.table(EX_BRONZE).where(&quot;_rescued_data IS NOT NULL&quot;)
# MAGIC        .select(&quot;order_id&quot;, F.from_json(&quot;_rescued_data&quot;, &quot;amount_chf STRING&quot;).alias(&quot;r&quot;))
# MAGIC        .first())
# MAGIC rescued_order = row.order_id
# MAGIC rescued_amount = row.r.amount_chf
# MAGIC print(rescued_order, rescued_amount)</code></pre></details>

# COMMAND ----------

ok = rescued_order == "o-00501" and rescued_amount == "12,50"
print("Correct!" if ok else f"Not yet – expected o-00501 and 12,50, got {rescued_order} and {rescued_amount}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.4 – How many schema versions? ★
# MAGIC
# MAGIC List the folder `f"{STATE}/_schema/_schemas"` and store the number of schema versions in `n_versions`. Then
# MAGIC print the field names of the newest version: the last line of the file is JSON, its key `dataSchemaJson`
# MAGIC holds the schema, again as a JSON string, with a list `fields`.

# COMMAND ----------

# TODO Task 3.4
raise NotImplementedError("Task 3.4")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'><code>dbutils.fs.ls</code> on the <code>_schemas</code> folder: one file per version. Sort by <code>int(f.name)</code>; the last line of each file is the schema as JSON.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>versions = sorted(dbutils.fs.ls(f&quot;{STATE}/_schema/_schemas&quot;), key=lambda f: int(f.name))
# MAGIC n_versions = ___(versions)</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>versions = sorted(dbutils.fs.ls(f&quot;{STATE}/_schema/_schemas&quot;), key=lambda f: int(f.name))
# MAGIC n_versions = len(versions)
# MAGIC newest = json.loads(dbutils.fs.head(versions[-1].path).splitlines()[-1])
# MAGIC print([f[&quot;name&quot;] for f in json.loads(newest[&quot;dataSchemaJson&quot;])[&quot;fields&quot;]])</code></pre></details>

# COMMAND ----------

print("Correct!" if n_versions == 2 else f"Not yet – expected 2, got {n_versions}")

# COMMAND ----------

# MAGIC %md
# MAGIC Done. More in `3_exercise_extended_schema_changes`, or go on with notebook 04.
