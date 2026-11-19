# Databricks notebook source
# MAGIC %md
# MAGIC # 03 When the files change shape (exercise, extended)
# MAGIC
# MAGIC For at home. The demo used `addNewColumns`. Here you try the other modes on the same Wednesday file, give
# MAGIC Auto Loader a hint about types, and at the end keep broken rows out of the way without losing them.
# MAGIC
# MAGIC | Part | Question |
# MAGIC |---|---|
# MAGIC | 1 | What do the other schema evolution modes do with `tip_chf`? |
# MAGIC | 2 | Can I tell Auto Loader which type a column has? |
# MAGIC | 3 | Where do broken rows go? |

# COMMAND ----------

# MAGIC %run ../_config

# COMMAND ----------

# MAGIC %run ../_generator

# COMMAND ----------

from pyspark.sql import functions as F

BASE = f"{PRACTICE_VOLUME}/ex03_extended"
for row in spark.sql(f"SHOW TABLES IN {PLAYGROUND} LIKE 'w10_ex03x_*'").collect():
    spark.sql(f"DROP TABLE IF EXISTS {PLAYGROUND}.{row.tableName}")
dbutils.fs.rm(BASE, recurse=True)


def auto_loader(name, **options):
    # Auto Loader on f"{BASE}/{name}/files" into the table w10_ex03x_<name>, with its own state folder.
    # Extra options as keyword arguments, e.g. **{"cloudFiles.schemaEvolutionMode": "rescue"}.
    # run_stream_once (from _config) raises a RuntimeError if the stream stops.
    return run_stream_once(f"{PLAYGROUND}.w10_ex03x_{name}", f"{BASE}/{name}_state",
                           folder=f"{BASE}/{name}/files", **options)


def monday_to_wednesday(name, **options):
    # Batches 1 and 2, a run, then batch 3 and up to two more runs. Returns how many runs failed.
    for batch in (1, 2):
        write_batch(batch, f"{BASE}/{name}/files")
    auto_loader(name, **options)
    write_batch(3, f"{BASE}/{name}/files")
    failures = 0
    for _ in range(2):
        try:
            auto_loader(name, **options)
        except RuntimeError:
            failures += 1
    return failures

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Four modes, one Wednesday
# MAGIC
# MAGIC ### What goes wrong
# MAGIC
# MAGIC `addNewColumns` stops the stream once. In a pipeline without retries, that stop wakes someone up at night.
# MAGIC So people switch to another mode without knowing what it does with the data.
# MAGIC
# MAGIC ### Why
# MAGIC
# MAGIC Every mode answers the question «a field I don't know, what now?» differently. None of them is wrong, but
# MAGIC each one puts the tip somewhere else, or nowhere.
# MAGIC
# MAGIC ### How to do it properly
# MAGIC
# MAGIC Choose on purpose. `monday_to_wednesday(name, ...)` above loads batches 1 and 2, then batch 3, with up to two
# MAGIC tries, and tells you how many runs failed. Look at the table afterwards.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.5 – Mode `rescue` ★★
# MAGIC
# MAGIC Run `monday_to_wednesday("rescue", ...)` with `cloudFiles.schemaEvolutionMode` = `rescue`. Store the number of
# MAGIC failed runs in `fails_rescue`. Then count the rows of `w10_ex03x_rescue` whose `_rescued_data` contains
# MAGIC `tip_chf` and store it in `tips_rescued`. Is there a column `tip_chf`?

# COMMAND ----------

# TODO Task 3.5
raise NotImplementedError("Task 3.5")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'><code>monday_to_wednesday(&quot;rescue&quot;, **{&quot;cloudFiles.schemaEvolutionMode&quot;: &quot;rescue&quot;})</code> returns the failed runs. Then filter the table on <code>F.col(&quot;_rescued_data&quot;).contains(&quot;tip_chf&quot;)</code>.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>fails_rescue = monday_to_wednesday(&quot;rescue&quot;, **{&quot;cloudFiles.schemaEvolutionMode&quot;: &quot;___&quot;})
# MAGIC t = spark.table(f&quot;{PLAYGROUND}.w10_ex03x_rescue&quot;)
# MAGIC tips_rescued = t.where(F.col(&quot;_rescued_data&quot;).contains(&quot;___&quot;)).count()</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>fails_rescue = monday_to_wednesday(&quot;rescue&quot;, **{&quot;cloudFiles.schemaEvolutionMode&quot;: &quot;rescue&quot;})
# MAGIC t = spark.table(f&quot;{PLAYGROUND}.w10_ex03x_rescue&quot;)
# MAGIC tips_rescued = t.where(F.col(&quot;_rescued_data&quot;).contains(&quot;tip_chf&quot;)).count()
# MAGIC print(fails_rescue, tips_rescued, &quot;tip_chf&quot; in t.columns)</code></pre></details>

# COMMAND ----------

ok = fails_rescue == 0 and tips_rescued == 150
print("Correct!" if ok else f"Not yet – expected 0 failures and 150 rescued tips, got {fails_rescue} and {tips_rescued}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.6 – Mode `failOnNewColumns` ★
# MAGIC
# MAGIC The same with `failOnNewColumns`. Store the failed runs in `fails_strict`. How many rows made it into
# MAGIC `w10_ex03x_strict`? When would you want a pipeline that behaves like this?

# COMMAND ----------

# TODO Task 3.6
raise NotImplementedError("Task 3.6")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>The same helper with the name <code>strict</code> and the mode <code>failOnNewColumns</code>.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>fails_strict = monday_to_wednesday(&quot;strict&quot;, **{&quot;cloudFiles.schemaEvolutionMode&quot;: &quot;___&quot;})</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>fails_strict = monday_to_wednesday(&quot;strict&quot;, **{&quot;cloudFiles.schemaEvolutionMode&quot;: &quot;failOnNewColumns&quot;})
# MAGIC print(fails_strict, spark.table(f&quot;{PLAYGROUND}.w10_ex03x_strict&quot;).count())</code></pre></details>

# COMMAND ----------

print("Correct!" if fails_strict == 2 else f"Not yet – expected 2 failed runs, got {fails_strict}")

# COMMAND ----------

# MAGIC %md
# MAGIC (When the schema is a contract and a change has to go through a review first. Then someone *should* look at
# MAGIC it before the data moves on.)
# MAGIC
# MAGIC ### Task 3.7 – Mode `none` ★
# MAGIC
# MAGIC And `none`. Store the failed runs in `fails_none` and the number of rows where the tip survived anywhere
# MAGIC (as a column or inside `_rescued_data`) in `tips_kept`.

# COMMAND ----------

# TODO Task 3.7
raise NotImplementedError("Task 3.7")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>Mode <code>none</code>. Then look for the tip in two places: a column <code>tip_chf</code> (if there is one) and inside <code>_rescued_data</code> (if there is that column).</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>fails_none = monday_to_wednesday(&quot;none&quot;, **{&quot;cloudFiles.schemaEvolutionMode&quot;: &quot;none&quot;})
# MAGIC t = spark.table(f&quot;{PLAYGROUND}.w10_ex03x_none&quot;)
# MAGIC tips_kept = 0
# MAGIC if &quot;_rescued_data&quot; in t.columns:
# MAGIC     tips_kept += t.where(F.col(&quot;_rescued_data&quot;).contains(&quot;tip_chf&quot;)).count()</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>fails_none = monday_to_wednesday(&quot;none&quot;, **{&quot;cloudFiles.schemaEvolutionMode&quot;: &quot;none&quot;})
# MAGIC t = spark.table(f&quot;{PLAYGROUND}.w10_ex03x_none&quot;)
# MAGIC tips_kept = t.where(F.col(&quot;_rescued_data&quot;).contains(&quot;tip_chf&quot;)).count() if &quot;_rescued_data&quot; in t.columns else 0
# MAGIC if &quot;tip_chf&quot; in t.columns:
# MAGIC     tips_kept += t.where(&quot;tip_chf IS NOT NULL&quot;).count()
# MAGIC print(fails_none, tips_kept)</code></pre></details>

# COMMAND ----------

ok = fails_none == 0 and tips_kept == 0
print("Correct!" if ok else f"Not yet – expected 0 and 0, got {fails_none} and {tips_kept}")

# COMMAND ----------

# MAGIC %md
# MAGIC `none` is the fixed schema of the demo, part 2, again: green job, tips gone.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Telling Auto Loader the type
# MAGIC
# MAGIC ### What goes wrong
# MAGIC
# MAGIC Inference guesses from the first files. `event_ts` looks like text to it, so bronze has it as a string, and
# MAGIC every later step has to convert it. If the first files had been unlucky, even `amount_chf` could have ended up
# MAGIC as a string.
# MAGIC
# MAGIC ### How to do it properly
# MAGIC
# MAGIC Let it infer the rest, but give a **hint** for the columns you care about:
# MAGIC `cloudFiles.schemaHints = "event_ts TIMESTAMP, amount_chf DOUBLE"`.
# MAGIC
# MAGIC ### Task 3.8 – Hints ★★
# MAGIC
# MAGIC Run `monday_to_wednesday("hints", ...)` with `addNewColumns` and the schema hints above. Store the data type of
# MAGIC `event_ts` in `w10_ex03x_hints` as a string in `ts_type` (`dict(table.dtypes)` helps). Is `o-00501` still rescued?

# COMMAND ----------

# TODO Task 3.8
raise NotImplementedError("Task 3.8")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>Two options: the mode <code>addNewColumns</code> and <code>cloudFiles.schemaHints</code>. <code>dict(t.dtypes)</code> maps column names to type names.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>monday_to_wednesday(&quot;hints&quot;, **{&quot;cloudFiles.schemaEvolutionMode&quot;: &quot;addNewColumns&quot;,
# MAGIC                                 &quot;cloudFiles.schemaHints&quot;: &quot;___&quot;})
# MAGIC ts_type = dict(spark.table(f&quot;{PLAYGROUND}.w10_ex03x_hints&quot;).dtypes)[&quot;___&quot;]</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>monday_to_wednesday(&quot;hints&quot;, **{&quot;cloudFiles.schemaEvolutionMode&quot;: &quot;addNewColumns&quot;,
# MAGIC                                 &quot;cloudFiles.schemaHints&quot;: &quot;event_ts TIMESTAMP, amount_chf DOUBLE&quot;})
# MAGIC t = spark.table(f&quot;{PLAYGROUND}.w10_ex03x_hints&quot;)
# MAGIC ts_type = dict(t.dtypes)[&quot;event_ts&quot;]
# MAGIC print(ts_type, t.where(&quot;_rescued_data IS NOT NULL&quot;).select(&quot;order_id&quot;).collect())</code></pre></details>

# COMMAND ----------

print("Correct!" if ts_type == "timestamp" else f"Not yet – expected timestamp, got {ts_type}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Broken rows out of the way, but not lost
# MAGIC
# MAGIC ### What goes wrong
# MAGIC
# MAGIC Bronze has `o-00501` with an empty amount. If it goes on to silver and gold like that, the revenue is a bit
# MAGIC too low and nobody knows why. If you simply filter it out, it's gone.
# MAGIC
# MAGIC ### How to do it properly
# MAGIC
# MAGIC A **quarantine** table: rows with something in `_rescued_data` go there, with the reason, the others go on.
# MAGIC Someone looks at the quarantine, fixes the source, and the rows can be loaded again. `foreachBatch` (notebook
# MAGIC 04) lets one streaming query write to two tables.
# MAGIC
# MAGIC ### Task 3.9 – A quarantine ★★★
# MAGIC
# MAGIC Write `split_batch(batch_df, batch_id)`: rows **with** `_rescued_data` are appended to
# MAGIC `f"{PLAYGROUND}.w10_ex03x_quarantine"`, rows without to `f"{PLAYGROUND}.w10_ex03x_clean"`. Then stream the
# MAGIC table `w10_ex03x_hints` from Task 3.8 through it (`readStream.table(...)`, `writeStream.foreachBatch(split_batch)`,
# MAGIC checkpoint `f"{BASE}/split_state"`, `availableNow`).

# COMMAND ----------

# TODO Task 3.9
raise NotImplementedError("Task 3.9")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>In <code>split_batch</code>, two plain batch writes: one with <code>where(&quot;_rescued_data IS NOT NULL&quot;)</code>, one with <code>IS NULL</code>, both <code>mode(&quot;append&quot;)</code>. Start the stream with <code>.foreachBatch(split_batch)</code> and <code>.start()</code>.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>def split_batch(batch_df, batch_id):
# MAGIC     batch_df.where(&quot;___&quot;).write.mode(&quot;append&quot;).saveAsTable(...quarantine)
# MAGIC     batch_df.where(&quot;___&quot;).write.mode(&quot;append&quot;).saveAsTable(...clean)
# MAGIC 
# MAGIC q = (spark.readStream.table(f&quot;{PLAYGROUND}.w10_ex03x_hints&quot;)
# MAGIC      .writeStream.foreachBatch(___)
# MAGIC      .option(&quot;checkpointLocation&quot;, f&quot;{BASE}/split_state&quot;)
# MAGIC      .trigger(availableNow=True).start())</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>def split_batch(batch_df, batch_id):
# MAGIC     batch_df.where(&quot;_rescued_data IS NOT NULL&quot;).write.mode(&quot;append&quot;).option(&quot;mergeSchema&quot;, &quot;true&quot;) \
# MAGIC         .saveAsTable(f&quot;{PLAYGROUND}.w10_ex03x_quarantine&quot;)
# MAGIC     batch_df.where(&quot;_rescued_data IS NULL&quot;).write.mode(&quot;append&quot;).option(&quot;mergeSchema&quot;, &quot;true&quot;) \
# MAGIC         .saveAsTable(f&quot;{PLAYGROUND}.w10_ex03x_clean&quot;)
# MAGIC 
# MAGIC 
# MAGIC q = (spark.readStream.table(f&quot;{PLAYGROUND}.w10_ex03x_hints&quot;)
# MAGIC      .writeStream.foreachBatch(split_batch)
# MAGIC      .option(&quot;checkpointLocation&quot;, f&quot;{BASE}/split_state&quot;)
# MAGIC      .trigger(availableNow=True)
# MAGIC      .start())
# MAGIC q.awaitTermination()</code></pre></details>

# COMMAND ----------

n_q = spark.table(f"{PLAYGROUND}.w10_ex03x_quarantine").count()
n_c = spark.table(f"{PLAYGROUND}.w10_ex03x_clean").count()
print("Correct!" if (n_q, n_c) == (1, 800) else f"Not yet – expected 1 in quarantine and 800 clean, got {n_q} and {n_c}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean up (optional)

# COMMAND ----------

for row in spark.sql(f"SHOW TABLES IN {PLAYGROUND} LIKE 'w10_ex03x_*'").collect():
    spark.sql(f"DROP TABLE IF EXISTS {PLAYGROUND}.{row.tableName}")
dbutils.fs.rm(BASE, recurse=True)
