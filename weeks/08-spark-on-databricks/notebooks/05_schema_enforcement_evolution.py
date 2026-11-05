# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 05 – Schema enforcement and schema evolution
# MAGIC
# MAGIC **Slides:** *Schema enforcement: two MERGEs, one new column* · *Schema evolution: the table grows with the data*
# MAGIC
# MAGIC **Goal (≈ 20 min):** replay the two slides on a real Delta table.
# MAGIC
# MAGIC | Section | What happens |
# MAGIC |---|---|
# MAGIC | 1 | MERGE with the same schema: works |
# MAGIC | 2 | A source with one column more (`cbd_fee`): `append` is **rejected** |
# MAGIC | 3 | The same source in a plain `MERGE`: runs – but `cbd_fee` is **silently dropped** |
# MAGIC | 4 | `MERGE WITH SCHEMA EVOLUTION`: the table **grows** a column, old rows get `NULL` |
# MAGIC | 5 | Exercises |
# MAGIC
# MAGIC Needs only the catalog from `00_setup`.

# COMMAND ----------

import importlib

import config

importlib.reload(config)   # picks up a change to config.py without restarting Python
catalog = config.CATALOG

spark.sql(f"USE CATALOG `{catalog}`")
spark.sql("USE SCHEMA playground")

TABLE = "silver_trips_schema_demo"
spark.sql(f"DROP TABLE IF EXISTS {TABLE}")
spark.sql(f"CREATE TABLE {TABLE} (trip_id INT, fare DOUBLE, tip DOUBLE)")
spark.sql(f"INSERT INTO {TABLE} VALUES (1, 12.5, 2.0), (2, 8.0, 0.0), (3, 23.0, 5.0)")
spark.table(TABLE).orderBy("trip_id").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 – MERGE with the same schema
# MAGIC
# MAGIC Trips 4 and 5 are new. Same columns, same types: the MERGE just works.

# COMMAND ----------

spark.createDataFrame([(4, 15.0, 3.5), (5, 9.5, 1.0)], "trip_id int, fare double, tip double") \
    .createOrReplaceTempView("source_same_schema")

spark.sql(f"""
    MERGE INTO {TABLE} AS t
    USING source_same_schema AS s ON t.trip_id = s.trip_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""").display()
spark.table(TABLE).orderBy("trip_id").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 – One column more: `append` is rejected
# MAGIC
# MAGIC Since 2025 the TLC charges the **congestion fee** for the central business district. The new delivery has a column
# MAGIC `cbd_fee`. A plain append does not quietly accept it – **schema enforcement**:

# COMMAND ----------

source_new_column = spark.createDataFrame(
    [(4, 15.0, 3.5, 0.75), (5, 9.5, 1.0, 0.75)], "trip_id int, fare double, tip double, cbd_fee double")
source_new_column.createOrReplaceTempView("source_new_column")

try:
    source_new_column.write.mode("append").saveAsTable(TABLE)
except Exception as e:
    print(type(e).__name__)
    print(str(e)[:600])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 – The same source in a plain MERGE: the column is silently dropped
# MAGIC
# MAGIC `UPDATE SET *` / `INSERT *` mean "all columns **of the target**". Without schema evolution the MERGE runs, but the
# MAGIC values of `cbd_fee` are lost – no error, no warning. That is the dangerous case.

# COMMAND ----------

spark.sql(f"""
    MERGE INTO {TABLE} AS t
    USING source_new_column AS s ON t.trip_id = s.trip_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
print("columns now:", spark.table(TABLE).columns)
spark.table(TABLE).orderBy("trip_id").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 – `MERGE WITH SCHEMA EVOLUTION`: the table grows with the data
# MAGIC
# MAGIC You allow the change **per statement**, not by accident. The new column is added to the table schema; the old rows
# MAGIC read `NULL` – their files are not rewritten.

# COMMAND ----------

spark.sql(f"""
    MERGE WITH SCHEMA EVOLUTION INTO {TABLE} AS t
    USING source_new_column AS s ON t.trip_id = s.trip_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
print("columns now:", spark.table(TABLE).columns)
spark.table(TABLE).orderBy("trip_id").display()

# COMMAND ----------

spark.sql(f"DESCRIBE HISTORY {TABLE}").select("version", "operation", "operationParameters").orderBy("version").display()

# COMMAND ----------

# MAGIC %md
# MAGIC Adding a column is evolution. Renaming, dropping or changing a type are separate `ALTER TABLE` operations
# MAGIC (rename and drop need column mapping on Delta).
# MAGIC
# MAGIC ## 5 – Exercises

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 5.1 – Evolution on append
# MAGIC
# MAGIC A new delivery has trip 6 with a column `airport_fee`. Append it to the table **with** schema evolution for this one
# MAGIC write (`.option("mergeSchema", "true")`) and show the result. Which value do trips 1–5 get in `airport_fee`?

# COMMAND ----------

# TODO Task 5.1 – append with mergeSchema
raise NotImplementedError("Task 5.1")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>Schema evolution can be allowed for a single write with <code>.option(&quot;mergeSchema&quot;, &quot;true&quot;)</code> before <code>.saveAsTable(...)</code>.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>delivery = spark.createDataFrame([(6, 52.0, 10.0, 0.75, 1.75)],
# MAGIC     &quot;trip_id int, fare double, tip double, cbd_fee double, airport_fee double&quot;)
# MAGIC delivery.write.mode(&quot;append&quot;).option(&quot;___&quot;, &quot;true&quot;).saveAsTable(TABLE)</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>delivery = spark.createDataFrame([(6, 52.0, 10.0, 0.75, 1.75)],
# MAGIC                                  &quot;trip_id int, fare double, tip double, cbd_fee double, airport_fee double&quot;)
# MAGIC delivery.write.mode(&quot;append&quot;).option(&quot;mergeSchema&quot;, &quot;true&quot;).saveAsTable(TABLE)
# MAGIC spark.table(TABLE).orderBy(&quot;trip_id&quot;).display()   # trips 1–5: airport_fee is NULL</code></pre></details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 5.2 – A wrong type
# MAGIC
# MAGIC Append two deliveries where `fare` arrives as a **string**: trip 7 with `"18.0"` and trip 8 with `"$18.50"`. Wrap
# MAGIC each append in `try/except` and print what happens. Which one gets in – and why is that not the same as "the types
# MAGIC are checked"?

# COMMAND ----------

# TODO Task 5.2 – append strings into a double column
raise NotImplementedError("Task 5.2")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>Create each DataFrame with the schema string <code>&quot;trip_id int, fare string, tip double&quot;</code>, loop over the two deliveries and wrap each append in <code>try/except</code>.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>for trip_id, fare in ((7, &quot;18.0&quot;), (8, &quot;$18.50&quot;)):
# MAGIC     delivery = spark.createDataFrame([(trip_id, fare, 2.0)], &quot;trip_id int, fare string, tip double&quot;)
# MAGIC     try:
# MAGIC         delivery.write.mode(&quot;append&quot;).saveAsTable(TABLE)
# MAGIC         print(f&quot;trip {trip_id}: appended – {fare!r} was cast to double&quot;)
# MAGIC     except Exception as e:
# MAGIC         print(f&quot;trip {trip_id}: rejected –&quot;, type(e).__name__, str(e).splitlines()[0][:150])
# MAGIC # &quot;18.0&quot; is cast silently (the column type wins), &quot;$18.50&quot; cannot be cast and fails.
# MAGIC # Enforcement checks the column names and whether a value fits the target type – not what the supplier meant.</code></pre></details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 5.3 – Your call
# MAGIC
# MAGIC For each case, decide: **enforce** (reject) or **evolve** (accept)? Write your answer as a comment and justify it.
# MAGIC
# MAGIC 1. The supplier adds an optional column `driver_rating`.
# MAGIC 2. A column `fare` suddenly arrives as text with a currency symbol (`"$12.50"`).
# MAGIC 3. A column `pickup_zone` is missing in today's file.

# COMMAND ----------

# TODO Task 5.3 – enforce or evolve
raise NotImplementedError("Task 5.3")
# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>Ask for each case: does accepting it break existing queries, or hide a broken delivery?</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code># 1. evolve – a new optional column does not break existing queries; old rows read NULL.
# MAGIC # 2. enforce – a changed type means the source changed its contract; parse it in silver, do not let text into a double.
# MAGIC # 3. enforce – a missing column usually means a broken delivery; stop and ask instead of loading NULLs.</code></pre></details>

