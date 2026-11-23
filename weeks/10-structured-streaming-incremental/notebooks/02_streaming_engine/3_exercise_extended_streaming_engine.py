# Databricks notebook source
# MAGIC %md
# MAGIC # 02 The streaming engine (exercise, extended)
# MAGIC
# MAGIC For at home. Four things that go wrong with streaming queries sooner or later, each with the reason and
# MAGIC the proper way, then a task. At the end a bigger query of your own.
# MAGIC
# MAGIC | Part | Question |
# MAGIC |---|---|
# MAGIC | 1 | What if I forget the checkpoint? |
# MAGIC | 2 | Can two queries share a checkpoint? |
# MAGIC | 3 | What happens when I change a query that already ran? |
# MAGIC | 4 | Why does the plain file source want a schema? |
# MAGIC | 5 | Revenue per restaurant, as a stream |

# COMMAND ----------

# MAGIC %run ../_config

# COMMAND ----------

# MAGIC %run ../_generator

# COMMAND ----------

from pyspark.sql import functions as F

BASE = f"{PRACTICE_VOLUME}/ex02_extended"
DATA = f"{BASE}/data"
for row in spark.sql(f"SHOW TABLES IN {PLAYGROUND} LIKE 'w10_ex02x_*'").collect():
    spark.sql(f"DROP TABLE IF EXISTS {PLAYGROUND}.{row.tableName}")
dbutils.fs.rm(BASE, recurse=True)
write_batch(1, DATA)
write_batch(2, DATA)

ORDER_SCHEMA = ("order_id STRING, customer STRING, restaurant STRING, city STRING, items INT, "
                "amount_chf DOUBLE, status STRING, event_ts STRING")


def first_line(e):
    return str(e).strip().splitlines()[0][:250]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. No checkpoint
# MAGIC
# MAGIC ### What goes wrong
# MAGIC
# MAGIC On a laptop Spark would quietly use a temporary checkpoint folder and forget it afterwards. Every run would
# MAGIC start from scratch, the classic way to load everything twice. On Databricks serverless it refuses:

# COMMAND ----------

try:
    (spark.readStream.schema(ORDER_SCHEMA).json(DATA)
     .writeStream.trigger(availableNow=True)
     .toTable(f"{PLAYGROUND}.w10_ex02x_nocheck"))
except Exception as e:
    print(first_line(e))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Why, and how to do it properly
# MAGIC
# MAGIC Without a checkpoint there is no memory, so no «only what's new». Every streaming query that writes
# MAGIC somewhere gets `checkpointLocation`, on a volume, in a folder that belongs to that query alone.
# MAGIC
# MAGIC ### Task 2.5 – Give it a checkpoint ★
# MAGIC
# MAGIC Run the same query with the checkpoint `f"{BASE}/state/nocheck"`. Store the rows it read in `n_rows`.

# COMMAND ----------

# TODO Task 2.5
raise NotImplementedError("Task 2.5")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>The query from the cell above, plus <code>.option(&quot;checkpointLocation&quot;, ...)</code> after <code>writeStream</code>. Then <code>awaitTermination()</code> and <code>input_rows(q)</code>.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>q = (spark.readStream.schema(ORDER_SCHEMA).json(DATA)
# MAGIC      .writeStream.option(&quot;checkpointLocation&quot;, ___)
# MAGIC      .trigger(availableNow=True).toTable(f&quot;{PLAYGROUND}.w10_ex02x_nocheck&quot;))
# MAGIC q.awaitTermination()
# MAGIC n_rows = input_rows(q)</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>q = (spark.readStream.schema(ORDER_SCHEMA).json(DATA)
# MAGIC      .writeStream.option(&quot;checkpointLocation&quot;, f&quot;{BASE}/state/nocheck&quot;)
# MAGIC      .trigger(availableNow=True)
# MAGIC      .toTable(f&quot;{PLAYGROUND}.w10_ex02x_nocheck&quot;))
# MAGIC q.awaitTermination()
# MAGIC n_rows = input_rows(q)</code></pre></details>

# COMMAND ----------

print("Correct!" if n_rows == 550 else f"Not yet – expected 550, got {n_rows}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Two queries, one checkpoint
# MAGIC
# MAGIC ### What goes wrong
# MAGIC
# MAGIC Someone builds a second table from the same folder and copies the first query, checkpoint path included.
# MAGIC The first query already read both files. What does the second one read?

# COMMAND ----------

def copy_to(table, checkpoint):
    q = (spark.readStream.schema(ORDER_SCHEMA).json(DATA)
         .writeStream.option("checkpointLocation", checkpoint)
         .trigger(availableNow=True)
         .toTable(table))
    q.awaitTermination()
    return input_rows(q)


print("first table :", copy_to(f"{PLAYGROUND}.w10_ex02x_first", f"{BASE}/state/shared"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.6 – Run the second query with the same checkpoint ★
# MAGIC
# MAGIC Call `copy_to` for the table `f"{PLAYGROUND}.w10_ex02x_second"` with the **same** checkpoint
# MAGIC `f"{BASE}/state/shared"`. Store the rows it read in `n_second`. Then look at the table: what's in it?

# COMMAND ----------

# TODO Task 2.6
raise NotImplementedError("Task 2.6")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'><code>copy_to(table, checkpoint)</code> with the new table name and the checkpoint that the first call used.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>n_second = copy_to(f&quot;{PLAYGROUND}.w10_ex02x_second&quot;, f&quot;{BASE}/state/___&quot;)</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>n_second = copy_to(f&quot;{PLAYGROUND}.w10_ex02x_second&quot;, f&quot;{BASE}/state/shared&quot;)
# MAGIC print(n_second)</code></pre></details>

# COMMAND ----------

print("Correct!" if n_second == 0 else f"Not yet – expected 0, got {n_second}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Why, and how to do it properly
# MAGIC
# MAGIC The checkpoint says «both files done». It doesn't know that the second query writes somewhere else. The
# MAGIC second table stays empty, without an error. Worse: from now on the two queries take turns, and each file
# MAGIC ends up in only one of the tables.
# MAGIC
# MAGIC Rule: **one query, one checkpoint**. Name the checkpoint folder after the table it feeds.
# MAGIC
# MAGIC ### Task 2.7 – Fix it ★
# MAGIC
# MAGIC Fill `w10_ex02x_second` with its own checkpoint `f"{BASE}/state/second"`. Store the rows read in `n_fixed`.

# COMMAND ----------

# TODO Task 2.7
raise NotImplementedError("Task 2.7")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>The same call, with a checkpoint folder that only this query uses.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>n_fixed = copy_to(f&quot;{PLAYGROUND}.w10_ex02x_second&quot;, f&quot;{BASE}/state/___&quot;)</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>n_fixed = copy_to(f&quot;{PLAYGROUND}.w10_ex02x_second&quot;, f&quot;{BASE}/state/second&quot;)</code></pre></details>

# COMMAND ----------

print("Correct!" if n_fixed == 550 else f"Not yet – expected 550, got {n_fixed}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Changing a query that already ran
# MAGIC
# MAGIC ### What goes wrong
# MAGIC
# MAGIC A query counts orders per status. Later we'd rather count per restaurant, so we change the `groupBy` and
# MAGIC start it again with the same checkpoint.

# COMMAND ----------

def count_by(column, table, checkpoint):
    q = (spark.readStream.schema(ORDER_SCHEMA).json(DATA)
         .groupBy(column).count()
         .writeStream.outputMode("complete")
         .option("checkpointLocation", checkpoint)
         .trigger(availableNow=True)
         .toTable(table))
    q.awaitTermination()
    return q


count_by("status", f"{PLAYGROUND}.w10_ex02x_counts", f"{BASE}/state/counts")
try:
    count_by("restaurant", f"{PLAYGROUND}.w10_ex02x_counts", f"{BASE}/state/counts")
except Exception as e:
    print(first_line(e))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Why
# MAGIC
# MAGIC The state in the checkpoint holds counts **per status**. The new query expects counts **per restaurant**.
# MAGIC Spark can't turn one into the other, so it stops instead of producing nonsense. Small changes are fine
# MAGIC (a filter, a new column in a `select`). Changes to the grouping, the aggregation, the sources or the sink
# MAGIC need a new checkpoint, and with it a new start.
# MAGIC
# MAGIC ### Task 2.8 – Start over properly ★★
# MAGIC
# MAGIC Run the per-restaurant count into a **new** table `f"{PLAYGROUND}.w10_ex02x_per_restaurant"` with a **new**
# MAGIC checkpoint `f"{BASE}/state/per_restaurant"`. Store the number of restaurants in the result in `n_restaurants`.
# MAGIC
# MAGIC Why a new table and not just a new checkpoint for the old table?

# COMMAND ----------

# TODO Task 2.8
raise NotImplementedError("Task 2.8")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'><code>count_by(&quot;restaurant&quot;, new_table, new_checkpoint)</code>, then count the rows of the new table.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>count_by(&quot;restaurant&quot;, f&quot;{PLAYGROUND}.w10_ex02x_per_restaurant&quot;, f&quot;{BASE}/state/___&quot;)
# MAGIC n_restaurants = spark.table(f&quot;{PLAYGROUND}.w10_ex02x_per_restaurant&quot;).___()</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>count_by(&quot;restaurant&quot;, f&quot;{PLAYGROUND}.w10_ex02x_per_restaurant&quot;, f&quot;{BASE}/state/per_restaurant&quot;)
# MAGIC n_restaurants = spark.table(f&quot;{PLAYGROUND}.w10_ex02x_per_restaurant&quot;).count()</code></pre></details>

# COMMAND ----------

print("Correct!" if n_restaurants == 6 else f"Not yet – expected 6, got {n_restaurants}")

# COMMAND ----------

# MAGIC %md
# MAGIC (A new checkpoint on the old table would work here, because `complete` overwrites the table anyway. With
# MAGIC `append` the new query would add everything again on top of the old rows.)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. The plain file source wants a schema
# MAGIC
# MAGIC ### What goes wrong

# COMMAND ----------

try:
    spark.readStream.json(DATA)
except Exception as e:
    print(first_line(e))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Why
# MAGIC
# MAGIC A batch read can look at all files before it starts and work out the schema. A stream can't: the files of
# MAGIC tomorrow aren't there yet, and the schema of a running query must not change by surprise. So you give it
# MAGIC one. Auto Loader solves this differently, with a schema location (notebook 03).
# MAGIC
# MAGIC ### How to do it properly
# MAGIC
# MAGIC Write the schema down as DDL, like `ORDER_SCHEMA`. Or, to get started, borrow it once from a batch read of
# MAGIC the files that are already there, and then write it down.
# MAGIC
# MAGIC ### Task 2.9 – Borrow the schema ★
# MAGIC
# MAGIC Read `DATA` once as a batch (`spark.read.json`), take its `.schema`, and use it for a streaming copy into
# MAGIC `f"{PLAYGROUND}.w10_ex02x_borrowed"` with checkpoint `f"{BASE}/state/borrowed"`. Store the rows read in
# MAGIC `n_borrowed`. Print the schema: is `event_ts` a timestamp or a string?

# COMMAND ----------

# TODO Task 2.9
raise NotImplementedError("Task 2.9")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'><code>spark.read.json(DATA).schema</code> is a <code>StructType</code>. <code>readStream.schema(...)</code> accepts it just like a DDL string.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>borrowed = spark.read.json(DATA).___
# MAGIC q = (spark.readStream.schema(borrowed).json(DATA)
# MAGIC      .writeStream.option(&quot;checkpointLocation&quot;, f&quot;{BASE}/state/borrowed&quot;)
# MAGIC      .trigger(availableNow=True).toTable(f&quot;{PLAYGROUND}.w10_ex02x_borrowed&quot;))</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>borrowed = spark.read.json(DATA).schema
# MAGIC print(borrowed.simpleString())
# MAGIC q = (spark.readStream.schema(borrowed).json(DATA)
# MAGIC      .writeStream.option(&quot;checkpointLocation&quot;, f&quot;{BASE}/state/borrowed&quot;)
# MAGIC      .trigger(availableNow=True)
# MAGIC      .toTable(f&quot;{PLAYGROUND}.w10_ex02x_borrowed&quot;))
# MAGIC q.awaitTermination()
# MAGIC n_borrowed = input_rows(q)</code></pre></details>

# COMMAND ----------

print("Correct!" if n_borrowed == 550 else f"Not yet – expected 550, got {n_borrowed}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Revenue per restaurant, as a stream
# MAGIC
# MAGIC ### Task 2.10 – Delivered revenue ★★★
# MAGIC
# MAGIC Build a streaming query on `DATA` that keeps, per restaurant, only for orders with status `delivered`:
# MAGIC the number of orders (`orders`), the revenue (`revenue_chf`, rounded to 2 decimals) and the average order
# MAGIC (`avg_chf`, rounded to 2 decimals). Write it to `f"{PLAYGROUND}.w10_ex02x_revenue"` with checkpoint
# MAGIC `f"{BASE}/state/revenue"`. Then add batch 5 to `DATA` and run it again: the result has to be right for all
# MAGIC three files. Store the finished table name in `revenue_table`.

# COMMAND ----------

# TODO Task 2.10
raise NotImplementedError("Task 2.10")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'><code>where</code> before <code>groupBy</code>, then <code>agg</code> with <code>F.count</code>, <code>F.sum</code> and <code>F.avg</code>, each rounded. Output mode <code>complete</code>. Put the query in a function so you can run it twice, with <code>write_batch(5, DATA)</code> in between.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>(spark.readStream.schema(ORDER_SCHEMA).json(DATA)
# MAGIC     .where(&quot;status = &#x27;delivered&#x27;&quot;)
# MAGIC     .groupBy(&quot;restaurant&quot;)
# MAGIC     .agg(F.count(&quot;*&quot;).alias(&quot;orders&quot;),
# MAGIC          F.round(F.sum(&quot;amount_chf&quot;), 2).alias(&quot;revenue_chf&quot;),
# MAGIC          F.round(F.___(&quot;amount_chf&quot;), 2).alias(&quot;avg_chf&quot;))
# MAGIC     .writeStream.outputMode(&quot;___&quot;) ...)</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>def run_revenue():
# MAGIC     q = (spark.readStream.schema(ORDER_SCHEMA).json(DATA)
# MAGIC          .where(&quot;status = &#x27;delivered&#x27;&quot;)
# MAGIC          .groupBy(&quot;restaurant&quot;)
# MAGIC          .agg(F.count(&quot;*&quot;).alias(&quot;orders&quot;),
# MAGIC               F.round(F.sum(&quot;amount_chf&quot;), 2).alias(&quot;revenue_chf&quot;),
# MAGIC               F.round(F.avg(&quot;amount_chf&quot;), 2).alias(&quot;avg_chf&quot;))
# MAGIC          .writeStream.outputMode(&quot;complete&quot;)
# MAGIC          .option(&quot;checkpointLocation&quot;, f&quot;{BASE}/state/revenue&quot;)
# MAGIC          .trigger(availableNow=True)
# MAGIC          .toTable(f&quot;{PLAYGROUND}.w10_ex02x_revenue&quot;))
# MAGIC     q.awaitTermination()
# MAGIC 
# MAGIC 
# MAGIC run_revenue()
# MAGIC write_batch(5, DATA)
# MAGIC run_revenue()
# MAGIC revenue_table = f&quot;{PLAYGROUND}.w10_ex02x_revenue&quot;
# MAGIC display(spark.table(revenue_table).orderBy(&quot;restaurant&quot;))</code></pre></details>

# COMMAND ----------

want = {r.restaurant: (r.orders, r.revenue_chf) for r in
        spark.read.schema(ORDER_SCHEMA).json(DATA).where("status = 'delivered'").groupBy("restaurant")
        .agg(F.count("*").alias("orders"), F.round(F.sum("amount_chf"), 2).alias("revenue_chf")).collect()}
got = {r.restaurant: (r.orders, r.revenue_chf) for r in spark.table(revenue_table).collect()}
print("Correct!" if got == want else f"Not yet – expected {want}, got {got}")

# COMMAND ----------

# MAGIC %md
# MAGIC About the rounding: inside the query Spark keeps the exact sum in the state and rounds only for the output,
# MAGIC so that's fine here. Where it bites is when *you* keep a rounded running total and add to it, run after
# MAGIC run. The small errors add up. Keep exact values, round at the end.
# MAGIC
# MAGIC ## Clean up (optional)

# COMMAND ----------

for row in spark.sql(f"SHOW TABLES IN {PLAYGROUND} LIKE 'w10_ex02x_*'").collect():
    spark.sql(f"DROP TABLE IF EXISTS {PLAYGROUND}.{row.tableName}")
dbutils.fs.rm(BASE, recurse=True)
