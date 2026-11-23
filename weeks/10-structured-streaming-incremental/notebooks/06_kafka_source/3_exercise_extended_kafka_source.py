# Databricks notebook source
# MAGIC %md
# MAGIC # 06 Straight from the topic (exercise, extended)
# MAGIC
# MAGIC For at home. You need parts 1 and 2 of the demo done (topic, `ca.pem`, secret scope).
# MAGIC
# MAGIC | Part | Question |
# MAGIC |---|---|
# MAGIC | 1 | What does `from_json` do with Wednesday's batch? |
# MAGIC | 2 | Can I start somewhere in the middle of the topic? |
# MAGIC | 3 | What if a million messages are waiting? |
# MAGIC | 4 | What happens when Kafka has already deleted messages? |
# MAGIC | 5 | Kafka bronze into silver |

# COMMAND ----------

# MAGIC %run ../_config

# COMMAND ----------

# MAGIC %run ../_generator

# COMMAND ----------

# MAGIC %run ../_kafka

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import Window

BASE = f"{PRACTICE_VOLUME}/ex06_extended"
for row in spark.sql(f"SHOW TABLES IN {PLAYGROUND} LIKE 'w10_ex06x_*'").collect():
    spark.sql(f"DROP TABLE IF EXISTS {PLAYGROUND}.{row.tableName}")
dbutils.fs.rm(BASE, recurse=True)


def kafka_to_table(table, checkpoint, **options):
    # A stream from the topic into `table`, extra source options as keyword arguments. Returns the finished query.
    reader = spark.readStream.format("kafka").options(**KAFKA_OPTIONS).option("subscribe", TOPIC)
    for key, value in options.items():
        reader = reader.option(key, value)
    query = (reader.load()
             .select(F.col("key").cast("string").alias("key"), F.col("value").cast("string").alias("value"),
                     "partition", "offset")
             .writeStream.option("checkpointLocation", checkpoint)
             .trigger(availableNow=True)
             .toTable(table))
    query.awaitTermination()
    return query


# Wednesday's batch, with the tips and the "12,50"
send_batch(3)
RAW = f"{PLAYGROUND}.w10_ex06x_raw"
kafka_to_table(RAW, f"{BASE}/raw", startingOffsets="earliest")
print(spark.table(RAW).count(), "messages in", RAW)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. `from_json` and Wednesday's batch
# MAGIC
# MAGIC ### What goes wrong
# MAGIC
# MAGIC The demo parsed `value` with a schema without `tip_chf`. Batch 3 has tips, and `o-00501` has `"12,50"` as amount.

# COMMAND ----------

OLD_SCHEMA = ("order_id STRING, customer STRING, restaurant STRING, city STRING, items INT, "
              "amount_chf DOUBLE, status STRING, event_ts TIMESTAMP")
parsed_old = spark.table(RAW).select(F.from_json("value", OLD_SCHEMA).alias("o"), "value").select("o.*", "value")
print("columns:", parsed_old.columns)
print("messages with a tip in value:", spark.table(RAW).where(F.col("value").contains("tip_chf")).count())
display(parsed_old.where("order_id = 'o-00501'").select("order_id", "amount_chf", "value"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Why
# MAGIC
# MAGIC `from_json` takes the schema you give it. A field that isn't in it is skipped, a value that doesn't fit becomes
# MAGIC `null`. No error, no rescued data. Auto Loader would have stopped once for the tip and kept `"12,50"` in
# MAGIC `_rescued_data` (notebook 03).
# MAGIC
# MAGIC ### How to do it properly
# MAGIC
# MAGIC Keep the raw `value` in bronze (we do). Parse with a schema that has every field you know, and check for values that
# MAGIC didn't parse: `amount_chf IS NULL` while the raw text has an amount.
# MAGIC
# MAGIC ### Task 6.5 – A better schema and a check ★★
# MAGIC
# MAGIC Parse `RAW` with a schema that also has `tip_chf DOUBLE`, into `parsed`. Store the number of rows with a tip in
# MAGIC `n_tips`. Then store in `broken` the `order_id`s (a sorted list, each once) of rows where `amount_chf` is `null`
# MAGIC although `value` contains `"amount_chf"`.

# COMMAND ----------

# TODO Task 6.5
raise NotImplementedError("Task 6.5")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>Add <code>, tip_chf DOUBLE</code> to the schema string. Broken amounts: <code>amount_chf IS NULL</code> and the raw <code>value</code> still contains <code>amount_chf</code>.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>NEW_SCHEMA = OLD_SCHEMA + &quot;, tip_chf DOUBLE&quot;
# MAGIC parsed = spark.table(RAW).select(F.from_json(&quot;value&quot;, NEW_SCHEMA).alias(&quot;o&quot;), &quot;value&quot;).select(&quot;o.*&quot;, &quot;value&quot;)
# MAGIC broken = sorted({r.order_id for r in parsed.where(&quot;amount_chf IS NULL AND value LIKE &#x27;%___%&#x27;&quot;).collect()})</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>NEW_SCHEMA = OLD_SCHEMA + &quot;, tip_chf DOUBLE&quot;
# MAGIC parsed = spark.table(RAW).select(F.from_json(&quot;value&quot;, NEW_SCHEMA).alias(&quot;o&quot;), &quot;value&quot;).select(&quot;o.*&quot;, &quot;value&quot;)
# MAGIC n_tips = parsed.where(&quot;tip_chf IS NOT NULL&quot;).count()
# MAGIC broken = sorted({r.order_id for r in parsed.where(&quot;amount_chf IS NULL AND value LIKE &#x27;%amount_chf%&#x27;&quot;).collect()})
# MAGIC print(n_tips, broken)</code></pre></details>

# COMMAND ----------

want_tips = spark.table(RAW).where(F.col("value").contains("tip_chf")).count()
ok = n_tips == want_tips and broken == ["o-00501"]
print("Correct!" if ok else f"Not yet – expected {want_tips} tips and ['o-00501'], got {n_tips} and {broken}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Starting in the middle
# MAGIC
# MAGIC ### What goes wrong
# MAGIC
# MAGIC A new table should only get the messages from a certain point on, say since a bug was fixed. `earliest` reads the
# MAGIC whole topic, `latest` nothing that is already there.
# MAGIC
# MAGIC ### How to do it properly
# MAGIC
# MAGIC `startingOffsets` also takes JSON: a start offset per partition, `{"orders": {"0": 120}}`. (`-2` means earliest,
# MAGIC `-1` latest.) It only counts for the first run of a new query, after that the checkpoint decides.
# MAGIC
# MAGIC ### Task 6.6 – The last 10 messages of each partition ★★
# MAGIC
# MAGIC Work out the next offset of every partition from `topic_now()` (the largest offset + 1), and build the JSON that
# MAGIC starts 10 messages before it. Start `kafka_to_table` into `f"{PLAYGROUND}.w10_ex06x_tail"` with checkpoint
# MAGIC `f"{BASE}/tail"` and that `startingOffsets`. Store the number of partitions in `n_partitions`.

# COMMAND ----------

# TODO Task 6.6
raise NotImplementedError("Task 6.6")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>Group <code>topic_now()</code> by <code>partition</code>, take <code>max(offset) + 1</code>. The JSON is <code>{topic: {&quot;0&quot;: start, ...}}</code> with partition numbers as strings. Pass it as <code>startingOffsets=...</code> to <code>kafka_to_table</code>.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>ends = {str(r.partition): r.next for r in topic_now().groupBy(&quot;partition&quot;)
# MAGIC         .agg((F.max(&quot;offset&quot;) + 1).alias(&quot;next&quot;)).collect()}
# MAGIC start = json.dumps({TOPIC: {p: max(n - ___, 0) for p, n in ends.items()}})</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>ends = {str(r.partition): r.next for r in topic_now().groupBy(&quot;partition&quot;).agg((F.max(&quot;offset&quot;) + 1).alias(&quot;next&quot;)).collect()}
# MAGIC start = json.dumps({TOPIC: {p: max(n - 10, 0) for p, n in ends.items()}})
# MAGIC print(start)
# MAGIC kafka_to_table(f&quot;{PLAYGROUND}.w10_ex06x_tail&quot;, f&quot;{BASE}/tail&quot;, startingOffsets=start)
# MAGIC n_partitions = len(ends)</code></pre></details>

# COMMAND ----------

n_tail = spark.table(f"{PLAYGROUND}.w10_ex06x_tail").count()
print("Correct!" if n_tail == 10 * n_partitions else f"Not yet – expected {10 * n_partitions} messages, got {n_tail}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. A million messages waiting
# MAGIC
# MAGIC ### What goes wrong
# MAGIC
# MAGIC The job was off for a weekend. `availableNow` wants to read everything at once, in one huge micro-batch.
# MAGIC
# MAGIC ### How to do it properly
# MAGIC
# MAGIC `maxOffsetsPerTrigger` caps the messages per micro-batch, like `maxFilesPerTrigger` for Auto Loader (notebook 01).
# MAGIC `availableNow` still reads everything, in several batches, each committed on its own.
# MAGIC
# MAGIC ### Task 6.7 – At most 100 per batch ★
# MAGIC
# MAGIC Read the whole topic (`earliest`) into `f"{PLAYGROUND}.w10_ex06x_capped"` with checkpoint `f"{BASE}/capped"` and
# MAGIC `maxOffsetsPerTrigger` = `100`. Store the query in `q_capped` and the largest micro-batch (`max(batch_sizes(q_capped))`)
# MAGIC in `biggest`.

# COMMAND ----------

# TODO Task 6.7
raise NotImplementedError("Task 6.7")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'><code>kafka_to_table</code> passes keyword arguments on as options: <code>startingOffsets=&quot;earliest&quot;</code> and <code>maxOffsetsPerTrigger=&quot;100&quot;</code>. <code>batch_sizes(query)</code> lists the rows per micro-batch.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>q_capped = kafka_to_table(f&quot;{PLAYGROUND}.w10_ex06x_capped&quot;, f&quot;{BASE}/capped&quot;,
# MAGIC                           startingOffsets=&quot;earliest&quot;, maxOffsetsPerTrigger=&quot;___&quot;)
# MAGIC biggest = max(batch_sizes(q_capped))</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>q_capped = kafka_to_table(f&quot;{PLAYGROUND}.w10_ex06x_capped&quot;, f&quot;{BASE}/capped&quot;,
# MAGIC                           startingOffsets=&quot;earliest&quot;, maxOffsetsPerTrigger=&quot;100&quot;)
# MAGIC biggest = max(batch_sizes(q_capped))
# MAGIC print(len(batch_sizes(q_capped)), &quot;micro-batches, the biggest with&quot;, biggest, &quot;messages&quot;)</code></pre></details>

# COMMAND ----------

total = spark.table(f"{PLAYGROUND}.w10_ex06x_capped").count()
ok = biggest <= 100 and total == topic_now().count()
print("Correct!" if ok else f"Not yet – biggest batch {biggest} (max 100), {total} of {topic_now().count()} messages read")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. When Kafka has already deleted messages
# MAGIC
# MAGIC Kafka keeps messages for the **retention time** of the topic (on the Aiven free tier a few days), then deletes
# MAGIC them, whether someone read them or not. If a query was off for longer, the offset in its checkpoint points to
# MAGIC messages that no longer exist.
# MAGIC
# MAGIC By default the query then **fails** (`failOnDataLoss = true`): better a red job than a silent gap. You can set it to
# MAGIC `false`, and the query continues with what is still there, but then you have to fill the gap yourself, usually
# MAGIC from bronze, because bronze has kept every message it ever read. That's the real reason bronze keeps the raw
# MAGIC `value`: it is your copy of the topic.
# MAGIC
# MAGIC No task here: you'd have to wait for the retention time.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Kafka bronze into silver
# MAGIC
# MAGIC ### Task 6.8 – One row per order, from Kafka ★★★
# MAGIC
# MAGIC Build `f"{PLAYGROUND}.w10_ex06x_silver"` (columns `order_id STRING, restaurant STRING, amount_chf DOUBLE,
# MAGIC status STRING, event_ts TIMESTAMP, tip_chf DOUBLE`) from `RAW`, as a stream (`readStream.table(RAW)`,
# MAGIC checkpoint `f"{BASE}/silver"`) with `foreachBatch`: parse `value` with `NEW_SCHEMA`, drop rows with a `null`
# MAGIC amount, keep the newest event per order, and `MERGE` with «only if newer» (notebook 04).
# MAGIC
# MAGIC The topic may contain some batches more than once (every run of these notebooks sends again). Why doesn't that
# MAGIC matter for silver?

# COMMAND ----------

SILVER_K = f"{PLAYGROUND}.w10_ex06x_silver"

# TODO Task 6.8
raise NotImplementedError("Task 6.8")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>In <code>foreachBatch</code>: <code>from_json</code> on <code>value</code>, drop <code>null</code> amounts, <code>row_number</code> per order newest first, keep 1, then the MERGE of notebook 04 with <code>s.event_ts &gt; t.event_ts</code>.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>latest = (batch_df.select(F.from_json(&quot;value&quot;, NEW_SCHEMA).alias(&quot;o&quot;)).select(&quot;o.*&quot;)
# MAGIC           .where(&quot;amount_chf IS NOT NULL&quot;)
# MAGIC           .withColumn(&quot;_rn&quot;, F.row_number().over(newest_first)).where(&quot;_rn = 1&quot;)
# MAGIC           .select(&quot;order_id&quot;, &quot;restaurant&quot;, &quot;amount_chf&quot;, &quot;status&quot;, &quot;event_ts&quot;, &quot;tip_chf&quot;))</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>spark.sql(f&quot;&quot;&quot;CREATE TABLE {SILVER_K} (order_id STRING, restaurant STRING, amount_chf DOUBLE, status STRING,
# MAGIC               event_ts TIMESTAMP, tip_chf DOUBLE)&quot;&quot;&quot;)
# MAGIC newest_first = Window.partitionBy(&quot;order_id&quot;).orderBy(F.col(&quot;event_ts&quot;).desc())
# MAGIC 
# MAGIC 
# MAGIC def to_silver(batch_df, batch_id):
# MAGIC     latest = (batch_df.select(F.from_json(&quot;value&quot;, NEW_SCHEMA).alias(&quot;o&quot;)).select(&quot;o.*&quot;)
# MAGIC               .where(&quot;amount_chf IS NOT NULL&quot;)
# MAGIC               .withColumn(&quot;_rn&quot;, F.row_number().over(newest_first)).where(&quot;_rn = 1&quot;)
# MAGIC               .select(&quot;order_id&quot;, &quot;restaurant&quot;, &quot;amount_chf&quot;, &quot;status&quot;, &quot;event_ts&quot;, &quot;tip_chf&quot;))
# MAGIC     (DeltaTable.forName(batch_df.sparkSession, SILVER_K).alias(&quot;t&quot;)
# MAGIC      .merge(latest.alias(&quot;s&quot;), &quot;t.order_id = s.order_id&quot;)
# MAGIC      .whenMatchedUpdateAll(condition=&quot;s.event_ts &gt; t.event_ts&quot;)
# MAGIC      .whenNotMatchedInsertAll()
# MAGIC      .execute())
# MAGIC 
# MAGIC 
# MAGIC q = (spark.readStream.table(RAW).writeStream.foreachBatch(to_silver)
# MAGIC      .option(&quot;checkpointLocation&quot;, f&quot;{BASE}/silver&quot;).trigger(availableNow=True).start())
# MAGIC q.awaitTermination()
# MAGIC display(spark.table(SILVER_K).groupBy(&quot;status&quot;).count())</code></pre></details>

# COMMAND ----------

want = (spark.table(RAW).select(F.from_json("value", NEW_SCHEMA).alias("o")).select("o.*")
        .where("amount_chf IS NOT NULL").select("order_id").distinct().count())
got = spark.table(SILVER_K).count()
dupes = spark.table(SILVER_K).groupBy("order_id").count().where("count > 1").count()
print("Correct!" if got == want and dupes == 0 else f"Not yet – expected {want} orders once each, got {got} rows, {dupes} orders twice")

# COMMAND ----------

# MAGIC %md
# MAGIC A batch that is in the topic twice brings the same events twice. «Newest per order» picks one of them, and «only
# MAGIC if newer» ignores an event silver already has. Kafka delivers at least once, the `MERGE` makes it exactly once
# MAGIC in silver.
# MAGIC
# MAGIC ## Clean up (optional)

# COMMAND ----------

for row in spark.sql(f"SHOW TABLES IN {PLAYGROUND} LIKE 'w10_ex06x_*'").collect():
    spark.sql(f"DROP TABLE IF EXISTS {PLAYGROUND}.{row.tableName}")
dbutils.fs.rm(BASE, recurse=True)
