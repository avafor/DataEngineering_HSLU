# Databricks notebook source
# MAGIC %md
# MAGIC # 06 Straight from the topic (exercise, short)
# MAGIC
# MAGIC About 10 minutes. You need parts 1 and 2 of the demo done: the topic `orders`, `ca.pem` in the volume, the secret
# MAGIC scope `aiven`. `_kafka` reads them and gives you `KAFKA_OPTIONS`, `TOPIC`, `send_batch(n)` and `topic_now()`.

# COMMAND ----------

# MAGIC %run ../_config

# COMMAND ----------

# MAGIC %run ../_generator

# COMMAND ----------

# MAGIC %run ../_kafka

# COMMAND ----------

EX_TABLE = f"{PLAYGROUND}.w10_ex06_kafka"
STATE = f"{PRACTICE_VOLUME}/ex06_short_state"
spark.sql(f"DROP TABLE IF EXISTS {EX_TABLE}")
dbutils.fs.rm(STATE, recurse=True)

in_topic = topic_now().count()
print(in_topic, "messages in the topic right now")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 6.1 – Read the whole topic ★
# MAGIC
# MAGIC Complete `read_topic()`: a stream from `TOPIC` with `KAFKA_OPTIONS`, starting at the **beginning** of the topic.
# MAGIC Keep `key` and `value` as strings, plus `partition` and `offset`. Append to `EX_TABLE`, checkpoint
# MAGIC `f"{STATE}/_checkpoint"`, `availableNow`. It returns the rows read. Call it and store the result in `n_first`.

# COMMAND ----------

# TODO Task 6.1
def read_topic():
    query = (
        spark.readStream.format("___")
        .options(**KAFKA_OPTIONS)
        .option("subscribe", ___)
        .option("startingOffsets", "___")
        .load()
        .select(F.col("key").cast("string").alias("key"),
                F.col("value").cast("string").alias("value"),
                "partition", "offset")
        .writeStream
        .option("checkpointLocation", f"{STATE}/_checkpoint")
        .trigger(availableNow=True)
        .toTable(EX_TABLE)
    )
    query.awaitTermination()
    return input_rows(query)


n_first = read_topic()
print(n_first)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>The format is <code>kafka</code>, the topic goes into <code>subscribe</code>, and <code>startingOffsets = earliest</code> starts at the beginning of the topic.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>spark.readStream.format(&quot;kafka&quot;)
# MAGIC     .options(**KAFKA_OPTIONS)
# MAGIC     .option(&quot;subscribe&quot;, TOPIC)
# MAGIC     .option(&quot;startingOffsets&quot;, &quot;___&quot;)</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>def read_topic():
# MAGIC     query = (
# MAGIC         spark.readStream.format(&quot;kafka&quot;)
# MAGIC         .options(**KAFKA_OPTIONS)
# MAGIC         .option(&quot;subscribe&quot;, TOPIC)
# MAGIC         .option(&quot;startingOffsets&quot;, &quot;earliest&quot;)
# MAGIC         .load()
# MAGIC         .select(F.col(&quot;key&quot;).cast(&quot;string&quot;).alias(&quot;key&quot;),
# MAGIC                 F.col(&quot;value&quot;).cast(&quot;string&quot;).alias(&quot;value&quot;),
# MAGIC                 &quot;partition&quot;, &quot;offset&quot;)
# MAGIC         .writeStream
# MAGIC         .option(&quot;checkpointLocation&quot;, f&quot;{STATE}/_checkpoint&quot;)
# MAGIC         .trigger(availableNow=True)
# MAGIC         .toTable(EX_TABLE)
# MAGIC     )
# MAGIC     query.awaitTermination()
# MAGIC     return input_rows(query)
# MAGIC 
# MAGIC 
# MAGIC n_first = read_topic()
# MAGIC print(n_first)</code></pre></details>

# COMMAND ----------

print("Correct!" if n_first == in_topic else f"Not yet – the topic has {in_topic} messages, you read {n_first}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 6.2 – Nothing new ★
# MAGIC
# MAGIC Call `read_topic()` again and store the result in `n_again`. Why does `startingOffsets = earliest` not read
# MAGIC everything a second time?

# COMMAND ----------

# TODO Task 6.2
raise NotImplementedError("Task 6.2")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>The same call once more. <code>startingOffsets</code> only counts for the very first run of a query.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>n_again = ___()</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>n_again = read_topic()</code></pre></details>

# COMMAND ----------

print("Correct!" if n_again == 0 else f"Not yet – expected 0, got {n_again}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 6.3 – Saturday's orders ★
# MAGIC
# MAGIC Send batch 5 with `send_batch(5)`, then read again. Store the rows read in `n_saturday`.

# COMMAND ----------

# TODO Task 6.3
raise NotImplementedError("Task 6.3")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'><code>send_batch(5)</code> from <code>_kafka</code> sends the 99 events. Then <code>read_topic()</code>.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>send_batch(___)
# MAGIC n_saturday = read_topic()</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>send_batch(5)
# MAGIC n_saturday = read_topic()</code></pre></details>

# COMMAND ----------

print("Correct!" if n_saturday == 99 else f"Not yet – batch 5 has 99 events, you read {n_saturday}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 6.4 – The bookmark ★★
# MAGIC
# MAGIC Read the newest file in `f"{STATE}/_checkpoint/offsets"` (sort by `int(f.name)`). Its last line is JSON:
# MAGIC `{"orders": {"<partition>": <next offset>, ...}}`. Store the sum of all next offsets in `next_total`.
# MAGIC
# MAGIC What does that number have to do with the number of messages in the topic?

# COMMAND ----------

# TODO Task 6.4
raise NotImplementedError("Task 6.4")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>List the offsets folder, take the file with the largest number, its last line is JSON. <code>json.loads</code> gives a dict <code>{topic: {partition: offset}}</code>; sum the inner values.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>newest = sorted(dbutils.fs.ls(f&quot;{STATE}/_checkpoint/offsets&quot;), key=lambda f: int(f.name))[-1]
# MAGIC offsets = json.loads(dbutils.fs.head(newest.path).splitlines()[___])
# MAGIC next_total = sum(offsets[TOPIC].___())</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>newest = sorted(dbutils.fs.ls(f&quot;{STATE}/_checkpoint/offsets&quot;), key=lambda f: int(f.name))[-1]
# MAGIC offsets = json.loads(dbutils.fs.head(newest.path).splitlines()[-1])
# MAGIC next_total = sum(offsets[TOPIC].values())
# MAGIC print(offsets, next_total)</code></pre></details>

# COMMAND ----------

want = topic_now().groupBy("partition").agg(F.max("offset").alias("m")).agg(F.sum(F.col("m") + 1)).first()[0]
print("Correct!" if next_total == want else f"Not yet – expected {want}, got {next_total}")

# COMMAND ----------

# MAGIC %md
# MAGIC The next offset of a partition is the number of messages that partition ever got. As long as Kafka hasn't deleted
# MAGIC old ones, the sum is the number of messages in the topic.
# MAGIC
# MAGIC Done. More in `3_exercise_extended_kafka_source`, or go on with notebook 07.
