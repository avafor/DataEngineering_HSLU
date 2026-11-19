# Databricks notebook source
# MAGIC %md
# MAGIC # 06 Straight from the topic (demo)
# MAGIC
# MAGIC In week 9 Anna's order was an event in a Kafka topic. All of this week, the events reached us as files in a
# MAGIC landing folder: someone (a Kafka connector) read the topic and wrote files, and we read the files. That's a
# MAGIC detour. The question for this notebook: **can Spark read the topic directly, and still load every event exactly
# MAGIC once?**
# MAGIC
# MAGIC ![The app sends orders to the topic orders on Aiven, a streaming query with availableNow reads them into bronze.orders_kafka, the checkpoint keeps the next offset per partition](./assets/pipeline_kafka.png)
# MAGIC
# MAGIC You need the free Kafka service on Aiven from the
# MAGIC [preparation](https://github.com/avafor/DataEngineering_HSLU/blob/main/preparation/week-10.md), status **Running**.
# MAGIC This notebook doesn't need notebooks 01 to 05, only `00_setup`.

# COMMAND ----------

# MAGIC %run ../_config

# COMMAND ----------

# MAGIC %run ../_generator

# COMMAND ----------

import json
import os
import socket

from pyspark.sql import functions as F

TOPIC = "orders"
SECRET_SCOPE = "aiven"
CA_PATH = f"/Volumes/{CATALOG}/landing/aiven/ca.pem"
KAFKA_BRONZE = f"{CATALOG}.bronze.orders_kafka"
KAFKA_CHECKPOINT = f"{STATE_VOLUME}/bronze_orders_kafka/_checkpoint"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Without Spark's Kafka source
# MAGIC
# MAGIC Two ways people do it.
# MAGIC
# MAGIC **A connector writes files.** Kafka Connect (or the app itself) writes the events into files every few minutes,
# MAGIC and Auto Loader takes it from there. It works, it's what we did in notebooks 01 to 05. But it's a second system to
# MAGIC run, the files are small and many, and every event waits for the next file.
# MAGIC
# MAGIC **A consumer of your own.** Like the consumers in week 9, in Python, writing into Delta:
# MAGIC
# MAGIC ```python
# MAGIC consumer = Consumer({"group.id": "bronze-loader", "enable.auto.commit": False, ...})
# MAGIC consumer.subscribe(["orders"])
# MAGIC while True:
# MAGIC     messages = consumer.consume(500, timeout=5)
# MAGIC     write_to_delta(messages)          # 1. the data is in the table
# MAGIC     consumer.commit()                 # 2. Kafka learns how far we got
# MAGIC ```
# MAGIC
# MAGIC Notebook 01 again. If the job dies between 1 and 2, Kafka doesn't know the messages were written, and the next
# MAGIC start writes them again. Commit first, and a crash loses them instead. The offset and the data live in two
# MAGIC systems, and no transaction covers both.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Connecting to your Kafka service
# MAGIC
# MAGIC ### The topic and the certificate
# MAGIC
# MAGIC If you did part 5 of the [preparation](https://github.com/avafor/DataEngineering_HSLU/blob/main/preparation/week-10.md#5-prepare-the-connection),
# MAGIC you have the topic and `ca.pem` already, skip to the upload below. Otherwise, in the Aiven Console, open your
# MAGIC Kafka service:
# MAGIC
# MAGIC 1. **Service settings** → **Advanced configuration** → **Configure** → `kafka_authentication_methods.sasl` =
# MAGIC    `true` → **Save changes**. A new service only accepts client certificates, we log in with user and password
# MAGIC    (SASL)
# MAGIC
# MAGIC    ![Aiven dialog Advanced configuration: kafka_authentication_methods.sasl set to true, button Save changes](./assets/aiven_sasl_enable.png)
# MAGIC
# MAGIC 2. **Topics** → **Create topic**, name `orders`, leave replication and partitions empty, **Create topic**
# MAGIC 3. **Overview** → **Connection information** → **Apache Kafka**, authentication method **SASL**. Keep this page
# MAGIC    open. (No SASL there? The setting from step 1 isn't saved yet, or the service is still restarting.)
# MAGIC 4. Next to **CA certificate**, click the download arrow. You get `ca.pem`
# MAGIC
# MAGIC ![Aiven Connection information, tab Apache Kafka, authentication method SASL selected, with Host, Port, User avnadmin, Password and the download arrow next to CA certificate](./assets/aiven_connection_info.png)
# MAGIC
# MAGIC Aiven signs its brokers with its own certificate authority, which Java doesn't know. `ca.pem` tells the Kafka
# MAGIC client to trust it. Run the next cell, it creates the volume `landing.aiven`. Then in **Catalog** open
# MAGIC `denghs26` → `landing` → `aiven` → **Upload to this volume** and upload `ca.pem`. Run the cell again.

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.landing.aiven")
print("ca.pem found" if os.path.exists(CA_PATH) else f"Upload ca.pem to {CA_PATH}, then run this cell again")

# COMMAND ----------

# MAGIC %md
# MAGIC ### The password goes into a secret scope
# MAGIC
# MAGIC Copy **Host**, **Port** and **Password** from Aiven (**Show** reveals the password) into the three widgets at
# MAGIC the top and run the next cell. It stores them in the secret scope `aiven`. Then **empty the password widget
# MAGIC again**. From now on every notebook reads the values with `dbutils.secrets.get`, and Databricks prints
# MAGIC `[REDACTED]` instead of any value that comes from a secret scope, the password and also host and port.

# COMMAND ----------

dbutils.widgets.text("kafka_host", "", "Aiven host")
dbutils.widgets.text("kafka_port", "", "Aiven port")
dbutils.widgets.text("kafka_password", "", "Aiven password (empty it afterwards)")

if dbutils.widgets.get("kafka_password"):
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    if SECRET_SCOPE not in [s.name for s in w.secrets.list_scopes()]:
        w.secrets.create_scope(scope=SECRET_SCOPE)
    w.secrets.put_secret(SECRET_SCOPE, "host", string_value=dbutils.widgets.get("kafka_host").strip())
    w.secrets.put_secret(SECRET_SCOPE, "port", string_value=dbutils.widgets.get("kafka_port").strip())
    w.secrets.put_secret(SECRET_SCOPE, "user", string_value="avnadmin")
    w.secrets.put_secret(SECRET_SCOPE, "password", string_value=dbutils.widgets.get("kafka_password").strip())
    print("Stored. Now empty the password widget.")

host = dbutils.secrets.get(SECRET_SCOPE, "host")
port = dbutils.secrets.get(SECRET_SCOPE, "port")
user = dbutils.secrets.get(SECRET_SCOPE, "user")
password = dbutils.secrets.get(SECRET_SCOPE, "password")
print(f"Broker {host}:{port}, user {user}, password {password}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Can Databricks reach it?
# MAGIC
# MAGIC Free Edition lets serverless compute reach only some hosts on the internet. Before we blame Spark for anything,
# MAGIC a plain network connection to the broker:

# COMMAND ----------

try:
    socket.create_connection((host, int(port)), timeout=10).close()
    print(f"OK, {host}:{port} is reachable")
except OSError as e:
    print(f"No connection to {host}:{port}: {e}")
    print("Is the service Running in Aiven? Host and port right? If both are fine, tell us.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### The connection options
# MAGIC
# MAGIC The same for reading and for writing, so they go into one dict:
# MAGIC
# MAGIC | Option | Why |
# MAGIC |---|---|
# MAGIC | `kafka.bootstrap.servers` | where the broker is |
# MAGIC | `kafka.security.protocol` = `SASL_SSL` | log in with user and password, over an encrypted connection |
# MAGIC | `kafka.sasl.mechanism` = `SCRAM-SHA-256` | how the password is checked. Aiven supports it by default |
# MAGIC | `kafka.sasl.jaas.config` | user and password. Databricks ships its own copy of the Kafka client, hence `kafkashaded.` |
# MAGIC | `kafka.ssl.truststore.*` | trust certificates signed by the Aiven CA, our `ca.pem` |
# MAGIC
# MAGIC Everything with the prefix `kafka.` goes straight to the Kafka client. Options without it, like `subscribe` or
# MAGIC `startingOffsets` below, are for Spark.

# COMMAND ----------

KAFKA_OPTIONS = {
    "kafka.bootstrap.servers": f"{host}:{port}",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "SCRAM-SHA-256",
    "kafka.sasl.jaas.config": (
        "kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required "
        f'username="{user}" password="{password}";'
    ),
    "kafka.ssl.truststore.type": "PEM",
    "kafka.ssl.truststore.location": CA_PATH,
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. The app sends orders
# MAGIC
# MAGIC In real life the delivery app is the producer. Here we borrow Spark for it: `send_batch(n)` takes the same events
# MAGIC as `write_batch(n)` and writes them into the topic. The key is the order id, so all events of one order go to the
# MAGIC same partition and stay in order (week 9).

# COMMAND ----------

def send_batch(batch):
    rows = _rows(batch)
    events = spark.createDataFrame([(r["order_id"], json.dumps(r)) for r in rows], "key STRING, value STRING")
    events.write.format("kafka").options(**KAFKA_OPTIONS).option("topic", TOPIC).save()
    print(f"Batch {batch}: {len(rows)} messages sent to {TOPIC}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Spark's Kafka source
# MAGIC
# MAGIC `format("kafka")` is a streaming source like Auto Loader. Each message arrives with the same columns: `key` and
# MAGIC `value` as bytes, plus `topic`, `partition`, `offset` and `timestamp`. Bronze keeps the message as it came, we only
# MAGIC turn the bytes into text.
# MAGIC
# MAGIC `startingOffsets` says where a **new** query starts: `earliest` (everything the topic still has) or `latest` (only
# MAGIC what comes from now on). After the first run the checkpoint decides. We take `latest` here, so it doesn't matter
# MAGIC what an earlier run of this notebook left in the topic.

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {KAFKA_BRONZE}")
dbutils.fs.rm(f"{STATE_VOLUME}/bronze_orders_kafka", recurse=True)


def run_kafka_bronze():
    query = (
        spark.readStream.format("kafka")
        .options(**KAFKA_OPTIONS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .load()
        .select(F.col("key").cast("string").alias("key"),
                F.col("value").cast("string").alias("value"),
                "topic", "partition", "offset",
                F.col("timestamp").alias("kafka_ts"))
        .withColumn("_ingested_at", F.current_timestamp())
        .writeStream
        .option("checkpointLocation", KAFKA_CHECKPOINT)
        .trigger(availableNow=True)
        .toTable(KAFKA_BRONZE)
    )
    query.awaitTermination()
    print(f"{input_rows(query)} new messages read")


run_kafka_bronze()

# COMMAND ----------

# MAGIC %md
# MAGIC 0 messages: the query started at the end of the topic and nothing new has arrived. But the checkpoint now knows
# MAGIC where «the end» was. Monday's orders arrive:

# COMMAND ----------

send_batch(1)
run_kafka_bronze()
display(spark.table(KAFKA_BRONZE).orderBy("offset").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC 200. In the Aiven Console you can see them too: **Topics** → `orders` → **Messages** → **Fetch messages**, then
# MAGIC switch on **Decode from Base64**.
# MAGIC
# MAGIC The same test as with the files. Again, nothing new:

# COMMAND ----------

run_kafka_bronze()

# COMMAND ----------

send_batch(2)
run_kafka_bronze()
print(spark.table(KAFKA_BRONZE).count(), "messages in bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC 0, then 350, 550 in total. Exactly once, like Auto Loader, without a single file.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Under the hood: where is the bookmark?
# MAGIC
# MAGIC In week 9 the consumer group remembered how far you had read, and Kafka kept that for you. Spark does it the other
# MAGIC way round: the offsets live in **the checkpoint**, next to the data's commit. That's what makes it exactly once:
# MAGIC data and bookmark move together, in one place. The newest offsets file:

# COMMAND ----------

offset_files = sorted(dbutils.fs.ls(f"{KAFKA_CHECKPOINT}/offsets"), key=lambda f: int(f.name))
print(dbutils.fs.head(offset_files[-1].path).splitlines()[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC `{"orders": {"0": …}}`: for topic `orders`, partition 0, the next offset to read. In Aiven, under
# MAGIC **Consumer groups**, you won't find our query: Spark never told Kafka anything. If you delete the checkpoint, the
# MAGIC bookmark is gone, and the next run starts again at `startingOffsets`.
# MAGIC
# MAGIC ![The app sends to the topic orders, readStream with availableNow reads into bronze.orders_kafka, the checkpoint holds the next offset per partition. Week 9: the consumer group stored the offset in Kafka](./assets/pipeline_kafka.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. From the message to the columns
# MAGIC
# MAGIC Auto Loader inferred the columns for us. With Kafka, `value` is just text, and we bring the schema ourselves, with
# MAGIC `from_json`:

# COMMAND ----------

ORDER_SCHEMA = ("order_id STRING, customer STRING, restaurant STRING, city STRING, items INT, "
                "amount_chf DOUBLE, status STRING, event_ts TIMESTAMP")

orders = (spark.table(KAFKA_BRONZE)
          .select(F.from_json("value", ORDER_SCHEMA).alias("o"), "partition", "offset")
          .select("o.*", "partition", "offset"))
display(orders.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC That's where silver would start (notebook 04, the same `MERGE`). But careful: `from_json` with a fixed schema is
# MAGIC the fixed schema of notebook 03 again. A new field is dropped, a broken value becomes `null`, without a word. The
# MAGIC extended exercise sends Wednesday's batch and shows it. That's one more reason why bronze keeps the raw `value`:
# MAGIC you can parse again with a better schema later, even after Kafka has deleted the message.

# COMMAND ----------

# MAGIC %md
# MAGIC ## What you saw
# MAGIC
# MAGIC | | files (01) | Kafka |
# MAGIC |---|---|---|
# MAGIC | source | `format("cloudFiles")` | `format("kafka")` + connection options |
# MAGIC | bookmark in the checkpoint | the list of files | the next offset per partition |
# MAGIC | schema | inferred, evolves (03) | you bring it, `from_json` |
# MAGIC | replay | the files stay in the volume | Kafka deletes messages after the retention time |
# MAGIC
# MAGIC Kafka gives us events with their time. The restaurant dashboard wants «orders per 15 minutes». When is a quarter
# MAGIC of an hour finished, if events can arrive late? That's notebook 07, the same question as in week 9 with Flink. First:
# MAGIC `2_exercise_short_kafka_source`.
