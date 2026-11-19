# Databricks notebook source
# MAGIC %md
# MAGIC # Kafka helpers
# MAGIC
# MAGIC The connection to your Kafka service on Aiven, as set up in `06_kafka_source/1_demo_kafka_source`, parts 1 and 2.
# MAGIC The exercises of notebook 06 pull it in with `%run` (after `_config` and `_generator`).

# COMMAND ----------

import json
import os
import socket

from pyspark.sql import functions as F

TOPIC = "orders"
SECRET_SCOPE = "aiven"
CA_PATH = f"/Volumes/{CATALOG}/landing/aiven/ca.pem"

missing = []
if not os.path.exists(CA_PATH):
    missing.append(f"ca.pem in {CA_PATH}")
try:
    _keys = {s.key for s in dbutils.secrets.list(SECRET_SCOPE)}
except Exception:
    _keys = set()
missing += [f"secret {k} in scope {SECRET_SCOPE}" for k in ("host", "port", "user", "password") if k not in _keys]
if missing:
    raise RuntimeError("Kafka isn't set up yet, run parts 1 and 2 of the demo of notebook 06 first. Missing: "
                       + ", ".join(missing))

host = dbutils.secrets.get(SECRET_SCOPE, "host")
port = dbutils.secrets.get(SECRET_SCOPE, "port")
user = dbutils.secrets.get(SECRET_SCOPE, "user")
password = dbutils.secrets.get(SECRET_SCOPE, "password")

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


def send_batch(batch, topic=TOPIC):
    # The events of batch n as messages, key = order id, value = the event as JSON. Kafka only appends:
    # sending a batch twice puts it into the topic twice.
    rows = _rows(batch)
    events = spark.createDataFrame([(r["order_id"], json.dumps(r)) for r in rows], "key STRING, value STRING")
    events.write.format("kafka").options(**KAFKA_OPTIONS).option("topic", topic).save()
    print(f"Batch {batch}: {len(rows)} messages sent to {topic}")
    return len(rows)


def topic_now(topic=TOPIC):
    # Everything that is in the topic right now, read once as a batch (not a stream)
    return (spark.read.format("kafka").options(**KAFKA_OPTIONS).option("subscribe", topic)
            .option("startingOffsets", "earliest").load())


print(f"Kafka: {host}:{port}, topic {TOPIC}")
