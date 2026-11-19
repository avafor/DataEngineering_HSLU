# Week 10 — Structured Streaming and incremental ingestion

[Module homepage](../../README.md) · [Preparation](../../preparation/week-10.md)

## Goal

Remember Anna's pizza from week 9? Her order was an event in a Kafka topic. This week we get those events into the
lakehouse and keep bronze, silver and gold up to date, without reloading everything each time. Every run only
processes what is new.

```
landing folder (JSON files)          Kafka topic (notebook 06)
   |  Auto Loader                       |  Kafka source
   v                                    v
bronze.orders                        bronze.orders_kafka        every event as it arrived
   |  Delta stream + MERGE
   v
silver.orders                        one row per order, latest status
   |  Change Data Feed
   v
gold.restaurant_revenue              active orders and revenue per restaurant
```

By the end of the week, you should be able to:

- load new files exactly once with Auto Loader, and explain what the checkpoint keeps;
- describe how Structured Streaming works: an unbounded table, micro-batches, state, triggers, output modes;
- explain what happens when a new column or a broken value arrives, and handle it;
- stream from one Delta table into the next and upsert with `foreachBatch` and `MERGE`;
- read the Change Data Feed and keep an aggregate correct when rows are updated or deleted;
- read a Kafka topic with Structured Streaming and explain where Spark keeps the offsets;
- count events in time windows with a watermark; and
- run the pipeline as a job with retries and a check.

## How the notebooks work

Every topic is a folder with three notebooks:

| Notebook | What for | When |
|---|---|---|
| `1_demo_…` | Step by step, with pictures. First the problem without the technology, then the technology, then a look under the hood. Run it cell by cell and read the output | in class, and to look things up |
| `2_exercise_short_…` | 3 to 5 small tasks, about 10 minutes | in class |
| `3_exercise_extended_…` | What goes wrong, why, and how to do it properly, with harder tasks (★ to ★★★) | at home |

A task cell stops with `NotImplementedError: Task x.y` until you replace that line with your code. Below every task
you find two hints and the solution, folded. Try first, then open them. After the task, a check cell prints
`Correct!` or what is still missing.

| Folder | Question | Time |
|---|---|---|
| `01_auto_loader` | New files every day: how do I load each one exactly once? | demo 15 min, short 10 min |
| `02_streaming_engine` | What is behind Auto Loader? | demo 15 min, short 10 min |
| `03_schema_changes` | What if a new column or a broken value arrives? | demo 10 min, short 10 min |
| `04_delta_stream_merge` | How do many events become one row per order? | demo 15 min, short 10 min |
| `05_change_data_feed` | Anna cancels: how does gold notice? | demo 15 min, short 10 min |
| `06_kafka_source` | Can we read the topic directly? (needs your Aiven service) | demo 20 min, short 10 min |
| `07_windows_watermarks` | Orders per 15 minutes: when is a window finished? | demo 15 min, short 10 min |
| `08_production_job` | Who runs it every 10 minutes? | demo 15 min, short 10 min |

In class we do 01 to 04. 05 to 08 and all extended exercises are for at home.

## Before you start

You need the Databricks Free Edition account from [week 8](../../preparation/week-08.md) and, for notebook 06, the
free Kafka service on Aiven from the [preparation](../../preparation/week-10.md#4-create-a-free-kafka-service-on-aiven).
Nothing to install or download: the notebooks write their own order events.

## Get the notebooks into your workspace

If you added the Git folder in week 8, open it, click the branch name → **Pull**, and open
`weeks/10-structured-streaming-incremental/notebooks`. Otherwise:

1. In Databricks, open **Workspace** → **Users** → your email.
2. Click **Create** → **Git folder**.
3. Paste `https://github.com/avafor/DataEngineering_HSLU`, choose **GitHub** as provider and click **Create Git folder**.
4. Open `weeks/10-structured-streaming-incremental/notebooks`.

Attach the notebooks to **Serverless**. Run `00_setup` once, then start with `01_auto_loader/1_demo_auto_loader`.

## Good to know

- **The catalog** is `denghs26`, the same as in week 8. `00_setup` creates it if you skipped week 8. Everything you
  try out goes into the schema `playground`, so the pipeline tables stay clean.
- **Skipped a notebook?** No problem. From notebook 03 on, every demo starts with `catch_up(n)`, which rebuilds the
  pipeline to where notebook `n` left it. That takes a minute or two.
- **`_config`, `_generator`, `_pipeline`, `_kafka`** are helpers the other notebooks load with `%run`. You don't run
  them yourself, but you're welcome to read them.
- **`_stream_once`** runs a single streaming query in a notebook run of its own. When a stream fails, Databricks
  marks the whole cell as failed, even inside `try`/`except`. For the streams that stop on purpose (notebook 03 and
  others), the demos use this helper, the way a job would, and get a normal exception back.
- **Free Edition** only allows `trigger(availableNow=True)`, and it has a daily compute limit. Notebook 08 creates
  jobs: leave their schedules paused.
- **At the end of the week**, `99_cleanup` deletes the tables, files and jobs of this week. Your Aiven service powers
  itself off after 24 hours without traffic; delete it in the Aiven Console when you don't need it anymore.

## If something goes wrong

| Error | What it means |
|---|---|
| `INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED` | Free Edition only supports `.trigger(availableNow=True)` |
| `STREAMING_CHECKPOINT_LOCATION_NOT_SPECIFIED` | you forgot `checkpointLocation`. It has to point to a volume |
| `UNKNOWN_FIELD_EXCEPTION` | a new column arrived and Auto Loader stopped once, on purpose. Run it again (notebook 03) |
| `STREAMING_OUTPUT_MODE.UNSUPPORTED_OPERATION` | `append` for an aggregation without a watermark. Use `complete`, or add a watermark (notebooks 02, 07) |
| `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE` | several rows per order in one batch. Keep the newest one before the `MERGE` (notebook 04) |
| `DELTA_SOURCE_TABLE_IGNORE_CHANGES` | a plain stream ran into an update. Use the Change Data Feed (notebook 05) |
| Aiven shows no **SASL** under Connection information | SASL isn't turned on yet, see [preparation, part 5](../../preparation/week-10.md#5-prepare-the-connection) |
| `SaslAuthenticationException` | wrong Aiven password or host. Run part 2 of the Kafka demo again |
| SSL handshake failed, `PKIX path building failed` | `ca.pem` is missing in `landing.aiven`, or it's from another Aiven project |
| No connection to Aiven, `Timeout` | the service is powered off or still rebuilding. Power it on and wait for **Running** |
| `UnknownTopicOrPartitionException` | the topic `orders` doesn't exist yet. Create it in Aiven under **Topics** |
| Numbers don't match a check | run the demo of that folder again: it rebuilds the pipeline with `catch_up` |

## Docs

- [Auto Loader](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/)
- [Structured Streaming concepts](https://docs.databricks.com/aws/en/structured-streaming/concepts)
- [Delta table streaming reads and writes](https://docs.databricks.com/aws/en/structured-streaming/delta-lake)
- [Change Data Feed](https://docs.databricks.com/aws/en/delta/delta-change-data-feed)
- [Stream processing with Apache Kafka and Databricks](https://docs.databricks.com/aws/en/connect/streaming/kafka)
- [Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/streaming/index.html)
- [Lakeflow Jobs](https://docs.databricks.com/aws/en/jobs/)
- [Aiven for Apache Kafka free tier](https://aiven.io/docs/products/kafka/free-tier/kafka-free-tier)
