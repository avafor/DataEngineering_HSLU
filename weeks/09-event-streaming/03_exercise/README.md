# Streaming Exercises

[Week 9 overview](../README.md)

Two progressive tracks. Both run end-to-end inside the Codespace stack —
do [../01_setup/](../01_setup/) first.

## Track A — Kafka with Python

Use case: **a food-delivery app**, the story from the lecture. Anna orders
a pizza, a rider picks it up, and the live map shows where every rider is.
Two topics carry the events:

| Topic | Events | Key | Example value |
|---|---|---|---|
| `rider-locations` | GPS pings from the riders' phones | rider id (`rider_7`, `rider_3`, …) | `{"rider_id": "rider_7", "lat": 47.0502, "lon": 8.3093, "speed_kmh": 18.4, "timestamp": ...}` |
| `orders` | order events | order id (`order-4711`, …) | `{"order_id": "order-4711", "customer": "Anna", "restaurant": "Pizzeria Roma", "total_chf": 32.50, "status": "placed", "timestamp": ...}` |

The key keeps related events together: all pings of one rider land in the
same partition, in order, and so do all status changes of one order.

The six notebooks build on each other — work them top to bottom.

| Step | File | What you practice |
|---|---|---|
| 01 | [exercise_01_produce_single.ipynb](exercise_01_produce_single.ipynb) | first producer, keys, broken-broker behaviour |
| 02 | [exercise_02_consume_basic.ipynb](exercise_02_consume_basic.ipynb) | basic poll loop, live consumer, `group.id` |
| 03 | [exercise_03_produce_batch.ipynb](exercise_03_produce_batch.ipynb) | batch producer, partition assignment, keyless events |
| 04 | [exercise_04_consume_groups.ipynb](exercise_04_consume_groups.ipynb) | manual offset commits, two consumers in one group |
| 05 | [exercise_05_produce_stream.ipynb](exercise_05_produce_stream.ipynb) | continuous GPS stream + GPS-glitch simulation |
| 06 | [exercise_06_consume_aggregate.ipynb](exercise_06_consume_aggregate.ipynb) | running aggregation per rider + anomaly detection |

> Tip: open producer and consumer notebooks in **two tabs side by side**
> so you see events flow in real time. Drag a notebook tab to the right
> edge to split the editor.

Solutions live in [../04_solution/](../04_solution/). Try the exercise
first.

## Track B — Flink SQL on the Bluesky firehose

[exercise_07_flink.md](exercise_07_flink.md) — progressive Flink SQL
exercises (filter → tumble → hop → session → reply rate → keyword)
running on the live Bluesky public-post firehose. Here we leave the
delivery app: Bluesky is real, messy live data.

Before starting, walk through [../01_setup/dinky_guide.md](../01_setup/dinky_guide.md):
it covers the one-time Dinky/Flink registration. The `bluesky_posts`
source table you'll use throughout the track is created in step S4 of
the exercise.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `KafkaError: Broker not available` (Python) | wait ~10 s after Codespace start, or run `rpk cluster info -X brokers=redpanda:29092` |
| Notebook can't find `confluent_kafka` | from a terminal: `cd streaming && uv sync`, then re-pick the kernel |
| Every event lands on partition 0 | the topic was created automatically with 1 partition. Run the smoke test (or the *Check the topics* cell in exercise 01): it grows the topics to 3 partitions |
| `Object 'bluesky_posts' not found` (Dinky) | re-run the `CREATE TABLE`; ensure catalog is *DefaultCatalog* |
| `No more slots available` (Flink) | stop another running job (red ■) |
| Dinky UI is in Chinese | open it in an incognito tab or clear localStorage for the Dinky tab |
