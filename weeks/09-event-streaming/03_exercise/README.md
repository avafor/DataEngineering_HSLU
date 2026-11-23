# Streaming Exercises

[Week 9 overview](../README.md)

Three progressive tracks. All run end-to-end inside the Codespace stack —
do [../01_setup/](../01_setup/) first.

## Track A — Kafka with Python

Use case: **a food-delivery app**, the story from the lecture. Anna orders
a pizza, a rider picks it up, and the live map shows where every rider is.
Two topics carry the events (a third one, `payments`, joins in for track C):

| Topic | Events | Key | Example value |
|---|---|---|---|
| `rider-locations` | GPS pings from the riders' phones | rider id (`rider_7`, `rider_3`, …) | `{"rider_id": "rider_7", "lat": 47.0502, "lon": 8.3093, "speed_kmh": 18.4, "timestamp": ...}` |
| `orders` | order events | order id (`order-4711`, …) | `{"order_id": "order-4711", "customer": "Anna", "restaurant": "Pizzeria Roma", "total_chf": 32.50, "status": "placed", "timestamp": ...}` |
| `payments` | payments (exercises 08–09) | order id | `{"order_id": "order-4711", "amount_chf": 32.50, "payment_method": "twint", "timestamp": ...}` |

The key keeps related events together: all pings of one rider land in the
same partition, in order, and so do all status changes of one order.

The six notebooks build on each other — work them top to bottom.

| Step | File | What you practice |
|---|---|---|
| 01 | [exercise_01_produce_single.ipynb](exercise_01_produce_single.ipynb) | first producer, keys, broken-broker behaviour |
| 02 | [exercise_02_consume_basic.ipynb](exercise_02_consume_basic.ipynb) | basic poll loop, live consumer, `group.id` |
| 03 | [exercise_03_produce_batch.ipynb](exercise_03_produce_batch.ipynb) | batch producer, partition assignment, keyless events |
| 04 | [exercise_04_consume_groups.ipynb](exercise_04_consume_groups.ipynb) | manual offset commits, two consumers in one group, `rpk topic create`, replay with `rpk group seek` |
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

## Track C — Flink SQL on the delivery data

Back to the food-delivery app, now with **event time** and **watermarks**.
Two producer scripts in [../examples/](../examples/) keep data flowing while
you query it — start them in a terminal from the repository root:

```bash
uv run --project streaming weeks/09-event-streaming/examples/order_producer.py
uv run --project streaming weeks/09-event-streaming/examples/producer.py --shifts --duration 1800
```

| Step | File | What you practice |
|---|---|---|
| 08 | [exercise_08_flink_food.md](exercise_08_flink_food.md) | event time + watermark; orders per minute (tumble), average speed per rider (hop), rider shifts (session); a late event with `order_producer.py --late 3` |
| 09 | [exercise_09_flink_joins.md](exercise_09_flink_joins.md) | enrich orders with a restaurant table; interval join orders ⨝ payments within 15 minutes; unpaid orders in a LEFT JOIN |

Do exercise 07 (or at least its setup) first: it introduces Dinky and windows.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `KafkaError: Broker not available` (Python) | wait ~10 s after Codespace start, or run `rpk cluster info -X brokers=redpanda:29092` |
| Notebook can't find `confluent_kafka` | from a terminal: `cd streaming && uv sync`, then re-pick the kernel |
| Every event lands on partition 0 | the topic was created automatically with 1 partition. Run the smoke test (or the *Check the topics* cell in exercise 01): it grows the topics to 3 partitions |
| `Object 'bluesky_posts' not found` (Dinky) | re-run the `CREATE TABLE`; ensure catalog is *DefaultCatalog* |
| Event-time windows (08/09) never produce a row | the producers must be running: without new events the watermark does not move, and no window closes |
| `No more slots available` (Flink) | stop another running job (red ■) |
| Dinky UI is in Chinese | open it in an incognito tab or clear localStorage for the Dinky tab |
