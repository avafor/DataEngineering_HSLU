# Flink SQL on our own food-delivery data

## Background

In [exercise 07](exercise_07_flink.md) the windows ran on **processing
time** (`PROCTIME()`): the wall clock of the Flink machine at the moment
a post arrived. That is simple, but it answers the wrong question when
events arrive late. A GPS ping sent at 12:00:58 from a tunnel may reach
Kafka at 12:01:20 — in which minute did it *happen*?

Now we go back to the delivery app and use **event time**: the
`timestamp` field that the rider's phone or the order service wrote into
each event. Flink then needs a **watermark** to know when a window is
complete:

> **Watermark** = "I have now seen everything up to time *W*".
> Here: the highest event time seen so far **minus 5 seconds**.
> A window `[12:00, 12:01)` is closed and its result written as soon as
> the watermark passes 12:01 — so about 5 seconds after the first event
> of 12:01 arrives. Events that come in *after* that are **late**.

In this track you'll:

1. Run two producers that simulate riders and orders continuously.
2. Define Flink SQL source tables with event time and a watermark.
3. Count orders per minute (tumbling window).
4. Track the average speed of each rider (hopping window).
5. Find the shifts of each rider (session window).
6. Send a deliberately late event and watch Flink ignore it.

```
examples/producer.py --shifts ──▶ rider-locations ──┐
                                                    ├──▶ Flink SQL (event time + watermark)
examples/order_producer.py ─────▶ orders ───────────┤
                           └────▶ payments ─────────┘    (payments: exercise 09)
```

> **Slots are limited (4 total).** Each running query uses one slot.
> Stop jobs you no longer need with the red ■ button before starting new ones.

---

## Setup

### S1 — Start the producers

Open **two** terminals in VS Code (the split icon in the terminal panel)
and start one producer in each, from the repository root:

```bash
uv run --project streaming weeks/09-event-streaming/examples/order_producer.py
```

```bash
uv run --project streaming weeks/09-event-streaming/examples/producer.py --shifts --duration 1800
```

- `order_producer.py` places about one new order every 2 seconds. Every
  order then goes through `placed → accepted → picked_up → delivered`
  (all with the same key, the order id). It also writes a **payment** to
  the topic `payments` a few seconds after `placed` — for about one order
  in five, never. It runs for 30 minutes; stop it with `Ctrl+C`.
- `producer.py --shifts` sends GPS pings of 9 riders. With `--shifts`
  every rider works for 20–60 seconds, then takes a break of 40–90
  seconds without a single ping. `--duration 1800` keeps it running for
  30 minutes.

**Check:**

1. Each terminal prints a status line every 10 seconds, e.g.
   `t+  20s  orders=  11  status updates=  19  payments=   7  never paid=  2`
   and `t+ 20s  sent=  …  rate=…/s`. The numbers grow.
2. **Redpanda Console** (port `8080`) → *Topics*: `orders`, `payments` and
   `rider-locations` fill up. Open a message of each and look at the fields.

Leave both terminals running for the whole exercise.

### S2 — Open Dinky

As in exercise 07: Dinky (port `8888`) → Data Studio → new FlinkSQL task.

> **For every new task** set in the right-hand panel:
> **Cluster Configuration** → `deng` and **Catalog** → `DefaultCatalog`.
> The one-time cluster registration is in
> [../01_setup/dinky_guide.md](../01_setup/dinky_guide.md).

> **Prefer a terminal?** The Flink SQL client works as well:
> `docker exec -it flink-jobmanager ./bin/sql-client.sh`, then
> `SET 'sql-client.execution.result-mode' = 'tableau';` so results
> scroll by as a table. Tables you create there live only as long as the
> client session: paste the `CREATE TABLE` statements again after a restart.

### S3 — Create the source tables

Run this **once** in a task:

```sql
DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
    order_id     STRING,
    customer     STRING,
    restaurant   STRING,
    total_chf    DOUBLE,
    status       STRING,
    `timestamp`  DOUBLE,   -- UNIX seconds, written by the producer
    event_time AS TO_TIMESTAMP_LTZ(CAST(`timestamp` * 1000 AS BIGINT), 3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'orders',
    'properties.bootstrap.servers' = 'redpanda:29092',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

DROP TABLE IF EXISTS rider_locations;
CREATE TABLE rider_locations (
    rider_id     STRING,
    lat          DOUBLE,
    lon          DOUBLE,
    speed_kmh    DOUBLE,
    `timestamp`  DOUBLE,
    event_time AS TO_TIMESTAMP_LTZ(CAST(`timestamp` * 1000 AS BIGINT), 3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'rider-locations',
    'properties.bootstrap.servers' = 'redpanda:29092',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);
```

> **What's new compared to exercise 07?**
>
> - `event_time AS TO_TIMESTAMP_LTZ(...)` is a **computed column**: it turns
>   the UNIX seconds of the event into a real timestamp.
> - `WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND` makes
>   `event_time` the **event-time attribute** of the table and tells Flink
>   to wait 5 seconds for stragglers.
> - `'scan.startup.mode' = 'latest-offset'`: each query only sees events
>   from the moment it starts. The old events of exercises 01–06 stay out.
> - Window times are shown in **UTC** (the time zone of the Flink container).

**Check:** `SHOW TABLES;` lists `orders` and `rider_locations`. Then, in a
new task, look at the data — rows should appear within seconds:

```sql
SELECT order_id, restaurant, status, event_time FROM orders;
```

Stop it (red ■) before you continue.

---

## Exercise A — tumbling window: orders per minute

**Goal:** for every minute, the number of new orders and their revenue.

Hints:

- Window table function (the newer syntax, also used in
  [examples/flink_rider_locations.sql](../examples/flink_rider_locations.sql)):
  `FROM TABLE(TUMBLE(TABLE orders, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))`
- It adds the columns `window_start` and `window_end`; group by both.
- Every order produces four events (one per status). Which ones do you count?

```sql
-- TODO: SELECT window_start, window_end, COUNT(*) AS orders, SUM(total_chf) AS revenue_chf
--       FROM TABLE(TUMBLE(...)) WHERE ... GROUP BY ...
```

**Check:** the **first row appears about one minute after you start** —
only when the minute is over (plus the 5 s watermark). Until then the
result stays empty; that is correct, not an error.

**Questions**

- How many orders per minute do you get? Does it match the producer (about one every 2 s)?
- The row for `12:03–12:04` appears at about `12:04:05`. Why not at `12:04:00`, and why not earlier?
- What does the count look like without the `WHERE` filter on `status`?
- **Keep this query running** — you need it in Exercise D.

---

## Exercise B — hopping window: average speed per rider

**Goal:** every minute, the average speed of each rider over the **last
5 minutes**.

Hints:

- `HOP(TABLE rider_locations, DESCRIPTOR(event_time), <slide>, <size>)` —
  **slide first, then size**.
- Group by `window_start, window_end, rider_id`.
- `ROUND(AVG(speed_kmh), 1)` keeps the numbers readable.

```sql
-- TODO: SELECT window_start, window_end, rider_id, COUNT(*) AS pings, ... AS avg_speed_kmh
--       FROM TABLE(HOP(...)) GROUP BY ...
```

**Check:** after the first full minute a block of rows appears — one per
rider who sent pings in the last 5 minutes — and then a new block every minute.

**Questions**

- How many rows per rider arrive every minute? How many windows does one ping belong to?
- Why do the first windows contain fewer pings than the later ones?
- A rider on a break sends no pings. What happens to the rows of that rider?

---

## Exercise C — session window: rider shifts

**Goal:** one row per **shift** of a rider. A shift ends when a rider has
sent no ping for **30 seconds**.

Hints:

- `SESSION(TABLE rider_locations PARTITION BY rider_id, DESCRIPTOR(event_time), INTERVAL '30' SECOND)`
- `PARTITION BY rider_id` gives every rider their own sessions.
- Rename the window columns: `window_start AS shift_start, window_end AS shift_end`.

```sql
-- TODO: SELECT rider_id, shift_start, shift_end, COUNT(*) AS pings
--       FROM TABLE(SESSION(...)) GROUP BY ...
```

> **Session window**
> No fixed size: as long as pings keep coming within the gap, the session
> grows. After 30 s of silence (plus the 5 s watermark delay) it closes.
> ```
> rider_7: |-p-p--p-p-p--p-|    (break > 30 s)    |-p--p-p-p-|
>               shift 1                             shift 2
> ```

**Check:** the first rows appear after one to two minutes — a shift row
only comes when that rider's break has lasted 30 seconds. Rows of different
riders arrive at different times.

**Questions**

- `shift_end` is about 30 seconds later than the rider's last ping. Why?
- How long are the shifts? Compare with the producer (20–60 s of pings).
- What happens to the number of shifts if you set the gap to `90` seconds? Why?

---

## Exercise D — late data

**Goal:** see what Flink does with an event that arrives after its window
has been closed.

### Step 1 — note a closed window

Look at the output of Exercise A. Pick a window from about 3 minutes ago
and note its `orders` value.

### Step 2 — send one late event

In a third terminal, from the repository root:

```bash
uv run --project streaming weeks/09-event-streaming/examples/order_producer.py --late 3
```

This sends **one** order with status `placed` whose event time lies
3 minutes in the past, and exits.

**Check:** the terminal prints a line like
`sent LATE order order-late-417: event time 08:12:05 UTC, now 08:15:05 UTC (3 min late)`.
Note the order id and the event time.

### Step 3 — observe

1. Wait for the next row of Exercise A. Does a new or corrected row appear for the window of the late event?
2. Check that the event really arrived: Redpanda Console → topic `orders` → newest message.
3. Open the Flink Web UI (port `8081`) → the running job of Exercise A →
   the box `GlobalWindowAggregate …` → tab **Metrics** → add
   `numLateRecordsDropped`.
4. Make the late events visible with a query of their own. `CURRENT_WATERMARK(event_time)`
   returns the current watermark:

```sql
-- TODO: SELECT order_id, event_time, CURRENT_WATERMARK(event_time) AS watermark
--       FROM orders WHERE ...   (only events that are older than the watermark)
```

Start this query, then send another late event (`--late 3`).

5. Now send an event that is only 3 seconds "late": `--late 0.05`. Is it counted?

**Questions**

- Why does the closed window not change?
- Why is an event that is 3 seconds old still counted, but one that is 3 minutes old is not?
- What would you have to change so that the late order is still counted? What does that cost?
- In a real delivery app: name a reason why an order event could arrive minutes late.

---

## Cleanup

Stop all running Flink jobs (red ■ in Dinky) and the producers
(`Ctrl+C` in their terminals). Exercise 09 needs `order_producer.py` again.
