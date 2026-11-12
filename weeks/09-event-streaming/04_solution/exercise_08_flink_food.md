# Flink SQL on our food-delivery data — Solutions

Reference answers for [03_exercise/exercise_08_flink_food.md](../03_exercise/exercise_08_flink_food.md).

---

## Exercise A — tumbling window: orders per minute

```sql
SELECT
    window_start,
    window_end,
    COUNT(*)                 AS orders,
    ROUND(SUM(total_chf), 2) AS revenue_chf
FROM TABLE(
    TUMBLE(TABLE orders, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
)
WHERE status = 'placed'
GROUP BY window_start, window_end;
```

Example output (one row per minute):

```
window_start      window_end        orders  revenue_chf
2026-11-12 06:24  2026-11-12 06:25      27       862.94
2026-11-12 06:25  2026-11-12 06:26      34      1075.54
2026-11-12 06:26  2026-11-12 06:27      32      1130.77
```

> **Why `WHERE status = 'placed'`?** Each order produces four events with
> the same `order_id`. Without the filter you count status events, not
> orders — roughly four times as many (a bit less, because an order placed
> at the end of a minute has its later statuses in the next minute).

> **Why the row appears about 5 seconds after the minute.** The window
> `[06:24, 06:25)` is complete once the watermark passes 06:25. The
> watermark is the highest event time seen minus 5 seconds, so it reaches
> 06:25 when the first event of about 06:25:05 arrives. Before that, Flink
> cannot know whether another 06:24 event is still on its way. The first
> row usually covers only part of a minute: the query started in the middle of it.

---

## Exercise B — hopping window: average speed per rider

```sql
SELECT
    window_start,
    window_end,
    rider_id,
    COUNT(*)                 AS pings,
    ROUND(AVG(speed_kmh), 1) AS avg_speed_kmh
FROM TABLE(
    HOP(TABLE rider_locations, DESCRIPTOR(event_time),
        INTERVAL '1' MINUTE,      -- slide: a new result every minute
        INTERVAL '5' MINUTES)     -- size: over the last 5 minutes
)
GROUP BY window_start, window_end, rider_id;
```

Example output (every minute, one row per rider):

```
window_start      window_end        rider_id  pings  avg_speed_kmh
2026-11-12 06:20  2026-11-12 06:25  rider_9      55           20.3
2026-11-12 06:20  2026-11-12 06:25  rider_5      41           15.7
2026-11-12 06:20  2026-11-12 06:25  rider_1      32           17.5
```

- Every minute you get **one row per rider** who sent at least one ping
  in the last 5 minutes. One ping belongs to size / slide = 5 / 1 = **5 windows**.
- The first windows start before the query did (`latest-offset`), so they
  only contain the last minute or two of data. After 5 minutes the windows are full.
- A rider on a break simply has fewer pings in the window; a rider who has
  not sent anything for 5 minutes has no row at all. Windows only exist
  where there is data.

---

## Exercise C — session window: rider shifts

```sql
SELECT
    rider_id,
    window_start AS shift_start,
    window_end   AS shift_end,
    COUNT(*)     AS pings
FROM TABLE(
    SESSION(TABLE rider_locations PARTITION BY rider_id,
            DESCRIPTOR(event_time), INTERVAL '30' SECOND)
)
GROUP BY window_start, window_end, rider_id;
```

Example output:

```
rider_id  shift_start               shift_end                 pings
rider_1   2026-11-12 06:23:48.003   2026-11-12 06:24:33.695      25
rider_5   2026-11-12 06:23:47.799   2026-11-12 06:24:49.378      41
rider_9   2026-11-12 06:23:48.613   2026-11-12 06:25:04.846      54
```

- `shift_end` = last ping **+ 30 s gap**: Flink extends the session by the
  gap after every event, and the session only ends when that gap has
  passed without a new ping. So a shift of 20–60 s of pings shows up as
  50–90 s from `shift_start` to `shift_end`.
- The row appears about 35 s after the last ping (gap + 5 s watermark delay).
- With a gap of 90 s most breaks (40–90 s) are shorter than the gap: two
  shifts merge into one long session, so you get fewer and longer
  sessions. The gap is a business decision: how long a break may be
  before we call it a new shift.

---

## Exercise D — late data

Query to make late events visible:

```sql
SELECT
    order_id,
    event_time,
    CURRENT_WATERMARK(event_time) AS watermark
FROM orders
WHERE event_time < CURRENT_WATERMARK(event_time);
```

What you observe after `order_producer.py --late 3`:

```
sent LATE order order-late-417: event time 06:23:37 UTC, now 06:26:37 UTC (3 min late)
```

- The query above prints exactly this order, with an event time about
  3 minutes behind the watermark.
- Exercise A writes **no new and no corrected row** for the window
  `[06:23, 06:24)`. Its result stays what it was.
- In the Flink Web UI the metric `numLateRecordsDropped` of
  `GlobalWindowAggregate` goes from `0` to `1`.
- The event *is* in the topic `orders` (Redpanda Console). Kafka stores
  everything; it's the window operator that ignores it.
- `--late 0.05` (3 seconds) is **counted** normally: 3 s is less than the
  5 s watermark delay, so its window is still open.

> **Why the window doesn't change.** When the watermark passed 06:24, Flink
> wrote the result of `[06:23, 06:24)` and **deleted the window's state**.
> The result went downstream as a final, insert-only row — maybe already
> to a dashboard or another topic. A late event finds no window to add
> itself to and is dropped. That's the trade-off a watermark expresses:
> results are final and memory stays small, at the price of ignoring
> stragglers.

**What one could do instead** (concepts only, not in this exercise):

- **A bigger watermark delay**, e.g. `event_time - INTERVAL '5' MINUTE`.
  The late order would be counted, but *every* result arrives 5 minutes
  later and Flink keeps 5 minutes more state.
- **Allowed lateness.** The window keeps its state a while longer after it
  fired; a late event triggers an *updated* result for the same window.
  Downstream must then handle corrections (an upsert sink instead of an
  append-only topic). Available in Flink's DataStream API
  (`allowedLateness`), not in the standard SQL windows.
- **Side output.** Route late events to a separate stream instead of
  dropping them (`sideOutputLateData` in the DataStream API), e.g. into a
  `late-orders` topic for a nightly correction job. In SQL the query above
  does the same job: write it with `INSERT INTO` into its own topic.

Typical reasons for late events in a delivery app: a rider's phone in a
cellar or tunnel buffers events and sends them when it is back online; a
restaurant tablet loses Wi-Fi; a service is restarted and re-sends a backlog.
