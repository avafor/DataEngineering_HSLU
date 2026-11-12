# Flink SQL joins on the food-delivery stream — Solutions

The complete exercise with the reference answers filled in: wherever the exercise has a `TODO`, you find **Solution:** here. Exercise without answers: [03_exercise/exercise_09_flink_joins.md](../03_exercise/exercise_09_flink_joins.md).

## Background

In a database, a join is easy: both tables are complete, so for every
order the database can look up the matching payment — or see that there
is none. In a stream neither side is ever complete:

- The **payment** for an order may come seconds later, minutes later — or
  **never** (the customer gives up, the card is declined).
- To match them, Flink has to **keep orders in memory** until the payment
  arrives. Forever? Then memory grows without limit.
- When can Flink say "this order has no payment"? Only when it decides to
  stop waiting.

This exercise shows the two joins you need most often:

| Join | Example | What Flink keeps in memory |
|---|---|---|
| **Stream–table join** (enrichment) | each order + city and cuisine of its restaurant | the small table, completely |
| **Stream–stream interval join** | each order + its payment *within 15 minutes* | orders and payments of the last 15 minutes |

```
orders ────────┐
               ├──▶ JOIN restaurants (small table)  ──▶ enriched orders
               │
               └──▶ JOIN payments within 15 min     ──▶ paid orders
payments ──────┘         LEFT JOIN                  ──▶ + unpaid orders (NULL)
```

> **Slots are limited (4 total).** Each running query uses one slot.
> Stop jobs you no longer need with the red ■ button.

---

## Setup

### S1 — Start the order producer

From the repository root (skip if it is still running from exercise 08):

```bash
uv run --project streaming weeks/09-event-streaming/examples/order_producer.py
```

Besides `orders` it writes the topic `payments`
(key = order id, value `{order_id, amount_chf, payment_method, timestamp}`).
A payment follows its order after 2–30 seconds — for about **one order in
five it never comes**. The restaurant *Thai Garden* is new on the platform.

**Check:** the terminal prints a status line every 10 seconds with
`orders=…  payments=…  never paid=…`, and in **Redpanda Console** the
topics `orders` and `payments` fill up.

### S2 — Source tables

Use Dinky as in exercise 08 (Cluster `deng`, Catalog `DefaultCatalog`).
You need the table `orders` from
[exercise 08, step S3](exercise_08_flink_food.md#s3--create-the-source-tables)
— create it again if it's missing. Then create `payments`:

```sql
DROP TABLE IF EXISTS payments;
CREATE TABLE payments (
    order_id        STRING,
    amount_chf      DOUBLE,
    payment_method  STRING,
    `timestamp`     DOUBLE,
    event_time AS TO_TIMESTAMP_LTZ(CAST(`timestamp` * 1000 AS BIGINT), 3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'payments',
    'properties.bootstrap.servers' = 'redpanda:29092',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);
```

**Check:** `SHOW TABLES;` lists `orders` and `payments`. A quick
`SELECT * FROM payments;` shows payments arriving within seconds — stop
it (red ■) afterwards.

### S3 — The restaurant table

The restaurant list is small and changes rarely. We define it directly
in Flink as a **view over fixed values** — no topic, no extra script:

```sql
DROP VIEW IF EXISTS restaurants;
CREATE VIEW restaurants AS
SELECT * FROM (VALUES
    ('Pizzeria Roma',   'Lucerne', 'Italian'),
    ('Sushi Bar Kyoto', 'Zug',     'Japanese'),
    ('Burger House',    'Lucerne', 'American'),
    ('Curry Palace',    'Kriens',  'Indian'),
    ('Taqueria Luz',    'Emmen',   'Mexican')
) AS t (restaurant, city, cuisine);
```

**Check:** `SELECT * FROM restaurants;` shows five rows, and the job finishes
by itself (the table is bounded, unlike a Kafka topic).

---

> **Why a view over `VALUES` for the restaurants?** It is the simplest
> table that really works in Flink 1.20 with this stack: no extra topic,
> no script to fill it, the same statement works in Dinky and in the SQL
> client, and the join is an ordinary SQL join. The price: the list is
> fixed in the query. In production the restaurant list lives elsewhere —
> in a database, read with a **lookup join** (JDBC connector,
> `FOR SYSTEM_TIME AS OF o.proc_time`), or in a compacted Kafka topic
> read with the `upsert-kafka` connector, so that a change of address
> reaches the running job.

---

## Exercise A — stream–table join: enrich orders

### A1 — inner join

**Goal:** every new order (status `placed`) with `city` and `cuisine` of its restaurant.

Hints:

- `FROM orders AS o JOIN restaurants AS r ON o.restaurant = r.restaurant`
- Only events with `o.status = 'placed'`.

**Solution:**

```sql
SELECT o.order_id, o.restaurant, r.city, r.cuisine, o.total_chf
FROM orders AS o
JOIN restaurants AS r ON o.restaurant = r.restaurant
WHERE o.status = 'placed';
```

Orders of *Thai Garden* are missing: the restaurant is not in the
table, so the inner join finds no partner and drops the order. Silent
data loss — a classic trap with enrichment joins.

**Check:** a new row for every new order, each with `city` and `cuisine`.
Orders of *Thai Garden* never appear here.

### A2 — left join

Run the same query with `LEFT JOIN` instead of `JOIN`.

**Check:** now *Thai Garden* orders appear too — with `NULL` in `city`
and `cuisine`.

**Solution:**

```sql
SELECT o.order_id, o.restaurant, r.city, r.cuisine, o.total_chf
FROM orders AS o
LEFT JOIN restaurants AS r ON o.restaurant = r.restaurant
WHERE o.status = 'placed';
```

Example output:

```
order_id      restaurant     city     cuisine  total_chf
order-646396  Taqueria Luz   Emmen    Mexican      25.84
order-646397  Thai Garden    NULL     NULL         37.51
order-646398  Pizzeria Roma  Lucerne  Italian      36.73
```

Every order appears; *Thai Garden* orders have `NULL` for `city` and
`cuisine`. That is the usual choice for enrichment: keep the event, and
make missing master data visible (e.g. count the `NULL`s).

### A3 — bonus: revenue per cuisine


**Questions**

- Orders of *Thai Garden* never appear in A1. Why? What do they look like in A2?
- In A3 there is no window. What happens to the rows over time? (In the
  SQL client you see `-U` and `+U`: what do they mean?)
- If a restaurant moved to another city, how would the view have to change?
  Why would a real system rather keep the restaurant list in a database or
  a Kafka topic?

**Solution:**

```sql
SELECT r.cuisine, COUNT(*) AS orders, ROUND(SUM(o.total_chf), 2) AS revenue_chf
FROM orders AS o
JOIN restaurants AS r ON o.restaurant = r.restaurant
WHERE o.status = 'placed'
GROUP BY r.cuisine;
```

Without a window the result is never final: every new order **updates**
its cuisine's row. In the SQL client:

```
op  cuisine   orders  revenue_chf
+I  Indian         1        29.4
-U  Indian         1        29.4
+U  Indian         2        57.8
```

`-U` retracts the old value, `+U` writes the new one. A sink for such a
result must be able to update rows (`upsert-kafka`, a database table),
an append-only topic is not enough.

**Restaurant moves to another city:** with the view you would have to
drop and re-create it and restart the query. With a database (lookup
join) or an upsert topic the change reaches the running job — orders
after the change get the new city, without a restart.

---

## Exercise B — stream–stream interval join: orders and payments

### B1 — paid orders

**Goal:** every order together with its payment, if the payment arrives
**within 15 minutes** after the order.

Hints:

- Join condition on the key **and** on time:
  ```sql
  ON  o.order_id = p.order_id
  AND p.event_time BETWEEN o.event_time AND o.event_time + INTERVAL '15' MINUTE
  ```
- Only orders with `o.status = 'placed'`.
- Select `o.event_time AS ordered_at` and, instead of `p.event_time`, the
  waiting time: `TIMESTAMPDIFF(SECOND, o.event_time, p.event_time) AS seconds_to_pay`.
  (Easier to read — and if you later write the result into a topic with
  `INSERT INTO`, Flink accepts only **one** event-time column in it.)


**Check:** rows appear a few seconds after each order — as soon as its
payment has arrived — with `seconds_to_pay` between 2 and 30.

Let it run for a few minutes, and keep the producer's terminal in view:
it reports how many orders will *never* be paid.

**Solution:**

```sql
SELECT
    o.order_id,
    o.customer,
    o.total_chf,
    o.event_time                                      AS ordered_at,
    TIMESTAMPDIFF(SECOND, o.event_time, p.event_time) AS seconds_to_pay,
    p.payment_method
FROM orders AS o
JOIN payments AS p
  ON  o.order_id = p.order_id
  AND p.event_time BETWEEN o.event_time AND o.event_time + INTERVAL '15' MINUTE
WHERE o.status = 'placed';
```

Example output:

```
order_id      customer  total_chf  ordered_at               seconds_to_pay  payment_method
order-646402  Clara         23.52  2026-11-12 06:29:02.172              28  twint
order-646415  Elif          22.57  2026-11-12 06:29:23.583              12  twint
order-646418  Fabio         45.71  2026-11-12 06:29:28.616               9  card
```

- A row appears **as soon as the payment arrives** — no need to wait for
  the 15 minutes.
- Orders that are never paid (about one in five) **never appear**. The
  inner join has no partner for them, and after 15 minutes Flink deletes
  them from its state without a trace.

### B2 — unpaid orders

Run the same query with `LEFT JOIN`. Look for rows where `seconds_to_pay`
and `payment_method` are `NULL`.

**Check:** at first the result looks like B1. The `NULL` rows only start
to appear **15 minutes** after you started the query (with the 1-minute
variant: after about one minute).

> **Don't want to wait 15 minutes?** Run the LEFT JOIN a second time with
> `INTERVAL '1' MINUTE` instead of `'15' MINUTE`.

**Questions**

- Some orders never show up in B1. Which ones, and why?
- In B2 the paid orders show up as soon as the payment arrives. When does
  an *unpaid* order show up? (Compare its `ordered_at` with the time it appears.)
- Why does Flink need the time condition at all? What would happen to its
  memory with only `ON o.order_id = p.order_id`?
- A payment arrives after 20 minutes. What do B1 and B2 show for this order?
- Which of the two joins would you use for a "payment missing" alert to customer service?

**Solution:**

```sql
SELECT
    o.order_id,
    o.customer,
    o.total_chf,
    o.event_time                                      AS ordered_at,
    TIMESTAMPDIFF(SECOND, o.event_time, p.event_time) AS seconds_to_pay,
    p.payment_method
FROM orders AS o
LEFT JOIN payments AS p
  ON  o.order_id = p.order_id
  AND p.event_time BETWEEN o.event_time AND o.event_time + INTERVAL '15' MINUTE
WHERE o.status = 'placed';
```

- Paid orders: the same rows as in B1, at the same moment.
- Unpaid orders: a row with `seconds_to_pay = NULL` and
  `payment_method = NULL`, but only **15 minutes (plus the 5 s watermark
  delay) after `ordered_at`**. Only then is Flink sure that no payment can
  match any more. With `INTERVAL '1' MINUTE` the `NULL` rows come after
  about one minute.

```
order_id      customer  total_chf  ordered_at               seconds_to_pay  payment_method
order-646401  Hugo          25.11  2026-11-12 06:28:59.561            NULL  NULL
```

**Why the time condition?** It tells Flink how long to wait. After the
watermark has passed `ordered_at + 15 min` the order can be removed from
memory. With only `ON o.order_id = p.order_id` this is a regular join:
Flink must keep **every** order and every payment forever, because a
partner could still come — the state grows without limit (unless you set
a state TTL, which again means "forget after a while").

**A payment after 20 minutes** falls outside the interval: B1 never shows
the order, B2 has already written it as unpaid (`NULL`). The late payment
itself is ignored by the join. The interval is a business rule —
choose it so that "no payment within the interval" really means "unpaid".

**For a "payment missing" alert** use the LEFT JOIN and filter
`WHERE p.order_id IS NULL`: exactly the orders that got no payment within
15 minutes, each reported once, 15 minutes after the order.

---

## Cleanup

Stop all running Flink jobs (red ■ in Dinky) and the producer (`Ctrl+C`).
