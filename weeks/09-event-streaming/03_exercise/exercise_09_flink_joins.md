# Flink SQL joins on the food-delivery stream

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

## Exercise A — stream–table join: enrich orders

### A1 — inner join

**Goal:** every new order (status `placed`) with `city` and `cuisine` of its restaurant.

Hints:

- `FROM orders AS o JOIN restaurants AS r ON o.restaurant = r.restaurant`
- Only events with `o.status = 'placed'`.

```sql
-- TODO: SELECT o.order_id, o.restaurant, r.city, r.cuisine, o.total_chf
--       FROM orders AS o JOIN restaurants AS r ON ... WHERE ...
```

**Check:** a new row for every new order, each with `city` and `cuisine`.
Orders of *Thai Garden* never appear here.

### A2 — left join

Run the same query with `LEFT JOIN` instead of `JOIN`.

**Check:** now *Thai Garden* orders appear too — with `NULL` in `city`
and `cuisine`.

### A3 — bonus: revenue per cuisine

```sql
-- TODO: SELECT r.cuisine, COUNT(*) AS orders, ROUND(SUM(o.total_chf), 2) AS revenue_chf
--       FROM orders AS o JOIN restaurants AS r ON ... WHERE ... GROUP BY r.cuisine
```

**Questions**

- Orders of *Thai Garden* never appear in A1. Why? What do they look like in A2?
- In A3 there is no window. What happens to the rows over time? (In the
  SQL client you see `-U` and `+U`: what do they mean?)
- If a restaurant moved to another city, how would the view have to change?
  Why would a real system rather keep the restaurant list in a database or
  a Kafka topic?

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

```sql
-- TODO: SELECT o.order_id, o.customer, o.total_chf, o.event_time AS ordered_at,
--              ... AS seconds_to_pay, p.payment_method
--       FROM orders AS o JOIN payments AS p ON ... AND ... WHERE ...
```

**Check:** rows appear a few seconds after each order — as soon as its
payment has arrived — with `seconds_to_pay` between 2 and 30.

Let it run for a few minutes, and keep the producer's terminal in view:
it reports how many orders will *never* be paid.

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

---

## Cleanup

Stop all running Flink jobs (red ■ in Dinky) and the producer (`Ctrl+C`).
