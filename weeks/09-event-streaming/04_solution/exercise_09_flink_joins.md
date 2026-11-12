# Flink SQL joins — Solutions

Reference answers for [03_exercise/exercise_09_flink_joins.md](../03_exercise/exercise_09_flink_joins.md).

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

## Exercise A1 — inner join

```sql
SELECT o.order_id, o.restaurant, r.city, r.cuisine, o.total_chf
FROM orders AS o
JOIN restaurants AS r ON o.restaurant = r.restaurant
WHERE o.status = 'placed';
```

Orders of *Thai Garden* are missing: the restaurant is not in the
table, so the inner join finds no partner and drops the order. Silent
data loss — a classic trap with enrichment joins.

## Exercise A2 — left join

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

## Exercise A3 — revenue per cuisine

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

## Exercise B1 — interval join, paid orders

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

## Exercise B2 — interval join, unpaid orders

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
