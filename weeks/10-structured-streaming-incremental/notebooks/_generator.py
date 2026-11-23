# Databricks notebook source
# MAGIC %md
# MAGIC # Order generator
# MAGIC
# MAGIC Fake order events for this week. `write_batch(n)` writes one JSON-lines file into the landing folder,
# MAGIC more or less what the delivery app (or a Kafka connector) would do. `_rows(n)` gives you the same events as a
# MAGIC list, for sending them to Kafka (notebook 06).
# MAGIC
# MAGIC | Batch | Content |
# MAGIC |---|---|
# MAGIC | 1 | orders 1 to 200 placed |
# MAGIC | 2 | orders 201 to 400 placed, including Anna's pizza (`o-00342`); orders 1 to 150 delivered |
# MAGIC | 3 | orders 401 to 500 placed; orders 151 to 300 delivered, now with `tip_chf`; one row with a broken amount |
# MAGIC | 4 | Anna and 19 others cancel; one event is sent twice |
# MAGIC | 5 | orders 502 to 560 placed; orders 301 to 340 delivered |
# MAGIC | 6 | orders 561 to 600 placed; orders 362 to 400 delivered (the new file for the job in notebook 08) |
# MAGIC | 7 | orders 601 to 610 placed, with a new field `voucher_code` (the retry in notebook 08) |
# MAGIC
# MAGIC The random values are seeded, so everyone gets the same numbers.

# COMMAND ----------

import json
import random
from datetime import datetime, timedelta

RESTAURANTS = ["Pizzeria Bella", "Sushi Zen", "Burger Bros", "Curry Corner", "Taco Loco", "Green Bowl"]
CITIES = ["Lucerne", "Zurich", "Bern", "Basel"]
CUSTOMERS = ["Lea", "Noah", "Mia", "Luca", "Emma", "Elias", "Lina", "Ben", "Sofia", "Leon", "Nina", "Jan"]

ANNA_ORDER = "o-00342"
START = datetime(2026, 11, 20, 17, 0, 0)


def _order_id(i):
    return f"o-{i:05d}"


def _base_order(i):
    # seeded with i, so order i always looks the same
    rnd = random.Random(i)
    order = {
        "order_id": _order_id(i),
        "customer": rnd.choice(CUSTOMERS),
        "restaurant": rnd.choice(RESTAURANTS),
        "city": rnd.choice(CITIES),
        "items": rnd.randint(1, 5),
        "amount_chf": round(rnd.uniform(15, 80) * 20) / 20,
    }
    if order["order_id"] == ANNA_ORDER:
        order.update(customer="Anna", restaurant="Pizzeria Bella", city="Lucerne", items=2, amount_chf=34.50)
    return order


def _ts(i, minutes_later=0):
    if _order_id(i) == ANNA_ORDER:
        base = datetime(2026, 11, 20, 19, 2, 0)
    else:
        base = START + timedelta(seconds=15 * i)
    return (base + timedelta(minutes=minutes_later)).isoformat()


def _event(i, status, minutes_later=0, tip=False):
    row = dict(_base_order(i), status=status, event_ts=_ts(i, minutes_later))
    if tip:
        row["tip_chf"] = round(random.Random(1000 + i).uniform(0, 6) * 2) / 2
    return row


def _rows(batch):
    if batch == 1:
        return [_event(i, "placed") for i in range(1, 201)]
    if batch == 2:
        placed = [_event(i, "placed") for i in range(201, 401)]
        delivered = [_event(i, "delivered", minutes_later=35) for i in range(1, 151)]
        return placed + delivered
    if batch == 3:
        placed = [_event(i, "placed") for i in range(401, 501)]
        delivered = [_event(i, "delivered", minutes_later=40, tip=True) for i in range(151, 301)]
        broken = dict(_event(501, "placed"), amount_chf="12,50")  # wrong decimal separator
        return placed + delivered + [broken]
    if batch == 4:
        cancelled = [_event(i, "cancelled", minutes_later=7) for i in range(342, 362)]
        duplicate = _event(401, "placed")  # the same event delivered twice
        return cancelled + [duplicate]
    if batch == 5:
        placed = [_event(i, "placed", minutes_later=90) for i in range(502, 561)]
        delivered = [_event(i, "delivered", minutes_later=40, tip=True) for i in range(301, 341)]
        return placed + delivered
    if batch == 6:
        placed = [_event(i, "placed", minutes_later=100) for i in range(561, 601)]
        # 342 to 361 were cancelled in batch 4, so deliveries start at 362
        delivered = [_event(i, "delivered", minutes_later=40, tip=True) for i in range(362, 401)]
        return placed + delivered
    if batch == 7:
        # The app has a new feature: voucher codes. A field bronze doesn't know yet
        return [dict(_event(i, "placed", minutes_later=110), voucher_code=f"WINTER{i % 7}") for i in range(601, 611)]
    raise ValueError("batch must be 1 to 7")


def _exists(path):
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False


def write_batch(batch, folder=None):
    # Writes batch n as one JSON-lines file. If the file is already there, it's left alone.
    # Without `folder` it goes to the landing folder of the pipeline.
    folder = folder or INCOMING_DIR
    path = f"{folder}/orders_batch_{batch}.json"
    if _exists(path):
        print(f"Batch {batch} is already there: {path}")
        return path
    rows = _rows(batch)
    dbutils.fs.put(path, "\n".join(json.dumps(r) for r in rows), overwrite=False)
    print(f"Batch {batch}: {len(rows)} events written to {path}")
    return path


def write_batch_in_parts(batch, parts, folder):
    # The same events as write_batch, but split into `parts` smaller files. Only used for practice.
    rows = _rows(batch)
    size = -(-len(rows) // parts)
    paths = []
    for p in range(parts):
        path = f"{folder}/orders_batch_{batch}_part_{p + 1}.json"
        if not _exists(path):
            dbutils.fs.put(path, "\n".join(json.dumps(r) for r in rows[p * size:(p + 1) * size]), overwrite=False)
        paths.append(path)
    print(f"Batch {batch}: {len(rows)} events in {parts} files in {folder}")
    return paths


def write_late_order(folder, event_ts="2026-11-20T17:20:00"):
    # One order placed at `event_ts`, arriving much later than the others (notebook 07)
    row = dict(_base_order(999), order_id="o-late-1", status="placed", event_ts=event_ts)
    path = f"{folder}/orders_late.json"
    dbutils.fs.put(path, json.dumps(row), overwrite=True)
    print(f"A late order, placed at {event_ts}, written to {path}")
    return path
