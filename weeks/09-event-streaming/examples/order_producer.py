"""Simulate the food-delivery app: orders with status updates, plus payments.

Writes to two topics:

- `orders`   key = order id, value = {order_id, customer, restaurant, total_chf, status, timestamp}
             every order goes through placed -> accepted -> picked_up -> delivered
- `payments` key = order id, value = {order_id, amount_chf, payment_method, timestamp}
             the payment arrives a few seconds after `placed`, or never (about 1 in 5 orders)

`timestamp` is the event time in UNIX seconds (float), like in all other producers.

Usage, from the repository root:

    uv run --project streaming weeks/09-event-streaming/examples/order_producer.py
    uv run --project streaming weeks/09-event-streaming/examples/order_producer.py --duration 600
    uv run --project streaming weeks/09-event-streaming/examples/order_producer.py --late 3

`--late N` sends ONE order whose event time lies N minutes in the past and exits.
"""

from __future__ import annotations

import argparse
import heapq
import itertools
import json
import os
import random
import time

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

BROKERS = os.environ.get("KAFKA_BROKERS", "redpanda:29092")
ORDERS_TOPIC = "orders"
PAYMENTS_TOPIC = "payments"
PARTITIONS = 3

CUSTOMERS = ["Anna", "Ben", "Clara", "David", "Elif", "Fabio", "Gina", "Hugo", "Ines", "Jonas"]
# Restaurant -> typical order total in CHF. "Thai Garden" is new on the platform:
# it is missing from the restaurants table of exercise 09 on purpose.
RESTAURANTS = {
    "Pizzeria Roma": 32.0,
    "Sushi Bar Kyoto": 48.0,
    "Burger House": 22.0,
    "Curry Palace": 29.0,
    "Taqueria Luz": 26.0,
    "Thai Garden": 31.0,
}
STATUSES = ["placed", "accepted", "picked_up", "delivered"]
# Seconds after `placed` at which the next status follows (min, max).
STATUS_DELAYS = {"accepted": (3, 10), "picked_up": (20, 40), "delivered": (45, 90)}
PAYMENT_DELAY = (2, 30)        # seconds after `placed`
NO_PAYMENT_SHARE = 0.2         # share of orders that are never paid


def ensure_topics(brokers: str) -> None:
    """Create `orders` and `payments` with 3 partitions if they do not exist yet."""
    admin = AdminClient({"bootstrap.servers": brokers})
    existing = admin.list_topics(timeout=10).topics
    for name in (ORDERS_TOPIC, PAYMENTS_TOPIC):
        if name not in existing:
            admin.create_topics([NewTopic(name, num_partitions=PARTITIONS)])[name].result()
            print(f"created topic {name!r} with {PARTITIONS} partitions", flush=True)


def delivery_callback(err, msg) -> None:
    if err is not None:
        print(f"delivery failed: {err}")


def send(producer: Producer, topic: str, key: str, event: dict) -> None:
    producer.produce(
        topic,
        key=key.encode("utf-8"),
        value=json.dumps(event).encode("utf-8"),
        on_delivery=delivery_callback,
    )
    producer.poll(0)


def new_order(order_id: str, event_time: float) -> dict:
    restaurant = random.choice(list(RESTAURANTS))
    return {
        "order_id": order_id,
        "customer": random.choice(CUSTOMERS),
        "restaurant": restaurant,
        "total_chf": round(RESTAURANTS[restaurant] * random.uniform(0.6, 1.5), 2),
        "status": "placed",
        "timestamp": event_time,
    }


def send_late_order(producer: Producer, minutes: float) -> None:
    """Send one `placed` order whose event time lies `minutes` in the past."""
    event_time = time.time() - minutes * 60
    order_id = f"order-late-{random.randint(100, 999)}"
    order = new_order(order_id, event_time)
    send(producer, ORDERS_TOPIC, order_id, order)
    producer.flush(10)
    print(
        f"sent LATE order {order_id}: event time {time.strftime('%H:%M:%S', time.gmtime(event_time))} UTC, "
        f"now {time.strftime('%H:%M:%S', time.gmtime())} UTC ({minutes:g} min late)",
        flush=True,
    )


def run(producer: Producer, duration: int, order_rate: float) -> None:
    """Place new orders continuously and send their follow-up events when they are due."""
    order_numbers = itertools.count(int(time.time()) % 100000 * 10)
    due: list[tuple[float, int, str, str, dict]] = []   # heap of (due_at, seq, topic, key, event)
    seq = itertools.count()
    start = time.time()
    end = start + duration
    next_order_at = start
    counts = {"orders": 0, "updates": 0, "payments": 0, "unpaid": 0}
    last_report = start

    print(
        f"producing to {ORDERS_TOPIC!r} and {PAYMENTS_TOPIC!r} on {BROKERS} "
        f"for {duration}s (~{order_rate:g} new orders/s), stop with Ctrl+C",
        flush=True,
    )
    while time.time() < end:
        now = time.time()

        if now >= next_order_at:
            order_id = f"order-{next(order_numbers)}"
            order = new_order(order_id, now)
            send(producer, ORDERS_TOPIC, order_id, order)
            counts["orders"] += 1
            for status in STATUSES[1:]:
                due_at = now + random.uniform(*STATUS_DELAYS[status])
                heapq.heappush(due, (due_at, next(seq), ORDERS_TOPIC, order_id, {**order, "status": status}))
            if random.random() < NO_PAYMENT_SHARE:
                counts["unpaid"] += 1          # this order will never be paid
            else:
                payment = {
                    "order_id": order_id,
                    "amount_chf": order["total_chf"],
                    "payment_method": random.choice(["twint", "card"]),
                }
                due_at = now + random.uniform(*PAYMENT_DELAY)
                heapq.heappush(due, (due_at, next(seq), PAYMENTS_TOPIC, order_id, payment))
            next_order_at = now + random.expovariate(order_rate)

        while due and due[0][0] <= now:
            _, _, topic, key, event = heapq.heappop(due)
            event["timestamp"] = time.time()           # event time = when it happens
            send(producer, topic, key, event)
            counts["updates" if topic == ORDERS_TOPIC else "payments"] += 1

        if now - last_report >= 10:
            print(
                f"  t+{int(now - start):>4}s  orders={counts['orders']:>4}  "
                f"status updates={counts['updates']:>4}  payments={counts['payments']:>4}  "
                f"never paid={counts['unpaid']:>3}",
                flush=True,
            )
            last_report = now
        time.sleep(0.05)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--duration", type=int, default=1800, help="seconds to run (default 1800 = 30 min)")
    parser.add_argument("--rate", type=float, default=0.5, help="new orders per second (default 0.5)")
    parser.add_argument("--late", type=float, metavar="MINUTES",
                        help="send ONE order with an event time MINUTES in the past and exit")
    args = parser.parse_args()

    ensure_topics(BROKERS)
    producer = Producer({"bootstrap.servers": BROKERS, "linger.ms": 50})

    if args.late is not None:
        send_late_order(producer, args.late)
        return

    try:
        run(producer, args.duration, args.rate)
    except KeyboardInterrupt:
        print("\nstopped", flush=True)
    finally:
        # Follow-up events still pending are dropped: those orders stay unfinished.
        producer.flush(10)
    print("done", flush=True)


if __name__ == "__main__":
    main()
