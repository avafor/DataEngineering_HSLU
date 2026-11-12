"""Simulate delivery riders and write their GPS pings to the `rider-locations` topic."""

from __future__ import annotations

import json
import os
import random
import time

from confluent_kafka import Producer

BROKERS = os.environ.get("KAFKA_BROKERS", "redpanda:29092")
TOPIC = os.environ.get("KAFKA_TOPIC", "rider-locations")
RATE_PER_SECOND = float(os.environ.get("RIDER_RATE", "5"))
DURATION_SECONDS = int(os.environ.get("RIDER_DURATION", "60"))
NUM_RIDERS = int(os.environ.get("RIDER_COUNT", "9"))

# Riders start somewhere near Lucerne main station.
START_LAT, START_LON = 47.0502, 8.3093
MAX_SPEED_KMH = 35.0


def delivery_callback(err, msg) -> None:
    if err is not None:
        print(f"delivery failed: {err}")


def main() -> None:
    producer = Producer({"bootstrap.servers": BROKERS, "linger.ms": 50})

    riders = {
        f"rider_{i}": [
            START_LAT + random.uniform(-0.01, 0.01),
            START_LON + random.uniform(-0.01, 0.01),
        ]
        for i in range(1, NUM_RIDERS + 1)
    }

    print(
        f"producing to {TOPIC} on {BROKERS} at ~{RATE_PER_SECOND}/s "
        f"for {DURATION_SECONDS}s ({NUM_RIDERS} riders)",
        flush=True,
    )
    sleep_between = 1.0 / RATE_PER_SECOND
    start = time.time()
    end = start + DURATION_SECONDS
    sent = 0
    last_tick = start
    last_sent = 0

    while time.time() < end:
        rider_id = random.choice(list(riders))
        position = riders[rider_id]
        position[0] += random.uniform(-0.0003, 0.0003)
        position[1] += random.uniform(-0.0003, 0.0003)
        event = {
            "rider_id": rider_id,
            "lat": round(position[0], 5),
            "lon": round(position[1], 5),
            "speed_kmh": round(random.uniform(0.0, MAX_SPEED_KMH), 1),
            "timestamp": time.time(),
        }
        producer.produce(
            TOPIC,
            key=rider_id.encode("utf-8"),
            value=json.dumps(event).encode("utf-8"),
            on_delivery=delivery_callback,
        )
        producer.poll(0)
        sent += 1
        time.sleep(sleep_between)

        now = time.time()
        if now - last_tick >= 1.0:
            elapsed = int(now - start)
            rate = sent - last_sent
            print(f"  t+{elapsed:>3}s  sent={sent:>6}  rate={rate}/s", flush=True)
            last_tick = now
            last_sent = sent

    producer.flush(10)
    print(f"done — sent {sent} events", flush=True)


if __name__ == "__main__":
    main()
