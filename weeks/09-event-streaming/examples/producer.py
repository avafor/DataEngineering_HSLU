"""Simulate delivery riders and write their GPS pings to the `rider-locations` topic.

Usage, from the repository root:

    uv run --project streaming weeks/09-event-streaming/examples/producer.py
    uv run --project streaming weeks/09-event-streaming/examples/producer.py --shifts --duration 900

`--shifts` lets every rider work in shifts: 20-60 s of pings, then a 40-90 s break
without any ping. Used for the session windows in exercise 08.
"""

from __future__ import annotations

import argparse
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
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--duration", type=int, default=DURATION_SECONDS,
                        help=f"seconds to run (default {DURATION_SECONDS})")
    parser.add_argument("--shifts", action="store_true",
                        help="riders work in shifts with breaks of 40-90 s between them")
    args = parser.parse_args()

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
        f"for {args.duration}s ({NUM_RIDERS} riders{', in shifts' if args.shifts else ''})",
        flush=True,
    )
    sleep_between = 1.0 / RATE_PER_SECOND
    start = time.time()
    end = start + args.duration
    # Shift mode: every rider is on shift (True) or on a break (False) until a given time.
    on_shift = {rider_id: True for rider_id in riders}
    switch_at = {rider_id: start + random.uniform(20, 60) for rider_id in riders}
    sent = 0
    last_tick = start
    last_sent = 0

    while time.time() < end:
        active = list(riders)
        if args.shifts:
            for r in riders:
                if time.time() >= switch_at[r]:
                    on_shift[r] = not on_shift[r]
                    switch_at[r] = time.time() + (random.uniform(20, 60) if on_shift[r] else random.uniform(40, 90))
            active = [r for r in riders if on_shift[r]]
            if not active:              # everybody on a break
                time.sleep(sleep_between)
                continue
        rider_id = random.choice(active)
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
