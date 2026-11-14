
"""Kafka to MongoDB smoke test for a specified replay run."""

import argparse
import json
import sys
from uuid import uuid4

from kafka import KafkaConsumer
from mongo_loader import get_collection, insert_events

EXPECTED_EVENTS = 12

TOPICS = [
    "firms.events",
    "epa.pm25.events",
    "noaa.weather.events",
]


def main():
    parser = argparse.ArgumentParser(
        description="Validate Kafka to MongoDB smoke-test ingestion."
    )
    parser.add_argument(
        "--run-code",
        required=True,
        help="Run code of the Kafka replay to validate",
    )
    args = parser.parse_args()

    if not args.run_code.strip():
        parser.error("--run-code cannot be empty")

    consumer = None
    client = None

    try:
        consumer = KafkaConsumer(
            *TOPICS,
            bootstrap_servers="localhost:9092",
            group_id=f"smokewatch-mongo-smoke-{uuid4().hex[:8]}",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=lambda data: json.loads(
                data.decode("utf-8")
            ),
            consumer_timeout_ms=20000,
        )

        client, collection = get_collection()

        matched = 0
        inserted = 0

        for message in consumer:
            event = message.value

            if event.get("run_code") != args.run_code:
                continue

            matched += 1
            inserted += insert_events(collection, [event])

            if matched >= EXPECTED_EVENTS:
                break

        stored = collection.count_documents({
            "run_code": args.run_code
        })

        print("Run code:", args.run_code)
        print("Kafka events matched:", matched)
        print("New MongoDB documents inserted:", inserted)
        print("MongoDB documents for run:", stored)

        if matched == EXPECTED_EVENTS and stored == EXPECTED_EVENTS:
            print("PASS: Kafka to MongoDB smoke test")
            return 0

        print("FAIL: Incomplete Kafka to MongoDB smoke test")
        return 1

    except Exception as exc:
        print(
            f"ERROR: {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 2

    finally:
        if consumer is not None:
            consumer.close()
        if client is not None:
            client.close()


if __name__ == "__main__":
    sys.exit(main())
