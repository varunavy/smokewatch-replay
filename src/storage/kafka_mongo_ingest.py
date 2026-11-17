
"""Full-run Kafka to MongoDB ingestion for SmokeWatch."""

import argparse
import json
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from kafka import KafkaConsumer

try:
    from .mongo_loader import get_collection, insert_events
except ImportError:
    from mongo_loader import get_collection, insert_events


TOPICS = [
    "firms.events",
    "epa.pm25.events",
    "noaa.weather.events",
]


def main():
    parser = argparse.ArgumentParser(
        description="Ingest a SmokeWatch Kafka replay into MongoDB."
    )
    parser.add_argument("--run-code", required=True)
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--expected-events", type=int, default=None)
    parser.add_argument("--idle-seconds", type=int, default=120)
    parser.add_argument("--max-seconds", type=int, default=3600)
    parser.add_argument("--evidence", default=None)
    args = parser.parse_args()

    if not args.run_code.strip():
        parser.error("--run-code must not be empty")
    if args.expected_events is not None and args.expected_events <= 0:
        parser.error("--expected-events must be positive")
    if args.idle_seconds <= 0 or args.max_seconds <= 0:
        parser.error("Timeouts must be positive")

    consumer = None
    client = None

    start = time.monotonic()
    last_match = start

    matched = 0
    inserted = 0
    duplicates = 0
    seen_event_ids = set()
    sources = Counter()

    status = "INCOMPLETE"
    stop_reason = "not_started"
    error_message = None
    stored = None
    collection_name = None

    try:
        client, collection = get_collection()
        collection_name = collection.full_name

        consumer = KafkaConsumer(
            *TOPICS,
            bootstrap_servers=args.bootstrap,
            group_id=f"smokewatch-mongo-pilot-{uuid4().hex}",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=lambda value: json.loads(
                value.decode("utf-8")
            ),
            consumer_timeout_ms=1000,
        )

        print("MongoDB:", collection_name, flush=True)
        print("Kafka broker:", args.bootstrap, flush=True)
        print("Run code:", args.run_code, flush=True)
        print("Topics:", ", ".join(TOPICS), flush=True)
        print("Waiting for Kafka events...", flush=True)

        stop_reason = "maximum_runtime_reached"

        while time.monotonic() - start < args.max_seconds:
            for message in consumer:
                event = message.value

                if not isinstance(event, dict):
                    raise ValueError(
                        "Kafka message must contain a JSON object"
                    )

                if event.get("run_code") != args.run_code:
                    continue

                matched += 1
                last_match = time.monotonic()

                event_id = event.get("event_id")
                if not isinstance(event_id, str) or not event_id:
                    raise ValueError(
                        "Matching Kafka event has no valid event_id"
                    )

                if event_id in seen_event_ids:
                    duplicates += 1
                    continue

                # MongoDB insertion must succeed before counting
                # this event as processed.
                inserted += insert_events(collection, [event])

                seen_event_ids.add(event_id)
                sources[str(event.get("source"))] += 1

            unique_count = len(seen_event_ids)

            if (
                args.expected_events is not None
                and unique_count >= args.expected_events
            ):
                stop_reason = "expected_unique_events_reached"
                break

            if time.monotonic() - last_match >= args.idle_seconds:
                stop_reason = "idle_timeout"
                print(
                    "Idle timeout reached before confirmed completion.",
                    flush=True,
                )
                break

        stored = collection.count_documents(
            {"run_code": args.run_code}
        )

        unique_count = len(seen_event_ids)

        if args.expected_events is not None:
            status = (
                "PASS"
                if unique_count == args.expected_events
                and stored == args.expected_events
                else "FAIL"
            )
        else:
            status = "UNVERIFIED"

    except Exception as exc:
        error_message = f"{type(exc).__name__}: {exc}"
        stop_reason = "exception"
        status = "ERROR"
        print("ERROR:", error_message, file=sys.stderr)

    finally:
        if consumer is not None:
            consumer.close()
        if client is not None:
            client.close()

    report = {
        "status": status,
        "stop_reason": stop_reason,
        "run_code": args.run_code,
        "kafka_broker": args.bootstrap,
        "mongo_collection": collection_name,
        "expected_events": args.expected_events,
        "matched_kafka_messages": matched,
        "unique_events_processed": len(seen_event_ids),
        "duplicate_kafka_messages": duplicates,
        "new_documents_inserted": inserted,
        "stored_documents": stored,
        "source_counts": dict(sources),
        "elapsed_seconds": round(time.monotonic() - start, 2),
        "error": error_message,
        "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
    }

    print(json.dumps(report, indent=2), flush=True)

    if args.evidence:
        try:
            evidence_path = Path(args.evidence)
            evidence_path.parent.mkdir(
                parents=True, exist_ok=True
            )
            evidence_path.write_text(
                json.dumps(report, indent=2),
                encoding="utf-8",
            )
            print("Evidence saved:", evidence_path, flush=True)
        except OSError as exc:
            print("Evidence write failed:", exc, file=sys.stderr)
            return 2

    return 0 if status == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
