"""Replay standardized source-event files into Kafka.

Input: JSON Lines files, one per source, each record containing at least
    event_id, source, county, event_hour (ISO 8601, e.g. 2025-01-08T14:00:00Z)
Any other fields are passed through in the "payload" of the Kafka message.

Output: one Kafka message per record, to the topic for its source.
    key   = county (UTF-8)
    value = JSON {source, county, event_hour, event_id, replay_scenario,
                  scheduled_arrival_time, payload}

Pacing: records are merged across files and ordered by scheduled arrival time.
Replay speed controls how many event-hours elapse per real second.
Delay profile shifts each record's arrival time later than its event hour.

Example:
    python src/kafka/replay_producer.py --input-dir data/standardized \
        --speed fast --delay moderate --scenario smoke_event_jan2025
"""
import argparse
import glob
import json
import os
import random
import sys
import time
from datetime import datetime, timedelta, timezone

from config import BOOTSTRAP_SERVERS, DELAY_PROFILES, REPLAY_SPEEDS, TOPICS

REQUIRED_FIELDS = ("event_id", "source", "county", "event_hour")


def parse_ts(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def fmt_ts(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_records(input_dir: str):
    """Read every *.jsonl file in input_dir. Returns (records, skipped_count)."""
    records, skipped = [], 0
    for path in sorted(glob.glob(os.path.join(input_dir, "*.jsonl"))):
        with open(path, encoding="utf-8") as fh:
            for line_no, line in enumerate(fh, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    if any(f not in rec for f in REQUIRED_FIELDS):
                        raise ValueError("missing required field")
                    if rec["source"] not in TOPICS:
                        raise ValueError(f"unknown source {rec['source']!r}")
                    parse_ts(rec["event_hour"])
                except (ValueError, json.JSONDecodeError) as exc:
                    skipped += 1
                    print(f"skip {path}:{line_no}: {exc}", file=sys.stderr)
                    continue
                records.append(rec)
    return records, skipped


def build_messages(records, scenario: str, delay_profile: str, seed: int):
    """Attach scheduled arrival time and return messages sorted by arrival."""
    rng = random.Random(seed)
    lo, hi = DELAY_PROFILES[delay_profile]
    out = []
    for rec in records:
        event_hour = parse_ts(rec["event_hour"])
        delay_h = rng.uniform(lo, hi) if hi > 0 else 0.0
        arrival = event_hour + timedelta(hours=delay_h)
        payload = {k: v for k, v in rec.items() if k not in REQUIRED_FIELDS}
        out.append({
            "source": rec["source"],
            "county": rec["county"],
            "event_hour": fmt_ts(event_hour),
            "event_id": rec["event_id"],
            "replay_scenario": scenario,
            "scheduled_arrival_time": fmt_ts(arrival),
            "payload": payload,
        })
    out.sort(key=lambda m: m["scheduled_arrival_time"])
    return out


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--input-dir", required=True, help="folder of standardized *.jsonl files")
    p.add_argument("--bootstrap", default=BOOTSTRAP_SERVERS)
    p.add_argument("--speed", choices=REPLAY_SPEEDS, default="medium")
    p.add_argument("--delay", choices=DELAY_PROFILES, default="none")
    p.add_argument("--scenario", default="default", help="replay scenario label")
    p.add_argument("--limit", type=int, default=None, help="max records (smoke tests)")
    p.add_argument("--seed", type=int, default=42, help="seed for reproducible delays")
    p.add_argument("--dry-run", action="store_true",
                   help="print messages instead of sending (no broker needed)")
    args = p.parse_args()

    records, skipped = load_records(args.input_dir)
    if not records:
        print("no valid records found", file=sys.stderr)
        return 1
    messages = build_messages(records, args.scenario, args.delay, args.seed)
    if args.limit:
        messages = messages[: args.limit]

    producer = None
    if not args.dry_run:
        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap,
            key_serializer=lambda k: k.encode("utf-8"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
        )

    hours_per_sec = REPLAY_SPEEDS[args.speed]
    t0 = parse_ts(messages[0]["scheduled_arrival_time"])
    wall0 = time.monotonic()
    sent = 0
    for msg in messages:
        # Sleep until this message's simulated arrival time is reached.
        sim_hours = (parse_ts(msg["scheduled_arrival_time"]) - t0).total_seconds() / 3600
        wait = sim_hours / hours_per_sec - (time.monotonic() - wall0)
        if wait > 0:
            time.sleep(wait)
        topic = TOPICS[msg["source"]]
        if args.dry_run:
            print(f"[dry-run] topic={topic} key={msg['county']} value={json.dumps(msg)}")
        else:
            producer.send(topic, key=msg["county"], value=msg)
        sent += 1

    if producer:
        producer.flush()
        producer.close()
    print(f"done: sent={sent} skipped_invalid={skipped} speed={args.speed} "
          f"delay={args.delay} scenario={args.scenario}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
