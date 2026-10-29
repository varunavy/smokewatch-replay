"""Replay standardized source-event files into Kafka.

Input: JSON Lines files (*.jsonl) in --input-dir, each record containing at least
    event_id, source, county, event_hour (ISO 8601, e.g. 2021-08-14T10:00:00Z)
Any other fields are passed through in the "payload" of the Kafka message.

Output: one Kafka message per record, to the topic for its source.
    key   = county (UTF-8)
    value = JSON {source, county, event_hour, event_id, replay_scenario,
                  scheduled_arrival_time, delay_hours, payload}

Delay rule (deterministic, SHA-256 based):
    u = first 8 bytes of SHA-256("<event_id>:select") scaled to [0, 1)
    event is delayed if u < fraction of the chosen delay profile
    delay = min_hours + v * (max_hours - min_hours), where v is derived the same
    way from SHA-256("<event_id>:amount")
    scheduled_arrival_time = event_hour + delay. Non-selected events get delay 0.
The rule depends only on event_id and the delay profile. The scenario label (for
example slow__moderate, medium__moderate, fast__moderate) and the replay speed are
NOT part of the hash, so every speed delays exactly the same events by exactly the
same amounts and replay speed is the only changed factor. The scenario is still
written into every Kafka message to identify the run.

Pacing: records are ordered by scheduled arrival time. Replay speed controls how
many event-hours elapse per real second.

Examples:
    # validate input only (no Kafka, no sending)
    python src/kafka/replay_producer.py --input-dir data/standardized --validate-only --expect-per-source 6000
    # dry run (prints messages, no broker needed)
    python src/kafka/replay_producer.py --input-dir data/standardized --speed fast --delay moderate --scenario baseline --dry-run
    # live run
    python src/kafka/replay_producer.py --input-dir data/standardized --speed fast --delay moderate --scenario baseline
"""
import argparse
import glob
import hashlib
import json
import os
import sys
import time
from collections import Counter
from datetime import datetime, timedelta, timezone

from config import BOOTSTRAP_SERVERS, DELAY_PROFILES, REPLAY_SPEEDS, TOPICS

REQUIRED_FIELDS = ("event_id", "source", "county", "event_hour")


def parse_ts(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def fmt_ts(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def hash_unit(event_id: str, label: str) -> float:
    """Deterministic number in [0, 1) from SHA-256 of event_id and label.

    The scenario/speed label is deliberately not an input.
    """
    digest = hashlib.sha256(f"{event_id}:{label}".encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") / 2**64


def delay_hours_for(event_id: str, delay_profile: str) -> float:
    """Return the delay (event-hours) for one event; 0.0 if not selected."""
    prof = DELAY_PROFILES[delay_profile]
    if prof["fraction"] <= 0:
        return 0.0
    if hash_unit(event_id, "select") >= prof["fraction"]:
        return 0.0
    v = hash_unit(event_id, "amount")
    return round(prof["min_hours"] + v * (prof["max_hours"] - prof["min_hours"]), 4)


def load_records(input_dir: str):
    """Read every *.jsonl file in input_dir. Returns (records, skipped_count)."""
    records, skipped = [], 0
    paths = sorted(glob.glob(os.path.join(input_dir, "*.jsonl")))
    if not paths:
        print(f"no *.jsonl files found in {input_dir}", file=sys.stderr)
    for path in paths:
        with open(path, encoding="utf-8") as fh:
            for line_no, line in enumerate(fh, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    if any(f not in rec or rec[f] in (None, "") for f in REQUIRED_FIELDS):
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


def build_messages(records, scenario: str, delay_profile: str):
    """Attach scheduled arrival time and return messages sorted by arrival."""
    out = []
    for rec in records:
        event_hour = parse_ts(rec["event_hour"])
        delay_h = delay_hours_for(str(rec["event_id"]), delay_profile)
        arrival = event_hour + timedelta(hours=delay_h)
        payload = {k: v for k, v in rec.items() if k not in REQUIRED_FIELDS}
        out.append({
            "source": rec["source"],
            "county": rec["county"],
            "event_hour": fmt_ts(event_hour),
            "event_id": rec["event_id"],
            "replay_scenario": scenario,
            "scheduled_arrival_time": fmt_ts(arrival),
            "delay_hours": delay_h,
            "payload": payload,
        })
    # event_id as final tie-breaker keeps ordering identical across runs
    out.sort(key=lambda m: (m["scheduled_arrival_time"], m["source"], str(m["event_id"])))
    return out


def report_counts(records, skipped, expect_per_source):
    counts = Counter(r["source"] for r in records)
    print(f"valid events: {len(records)}  skipped invalid: {skipped}")
    for source in TOPICS:
        print(f"  {source}: {counts.get(source, 0)}")
    if expect_per_source is not None:
        ok = skipped == 0 and all(counts.get(s, 0) == expect_per_source for s in TOPICS)
        print(f"expected {expect_per_source} per source, {expect_per_source * len(TOPICS)} total: "
              f"{'OK' if ok else 'MISMATCH'}")
        return ok
    return True


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--input-dir", required=True, help="folder of standardized *.jsonl files")
    p.add_argument("--bootstrap", default=BOOTSTRAP_SERVERS)
    p.add_argument("--speed", choices=REPLAY_SPEEDS, default="medium")
    p.add_argument("--delay", choices=DELAY_PROFILES, default="none")
    p.add_argument("--scenario", default="default", help="replay scenario label written into each message (does not change which events are delayed)")
    p.add_argument("--limit", type=int, default=None, help="max records to send (smoke tests)")
    p.add_argument("--dry-run", action="store_true",
                   help="print messages instead of sending (no broker needed)")
    p.add_argument("--quiet", action="store_true",
                   help="with --dry-run, print only the summary and the first 5 messages")
    p.add_argument("--no-pacing", action="store_true",
                   help="skip the replay-speed sleeps (useful for fast full-input dry runs)")
    p.add_argument("--validate-only", action="store_true",
                   help="read and count input records, then exit (no sending)")
    p.add_argument("--expect-per-source", type=int, default=None,
                   help="with --validate-only or --dry-run, exit 1 unless every source has exactly N valid events")
    args = p.parse_args()

    records, skipped = load_records(args.input_dir)
    ok = report_counts(records, skipped, args.expect_per_source)
    if args.validate_only:
        return 0 if ok and records else 1
    if not records:
        print("no valid records found", file=sys.stderr)
        return 1
    if not ok:
        return 1

    messages = build_messages(records, args.scenario, args.delay)
    delayed = sum(1 for m in messages if m["delay_hours"] > 0)
    print(f"delay profile={args.delay}: {delayed}/{len(messages)} events delayed "
          f"({100 * delayed / len(messages):.1f}%)")
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
        if wait > 0 and not args.no_pacing:
            time.sleep(wait)
        topic = TOPICS[msg["source"]]
        if args.dry_run:
            if not args.quiet or sent < 5:
                print(f"[dry-run] topic={topic} key={msg['county']} value={json.dumps(msg)}")
        else:
            producer.send(topic, key=msg["county"], value=msg)
        sent += 1

    if producer:
        producer.flush()
        producer.close()
    mode = "DRY RUN (nothing sent to Kafka)" if args.dry_run else "LIVE"
    print(f"done [{mode}]: processed={sent} skipped_invalid={skipped} speed={args.speed} "
          f"delay={args.delay} scenario={args.scenario}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
