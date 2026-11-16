"""Producer-only run-code regression check. Sends no Kafka messages."""

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def dry_run(run_code):
    command = [
        sys.executable,
        "src/kafka/replay_producer.py",
        "--input-dir", "data/replay_input",
        "--speed", "fast",
        "--delay", "none",
        "--scenario", "pair_run_code_check",
        "--run-code", run_code,
        "--limit", "12",
        "--dry-run",
    ]
    result = subprocess.run(
        command,
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
        timeout=60,
    )
    events = [
        json.loads(line.split(" value=", 1)[1])
        for line in result.stdout.splitlines()
        if line.startswith("[dry-run]") and " value=" in line
    ]
    assert len(events) == 12, f"Expected 12 events, got {len(events)}"
    assert len({e["event_id"] for e in events}) == 12, "Duplicate IDs"
    assert all(e["run_code"] == run_code for e in events), "Wrong run code"
    return events


def main():
    first = dry_run("pair_check_A")
    second = dry_run("pair_check_B")

    def without_run_code(events):
        return [
            {key: value for key, value in event.items() if key != "run_code"}
            for event in events
        ]

    assert without_run_code(first) == without_run_code(second), (
        "Events differ in fields other than run_code"
    )
    print("PASS: 12 unique events per dry run; correct run codes.")
    print("PASS: event IDs and all other message fields are unchanged.")


if __name__ == "__main__":
    main()
