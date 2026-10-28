"""Convert a standardized CSV file into the JSONL input the replay producer reads.

One CSV per source. Column names differ between sources, so you tell the script
which column holds what. Every other column is kept and goes into the message payload.

Example (adapt the column names to your files):
    python src/kafka/convert_to_jsonl.py --source nasa_firms \
        --input data/standardized/firms_2021.csv --output data/replay_input/firms.jsonl \
        --county-col county --time-col event_hour --event-id-col event_id

If the file has no event ID column, omit --event-id-col: IDs are built as
"<source>-<row number>" (stable as long as the file's row order does not change).
If date and hour are separate columns, use --date-col and --hour-col instead of --time-col.
Timestamps are floored to the hour and written as UTC ISO 8601.
"""
import argparse
import csv
import json
import os
import sys
from datetime import datetime, timezone

from config import TOPICS


def to_event_hour(row, args) -> str:
    if args.time_col:
        raw = row[args.time_col].strip()
    else:
        date = row[args.date_col].strip()
        hour = int(float(row[args.hour_col]))
        raw = f"{date}T{hour:02d}:00:00"
    dt = datetime.fromisoformat(raw.replace("Z", "+00:00").replace(" ", "T"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--source", required=True, choices=sorted(TOPICS))
    p.add_argument("--input", required=True, help="input CSV")
    p.add_argument("--output", required=True, help="output JSONL")
    p.add_argument("--county-col", required=True)
    p.add_argument("--event-id-col", default=None)
    p.add_argument("--time-col", default=None, help="column with a full timestamp")
    p.add_argument("--date-col", default=None)
    p.add_argument("--hour-col", default=None)
    args = p.parse_args()

    if not args.time_col and not (args.date_col and args.hour_col):
        p.error("give --time-col, or both --date-col and --hour-col")

    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
    written = skipped = 0
    with open(args.input, newline="", encoding="utf-8-sig") as fin, \
            open(args.output, "w", encoding="utf-8") as fout:
        reader = csv.DictReader(fin)
        used = {args.county_col, args.event_id_col, args.time_col, args.date_col, args.hour_col}
        for row_no, row in enumerate(reader, 1):
            try:
                county = row[args.county_col].strip()
                if not county:
                    raise ValueError("empty county")
                event_id = (row[args.event_id_col].strip() if args.event_id_col
                            else f"{args.source}-{row_no:06d}")
                if not event_id:
                    raise ValueError("empty event id")
                rec = {
                    "event_id": event_id,
                    "source": args.source,
                    "county": county,
                    "event_hour": to_event_hour(row, args),
                }
                for k, v in row.items():
                    if k not in used and k is not None:
                        rec[k] = v
                fout.write(json.dumps(rec) + "\n")
                written += 1
            except (KeyError, ValueError) as exc:
                skipped += 1
                print(f"skip row {row_no}: {exc}", file=sys.stderr)
    print(f"{args.source}: wrote {written} records to {args.output}, skipped {skipped}")
    return 0 if written else 1


if __name__ == "__main__":
    sys.exit(main())
