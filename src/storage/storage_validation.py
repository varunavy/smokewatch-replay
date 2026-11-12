"""Read-only SmokeWatch MongoDB/PostgreSQL storage checks.

Run from repository root with the smoke-test data already loaded:
  python src/storage/storage_validation.py --mode smoke --mongo-scenario sanjana_smoke_test --pg-run sanjana_pg_smoke_01

For a complete run after Priya adds unique run_code to the Kafka envelope:
  python src/storage/storage_validation.py --mode full --mongo-run-code RUN_CODE --mongo-scenario SCENARIO --pg-run RUN_CODE --mongo-database RUN_DATABASE

This script does NOT insert, delete, or reset data. It does not measure
end-to-end timeliness or compare source measurement values to the batch data.
"""
from __future__ import annotations
import argparse
import sys
from datetime import datetime, timezone

from pymongo import MongoClient
from src.storage import mongo_loader as ml
from src.storage.postgres_loader import get_connection

REQUIRED = ("source", "event_id", "county", "event_hour", "replay_scenario", "scheduled_arrival_time", "delay_hours", "payload")
SOURCES = ("FIRMS", "EPA", "NOAA")


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--mode", choices=("smoke", "full"), required=True)
    p.add_argument("--mongo-scenario", required=True)
    p.add_argument("--mongo-run-code", help="Required for full run; must be present in raw_message.run_code")
    p.add_argument("--mongo-database", help="MongoDB database name; defaults to mongo_loader.DATABASE_NAME")
    p.add_argument("--pg-run", required=True)
    args = p.parse_args()
    if args.mode == "full" and not args.mongo_run_code:
        p.error("--mongo-run-code is required for --mode full")
    expected_events, expected_facts = (12, 4) if args.mode == "smoke" else (18000, 6000)
    expected_source = expected_events // 3
    print("SmokeWatch storage validation | mode:", args.mode)
    print("UTC timestamp:", datetime.now(timezone.utc).isoformat())
    print("Expected MongoDB events:", expected_events, "| PostgreSQL facts:", expected_facts)
    checks = {}
    client = None
    conn = None
    try:
        client = MongoClient(ml.MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        database_name = args.mongo_database or ml.DATABASE_NAME
        col = client[database_name][ml.COLLECTION_NAME]
        query = {"raw_message.replay_scenario": args.mongo_scenario}
        if args.mongo_run_code:
            query["raw_message.run_code"] = args.mongo_run_code
        total = col.count_documents(query)
        archive_total = col.count_documents({})
        run_total = (col.count_documents({"raw_message.run_code": args.mongo_run_code})
                     if args.mongo_run_code else None)
        by_source = {src: col.count_documents({**query, "source_name": src}) for src in SOURCES}
        missing_envelope = col.count_documents({**query, "$or": [
            {f"raw_message.{field}": {"$exists": False}} for field in REQUIRED
        ]})
        duplicates = list(col.aggregate([
            {"$match": query},
            {"$group": {"_id": {"source": "$source_name", "event_id": "$event_id"}, "n": {"$sum": 1}}},
            {"$match": {"n": {"$gt": 1}}},
            {"$limit": 1},
        ]))
        print("MongoDB database:", database_name)
        print("MongoDB matched events:", total, "| total archive documents:", archive_total)
        print("MongoDB source counts:", by_source)
        print("MongoDB missing required raw fields:", missing_envelope)
        print("MongoDB duplicate source/event keys:", len(duplicates))
        checks["mongo_event_count"] = total == expected_events
        checks["mongo_source_counts"] = all(v == expected_source for v in by_source.values())
        checks["mongo_raw_envelope_fields"] = missing_envelope == 0
        checks["mongo_no_duplicates"] = not duplicates
        if args.mode == "full":
            # Allows either one collection per run or a shared run-aware collection.
            checks["mongo_run_scenario_scope"] = run_total == total
            print("MongoDB documents for this run_code:", run_total)
            print("MongoDB run/scenario scope:", "PASS" if checks["mongo_run_scenario_scope"] else "FAIL (run contains other scenario or extra messages)")

        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) AS total,
                       COUNT(DISTINCT (f.scenario_id, f.county_id, f.time_id)) AS unique_keys,
                       COUNT(*) FILTER (WHERE f.distinct_source_count = 3) AS joined,
                       COUNT(*) FILTER (WHERE f.firms_available AND f.epa_available AND f.noaa_available) AS flags,
                       COUNT(*) FILTER (WHERE f.distinct_source_count = 3 AND f.first_join_ready_at IS NULL) AS missing_ready,
                       COUNT(DISTINCT c.county_name) AS counties,
                       COUNT(DISTINCT t.event_hour) AS hours
                FROM fact_county_hour f
                JOIN dim_run r ON r.run_id = f.run_id
                JOIN dim_county c ON c.county_id = f.county_id
                JOIN dim_time t ON t.time_id = f.time_id
                WHERE r.run_code = %s
            """, (args.pg_run,))
            total_pg, unique_keys, joined, flags, missing_ready, counties, hours = cur.fetchone()
        print("PostgreSQL run:", args.pg_run)
        print("PostgreSQL facts:", total_pg, "| unique fact keys:", unique_keys)
        print("PostgreSQL complete joins:", joined, "| all flags true:", flags)
        print("PostgreSQL missing join-ready timestamps:", missing_ready)
        print("PostgreSQL counties:", counties, "| distinct UTC hours:", hours)
        checks["pg_fact_count"] = total_pg == expected_facts
        checks["pg_no_duplicate_keys"] = total_pg == unique_keys
        checks["pg_join_complete"] = joined == expected_facts
        checks["pg_source_flags"] = flags == expected_facts
        checks["pg_ready_non_null"] = missing_ready == 0
        if args.mode == "full":
            checks["pg_five_counties"] = counties == 5
            checks["pg_1200_hours"] = hours == 1200

        for name, passed in checks.items():
            print(("PASS" if passed else "FAIL") + ": " + name)
        print("Overall:", "PASS" if all(checks.values()) else "FAIL")
        print("Limitations: this is a storage count/integrity audit; it does NOT validate replay-clock timeliness, measured throughput, or batch-reference value equality.")
        return 0 if all(checks.values()) else 1
    except Exception as exc:
        print("ERROR:", type(exc).__name__ + ":", str(exc), file=sys.stderr)
        return 2
    finally:
        if conn is not None:
            conn.close()
        if client is not None:
            client.close()


if __name__ == "__main__":
    sys.exit(main())
