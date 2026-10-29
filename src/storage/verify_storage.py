"""Run genuine local storage checks; stdout can be saved as execution evidence."""
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from mongo_loader import get_collection, insert_events
from postgres_loader import get_connection, upsert_county_hour


def main():
    suffix = uuid4().hex[:10]
    event_hour = "2021-07-13T10:00:00Z"
    print("STORAGE INTEGRATION CHECK — SYNTHETIC TEST DATA")
    print("Test ID:", suffix)

    client, collection = get_collection()
    try:
        print("MongoDB connection succeeded")
        events = [
            {
                "source": source,
                "event_id": f"synthetic-{suffix}-{source}",
                "county": "Plumas",
                "event_hour": event_hour,
                "replay_scenario": f"synthetic-{suffix}",
                "scheduled_arrival_time": "2021-07-13T10:00:00Z",
                "delay_hours": 0,
                "payload": payload,
            }
            for source, payload in [
                ("nasa_firms", {"fire_detection_count": 3}),
                ("epa_pm25", {"pm25_ug_m3": 42.5}),
                ("noaa_weather", {"temperature_c": 29.1}),
            ]
        ]
        print("MongoDB first insert (expected 3):", insert_events(collection, events))
        print("MongoDB repeat insert (expected 0):", insert_events(collection, events))
        print("MongoDB matching synthetic documents:", collection.count_documents(
            {"event_id": {"$in": [e["event_id"] for e in events]}}
        ))
        sample = collection.find_one({"event_id": events[0]["event_id"]})
        assert sample and sample["raw_message"] == events[0]
        print("MongoDB original Kafka message preserved: True")
    finally:
        client.close()

    conn = get_connection()
    try:
        print("PostgreSQL connection succeeded")
        root = Path(__file__).resolve().parents[2]
        with conn:
            with conn.cursor() as cursor:
                cursor.execute((root / "sql" / "schema.sql").read_text(encoding="utf-8"))
        print("PostgreSQL schema creation succeeded")
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema='public' AND table_type='BASE TABLE'
                  AND table_name IN (
                    'dim_time','dim_county','dim_source','dim_replay_scenario',
                    'dim_run','fact_source_event','fact_county_hour')
                ORDER BY table_name
            """)
            tables = [row[0] for row in cursor.fetchall()]
            print("Expected PostgreSQL tables:", tables)
            assert len(tables) == 7

        run_code = f"synthetic-storage-verification-{suffix}"
        record = {
            "run_code": run_code,
            "scenario_code": f"synthetic-verification-{suffix}",
            "speed_seconds_per_event_hour": 1.0,
            "delay_level": "none",
            "county": "Plumas",
            "event_hour": event_hour,
            "distinct_source_count": 3,
            "fire_detection_count": 3,
            "pm25_ug_m3": 42.5,
            "temperature_c": 29.1,
            "humidity_pct": 24,
            "wind_speed_m_s": 3.2,
            "wind_direction_deg": None,
            "firms_available": True,
            "epa_available": True,
            "noaa_available": True,
            "first_join_ready_at": datetime(2021, 7, 13, 11, tzinfo=timezone.utc),
            "processing_latency_ms": 250.0,
            "freshness_lag_event_hours": 1.0,
        }
        first_id = upsert_county_hour(conn, record)
        second_id = upsert_county_hour(conn, record)
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) FROM fact_county_hour f
                JOIN dim_run r ON r.run_id = f.run_id
                WHERE r.run_code = %s
            """, (run_code,))
            count = cursor.fetchone()[0]
        print("PostgreSQL upsert fact IDs:", first_id, second_id)
        print("PostgreSQL rows after two upserts (expected 1):", count)
        assert first_id == second_id and count == 1

        query_text = (root / "sql" / "analysis_queries.sql").read_text(encoding="utf-8")
        for i, query in enumerate(query_text.split(";"), start=1):
            if not query.strip():
                continue
            with conn.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()
            print(f"Analytical query {i}: succeeded; returned {len(rows)} row(s)")
        print("All local storage checks passed")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
