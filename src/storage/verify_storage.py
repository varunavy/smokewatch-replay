
"""Run genuine local storage checks; stdout can be saved as execution evidence."""

from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from mongo_loader import get_collection, insert_events
from postgres_loader import get_connection, upsert_county_hour


def main():
    suffix = uuid4().hex[:10]
    event_hour = "2021-07-13T10:00:00Z"

    # Use the same run code for MongoDB and PostgreSQL.
    run_code = f"synthetic-storage-verification-{suffix}"

    print("STORAGE INTEGRATION CHECK - SYNTHETIC TEST DATA")
    print("Test ID:", suffix)
    print("Run code:", run_code)

    # --------------------------------------------------
    # MongoDB validation
    # --------------------------------------------------
    client, collection = get_collection()

    try:
        print("MongoDB connection succeeded")

        events = [
            {
                "run_code": run_code,
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

        # Test 1: Insert three events into Run A.
        first_insert = insert_events(collection, events)
        print("MongoDB first insert (expected 3):", first_insert)
        assert first_insert == 3

        # Test 2: Retry the same events within Run A.
        repeat_insert = insert_events(collection, events)
        print("MongoDB repeat insert (expected 0):", repeat_insert)
        assert repeat_insert == 0

        event_ids = [event["event_id"] for event in events]

        run_a_count = collection.count_documents({
            "run_code": run_code,
            "event_id": {"$in": event_ids},
        })

        print("MongoDB Run A documents (expected 3):", run_a_count)
        assert run_a_count == 3

        # Test 3: Same event IDs under a different run code.
        second_run_code = f"{run_code}-second-run"

        second_run_events = [
            {**event, "run_code": second_run_code}
            for event in events
        ]

        second_run_insert = insert_events(
            collection,
            second_run_events
        )

        print(
            "MongoDB Run B first insert (expected 3):",
            second_run_insert
        )
        assert second_run_insert == 3

        # Test 4: Retry Run B.
        second_run_repeat = insert_events(
            collection,
            second_run_events
        )

        print(
            "MongoDB Run B repeat insert (expected 0):",
            second_run_repeat
        )
        assert second_run_repeat == 0

        run_b_count = collection.count_documents({
            "run_code": second_run_code,
            "event_id": {"$in": event_ids},
        })

        print("MongoDB Run B documents (expected 3):", run_b_count)
        assert run_b_count == 3

        # Verify both runs are retained.
        total_test_documents = collection.count_documents({
            "run_code": {"$in": [run_code, second_run_code]},
            "event_id": {"$in": event_ids},
        })

        print(
            "MongoDB documents across both runs (expected 6):",
            total_test_documents
        )
        assert total_test_documents == 6

        # Verify the original Kafka messages are preserved.
        sample_a = collection.find_one({
            "run_code": run_code,
            "event_id": events[0]["event_id"],
        })

        sample_b = collection.find_one({
            "run_code": second_run_code,
            "event_id": events[0]["event_id"],
        })

        assert sample_a is not None
        assert sample_b is not None

        assert sample_a["raw_message"] == events[0]
        assert sample_b["raw_message"] == second_run_events[0]

        print("MongoDB original Kafka messages preserved: True")
        print("PASS: MongoDB run_code + event_id deduplication")

    finally:
        client.close()

    # --------------------------------------------------
    # PostgreSQL validation
    # --------------------------------------------------
    conn = get_connection()

    try:
        print("PostgreSQL connection succeeded")

        root = Path(__file__).resolve().parents[2]

        with conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    (root / "sql" / "schema.sql").read_text(
                        encoding="utf-8"
                    )
                )

        print("PostgreSQL schema creation succeeded")

        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_type = 'BASE TABLE'
                  AND table_name IN (
                      'dim_time',
                      'dim_county',
                      'dim_source',
                      'dim_replay_scenario',
                      'dim_run',
                      'fact_source_event',
                      'fact_county_hour'
                  )
                ORDER BY table_name
            """)

            tables = [row[0] for row in cursor.fetchall()]

            print("Expected PostgreSQL tables:", tables)
            assert len(tables) == 7

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
            "first_join_ready_at": datetime(
                2021, 7, 13, 11, tzinfo=timezone.utc
            ),
            "processing_latency_ms": 250.0,
            "freshness_lag_event_hours": 1.0,
        }

        # Test PostgreSQL upsert idempotency.
        first_id = upsert_county_hour(conn, record)
        second_id = upsert_county_hour(conn, record)

        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*)
                FROM fact_county_hour f
                JOIN dim_run r ON r.run_id = f.run_id
                WHERE r.run_code = %s
            """, (run_code,))

            count = cursor.fetchone()[0]

        print("PostgreSQL upsert fact IDs:", first_id, second_id)
        print(
            "PostgreSQL rows after two upserts (expected 1):",
            count
        )

        assert first_id == second_id
        assert count == 1

        print("PASS: PostgreSQL upsert idempotency")

        # Execute analytical SQL queries.
        query_text = (
            root / "sql" / "analysis_queries.sql"
        ).read_text(encoding="utf-8")

        for i, query in enumerate(query_text.split(";"), start=1):
            if not query.strip():
                continue

            with conn.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()

            print(
                f"Analytical query {i}: succeeded; "
                f"returned {len(rows)} row(s)"
            )

        print("All local storage checks passed")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
