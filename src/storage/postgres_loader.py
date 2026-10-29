
"""
SmokeWatch Replay - PostgreSQL Storage Loader

Creates warehouse dimensions and inserts/updates
county-hour analytical results.

Install:
    pip install psycopg2-binary
"""

import os
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import Json


def get_connection():
    """Connect to the PostgreSQL warehouse."""
    password = os.getenv("POSTGRES_PASSWORD")
    if not password:
        raise ValueError("POSTGRES_PASSWORD environment variable is required")

    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "smokewatch"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=password,
        connect_timeout=5,
    )


def get_or_create_dimension(
    cursor, table, key_column, value_column, value
):
    """Insert a dimension value if missing and return its ID."""

    allowed = {
        ("dim_county", "county_id", "county_name"),
        ("dim_source", "source_id", "source_name"),
        ("dim_run", "run_id", "run_code"),
    }

    if (table, key_column, value_column) not in allowed:
        raise ValueError("Unsupported dimension")

    cursor.execute(
        f"""
        INSERT INTO {table} ({value_column})
        VALUES (%s)
        ON CONFLICT ({value_column})
        DO UPDATE SET {value_column} = EXCLUDED.{value_column}
        RETURNING {key_column}
        """,
        (value,),
    )

    return cursor.fetchone()[0]


def get_or_create_time(cursor, event_hour):
    """Create or retrieve the time dimension."""

    if isinstance(event_hour, str):
        event_hour = datetime.fromisoformat(
            event_hour.replace("Z", "+00:00")
        )

    if event_hour.tzinfo is None:
        event_hour = event_hour.replace(
            tzinfo=timezone.utc
        )

    event_hour = event_hour.astimezone(timezone.utc)

    cursor.execute(
        """
        INSERT INTO dim_time (
            event_hour, event_date, hour_of_day
        )
        VALUES (%s, %s, %s)
        ON CONFLICT (event_hour)
        DO UPDATE SET event_hour = EXCLUDED.event_hour
        RETURNING time_id
        """,
        (
            event_hour,
            event_hour.date(),
            event_hour.hour,
        ),
    )

    return cursor.fetchone()[0]


def get_or_create_scenario(
    cursor, scenario_code, speed, delay_level
):
    """Create or retrieve a replay scenario."""

    cursor.execute(
        """
        INSERT INTO dim_replay_scenario (
            scenario_code,
            speed_seconds_per_event_hour,
            delay_level
        )
        VALUES (%s, %s, %s)
        ON CONFLICT (scenario_code)
        DO UPDATE SET
            speed_seconds_per_event_hour =
                EXCLUDED.speed_seconds_per_event_hour,
            delay_level = EXCLUDED.delay_level
        RETURNING scenario_id
        """,
        (scenario_code, speed, delay_level),
    )

    return cursor.fetchone()[0]


def upsert_county_hour(connection, record):
    """
    Insert or update a county-hour fact.

    The first join-ready timestamp is preserved
    when a record is updated again.
    """

    with connection:
        with connection.cursor() as cursor:
            run_id = get_or_create_dimension(
                cursor,
                "dim_run",
                "run_id",
                "run_code",
                record["run_code"],
            )

            county_id = get_or_create_dimension(
                cursor,
                "dim_county",
                "county_id",
                "county_name",
                record["county"],
            )

            time_id = get_or_create_time(
                cursor, record["event_hour"]
            )

            scenario_id = get_or_create_scenario(
                cursor,
                record["scenario_code"],
                record["speed_seconds_per_event_hour"],
                record["delay_level"],
            )

            cursor.execute(
                """
                INSERT INTO fact_county_hour (
                    run_id,
                    scenario_id,
                    county_id,
                    time_id,
                    distinct_source_count,
                    fire_detection_count,
                    pm25_ug_m3,
                    temperature_c,
                    humidity_pct,
                    wind_speed_m_s,
                    wind_direction_deg,
                    firms_available,
                    epa_available,
                    noaa_available,
                    first_join_ready_at,
                    processing_latency_ms,
                    freshness_lag_event_hours
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s
                )
                ON CONFLICT (
                    run_id, scenario_id, county_id, time_id
                )
                DO UPDATE SET
                    distinct_source_count =
                        EXCLUDED.distinct_source_count,
                    fire_detection_count =
                        EXCLUDED.fire_detection_count,
                    pm25_ug_m3 =
                        EXCLUDED.pm25_ug_m3,
                    temperature_c =
                        EXCLUDED.temperature_c,
                    humidity_pct =
                        EXCLUDED.humidity_pct,
                    wind_speed_m_s =
                        EXCLUDED.wind_speed_m_s,
                    wind_direction_deg =
                        EXCLUDED.wind_direction_deg,
                    firms_available =
                        EXCLUDED.firms_available,
                    epa_available =
                        EXCLUDED.epa_available,
                    noaa_available =
                        EXCLUDED.noaa_available,
                    first_join_ready_at = COALESCE(
                        fact_county_hour.first_join_ready_at,
                        EXCLUDED.first_join_ready_at
                    ),
                    processing_latency_ms =
                        EXCLUDED.processing_latency_ms,
                    freshness_lag_event_hours =
                        EXCLUDED.freshness_lag_event_hours,
                    updated_at = NOW()
                RETURNING fact_id
                """,
                (
                    run_id,
                    scenario_id,
                    county_id,
                    time_id,
                    record.get("distinct_source_count", 0),
                    record.get("fire_detection_count"),
                    record.get("pm25_ug_m3"),
                    record.get("temperature_c"),
                    record.get("humidity_pct"),
                    record.get("wind_speed_m_s"),
                    record.get("wind_direction_deg"),
                    record.get("firms_available", False),
                    record.get("epa_available", False),
                    record.get("noaa_available", False),
                    record.get("first_join_ready_at"),
                    record.get("processing_latency_ms"),
                    record.get("freshness_lag_event_hours"),
                ),
            )

            return cursor.fetchone()[0]


def insert_source_event(connection, record):
    """Insert event metadata and optional limited provenance/audit JSON.

    MongoDB is the authoritative raw-event archive; source_payload is
    not intended to contain or replace the full original Kafka message.
    """

    with connection:
        with connection.cursor() as cursor:
            run_id = get_or_create_dimension(
                cursor,
                "dim_run",
                "run_id",
                "run_code",
                record["run_code"],
            )

            source_id = get_or_create_dimension(
                cursor,
                "dim_source",
                "source_id",
                "source_name",
                record["source_name"].upper(),
            )

            county_id = get_or_create_dimension(
                cursor,
                "dim_county",
                "county_id",
                "county_name",
                record["county"],
            )

            time_id = get_or_create_time(
                cursor, record["event_hour"]
            )

            scenario_id = get_or_create_scenario(
                cursor,
                record["scenario_code"],
                record["speed_seconds_per_event_hour"],
                record["delay_level"],
            )

            cursor.execute(
                """
                INSERT INTO fact_source_event (
                    run_id,
                    scenario_id,
                    source_id,
                    county_id,
                    time_id,
                    event_id,
                    ingestion_timestamp,
                    source_payload
                )
                VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s, %s
                )
                ON CONFLICT (
                    run_id, scenario_id, source_id, event_id
                )
                DO NOTHING
                RETURNING source_event_key
                """,
                (
                    run_id,
                    scenario_id,
                    source_id,
                    county_id,
                    time_id,
                    str(record["event_id"]),
                    record.get(
                        "ingestion_timestamp",
                        datetime.now(timezone.utc),
                    ),
                    Json(record.get("source_payload", {})),
                ),
            )

            result = cursor.fetchone()
            return result[0] if result else None


if __name__ == "__main__":
    try:
        connection = get_connection()
        print("PostgreSQL connection successful")

        with connection.cursor() as cursor:
            cursor.execute("SELECT version()")
            print(cursor.fetchone()[0])

        connection.close()

    except psycopg2.Error as error:
        print("PostgreSQL connection failed:", error)
        raise
