# SmokeWatch Replay — Storage Component

## Purpose and ownership

- **MongoDB (`smokewatch.raw_events`) is the authoritative, complete raw-event archive.** It retains each incoming agency/Kafka message in `raw_message` unchanged, including `source`, `event_id`, `county`, `event_hour`, `replay_scenario`, `scheduled_arrival_time`, `delay_hours`, and nested `payload`. Searchable normalized fields and an `ingestion_timestamp` are stored alongside it.
- **PostgreSQL is the analytical warehouse.** `fact_county_hour` holds Spark's processed county-hour measurements and join/timing metrics; dimensions describe run, scenario, county, source, and UTC hour. `fact_source_event.source_payload JSONB` is **only an optional limited provenance/audit copy**, not a complete raw archive or substitute for MongoDB.

## Files and setup (Windows Command Prompt, repository root)

- `src/storage/mongo_loader.py`: raw Kafka/agency event archive
- `src/storage/postgres_loader.py`: dimensional warehouse upsert functions
- `src/storage/load_demo.py`: optional event-metadata demonstration
- `src/storage/verify_storage.py`: real local integration checks and evidence
- `sql/schema.sql`: schema, constraints, indexes and source dimensions
- `sql/analysis_queries.sql`: eight analytical and validation queries

Requires Python 3.10+, MongoDB on `localhost:27017`, and PostgreSQL database `smokewatch` (Docker mapping in this project: **host port 5433** to container port 5432).

```bat
python -m pip install -r src\storage\requirements.txt
set POSTGRES_HOST=127.0.0.1
set POSTGRES_PORT=5433
set POSTGRES_DB=smokewatch
set POSTGRES_USER=postgres
set POSTGRES_PASSWORD=YOUR_LOCAL_POSTGRES_PASSWORD
python src\storage\mongo_loader.py
python src\storage\postgres_loader.py
```

`POSTGRES_PASSWORD` is **required**; there is no password default. Set it in your local terminal or secure secret manager. **Never commit actual passwords, `.env`, or credentials.** The Docker container may have a different password if its database volume was initialized earlier.

Apply the schema and run the analytical SQL (when using the `smokewatch-postgres` Docker container):

```bat
docker exec -i smokewatch-postgres psql -v ON_ERROR_STOP=1 -U postgres -d smokewatch < sql\schema.sql
docker exec -i smokewatch-postgres psql -v ON_ERROR_STOP=1 -U postgres -d smokewatch < sql\analysis_queries.sql
```

## Priya's Kafka → MongoDB contract

`insert_events(collection, [message])` accepts `source` values `nasa_firms`, `epa_pm25`, and `noaa_weather`, mapped to normalized `source_name` `FIRMS`, `EPA`, and `NOAA`. Existing `source_name` (`FIRMS`, `EPA`, `NOAA`) input remains supported. Required: source/source_name, event_id, county, event_hour (ISO-8601 string or datetime). Other Kafka fields are retained without filtering.

Example incoming message (synthetic):

```json
{
  "source": "nasa_firms",
  "event_id": "demo-kafka-firms-001",
  "county": "Plumas",
  "event_hour": "2021-07-13T10:00:00Z",
  "replay_scenario": "demo-normal",
  "scheduled_arrival_time": "2021-07-13T10:00:00Z",
  "delay_hours": 0,
  "payload": {"fire_detection_count": 3, "satellite": "VIIRS"}
}
```

The MongoDB document contains the **complete original** under `raw_message` and retains top-level Kafka fields, plus normalized `source_name`, UTC `event_hour` and storage `ingestion_timestamp`. Unique `(source_name, event_id)` index and duplicate-key handling make repeated insertion idempotent. `scheduled_arrival_time` and `delay_hours` are retained from Kafka; they are not recomputed.

## River's Spark → PostgreSQL input contract

Spark should call `upsert_county_hour(connection, record)` from `src/storage/postgres_loader.py` with one **processed** dictionary per `(run_code, scenario_code, county, event_hour)`. Required fields are shown below; measurements may be `None` when unavailable. Use UTC-aware datetimes or ISO timestamps for `event_hour` and `first_join_ready_at`. `first_join_ready_at` must be `None` unless `distinct_source_count == 3` (database check constraint).

```python
from datetime import datetime, timezone
from postgres_loader import get_connection, upsert_county_hour

record = {  # SYNTHETIC EXAMPLE
    "run_code": "demo-run-001",
    "scenario_code": "demo-normal",
    "speed_seconds_per_event_hour": 1.0,
    "delay_level": "none",  # none | moderate | severe
    "county": "Plumas",
    "event_hour": "2021-07-13T10:00:00Z",
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
conn = get_connection()
try:
    fact_id = upsert_county_hour(conn, record)
finally:
    conn.close()
```

**Field conversions:** Kafka `source` → MongoDB `source_name` (`nasa_firms` → `FIRMS`, `epa_pm25` → `EPA`, `noaa_weather` → `NOAA`). Kafka `replay_scenario` → Spark `scenario_code`; Spark supplies `run_code`, replay `speed_seconds_per_event_hour`, and `delay_level`. Kafka `payload` is preserved whole in MongoDB; if the optional PostgreSQL source-event audit path is used, a **selected subset** may be copied as `source_payload`, but it is **not** the complete original Kafka envelope. Spark extracts the source-specific measurements from Kafka `payload` into the flat county-hour measurement columns, and computes the source flags, `distinct_source_count`, join-ready time and latency/lag measures. No automatic conversion of Kafka envelopes to county-hour facts occurs in the storage loader.

`fact_county_hour` uses `ON CONFLICT (run_id, scenario_id, county_id, time_id)` to update the existing row rather than insert a duplicate; the first non-null `first_join_ready_at` is retained. `fact_source_event` also has a unique event key for optional audit metadata.

## Analysis and evaluation metrics

`sql/analysis_queries.sql` includes source counts, county results, scenario results, latency and freshness, missing source flags, duplicate checks, **final join completeness** (`distinct_source_count = 3`), and **on-time join rates** (complete joins with `freshness_lag_event_hours <= 1`, `<= 3`, `<= 6`, inclusive). Each rate uses **all county-hour facts in the scenario** as the denominator; a missing lag or incomplete join is not on time. A zero-row fact table legitimately produces empty grouped query results until Spark outputs are loaded.

## Execution evidence (real local output only)

After both databases are running, execute from the repository root in the **same Command Prompt** where you set `POSTGRES_PASSWORD`:

```bat
python src\storage\verify_storage.py > src\storage\storage_demo_output.txt 2>&1
type src\storage\storage_demo_output.txt
```

`verify_storage.py` connects to MongoDB and PostgreSQL, applies the idempotent schema, inserts three **clearly labeled synthetic Kafka-format documents**, repeats the insertion to confirm **zero new duplicates**, upserts one **synthetic county-hour fact twice**, checks the fact count, and runs all eight SQL queries. It prints real results or the real exception; **do not fabricate evidence**. It generates a new synthetic test ID each run, so repeat executions add more synthetic test records. Review the output before committing `storage_demo_output.txt`.

## Limitations

- Actual Spark integration and production replay performance metrics require River's processed output and end-to-end testing.
- Test records in `mongo_loader.py` and `verify_storage.py` are synthetic, not real agency measurements.
- SQL evaluation queries may return no rows until county-hour facts are loaded.

Contributor: Sanjana Phalke — MongoDB, PostgreSQL, warehouse schema and analytical queries.
