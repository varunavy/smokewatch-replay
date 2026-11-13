
# SmokeWatch Replay — Storage Component

## Purpose and Ownership

- **MongoDB (`smokewatch.raw_events`) is the authoritative raw-event archive.** It preserves the complete incoming agency/Kafka message under `raw_message`, including `run_code`, `source`, `event_id`, `county`, `event_hour`, `replay_scenario`, `scheduled_arrival_time`, `delay_hours`, and nested `payload`. Normalized searchable fields and an `ingestion_timestamp` are stored alongside the original message.

- **PostgreSQL is the analytical warehouse.** The `fact_county_hour` table stores Spark's processed county-hour measurements and join/timing metrics. Dimension tables describe runs, replay scenarios, counties, sources, and UTC hours. The optional `fact_source_event.source_payload JSONB` field stores limited provenance/audit information and is not a substitute for MongoDB's complete raw-event archive.

## Files and Setup

The following files implement the storage component:

- `src/storage/mongo_loader.py` — MongoDB raw Kafka/agency event storage
- `src/storage/postgres_loader.py` — PostgreSQL dimensional warehouse upsert functions
- `src/storage/load_demo.py` — Optional event-metadata demonstration
- `src/storage/verify_storage.py` — Local storage integration and idempotency verification
- `src/storage/requirements.txt` — Python dependencies
- `src/storage/storage_demo_output.txt` — Captured local execution evidence
- `sql/schema.sql` — PostgreSQL schema, constraints, and indexes
- `sql/analysis_queries.sql` — Eight analytical and validation queries

### Prerequisites

- Python 3.10 or later
- MongoDB running on `localhost:27017`
- PostgreSQL database named `smokewatch`
- PostgreSQL Docker port mapping: host port `5433` to container port `5432`

From the repository root in Windows Command Prompt:

```bat
python -m pip install -r src\storage\requirements.txt

set POSTGRES_HOST=127.0.0.1
set POSTGRES_PORT=5433
set POSTGRES_DB=smokewatch
set POSTGRES_USER=postgres
set POSTGRES_PASSWORD=YOUR_LOCAL_POSTGRES_PASSWORD
```

`POSTGRES_PASSWORD` is required and has no hardcoded default.

Set the actual password only in your local terminal or a secure secret manager. Never commit passwords, `.env` files containing credentials, or other secrets.

An existing PostgreSQL Docker volume may have been initialized with a different password.

### Apply PostgreSQL Schema

When using the `smokewatch-postgres` Docker container:

```bat
docker exec -i smokewatch-postgres psql -v ON_ERROR_STOP=1 -U postgres -d smokewatch < sql\schema.sql
```

Run the analytical queries:

```bat
docker exec -i smokewatch-postgres psql -v ON_ERROR_STOP=1 -U postgres -d smokewatch < sql\analysis_queries.sql
```

## Priya's Kafka → MongoDB Contract

The MongoDB loader accepts incoming Kafka messages through:

```python
insert_events(collection, [message])
```

Supported Kafka source values:

| Kafka `source` | MongoDB `source_name` |
|---|---|
| `nasa_firms` | `FIRMS` |
| `epa_pm25` | `EPA` |
| `noaa_weather` | `NOAA` |

Existing normalized `source_name` values (`FIRMS`, `EPA`, `NOAA`) are also supported.

### Required Fields

Each incoming event must contain:

- `run_code` — Non-empty string identifying the experiment run
- `event_id` — Source event identifier
- `source` or `source_name` — Supported agency/source
- `county` — County name
- `event_hour` — ISO-8601 timestamp or Python datetime

Additional Kafka fields are retained without filtering.

### Synthetic Kafka Message Example

```json
{
  "run_code": "demo_storage_01",
  "source": "nasa_firms",
  "event_id": "demo-kafka-firms-001",
  "county": "Plumas",
  "event_hour": "2021-07-13T10:00:00Z",
  "replay_scenario": "demo-normal",
  "scheduled_arrival_time": "2021-07-13T10:00:00Z",
  "delay_hours": 0,
  "payload": {
    "fire_detection_count": 3,
    "satellite": "VIIRS"
  }
}
```

The MongoDB document preserves the complete original message in `raw_message`.

The loader additionally stores:

- Normalized `source_name`
- String-normalized `event_id`
- Validated and trimmed `run_code`
- UTC-normalized `event_hour`
- Storage `ingestion_timestamp`

The original Kafka envelope remains preserved in `raw_message`, even when searchable fields are normalized.

The loader does not recompute `scheduled_arrival_time` or `delay_hours`.

### Run-Aware Deduplication

MongoDB uses a unique compound index on:

```text
(run_code, event_id)
```

Index name:

```text
uniq_run_code_event_id
```

The index applies to documents with string-valued `run_code` and `event_id`.

This design ensures:

1. The same event ID can be stored in different experiment runs.
2. Repeated insertion of the same event ID within one run does not create duplicates.
3. Each experiment run retains its own raw-event records.
4. The complete original Kafka message remains available for auditing.

For example:

| Run code | Event ID | Expected result |
|---|---|---|
| `run_A` | `event_001` | Inserted |
| `run_A` | `event_001` | Duplicate skipped |
| `run_B` | `event_001` | Inserted |
| `run_B` | `event_001` | Duplicate skipped |

This behavior supports multiple replay experiments using unchanged source event IDs.

## River's Spark → PostgreSQL Contract

Spark should call:

```python
upsert_county_hour(connection, record)
```

from `src/storage/postgres_loader.py`.

Each input record represents one processed county-hour observation identified by:

```text
(run_code, scenario_code, county, event_hour)
```

Spark supplies the processed measurements, source availability flags, join completeness, and timing metrics.

### Synthetic Processed Record Example

```python
from datetime import datetime, timezone
from postgres_loader import get_connection, upsert_county_hour

record = {
    "run_code": "demo-run-001",
    "scenario_code": "demo-normal",
    "speed_seconds_per_event_hour": 1.0,
    "delay_level": "none",
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
    "first_join_ready_at": datetime(
        2021, 7, 13, 11, tzinfo=timezone.utc
    ),
    "processing_latency_ms": 250.0,
    "freshness_lag_event_hours": 1.0,
}

conn = get_connection()

try:
    fact_id = upsert_county_hour(conn, record)
finally:
    conn.close()
```

The example uses synthetic measurements and is not an actual agency observation.

### Field Mapping

- Kafka `source` maps to normalized MongoDB `source_name`.
- Priya's Kafka envelope carries `run_code`. MongoDB preserves it, and Spark passes the run identifier to PostgreSQL.
- Kafka `replay_scenario` maps to the processed `scenario_code`.
- Spark provides the replay configuration fields `speed_seconds_per_event_hour` and `delay_level`.
- Kafka `payload` is preserved in full in MongoDB.
- Spark extracts source-specific measurements from `payload` into PostgreSQL county-hour measurement columns.
- Spark computes source availability flags, `distinct_source_count`, join-ready time, processing latency, and replay-relative freshness lag.

The optional PostgreSQL source-event audit path may copy selected source metadata into `source_payload`, but it does not replace the original Kafka envelope in MongoDB.

No automatic conversion of raw Kafka messages into processed county-hour facts occurs inside the storage loader.

### PostgreSQL Idempotency

The `fact_county_hour` table uses:

```sql
ON CONFLICT (run_id, scenario_id, county_id, time_id)
```

to update an existing fact instead of inserting a duplicate.

The first non-null `first_join_ready_at` is retained during updates.

`first_join_ready_at` must be null unless `distinct_source_count = 3`, according to the database constraint.

The optional `fact_source_event` table also includes a unique event key for audit metadata.

This design allows Spark microbatches to be retried without creating duplicate county-hour fact rows.

## Analysis and Evaluation Metrics

The file `sql/analysis_queries.sql` contains eight analytical and validation queries.

| Query | Purpose |
|---|---|
| 1 | Source event counts |
| 2 | County-level results |
| 3 | Replay scenario results |
| 4 | Processing latency and freshness |
| 5 | Missing source availability flags |
| 6 | Duplicate county-hour key checks |
| 7 | Final three-source join completeness |
| 8 | On-time join rates within 1, 3, and 6 event hours |

### Final Join Completeness

A county-hour record is considered fully joined when:

```text
distinct_source_count = 3
```

Final join completeness measures the percentage of county-hour facts with all three source types available.

### On-Time Join Rates

A join is on time when:

- All three sources are available.
- `freshness_lag_event_hours` is not null.
- Freshness lag is within the selected threshold.

The evaluated thresholds are:

```text
<= 1 event hour
<= 3 event hours
<= 6 event hours
```

The comparisons are inclusive.

Each on-time rate uses all county-hour fact rows within the scenario as its denominator.

Incomplete joins and records with missing freshness lag are not counted as on time.

A zero-row fact table can legitimately produce empty grouped query results until Spark output is loaded.

## Execution Evidence

Local storage verification is performed using:

```bat
python src\storage\verify_storage.py
```

To capture genuine execution output:

```bat
python src\storage\verify_storage.py > src\storage\storage_demo_output.txt 2>&1

type src\storage\storage_demo_output.txt
```

Run these commands from the repository root in the same Command Prompt where the PostgreSQL environment variables were configured.

The verification script uses synthetic data and performs the following checks.

### MongoDB Verification

1. Connects successfully to MongoDB.
2. Inserts three synthetic events into Run A.
3. Repeats the Run A insertion to verify duplicate prevention.
4. Inserts the same three event IDs into Run B.
5. Repeats the Run B insertion.
6. Verifies that six documents exist across both runs.
7. Confirms that the complete original Kafka messages are preserved.

### PostgreSQL Verification

1. Connects successfully to PostgreSQL.
2. Applies the database schema.
3. Verifies that all seven expected warehouse tables exist.
4. Upserts one synthetic county-hour fact twice.
5. Confirms that both upserts refer to the same fact.
6. Confirms that only one fact row exists for the test run.
7. Executes all eight analytical SQL queries.

The script prints actual results or exceptions. No execution output should be fabricated.

Each execution generates a new synthetic test identifier, so repeated executions may add additional synthetic test records to the local databases.

## MongoDB Index Migration

The previous MongoDB unique index used:

```text
(source_name, event_id)
```

This prevented the same event ID from being retained across different experiment runs for the same source.

The replacement index uses:

```text
(run_code, event_id)
```

with the name:

```text
uniq_run_code_event_id
```

### Local Migration Performed

The new index was created and verified successfully in the local MongoDB instance.

After verifying the replacement index, the obsolete index:

```text
source_name_1_event_id_1
```

was removed.

No MongoDB documents were deleted during this local index migration.

Other environments with the obsolete index must perform their own controlled migration after verifying the replacement index and coordinating with active writers.

## Local Validation Results

The following results were obtained from successful local synthetic-data tests.

### MongoDB Results

| Validation check | Actual result | Status |
|---|---:|---|
| Run A first insertion | 3 | PASS |
| Run A retry insertion | 0 | PASS |
| Run A stored documents | 3 | PASS |
| Run B first insertion with identical event IDs | 3 | PASS |
| Run B retry insertion | 0 | PASS |
| Run B stored documents | 3 | PASS |
| Total documents across both runs | 6 | PASS |
| Original Kafka messages preserved | True | PASS |

**MongoDB result: PASS**

The test confirms that duplicate retries are prevented within each run while identical source event IDs are retained across separate runs.

### PostgreSQL Results

| Validation check | Actual result | Status |
|---|---|---|
| Database connection | Successful | PASS |
| Schema creation | Successful | PASS |
| Expected warehouse tables | 7 | PASS |
| Two upserts of the same fact | Same fact ID | PASS |
| Fact rows after two upserts | 1 | PASS |
| Analytical SQL queries | All 8 successful | PASS |

**PostgreSQL result: PASS**

### Overall Storage Verification

**All local storage checks passed.**

The captured output is available in:

```text
src/storage/storage_demo_output.txt
```

These results demonstrate local MongoDB run-aware deduplication, PostgreSQL upsert idempotency, and successful execution of the analytical queries.

They do not establish full Kafka/Spark streaming throughput, end-to-end latency, or production replay performance.

## Limitations

- The local storage tests use synthetic events rather than the complete agency dataset.
- Full Kafka-to-MongoDB integration requires the complete replay event stream.
- Full Spark-to-PostgreSQL integration requires River's processed county-hour output.
- End-to-end throughput, freshness, latency, and join-completeness measurements require actual integrated streaming experiments.
- Analytical query results may include synthetic verification records until full experiment data is loaded and filtered by the relevant run identifiers.

## Contributor

**Sanjana Phalke**

Responsibilities:

- MongoDB raw-event storage
- Run-aware event deduplication
- PostgreSQL analytical warehouse
- Dimensional schema and upsert validation
- Analytical SQL queries
- Local storage integration testing and execution evidence