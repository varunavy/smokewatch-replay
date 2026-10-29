
# SmokeWatch Replay — Storage Component

## Purpose

This component stores and analyzes wildfire, air-quality,
and weather data for the SmokeWatch Replay project.

- MongoDB archives raw NASA FIRMS, EPA, and NOAA events.
- PostgreSQL stores source events and county-hour analytical
  results using a dimensional warehouse schema.
- SQL queries support county, source, replay-scenario,
  performance, and data-quality analysis.

## Project Files

- `src/storage/mongo_loader.py` — MongoDB event loader
- `src/storage/postgres_loader.py` — PostgreSQL loader
- `sql/schema.sql` — Warehouse schema and constraints
- `sql/analysis_queries.sql` — Analytical SQL queries

## Requirements

- Python 3.10+
- MongoDB
- PostgreSQL
- pymongo
- psycopg2-binary

Install Python dependencies:

```bash
python -m pip install pymongo psycopg2-binary
```

## MongoDB Setup

Start MongoDB locally on port 27017.

Default connection:

```text
mongodb://localhost:27017/
```

Database: `smokewatch`

Collection: `raw_events`

Run the sample insertion and retrieval test from the
repository root:

```bash
python src/storage/mongo_loader.py
```

Expected output includes:
- Successful MongoDB connection
- Number of new records inserted
- Total document count
- Counts grouped by source
- Retrieved Plumas County sample records

The included demonstration records are synthetic.

## PostgreSQL Setup

Create a PostgreSQL database named `smokewatch`.

Apply the warehouse schema from the repository root:

```bash
psql -U postgres -d smokewatch -f sql/schema.sql
```

Run the connection test:

```bash
python src/storage/postgres_loader.py
```

Run analytical queries:

```bash
psql -U postgres -d smokewatch -f sql/analysis_queries.sql
```

## Warehouse Tables

Dimensions:
- `dim_time`
- `dim_county`
- `dim_source`
- `dim_replay_scenario`
- `dim_run`

Fact tables:
- `fact_source_event`
- `fact_county_hour`

The county-hour fact grain is one row per run,
replay scenario, county, and UTC event hour.

## Duplicate Handling

MongoDB uses a unique index on:
`source_name`, `event_id`.

PostgreSQL uses unique constraints and
`ON CONFLICT` logic to prevent duplicate records.

The county-hour upsert preserves the first
join-ready timestamp.

## Verification Queries

MongoDB:

```python
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["smokewatch"]

print(db.raw_events.count_documents({}))
print(db.raw_events.find_one({}, {"_id": 0}))

client.close()
```

PostgreSQL:

```sql
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public'
ORDER BY table_name;

SELECT COUNT(*) FROM fact_source_event;
SELECT COUNT(*) FROM fact_county_hour;
```

## Limitations

- A running MongoDB and PostgreSQL instance is required.
- The included MongoDB demonstration uses synthetic records.
- PostgreSQL fact tables remain empty until data is loaded.
- Spark integration and micro-batch retry tracking require
  additional integration testing.
- Actual replay performance metrics must be collected
  from completed experiments.

## Contributor

Sanjana Phalke — MongoDB, PostgreSQL,
warehouse schema, and analytical queries.
