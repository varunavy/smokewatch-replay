# Spark Structured Streaming county-hour job

Builds join-ready county-hour records from the three Kafka source streams.

## Spark version and dependencies

| Item | Value |
|---|---|
| Spark / PySpark | 3.5.1 |
| Scala binary | 2.12 |
| Java | 17 |
| Kafka connector | `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1` |
| Python | 3.8–3.11 |

```bash
python3.10 -m pip install -r src/spark/requirements.txt
```

The Kafka connector is a Spark package, not a pip package. `streaming_job.py`
adds it when `--source kafka` is used. File-source runs do not need a broker.

Run scripts from the repository root. Set `JAVA_HOME` to a JDK 17 install.

## Bring up Kafka (Priya PR #5 Compose)

```bash
docker compose -f docker/docker-compose.yml up -d
python src/kafka/create_topics.py
```

Broker: `localhost:9092`. Topics: `firms.events`, `epa.pm25.events`,
`noaa.weather.events`.

## Command used to start the job

Produce sample records into Kafka:

```bash
python src/kafka/replay_producer.py \
  --input-dir data/samples --speed fast --delay none \
  --scenario spark_kafka_smoke --no-pacing
```

Start the streaming job (Kafka is the default and the assignment path):

```bash
python src/spark/streaming_job.py \
  --source kafka \
  --bootstrap localhost:9092 \
  --checkpoint data/interim/spark/checkpoint \
  --output data/interim/spark/county_hour \
  --run-code kafka-smoke-001 \
  --scenario spark_kafka_smoke \
  --speed fast \
  --delay none \
  --reset-checkpoint
```

Optional warehouse sink (Sanjana’s loader; requires `POSTGRES_*` env vars,
default host port **5433** per `src/storage/README.md`):

```bash
export POSTGRES_HOST=127.0.0.1 POSTGRES_PORT=5433
export POSTGRES_DB=smokewatch POSTGRES_USER=postgres
export POSTGRES_PASSWORD=...   # required; never commit
python src/spark/streaming_job.py --source file --input-dir data/samples \
  --trigger once --reset-checkpoint --write-postgres
```

**Local regression without a broker** (`--source file` uses the same Kafka envelope):

```bash
python src/spark/streaming_job.py \
  --source file \
  --input-dir data/samples \
  --trigger once \
  --reset-checkpoint \
  --run-code spark-local-001 \
  --scenario spark_smoke \
  --speed fast \
  --delay none
```

Checkpoint: `data/interim/spark/checkpoint` (`checkpointLocation`).
JSON output: `data/interim/spark/county_hour`. Both are gitignored.

## Input and output fields

### Input (Kafka envelope)

| Field | Type | Notes |
|---|---|---|
| `source` | string | `nasa_firms`, `epa_pm25`, or `noaa_weather` (retained) |
| `county` | string | Kafka message key; grouping key |
| `event_hour` | ISO-8601 UTC | Event time; grouping key |
| `event_id` | string | Pass-through identity |
| `replay_scenario` | string | Grouping key; output `scenario_code` |
| `run_code` | string | Optional (PR #23). Unique per execution; preferred for warehouse `run_code` when present; otherwise CLI `--run-code` |
| `scheduled_arrival_time` | ISO-8601 UTC | Replay arrival, including delay |
| `delay_hours` | double | Event-hour delay applied by the producer |
| `payload` | object | Source measurements (aliases accepted) |

Topics: `firms.events`, `epa.pm25.events`, `noaa.weather.events`.

Payload aliases (`config.PAYLOAD_FIELD_ALIASES`):

| Warehouse column | Production key | Sample alias |
|---|---|---|
| `fire_detection_count` | `fire_detection_count` | `frp` present counts as 1 |
| `pm25_ug_m3` | `pm25_ug_m3` | `pm25_ugm3` |
| `temperature_c` | `temperature_c` | `temp_c` |
| `humidity_pct` | `relative_humidity_percent` | `humidity_pct` / `relative_humidity_pct` |
| `wind_speed_m_s` | `wind_speed_m_s` | `wind_speed_ms` |
| `wind_direction_deg` | `wind_direction_degrees` | `wind_dir_deg` / `wind_direction_deg` |

### Output (join-ready county-hour row)

Matches `src/storage/postgres_loader.py` `upsert_county_hour`:

`run_code`, `scenario_code`, `speed_seconds_per_event_hour`, `delay_level`,
`county`, `event_hour`, `distinct_source_count`, `sources`, `input_event_count`,
`fire_detection_count`, `pm25_ug_m3`, `temperature_c`, `humidity_pct`,
`wind_speed_m_s`, `wind_direction_deg`, `firms_available`, `epa_available`,
`noaa_available`, `is_join_ready`, `first_join_ready_at`, `processed_at`,
`processing_latency_ms`, `freshness_lag_event_hours`.

`is_join_ready` / `first_join_ready_at` are set only when
`distinct_source_count = 3`. The warehouse `COALESCE` keeps the first
non-null `first_join_ready_at` on upsert.

## Watermark and aggregation logic

1. Read each of the three Kafka topics as its own stream (`subscribe`,
   `startingOffsets=earliest`), parse the value with `from_json` and the
   envelope schema, and **retain `source`**.
2. `unionByName` the three streams.
3. Event time is `event_hour_ts` (parsed UTC `event_hour`).
4. **Watermark = 13 hours** on `event_hour_ts`. Severe delay is 4–12
   event-hours, so a 12-hour watermark would drop the 12-hour boundary; 13
   hours keeps the full delay window plus one hour of slack.
5. Stateful aggregation: `groupBy(county, event_hour_ts, replay_scenario)`
   after `withWatermark`. `collect_set(source)` becomes `distinct_source_count`
   and the `firms_available` / `epa_available` / `noaa_available` flags.
   Measurements use `max` (one event per source per county-hour in the batch
   grid). `input_event_count` is `count(*)`.
6. `is_join_ready` when `distinct_source_count == 3`.
7. `foreachBatch` sets:
   - `processed_at = current_timestamp()`
   - `first_join_ready_at = scheduled_arrival_time` when join-ready
   - **`freshness_lag_event_hours` = replay-relative**
     `(scheduled_arrival_time - event_hour) / 3600`
   - `processing_latency_ms` from Kafka timestamp (or scheduled arrival)
     to `processed_at`
   - JSON append sink; optional `--write-postgres` → `upsert_county_hour`

Output mode is `update` so county-hour groups are emitted as additional
sources arrive, rather than waiting for the 13-hour watermark to close the
group.

## Execution evidence

### Live Kafka smoke (2026-11-10)

Broker: `smokewatch-kafka` via `docker/docker-compose.yml` (PR #5).  
Producer: 12 sample envelopes, `fast` / `none` / `spark_kafka_smoke`.  
Artifacts: `src/spark/evidence/kafka_smoke_*`, `src/spark/evidence/part-*.json`.

| Measure | Result |
|---|---|
| Runtime | Spark 3.5.1, Java 17, `--source kafka` |
| Input records | **12** (4 FIRMS + 4 EPA + 4 NOAA) |
| County-hour groups | **4**, all `is_join_ready` with `distinct_source_count = 3` |
| Groups | Butte 10:00, Shasta 11:00, Butte 12:00, Tehama 13:00 |
| Freshness lag | **0.0** event-hours (delay=none, replay-relative) |
| JSON sink | `data/interim/spark/county_hour` |

### Delay-profile freshness check

See `src/spark/evidence/delay_profiles/SUMMARY.md`. Moderate max lag ~1.72 h;
severe max lag ~6.89 h; `droppedByWatermark=0` under the 13 h watermark.

### PostgreSQL upsert

See `src/spark/evidence/postgres_wire_db.txt` and
`src/spark/evidence/postgres_pair_session.md`. Two passes → **4** fact rows,
same `fact_id`s (idempotent).

### File-source regression

Still supported (`--source file`). Useful when Docker/Kafka is unavailable.

## Remaining limitations

- Full 18,000-event / 27-run experiment pack is scheduled with Priya after this
  smoke; metrics from that window will extend the evidence folder.
- Sample `data/samples/` files are synthetic, not the 2021 replay set. Sample
  NOAA rows have no humidity, so `humidity_pct` is null.
- File-source `kafka_timestamp` is assigned at read time, so
  `processing_latency_ms` is local processing duration, not broker lag.
- Benign local warnings: missing native Hadoop library; Adaptive Query
  Execution disabled for streaming.
- LSH / privacy claims are **out of scope** unless River + Sanjana produce
  separate evidence.
