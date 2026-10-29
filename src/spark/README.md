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

Run scripts from the repository root.

## Command used to start the job

Produce the 12 sample records into Kafka (broker and topics must exist):

```bash
python src/kafka/create_topics.py
python src/kafka/replay_producer.py --input-dir data/samples --speed fast --delay none --scenario spark_smoke --no-pacing
```

Start the streaming job (Kafka is the default and the assignment path):

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  src/spark/streaming_job.py \
  --bootstrap localhost:9092 \
  --checkpoint data/interim/spark/checkpoint \
  --output data/interim/spark/county_hour \
  --run-code spark-local-001 \
  --scenario spark_smoke \
  --speed fast \
  --delay none
```

Equivalent Python entry point:

```bash
python src/spark/streaming_job.py \
  --source kafka \
  --bootstrap localhost:9092 \
  --checkpoint data/interim/spark/checkpoint \
  --output data/interim/spark/county_hour
```

**Local evidence without a broker** (`--source file` reads the same Kafka envelope):

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
7. `foreachBatch` sets `processed_at = current_timestamp()`,
   `first_join_ready_at` when join-ready, `freshness_lag_event_hours` as hours
   between `processed_at` and `event_hour_ts`, and `processing_latency_ms`
   from the Kafka timestamp (or `scheduled_arrival_time`) to `processed_at`.
   It prints the micro-batch and appends JSON. A commented stub shows where
   `upsert_county_hour` will be called.

Output mode is `update` so county-hour groups are emitted as additional
sources arrive, rather than waiting for the 13-hour watermark to close the
group.

## Execution evidence

Console output: `src/spark/spark_console_output.txt`.

Kafka on `localhost:9092` was not running, and the Docker daemon was not
available, so the captured run uses `--source file` on the same 12 sample
envelopes.

```bash
python src/spark/streaming_job.py --source file --input-dir data/samples --trigger once --reset-checkpoint --run-code spark-local-001 --scenario spark_smoke --speed fast --delay none
```

| Measure | Result |
|---|---|
| Runtime | Spark 3.5.1, Java 17, file source, `availableNow` |
| Input records | **12** (4 FIRMS + 4 EPA + 4 NOAA) |
| County-hour groups | **4**, all `is_join_ready` with `distinct_source_count = 3` |
| Groups | Butte 10:00, Shasta 11:00, Butte 12:00, Tehama 13:00 |
| Rows dropped by watermark | **0** |
| Watermark after the first batch | `2025-01-08T00:00:00.000Z` (latest event hour 13:00 minus 13 hours) |
| JSON sink | `data/interim/spark/county_hour` |

## Remaining limitations

- Live Kafka (`--source kafka`) has not been run: no broker on
  `localhost:9092`, and Docker was not running to start one.
- `foreachBatch` does not call `upsert_county_hour`. Console and JSON are the
  current sinks; a commented stub marks the integration point.
- Sample `data/samples/` files are synthetic, not the 18,000-event 2021
  replay set. Sample NOAA rows have no humidity, so `humidity_pct` is null.
- `freshness_lag_event_hours` is wall-clock hours from `processed_at` back to
  historical `event_hour`, so it is large on the 2025 sample timestamps.
- File-source `kafka_timestamp` is assigned at read time, so
  `processing_latency_ms` is local processing duration, not broker lag.
- Benign local warnings: missing native Hadoop library; Adaptive Query
  Execution disabled for streaming.
