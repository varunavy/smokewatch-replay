# Report sections + slide bullets (River / Spark)

Copy into the shared report and slide deck; keep wording consistent with the
schematic in `flow-schematic.md`.

## Report — Spark Structured Streaming

SmokeWatch Replay uses Apache Spark Structured Streaming to turn three
asynchronous agency streams into county-hour analytical facts. Each Kafka
topic is read as its own stream, parsed against a shared envelope schema, and
unioned while retaining the `source` label. A stateful aggregation groups by
`(county, event_hour, replay_scenario)`, collects distinct sources, and marks
a row **join-ready** only when FIRMS, EPA PM2.5, and NOAA weather are all
present (`distinct_source_count = 3`). Micro-batches are written through
`foreachBatch` to JSON (evidence) and, when enabled, to PostgreSQL via
`upsert_county_hour`.

## Report — Event time and watermark

Event time is the UTC `event_hour` carried in the Kafka envelope. The job
applies a **13-hour watermark** on that field so late arrivals from the
severe-delay profile (4–12 event-hours) remain eligible for the join state.
`freshness_lag_event_hours` is defined as replay-relative delay
`(scheduled_arrival_time − event_hour) / 3600`, not wall-clock age versus
historical fire timestamps. That keeps on-time join metrics meaningful across
`none` / `moderate` / `severe` delay conditions.

## Report — Technical difficulty and lesson learned

The hardest integration risk was not the SQL of the aggregation but **time
semantics under replay**: wall-clock freshness made every historical hour look
stale, and driver-local timezone conversion could shift `event_hour` when
upserting to Postgres. Fixing both (replay-relative lag + UTC ISO export)
made Kafka smoke metrics and warehouse facts agree. Lesson: treat replay
clocks as first-class data products—document which clock each metric uses
before running multi-hour experiment packs.

## Slide — Spark component (talk track ~60s)

- Three Kafka topics → union → 13 h event-time watermark  
- Stateful county-hour join; ready only with all three sources  
- Metrics: processing latency, replay-relative freshness, join-ready rate  
- Sink: JSON evidence + optional Postgres upsert  

## Slide — System flow (talk track ~45s)

- Priya produces delayed envelopes into Kafka  
- River joins in Spark; Sanjana stores raw (Mongo) + facts (Postgres)  
- Varuna validates against batch keys and packages the submission  

## Slide — Short code walkthrough

1. `read_kafka_topic` / `parse_kafka_envelope`  
2. `aggregate_county_hour` (`withWatermark` + `collect_set(source)`)  
3. `write_batch` (freshness + optional `upsert_county_hour`)  

## Explicit non-claims

Do **not** claim LSH, differential privacy, or Bloom-filter ownership in the
Spark section unless River and Sanjana attach separate tested evidence.
