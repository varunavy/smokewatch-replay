# SmokeWatch Replay Final Validation Contract
## Purpose
This document defines how the final streaming output will be checked against the completed Python batch reference. It separates raw-event validation from county-hour analytical validation so that source delivery, measurement availability, and pipeline correctness are not treated as the same result.
## Reference Data
The completed batch workflow contains:
- 6,000 NASA FIRMS county-hour rows
- 6,000 EPA PM2.5 county-hour rows
- 6,000 NOAA weather county-hour rows
- 18,000 source events in total
- 6,000 integrated county-hour rows
- 1,200 UTC hours across five counties
- No duplicate county-hour keys
- No invalid event timestamps
The five counties are Butte, Lassen, Plumas, Shasta, and Tehama.
## Validation Layer 1: Raw Source Events
The first layer will confirm that Kafka and MongoDB retain the complete source-event input.
The following checks must pass:
1. Kafka receives 18,000 valid source events.
2. Each source topic receives 6,000 events.
3. The 18,000 event IDs are unique before deliberate duplicate injection.
4. Every event contains a source, county, event hour, event ID, replay scenario, scheduled arrival time, and source payload.
5. MongoDB retains the original event envelope and source-specific payload.
6. Event counts and identifiers in MongoDB match the Kafka producer evidence.
7. Missing measurements remain null and are not converted to zero.
The Bloom-filter experiment is evaluated separately and must not change the 18,000-event reference input or the main pipeline output.
## Validation Layer 2: County-Hour Analytical Results
The second layer will compare PostgreSQL county-hour output with the integrated batch reference.
The expected primary key is:
- run
- replay scenario
- county
- UTC event hour
Each completed run should contain 6,000 unique county-hour keys after all three sources have been processed.
## Batch-to-Streaming Field Mapping
| Batch reference field | Streaming or PostgreSQL field | Validation rule |
|---|---|---|
| `event_hour_utc` | `event_hour` | Exact UTC timestamp match |
| `county` | `county` or `county_name` | Exact text match |
| `fire_detection_count` | `fire_detection_count` | Exact integer match |
| `pm25_ug_m3` | `pm25_ug_m3` | Numeric match, preserving nulls |
| `temperature_c` | `temperature_c` | Numeric match, preserving nulls |
| `relative_humidity_percent` | `humidity_pct` | Numeric match, preserving nulls |
| `wind_speed_m_s` | `wind_speed_m_s` | Numeric match, preserving nulls |
| `wind_direction_degrees` | `wind_direction_deg` | Numeric match, preserving nulls |
Numeric comparisons will use a small floating-point tolerance where necessary. Missing values must match as missing values.
## Source Presence and Measurement Availability
Source presence and measurement availability have different meanings.
The Spark fields `firms_available`, `epa_available`, and `noaa_available` currently indicate that a source record reached the county-hour aggregation. They do not prove that the agency reported a usable measurement.
The batch fields `pm25_available` and `weather_available` indicate whether a PM2.5 or weather measurement is present. These fields must not be compared directly with Spark source-presence flags.
Before final validation, the team must use one of these approaches:
1. Add separate PM2.5 and weather measurement-availability fields to the analytical output; or
2. Validate measurement availability from the preserved source payloads in MongoDB.
The selected approach must be documented in the final report.
## Correctness Checks
For every measured run, validation will check:
- Exactly 6,000 county-hour keys after final processing
- No duplicate run-scenario-county-hour keys
- All five expected counties
- Exactly 1,200 event hours for each county
- Three distinct source records for every completed county-hour
- Exact agreement for integer fields
- Tolerance-based agreement for continuous measurements
- Matching null and availability behavior
- No unexplained record loss
- No values from one replay scenario appearing in another scenario
`first_join_ready_at`, processing latency, and freshness lag do not have batch equivalents. They will be validated through timing rules and experiment logs rather than by matching the batch table.
## Timing Validation
Processing latency will measure the wall-clock interval between scheduled arrival and completed processing.
Freshness lag must be calculated relative to the replay schedule. It must not be calculated by subtracting the 2021 historical event timestamp directly from the 2026 processing timestamp.
A county-hour becomes join-ready when three distinct sources have arrived. Duplicate records from one source must not cause the source count to reach three.
The on-time join rate will be reported at inclusive thresholds of one, three, and six simulated event-hours.
## Current Dependencies
Final validation depends on:
- Priya’s Kafka PR being updated with the latest `main`
- River normalizing all NOAA payload aliases
- River replacing historical-date freshness with replay-relative lag
- River and Sanjana connecting `foreachBatch` to the idempotent PostgreSQL upsert
- The team deciding how measurement-availability flags will be retained
- Completion of the live integrated experiment
Smoke tests, dry runs, and the earlier Python simulation will remain labeled separately from final Kafka, Spark, and PostgreSQL measurements.
