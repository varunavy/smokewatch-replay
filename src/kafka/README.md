# Kafka producer and replay control

Replays standardized source-event files into Kafka at a controlled speed and with simulated late arrival.

## Required setup

1. Python 3.9+ and, for live runs only, a Kafka broker (default `localhost:9092`).
2. Install the client: `pip install -r src/kafka/requirements.txt` (installs `kafka-python`).
3. Create the topics once (live runs only): `python src/kafka/create_topics.py`
4. Run scripts from the repo root as `python src/kafka/<script>.py`. Do not run them from inside `src/` — the folder name `kafka` would shadow the `kafka-python` package.

Dry runs and input validation need no broker and no `kafka-python`.

## Topic names

| Source | `source` value | Topic |
|---|---|---|
| NASA FIRMS | `nasa_firms` | `firms.events` |
| EPA PM2.5 | `epa_pm25` | `epa.pm25.events` |
| NOAA weather | `noaa_weather` | `noaa.weather.events` |

Each topic has 3 partitions, replication factor 1. Edit `config.py` to change topic names, speeds, or delay rules.

## Input format

A folder of JSON Lines (`*.jsonl`) files, one event per line. Required fields:
`event_id`, `source`, `county`, `event_hour` (ISO 8601 UTC). All other fields are carried through in `payload`.
Invalid lines are skipped and reported on stderr. Small sample files are in `data/samples/`.

### Converting the standardized files (CSV to JSONL)

The standardized 2021 source-event files (`smokewatch_replay_source_events_2021.zip`) are three CSVs with 6,000 rows each. Unzip them into a folder, then convert each to JSONL with `convert_to_jsonl.py`. Column mapping used: `source_name` is the source (checked against `--source`), `event_hour_utc` becomes `event_hour`; `event_id` and `county` already match. All other columns are kept in `payload` (numbers stay numbers, empty cells become null).

```bash
python src/kafka/convert_to_jsonl.py --source nasa_firms --input <folder>/nasa_firms_events_full_2021-07-13_2021-08-31.csv --output data/replay_input/firms.jsonl --county-col county --time-col event_hour_utc --event-id-col event_id --source-col source_name
python src/kafka/convert_to_jsonl.py --source epa_pm25 --input <folder>/epa_pm25_events_full_2021-07-13_2021-08-31.csv --output data/replay_input/epa_pm25.jsonl --county-col county --time-col event_hour_utc --event-id-col event_id --source-col source_name
python src/kafka/convert_to_jsonl.py --source noaa_weather --input <folder>/noaa_weather_events_full_2021-07-13_2021-08-31.csv --output data/replay_input/noaa_weather.jsonl --county-col county --time-col event_hour_utc --event-id-col event_id --source-col source_name
```

`<folder>` is where the zip was unzipped. For other files, use `--date-col` plus `--hour-col` if date and hour are separate columns; if there is no event ID column, leave out `--event-id-col` and IDs are built as `<source>-<row number>`. `data/replay_input/` holds generated data and is not committed.
Check the result (expects 18,000 valid events, 6,000 per source):

```bash
python src/kafka/replay_producer.py --input-dir data/replay_input --validate-only --expect-per-source 6000
```

## Output format

Kafka message **key**: the county name (UTF-8), so all events for a county land in the same partition in order.
Message **value** (JSON):

```json
{"source": "nasa_firms", "county": "Butte", "event_hour": "2025-01-08T10:00:00Z",
 "event_id": "firms-0001", "replay_scenario": "smoke_test",
 "scheduled_arrival_time": "2025-01-08T10:00:00Z", "delay_hours": 0.0,
 "payload": {"latitude": 39.72, "longitude": -121.61, "frp": 18.4}}
```

`delay_hours` is added so a delay can be checked directly; `scheduled_arrival_time` = `event_hour` + `delay_hours`.

## Replay settings

| `--speed` | Simulated event-hours per real second |
|---|---|
| `slow` | 1 |
| `medium` | 10 |
| `fast` | 100 |

### Delay rules (deterministic)

| `--delay` | Events delayed | Delay for delayed events | All other events |
|---|---|---|---|
| `none` | 0% | none | zero delay |
| `moderate` | about 10% | 1 to 3 event-hours | zero delay |
| `severe` | about 25% | 4 to 12 event-hours | zero delay |

Selection uses SHA-256 of `"<scenario>:<event_id>:select"`: the first 8 bytes are scaled to a number in [0, 1), and the event is delayed when that number is below the profile's fraction (0.10 or 0.25). The delay amount is read the same way from `"<scenario>:<event_id>:amount"` and scaled into the profile's range. The same `event_id` and `--scenario` therefore always select the same events and give the same delays in repeated runs; a different scenario selects a different set. Messages are sent in order of scheduled arrival time, so delayed records arrive out of event order.

## Commands

**Dry run** (prints messages, nothing is sent to Kafka, no broker needed):

```bash
python src/kafka/replay_producer.py --input-dir data/samples --speed fast --delay moderate --scenario smoke_test --dry-run
```

Add `--no-pacing` to skip the replay sleeps, `--quiet` to print only the summary and the first 5 messages. Full-input dry run example:

```bash
python src/kafka/replay_producer.py --input-dir data/replay_input --speed fast --delay moderate --scenario baseline --dry-run --no-pacing --quiet --expect-per-source 6000
```

**Live run** (sends to Kafka; topics must exist):

```bash
python src/kafka/replay_producer.py --input-dir data/samples --speed fast --delay moderate --scenario smoke_test
```

Other options: `--limit N` to send only N records, `--bootstrap host:port` for a non-default broker.

**Tests** for the delay rules: `python src/kafka/test_delay_rules.py`

## Execution evidence

### Dry run (no Kafka involved)

Sample data, `--speed fast --delay severe --scenario smoke_test --dry-run --no-pacing`: **12 records** (4 per source), 0 skipped, 3 of 12 events delayed. Console output: `src/kafka/dry_run_output.txt`.

Delay-rule tests (`test_delay_rules.py`, 6 tests passing) on 18,000 generated events (6,000 per source): moderate delayed about 10% (9.7% in one run) within 1 to 3 hours; severe about 25% (24.4%) within 4 to 12 hours; repeated runs gave identical output.

### Full real-input dry run (no Kafka involved)

Real 2021 source-event files (July 13 to August 31, 2021; five counties), converted as above, `--scenario baseline --speed fast --dry-run --no-pacing`. Console output: `src/kafka/full_input_dry_run_output.txt`.

- Input check: **18,000 valid events, 0 skipped, 6,000 per source** (`--expect-per-source 6000`: OK). All 18,000 event IDs are unique.

| `--delay` | Events delayed | Delay range (event-hours) | Repeated run identical |
|---|---|---|---|
| `none` | 0 of 18,000 (0%) | n/a | yes |
| `moderate` | 1,750 of 18,000 (9.72%) | 1.00 to 3.00 | yes |
| `severe` | 4,552 of 18,000 (25.29%) | 4.01 to 12.00 | yes |

All events that were not selected had zero delay in every run.

### Live Kafka test

**Not yet run.** Topic-creation output and consumer output are remaining work. To produce them:

```bash
python src/kafka/create_topics.py
python src/kafka/replay_producer.py --input-dir data/samples --speed fast --delay moderate --scenario smoke_test
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic firms.events --from-beginning --property print.key=true
```

## Remaining limitations

- Live Kafka execution (topic creation and consumer output) has not been done.
- Files in `data/samples/` are synthetic examples, not real data.
