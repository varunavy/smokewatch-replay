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

Selection uses SHA-256 of `"<event_id>:select"`: the first 8 bytes are scaled to a number in [0, 1), and the event is delayed when that number is below the profile's fraction (0.10 or 0.25). The delay amount is read the same way from `"<event_id>:amount"` and scaled into the profile's range. The rule depends only on `event_id` and the delay profile, so repeated runs always select the same events with the same delays.

**The scenario label and the replay speed are not part of the hash.** Scenario labels such as `slow__moderate`, `medium__moderate` and `fast__moderate` therefore delay exactly the same events by exactly the same amounts, and replay speed is the only changed factor. `--scenario` is still written into every Kafka message as `replay_scenario` to identify the run. Messages are sent in order of scheduled arrival time, so delayed records arrive out of event order.

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

**Tests** for the delay rules (needs `pip install pytest`):

```bash
python3 -m pytest src/kafka/test_delay_rules.py -q
```

## Execution evidence

### Dry run (no Kafka involved)

Sample data, `--speed fast --delay severe --scenario smoke_test --dry-run --no-pacing`: **12 records** (4 per source), 0 skipped, 4 of 12 events delayed. Console output: `src/kafka/dry_run_output.txt`.

Delay-rule tests (`test_delay_rules.py`, **9 tests passing**) on 18,000 generated events (6,000 per source): moderate delays about 10% within 1 to 3 hours; severe about 25% within 4 to 12 hours; repeated runs give identical output; and slow, medium and fast scenario labels (`slow__moderate`, `medium__moderate`, `fast__moderate`, and the same for severe) give identical `delay_hours` and scheduled arrival time for every event.

### Full real-input dry run (no Kafka involved)

Real 2021 source-event files (July 13 to August 31, 2021; five counties), converted as above, `--scenario baseline --speed fast --dry-run --no-pacing`. Console output: `src/kafka/full_input_dry_run_output.txt`.

- Input check: **18,000 valid events, 0 skipped, 6,000 per source** (`--expect-per-source 6000`: OK). All 18,000 event IDs are unique.

| `--delay` | Events delayed | Delay range (event-hours) | Repeated run identical |
|---|---|---|---|
| `none` | 0 of 18,000 (0%) | n/a | yes |
| `moderate` | 1,846 of 18,000 (10.26%) | 1.00 to 3.00 | yes |
| `severe` | 4,549 of 18,000 (25.27%) | 4.00 to 12.00 | yes |

All events that were not selected had zero delay in every run.

**Speed independence on the real data:** with scenarios `slow__moderate`, `medium__moderate` and `fast__moderate` (and `slow__severe`, `medium__severe`, `fast__severe`), all three speeds delay the same events (moderate 1,846; severe 4,549), and `delay_hours` and `scheduled_arrival_time` are identical for every one of the 18,000 events. Output is in `src/kafka/full_input_dry_run_output.txt`.

### Live Kafka test

**Run on Nov 10, 2026** (logs are in `evidence/kafka/`). The original smoke-test commands were:

```bash
python src/kafka/create_topics.py
python src/kafka/replay_producer.py --input-dir data/samples --speed fast --delay moderate --scenario smoke_test
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic firms.events --from-beginning --property print.key=true
```

## Remaining limitations

- Live Kafka execution was completed on Nov 10, 2026 (see `evidence/kafka/`). The nine-condition runs with Spark are still to be done.
- Files in `data/samples/` are synthetic examples, not real data.

## Live Kafka run (Nov 10, 2026)

- Broker: `docker/docker-compose.yml` (single broker, Apache Kafka 3.8.0 in Docker)
- Topics created with `python src/kafka/create_topics.py` (`evidence/kafka/topic_creation.log`)
- Full replay of `data/replay_input`: 18,000 events sent, 0 invalid, 6,000 in each topic (`evidence/kafka/producer_full_replay.log`, `evidence/kafka/topic_counts_after_replay.log`)
- Experiment runner: `src/kafka/run_experiments.sh`, one condition tested (`evidence/kafka/test_runs/`)
- Delay and speed checks: `evidence/kafka/determinism_dry_run.log` and `evidence/kafka/live_checks/`

## Scope of run_experiments.sh

- The script records producer-side Kafka runs only: duration, producer exit code and per-topic message counts for each condition.
- Before every run it deletes and recreates the three Kafka topics. Do not run it while a Spark query is active, because the topics would disappear under the running query.
- It stops with an error if topic creation, the empty-topic check or the replay producer fails, so a failed run cannot produce misleading zero counts.
- The integrated Kafka-to-Spark experiment needs coordinated Spark restarts: stop Spark before each topic reset and start it again afterwards with a fresh, isolated checkpoint directory for every run, so old offsets or state cannot affect the next condition.
