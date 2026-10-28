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

If the standardized 2021 files are CSV, convert one file per source with `convert_to_jsonl.py`, telling it which columns hold the county and time. Example (change the column names to match the real files):

```bash
python src/kafka/convert_to_jsonl.py --source nasa_firms --input <firms_2021.csv> --output data/replay_input/firms.jsonl --county-col <county_column> --time-col <timestamp_column> --event-id-col <id_column>
```

Repeat with `--source epa_pm25` and `--source noaa_weather`. Use `--date-col` plus `--hour-col` if date and hour are separate columns. If a file has no event ID column, leave out `--event-id-col` and IDs are built as `<source>-<row number>`.
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

Full real-input dry run (18,000 events, 6,000 per source): **not yet run on the real 2021 files** — to be added after conversion (see the converter command above).

### Live Kafka test

**Not yet run.** Topic-creation output and consumer output are remaining work. To produce them:

```bash
python src/kafka/create_topics.py
python src/kafka/replay_producer.py --input-dir data/samples --speed fast --delay moderate --scenario smoke_test
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic firms.events --from-beginning --property print.key=true
```

## Remaining limitations

- Live Kafka execution (topic creation and consumer output) has not been done.
- The converter has not been run on the real standardized 2021 files; column names must be supplied by the user.
- Files in `data/samples/` are synthetic examples, not real data.
