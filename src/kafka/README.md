# Kafka producer and replay control

Replays standardized source-event files into Kafka at a controlled speed and with simulated late arrival.

## Required setup

1. Python 3.9+ and a running Kafka broker (default `localhost:9092`).
2. Install the client: `pip install -r src/kafka/requirements.txt` (installs `kafka-python`).
3. Create the topics once: `python src/kafka/create_topics.py`
4. Run scripts from the repo root as `python src/kafka/<script>.py`. Do not run them from inside `src/` — the folder name `kafka` would shadow the `kafka-python` package.

## Topic names

| Source | `source` value | Topic |
|---|---|---|
| NASA FIRMS | `nasa_firms` | `firms.events` |
| EPA PM2.5 | `epa_pm25` | `epa.pm25.events` |
| NOAA weather | `noaa_weather` | `noaa.weather.events` |

Each topic has 3 partitions, replication factor 1. Edit `config.py` to change topic names, speeds, or delay ranges.

## Input format

A folder of JSON Lines (`*.jsonl`) files, one event per line. Required fields:
`event_id`, `source`, `county`, `event_hour` (ISO 8601 UTC). All other fields are carried through in `payload`.
Invalid lines are skipped and reported on stderr. Sample files are in `data/samples/`.

## Output format

Kafka message **key**: the county name (UTF-8), so all events for a county land in the same partition in order.
Message **value** (JSON):

```json
{"source": "nasa_firms", "county": "Butte", "event_hour": "2025-01-08T10:00:00Z",
 "event_id": "firms-0001", "replay_scenario": "smoke_test",
 "scheduled_arrival_time": "2025-01-08T14:40:56Z",
 "payload": {"latitude": 39.72, "longitude": -121.61, "frp": 18.4}}
```

`scheduled_arrival_time` = `event_hour` + a random delay from the chosen delay profile (seeded, so reproducible).

## Replay settings

| `--speed` | Simulated event-hours per real second |
|---|---|
| `slow` | 1 |
| `medium` | 10 |
| `fast` | 100 |

| `--delay` | Added delay (hours) |
|---|---|
| `none` | 0 |
| `moderate` | 1 to 6 |
| `severe` | 12 to 48 |

Messages are sent in order of scheduled arrival time, so delayed records arrive out of event order.

## Exact command

```bash
python src/kafka/replay_producer.py --input-dir data/samples --speed fast --delay moderate --scenario smoke_test
```

Add `--dry-run` to print messages without a broker, `--limit N` to send only N records, `--bootstrap host:port` for a non-default broker.

## Execution evidence

Smoke test (dry-run mode, sample data, `--speed fast --delay moderate`): **12 records** (4 per source), 0 skipped. Full console output: `src/kafka/smoke_test_output.txt`.
Sample line:

```
[dry-run] topic=firms.events key=Butte value={"source": "nasa_firms", "county": "Butte", "event_hour": "2025-01-08T10:00:00Z", "event_id": "firms-0001", "replay_scenario": "smoke_test", "scheduled_arrival_time": "2025-01-08T14:40:56Z", "payload": {...}}
```

## Remaining limitations

- **Not yet tested against a live broker.** The development sandbox had no Kafka broker and could not install `kafka-python`, so only `--dry-run` was executed. Before merging, run `create_topics.py` and the producer against a real broker and paste consumer output here, e.g.:
  `kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic firms.events --from-beginning --property print.key=true`
- Sample data is synthetic. Replace with the real standardized files once available and confirm field names match.
- Delays are random within a range, not derived from real source latency.
