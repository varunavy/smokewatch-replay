# Kafka code walkthrough (about 2.5 minutes)

## 1. config.py (30 s)
- Maps each source to its topic: nasa_firms to firms.events, epa_pm25 to epa.pm25.events, noaa_weather to noaa.weather.events.
- 3 partitions and replication factor 1 on a single Docker broker.
- Replay speeds: slow 1, medium 10, fast 100 simulated event-hours per real second.
- Delay profiles: none, moderate (10% of events, 1 to 3 hours late), severe (25%, 4 to 12 hours late).
- The delay rule uses a SHA-256 hash of the event ID only, so the same events are late at every speed.

## 2. replay_producer.py (60 s)
- Reads the JSONL files and checks every record has event_id, source, county and event_hour.
- Gives each event a delay from the delay profile, then sets scheduled arrival time = event hour + delay.
- Sorts all messages by scheduled arrival time, so late events come after events with later event hours.
- Sends each message to its source's topic, keyed by county.
- Waits between messages according to the replay speed, measured from the first event's arrival time.
- Options for checking without sending: --validate-only counts the input, --dry-run prints messages without a broker.

## 3. create_topics.py (10 s)
- Creates the three topics with the settings from config.py.

## 4. run_experiments.sh (30 s)
- For each speed and delay condition: delete and recreate the topics, check they are empty, replay, then record the duration and the message count per topic.
- Writes one summary line per run to a CSV file and one log per run.

## 5. Demo (about 30 s)
- Run one fast condition (about 12 seconds), then show the summary line: exit 0, 6,000 / 6,000 / 6,000.
- Caution: the runner deletes the topics. Do not run it while Spark is reading from them.
