# Priya: Kafka, replay and Bloom filter (slide content)

## Slide 1: Kafka ingestion
- Three Kafka topics, one per source: firms.events, epa.pm25.events, noaa.weather.events
- Messages keyed by county; 3 partitions per topic; single broker in Docker
- 18,000 events = 3 sources x 5 counties x 1,200 hours (Jul 13 to Aug 31, 2021)
- Each message: common envelope (source, county, event hour, event ID, delay, arrival time) plus a source-specific payload
- Use the team flow schematic here (same figure as the report)

## Slide 2: Replay design
- Replay speed = simulated event-hours per real second: slow 1, medium 10, fast 100
- Full replay takes about 20 min, 2 min and 12 s; fast measured at 12 s
- Delay conditions: none; moderate (10.26% of events, 1 to 3 h late); severe (25.27%, 4 to 12 h late)
- Delay depends only on the event ID and level, so the same events are late at every speed
- Messages are sent in order of arrival time, so late data arrives out of event-time order
- 3 speeds x 3 delays = 9 conditions, 3 repetitions each

## Slide 3: Live verification
- Full live replay: 18,000 sent, 0 invalid
- Each topic holds exactly 6,000 messages
- Consumer samples: message source matches the topic
- Every event in a source has the same set of fields (checked on all 18,000)
- Experiment runner: reset topics, check they are empty, replay, record counts and duration
- Test run fast__none: 12 s, exit 0, 6,000 / 6,000 / 6,000

## Slide 4: Bloom-filter duplicate detection
- Test: 18,000 event IDs plus 1,800 injected duplicates, 5 trials per filter size
- Results (target false-positive rate 5% / 1% / 0.1%):
  - Filter size: 13.7 / 21.1 / 31.6 KiB
  - Duplicates missed: 0 / 0 / 0 (all 1,800 found)
  - False positives: 222 / 29 / 2.2
  - Measured false-positive rate: 1.23% / 0.16% / 0.012%
- 58 to 133 times smaller than an exact Python set (1.78 MiB)
- Caveats: duplicates are synthetic; standalone experiment, not in the Kafka or Spark pipeline

## Slide 5: Technical difficulty and lesson learned
(Replace with the difficulty you found most important)
- Challenge: Kafka keeps messages, so repeated runs would mix events
- Fix: runner deletes and recreates the topics and checks offsets are zero before every run
- Lesson: make experiments repeatable by resetting state and checking it

## To add after the nine-condition runs
- Results table from River's Spark metrics (throughput, latency, freshness, join completeness)
- Do not add numbers until they come from real runs

## CRediT roles to confirm (Priya)
Software; Methodology (replay and delay design); Investigation (live runs, Bloom experiment); Validation; Writing - original draft
