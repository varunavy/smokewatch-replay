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
- Difficulty: the 20-minute slow replay stalled for 2 hours. The Mac most likely slept and paused the replay's timer.
- Fix: stopped the run, discarded the partial log, re-ran with sleep prevention (caffeinate -dims). It finished in 1,200 s with 18,000 messages.
- Lesson: run long replays on a machine that cannot sleep, and judge each run by recorded duration, exit code and per-topic counts, not by "the process is still running".

## To add after the nine-condition runs
- Results table from River's Spark metrics (throughput, latency, freshness, join completeness)
- Do not add numbers until they come from real runs

## CRediT roles to confirm (Priya)
Software; Methodology (replay and delay design, LSH evaluation design); Investigation (live runs, Bloom filter and LSH experiments); Formal analysis (LSH recall and candidate fraction); Validation (including review of PR #20); Writing - original draft

## LSH: finding similar earlier conditions
- Task: for each county-hour, retrieve the 10 most similar earlier county-hours by fire, smoke and weather (6,000 rows, 13 features)
- Method: random-hyperplane LSH, 128-bit signatures, banding 32x4, 16x8 and 8x16
- Baseline: exact brute-force cosine top-10 over the same strictly earlier rows (queries in time order)
- Result: 16x8 keeps recall@10 at 0.98 while examining 23% of earlier rows; 8x16 examines 4% but recall drops to 0.79
- Honest finding: at 6,000 rows brute force takes about 47-52 us per query, so LSH is faster only at 8x16, and its index uses 2-3x the memory of the raw vectors
- Limit: one dataset size, no scaling measured

## Technical difficulty and lesson learned (LSH)
- Difficulty: median-filling missing FRP values would have made quiet hours look like fire hours
- Fix: 5,614 missing FRP rows equal the 5,614 no-fire rows, so empty means "no fire" and was set to 0
- Lesson: check what a missing value means before imputing it
