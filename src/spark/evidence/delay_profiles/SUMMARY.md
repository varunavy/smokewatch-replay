# Freshness lag — delay profile verification (2026-11-10)

Replay-relative definition used by `streaming_job.write_batch`:

```
freshness_lag_event_hours =
  (scheduled_arrival_time - event_hour) / 3600
```

`scheduled_arrival_time` is the max arrival among sources in the county-hour
group (includes producer delay). Wall-clock `processed_at` is **not** used for
freshness, so historical event hours and severe-delay runs are not inflated.

| Profile | Input | County-hour groups | Join-ready | Freshness min | Freshness max | droppedByWatermark |
|---|---|---|---|---|---|---|
| none | 12 sample envelopes | 4 | 4 | 0.0 | 0.0 | 0 |
| moderate | 12 sample envelopes | 4 | 4 | 0.0 | ~1.72 | 0 |
| severe | 12 sample envelopes | 4 | 4 | 0.0 | ~6.89 | 0 |

Watermark remains **13 hours** on `event_hour_ts`. Severe producer delay is
4–12 event-hours; progress logs for severe show `droppedByWatermark=0` and a
watermark of `2025-01-08T00:00:00.000Z` after the first non-empty batch
(latest event hour 13:00 minus 13h).

Live Kafka smoke (`delay=none`) also reported `freshness_lag_event_hours=0.0`
on all four join-ready groups — see `../kafka_smoke_*`.
