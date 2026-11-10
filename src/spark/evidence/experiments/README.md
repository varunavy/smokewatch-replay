# Experiment metrics (Priya 27 runs)

Drop per-run Spark progress excerpts and metric summaries here during the
9×3 window. Suggested naming:

```
{speed}__{delay}__rep{N}_progress.log
{speed}__{delay}__rep{N}_metrics.json
```

Capture: throughput (input rows / batch), `processing_latency_ms`,
replay-relative `freshness_lag_event_hours`, join-ready counts,
`droppedByWatermark` from progress logs.
