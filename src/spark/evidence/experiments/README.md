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

## Coordination with `run_experiments.sh`

Priya’s runner **deletes and recreates Kafka topics between runs**. Do **not**
leave a Spark query running across that boundary.

For each measured condition:

1. Stop Spark (or await graceful query stop).
2. Reset checkpoint + JSON output for that run, e.g.
   `--reset-checkpoint --checkpoint data/interim/spark/ckpt_{run_code} --output data/interim/spark/out_{run_code}`.
3. Start Spark with matching `--scenario` / `--speed` / `--delay` (Kafka
   `run_code` in the envelope becomes the warehouse `run_code` when present).
4. Start / allow the producer for that condition.
5. After join-ready / idle, stop Spark and archive logs under this folder.
