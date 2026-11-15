# Experiment metrics (Priya 27 runs)

Drop per-run Spark progress excerpts and metric summaries here during the
9×3 window. Suggested naming: folders
`{speed}__{delay}__rep{N}__{UTC}/` with `metrics.json`, `produce.log`,
`spark_console.log`, `postgres_db.txt`.

Capture: throughput (input rows / batch), `processing_latency_ms`,
replay-relative `freshness_lag_event_hours`, join-ready counts,
`droppedByWatermark` from progress logs.

## Completed (2026-11-15)

See [`SUMMARY_2026-11-15.md`](SUMMARY_2026-11-15.md): three full-data
`fast × {none,moderate,severe}` runs with Postgres upserts (6,000 facts each).

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
