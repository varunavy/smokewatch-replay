# SmokeWatch Replay

SmokeWatch Replay is a big data course project that evaluates a multi-source pipeline using historical California wildfire, air-quality and weather data.

## Initial Research Question

How do replay velocity and delayed records affect the throughput, latency, freshness, join completeness and correctness of a Kafka–Spark streaming pipeline?

## Initial Case Study

- Event: 2021 Dixie Fire
- Pilot period: July 13 to August 31, 2021
- Region: Butte, Plumas, Shasta, Lassen and Tehama counties
- Time resolution: One-hour windows

## Planned Data Sources

- NASA FIRMS active-fire detections
- EPA hourly PM2.5 observations
- NOAA hourly weather observations
- CAL FIRE incident information

## Project Status

Current phase: Batch-reference and replay-simulation validation are complete.
Kafka, Spark Structured Streaming, MongoDB and PostgreSQL implementation are
the next integration stage.

### Completed artifacts

- `notebooks/01_full_period_pipeline.ipynb` builds and validates the complete
  50-day, five-county data pipeline.
- `notebooks/02_replay_preparation.ipynb` creates 18,000 deterministic replay
  events and nine controlled replay schedules.
- `reports/tables/` contains controlled-simulation metrics.
- `reports/figures/` contains the corresponding simulation figures.
- `src/evaluation/validate_batch_reference.py` independently checks the saved
  county-hour source files and integrated batch reference.

The performance tables currently describe a controlled Python simulation.
They are not measured Kafka or Spark benchmark results.

## Local data validation

Raw and generated datasets are intentionally excluded from Git. After the
local data files have been generated, run:

```bash
python src/evaluation/validate_batch_reference.py
```

The command writes a validation table to
`reports/tables/batch_validation_summary.csv`.
