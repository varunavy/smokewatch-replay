# ETL and Batch-Reference Validation Summary

## Verified scope

The executed full-period pipeline covers July 13 through August 31, 2021,
across Butte, Lassen, Plumas, Shasta and Tehama counties. The 50-day period
contains 1,200 UTC hours and therefore 6,000 county-hour keys.

| Output | Rows | Columns | Verified result |
|---|---:|---:|---|
| NASA FIRMS county-hour table | 6,000 | 8 | 53,394 qualifying detections; 386 detection hours |
| EPA PM2.5 county-hour table | 6,000 | 9 | 4,791 hours with PM2.5 measurements |
| NOAA weather county-hour table | 6,000 | 14 | 5,804 weather-available hours |
| Integrated batch reference | 6,000 | 27 | Zero duplicate county-hour keys |

Each source contains five counties and 1,200 rows per county. The integrated
table preserves the source totals and uses the same UTC county-hour keys.

## Additional data-quality evidence

- NASA FIRMS ingestion processed 143,979 raw records and retained 53,394
  qualifying detections after fire-type, confidence, county and buffered
  incident-perimeter filtering.
- The five-day NASA overlap reproduced 976 detections across 19 detection
  county-hours.
- EPA processing retained 9,238 measurements across eight monitoring sites.
  Lassen County has no PM2.5 coverage and remains explicitly missing.
- NOAA produced 5,805 county-hours with raw observations and 5,804 hours with
  at least one available weather measurement. The Plumas proxy station remains
  explicitly labeled.
- The replay-preparation notebook created 18,000 unique source-event IDs and
  nine deterministic schedules containing 162,000 scheduled event rows.

## Evidence boundary

The replay throughput, latency, queue-depth and timely-completeness tables are
outputs of a controlled Python capacity simulation with an assumed processing
capacity of 500 events per second. They are useful for experimental planning,
but they are not measured Kafka or Spark performance results. Measured results
will be reported only after the streaming implementation is executed.

## Reproduction

Run the executed notebooks and then run:

```bash
python src/evaluation/validate_batch_reference.py
```

The validation script writes its machine-readable evidence to
`reports/tables/batch_validation_summary.csv`.
