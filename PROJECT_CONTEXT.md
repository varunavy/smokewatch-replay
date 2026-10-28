# Project Context

SmokeWatch Replay studies how replay velocity and delayed records affect a
multi-source environmental-data pipeline. The focused case is the 2021 Dixie
Fire from July 13 through August 31 across Butte, Lassen, Plumas, Shasta and
Tehama counties.

NASA FIRMS supplies fire detections, EPA AirData supplies hourly PM2.5
measurements, NOAA GHCNh supplies weather observations, and the CAL FIRE final
incident perimeter is used as a geographic reference. The three time-based
sources are normalized to a complete UTC county-hour grid.

## Verified batch scope

- 50 days and 1,200 UTC hours
- 5 California counties
- 6,000 county-hour rows per time-based source
- 18,000 source-specific replay events
- 6,000 rows in the integrated batch reference
- 9 controlled replay scenarios and 162,000 scheduled event deliveries

## Current boundary

The batch ETL, integrated reference, deterministic replay schedules and Python
capacity simulation are complete. Kafka, Spark Structured Streaming, MongoDB
and PostgreSQL remain the distributed-system implementation stage. Simulated
metrics must not be reported as measured streaming-system performance.
