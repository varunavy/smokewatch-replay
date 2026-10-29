# SmokeWatch Replay: Intermediate Status Report

## 1. Progress achieved

The project has completed its batch-data foundation for the 2021 Dixie Fire
case study. NASA FIRMS fire detections, EPA AirData PM2.5 readings and NOAA
GHCNh weather observations were standardized to a complete UTC county-hour
grid covering July 13 through August 31 across Butte, Lassen, Plumas, Shasta
and Tehama counties. Each source contains 6,000 county-hour rows, and the
integrated batch reference contains 6,000 unique county-hour rows with no
duplicate keys.

Replay preparation is also complete. The three sources produce 18,000 unique
source-event IDs. Three replay speeds and three deterministic delay profiles
produce nine schedules and 162,000 scheduled event deliveries. A controlled
Python simulation was used to check whether the proposed evaluation measures
respond sensibly before the distributed pipeline is run. GitHub currently
contains the executed notebooks, simulation tables and figures. A separate
validation script has been added so the saved batch files can be checked again
before streaming correctness is evaluated.

**Teammate implementation update:** [REPLACE AFTER PULL REQUEST REVIEW]

## 2. Findings and results so far

The full-period NASA workflow processed 143,979 raw records and retained
53,394 detections after applying the fire-type, confidence, county and
buffered-perimeter rules. EPA processing produced 4,791 county-hours with
PM2.5 measurements. Lassen County has no PM2.5 coverage, so those measurements
remain missing rather than being replaced with zero. NOAA produced 5,804
weather-available county-hours; the nearby station used for Plumas remains
marked as a proxy.

The controlled simulation showed the expected distinction between delay and
capacity effects. At assumed rates of 15 and 150 events per second, the model
remained below its configured 500-event-per-second capacity. At 1,500 events
per second, the queue became the main source of lag. These are simulation
findings used to refine the experiment; they are not measured Kafka or Spark
results.

**Measured implementation findings:** [ADD ONLY AFTER SUCCESSFUL EXECUTION]

## 3. Difficulties and resolution plan

The source agencies use different schemas, reporting coverage and missing-data
conventions. The project addressed this by using a common county-hour key,
separate availability flags and documented source-selection rules. Raw data is
excluded from Git because of its size, so executed notebooks and a repeatable
validation script provide the current evidence trail.

The main remaining difficulty is that the Kafka, Spark and database components
have not yet been integrated into the default branch. Each component has a
named owner, branch, README and execution-evidence requirement. Pull requests
will be reviewed against the shared event envelope and the integrated batch
reference before they are merged.

**Current technical blockers:** [REPLACE WITH TEAM-SUPPLIED EVIDENCE]

## 4. Remaining tasks

| Owner | Remaining task | Required evidence |
|---|---|---|
| Priya Venugopal Nair | Kafka topics, rate-controlled producer and delay schedules | Pull request, run command and consumer output |
| River Roseveare-Hunt | Spark schema, watermark and county-hour stateful aggregation | Pull request, run command and streaming output |
| Sanjana Vivek Phalke | MongoDB archive, PostgreSQL star schema and queries | Pull request, document/table counts and query output |
| Varuna Vyomessh | Batch validation, integration review and correctness comparison | Validation table, pull-request review and report |

After integration, the team will run the nine measured conditions, repeat each
condition three times, compare the streamed keys and values with the batch
reference, and report throughput, latency, freshness lag, on-time join rate,
final completeness and correctness. An expanded stress test will respond to
the proposal feedback about workload scale.

## 5. Generative AI disclosure

Yes. ChatGPT/Codex was used to map the status-report requirements, inspect the
repository evidence, distinguish completed batch work from planned streaming
work, and draft a report structure. One exact prompt was: “I need to do this
next step, help me finish it so that I can receive complete rubrics that we
missed last time.” Suggestions were checked against the GitHub files, executed
notebook outputs and assignment screenshot before adoption. AI was not used to
create or alter experimental measurements.

**Team-specific AI use:** [ADD EACH TOOL, EXACT PROMPT, SECTION AND VERIFICATION]

The required SAID certificate will be submitted with the report.
