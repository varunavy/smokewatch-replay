# Varuna: Differential privacy experiment (slide content)

## Slide 1: Query and privacy design

- Goal: release the daily number of county-hours with at least one NASA FIRMS fire detection
- Input: 6,000 county-hour rows, five counties, 50 dates, 250 county-day groups
- Convert each county-hour to a bounded contribution: detection present = 1, otherwise 0
- Each daily county result is between 0 and 24; observed exact range was 0--4
- Record-level adjacency: one county-hour row can change one daily answer by at most 1
- L1 sensitivity = 1; add Laplace noise with scale `1 / epsilon`
- Epsilon values: 0.1, 0.5, 1, 2 and 5

Speaker point: We deliberately did not add noise to the raw fire-detection count because its maximum was 1,894. The binary contribution gives a clear and defensible sensitivity.

## Slide 2: Measured privacy--utility trade-off

Use a line chart with epsilon on the x-axis and MAE on the y-axis, plus the main values below.

| Epsilon | MAE (active hours) | RMSE | P95 absolute error |
| ---: | ---: | ---: | ---: |
| 0.1 | 5.157 | 8.243 | 21.000 |
| 0.5 | 1.402 | 2.198 | 4.575 |
| 1.0 | 0.755 | 1.183 | 2.595 |
| 2.0 | 0.393 | 0.621 | 1.364 |
| 5.0 | 0.158 | 0.253 | 0.556 |

- 200 trials x 250 county-day answers = 50,000 evaluated releases per epsilon
- Smaller epsilon gives stronger privacy and larger error
- At epsilon 1, the average error was below one active hour
- Epsilon 5 had the lowest error, but it provides weaker privacy

## Slide 3: Validation, limitation and lesson

- All 6,000 rows passed validation; every county-day had 24 hourly rows
- Fixed seed 226; the repeated 200-trial run produced an identical results file
- No external Python package was required
- Clipping to 0--24 created positive boundary bias because many exact answers were zero
- The trials estimate utility only; publishing repeated answers would consume privacy budget
- The source data are public environmental observations, so this is a mechanism demonstration rather than a real personal-data deployment

Technical difficulty: defining a query with a justified contribution bound and sensitivity, rather than choosing a noise value first.

Lesson learned: differential privacy begins with the query and adjacency definition. The noise is meaningful only after sensitivity has been established.

## CRediT roles to confirm (Varuna)

Conceptualization; Methodology; Software; Validation; Formal analysis; Investigation; Data curation; Writing -- original draft; Project administration.
