# Differential privacy experiment

This experiment studies the privacy--utility trade-off for one aggregate query over the NASA FIRMS replay data. It is an educational demonstration of differential privacy; the source data describe environmental observations rather than people.

## Query

For each county and date, the script releases the number of county-hours that had at least one fire detection:

1. Convert each county-hour to `1` when `fire_detection_count > 0`, otherwise `0`.
2. Sum the 24 hourly indicators for each county-day.

The exact answer is therefore bounded between 0 and 24. The input contains 6,000 county-hour rows, five counties, 50 dates, and 250 county-day groups. The observed exact answers range from 0 to 4.

## Privacy model

The experiment uses record-level adjacency: neighboring inputs differ in one county-hour row. One row contributes at most one active-hour indicator to one county-day result, so the vector query has L1 sensitivity 1.

Independent Laplace noise is added to every county-day result with scale

`sensitivity / epsilon = 1 / epsilon`.

The noisy answers are clipped to the valid range 0--24. Clipping is post-processing and does not weaken the differential-privacy guarantee. Smaller epsilon values provide stronger privacy but require more noise.

## Reproducible input

The canonical experiment input is committed at
`experiments/differential_privacy/input/county_hour_fire_activity.csv`.
It contains 6,000 county-hour rows and only the three fields required by this
experiment: `event_hour_utc`, `county`, and `fire_detection_count`.

The fixture is a minimal projection of the full pipeline output
`data/processed/replay/nasa_firms_events_full_2021-07-13_2021-08-31.csv`.
The full processed dataset remains excluded from Git because it is generated
from the upstream data pipeline. Committing this small experiment-specific
fixture allows the experiment to run from a clean clone without private API
keys or other local source files.

Fixture SHA-256:

`0a85e87577f20804ba3d37305aa1cc05ad4dab68169a950bf59f49ae220caadc`

A validation run using this fixture produced a byte-identical `results.csv`
with seed 226.

## Run the experiment

From the repository root:

```bash
python experiments/differential_privacy/privacy_experiment.py \
  --trials 200 \
  --output experiments/differential_privacy/results.csv \
  | tee experiments/differential_privacy/run_output.log
```

The script uses seed 226 by default. A second 200-trial run with the same seed produced an identical CSV, recorded in `determinism_check.log`.

## Results

Each epsilon was evaluated across 200 trials and 250 county-day answers, giving 50,000 evaluated releases per setting.

| Epsilon | Noise scale | MAE | RMSE | 95th-percentile absolute error | Clipped fraction |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 0.1 | 10.0 | 5.157 | 8.243 | 21.000 | 0.489 |
| 0.5 | 2.0 | 1.402 | 2.198 | 4.575 | 0.296 |
| 1.0 | 1.0 | 0.755 | 1.183 | 2.595 | 0.240 |
| 2.0 | 0.5 | 0.393 | 0.621 | 1.364 | 0.218 |
| 5.0 | 0.2 | 0.158 | 0.253 | 0.556 | 0.217 |

The results show the expected trade-off. At epsilon 0.1, strong noise protection increases the mean absolute error to about 5.16 active hours. At epsilon 1, the mean absolute error falls below one hour, while epsilon 5 produces the smallest error but offers weaker privacy. The positive mean signed error and the clipping rate occur mainly because many exact answers are zero: negative noisy values are clipped to zero, while positive noisy values remain positive.

## Validation and limitations

- All 6,000 input rows passed numeric validation.
- Every county-day group contained 24 hourly rows.
- The repeated run produced byte-identical results with seed 226.
- The raw fire-detection count reached 1,894, so the experiment uses a bounded binary contribution instead of applying noise directly to an unbounded raw count.
- The 200 trials estimate utility only. Publishing repeated noisy answers would consume additional privacy budget.
- Because this dataset is public environmental data, the experiment does not claim to protect real personal information. It demonstrates how query definition, contribution bounds, sensitivity, epsilon, and error are connected.

## Files

- `input/county_hour_fire_activity.csv`: committed 6,000-row experiment input fixture
- `privacy_experiment.py`: experiment and validation code
- `results.csv`: aggregate metrics for the five epsilon settings
- `run_output.log`: final 200-trial execution record
- `determinism_check.log`: repeatability check

## Reference

Cynthia Dwork and Aaron Roth, *The Algorithmic Foundations of Differential Privacy*, 2014, DOI: 10.1561/0400000042.
