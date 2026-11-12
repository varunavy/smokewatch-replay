# LSH experiment: similar-condition retrieval

For every county-hour, find the 10 most similar strictly earlier county-hours by cosine similarity. Ground truth is exact brute-force cosine top-10 over the same earlier rows. The LSH method is random-hyperplane (SimHash) signatures with banding: two rows are candidates if they share a bucket in any band.

## Reproduce

The replay input files are not committed. Create `firms.jsonl`, `epa_pm25.jsonl` and `noaa_weather.jsonl` in `data/replay_input/` by following the conversion steps in `src/kafka/README.md`. Install the tested dependency (NumPy 2.4.6 with Python 3.14.6):

    pip install -r experiments/lsh/requirements.txt

Then run from the repository root:

    python experiments/lsh/features.py
    python experiments/lsh/lsh_experiment.py | tee experiments/lsh/run_output.log

`features.py` joins the three sources by county and hour into 6,000 rows (5 counties x 1,200 hours) and 13 standardized features. `lsh_experiment.py` writes `results.csv` next to it.

## Method

- Queries run in timestamp order. All five county rows of an hour are queried before any of them is inserted, so candidates are strictly earlier. The exact baseline uses the same earlier rows.
- Queries with fewer than 10 earlier rows are skipped, leaving 5,990 queries per run.
- Signature length is fixed at 128 bits. Banding settings are 32x4, 16x8 and 8x16 (bands x rows per band).
- Random seeds are 101 to 105. Results are the mean and sample standard deviation (n-1) across the five seeds.
- Metrics: recall@10 (overlap of the LSH top-10 with the exact top-10, divided by 10), candidate fraction (candidates divided by the number of eligible earlier rows), mean query time for exact and LSH, and memory of the LSH bucket structure.

## Results (Nov 12, 2026 run)

| Banding | Recall@10 | Candidate fraction | Exact query (us) | LSH query (us) | LSH index (KB) |
|---|---|---|---|---|---|
| 32x4 | 0.9897 +/- 0.0003 | 0.7493 +/- 0.0084 | 48.9 | 241.3 | 1828.4 |
| 16x8 | 0.9804 +/- 0.0012 | 0.2278 +/- 0.0210 | 51.6 | 87.0 | 1246.3 |
| 8x16 | 0.7939 +/- 0.0100 | 0.0414 +/- 0.0067 | 46.9 | 24.7 | 1485.3 |

The exact baseline stores 609.4 KB of vectors. Recall and candidate fraction are deterministic given the seeds. Query times and index memory are approximate, environment-specific measurements that vary with the machine, its load and the Python and NumPy versions. The reported run used Python 3.14.6 and NumPy 2.4.6.

## Limitations

- One dataset size only (6,000 rows). No scaling is claimed. Better relative cost for LSH on larger data is expected, but it was not measured here.
- At this size and dimension, brute force is already fast, so LSH is slower in two of the three settings and uses more memory than the raw vectors.
- Missing values: FRP values are empty when there are no fire detections and are filled with 0. Missing wind direction (22.7 percent of rows) is filled with 0 for sine and cosine. Missing PM2.5 (20.2 percent) and other weather values are filled with the column median.
- Standardization uses statistics from the whole table, which is acceptable for an offline evaluation but would need a running estimate in a live system.
- Neighbors may come from any county, since the query asks for similar conditions rather than similar places.
- Recall is measured by index overlap, so ties in similarity are broken arbitrarily.
- Index memory is measured with tracemalloc for the bucket dictionaries and lists only.
