## Summary

- **Measured official paced run (use for claims):**
  `fast__none__rep1__20261116T225429Z` — checksum-matched JSONL,
  consume-while-produce, **paced** producer (~13s wall), 18k → 6k Postgres
  facts / 6k join-ready; full `postgres_county_hour.csv` (batch compare
  passed).
- **Correctness pilot only (unpaced):**
  `fast__none__rep1__20261115T213514Z` — `--no-pacing`; batch equality only;
  not a measured paced fast-speed run.
- **Diagnostics (exclude):** synthetic-input
  `…T203657Z` / `…T203746Z` / `…T203822Z`.
- Handoff clarifies freshness (scheduled delay) vs latency (Kafka timestamp →
  Spark `processed_at`, not Postgres write time). Mongo not included in these
  Spark pilots (`MONGO_STATUS_2026-11-16.md`).

## Test plan

- [x] Official paced `fast`/`none` with CSV + commands + checksums pointer
- [x] Batch-reference equality on paced CSV (Varuna)
- [x] Label unpaced + synthetic runs separately
- [ ] Remaining paced delays/speeds/reps (pending; may not finish pre-submission)
- [ ] Mongo / #15 with Sanjana (pending or async)

## Evidence paths

- Paced: `src/spark/evidence/experiments/fast__none__rep1__20261116T225429Z/`
- Unpaced: `src/spark/evidence/experiments/fast__none__rep1__20261115T213514Z/`
- Summary: `src/spark/evidence/experiments/SUMMARY_2026-11-15.md`
