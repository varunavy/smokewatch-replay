# Pair / integration session — Spark → PostgreSQL

## Session A — contract wiring (completed 2026-11-10)

| Field | Value |
|---|---|
| Date | 2026-11-10 |
| Driver | River Roseveare-Hunt |
| Navigator / contract author | Sanjana Vivek Phalke (via `src/storage/postgres_loader.py` + `src/storage/README.md`) |
| Task | Wire `foreachBatch` → `upsert_county_hour`; prove idempotent upsert |
| Result | **Pass** — see `postgres_wire_console.log`, `postgres_wire_db.txt` |

### What was done

1. Enabled `--write-postgres` on `src/spark/streaming_job.py`.
2. Mapped Spark county-hour rows to Sanjana’s required fields (UTC ISO `event_hour`,
   `first_join_ready_at` only when `is_join_ready`).
3. Applied `sql/schema.sql` to local `smokewatch-postgres` on host port **5433**.
4. Ran the sample file source twice with the same `run_code` /
   `scenario_code`. Second pass returned the **same** `fact_id` values (1–4);
   `fact_county_hour` stayed at **4** rows (no duplicates).

### Local DB env used for evidence (not committed)

```bash
export POSTGRES_HOST=127.0.0.1
export POSTGRES_PORT=5433
export POSTGRES_DB=smokewatch
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=  # local only; never commit
```

### Command

```bash
python src/spark/streaming_job.py \
  --source file --input-dir data/samples --trigger once --reset-checkpoint \
  --run-code postgres-wire-001 --scenario spark_postgres_wire \
  --speed fast --delay none --write-postgres
```

## Session B — live pair with Sanjana (scheduled)

Draft ask is in `GroupOps/sources/email/drafts/2026-11-10-river-replies.md`.
When Sanjana confirms host/credentials and a slot, record driver/navigator/
duration here and attach any additional DB screenshots.
