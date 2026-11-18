# Async peer review — issue #15 (River ↔ Sanjana)

**Type:** asynchronous peer review (not live pair programming)  
**Date:** 2026-11-17  
**Reviewer:** River Roseveare-Hunt (Spark)  
**Author under review:** Sanjana Phalke (storage / PR #29)  
**Branch / commits inspected:** `sanjana-final-testing` @ `48243cf` (also `cedfa2e`, `8720aae`, `7c1ddfe`)

## Files / behaviour checked

1. `src/spark/streaming_job.py` — `--starting-offsets {earliest,latest}` (default `earliest`); Kafka-only filter `message_run_code == --run-code`.
2. `evidence/spark_official/validation_summary.txt` — official Spark→Postgres counts.
3. `evidence/mongo_official/run_commands.txt` + ingestion JSON/logs — 18k Mongo path.
4. Cross-check against River’s merged paced pilot `fast__none__rep1__20261116T225429Z` (PR #26) for schema/`run_code` contract only (separate run).

## Expected vs observed

| Check | Expected | Observed | How verified |
|---|---|---|---|
| Default offsets | `earliest` so measured replays still drain full topics from a fresh checkpoint | Default remains `earliest`; `latest` is opt-in | Code review of diff `origin/main...48243cf` |
| `latest` use | Only with fresh checkpoint + coordinated produce (or clear topic window) | Flag added for isolated integration tests; README not expanded in this diff | Code review |
| `run_code` filter | Isolates warehouse facts to one execution when topics are shared | Filter applied on Kafka path after parse | Code review |
| Official Spark/Postgres | 18k in → 6k facts, join-ready, idempotent | Summary reports 6k groups, 6k facts, 0 duplicate keys, repeat counts and fact-ID ranges unchanged; complete fact-ID-set equality not verified | Inspected committed `validation_summary.txt` (did not re-execute) |
| Official Mongo | 18k unique docs; retry does not grow set | Commands + evidence under `evidence/mongo_official/` claim 18k + repeat consumer exit 0 | Inspected committed commands/logs (did not re-execute) |

## Findings

- **Approve** the Spark CLI addition: default `earliest` preserves the measured paced pilot protocol; `latest` is acceptable for Sanjana’s isolated shared-topic tests when paired with a **fresh checkpoint** and matching `--run-code`.
- **Caveat:** the new `message_run_code == --run-code` filter requires the CLI `--run-code` to match the producer envelope exactly; messages with null/missing `run_code` would be dropped. Call that out in runbooks.
- Her Mongo producer command used `--no-pacing` — fine for storage correctness, not a paced Spark speed claim (consistent with how we labeled River’s unpaced correctness pilot).
- Her official validation uses run_code `SANJANA_MONGO_OFFICIAL_18K_01`, separate from River’s paced `fast__none__rep1__20261116T225429Z`. Report should keep both labeled by role (storage integration vs paced Spark measurement).

## Author response / recheck

Pending Sanjana’s ack on the `run_code` filter caveat. No Spark code change requested from River for submission if PR #29 documents the flag.

## Technical takeaway (report / slides)

Shared Kafka topics need **explicit offset policy + `run_code` isolation + fresh checkpoints**; otherwise a second consumer can either miss the replay (`latest` too late) or mix executions (`earliest` without filtering).
