# SmokeWatch Spark — River handoff pack (target noon Nov 16)

Owner: River Roseveare-Hunt · Branch: `river-spark`

## Deliverables

| Artifact | Location |
|---|---|
| Working Kafka-connected job | `src/spark/streaming_job.py` |
| Live Kafka evidence | `src/spark/evidence/kafka_smoke_*`, `part-*.json` |
| Replay-relative freshness | `streaming_job.write_batch` + `evidence/delay_profiles/` |
| Postgres upsert path | `--write-postgres` → `src/storage/postgres_loader.py` |
| Pair / integration notes | `evidence/postgres_pair_session.md` |
| Flow schematic | `handoff/flow-schematic.md` |
| Report + slide text | `handoff/report-and-slides.md` |
| IEEE / literature refs | `handoff/references.md` |
| AI prompts log | `handoff/ai-prompts-log.md` |
| Non-claims | LSH / privacy **not** claimed without separate evidence |

## Quick verify

```bash
docker compose -f docker/docker-compose.yml up -d
python src/kafka/create_topics.py
# terminal A
python src/spark/streaming_job.py --source kafka --reset-checkpoint \
  --run-code kafka-smoke-001 --scenario spark_kafka_smoke
# terminal B
python src/kafka/replay_producer.py --input-dir data/samples \
  --speed fast --delay none --scenario spark_kafka_smoke --no-pacing
```

Expect: `join_ready=4` for the 12-sample smoke.
