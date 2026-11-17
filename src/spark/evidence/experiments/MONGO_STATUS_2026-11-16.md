# MongoDB status relative to official Spark pilots (2026-11-16)

- Unpaced correctness pilot `fast__none__rep1__20261115T213514Z` was
  **Kafka→Spark→PostgreSQL only**. MongoDB was not ingested in that run.
- Kafka topics still held the 18k official messages afterward so Sanjana
  could capture Mongo evidence. As of the paced rerun start on 2026-11-16,
  those topics are being reset for an isolated paced experiment; Mongo
  capture from that prior topic state is therefore **pending / not included**
  unless Sanjana records a separate ingest under that `run_code`.
- #15 (River–Sanjana) remains open for live pair or async peer review;
  document whichever actually occurs.
