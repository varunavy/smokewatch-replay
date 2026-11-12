# AI prompts / assistance log (River — Spark track)

Course requirement: disclose AI use. Summaries only; no secrets.

| Date | Tool | Prompt / task (summary) | How output was used |
|---|---|---|---|
| 2026-11-10 | Cursor Agent | Implement SmokeWatch Spark execution plan (Kafka smoke, freshness, Postgres, handoff) | Drove live smoke, code edits, evidence, handoff pack |
| 2026-11-10 | Cursor Agent | Draft replies to Priya / Sanjana / Varuna for schema + ETA + pair slot | Saved under `GroupOps/sources/email/drafts/` for human send |
| Prior weeks | Cursor / ChatGPT | Spark watermark / Structured Streaming design notes; Adaptive Watermarks SPP | Informed 13 h watermark rationale and report wording; human verified against Spark docs + EDBT paper |

**Human responsibility:** All evidence commands were run locally; metrics in
`src/spark/evidence/` are from real console/DB output. Draft emails must be
sent from River’s SJSU account after review.
