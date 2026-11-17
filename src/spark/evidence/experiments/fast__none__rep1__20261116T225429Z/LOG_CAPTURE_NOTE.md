# Spark log capture note (`fast__none__rep1__20261116T225429Z`)

Committed `spark_console.log` contains the application banner, topic
subscribe lines, and per-batch summaries (input_records / county-hour groups /
join_ready / postgres upsert complete).

**Not retained in git:** the full Spark driver progress JSON stream and an
explicit graceful-shutdown dump (query stop metrics). Those lived only in the
local runtime log (`spark_console_full.log`), which was trimmed before commit
to keep the PR reviewable. Watermark drop counts were not separately exported;
batch summaries did not report `numRowsDroppedByWatermark` for this run.

Producer duration/exit and exact commands are in `meta.txt` / `commands.txt`.
Input checksums: `evidence/kafka/replay_input_checksums.txt`.
