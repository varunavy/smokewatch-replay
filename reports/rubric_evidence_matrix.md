# SmokeWatch Replay Rubric Evidence Matrix

This tracker follows the final-project rubric in the same order. A criterion is marked complete only when the claimed work and accessible evidence both exist. Placeholder text must be replaced before submission.

| # | Criterion | Points | Current status | Evidence available now | Remaining action and owner |
|---:|---|---:|---|---|---|
| 1 | Code Walkthrough | 5 | In progress | `docs/kafka_walkthrough.md`; component READMEs and source files | Create one timed walkthrough plan covering ETL, Kafka, Spark, MongoDB, PostgreSQL, Bloom filter, and validation. Each member explains the code they own. Varuna compiles; all rehearse. |
| 2 | Presentation Skills, including time management | 5 | Not yet evidenced | Individual slide notes are beginning to appear in `slides/` | Fix the final presentation length, assign speaking times, and complete at least two timed rehearsals. Save a rehearsal log. All members. |
| 3 | Discussion / Q&A | 5 | Not yet evidenced | Technical decisions are documented in the proposal, READMEs, PR discussions, and validation contract | Prepare likely questions about data volume, synthetic delays, watermarking, Bloom-filter false positives, missing observations, and system limitations. Create shared Q&A notes. All members. |
| 4 | Demo | 5 | Partial | Kafka live-run logs; Spark smoke-test output; storage demo output | Complete the live Kafka-to-Spark-to-PostgreSQL path. Prepare a short live demo plus a tested fallback using saved logs and outputs. River and Sanjana lead; Priya and Varuna support. |
| 5 | Report | 7 | In progress | Proposal, intermediate report, team LaTeX sections, validation documents | Produce a 6–11 page IEEE LaTeX report, excluding appendices. Do not include code or unnecessary screenshots. Check grammar, citations, Turnitin readability, completeness, and final PDF size. Varuna compiles; all review. |
| 6 | Version Control | 3 | Strong, final check pending | Public GitHub repository, branches, commits, pull requests, and reviews | Confirm the repository is public and every evidence link works without collaborator access. Preserve commit and PR history. Varuna verifies. |
| 7 | Lessons Learned | 5 | Partial | Priya documented the stalled slow replay and sleep-prevention lesson; validation and integration issues are recorded | Collect at least one specific technical lesson and best practice from every member. Include them in both report and slides. All members. |
| 8 | Prospects of Winning a Competition / Publication | 3 | Partial | Proposal discusses publication potential | Name a realistic venue after verifying scope and eligibility. State what stronger scale, comparison, or real-delay evidence would be required. Do not claim acceptance or publishability as a fact. Varuna leads. |
| 9 | Innovation | 5 | Partial | Deterministic replay of multi-agency wildfire records; separation of final completeness from on-time completion | Write a precise innovation statement and compare it with relevant replay and streaming benchmarks. Avoid claiming that replay itself is new. Varuna and Priya. |
| 10 | Teamwork | 5 | Strong, final documentation pending | Four component owners, separate branches and PRs, review messages, shared integration decisions | Add a CRediT statement and a concise contribution table. Link each member’s commits, PR, files, and integration work. All members verify. |
| 11 | Technical Difficulty | 5 | Strong, final documentation pending | Multi-source ETL, deterministic replay, event-time aggregation, watermark, idempotent storage, alias mismatch, sleep-related replay failure | Explain the difficult parts, failed attempts, fixes, and remaining limitations using evidence. Avoid describing routine setup as the main difficulty. All members. |
| 12 | Practiced Pair Programming | 2 | Planned, not completed | Proposed Varuna–Priya and River–Sanjana sessions | Conduct genuine sessions. Record date, driver, navigator, task, role switch, result, and related commit or PR comment. Varuna maintains evidence. |
| 13 | Practiced Agile / Scrum | 2 | At risk | Team deadlines and component ownership exist, but formal sprint evidence is incomplete | Run an honest one-week sprint through Nov. 16 using GitHub Issues/Projects or Trello. Save backlog, owners, status changes, check-in notes, and retrospective. Do not recreate fictitious earlier sprints. Varuna maintains; all update. |
| 14 | Slides | 3 | In progress | `slides/priya_slides.md`; proposal figures and component notes | Build one consistent deck, include results and limitations, add CRediT, and export to PDF or PPTX. Varuna compiles; all review. |
| 15 | Used LaTeX | 2 | In progress | LaTeX component sections and BibTeX entries exist | Create and compile a native IEEE `main.tex`, bibliography, figures, tables, and appendix. Submit the `.tex` source and PDF. Varuna compiles with Priya’s sections. |
| 16 | Creative Presentation Techniques | 2 | Not yet evidenced | None yet | Add useful, restrained animation or another clear visual technique to explain replay speed, delayed arrival, or the pipeline. It must improve understanding rather than decorate the slides. Presentation team. |
| 17 | Literature Survey | 7 | In progress | Proposal references; `report/references_priya.bib`; `report/sections/literature_priya.tex` | Organize related work into meaningful subsections, verify every entry against an original or authoritative source, cite every factual claim, and remove duplicates. Varuna and Priya. |
| 18 | Use of Streaming Algorithms | 5 | Strong, final verification pending | `experiments/bloom_filter/bloom_experiment.py`, results, run log, report section | Preserve code and evidence, explain synthetic duplicates, false positives, memory trade-off, and why the standalone experiment does not modify the main pipeline. Priya. |
| 19 | Use of Stream Processing Frameworks such as Spark, Flink, and Kafka | 5 | In progress | Kafka live-run evidence; Spark streaming code and smoke-test evidence | Complete live Kafka-to-Spark execution and capture counts, progress, latency, and database output. Distinguish producer evidence from integrated measurements. Priya and River. |
| 20 | Use of Locality-Sensitive Hashing | 5 | Missing | None | Decide whether a relevant LSH experiment can be completed and defended, such as approximate similarity between standardized county-hour environmental profiles. Assign an owner and document method, baseline, results, and limitations. Do not add an unrelated demonstration only to claim points. |
| 21 | Use of Privacy Techniques | 5 | Missing | Public environmental data and data-minimization decisions are documented, but no rubric-level privacy experiment exists | Decide on a relevant technique and justification. A small differential-privacy release experiment on aggregate PM2.5 statistics may be defensible if its purpose, privacy assumptions, error, and limitation are explicit. Assign an owner. |
| 22 | Other Course Tools and Techniques | 5 | Strong candidates, final mapping pending | ETL, MongoDB, PostgreSQL star schema, dimensional modeling, event time, watermarks, idempotent upserts, reproducible validation | Select the strongest course techniques not already counted elsewhere. Explain their role and link evidence without double-counting the same work. Varuna and Sanjana. |
| 23 | New Tools Not Used for Homework | 3 | Likely met, verification pending | Kafka, Docker Compose, MongoDB/PostgreSQL integration, or another project tool | Verify which tools were genuinely new relative to all team members’ homework. Name the tool, learning process, difficulty, and evidence. All members verify. |
| 24 | Flow Schematic in Slides and Report | 1 | Partial | Earlier proposal architecture figure | Update the final diagram to match the implemented path: agency files → conversion/producer → Kafka → Spark → MongoDB/PostgreSQL → validation and analysis. Include it in report and slides. Varuna compiles. |

## Submission Requirements Outside the Point Table

- Use the IEEE Computer Society LaTeX template.
- Keep the report between 6 and 11 pages, excluding appendices.
- Keep submitted files near 1 MB or less where practical.
- Do not place source code in the report.
- Submit relevant `.py` files. If an `.ipynb` is submitted, include its converted `.py` file.
- Submit slides as PDF or PPTX.
- Include the rubric appendix in this exact order and support every claim with accessible evidence.
- Include public GitHub, Agile/Scrum, pair-programming, video, and other external links in the appendix when applicable.
- Check every external link while signed out or in a private browser window.
- List all generative-AI and other tools used, with accurate task-level disclosure.
- Include key learnings, technical difficulties, and best practices.
- Include a CRediT contributor statement in the slides.
- Attend and present at the assigned date and time.

## Immediate Priority Queue

1. Complete and merge corrected PR #5.
2. Resolve River’s NOAA aliases, replay-relative freshness, and live Kafka consumer.
3. Connect Spark `foreachBatch` to Sanjana’s idempotent PostgreSQL upsert.
4. Start and document the real one-week sprint.
5. Complete and record pair-programming sessions.
6. Decide owners and scope for LSH and privacy criteria.
7. Create the native IEEE LaTeX report shell and final flow schematic.
