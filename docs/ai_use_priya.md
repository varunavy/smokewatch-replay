# AI use: Priya (Kafka ingestion, replay and Bloom filter)

Tool: Claude (Anthropic), Claude Sonnet 5.5 in the Claude app, with web search used for the literature references

## What it was used for
- Step-by-step guidance for running the live Kafka replay (Docker Compose broker, topic creation, replay, count checks)
- Writing the experiment runner script (src/kafka/run_experiments.sh)
- Writing the Bloom-filter experiment (experiments/bloom_filter/bloom_experiment.py)
- Drafting report sections (Bloom filter, Kafka and replay design) and the message schema document
- Git and GitHub guidance

## What I checked myself
- Ran every command and script and saved the logs under evidence/kafka/
- Compared topic counts (6,000 per topic) and the message fields with the real data
- Checked the report claims against my results and the producer code

## Prompts used
(Paste the exact prompts below, with the date of each.)

All prompts below were sent on Nov 10, 2026. Messages that were only screenshots (terminal output, GitHub Desktop, Docker Desktop) had no typed text and are not listed. Where I pasted earlier output or a file, it is noted in brackets.

1. [attached the team plan file] how do i do this on gi.Only tell me what is required for Priya
2. how do i do this work
3. [pasted the project README] this is te read me.tell me step. y step what to do
4. what shoud i do tell me step by step
5. where i save the evidence from step 1
6. only after step 2 can i go to step 3
7. this is what my desktop shows
8. Before the full 27 runs: check with River what do i do now.wait for river
9. [pasted a list of suggested tasks] tell me what to do step by step
10. what do i need to do next
11. [pasted the instructions for opening a pull request] how do i do this
12. Paste the description I gave you. what description
13. [pasted the instructions for sending the pull request link] tell me what to do here
14. copy url and paste where
15. so i need to copy url and paste the mesgase to every one
16. i send messages
17. how much of my work is left
18. can you directly do step 1
19. i mean can i start Step 1: Check that your input files match the schema I'm about to write this without anyone replying or should i wait people to replky
20. no one has replied
21. [pasted two suggested checks] how do i check this
22. yes
23. can i start step 1
24. [attached the project proposal assignment file; no typed text]
25. [attached the final project assignment and rubric file; no typed text]
26. tell me what i need to do next
27. [pasted a draft message to Varuna] remove the 4th point
28. sorry not the 4th point the 5th point.keep the 4th point
29. 3. Fix your AI-use log how do i do this

## Help provided, by task
Prompt numbers refer to the list above.

- Git and GitHub workflow (prompts 1, 5, 7, 11 to 16): explained how to use a branch, which files to commit, how to push, and how to open a pull request and share its link. I chose which files to commit and kept the data files out.
- Kafka live run and replay (prompts 2 to 4): gave the Docker Compose broker file, the topic-creation and replay commands, and how to check per-topic counts. I ran every command myself and saved the logs under evidence/kafka/.
- Experiment runner (prompts 8 and 9): wrote src/kafka/run_experiments.sh, which resets the topics, replays one condition and records counts and duration. I tested it on the fast__none condition.
- Bloom-filter experiment (prompt 9): wrote experiments/bloom_filter/bloom_experiment.py. I ran it and the results are in experiments/bloom_filter/.
- Report sections, message schema and related work (prompts 9, 10, 17 to 22): drafted the LaTeX sections in report/sections/ and docs/message_schema.md from my results. I checked the schema against all 18,000 events and checked the delay-ordering claim against the producer code. For the references, Claude searched the web and gave BibTeX entries.
- Rubric review and planning (prompts 10, 17, 24 to 26): summarised the assignment and rubric files and listed what was missing for my part and for the project.
- Coordination messages (prompts 6, 18 to 20, 27, 28): helped word messages to teammates, which I edited and sent myself.
