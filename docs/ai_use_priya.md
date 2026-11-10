# AI use: Priya (Kafka ingestion, replay and Bloom filter)

Tool: Claude (Anthropic)

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
