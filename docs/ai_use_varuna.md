# AI use: Varuna (validation, differential privacy and coordination)

Tool: ChatGPT Work / Codex (OpenAI), with web search used to verify the differential privacy references.

## What it was used for

- Step-by-step Git and GitHub guidance for reviewing and merging PR #5, synchronizing branches and making small, traceable commits
- Planning the GitHub Project board, task ownership, status updates and rubric evidence
- Checking the repository for likely credential files and common secret patterns before and after making it public
- Selecting a bounded differential privacy query and explaining adjacency, sensitivity, epsilon and the Laplace mechanism
- Drafting `experiments/differential_privacy/privacy_experiment.py`
- Interpreting the experiment output and drafting the README, report section, references and slide notes
- Troubleshooting the Vim merge-message screen, an unmatched shell quote and CSV line-ending warnings

## What I checked myself

- Reviewed Priya's final commit and PR #5 changes before approving and merging
- Ran the repository and secret checks locally and enabled GitHub Secret Protection; GitHub reported no secret alerts
- Ran the privacy script first with 20 trials and then with 200 trials
- Confirmed the input checks: 6,000 rows, five counties, 50 dates, 250 county-day groups and no invalid counts
- Checked that the exact daily active-hour answers were between 0 and 4 and that the maximum raw fire-detection count was 1,894
- Inspected the five epsilon results and compared the privacy--utility pattern
- Repeated the full 200-trial run with seed 226 and confirmed that the result files were identical
- Ran `git diff --check`, corrected the CSV line endings and verified a clean working tree after each push
- Checked the report claims against `results.csv`, `run_output.log`, `determinism_check.log` and the experiment code

## Prompts and interactions

The work below was completed on Nov. 12, 2026. Terminal and GitHub outputs were often supplied as screenshots, so those screenshots are not transcribed here.

1. Asked for a simple explanation of the GitHub Project board and status-update feature.
2. Asked for a concise team message covering Priya's PR, Sanjana's review request, write access and Agile evidence.
3. Shared Priya's final commit and terminal checks and asked what to do next.
4. Asked for exact navigation to PR #5 and for future repository presentation improvements suitable for a resume.
5. Shared the merge-message editor and asked how to exit and complete the merge safely.
6. Asked what GitHub Secret scanning is for and whether it should be enabled.
7. Asked when the privacy work and personal commits would begin and requested that no rubric work be missed.
8. Shared the processed-data file list and headers and asked to begin the privacy task.
9. Asked whether the downloaded file was `privacy_experiment.py`.
10. Shared the 20-trial and 200-trial outputs, determinism check and Git status after each stage.
11. Shared the trailing-whitespace and unmatched-quote problems and asked how to resolve them.
12. Asked for the report, citation, AI-use and presentation evidence to follow the existing team format.

## Reference verification

On Nov. 12, 2026, ChatGPT used web search to locate the publisher records for Dwork et al. (2006) and Dwork and Roth (2014). I checked that the authors, publication details, page ranges and DOI values in `report/references_varuna.bib` matched those records.
