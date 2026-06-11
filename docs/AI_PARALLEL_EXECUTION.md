# AI Parallel Execution

This repository supports parallel AI work only when every session stays inside one explicit lane and reports its work honestly.

## Authority Model

- Gawain defines doctrine, slice scope, and merge order.
- Codex implements one assigned lane per session.
- Gemini reviews and criticizes the work.
- Gawain reconciles Gemini criticism and issues corrected prompts.
- Gemini criticism is not optional; unresolved criticism must be addressed or explicitly escalated to Gawain.

## Operating Rules

- One lane per Codex session.
- One branch per lane.
- No stacked unrelated work.
- Inspect first: read repo docs, current status, relevant files, and validation commands before editing.
- Choose the smallest safe slice that satisfies the assigned prompt.
- Prefer contracts and tests before behavior when possible.
- Do not fake status.
- Do not fake commits.
- Do not fake test results.
- Do not import doctrine, naming, or implementation patterns from another brand unless Gawain explicitly assigns that cross-repo work.
- Do not touch files outside the assigned lane unless the need is reported in the return.
- Validate before commit using the repo's normal command when it exists.
- Gemini review is required before merge.
- Gawain controls merge order.

## Branch Rules

Create a dedicated branch for each lane:

```text
<lane-type>/<short-lane-name>
```

Examples:

```text
docs/parallel-ai-execution-lanes
test/phase1-snapshot-contract
engine/regime-scoring-v1
```

Do not stack unrelated changes on a branch. If the prompt changes lanes, stop and create a new branch or ask Gawain for direction.

## File Discipline

Before editing, identify:

- Files inspected.
- Files expected to change.
- Files that are banned for the chosen lane.
- Validation command.
- Current branch and baseline SHA.

If work requires touching a file outside the lane, do the smallest possible edit and report it clearly.

## Validation Discipline

Use the repo's documented validation command when known. If it is unknown, inspect project files and docs first.

For this repo, the normal validation command is:

```powershell
pytest
```

Never claim a validation command passed unless it actually ran and returned success.

## Commit Discipline

Do not invent commit SHAs. If no commit was made, report `not committed`.

Commit only the lane's intended files. Do not sweep unrelated dirty work into the commit.

## Required Return Format

Every Codex lane must return:

```text
repo:
lane chosen:
branch:
baseline SHA:
files inspected:
files changed:
tests run:
test results:
commit SHA if committed:
PR link if opened:
final git status:
risks / follow-up needed:
```
