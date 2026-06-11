# Repository Lanes

## Repo

AutoBott v2

## Repo Doctrine

AutoBott is a regime-first, read-only options decision-card system in Phase 1.

Current doctrine:

- Phase 1 only unless Gawain assigns a later phase.
- Read-only decision cards only.
- No broker orders.
- No execution paths.
- No live trading.
- No fake runtime market data.
- Tests may use deterministic fixtures.
- Local validation must not require credentials.
- Broker execution comes later only after explicit approval.

## Safe Parallel Lanes

### `docs`

Purpose: repository doctrine, operating process, handoff notes, and format documentation.

Allowed files:

- `README.md`
- `docs/**/*.md`
- `schemas/**/*.json` only when documenting or locking a contract assigned in the prompt

Banned files:

- `src/**/*.py`
- `tests/**/*.py`
- Runtime config files
- Generated outputs and ledgers

Validation:

```powershell
pytest
```

### `schema-contract`

Purpose: JSON schemas and contract validation tests for Phase 1 inputs and outputs.

Allowed files:

- `schemas/**/*.json`
- `docs/autobott_v2/**/*SNAPSHOT*.md`
- `src/autobott_v2/*contract*.py`
- `src/autobott_v2/*validate*.py`
- `tests/test_*schema*.py`
- `tests/test_*snapshot*.py`

Banned files:

- Broker adapters
- Execution engines
- Order models
- Live data ingestion

Validation:

```powershell
pytest
```

### `decision-engine`

Purpose: regime, direction, volatility, contract scoring, and decision-card behavior.

Allowed files:

- `src/autobott_v2/phase1_engine.py`
- `src/autobott_v2/phase1_models.py`
- `tests/test_phase1_decision_cards.py`
- Contract docs only when the behavior changes the contract

Banned files:

- Broker adapters
- Order placement code
- External API clients
- Ledger migration scripts unless assigned

Validation:

```powershell
pytest
```

### `ledger-learning`

Purpose: learning ledger structure, forward-outcome placeholders, replay-readiness fields, and persistence tests.

Allowed files:

- `src/autobott_v2/phase1_ledger.py`
- `src/autobott_v2/phase1_models.py`
- `schemas/phase1_decision_card.schema.json`
- `tests/test_phase1_decision_cards.py`
- `tests/test_phase1_snapshot_validation.py`
- `docs/autobott_v2/**/*.md`

Banned files:

- Broker adapters
- Order placement code
- Alpaca API calls
- Backtest engines unless assigned

Validation:

```powershell
pytest
```

### `read-only-config`

Purpose: environment-variable loading and local validation ergonomics without requiring credentials.

Allowed files:

- `src/autobott_v2/phase1_config.py`
- `README.md`
- `docs/**/*.md`
- `tests/test_phase1_decision_cards.py`

Banned files:

- Live API calls
- Broker execution code
- Credential-required tests
- Secrets or `.env` files

Validation:

```powershell
pytest
```

## Unsafe Lane Pairings

Do not run these lanes in parallel without Gawain sequencing:

- `decision-engine` with `schema-contract`, when decision-card shape or snapshot shape is changing.
- `decision-engine` with `ledger-learning`, when `DecisionCard` fields are changing.
- `schema-contract` with `ledger-learning`, when output schemas or forward-outcome fields are changing.
- Any lane with a future `execution` lane.
- Any lane with broad repo cleanup.

## Branch Naming Convention

Use:

```text
<lane-type>/<short-description>
```

Examples:

```text
docs/parallel-ai-execution-lanes
schema/phase1-market-snapshot-contract
engine/volatility-block-rules
ledger/forward-outcome-fields
```

## Validation Expectations

Default validation command:

```powershell
pytest
```

If validation cannot run, report the exact reason. Do not invent a pass.

## Return Format

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
