# AutoBott v2

AutoBott v2 is a regime-first, risk-gated, replayable options decision engine.

Phase 1 is paper-first and non-executing. It produces decision cards and learning-ledger rows, not broker orders.
Paper-only Alpaca connectivity is allowed for market/status/capture workflows, but no live broker execution or order placement is implemented.

## Scope

- P0: Doctrine + scope lock
- P1: Read-only options decision cards
- P2: Paper execution after decision cards are stable
- P3: Learning ledger and forward-outcome measurement
- P4: Backtest/replay harness
- P5: Broker adapter preview (no live execution)
- P6: Live adapter approval gate

## Phase 1 Validation

Run the test suite:

```powershell
pytest
```

Validate a captured market/options snapshot and optionally append the decision card to a JSONL ledger:

```powershell
.\.venv\Scripts\python.exe -m autobott_v2.phase1_validate --snapshot .\path\to\real_snapshot.json --ledger .\data\learning_ledger.jsonl
```

The validator expects real captured inputs: market bars, option-chain quotes, SPY/QQQ/VIX context, event blackout flags, and IV history. It does not synthesize market data.

Phase 1F/1G adds two paper-only operator surfaces:

- Alpaca paper capture/status plumbing for read-only market snapshots and raw payload preservation
- a Render-hosted operator console for safe capture, advisory replay, and report inspection

Those additions do not allow order placement, live trading, or browser-side secret entry.

The read-only Alpaca config loader accepts the old bot's common environment names:

- `APCA_API_KEY_ID` or `ALPACA_API_KEY`
- `APCA_API_SECRET_KEY` or `ALPACA_SECRET_KEY`
- `APCA_API_BASE_URL` or `ALPACA_BASE_URL`
- `APCA_API_DATA_URL` or `ALPACA_DATA_URL`
- `ALPACA_PAPER`

Phase 1 cycle-strategy gating starts from `data/PHASE1_CYCLE_GATE.json`, which is intentionally disabled by default and separated from older strategy gates.

Phase 1 now distinguishes:

- decision cards: what the engine wanted, including `schema_version`, `decision_id`, and `reason_codes`
- paper capture: raw Alpaca paper-market payload capture plus minimal manifest-backed snapshots via `autobott_v2.phase1_alpaca_capture_now`
- ledger events: what actually happened in paper validation, including fill model, spread, quote age, and tactical/rider leg role
- execution simulation: tradability checks plus realistic paper fills via `autobott_v2.phase1_execution_sim`
- replay + exit lifecycle: deterministic replay artifacts, manifesting, unresolved-position handling, and fixed exit policies via `autobott_v2.phase1_replay` and `autobott_v2.phase1_exit_engine`
- slippage sweep: fill-model sensitivity runs via `autobott_v2.phase1_slippage_sweep`
- replay campaign + bucket eligibility review: advisory campaign artifacts and per-bucket paper/live-review checks via `autobott_v2.phase1_replay_campaign` and `autobott_v2.phase1_bucket_eligibility`
- scorecard / gate updates: aggregated outcome stats, lifecycle diagnostics, and bucket authorization that keep trading disabled unless the phase gate passes
- operator dashboard: a paper-only operator console for status, capture, replay, and report reads via `autobott_v2.dashboard_app`

## Purpose Lock

P2 and later engine expansion are gated by the Purpose Lock document.

Read:

- docs/DOCTRINE.md
- docs/AUTOBOTT_V2_PURPOSE_LOCK.md
- docs/BUILD_PLAN.md
