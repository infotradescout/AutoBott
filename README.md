# AutoBott v2

AutoBott v2 is a paper-first, risk-gated, replayable decision engine.

This repository intentionally starts with doctrine and a pure paper engine.
No live broker execution is implemented.

## Scope

- P0: Doctrine + scope lock
- P1: Paper trading engine
- P2: Risk gate hardening
- P3: Signal intake pipeline
- P4: Backtest/replay harness
- P5: Broker adapter preview (no live execution)
- P6: Live adapter approval gate

## Purpose Lock

P2 and later engine expansion are gated by the Purpose Lock document.

Read:

- docs/DOCTRINE.md
- docs/AUTOBOTT_V2_PURPOSE_LOCK.md
- docs/BUILD_PLAN.md
