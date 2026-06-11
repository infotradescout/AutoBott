# AutoBott v2 Build Plan

## P0 Doctrine + Scope Lock

- Lock paper-first doctrine.
- Define forbidden capabilities in early phases.
- Add tests that enforce doctrine language.

## P1 Paper Trading Engine

- Add typed models.
- Add pure decision engine.
- Add fail-closed checks and reason codes.
- Add replayable JSON decision output.

## P2 Risk Gate

Prerequisite: Purpose lock must be present and accepted in docs/AUTOBOTT_V2_PURPOSE_LOCK.md.

- Expand risk policy checks.
- Add rule-level metrics and audit fields.

## P3 Signal Intake

- Add deterministic signal ingestion contract.
- Normalize and validate raw signals.

## P4 Backtest / Replay Harness

- Replay historical signals and market states.
- Produce scorecards and drift diagnostics.

## P5 Broker Adapter Preview

- Add interface-only adapter preview.
- Keep execution disabled.

## P6 Live Adapter Gate

- Add explicit approval gate and release checklist.
- Enable live pathways only after all paper controls pass.
