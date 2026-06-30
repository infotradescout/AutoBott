# AutoBott v2 Build Plan

## P0 Doctrine + Scope Lock

- Lock paper-first doctrine.
- Define forbidden capabilities in early phases.
- Add tests that enforce doctrine language.

## P1 Read-Only Options Decision Cards

- Add typed models.
- Add data-layer snapshot contracts for market bars, option chains, quotes/spreads, SPY/QQQ/VIX context, and event blackout flags.
- Add regime, direction, volatility, and contract-selection engines.
- Add fail-closed checks and blocked reasons.
- Add replayable JSON decision-card output and JSONL learning ledger.

## P2 Paper Execution

Prerequisite: Phase 1 decision cards must be stable and auditable.

- Approved decision card to limit buy simulation.
- Fill confirmation simulation with tradability checks, spread/quote-age rejection, and leg-level tactical/rider outcomes.
- Monitor and limit sell simulation.
- No live broker orders.

## P3 Learning

- Persist accepted and rejected candidates.
- Attach 5m, 15m, 30m, and 1h forward outcomes.
- Surface regime, signal, ticker, and volatility failure modes.

## P4 Backtest / Replay Harness

- Replay historical signals and market states with manifest capture and isolated replay gate artifacts.
- Produce scorecards, slippage-sensitivity comparisons, campaign-level bucket eligibility reports, and drift diagnostics.

## P5 Broker Adapter Preview

- Add interface-only adapter preview.
- Keep execution disabled.

## P6 Live Adapter Gate

- Add explicit approval gate and release checklist.
- Enable live pathways only after all paper controls pass.
