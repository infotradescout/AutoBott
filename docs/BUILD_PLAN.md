# AutoBott v2 Build Plan

## Product Goal

Build an automated options trading bot for long calls and long puts with operator oversight, broker execution,
risk controls, replayability, and auditable history.

Research, replay, and historical analysis remain core support systems, but they exist to improve the trading bot
rather than serve as the end product.

## P0 Identity + Architecture Lock

- Lock the top-level product definition around automated trading.
- Separate product layers from support layers in docs and package organization.
- Add tests that enforce the product identity instead of a paper-only identity.

## P1 Signal Foundation

- Maintain typed models for decision inputs and outputs.
- Keep snapshot contracts for market bars, option chains, quotes/spreads, SPY/QQQ/VIX context, and event blackout flags.
- Harden regime, direction, volatility, and contract-selection engines.
- Keep fail-closed checks and explicit blocked reasons.

## P2 Research and Evidence Support

- Preserve replayable JSON decision-card output and JSONL learning ledger support.
- Preserve paper-market status/capture plumbing for real snapshot collection.
- Preserve replay campaigns, slippage sweeps, scorecards, and historical corpus generation.
- Keep the operator dashboard useful for evidence review and operational visibility.

## P3 Execution Domain

- Add broker-facing execution interfaces as a first-class domain.
- Add order-intent models, order-state tracking, retries, and cancel/replace behavior.
- Add pre-trade risk validation that sits between signal generation and broker submission.
- Add audit records for every order submission, rejection, fill, and cancel event.

## P4 Position and Exit Domain

- Add open-position state management.
- Add exit policy enforcement for profit-taking, max-loss, and stale-position handling.
- Add position-level exposure checks and daily risk limits.

## P5 Operator Control Domain

- Add operator-visible kill switches and execution-state indicators.
- Add health, risk, broker, and portfolio visibility in the dashboard.
- Preserve server-side secret storage and authenticated operator access.

## P6 Deployment and Promotion

- Treat hosted environments as real operational surfaces, not just demo dashboards.
- Require durable storage, environment validation, smoke checks, and rollback steps.
- Promote from paper execution to live execution only after paper controls and auditability are proven.
