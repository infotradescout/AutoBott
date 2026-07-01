# AutoBott v2 Doctrine

AutoBott v2 is built as an automated options trading system whose research stack, replay stack, and historical stack
exist to improve and audit live trading behavior rather than replace it.

## Non-Negotiables

- The product is the bot, not the backtest harness.
- Long calls and long puts only unless scope is explicitly expanded.
- Risk-gated at decision time and execution time.
- Deterministic, explainable signal generation.
- Fail-closed on missing, stale, or unsafe inputs.
- Replayable and auditable order, fill, and position records.
- Operator-visible safety status, with kill-switch support.
- Secrets remain server-side.

## Forbidden

- Naked option selling.
- Undocumented discretionary overrides.
- Secret exposure in frontend/operator surfaces.
- Silent fallback from protected broker execution to undefined behavior.
- Strategy changes that bypass audit trails.
- Production execution without explicit risk controls, position controls, and operator-visible status.

## Transitional Current State

The current repository is still in a pre-execution stage:

- signal, replay, scorecard, capture, and dashboard layers exist
- research and history are already strong support systems
- committed broker order placement is not yet implemented

That is a repository maturity fact, not the product identity.

## Build Direction

- Research, replay, and historical analysis must inform the trading bot.
- Operator surfaces must graduate from monitor-only to controlled execution oversight.
- Execution capability must be added as a first-class domain, not hidden inside research modules.

## Initial KPIs

- 100% of trade candidates have a decision record.
- 100% of submitted orders have an immutable audit trail.
- 0 trades bypass risk or position controls.
- 0 secrets reach browser clients.
