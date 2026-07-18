# AutoBott v2 Product Architecture

## Canonical Identity

AutoBott is an automated options trading bot for long calls and long puts.

Research, replay, scorecards, and historical analysis are critical to the system, but they are supporting
capabilities that exist to improve and supervise the bot rather than substitute for it.

## Product Domains

### Strategy registry

Strategies are additive modules registered with identifiers, supported underlying types, configuration schemas,
preflight validators, lifecycle handlers, risk extensions, analytics definitions, screens, and simulation/broker
capability flags. Generic execution code must not assume a strategy has one call and one put.

The first registered specialized module is `vix_paired_options`; see `docs/VIX_TRADER.md`.

### `signal`

What the bot wants to trade and why.

Current repo examples:

- `src/autobott_v2/phase1_engine.py`
- `src/autobott_v2/phase1_models.py`

### `risk`

Whether the bot is allowed to trade and at what size.

Current repo examples:

- `src/autobott_v2/phase1_bucket_eligibility.py`
- `src/autobott_v2/phase1_scorecard.py`

Missing product-grade pieces:

- pre-trade risk policy
- exposure caps
- position sizing policy
- kill-switch state

### `execution`

How the bot submits, monitors, cancels, and replaces orders.

Current repo state:

- simulation exists in `src/autobott_v2/phase1_execution_sim.py`
- production order-routing does not yet exist

### `positions`

How the bot manages open trades after entry.

Current repo state:

- replay exits exist in `src/autobott_v2/phase1_exit_engine.py`
- production position lifecycle management does not yet exist

### `operator`

How the human owner supervises, inspects, and interrupts the bot.

Current repo examples:

- `src/autobott_v2/dashboard_app.py`
- `src/autobott_v2/phase1_alpaca_capture_now.py`

## Supporting Domains

### `capture`

Real market-data collection used by the rest of the system.

### `replay`

Deterministic scenario playback for strategy and operations review.

### `history`

Historical corpus and backtest-style evidence generation.

### `scorecards`

Performance summaries, gate reports, and drift analysis.

## Current Gap

The repository is strongest in supporting domains and weakest in the product domains of `execution` and
`positions`.

That is why the repo feels like an analysis platform today: the support stack is mature enough to be visible,
while the trading stack is still incomplete.

## Direction

Every future architecture decision should answer this question first:

"Does this make the automated trading bot more real, more controllable, and more auditable?"

If the answer is no, it belongs to a support lane rather than the product core.
