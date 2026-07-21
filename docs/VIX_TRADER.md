# VIX Trader

VIX Trader is an additive strategy module inside AutoBott / Trader's Corner. It does not replace the platform
dashboard, generic execution infrastructure, existing options workflows, analytics, capture, replay, or account
controls.

## Current capability

- Strategy registration through `autobott_v2.strategy_registry`.
- Strategy-agnostic cycle, order-state, and immutable audit models in `autobott_v2.execution_cycle`.
- Paired VIX/VIXW call-and-put cycle representation in `autobott_v2.vix_trader`.
- Preflight checks for product mismatch, expiration mismatch, session, settlement, remaining sessions, duplicate
  requests, overlapping exposure, quantities, debit, and cycle capital.
- Separate strategy-performance and execution-quality reports.
- Dedicated `/vix-trader` workspace inside the existing dashboard service.
- Locked JSONL cycle persistence under the configured AutoBott data root, with atomic duplicate-request and active-expiration checks.
- Durable strategy configuration through environment variables, `data/vix_trader/config.json`, or the authenticated configuration API.

## Truthful execution status

VIX Trader is currently `simulation_and_preflight_only`. The existing Alpaca integration in this repository reads
and trades equity/ETF options. It does not prove the required actual Cboe VIX/VIXW index-option chain and order
capabilities. No VIX/VIXW order is submitted, no fill is fabricated, and no profitability is claimed.

The broker boundary requires account, chain, session, quote, preview, submit, cancel, replace, order, fill, and
position reconciliation capabilities before broker execution can be enabled.

## Configuration status

The working thesis records the preferred spot VIX range as the 17s.

Executable strategy parameters are **not** typed in by the operator. AutoBott selects them from a
predeclared candidate grid by measuring closed VIX cycle outcomes (sample size, expectancy, profit
factor, drawdown). Preflight returns `strategy_evidence_insufficient` until a candidate clears the
evidence gate.

Operator-saved values and `AUTOBOTT_VIX_*` environment variables are optional **risk ceilings** only.
They may tighten a promoted candidate; they cannot invent an unproven strategy.

## Settlement and session basis

The safety defaults reflect Cboe's published VIX/VIXW specifications: VIX options have regular hours, separate
global/curb sessions, morning settlement, and a last trading day immediately before settlement. AutoBott defaults to
regular-hours-only entry and schedules an exit deadline before the final tradable timestamp.

Primary references:

- https://www.cboe.com/tradable-products/vix/vix-options/specifications
- https://www.cboe.com/en/tradable-products/vix/vix-options/
- https://www.cboe.com/about/hours/us-options/

Exchange calendars and holiday/early-close exceptions must come from an authoritative Cboe-sourced calendar snapshot
before preflight can pass. The calendar model handles Sunday GTH, the Friday-evening closure, holidays, early closes,
RTH, curb, and final-trading-day timing. Hosted preflight fails closed if that snapshot is absent. It also requires
broker/exchange contract metadata; client-entered product, strike, expiration, and settlement descriptions are not
treated as contract truth.

The durable calendar file is `data/vix_trader/cboe_calendar.json`. It must contain `source` beginning with `cboe`, a
`source_url` on `cboe.com`, timezone-aware `published_at`, `coverage_start`, `coverage_end`, `holidays`, and
`early_closes`. Preflight rejects a valid-looking artifact when its coverage does not span the decision through the
selected expiration. The status API reports the loaded provenance and coverage window.

Client timestamps and override identities are ignored by the hosted route. The server supplies the decision timestamp,
and overrides require authenticated server-side authorization. Broker fills and quotes—not editable performance fields—
derive committed capital, proceeds, open value, realized P&L, and unrealized P&L.

## Broker adapter selection

`VixBrokerAdapter` is the capability boundary. Current status: **not implemented**;
`broker_execution_supported` remains `false`.

| Candidate | VIX / VIXW | Fit with AutoBott today | Decision |
|-----------|------------|-------------------------|----------|
| Alpaca (current) | Index options including VIX for **broker-partner** accounts; retail still blocked as of mid-2026; VIXW not confirmed | Already integrated for equity/ETF options paper | **Not sufficient** for this module yet |
| Interactive Brokers (TWS / Client Portal API) | Supports Cboe index options including VIX and weekly VIXW with proper market-data subscriptions | Separate adapter; Gateway/TWS ops cost | **Selected next adapter** |
| Proxies (VXX / UVXY equity options) | Not actual VIX/VIXW index options | Would fake product identity | **Rejected** |

Until an IBKR (or future Alpaca retail) adapter proves account, chain, session, quote, preview,
submit, cancel, replace, fill, and position reconciliation for VIX/VIXW, the module stays
simulation/preflight-only. No fills are fabricated.

Dual opt-in is required before any VIX broker object can do work:

- `AUTOBOTT_VIX_BROKER=ibkr`
- `AUTOBOTT_VIX_EXECUTION_ENABLED=true`

Defaults keep both off. The IBKR scaffold lives in `vix_ibkr_broker.py` and is unreachable from
`trading_cycle`, `session_runner`, or `AlpacaExecutionBroker`.

## Isolated evidence simulation

`python -m autobott_v2.vix_sim_runner` (or `POST /api/vix-trader/sim/run` with
`AUTOBOTT_VIX_SIM_ENABLED=true`) accumulates fingerprinted CLOSED cycles offline so the evidence
gate can promote a candidate. This path:

- never submits Alpaca orders
- never arms or pauses paper execution
- never runs inside the paper session supervisor

## Existing platform preservation

The existing `/` dashboard and all pre-existing API routes remain present. VIX Trader adds:

- `GET /vix-trader`
- `GET /api/strategies`
- `GET /api/vix-trader/status`
- `GET /api/vix-trader/config`
- `PUT /api/vix-trader/config`
- `GET /api/vix-trader/cycles`
- `POST /api/vix-trader/preflight`
- `POST /api/vix-trader/cycles`

No existing route, module, data model, migration, or test was removed.
