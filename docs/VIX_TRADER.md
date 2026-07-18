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

The working thesis records the preferred spot VIX range as the 17s. Unprovided strategy values deliberately default
to `null`, including:

- minimum full sessions remaining;
- maximum DTE;
- combined-debit and cycle-allocation caps;
- first-leg target;
- second-leg rule;
- addition count, capital, sizing, and trigger.

Preflight returns `strategy_configuration_incomplete` until Thomas supplies those rules. Values can be supplied with
the `AUTOBOTT_VIX_*` environment variables declared in `render.yaml`, persisted through `PUT /api/vix-trader/config`,
or stored in the data-root configuration file. This prevents placeholder values from silently becoming executable
strategy logic while allowing hosted configuration to survive restarts.

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
