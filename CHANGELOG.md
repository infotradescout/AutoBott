# Changelog

## 2026-04-09

- Added pre-open readiness behavior with a 10-minute window before market open.
- Added weekly loss circuit breaker on top of existing daily loss controls.
- Added drawdown-aware position sizing using account-equity risk and streak-based size reduction.
- Added execution slippage protections for entry quotes and filled prices.
- Added configurable event-day block list via `NEWS_BLOCK_DATES_ET`.
- Added runtime state persistence (`autotrader/runtime_state.json`) so key counters and metadata survive restarts.
- Added webhook alerting and heartbeat support (Discord + generic webhook).
- Added crash alerting in `render_service.py`.
- Updated defaults to build morning watchlist at `09:30 ET`.
- Documented new controls and env vars in `.env.example` and `README.md`.
