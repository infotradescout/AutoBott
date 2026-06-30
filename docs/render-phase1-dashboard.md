# Render Phase 1 Dashboard

This dashboard is an operator console for Phase 1 only.

- Paper only
- Live trading locked
- Order placement disabled
- Advisory artifacts only

## Render Service

Use one Render Web Service for v1:

- build command: `pip install .`
- start command: `python -m autobott_v2.dashboard_app`
- health check path: `/api/health`

If you want `data/` and `artifacts/` to survive deploys or restarts, attach a persistent disk and point the service at durable storage for those directories.

## Required Environment Variables

- `ALPACA_ENV=paper`
- `ALPACA_API_KEY_ID=<paper key>`
- `ALPACA_API_SECRET_KEY=<paper secret>`
- `ALPACA_TRADING_BASE_URL=https://paper-api.alpaca.markets`
- `ALPACA_DATA_BASE_URL=https://data.alpaca.markets`
- `AUTOBOTT_LIVE_TRADING_ENABLED=false`
- `AUTOBOTT_ALLOW_ORDER_PLACEMENT=false`
- `AUTOBOTT_PAPER_ONLY=true`
- `AUTOBOTT_DASHBOARD_AUTH_TOKEN=<long random token>`

Do not expose Alpaca credentials in frontend code.

## Local Run

```powershell
.\.venv\Scripts\python.exe -m autobott_v2.dashboard_app
```

## Safe Actions

- Start a paper capture
- Run an advisory replay campaign
- Refresh corpus and report summaries

## Safety Checks

- Verify the banner shows `PAPER ONLY | LIVE TRADING LOCKED | ORDERS DISABLED`
- Check `/api/safety`
- Check `git diff -- data/PHASE1_CYCLE_GATE.json`
- Confirm capture and campaign responses report `active_gate_changed: false`
