# Render Phase 1 Dashboard

This dashboard is an operator console for Phase 1 only.

- Paper only
- Live trading locked
- Order placement disabled
- Advisory artifacts only
- Not an execution console

## Render Service

Use one Render Web Service for v1:

- build command: `pip install .`
- start command: `python -m autobott_v2.dashboard_app`
- health check path: `/api/health`
- persistent disk mount: `/var/data/autobott`

Deploy remains blocked until the persistent disk and hosted env vars below are configured. Render web services are ephemeral by default, so `data/` and `artifacts/` are not durable without this disk-backed layout.

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
- `AUTOBOTT_DATA_ROOT=/var/data/autobott/data`
- `AUTOBOTT_ARTIFACTS_ROOT=/var/data/autobott/artifacts`
- `AUTOBOTT_GATE_PATH=/var/data/autobott/data/PHASE1_CYCLE_GATE.json`

Do not expose Alpaca credentials in frontend code.

## Local Defaults

When the hosted persistence env vars are absent, local development keeps the existing repo-relative defaults:

- `AUTOBOTT_DATA_ROOT -> data/`
- `AUTOBOTT_ARTIFACTS_ROOT -> artifacts/`
- `AUTOBOTT_GATE_PATH -> data/PHASE1_CYCLE_GATE.json`

## Local Run

```powershell
.\.venv\Scripts\python.exe -m autobott_v2.dashboard_app
```

## Safe Actions

- Start a paper capture
- Run an advisory replay campaign
- Refresh corpus and report summaries

The dashboard is allowed to read status, start paper-only capture, and run advisory replay. It must never expose Alpaca secrets, submit orders, enable live trading, or act as a broker execution console.

## Persistence Smoke Check

Before treating hosted operation as usable, verify persistence without enabling trading:

1. Run one paper-only capture or campaign so a new artifact is written under the mounted disk roots.
2. Restart or redeploy the Render service.
3. Confirm the captured manifest or replay report still exists after restart.
4. Confirm `/api/safety` still reports paper-only mode and orders disabled.
5. Confirm no order-placement methods or endpoints appear.
6. Confirm live trading remains disabled.

## Safety Checks

- Verify the banner shows `PAPER ONLY | LIVE TRADING LOCKED | ORDERS DISABLED`
- Check `/api/safety`
- Check the gate file configured by `AUTOBOTT_GATE_PATH`
- Confirm capture and campaign responses report `active_gate_changed: false`
