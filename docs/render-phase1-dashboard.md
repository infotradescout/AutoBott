# Render Phase 1 Dashboard

This dashboard is an operator console for Phase 1 only.

- Paper only
- Live trading locked
- Paper execution runtime-gated
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
- `AUTOBOTT_ALLOW_ORDER_PLACEMENT=true`
- `AUTOBOTT_PAPER_TRADE_ALL_PASSED_SIGNALS=true`
- `AUTOBOTT_PAPER_MAX_NEW_ENTRY_ATTEMPTS_PER_LOOP=25`
- `AUTOBOTT_PAPER_MAX_OPEN_ENTRY_BUY_ORDERS=25`
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

## Cutover Command

For tomorrow's live-data paper-trading cutover, use one readiness command after loading env vars:

```powershell
.\.venv\Scripts\python.exe -m autobott_v2.paper_readiness --symbol SPY --arm-runtime --arm-reason tomorrow_cutover --require-trading-ready
```

Expected result:

- exit code `0`
- JSON status `paper_trading_ready`
- `paper_execution_ready: true`

## Safe Actions

- Start a paper capture
- Run an advisory replay campaign
- Arm paper execution
- Run a protected paper trading cycle
- Start a paper session
- Refresh corpus and report summaries

The dashboard is allowed to read status, start paper-only capture, run advisory replay, and supervise paper-account order submission. It must never expose Alpaca secrets or enable live trading.

## Persistence Smoke Check

Before treating hosted operation as usable, verify persistence and paper execution readiness:

1. Run one paper-only capture or campaign so a new artifact is written under the mounted disk roots.
2. Restart or redeploy the Render service.
3. Confirm the captured manifest or replay report still exists after restart.
4. Confirm `/api/paper/readiness` reports `paper_trading_ready`.
5. Confirm `/api/safety` reports paper-only mode and `PAPER EXECUTION ARMED` only after runtime arm.
6. Confirm live trading remains disabled.
7. Confirm execution outcomes and order submissions are being journaled under the mounted data root.

## Safety Checks

- Verify the banner shows one of:
- `PAPER ONLY | LIVE TRADING LOCKED | ORDER PLACEMENT CONFIG DISABLED`
- `PAPER ONLY | LIVE TRADING LOCKED | RUNTIME EXECUTION PAUSED`
- `PAPER ONLY | LIVE TRADING LOCKED | PAPER EXECUTION ARMED`
- Check `/api/safety`
- Check `/api/paper/readiness`
- Check the gate file configured by `AUTOBOTT_GATE_PATH`
- Confirm capture and campaign responses report `active_gate_changed: false`
