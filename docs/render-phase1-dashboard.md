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
- `AUTOBOTT_SESSION_AUTOSTART=true`
- `AUTOBOTT_SESSION_SYMBOLS=SPY`
- `AUTOBOTT_SESSION_INTERVAL_SECONDS=300`
- `AUTOBOTT_SESSION_START_TIME=09:35`
- `AUTOBOTT_SESSION_END_TIME=15:55`
- `AUTOBOTT_SESSION_MARKET_TIMEZONE=America/New_York`
- `AUTOBOTT_SESSION_ARM_PAPER_EXECUTION=true`
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
. .\local.env.ps1
.\.venv\Scripts\python.exe -m autobott_v2.dashboard_app
```

With those session env vars enabled, startup will arm paper execution automatically and wait for the configured New York session window before running cycles.

For the local Windows operator path, you can skip PowerShell entirely and launch:

```text
start_paper_dashboard.cmd
```

That launcher auto-loads `C:\Users\flavo\Downloads\AutoBott.env`, applies the local paper/session defaults, binds the dashboard to `127.0.0.1:8000`, and uses token `autobott-local` unless overridden.

For automatic weekday local operation, run this once on the workstation:

```text
install_trading_hours_tasks.ps1
```

That installs Windows scheduled tasks to start the local dashboard at `08:35 America/Chicago` and stop it at `14:56 America/Chicago` each trading weekday.

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
