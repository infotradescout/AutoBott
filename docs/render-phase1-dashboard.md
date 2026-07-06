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

Deploy remains blocked until the persistent disk and hosted secrets below are configured. Render web services are ephemeral by default, so `data/` and `artifacts/` are not durable without this disk-backed layout.

## Required Hosted Secrets

- `ALPACA_API_KEY_ID=<paper key>`
- `ALPACA_API_SECRET_KEY=<paper secret>`
- `AUTOBOTT_DASHBOARD_AUTH_TOKEN=<long random token>`

Do not expose Alpaca credentials in frontend code.

The Render blueprint now bakes in the non-secret paper defaults:

- paper Alpaca endpoints
- live trading disabled
- order placement enabled for paper
- session autostart enabled
- `SPY` session loop every `300` seconds
- session window `09:35` to `15:55` in `America/New_York`
- paper execution armed on startup
- disk-backed data, artifacts, and gate paths under `/var/data/autobott`

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
5. Confirm `/api/safety` reports paper-only mode and `PAPER EXECUTION ARMED` after service startup.
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
