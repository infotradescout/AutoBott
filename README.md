# AutoBott

Intraday options autotrader + dashboard for Alpaca.

## Deploy On Render (No GitHub Linking Required)

Use this one-click link:

[![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com/deploy?repo=https://github.com/infotradescout/AutoBott)

If the button does not open the blueprint flow, use:

1. Render Dashboard -> New + -> Blueprint
2. Public Git repository URL: `https://github.com/infotradescout/AutoBott`
3. Confirm both services from `render.yaml`:
   - `autobott-trader` (worker)
   - `autobott-dashboard` (web)
4. Add env vars to both services:
   - `ALPACA_API_KEY`
   - `ALPACA_SECRET_KEY`

## Local Run

```powershell
cd autotrader
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
# fill in real Alpaca keys in .env
python dashboard.py

# Safety smoke check (compile + key dashboard endpoints)
python smoke_check.py
```

## Safety + Ops Controls

The trader now includes:
- Pre-open readiness (`PREOPEN_READY_MINUTES`, default 10).
- Daily and weekly loss circuit breakers.
- Drawdown-aware position size reduction after losing streaks.
- Entry/fill slippage guards.
- Optional event-day entry block list (`NEWS_BLOCK_DATES_ET`).
- Runtime state persistence (`autotrader/runtime_state.json`) for restart continuity.
- Optional alerting/heartbeat to Discord and/or a generic webhook.

Configure these in `autotrader/.env` (see `autotrader/.env.example`).

Render note:
- Attach a persistent disk and set `DATA_DIR=/data` so runtime files survive restarts:
  - `trades.csv`
  - `scan_log.csv`
  - `runtime_state.json`
  - `trading_control.json`
