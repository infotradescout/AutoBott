# AutoBott

Intraday options autotrader + dashboard for Alpaca.

## Deploy on Render

Use this one-click link:

[![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com/deploy?repo=https://github.com/infotradescout/AutoBott)

AutoBott currently deploys as **one Render web service**, not two separate services.
The service defined in `render.yaml` is:

- `autobott`

That single service runs both:

- the trader loop
- the dashboard web app

via:

```yaml
startCommand: python -u autotrader/render_service.py
```

### Render setup

1. Render Dashboard -> New + -> Blueprint
2. Public Git repository URL: `https://github.com/infotradescout/AutoBott`
3. Confirm the single service from `render.yaml`:
   - `autobott` (web)
4. Add these required environment variables:
   - `ALPACA_API_KEY`
   - `ALPACA_SECRET_KEY`
   - `DASHBOARD_CONTROL_TOKEN`
5. When `PAPER_TRADING=true` for options, also set these (required for options contract/quote lookups):
   - `ALPACA_LIVE_API_KEY`
   - `ALPACA_LIVE_SECRET_KEY`
6. Keep `DATA_DIR=/data` so runtime files survive restarts on the persistent disk.

### Render persistence

Attach a persistent disk and keep:

- `DATA_DIR=/data`

Important runtime files stored there include:

- `trades.csv`
- `scan_log.csv`
- `runtime_state.json`
- `trading_control.json`

## Local run

```powershell
cd autotrader
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
# fill in real Alpaca keys in .env
# Starts dashboard only (does not start trader loop)
python dashboard.py

# Safety smoke check (compile + key dashboard endpoints)
python smoke_check.py
```

## Render-style local run

To run the same single-process service used on Render:

```powershell
cd autotrader
python render_service.py
```

## Safety + ops controls

The trader includes:

- Pre-open readiness (`PREOPEN_READY_MINUTES`, default 10).
- Daily and weekly loss circuit breakers.
- Drawdown-aware position size reduction after losing streaks.
- Entry/fill slippage guards.
- Optional event-day entry block list (`NEWS_BLOCK_DATES_ET`).
- Runtime state persistence (`autotrader/runtime_state.json`) for restart continuity.
- Optional alerting/heartbeat to Discord and/or a generic webhook.

Configure these in `autotrader/.env` (see `autotrader/.env.example`).

## Trade analytics

Notes:
- This is a report-only CLI (`autotrader/review.py`).
- It reads existing trade logs; it does not run trading.

Run the terminal report:

```powershell
python autotrader/review.py
```

Emit structured JSON instead of terminal text:

```powershell
python autotrader/review.py --format json
python autotrader/review.py --format json --output autotrader\trade_report.json
```

Export grouped CSV breakdowns for comparisons or downstream tooling:

```powershell
python autotrader/review.py --export-csv-dir autotrader\reports
```
