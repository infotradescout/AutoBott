# AutoBott

Intraday options autotrader + dashboard for Alpaca.

## Deploy On Render (No GitHub Linking Required)

Use this one-click link:

[![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com/deploy?repo=https://github.com/infotradescout/AutoBott)

If the button does not open the blueprint flow, use:

1. Render Dashboard -> New + -> Blueprint
2. Public Git repository URL: `https://github.com/infotradescout/AutoBott`
3. Confirm the single service from `render.yaml`:
   - `autobott` (web; runs trader loop + dashboard via `autotrader/render_service.py`)
4. Add env vars:
   - `ALPACA_API_KEY`
   - `ALPACA_SECRET_KEY`
   - `ALPACA_LIVE_API_KEY`
   - `ALPACA_LIVE_SECRET_KEY`

Current deployment architecture:
- One Render web service (`autobott`) hosts both components.
- Trader loop and dashboard run in one process under `render_service.py`.
- Persistent runtime files are stored on the mounted disk (`DATA_DIR=/data`).

## Paper Trading Note (Important)

Paper order routing still requires live Alpaca API keys for options contract/quote lookups.

Why:
- Alpaca options reference endpoints can reject paper keys.
- The bot uses live keys for options metadata/quotes only.
- Order placement remains paper when `PAPER_TRADING=true`.

Operational implication:
- Treat this as a required dependency, not an optional convenience.
- If live keys are missing/invalid, options lookups may fail even in paper mode.

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

## Production Baseline (6k Account)

To reduce overfitting risk and operator confusion, use one baseline profile first.

Recommended baseline mode:
- Keep universe broad but controlled.
- Keep HTF mismatch as a soft penalty (not hard reject).
- Keep one entry style active at a time.

Start with these three KPIs only:
- Conservative executable expectancy per trade.
- Conservative win rate.
- Max intraday drawdown.

Do not optimize secondary metrics until these three are stable.

## Fail-Safes

Runbook checks before market open:
1. Confirm deploy health and service timestamp.
2. Confirm `DATA_DIR` points to mounted persistent disk.
3. Confirm `trades.csv` and `runtime_state.json` are writable.
4. Confirm `manual_stop=false` and expected strategy profile.

Runtime guardrails:
1. Daily and weekly loss limits enabled.
2. Slippage and spread guards enabled.
3. No overnight hold beyond configured exit cutoff.

After close:
1. Run `review.py`.
2. Compare paper-reported PnL vs conservative executable PnL.
3. If conservative expectancy drops below zero, reduce complexity before retuning.

## Trade Analytics

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

Render note:
- Attach a persistent disk and set `DATA_DIR=/data` so runtime files survive restarts:
  - `trades.csv`
  - `scan_log.csv`
  - `runtime_state.json`
  - `trading_control.json`
