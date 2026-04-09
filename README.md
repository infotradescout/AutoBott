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
```
