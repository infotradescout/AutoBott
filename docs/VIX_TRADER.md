# VIX Trader

VIX Trader is **paper trading + Robinhood mirror reporting**.

AutoBott papers the paired VIX/VIXW trade, keeps the ledger, and tells you exactly what to
copy on Robinhood for real money. AutoBott does **not** submit live VIX broker orders.

## Product

1. Log / paper a paired VIX or VIXW call+put cycle in AutoBott.
2. Read the Robinhood action queue (`BUY_TO_OPEN`, `HOLD`, `SELL_TO_CLOSE`) with strike, expiration, quantity, and limit hints.
3. Place the same trade on Robinhood.
4. Review the paper performance report so future entries follow what worked.

## Surfaces

- Workspace: `/vix-trader`
- Mirror report: `GET /api/vix-trader/robinhood-mirror`
- Status: `GET /api/vix-trader/status` (`mode=paper_trading_with_robinhood_reporting`)
- Paper entry: `POST /api/vix-trader/preflight` and `POST /api/vix-trader/cycles`

## What this is not

- Not an IBKR live execution product.
- Not a blocker for the existing Alpaca equity/ETF options paper session.
- Not a claim that paper P&amp;L equals Robinhood fills.

## Evidence

Closed paper cycles still feed parameter selection over time. Until enough closed paper results
exist, the workspace uses an explicit **paper candidate** so you can keep trading and mirroring
instead of waiting on a form or a broker adapter.
