# Phase 1 Historical Backtest (Approximate)

`autobott_v2.phase1_historical_backfill` synthesizes a Phase 1 snapshot corpus from **real** historical
stock bars (pulled from Alpaca's paper-account market-data endpoint) so the existing replay/campaign/scorecard
pipeline can run against something other than synthetic unit-test fixtures.

## What is real and what is modeled

- Real: underlying stock closing prices for the traded symbol, `SPY`, `QQQ`, and `VIXY` (VIX proxy), pulled
  from Alpaca's historical bars endpoint.
- Modeled (not real market quotes): the option chain. There is no free historical options-quote source, so
  each snapshot's option chain is generated with a Black-Scholes pricer using trailing realized volatility
  (x1.10) as an IV proxy. This means:
  - Flat volatility surface: no skew/smile, no term-structure beyond the two DTE buckets used.
  - No dividends.
  - Bid/ask spread is a fixed 6% of modeled mid-price, not a real observed spread.
  - Volume/open interest are fixed placeholder values, not real liquidity.

Because of this, backtest output is a **directional signal**, not proof of real edge. Treat a good result as
"worth running forward in live paper capture," not as validated profitability.

## Isolation from the real gate

Results are written to `data/PHASE1_HISTORICAL_BACKTEST_GATE.json` and
`artifacts/phase1_historical_backtest/`, never to `data/PHASE1_CYCLE_GATE.json`. The real gate is reserved for
evidence produced by genuine `phase1_alpaca_capture_now` paper captures. Do not repoint the historical backtest
at the real gate path.

## Known pipeline limitation: run one symbol at a time

`phase1_replay.run_replay` evaluates every open position's exit against whatever snapshot it's currently
processing, matching on `option_symbol` only — it does not filter by ticker. If a corpus interleaves multiple
symbols in one replay run, positions get marked `unresolved` as soon as the next snapshot belongs to a
different symbol. This is a pre-existing property of the replay engine (it was written and tested against
single-symbol snapshot streams), not something this backfill tool works around. Because
`run_phase1_campaign` / `run_phase1_backfill` accept a `symbols` filter, run one campaign per symbol against
the shared corpus and combine the resulting scorecards yourself when reading results across a basket of
tickers.

## Usage

```powershell
. .\local.env.ps1
.venv\Scripts\python.exe -m autobott_v2.phase1_historical_backfill --symbols AAPL --start 2024-01-01 --end 2026-06-30 --corpus-root data\phase1_historical_corpus

.venv\Scripts\python.exe -m autobott_v2.phase1_campaign_runner --snapshot-corpus data\phase1_historical_corpus --symbols AAPL --out artifacts\phase1_historical_backtest --campaign-run-id AAPL_v1
```

Repeat the campaign-runner step once per symbol (`--symbols MSFT`, `--symbols NVDA`, ...) against the same
corpus root, since the corpus can hold multiple symbols even though each campaign run should only touch one.

Read `artifacts/phase1_historical_backtest/<run_id>/gate_candidate_report.json` and
`replay_campaign_summary.md` for win rate, profit factor, and expectancy per symbol.
