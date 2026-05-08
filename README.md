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

## Historical Replay Trainer

Replay historical stock bars through the live scanner logic without placing orders or writing to the live scan log:

```powershell
.\.venv\Scripts\python.exe autotrader\historical_replay.py --symbols SPY,QQQ,AAPL --start YYYY-MM-DD --end YYYY-MM-DD --interval 5m
```
By default this script now runs offline (cache-only). Add `--no-offline` temporarily if you want it to fill missing cache files from yfinance.

By default this writes to the active data directory:
- `historical_replay_results.csv`
- `historical_replay_results.summary.json`
- cached yfinance bars under `historical_cache`

The active data directory follows the app config fallback order: `DATA_DIR`, then `/data`, then `autotrader`, then `/tmp/autotrader-data`. Intraday yfinance intervals such as `1m` and `5m` are best for recent date ranges; use wider intervals for older studies.

To train on everything options can trade across the US equity exchange, set:
- `UNIVERSE_MODE="all_optionable"` in `config.py` (or keep defaults and pass a broad `--symbols` list to replay).
- `UNIVERSE_MAX_TICKERS` to a high value, e.g. `2000`, so the scan actually spans the full universe.

Live/replay behavior remains long-only: this bot only opens long CALL or PUT options and only tracks directional long outcomes.

Continuously sweep replay candidates and write a leaderboard:

```powershell
.\.venv\Scripts\python.exe autotrader\replay_optimizer.py --symbols SPY,QQQ,AAPL,AMD --start YYYY-MM-DD --end YYYY-MM-DD --iterations 0
```

The optimizer writes `replay_optimizer\optimizer_runs.csv`, `replay_optimizer\best_candidate.json`, and `replay_optimizer\optimizer_win_loss_ratio.csv` in the active data directory. It is replay-only: no orders are placed, and any "best" candidate should be reviewed before changing live settings.
It starts in offline mode by default. Use `--no-offline` when you want rolling windows to keep moving without pre-seeding cache for every new day.
Reduce scan memory/compute by lowering `--scan-bars` (how many intraday bars each scan sees).
Use `--min-win-loss-ratio` to require a stronger wins/losses edge before a candidate is marked passable or promotable.
For fresh data loops, add rolling mode and let the optimizer advance the window after each run:

```powershell
.\.venv\Scripts\python.exe autotrader\replay_optimizer.py --symbols SPY,QQQ,AAPL --start 2026-04-01 --end 2026-05-01 --iterations 0 --rolling --rolling-step-days 1 --rolling-end-policy cache
```

`--rolling-end-policy cache` keeps advancing using the latest end date available in local cache files for every symbol (all offline), while `--rolling-end-policy today` uses the current date.
When `--no-offline` is enabled, rolling windows can now advance even if cache is not already populated for the next window (missing bars are fetched during replay).
`--iterations 0` keeps the optimizer running continuously and appends a cumulative win/loss rollup per candidate to `optimizer_win_loss_ratio.csv`.

To sweep the full US optionsable universe from Alpaca, run:

```powershell
.\.venv\Scripts\python.exe autotrader\replay_optimizer.py --symbols-source all_optionable --symbols-limit 0 --start YYYY-MM-DD --end YYYY-MM-DD --iterations 0
```
For offline runs with the full universe, provide a cached symbol file and keep offline mode:
```powershell
.\.venv\Scripts\python.exe autotrader\replay_optimizer.py --symbols-source all_optionable --symbols-file autotrader\data\all_optionable_symbols.txt --offline --start YYYY-MM-DD --end YYYY-MM-DD --iterations 0
```
Set `--symbols-limit` to throttle during initial warm-up (for example 3000).

Run several independent optimizers as a replay farm:

```powershell
.\.venv\Scripts\python.exe autotrader\replay_farm.py start --workers indexes_recent,mega_cap_recent,semis_recent,high_beta_recent
.\.venv\Scripts\python.exe autotrader\replay_farm.py start --workers-file autotrader\replay_workers.json --offline
.\.venv\Scripts\python.exe autotrader\replay_farm.py status
.\.venv\Scripts\python.exe autotrader\replay_farm.py stop --workers-file autotrader\replay_workers.json --workers all
.\.venv\Scripts\python.exe autotrader\replay_farm.py aggregate
```

`replay_farm start` uses offline mode by default (cache-only). Add `--no-offline` if you want workers to fetch missing bars.

You can also run the same bot set over custom datasets using a workers file:

```powershell
.\.venv\Scripts\python.exe autotrader\replay_farm.py start --workers-file autotrader\data\replay_workers.json --offline --stagger-seconds 45
```

If you want it fully hands-off, use the project launcher:

```powershell
powershell -ExecutionPolicy Bypass -File .\start_replay_farm.ps1
```

The launcher defaults to `--no-offline` so rolling windows keep learning without manual cache warm-ups. Add `-Offline` if you intentionally want cache-only runs.
If `DATA_DIR` is set, launcher defaults now automatically use `DATA_DIR/replay_farm` and `DATA_DIR/historical_cache`.

For continuous supervised uptime with auto-restart and live ratio snapshots, use:

```powershell
powershell -ExecutionPolicy Bypass -File .\start_replay_farm_supervisor.ps1
```

This supervisor launcher also defaults to `--no-offline`; pass `-Offline` for cache-only behavior.

The supervisor keeps workers running, checks status every `-HealthCheckSeconds` (default 60), and logs each cycle with latest `win_loss_ratio` / `win_rate` values from each worker’s `optimizer_win_loss_ratio.csv`.
Use it for long-running local operation.
It also runs farm aggregate checks every `-AggregateEveryCycles` cycles (default 5) and logs promotable count + best candidate quality using the same aggregate thresholds as `replay_farm.py aggregate`.
By default it writes aggregate snapshots to `replay_farm\snapshots`:
- `farm_summary_YYYY-MM-DD.json` (latest daily state)
- `farm_summary_history.csv` (append-only cycle history)

Render service note:
- `render_service.py` now includes a built-in replay-farm supervisor thread for 24/7 historical learning.
- Control it with `ENABLE_HISTORICAL_REPLAY_LEARNING` (default `true`) and `HISTORICAL_REPLAY_OFFLINE` (default `false`).
- Optional auto-promote is also built in: when aggregate replay evidence is promotable, the service can apply allowlisted scanner thresholds (`MIN_SIGNAL_SCORE`, `DIRECTION_CONVICTION_MIN`, `RVOL_MIN`, `ATR_PCT_MIN`) at runtime.
- Auto-promote controls: `ENABLE_REPLAY_AUTO_PROMOTE` (default `true`) and `REPLAY_AUTO_PROMOTE_PAPER_ONLY` (default `true`).
- Promotion decisions are logged to `DATA_DIR/replay_auto_promote_events.csv` for audit history.
- Read-only API: `GET /api/replay-auto-promote` returns current auto-promote status and recent audit events.

```powershell
powershell -ExecutionPolicy Bypass -File .\start_replay_farm_supervisor.ps1 -HealthCheckSeconds 120 -StaggerSeconds 45
```

Optional stricter ratchet mode (off by default): escalate aggregate ratio thresholds after repeated no-promotable cycles.

```powershell
powershell -ExecutionPolicy Bypass -File .\start_replay_farm_supervisor.ps1 -EscalateAfterNoPromotableCycles 12 -EscalateWinLossStep 0.05 -EscalateMaxWinLossRatio 2.0
```

Optional safety pause (off by default): stop all workers when new stderr error signatures keep appearing across cycles.

```powershell
powershell -ExecutionPolicy Bypass -File .\start_replay_farm_supervisor.ps1 -PauseAfterErrorCycles 3 -PauseIfWorkersWithNewErrors 1
```

To run one validation cycle and stop workers automatically:

```powershell
powershell -ExecutionPolicy Bypass -File .\start_replay_farm_supervisor.ps1 -OneShot
```

To stop workers started under the replay farm:

```powershell
.\.venv\Scripts\python.exe autotrader\replay_farm.py stop --output-root .\autotrader\replay_farm --workers-file .\autotrader\replay_workers.json --workers all
```

The launcher starts `replay_farm.py` in the background, writes logs under `autotrader\replay_farm\logs` (or the configured `--output-root`), and records per-run outcomes in each worker folder:
- `replay_optimizer\optimizer_runs.csv`
- `replay_optimizer\optimizer_win_loss_ratio.csv`
- `replay_optimizer\best_candidate.json`

`replay_workers.json` should be a JSON list (or object with `workers`) of spec objects:

```json
[
  {
    "name": "dataset_recent_momentum",
    "symbols": ["AAPL", "MSFT", "NVDA", "TSLA"],
    "interval": "5m",
    "start": "2026-03-01",
    "end": "2026-04-01",
    "window_days": 5,
    "step_days": 5,
    "max_windows": 4,
    "sleep_seconds": 900,
    "scan_bars": 15,
    "min_trades": 5,
    "min_win_loss_ratio": 1.25,
    "daily_lookback_days": 90
  }
]
```

`symbols` can alternatively be omitted and loaded from `symbols_file` (one symbol per line) for very large universes.

Common gotcha for offline mode:
if a symbol lacks cached bars, workers will fail fast and exit. Seed cache first (or remove `--offline`) with a warm-up run for each dataset if you expect immediate 24/7 operation.

Farm outputs land under `replay_farm` in the active data directory. Each worker keeps its own `optimizer_runs.csv`; the farm writes combined evidence to `farm_runs.csv` and `farm_leaderboard.json`. A candidate is only promotable at the farm level when it clears the configured thresholds across multiple datasets.
You can harden farm promotion with `--min-win-loss-ratio` (aggregate gate) and `--min-worker-win-loss-ratio` (per-worker gate).

Render note:
- Attach a persistent disk and set `DATA_DIR=/data` so runtime files survive restarts:
  - `trades.csv`
  - `scan_log.csv`
  - `runtime_state.json`
  - `trading_control.json`
