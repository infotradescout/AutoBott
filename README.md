# AutoBott v2

AutoBott v2 is being built as an automated options trading system for long calls and long puts.

The product is the trading bot itself: signal generation, risk controls, broker execution, position management,
operator oversight, and auditability. Research, replay, backtests, and historical analysis are crucial support
systems, but they are not the product.

## Product Identity

AutoBott has five major layers:

- `signal`: regime, direction, volatility, and contract selection
- `risk`: trade eligibility, sizing, exposure limits, and kill switches
- `execution`: broker routing, order lifecycle, retries, and cancel/replace behavior
- `positions`: open-position monitoring and exits
- `operator`: dashboard, health, alerts, and administrative controls

Supporting systems exist to improve and audit the bot:

- `capture`: real market snapshot collection
- `replay`: deterministic campaign evaluation
- `history`: historical corpus generation and backtest-style analysis
- `scorecards`: edge review, drift review, and gate reporting

Today the repository is still weighted toward the support layers. The execution product is not complete yet.

## Current State

The committed runtime currently provides:

- a decision-engine foundation
- paper-market capture and status plumbing
- replay, scorecard, and campaign analysis
- a Render-hosted operator console
- no committed live order-routing path yet

That means the repo contains important building blocks, but it is not yet the complete automated trading system
described above.

## Validation

Local paper dashboard, no PowerShell setup:

```text
Double-click start_paper_dashboard.cmd
```

That launcher auto-loads `C:\Users\flavo\Downloads\AutoBott.env`, applies the local paper defaults, starts the dashboard on `http://127.0.0.1:8000`, and uses dashboard token `autobott-local` unless you override it in the env file.

Run the test suite:

```powershell
pytest
```

Validate a captured market/options snapshot and optionally append the decision card to a JSONL ledger:

```powershell
.\.venv\Scripts\python.exe -m autobott_v2.phase1_validate --snapshot .\path\to\real_snapshot.json --ledger .\data\learning_ledger.jsonl
```

The validator expects real captured inputs: market bars, option-chain quotes, SPY/QQQ/VIX context, event blackout
flags, and IV history. It does not synthesize market data.

## Architecture Docs

Read:

- docs/DOCTRINE.md
- docs/AUTOBOTT_V2_PURPOSE_LOCK.md
- docs/BUILD_PLAN.md
- docs/REPO_LANES.md
