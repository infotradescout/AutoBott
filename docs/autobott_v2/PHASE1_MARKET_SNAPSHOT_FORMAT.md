# Phase 1 Market Snapshot Format

AutoBott Phase 1 validates one real, captured market snapshot before it builds a read-only options decision card.

The contract lives at `schemas/phase1_market_snapshot.schema.json`. The validator does not fetch market data, fill missing fields, or synthesize defaults. A snapshot file must be supplied by the user or by a future read-only data capture process.

## Required Sections

- `schema_version`: Version label for the snapshot contract.
- `source`: Where the snapshot came from, the environment, and the latency assumption used when it was captured.
- `captured_at`: When the snapshot file was captured.
- `market_timezone` optional: The market-session timezone used for normalized capture timestamps.
- `timestamp_utc` optional: The normalized UTC decision timestamp used for deterministic sorting.
- `timestamp_market` optional: The same decision timestamp expressed in market session time.
- `ticker`: Underlying symbol being evaluated.
- `timestamp`: Decision timestamp for the snapshot.
- `underlying_quote`: Current underlying bid, ask, last, spread, spread percent, and quote timestamp.
- `market_bars`: At least 30 underlying OHLCV bars for regime and direction scoring.
- `option_chain`: One or more option contract quotes with expiration, strike, bid/ask, spread, volume, open interest, greeks, IV, IV percentile, and realized-volatility context.
- `context`: SPY, QQQ, and VIX bars plus event blackout flags.
- `iv_history`: Historical IV observations used for percentile scoring.
- `cycle_profile` optional: Median valley-to-peak and peak-to-valley timing, bars since the last turning points, and expected holding days for rider DTE targeting.
  Include `cycle_confidence` and `last_pivot_type` when available so reversal setups can be audited explicitly.

## Deterministic Fixture Example Only

The JSON below is a deterministic fixture shape for tests and documentation. It is not runtime market data and must not be treated as authoritative production input.

```json
{
  "schema_version": "phase1.snapshot.v1",
  "source": {
    "name": "deterministic_fixture",
    "environment": "test",
    "latency_assumption": "retail_api_latency"
  },
  "captured_at": "2026-06-01T15:30:00+00:00",
  "market_timezone": "America/New_York",
  "timestamp_utc": "2026-06-01T15:30:00Z",
  "timestamp_market": "2026-06-01T11:30:00-04:00",
  "ticker": "AAPL",
  "timestamp": "2026-06-01T15:30:00+00:00",
  "underlying_quote": {
    "symbol": "AAPL",
    "bid": 214.9,
    "ask": 215.1,
    "last": 215.0,
    "spread": 0.2,
    "spread_pct": 0.0009,
    "quote_timestamp": "2026-06-01T15:30:00+00:00"
  },
  "market_bars": [
    {
      "timestamp": "2026-06-01T14:56:00+00:00",
      "open": 199.95,
      "high": 200.25,
      "low": 199.75,
      "close": 200.0,
      "volume": 1000
    }
  ],
  "option_chain": [
    {
      "option_symbol": "AAPL260619C00215000",
      "underlying": "AAPL",
      "expiration": "2026-06-19",
      "strike": 215.0,
      "option_type": "call",
      "bid": 4.9,
      "ask": 5.1,
      "last": 5.0,
      "spread": 0.2,
      "spread_pct": 0.04,
      "quote_timestamp": "2026-06-01T15:30:00+00:00",
      "volume": 50,
      "open_interest": 500,
      "delta": 0.48,
      "theta": -0.04,
      "vega": 0.08,
      "implied_volatility": 0.25,
      "iv_percentile": 0.4,
      "realized_volatility": 0.2
    }
  ],
  "context": {
    "spy_bars": [
      {
        "timestamp": "2026-06-01T15:30:00+00:00",
        "open": 500.0,
        "high": 500.5,
        "low": 499.5,
        "close": 500.2,
        "volume": 1000
      }
    ],
    "qqq_bars": [
      {
        "timestamp": "2026-06-01T15:30:00+00:00",
        "open": 430.0,
        "high": 430.5,
        "low": 429.5,
        "close": 430.2,
        "volume": 1000
      }
    ],
    "vix_bars": [
      {
        "timestamp": "2026-06-01T15:30:00+00:00",
        "open": 16.0,
        "high": 16.1,
        "low": 15.8,
        "close": 15.9,
        "volume": 0
      }
    ],
    "blackout_event": false,
    "event_labels": []
  },
  "iv_history": [0.18, 0.2, 0.23, 0.27, 0.31],
  "cycle_profile": {
    "median_valley_to_peak_bars": 24,
    "median_peak_to_valley_bars": 18,
    "bars_since_last_valley": 9,
    "bars_since_last_peak": 0,
    "expected_holding_days": 4,
    "cycle_confidence": "medium",
    "last_pivot_type": "valley"
  }
}
```

Real validation requires full arrays, including at least 30 `market_bars`. The abbreviated arrays above are shown only to explain field shape.

## Validation Command

The validator first enforces the snapshot contract, then converts the payload into a Phase 1 `DecisionInput`, runs the read-only decision engine, and prints a JSON evaluation record. If `--ledger` is provided, that same record is appended to the JSONL ledger with the decision-card fields plus the snapshot metadata.

```powershell
.\.venv\Scripts\python.exe -m autobott_v2.phase1_validate --snapshot .\path\to\real_snapshot.json --ledger .\data\learning_ledger.jsonl
```
