from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from autobott_v2.historical_live_sim import run_historical_live_simulation
from autobott_v2.phase1_snapshot_capture import write_snapshot_day_manifest


BASE_TIME = datetime(2026, 6, 1, 15, 30, tzinfo=timezone.utc)


def _bar(index: int, base_time: datetime, start: float = 200.0, step: float = 0.45) -> dict[str, object]:
    close = start + index * step
    return {
        "timestamp": (base_time - timedelta(minutes=34 - index)).isoformat(),
        "open": close - 0.05,
        "high": close + 0.25,
        "low": close - 0.25,
        "close": close,
        "volume": 1000 + index * 10,
    }


def _bars(base_time: datetime, start: float = 200.0, step: float = 0.45) -> list[dict[str, object]]:
    return [_bar(index, base_time, start, step) for index in range(35)]


def _snapshot(timestamp: datetime, tactical_bid: float = 4.9, tactical_ask: float = 5.1) -> dict[str, object]:
    return {
        "schema_version": "phase1.snapshot.v1",
        "source": {"name": "deterministic_fixture", "environment": "test", "latency_assumption": "retail_api_latency", "corpus_type": "historical_replay"},
        "captured_at": timestamp.isoformat(),
        "market_timezone": "America/New_York",
        "ticker": "AAPL",
        "timestamp": timestamp.isoformat(),
        "underlying_quote": {
            "symbol": "AAPL",
            "bid": 214.90,
            "ask": 215.10,
            "last": 215.00,
            "spread": 0.20,
            "spread_pct": 0.0009,
            "quote_timestamp": timestamp.isoformat(),
        },
        "market_bars": _bars(timestamp),
        "option_chain": [
            {
                "option_symbol": "AAPL260602C00215000",
                "underlying": "AAPL",
                "expiration": "2026-06-02",
                "strike": 215.0,
                "option_type": "call",
                "bid": tactical_bid,
                "ask": tactical_ask,
                "last": round((tactical_bid + tactical_ask) / 2, 4),
                "spread": round(tactical_ask - tactical_bid, 4),
                "spread_pct": round((tactical_ask - tactical_bid) / ((tactical_bid + tactical_ask) / 2), 4),
                "quote_timestamp": timestamp.isoformat(),
                "volume": 250,
                "open_interest": 900,
                "delta": 0.56,
                "theta": -0.04,
                "vega": 0.08,
                "implied_volatility": 0.01,
                "iv_percentile": 0.25,
                "realized_volatility": 0.01,
            },
            {
                "option_symbol": "AAPL260619C00215000",
                "underlying": "AAPL",
                "expiration": "2026-06-19",
                "strike": 215.0,
                "option_type": "call",
                "bid": tactical_bid,
                "ask": tactical_ask,
                "last": round((tactical_bid + tactical_ask) / 2, 4),
                "spread": round(tactical_ask - tactical_bid, 4),
                "spread_pct": round((tactical_ask - tactical_bid) / ((tactical_bid + tactical_ask) / 2), 4),
                "quote_timestamp": timestamp.isoformat(),
                "volume": 250,
                "open_interest": 1200,
                "delta": 0.48,
                "theta": -0.04,
                "vega": 0.08,
                "implied_volatility": 0.01,
                "iv_percentile": 0.25,
                "realized_volatility": 0.01,
            }
        ],
        "context": {
            "spy_bars": _bars(timestamp, 500.0, 0.20),
            "qqq_bars": _bars(timestamp, 430.0, 0.15),
            "vix_bars": _bars(timestamp, 16.0, -0.02),
            "blackout_event": False,
            "event_labels": [],
        },
        "iv_history": [0.01, 0.02, 0.03, 0.04],
        "cycle_profile": {"expected_holding_days": 6, "cycle_confidence": "high", "last_pivot_type": "unknown"},
    }


def test_historical_live_simulation_runs_and_closes_trade(tmp_path) -> None:
    symbol_dir = tmp_path / "corpus" / "2026-06-01" / "AAPL"
    snapshot_dir = symbol_dir / "snapshots"
    option_dir = symbol_dir / "option_quotes"
    snapshot_dir.mkdir(parents=True)
    option_dir.mkdir(parents=True)
    first = _snapshot(BASE_TIME)
    second = _snapshot(BASE_TIME + timedelta(minutes=20), tactical_bid=7.2, tactical_ask=7.4)
    (snapshot_dir / "153000.json").write_text(json.dumps(first), encoding="utf-8")
    (snapshot_dir / "155000.json").write_text(json.dumps(second), encoding="utf-8")
    (option_dir / "153000.json").write_text(json.dumps({"contracts": first["option_chain"]}), encoding="utf-8")
    (option_dir / "155000.json").write_text(json.dumps({"contracts": second["option_chain"]}), encoding="utf-8")
    write_snapshot_day_manifest(symbol_dir, trading_date="2026-06-01", symbol="AAPL", source="historical_synthesis", capture_interval_seconds=1200, corpus_type="historical_replay")

    result = run_historical_live_simulation(tmp_path / "corpus", artifacts_root=tmp_path / "artifacts", run_id="sim1")

    assert result["snapshots_processed"] == 2
    assert result["decisions_generated"] == 2
    assert result["orders_attempted"] >= 1
    assert result["closed_trades"] >= 1
    assert "tactical_2dte_pass_rate" in result["thesis_validation"]
    assert (tmp_path / "artifacts" / "sim1" / "simulation_summary.json").exists()
