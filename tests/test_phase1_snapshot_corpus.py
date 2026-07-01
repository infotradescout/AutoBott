from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from autobott_v2.phase1_snapshot_capture import write_snapshot_day_manifest
from autobott_v2.phase1_snapshot_corpus import load_snapshot_corpus


BASE_TIME = datetime(2026, 6, 29, 15, 30, tzinfo=timezone.utc)


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


def _snapshot(timestamp: datetime, *, ticker: str = "SPY", quote_age_seconds: int = 0) -> dict[str, object]:
    quote_timestamp = (timestamp - timedelta(seconds=quote_age_seconds)).isoformat()
    return {
        "schema_version": "phase1.snapshot.v1",
        "source": {
            "name": "deterministic_fixture",
            "environment": "test",
            "latency_assumption": "retail_api_latency",
        },
        "captured_at": timestamp.isoformat(),
        "ticker": ticker,
        "timestamp": timestamp.isoformat(),
        "underlying_quote": {
            "symbol": ticker,
            "bid": 599.9,
            "ask": 600.1,
            "last": 600.0,
            "spread": 0.2,
            "spread_pct": 0.0003,
            "quote_timestamp": timestamp.isoformat(),
        },
        "market_bars": _bars(timestamp, 600.0, 0.25),
        "option_chain": [
            {
                "option_symbol": f"{ticker}260701C00600000",
                "underlying": ticker,
                "expiration": "2026-07-01",
                "strike": 600.0,
                "option_type": "call",
                "bid": 4.9,
                "ask": 5.1,
                "last": 5.0,
                "spread": 0.2,
                "spread_pct": 0.04,
                "quote_timestamp": quote_timestamp,
                "volume": 250,
                "open_interest": 900,
                "delta": 0.56,
                "theta": -0.04,
                "vega": 0.08,
                "implied_volatility": 0.01,
                "iv_percentile": 0.25,
                "realized_volatility": 0.01,
            }
        ],
        "context": {
            "spy_bars": _bars(timestamp, 600.0, 0.20),
            "qqq_bars": _bars(timestamp, 520.0, 0.15),
            "vix_bars": _bars(timestamp, 16.0, -0.02),
            "blackout_event": False,
            "event_labels": [],
        },
        "iv_history": [0.01, 0.02, 0.03, 0.04],
        "cycle_profile": {
            "expected_holding_days": 6,
            "cycle_confidence": "high",
            "last_pivot_type": "unknown",
        },
    }


def _write_day(
    root,
    *,
    trading_date: str,
    symbol: str = "SPY",
    timestamps: list[datetime] | None = None,
    option_quote_count: int | None = None,
    corpus_type: str = "test_fixture",
    data_quality_flags: list[str] | None = None,
    quote_age_seconds: int = 0,
):
    symbol_dir = root / trading_date / symbol
    snapshots_dir = symbol_dir / "snapshots"
    option_quotes_dir = symbol_dir / "option_quotes"
    snapshots_dir.mkdir(parents=True)
    option_quotes_dir.mkdir(parents=True)
    day_timestamps = timestamps or [BASE_TIME, BASE_TIME + timedelta(minutes=1)]
    for timestamp in day_timestamps:
        (snapshots_dir / f"{timestamp.strftime('%H%M%S')}.json").write_text(
            json.dumps(_snapshot(timestamp, ticker=symbol, quote_age_seconds=quote_age_seconds)),
            encoding="utf-8",
        )
    total_option_quotes = len(day_timestamps) if option_quote_count is None else option_quote_count
    for index, timestamp in enumerate(day_timestamps[:total_option_quotes]):
        (option_quotes_dir / f"{timestamp.strftime('%H%M%S')}.json").write_text(json.dumps({"captured_at": timestamp.isoformat()}), encoding="utf-8")
    return write_snapshot_day_manifest(
        symbol_dir,
        trading_date=trading_date,
        symbol=symbol,
        source="alpaca",
        capture_interval_seconds=60,
        corpus_type=corpus_type,
        data_quality_flags=data_quality_flags,
    )


def test_snapshot_corpus_manifest_written(tmp_path) -> None:
    manifest = _write_day(tmp_path, trading_date="2026-06-29")
    manifest_path = tmp_path / "2026-06-29" / "SPY" / "manifest.json"

    assert manifest_path.exists()
    assert manifest["schema_version"] == "phase1_snapshot_day_manifest.v1"
    assert manifest["snapshots_captured"] == 2
    assert manifest["option_quotes_captured"] == 2


def test_snapshot_corpus_rejects_mixed_schema_versions(tmp_path) -> None:
    _write_day(tmp_path, trading_date="2026-06-29", symbol="SPY")
    second = _write_day(tmp_path, trading_date="2026-06-30", symbol="QQQ")
    second_path = tmp_path / "2026-06-30" / "QQQ" / "manifest.json"
    second["snapshot_schema_version"] = "phase1.snapshot.v2"
    second_path.write_text(json.dumps(second, indent=2, sort_keys=True), encoding="utf-8")

    with pytest.raises(ValueError, match="mixed_schema_versions"):
        load_snapshot_corpus(tmp_path)


def test_snapshot_corpus_detects_missing_intervals(tmp_path) -> None:
    manifest = _write_day(
        tmp_path,
        trading_date="2026-06-29",
        timestamps=[BASE_TIME, BASE_TIME + timedelta(minutes=2)],
    )
    summary = load_snapshot_corpus(tmp_path)

    assert manifest["missing_intervals"] == ["11:31:00"]
    assert summary["quality"]["missing_intervals"] == ["2026-06-29/15:31:00"]
    assert "missing_intervals_detected" in summary["quality"]["data_quality_flags"]
