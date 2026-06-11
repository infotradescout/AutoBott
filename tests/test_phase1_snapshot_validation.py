from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from autobott_v2.phase1_snapshot_contract import SnapshotValidationError, validate_market_snapshot
from autobott_v2.phase1_validate import main


BASE_TIME = datetime(2026, 6, 1, 15, 30, tzinfo=timezone.utc)


def _bar(index: int, start: float = 200.0, step: float = 0.45) -> dict[str, object]:
    close = start + index * step
    return {
        "timestamp": (BASE_TIME - timedelta(minutes=34 - index)).isoformat(),
        "open": close - 0.05,
        "high": close + 0.25,
        "low": close - 0.25,
        "close": close,
        "volume": 1000 + index * 10,
    }


def _bars(start: float = 200.0, step: float = 0.45) -> list[dict[str, object]]:
    return [_bar(index, start, step) for index in range(35)]


def _valid_snapshot() -> dict[str, object]:
    return {
        "schema_version": "phase1.snapshot.v1",
        "source": {
            "name": "deterministic_fixture",
            "environment": "test",
            "latency_assumption": "retail_api_latency",
        },
        "captured_at": BASE_TIME.isoformat(),
        "ticker": "AAPL",
        "timestamp": BASE_TIME.isoformat(),
        "underlying_quote": {
            "symbol": "AAPL",
            "bid": 214.90,
            "ask": 215.10,
            "last": 215.00,
            "spread": 0.20,
            "spread_pct": 0.0009,
            "quote_timestamp": BASE_TIME.isoformat(),
        },
        "market_bars": _bars(),
        "option_chain": [
            {
                "option_symbol": "AAPL260619C00215000",
                "underlying": "AAPL",
                "expiration": "2026-06-19",
                "strike": 215.0,
                "option_type": "call",
                "bid": 4.90,
                "ask": 5.10,
                "last": 5.0,
                "spread": 0.20,
                "spread_pct": 0.04,
                "quote_timestamp": BASE_TIME.isoformat(),
                "volume": 50,
                "open_interest": 500,
                "delta": 0.48,
                "theta": -0.04,
                "vega": 0.08,
                "implied_volatility": 0.25,
                "iv_percentile": 0.40,
                "realized_volatility": 0.20,
            }
        ],
        "context": {
            "spy_bars": _bars(500.0, 0.20),
            "qqq_bars": _bars(430.0, 0.15),
            "vix_bars": _bars(16.0, -0.02),
            "blackout_event": False,
            "event_labels": [],
        },
        "iv_history": [0.18, 0.20, 0.23, 0.27, 0.31],
    }


def test_valid_fixture_snapshot_passes_schema_validation() -> None:
    validate_market_snapshot(_valid_snapshot())


def test_missing_critical_snapshot_fields_fail_validation() -> None:
    snapshot = _valid_snapshot()
    del snapshot["underlying_quote"]
    del snapshot["context"]["spy_bars"]  # type: ignore[index]

    try:
        validate_market_snapshot(snapshot)
    except SnapshotValidationError as exc:
        message = str(exc)
    else:
        raise AssertionError("Expected snapshot validation to fail.")

    assert "$.underlying_quote: required field is missing" in message
    assert "$.context.spy_bars: required field is missing" in message


def test_phase1_validate_refuses_incomplete_snapshot_input(tmp_path, capsys) -> None:
    snapshot = _valid_snapshot()
    snapshot["market_bars"] = []
    snapshot_path = tmp_path / "bad_snapshot.json"
    snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")

    exit_code = main(["--snapshot", str(snapshot_path)])
    captured = capsys.readouterr()

    assert exit_code == 2
    assert "$.market_bars: expected at least 30 items" in captured.err


def test_phase1_validate_writes_ledger_with_empty_forward_outcomes(tmp_path) -> None:
    snapshot_path = tmp_path / "snapshot.json"
    ledger_path = tmp_path / "ledger.jsonl"
    snapshot_path.write_text(json.dumps(_valid_snapshot()), encoding="utf-8")

    exit_code = main(["--snapshot", str(snapshot_path), "--ledger", str(ledger_path)])
    row = json.loads(ledger_path.read_text(encoding="utf-8"))

    assert exit_code == 0
    assert row["validation_status"] == "SNAPSHOT_VALID"
    assert row["snapshot_path"] == str(snapshot_path)
    assert row["forward_outcomes"] == {
        "after_5m": None,
        "after_15m": None,
        "after_30m": None,
        "after_1h": None,
    }
    assert "order_id" not in json.dumps(row).lower()
