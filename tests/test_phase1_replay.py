from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from autobott_v2.phase1_replay import run_replay
from autobott_v2.phase1_slippage_sweep import run_slippage_sweep


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


def _snapshot(timestamp: datetime, tactical_bid: float = 4.9, tactical_ask: float = 5.1, rider_bid: float = 4.9, rider_ask: float = 5.1) -> dict[str, object]:
    return {
        "schema_version": "phase1.snapshot.v1",
        "source": {
            "name": "deterministic_fixture",
            "environment": "test",
            "latency_assumption": "retail_api_latency",
        },
        "captured_at": timestamp.isoformat(),
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
                "bid": rider_bid,
                "ask": rider_ask,
                "last": round((rider_bid + rider_ask) / 2, 4),
                "spread": round(rider_ask - rider_bid, 4),
                "spread_pct": round((rider_ask - rider_bid) / ((rider_bid + rider_ask) / 2), 4),
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
        "cycle_profile": {
            "expected_holding_days": 6,
            "cycle_confidence": "high",
            "last_pivot_type": "unknown"
        },
    }


def test_replay_writes_decisions_orders_fills_outcomes(tmp_path) -> None:
    snapshots_dir = tmp_path / "snapshots"
    snapshots_dir.mkdir()
    (snapshots_dir / "001.json").write_text(json.dumps(_snapshot(BASE_TIME)), encoding="utf-8")
    (snapshots_dir / "002.json").write_text(json.dumps(_snapshot(BASE_TIME + timedelta(minutes=20), tactical_bid=7.2, tactical_ask=7.4, rider_bid=8.5, rider_ask=8.7)), encoding="utf-8")

    result = run_replay(snapshots_dir, artifacts_root=tmp_path / "artifacts", run_id="run1")
    artifact_dir = tmp_path / "artifacts" / "run1"

    assert result["snapshots_processed"] == 2
    assert (artifact_dir / "decisions.jsonl").exists()
    assert (artifact_dir / "orders.jsonl").exists()
    assert (artifact_dir / "fills.jsonl").exists()
    assert (artifact_dir / "outcomes.jsonl").exists()
    assert (artifact_dir / "summary.md").exists()
    assert (artifact_dir / "manifest.json").exists()


def test_replay_run_is_deterministic(tmp_path) -> None:
    snapshots_dir = tmp_path / "snapshots"
    snapshots_dir.mkdir()
    (snapshots_dir / "001.json").write_text(json.dumps(_snapshot(BASE_TIME)), encoding="utf-8")
    (snapshots_dir / "002.json").write_text(json.dumps(_snapshot(BASE_TIME + timedelta(minutes=20), tactical_bid=7.2, tactical_ask=7.4, rider_bid=8.5, rider_ask=8.7)), encoding="utf-8")

    first = run_replay(snapshots_dir, artifacts_root=tmp_path / "artifacts", run_id="runA")
    second = run_replay(snapshots_dir, artifacts_root=tmp_path / "artifacts", run_id="runB")

    first_summary = (tmp_path / "artifacts" / "runA" / "summary.md").read_text(encoding="utf-8")
    second_summary = (tmp_path / "artifacts" / "runB" / "summary.md").read_text(encoding="utf-8")
    assert {key: value for key, value in first.items() if key not in {"artifact_dir", "run_id"}} == {key: value for key, value in second.items() if key not in {"artifact_dir", "run_id"}}
    assert first_summary.replace("runA", "runX") == second_summary.replace("runB", "runX")


def test_replay_does_not_mutate_active_gate_by_default(tmp_path) -> None:
    snapshots_dir = tmp_path / "snapshots"
    snapshots_dir.mkdir()
    (snapshots_dir / "001.json").write_text(json.dumps(_snapshot(BASE_TIME)), encoding="utf-8")
    (snapshots_dir / "002.json").write_text(json.dumps(_snapshot(BASE_TIME + timedelta(minutes=20), tactical_bid=7.2, tactical_ask=7.4, rider_bid=8.5, rider_ask=8.7)), encoding="utf-8")
    active_gate = tmp_path / "active_gate.json"
    active_gate.write_text(json.dumps({"sentinel": True}, sort_keys=True), encoding="utf-8")

    run_replay(snapshots_dir, artifacts_root=tmp_path / "artifacts", run_id="run1", active_gate_path=active_gate)

    assert json.loads(active_gate.read_text(encoding="utf-8")) == {"sentinel": True}


def test_replay_manifest_is_written_and_config_hash_is_stable(tmp_path) -> None:
    snapshots_dir = tmp_path / "snapshots"
    snapshots_dir.mkdir()
    (snapshots_dir / "001.json").write_text(json.dumps(_snapshot(BASE_TIME)), encoding="utf-8")
    (snapshots_dir / "002.json").write_text(json.dumps(_snapshot(BASE_TIME + timedelta(minutes=20), tactical_bid=7.2, tactical_ask=7.4, rider_bid=8.5, rider_ask=8.7)), encoding="utf-8")

    run_replay(snapshots_dir, artifacts_root=tmp_path / "artifacts", run_id="runA")
    run_replay(snapshots_dir, artifacts_root=tmp_path / "artifacts", run_id="runB")
    manifest_a = json.loads((tmp_path / "artifacts" / "runA" / "manifest.json").read_text(encoding="utf-8"))
    manifest_b = json.loads((tmp_path / "artifacts" / "runB" / "manifest.json").read_text(encoding="utf-8"))

    assert manifest_a["replay_config_hash"] == manifest_b["replay_config_hash"]
    assert manifest_a["input_snapshot_hash"] == manifest_b["input_snapshot_hash"]


def test_missing_exit_quote_creates_unresolved_position_and_does_not_count_as_closed_trade(tmp_path) -> None:
    snapshots_dir = tmp_path / "snapshots"
    snapshots_dir.mkdir()
    (snapshots_dir / "001.json").write_text(json.dumps(_snapshot(BASE_TIME)), encoding="utf-8")
    broken = _snapshot(BASE_TIME + timedelta(minutes=20), tactical_bid=7.2, tactical_ask=7.4, rider_bid=8.5, rider_ask=8.7)
    broken["option_chain"] = [
        {
            **broken["option_chain"][0],
            "option_symbol": "OTHER260602C00215000",
        },
        {
            **broken["option_chain"][1],
            "option_symbol": "OTHER260619C00215000",
        },
    ]
    (snapshots_dir / "002.json").write_text(json.dumps(broken), encoding="utf-8")

    run_replay(snapshots_dir, artifacts_root=tmp_path / "artifacts", run_id="run1")
    gate = json.loads((tmp_path / "artifacts" / "run1" / "gate.json").read_text(encoding="utf-8"))

    assert gate["position_stats"]["positions_unresolved"] > 0
    assert gate["trade_stats"]["closed_trades"] == 0


def test_slippage_sweep_writes_base_realistic_conservative_results(tmp_path) -> None:
    snapshots_dir = tmp_path / "snapshots"
    snapshots_dir.mkdir()
    (snapshots_dir / "001.json").write_text(json.dumps(_snapshot(BASE_TIME)), encoding="utf-8")
    (snapshots_dir / "002.json").write_text(json.dumps(_snapshot(BASE_TIME + timedelta(minutes=20), tactical_bid=7.2, tactical_ask=7.4, rider_bid=8.5, rider_ask=8.7)), encoding="utf-8")

    report = run_slippage_sweep(snapshots_dir, artifacts_root=tmp_path / "artifacts", run_id="sweep1")

    assert set(report["results"]) == {"optimistic_mid", "realistic_mid_penalty", "conservative", "stress"}
    assert (tmp_path / "artifacts" / "sweep1" / "slippage_sweep.json").exists()
