from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from autobott_v2.phase1_replay_campaign import run_replay_campaign


BASE_TIME = datetime(2026, 6, 1, 15, 20, tzinfo=timezone.utc)


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


def _snapshot(
    timestamp: datetime,
    *,
    tactical_bid: float = 4.9,
    tactical_ask: float = 5.1,
    rider_bid: float = 4.9,
    rider_ask: float = 5.1,
) -> dict[str, object]:
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
            "bid": 214.9,
            "ask": 215.1,
            "last": 215.0,
            "spread": 0.2,
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
            },
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
            "last_pivot_type": "unknown",
        },
    }


def _campaign_fixture(tmp_path):
    snapshots_dir = tmp_path / "snapshots"
    snapshots_dir.mkdir()
    (snapshots_dir / "001.json").write_text(json.dumps(_snapshot(BASE_TIME)), encoding="utf-8")
    (snapshots_dir / "002.json").write_text(
        json.dumps(_snapshot(BASE_TIME + timedelta(minutes=10), tactical_bid=6.67, tactical_ask=6.85, rider_bid=6.2, rider_ask=6.4)),
        encoding="utf-8",
    )
    (snapshots_dir / "003.json").write_text(
        json.dumps(_snapshot(BASE_TIME + timedelta(minutes=30), tactical_bid=6.6, tactical_ask=6.8, rider_bid=6.3, rider_ask=6.5)),
        encoding="utf-8",
    )
    return snapshots_dir


def test_slippage_scenarios_can_diverge_exit_timing(tmp_path) -> None:
    snapshots_dir = _campaign_fixture(tmp_path)

    run_replay_campaign(snapshots_dir, artifacts_root=tmp_path / "artifacts", campaign_run_id="campaign1")
    optimistic = [json.loads(line) for line in (tmp_path / "artifacts" / "campaign1" / "fill_model_results" / "optimistic_mid" / "outcomes.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
    conservative = [json.loads(line) for line in (tmp_path / "artifacts" / "campaign1" / "fill_model_results" / "conservative" / "outcomes.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
    optimistic_tactical = next(row for row in optimistic if row["leg_role"] == "tactical")
    conservative_tactical = next(row for row in conservative if row["leg_role"] == "tactical")

    assert optimistic_tactical["timestamp"] != conservative_tactical["timestamp"]


def test_campaign_summary_contains_all_fill_models(tmp_path) -> None:
    snapshots_dir = _campaign_fixture(tmp_path)

    run_replay_campaign(snapshots_dir, artifacts_root=tmp_path / "artifacts", campaign_run_id="campaign1")
    summary = (tmp_path / "artifacts" / "campaign1" / "replay_campaign_summary.md").read_text(encoding="utf-8")

    assert "optimistic_mid              diagnostic only" in summary
    assert "realistic_mid_penalty       primary eligibility model" in summary
    assert "conservative                robustness check" in summary
    assert "stress                      adverse robustness check" in summary
    assert "thesis_pass_rate=" in summary
    assert "tactical_2dte_pass_rate=" in summary


def test_gate_candidate_report_does_not_mutate_active_gate(tmp_path) -> None:
    snapshots_dir = _campaign_fixture(tmp_path)
    active_gate = tmp_path / "active_gate.json"
    active_gate.write_text(json.dumps({"sentinel": True}, sort_keys=True), encoding="utf-8")

    run_replay_campaign(snapshots_dir, artifacts_root=tmp_path / "artifacts", campaign_run_id="campaign1", active_gate_path=active_gate)

    assert json.loads(active_gate.read_text(encoding="utf-8")) == {"sentinel": True}


def test_same_campaign_inputs_produce_same_bucket_edge_report(tmp_path) -> None:
    snapshots_dir = _campaign_fixture(tmp_path)

    run_replay_campaign(snapshots_dir, artifacts_root=tmp_path / "artifacts", campaign_run_id="campaignA")
    run_replay_campaign(snapshots_dir, artifacts_root=tmp_path / "artifacts", campaign_run_id="campaignB")
    report_a = json.loads((tmp_path / "artifacts" / "campaignA" / "bucket_edge_report.json").read_text(encoding="utf-8"))
    report_b = json.loads((tmp_path / "artifacts" / "campaignB" / "bucket_edge_report.json").read_text(encoding="utf-8"))

    assert {key: value for key, value in report_a.items() if key != "campaign_run_id"} == {key: value for key, value in report_b.items() if key != "campaign_run_id"}


def test_campaign_manifest_includes_thesis_rollups(tmp_path) -> None:
    snapshots_dir = _campaign_fixture(tmp_path)

    run_replay_campaign(snapshots_dir, artifacts_root=tmp_path / "artifacts", campaign_run_id="campaign1")
    manifest = json.loads((tmp_path / "artifacts" / "campaign1" / "manifest.json").read_text(encoding="utf-8"))

    thesis = manifest["thesis_validation_by_fill_model"]
    assert "realistic_mid_penalty" in thesis
    assert "pass_rate" in thesis["realistic_mid_penalty"]


def test_replay_campaign_uses_env_artifacts_and_gate_paths(monkeypatch, tmp_path) -> None:
    snapshots_dir = _campaign_fixture(tmp_path)
    artifacts_root = tmp_path / "durable-artifacts"
    gate_path = tmp_path / "durable-data" / "PHASE1_CYCLE_GATE.json"
    gate_path.parent.mkdir(parents=True, exist_ok=True)
    gate_path.write_text(json.dumps({"sentinel": True}, sort_keys=True), encoding="utf-8")
    monkeypatch.setenv("AUTOBOTT_ARTIFACTS_ROOT", str(artifacts_root))
    monkeypatch.setenv("AUTOBOTT_GATE_PATH", str(gate_path))

    result = run_replay_campaign(snapshots_dir, campaign_run_id="campaign1")

    campaign_dir = artifacts_root / "phase1_replay_campaign" / "campaign1"
    assert campaign_dir.exists()
    assert Path(result["artifact_dir"]) == campaign_dir
    assert json.loads(gate_path.read_text(encoding="utf-8")) == {"sentinel": True}
