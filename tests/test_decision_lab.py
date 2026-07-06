from __future__ import annotations

import json
from pathlib import Path

from autobott_v2.decision_lab import build_decision_lab_report


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows), encoding="utf-8")


def _contract(option_type: str, *, mid: float, delta: float, theta: float, iv: float, spread_pct: float = 0.08) -> dict:
    return {
        "option_symbol": f"TEST260717{option_type[0].upper()}00100000",
        "option_type": option_type,
        "expiration": "2026-07-17",
        "strike": 100.0,
        "bid": round(mid * (1 - spread_pct / 2), 2),
        "ask": round(mid * (1 + spread_pct / 2), 2),
        "mid": mid,
        "spread_pct": spread_pct,
        "open_interest": 500,
        "volume": 100,
        "delta": delta,
        "theta": theta,
        "vega": 0.08,
        "implied_volatility": iv,
    }


def test_decision_lab_report_scores_baselines_and_buckets(tmp_path) -> None:
    campaign = tmp_path / "campaign1"
    primary = campaign / "fill_model_results" / "realistic_mid_penalty"
    call_contract = _contract("call", mid=2.5, delta=0.52, theta=-0.04, iv=0.35)
    put_contract = _contract("put", mid=5.5, delta=-0.55, theta=-0.18, iv=0.62, spread_pct=0.14)
    decisions = [
        {
            "decision_id": "call-win",
            "ticker": "GOOGL",
            "timestamp": "2026-07-06T15:00:00+00:00",
            "decision": "TRADE_CANDIDATE",
            "trade_setup": "bullish_continuation",
            "execution_layer": "tactical",
            "selected_contract": call_contract,
        },
        {
            "decision_id": "put-loss",
            "ticker": "QQQ",
            "timestamp": "2026-07-06T15:00:00+00:00",
            "decision": "TRADE_CANDIDATE",
            "trade_setup": "bearish_continuation",
            "execution_layer": "tactical",
            "selected_contract": put_contract,
        },
    ]
    outcomes = [
        {
            "decision_id": "call-win",
            "ticker": "GOOGL",
            "trade_setup": "bullish_continuation",
            "execution_layer": "tactical",
            "selected_contract": call_contract,
            "lifecycle_status": "closed",
            "entry_fill_price": 2.5,
            "exit_fill_price": 3.25,
            "pnl": 75.0,
            "exit_reason": "profit_target",
        },
        {
            "decision_id": "put-loss",
            "ticker": "QQQ",
            "trade_setup": "bearish_continuation",
            "execution_layer": "tactical",
            "selected_contract": put_contract,
            "lifecycle_status": "closed",
            "entry_fill_price": 5.5,
            "exit_fill_price": 4.0,
            "pnl": -150.0,
            "exit_reason": "stop_loss",
        },
    ]
    thesis = [
        {"decision_id": "call-win", "contract_dte_days": 11, "passed": True, "directional_match": True, "favorable_move_pct": 0.02, "adverse_move_pct": -0.004},
        {"decision_id": "put-loss", "contract_dte_days": 11, "passed": False, "directional_match": False, "favorable_move_pct": 0.001, "adverse_move_pct": -0.025},
    ]
    _write_jsonl(primary / "decisions.jsonl", decisions)
    _write_jsonl(primary / "orders.jsonl", outcomes)
    _write_jsonl(primary / "outcomes.jsonl", outcomes)
    _write_jsonl(primary / "thesis_validation.jsonl", thesis)
    (primary / "scorecard.json").write_text(
        json.dumps(
            {
                "win_rate": 0.5,
                "profit_factor": 0.5,
                "expectancy_per_trade": -37.5,
                "max_drawdown_pct_observed": 1.5,
                "thesis_validation": {"pass_rate": 0.5},
            }
        ),
        encoding="utf-8",
    )
    (campaign / "bucket_edge_report.json").write_text(json.dumps({"campaign_quality": {"campaign_valid": True}}), encoding="utf-8")

    report = build_decision_lab_report(campaign)

    assert report["summary"]["closed_trades"] == 2
    assert report["baselines"]["actual_strategy"]["net_pnl"] == -75.0
    assert report["baselines"]["actual_vs_no_trade"] == -75.0
    assert {"action": "do_not_scale", "severity": "danger", "reason": "actual strategy has not beaten no-trade baseline"} in report["recommendations"]
    call_bucket = next(bucket for bucket in report["buckets"] if bucket["bucket"] == "type:call")
    put_bucket = next(bucket for bucket in report["buckets"] if bucket["bucket"] == "type:put")
    assert call_bucket["expectancy"] == 75.0
    assert put_bucket["expectancy"] == -150.0
