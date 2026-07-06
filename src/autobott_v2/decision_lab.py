from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any


PRIMARY_FILL_MODEL = "realistic_mid_penalty"


def build_decision_lab_report(campaign_dir: str | Path, *, primary_fill_model: str = PRIMARY_FILL_MODEL) -> dict[str, Any]:
    campaign_path = Path(campaign_dir)
    primary_dir = campaign_path / "fill_model_results" / primary_fill_model
    decisions = _read_jsonl(primary_dir / "decisions.jsonl")
    orders = _read_jsonl(primary_dir / "orders.jsonl")
    outcomes = _read_jsonl(primary_dir / "outcomes.jsonl")
    thesis_rows = _read_jsonl(primary_dir / "thesis_validation.jsonl")
    scorecard = _read_json(primary_dir / "scorecard.json")
    edge_report = _read_json(campaign_path / "bucket_edge_report.json")

    decision_map = {str(row.get("decision_id")): row for row in decisions if row.get("decision_id")}
    thesis_map = {str(row.get("decision_id")): row for row in thesis_rows if row.get("decision_id")}
    closed_outcomes = [row for row in outcomes if row.get("lifecycle_status") == "closed" and row.get("pnl") is not None]

    evaluated_rows = [_decision_lab_row(row, decision_map, thesis_map) for row in closed_outcomes]
    candidate_decisions = [row for row in decisions if row.get("decision") == "TRADE_CANDIDATE"]
    baselines = _baseline_summary(evaluated_rows, candidate_decisions, thesis_rows)
    buckets = _bucket_summaries(evaluated_rows)
    recommendations = _recommendations(buckets, baselines, edge_report)

    return {
        "ok": True,
        "schema_version": "decision_lab_report.v1",
        "campaign_dir": str(campaign_path),
        "primary_fill_model": primary_fill_model,
        "summary": {
            "decisions": len(decisions),
            "trade_candidates": len(candidate_decisions),
            "orders_attempted": len(orders),
            "closed_trades": len(closed_outcomes),
            "win_rate": scorecard.get("win_rate", 0.0),
            "profit_factor": scorecard.get("profit_factor", 0.0),
            "expectancy_per_trade": scorecard.get("expectancy_per_trade", 0.0),
            "max_drawdown_pct": scorecard.get("max_drawdown_pct_observed", 0.0),
            "thesis_pass_rate": scorecard.get("thesis_validation", {}).get("pass_rate", 0.0),
        },
        "baselines": baselines,
        "buckets": buckets,
        "recommendations": recommendations,
    }


def _decision_lab_row(outcome: dict[str, Any], decision_map: dict[str, dict[str, Any]], thesis_map: dict[str, dict[str, Any]]) -> dict[str, Any]:
    decision_id = str(outcome.get("parent_decision_id") or outcome.get("decision_id") or "")
    decision = decision_map.get(decision_id, {})
    selected = decision.get("selected_contract") or outcome.get("selected_contract") or {}
    thesis = thesis_map.get(decision_id, {})
    dte = thesis.get("contract_dte_days")
    if dte is None:
        dte = _dte_from_snapshot(selected, decision)
    pnl = float(outcome.get("pnl") or 0.0)
    entry = _float_or_none(outcome.get("entry_fill_price"))
    exit_price = _float_or_none(outcome.get("exit_fill_price"))
    option_return = (exit_price - entry) / entry if entry and exit_price is not None else outcome.get("option_return_pct")
    return {
        "decision_id": decision_id,
        "ticker": outcome.get("ticker") or decision.get("ticker"),
        "trade_setup": outcome.get("trade_setup") or decision.get("trade_setup"),
        "execution_layer": outcome.get("execution_layer") or decision.get("execution_layer"),
        "option_type": selected.get("option_type"),
        "dte": dte,
        "price_band": _price_band(entry or selected.get("mid")),
        "delta_band": _delta_band(_float_or_none(selected.get("delta"))),
        "theta_band": _theta_band(_float_or_none(selected.get("theta"))),
        "iv_band": _iv_band(_float_or_none(selected.get("implied_volatility"))),
        "spread_band": _spread_band(_float_or_none(selected.get("spread_pct"))),
        "pnl": round(pnl, 4),
        "option_return_pct": round(float(option_return), 4) if option_return is not None else None,
        "thesis_passed": bool(thesis.get("passed")),
        "directional_match": bool(thesis.get("directional_match")),
        "favorable_move_pct": thesis.get("favorable_move_pct"),
        "adverse_move_pct": thesis.get("adverse_move_pct"),
        "exit_reason": outcome.get("exit_reason"),
    }


def _baseline_summary(rows: list[dict[str, Any]], candidate_decisions: list[dict[str, Any]], thesis_rows: list[dict[str, Any]]) -> dict[str, Any]:
    no_trade = {"label": "no_trade", "net_pnl": 0.0, "expectancy": 0.0}
    actual = _metrics(rows)
    thesis_passes = [row for row in thesis_rows if row.get("passed")]
    thesis_baseline = {
        "label": "directional_followthrough",
        "evaluated": len(thesis_rows),
        "pass_rate": round(len(thesis_passes) / len(thesis_rows), 4) if thesis_rows else 0.0,
    }
    candidate_mix = _count_by(candidate_decisions, lambda row: str((row.get("selected_contract") or {}).get("option_type") or "unknown"))
    return {
        "actual_strategy": actual,
        "no_trade": no_trade,
        "directional_thesis": thesis_baseline,
        "candidate_option_type_mix": candidate_mix,
        "actual_vs_no_trade": round(actual["net_pnl"] - no_trade["net_pnl"], 4),
    }


def _bucket_summaries(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    bucketed: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        for prefix, value in (
            ("type", row.get("option_type")),
            ("ticker", row.get("ticker")),
            ("dte", _dte_band(row.get("dte"))),
            ("price", row.get("price_band")),
            ("delta", row.get("delta_band")),
            ("theta", row.get("theta_band")),
            ("iv", row.get("iv_band")),
            ("spread", row.get("spread_band")),
            ("setup", row.get("trade_setup")),
        ):
            bucketed[f"{prefix}:{value or 'unknown'}"].append(row)
    summaries = []
    for bucket, bucket_rows in bucketed.items():
        metrics = _metrics(bucket_rows)
        summaries.append(
            {
                "bucket": bucket,
                **metrics,
                "thesis_pass_rate": _rate(bucket_rows, "thesis_passed"),
                "directional_match_rate": _rate(bucket_rows, "directional_match"),
                "status": _bucket_status(metrics),
            }
        )
    return sorted(summaries, key=lambda row: (row["status"] != "approved", -row["closed_trades"], -row["expectancy"]))[:40]


def _recommendations(buckets: list[dict[str, Any]], baselines: dict[str, Any], edge_report: dict[str, Any]) -> list[dict[str, Any]]:
    recommendations = []
    actual = baselines["actual_strategy"]
    if actual["closed_trades"] < 50:
        recommendations.append(
            {
                "action": "collect_more_history",
                "severity": "warn",
                "reason": f"closed_trades={actual['closed_trades']} below 50-trade minimum for strategy judgment",
            }
        )
    if actual["net_pnl"] <= baselines["no_trade"]["net_pnl"]:
        recommendations.append(
            {
                "action": "do_not_scale",
                "severity": "danger",
                "reason": "actual strategy has not beaten no-trade baseline",
            }
        )
    blocked = [bucket for bucket in buckets if bucket["status"] == "underperforming"][:5]
    for bucket in blocked:
        recommendations.append(
            {
                "action": "downgrade_bucket",
                "severity": "danger",
                "bucket": bucket["bucket"],
                "reason": f"expectancy={bucket['expectancy']} profit_factor={bucket['profit_factor']}",
            }
        )
    approved = [bucket for bucket in buckets if bucket["status"] == "approved"][:5]
    for bucket in approved:
        recommendations.append(
            {
                "action": "watchlist_bucket",
                "severity": "safe",
                "bucket": bucket["bucket"],
                "reason": f"closed={bucket['closed_trades']} expectancy={bucket['expectancy']} profit_factor={bucket['profit_factor']}",
            }
        )
    if edge_report.get("campaign_quality", {}).get("campaign_valid") is False:
        recommendations.append(
            {
                "action": "fix_corpus_quality",
                "severity": "warn",
                "reason": "latest replay campaign marked corpus/campaign quality invalid",
            }
        )
    return recommendations


def _metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    pnls = [float(row.get("pnl") or 0.0) for row in rows]
    wins = [pnl for pnl in pnls if pnl > 0]
    losses = [pnl for pnl in pnls if pnl < 0]
    net = round(sum(pnls), 4)
    return {
        "closed_trades": len(rows),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": round(len(wins) / len(rows), 4) if rows else 0.0,
        "net_pnl": net,
        "expectancy": round(net / len(rows), 4) if rows else 0.0,
        "profit_factor": round(sum(wins) / abs(sum(losses)), 4) if losses else (float("inf") if wins else 0.0),
    }


def _bucket_status(metrics: dict[str, Any]) -> str:
    if metrics["closed_trades"] < 10:
        return "needs_sample"
    if metrics["expectancy"] > 0 and metrics["profit_factor"] >= 1.25:
        return "approved"
    return "underperforming"


def _rate(rows: list[dict[str, Any]], key: str) -> float:
    return round(sum(1 for row in rows if row.get(key)) / len(rows), 4) if rows else 0.0


def _count_by(rows: list[dict[str, Any]], key_fn: Any) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        key = key_fn(row)
        counts[key] = counts.get(key, 0) + 1
    return counts


def _dte_from_snapshot(selected: dict[str, Any], decision: dict[str, Any]) -> int | None:
    expiration = selected.get("expiration")
    timestamp = decision.get("timestamp")
    if not expiration or not timestamp:
        return None
    try:
        from datetime import date, datetime

        expiry = date.fromisoformat(str(expiration)[:10])
        as_of = datetime.fromisoformat(str(timestamp).replace("Z", "+00:00")).date()
        return max(0, (expiry - as_of).days)
    except ValueError:
        return None


def _price_band(value: Any) -> str:
    price = _float_or_none(value)
    if price is None:
        return "unknown"
    if price < 1:
        return "under_1"
    if price < 3:
        return "1_to_3"
    if price < 7:
        return "3_to_7"
    return "7_plus"


def _dte_band(value: Any) -> str:
    dte = _float_or_none(value)
    if dte is None:
        return "unknown"
    if dte <= 3:
        return "0_to_3"
    if dte <= 10:
        return "4_to_10"
    if dte <= 30:
        return "11_to_30"
    return "31_plus"


def _delta_band(value: float | None) -> str:
    if value is None:
        return "unknown"
    abs_delta = abs(value)
    if abs_delta < 0.30:
        return "low"
    if abs_delta < 0.50:
        return "medium"
    if abs_delta <= 0.70:
        return "target"
    return "high"


def _theta_band(value: float | None) -> str:
    if value is None:
        return "unknown"
    theta = abs(value)
    if theta < 0.05:
        return "low_decay"
    if theta <= 0.15:
        return "medium_decay"
    return "high_decay"


def _iv_band(value: float | None) -> str:
    if value is None:
        return "unknown"
    if value < 0.30:
        return "low_iv"
    if value < 0.60:
        return "medium_iv"
    return "high_iv"


def _spread_band(value: float | None) -> str:
    if value is None:
        return "unknown"
    if value <= 0.05:
        return "tight"
    if value <= 0.12:
        return "normal"
    if value <= 0.18:
        return "wide_allowed"
    return "too_wide"


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))
