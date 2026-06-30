from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from typing import Any


FILL_MODEL_ORDER = ("optimistic_mid", "realistic_mid_penalty", "conservative", "stress")
PRIMARY_FILL_MODEL = "realistic_mid_penalty"
EXIT_POLICY_VERSION = "fixed_v1"


@dataclass(frozen=True)
class BucketEligibilityRules:
    primary_fill_model: str = PRIMARY_FILL_MODEL
    exit_policy_version: str = EXIT_POLICY_VERSION
    paper_min_closed_trades: int = 50
    paper_min_profit_factor: float = 1.25
    paper_min_expectancy: float = 0.0
    live_min_closed_trades: int = 100
    live_min_profit_factor: float = 1.35
    live_min_expectancy: float = 0.0
    max_drawdown_pct: float = 5.0
    min_fill_rate: float = 0.50
    max_unresolved_position_rate: float = 0.20
    outlier_profit_dependency_threshold: float = 0.50
    min_live_trading_days: int = 3
    stress_profit_factor_floor: float = 0.75
    stress_expectancy_floor: float = -0.05


def bucket_key_from_row(row: dict[str, Any]) -> str | None:
    trade_setup = row.get("trade_setup")
    execution_layer = row.get("execution_layer")
    leg_role = row.get("leg_role")
    if not trade_setup or not execution_layer or not leg_role:
        return None
    return f"{trade_setup}:{execution_layer}:{leg_role}"


def bucket_dimensions(bucket_key: str) -> dict[str, str]:
    trade_setup, execution_layer, leg_role = bucket_key.split(":")
    return {
        "trade_setup": trade_setup,
        "execution_layer": execution_layer,
        "leg_role": leg_role,
    }


def build_bucket_edge_report(
    *,
    campaign_run_id: str,
    fill_model_payloads: dict[str, dict[str, Any]],
    rules: BucketEligibilityRules | None = None,
) -> dict[str, Any]:
    rules = rules or BucketEligibilityRules()
    bucket_keys = sorted(_bucket_keys(fill_model_payloads))
    buckets: dict[str, Any] = {}

    for bucket_key in bucket_keys:
        metrics_by_fill_model: dict[str, Any] = {}
        auxiliary_dimensions = {
            "symbols": set(),
            "dte_buckets": set(),
            "time_of_day_buckets": set(),
            "cycle_confidence": set(),
            "trend_score_buckets": set(),
            "spread_pct_buckets": set(),
            "exit_reason": set(),
            "fill_model": set(),
            "exit_policy_version": {rules.exit_policy_version},
        }

        for fill_model in FILL_MODEL_ORDER:
            payload = fill_model_payloads.get(fill_model, _empty_payload())
            metrics, aux = _bucket_metrics(bucket_key, payload, fill_model)
            metrics_by_fill_model[fill_model] = metrics
            for key, values in aux.items():
                auxiliary_dimensions[key].update(values)

        eligibility = evaluate_bucket_eligibility(metrics_by_fill_model, rules)
        buckets[bucket_key] = {
            "dimensions": bucket_dimensions(bucket_key),
            "auxiliary_dimensions": {key: sorted(values) for key, values in auxiliary_dimensions.items()},
            "metrics_by_fill_model": metrics_by_fill_model,
            "eligibility": eligibility,
        }

    return {
        "schema_version": "phase1_bucket_edge_report.v1",
        "campaign_run_id": campaign_run_id,
        "exit_policy_version": rules.exit_policy_version,
        "primary_fill_model": rules.primary_fill_model,
        "buckets": buckets,
    }


def build_gate_candidate_report(
    *,
    campaign_run_id: str,
    edge_report: dict[str, Any],
    rules: BucketEligibilityRules | None = None,
) -> dict[str, Any]:
    rules = rules or BucketEligibilityRules()
    bucket_candidates: dict[str, Any] = {}
    for bucket_key, bucket in edge_report["buckets"].items():
        eligibility = bucket["eligibility"]
        bucket_candidates[bucket_key] = {
            "eligible_for_paper_forward": eligibility["eligible_for_paper_forward"],
            "eligible_for_live_review": eligibility["eligible_for_live_review"],
            "reasons": eligibility["blocking_reasons"],
            "metrics_by_fill_model": bucket["metrics_by_fill_model"],
        }

    return {
        "schema_version": "phase1_gate_candidate_report.v1",
        "campaign_run_id": campaign_run_id,
        "created_at": None,
        "primary_fill_model": rules.primary_fill_model,
        "exit_policy_version": rules.exit_policy_version,
        "live_enabled": False,
        "live_enablement_requires_manual_approval": True,
        "bucket_candidates": bucket_candidates,
    }


def evaluate_bucket_eligibility(metrics_by_fill_model: dict[str, dict[str, Any]], rules: BucketEligibilityRules | None = None) -> dict[str, Any]:
    rules = rules or BucketEligibilityRules()
    primary = metrics_by_fill_model.get(rules.primary_fill_model, {})
    optimistic = metrics_by_fill_model.get("optimistic_mid", {})
    stress = metrics_by_fill_model.get("stress", {})

    blocking_reasons = _paper_blocking_reasons(primary, optimistic, rules)
    live_blocking_reasons = _live_blocking_reasons(primary, stress, rules)

    return {
        "eligible_for_paper_forward": not blocking_reasons,
        "eligible_for_live_review": not blocking_reasons and not live_blocking_reasons,
        "blocking_reasons": sorted(set(blocking_reasons + live_blocking_reasons)),
    }


def _paper_blocking_reasons(primary: dict[str, Any], optimistic: dict[str, Any], rules: BucketEligibilityRules) -> list[str]:
    reasons: list[str] = []
    if primary.get("closed_trades", 0) < rules.paper_min_closed_trades:
        reasons.append("closed_trades_below_paper_threshold")
    if primary.get("profit_factor", 0.0) < rules.paper_min_profit_factor:
        reasons.append("profit_factor_below_paper_threshold")
    if primary.get("expectancy", 0.0) <= rules.paper_min_expectancy:
        reasons.append("expectancy_not_positive_under_primary_fill_model")
    if primary.get("max_drawdown", 0.0) > rules.max_drawdown_pct:
        reasons.append("max_drawdown_above_threshold")
    if primary.get("fill_rate", 0.0) < rules.min_fill_rate:
        reasons.append("fill_rate_below_threshold")
    if primary.get("unresolved_position_rate", 0.0) > rules.max_unresolved_position_rate:
        reasons.append("high_unresolved_position_rate")
    if primary.get("net_profit", 0.0) > 0 and primary.get("largest_win_pct_of_total_net_profit", 0.0) >= rules.outlier_profit_dependency_threshold:
        reasons.append("single_outlier_profit_dependency")
    if optimistic.get("expectancy", 0.0) > 0 and primary.get("expectancy", 0.0) <= 0:
        reasons.append("optimistic_only_profitability")
    return reasons


def _live_blocking_reasons(primary: dict[str, Any], stress: dict[str, Any], rules: BucketEligibilityRules) -> list[str]:
    reasons: list[str] = []
    if primary.get("closed_trades", 0) < rules.live_min_closed_trades:
        reasons.append("closed_trades_below_live_review_threshold")
    if primary.get("profit_factor", 0.0) < rules.live_min_profit_factor:
        reasons.append("profit_factor_below_live_review_threshold")
    if primary.get("expectancy", 0.0) <= rules.live_min_expectancy:
        reasons.append("expectancy_not_positive_for_live_review")
    if primary.get("trading_days_covered", 0) < rules.min_live_trading_days:
        reasons.append("insufficient_trading_day_coverage")
    if stress.get("profit_factor", 0.0) < rules.stress_profit_factor_floor or stress.get("expectancy", 0.0) < rules.stress_expectancy_floor:
        reasons.append("stress_result_catastrophically_negative")
    return reasons


def _bucket_keys(fill_model_payloads: dict[str, dict[str, Any]]) -> set[str]:
    keys: set[str] = set()
    for payload in fill_model_payloads.values():
        for row in payload.get("orders", []):
            key = bucket_key_from_row(row)
            if key is not None:
                keys.add(key)
        for row in payload.get("outcomes", []):
            key = bucket_key_from_row(row)
            if key is not None:
                keys.add(key)
    return keys


def _bucket_metrics(bucket_key: str, payload: dict[str, Any], fill_model: str) -> tuple[dict[str, Any], dict[str, set[str]]]:
    orders = [row for row in payload.get("orders", []) if bucket_key_from_row(row) == bucket_key]
    outcomes = [row for row in payload.get("outcomes", []) if bucket_key_from_row(row) == bucket_key]
    latest_positions = _latest_positions(payload.get("positions", []), bucket_key)
    decisions_by_id = {row.get("decision_id"): row for row in payload.get("decisions", [])}

    attempts = len(orders)
    filled_entries = len([row for row in orders if row.get("filled")])
    closed_trades = len(outcomes)
    unresolved = len([row for row in latest_positions.values() if row.get("lifecycle_status") == "unresolved"])
    pnls = [float(row.get("pnl") or 0.0) for row in outcomes]
    net_profit = round(sum(pnls), 4)
    wins = [pnl for pnl in pnls if pnl > 0]
    losses = [pnl for pnl in pnls if pnl < 0]
    largest_win = max(wins) if wins else 0.0
    largest_win_pct_of_total_net_profit = round(largest_win / net_profit, 4) if net_profit > 0 else 0.0
    trading_days = {(_parse_datetime(row["timestamp"]).date()).isoformat() for row in orders}

    auxiliary_dimensions = {
        "symbols": {str(row.get("ticker")) for row in orders if row.get("ticker")},
        "dte_buckets": {_dte_bucket(row) for row in orders if row.get("selected_contract")},
        "time_of_day_buckets": {_time_of_day_bucket(row["timestamp"]) for row in orders if row.get("timestamp")},
        "cycle_confidence": {str(row.get("cycle_confidence")) for row in orders if row.get("cycle_confidence")},
        "trend_score_buckets": {_trend_score_bucket(decisions_by_id.get(_root_decision_id(row), {})) for row in orders},
        "spread_pct_buckets": {_spread_pct_bucket(row.get("entry_spread_pct")) for row in orders if row.get("entry_spread_pct") is not None},
        "exit_reason": {str(row.get("exit_reason")) for row in outcomes if row.get("exit_reason")},
        "fill_model": {fill_model},
        "exit_policy_version": {"fixed_v1"},
    }
    auxiliary_dimensions = {key: {value for value in values if value} for key, values in auxiliary_dimensions.items()}

    metrics = {
        "closed_trades": closed_trades,
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": round(len(wins) / closed_trades, 4) if closed_trades else 0.0,
        "profit_factor": round(sum(wins) / abs(sum(losses)), 4) if losses else float("inf") if wins else 0.0,
        "expectancy": round(net_profit / closed_trades, 4) if closed_trades else 0.0,
        "max_drawdown": round(_max_drawdown(pnls), 4),
        "fill_rate": round(filled_entries / attempts, 4) if attempts else 0.0,
        "unresolved_position_rate": round(unresolved / filled_entries, 4) if filled_entries else 0.0,
        "largest_win_pct_of_total_net_profit": largest_win_pct_of_total_net_profit,
        "trading_days_covered": len(trading_days),
        "net_profit": net_profit,
    }
    return metrics, auxiliary_dimensions


def _latest_positions(rows: list[dict[str, Any]], bucket_key: str) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for row in rows:
        if bucket_key_from_row(row) != bucket_key:
            continue
        latest[row["decision_id"]] = row
    return latest


def _root_decision_id(row: dict[str, Any]) -> str:
    return row.get("parent_decision_id") or row.get("decision_id")


def _dte_bucket(row: dict[str, Any]) -> str:
    selected = row.get("selected_contract") or {}
    expiration = selected.get("expiration")
    timestamp = row.get("timestamp")
    if not expiration or not timestamp:
        return "unknown"
    dte = (date.fromisoformat(expiration) - _parse_datetime(timestamp).date()).days
    if dte <= 2:
        return "2DTE tactical"
    if dte <= 14:
        return "7-14DTE rider"
    if dte <= 30:
        return "14-30DTE rider"
    return "30DTE+ rider"


def _time_of_day_bucket(timestamp: str) -> str:
    current = _parse_datetime(timestamp).timetz().replace(tzinfo=None)
    if current < time(10, 0):
        return "open_30m"
    if current < time(11, 30):
        return "morning"
    if current < time(14, 0):
        return "midday"
    if current < time(15, 45):
        return "power_hour"
    return "closing_15m"


def _trend_score_bucket(decision: dict[str, Any]) -> str:
    trend_score = decision.get("cycle", {}).get("trend_score")
    if trend_score is None:
        return "unknown"
    if trend_score >= 2:
        return "trend_score_ge_2"
    if trend_score <= -2:
        return "trend_score_le_-2"
    return "trend_score_mid"


def _spread_pct_bucket(value: float | None) -> str:
    if value is None:
        return "unknown"
    if value <= 0.05:
        return "tight"
    if value <= 0.12:
        return "normal"
    return "wide"


def _max_drawdown(pnls: list[float]) -> float:
    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for pnl in pnls:
        equity += pnl
        peak = max(peak, equity)
        if peak > 0:
            max_drawdown = max(max_drawdown, ((peak - equity) / peak) * 100)
    return max_drawdown


def _parse_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _empty_payload() -> dict[str, Any]:
    return {"decisions": [], "orders": [], "positions": [], "outcomes": []}
