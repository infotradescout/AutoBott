from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .phase1_models import CycleStatus, ExecutionLayer, LegRole, LifecycleStatus, Phase1LedgerEvent, TradeSetup
from .runtime_paths import gate_path as default_gate_path

DEFAULT_PHASE1_GATE = {
    "trading_enabled": False,
    "global_trading_enabled": False,
    "min_total_trades": 50,
    "min_trades_per_enabled_bucket": 10,
    "min_profit_factor": 1.25,
    "min_expectancy_per_trade": 0.01,
    "max_drawdown_pct": 5.0,
    "min_fill_rate": 0.5,
    "max_unresolved_position_rate": 0.2,
    "allow_live_trading": False,
    "eligible_for_paper": False,
    "eligible_for_live_review": False,
    "live_enabled": False,
    "paper_only": True,
    "decision_stats": {
        "snapshots_processed": 0,
        "decisions_generated": 0,
        "no_trade_decisions": 0,
        "order_attempts": 0,
        "orders_rejected": 0,
        "orders_filled": 0,
        "fill_rate": 0.0,
        "rejection_reasons": {},
    },
    "position_stats": {
        "positions_opened": 0,
        "positions_closed": 0,
        "positions_unresolved": 0,
        "avg_hold_minutes": 0.0,
        "exit_reasons": {},
    },
    "trade_stats": {
        "closed_trades": 0,
        "wins": 0,
        "losses": 0,
        "win_rate": 0.0,
        "profit_factor": 0.0,
        "expectancy": 0.0,
        "max_drawdown": 0.0,
    },
    "decisions": 0,
    "orders_attempted": 0,
    "orders_filled": 0,
    "orders_rejected": 0,
    "fill_rate": 0.0,
    "unfilled_signal_followthrough_rate": 0.0,
    "sample_size": 0,
    "win_rate": 0.0,
    "expectancy_per_trade": 0.0,
    "profit_factor": 0.0,
    "max_drawdown_pct_observed": 0.0,
    "by_setup": {setup.value: {} for setup in TradeSetup if setup is not TradeSetup.NO_TRADE},
    "by_execution_layer": {layer.value: {} for layer in ExecutionLayer if layer not in {ExecutionLayer.NONE}},
    "authorized_buckets": {
        f"{setup.value}:{layer.value}": False
        for setup in TradeSetup
        if setup is not TradeSetup.NO_TRADE
        for layer in (ExecutionLayer.TACTICAL, ExecutionLayer.RIDER, ExecutionLayer.BOTH)
    },
    "last_updated": None,
    "fill_model": "unknown",
}


@dataclass(frozen=True)
class GateEvaluation:
    enabled: bool
    reason: str
    gate: dict[str, Any]


def gate_path(path: str | Path | None = None) -> Path:
    if path is not None:
        return Path(path)
    return default_gate_path()


def load_phase1_gate(path: str | Path | None = None) -> GateEvaluation:
    resolved = gate_path(path)
    if not resolved.exists():
        return GateEvaluation(False, "missing_gate_file_disables_phase1_trading", DEFAULT_PHASE1_GATE.copy())
    try:
        gate = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return GateEvaluation(False, "invalid_gate_file_disables_phase1_trading", DEFAULT_PHASE1_GATE.copy())
    return load_phase1_gate_from_dict(gate)


def create_ledger_event(
    *,
    decision_id: str,
    ticker: str,
    timestamp: Any,
    trade_setup: TradeSetup,
    execution_layer: ExecutionLayer,
    cycle_confidence: CycleStatus,
    selected_contract: Any,
    filled: bool,
    lifecycle_status: LifecycleStatus | None = None,
    parent_decision_id: str | None = None,
    leg_role: LegRole | None = None,
    entry_fill_model: str = "rejected",
    entry_underlying_price: float | None = None,
    entry_option_bid: float | None = None,
    entry_option_ask: float | None = None,
    entry_option_mid: float | None = None,
    entry_spread_pct: float | None = None,
    entry_fill_price: float | None = None,
    exit_option_bid: float | None = None,
    exit_option_ask: float | None = None,
    exit_option_mid: float | None = None,
    exit_spread_pct: float | None = None,
    exit_fill_model: str | None = None,
    exit_fill_price: float | None = None,
    exit_reason: str | None = None,
    option_return_pct: float | None = None,
    pnl: float | None = None,
    max_favorable_excursion: float | None = None,
    max_adverse_excursion: float | None = None,
    hold_minutes: int | None = None,
    contract_volume: int | None = None,
    contract_open_interest: int | None = None,
    quote_age_seconds: int | None = None,
    underlying_price_at_exit: float | None = None,
) -> Phase1LedgerEvent:
    return Phase1LedgerEvent(
        schema_version="phase1_ledger_event.v1",
        decision_id=decision_id,
        parent_decision_id=parent_decision_id,
        leg_role=leg_role,
        ticker=ticker,
        timestamp=timestamp,
        trade_setup=trade_setup,
        execution_layer=execution_layer,
        cycle_confidence=cycle_confidence,
        selected_contract=selected_contract,
        filled=filled,
        lifecycle_status=lifecycle_status or (LifecycleStatus.OPEN if filled else LifecycleStatus.REJECTED),
        entry_fill_model=entry_fill_model,
        entry_underlying_price=entry_underlying_price,
        entry_option_bid=entry_option_bid,
        entry_option_ask=entry_option_ask,
        entry_option_mid=entry_option_mid,
        entry_spread_pct=entry_spread_pct,
        entry_fill_price=entry_fill_price,
        exit_option_bid=exit_option_bid,
        exit_option_ask=exit_option_ask,
        exit_option_mid=exit_option_mid,
        exit_spread_pct=exit_spread_pct,
        exit_fill_model=exit_fill_model,
        exit_fill_price=exit_fill_price,
        exit_reason=exit_reason,
        option_return_pct=option_return_pct,
        pnl=pnl,
        max_favorable_excursion=max_favorable_excursion,
        max_adverse_excursion=max_adverse_excursion,
        hold_minutes=hold_minutes,
        contract_volume=contract_volume,
        contract_open_interest=contract_open_interest,
        quote_age_seconds=quote_age_seconds,
        underlying_price_at_exit=underlying_price_at_exit,
    )


def update_phase1_gate(events: list[Phase1LedgerEvent], path: str | Path | None = None) -> dict[str, Any]:
    gate = DEFAULT_PHASE1_GATE.copy()
    gate["by_setup"] = {key: {} for key in DEFAULT_PHASE1_GATE["by_setup"]}
    gate["by_execution_layer"] = {key: {} for key in DEFAULT_PHASE1_GATE["by_execution_layer"]}
    gate["authorized_buckets"] = {key: False for key in DEFAULT_PHASE1_GATE["authorized_buckets"]}

    order_events = [event for event in events if event.lifecycle_status in {LifecycleStatus.REJECTED, LifecycleStatus.OPEN, LifecycleStatus.CLOSED, LifecycleStatus.UNRESOLVED}]
    closed_events = [event for event in events if event.lifecycle_status == LifecycleStatus.CLOSED and event.filled and event.pnl is not None]
    rejected_events = [event for event in events if event.lifecycle_status == LifecycleStatus.REJECTED]
    unresolved_events = [event for event in events if event.lifecycle_status == LifecycleStatus.UNRESOLVED]
    opened_events = [event for event in events if event.lifecycle_status in {LifecycleStatus.OPEN, LifecycleStatus.CLOSED, LifecycleStatus.UNRESOLVED} and event.filled]

    gate["decisions"] = len({event.parent_decision_id or event.decision_id for event in order_events})
    gate["orders_attempted"] = len(order_events)
    gate["orders_filled"] = len([event for event in order_events if event.filled])
    gate["orders_rejected"] = gate["orders_attempted"] - gate["orders_filled"]
    gate["fill_rate"] = round(gate["orders_filled"] / gate["decisions"], 4) if gate["decisions"] else 0.0
    gate["sample_size"] = len(closed_events)
    gate["last_updated"] = events[-1].timestamp.isoformat() if events else None
    gate["fill_model"] = next((event.entry_fill_model for event in events if event.entry_fill_model), "unknown")

    if closed_events:
        pnls = [event.pnl or 0.0 for event in closed_events]
        wins = [pnl for pnl in pnls if pnl > 0]
        losses = [pnl for pnl in pnls if pnl < 0]
        gate["win_rate"] = round(len(wins) / len(closed_events), 4)
        gate["expectancy_per_trade"] = round(sum(pnls) / len(closed_events), 4)
        gate["profit_factor"] = round(sum(wins) / abs(sum(losses)), 4) if losses else float("inf")
        gate["max_drawdown_pct_observed"] = round(_max_drawdown_pct(pnls), 4)

    gate["decision_stats"] = {
        "snapshots_processed": 0,
        "decisions_generated": gate["decisions"],
        "no_trade_decisions": 0,
        "order_attempts": gate["orders_attempted"],
        "orders_rejected": len(rejected_events),
        "orders_filled": gate["orders_filled"],
        "fill_rate": gate["fill_rate"],
        "rejection_reasons": _reason_counts(rejected_events),
    }
    gate["position_stats"] = {
        "positions_opened": len(opened_events),
        "positions_closed": len(closed_events),
        "positions_unresolved": len(unresolved_events),
        "avg_hold_minutes": round(sum(event.hold_minutes or 0 for event in closed_events) / len(closed_events), 4) if closed_events else 0.0,
        "exit_reasons": _reason_counts([event for event in closed_events + unresolved_events if event.exit_reason]),
    }
    gate["trade_stats"] = {
        "closed_trades": len(closed_events),
        "wins": len([event for event in closed_events if (event.pnl or 0.0) > 0]),
        "losses": len([event for event in closed_events if (event.pnl or 0.0) < 0]),
        "win_rate": gate["win_rate"],
        "profit_factor": gate["profit_factor"],
        "expectancy": gate["expectancy_per_trade"],
        "max_drawdown": gate["max_drawdown_pct_observed"],
    }

    for setup in gate["by_setup"]:
        bucket_events = [event for event in closed_events if event.trade_setup.value == setup]
        gate["by_setup"][setup] = _bucket_stats(bucket_events)
    for layer in gate["by_execution_layer"]:
        bucket_events = [event for event in closed_events if event.execution_layer.value == layer]
        gate["by_execution_layer"][layer] = _bucket_stats(bucket_events)

    for bucket_key in gate["authorized_buckets"]:
        setup_value, layer_value = bucket_key.split(":")
        setup_bucket = gate["by_setup"].get(setup_value, {})
        layer_bucket = gate["by_execution_layer"].get(layer_value, {})
        gate["authorized_buckets"][bucket_key] = _bucket_authorized(setup_bucket, layer_bucket, gate)

    evaluation = load_phase1_gate_from_dict(gate)
    gate["trading_enabled"] = evaluation.enabled
    gate["global_trading_enabled"] = evaluation.enabled
    gate["eligible_for_paper"] = evaluation.enabled
    gate["eligible_for_live_review"] = evaluation.enabled and not gate["allow_live_trading"]
    gate["live_enabled"] = False
    resolved = gate_path(path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    resolved.write_text(json.dumps(gate, indent=2, sort_keys=True), encoding="utf-8")
    return gate


def load_phase1_gate_from_dict(gate: dict[str, Any]) -> GateEvaluation:
    if gate.get("sample_size", 0) < gate.get("min_total_trades", 50):
        return GateEvaluation(False, "sample_size_below_threshold", gate)
    if gate.get("fill_rate", 0.0) < gate.get("min_fill_rate", 0.5):
        return GateEvaluation(False, "fill_rate_below_threshold", gate)
    position_stats = gate.get("position_stats", {})
    unresolved_rate = (position_stats.get("positions_unresolved", 0) / position_stats.get("positions_opened", 1)) if position_stats.get("positions_opened", 0) else 0.0
    if unresolved_rate > gate.get("max_unresolved_position_rate", 0.2):
        return GateEvaluation(False, "unresolved_position_rate_above_threshold", gate)
    if gate.get("expectancy_per_trade", 0.0) <= gate.get("min_expectancy_per_trade", 0.01):
        return GateEvaluation(False, "expectancy_below_threshold", gate)
    if gate.get("profit_factor", 0.0) < gate.get("min_profit_factor", 1.25):
        return GateEvaluation(False, "profit_factor_below_threshold", gate)
    if gate.get("max_drawdown_pct_observed", 0.0) > gate.get("max_drawdown_pct", 5.0):
        return GateEvaluation(False, "max_drawdown_above_threshold", gate)
    for bucket_name, bucket in gate.get("by_setup", {}).items():
        if bucket.get("trades", 0) < gate.get("min_trades_per_enabled_bucket", 10):
            return GateEvaluation(False, f"under_sampled_setup_bucket:{bucket_name}", gate)
    for bucket_name, bucket in gate.get("by_execution_layer", {}).items():
        if bucket.get("trades", 0) < gate.get("min_trades_per_enabled_bucket", 10):
            return GateEvaluation(False, f"under_sampled_execution_layer_bucket:{bucket_name}", gate)
    return GateEvaluation(True, "gate_passed", gate)


def bucket_is_authorized(gate: dict[str, Any], trade_setup: TradeSetup, execution_layer: ExecutionLayer) -> bool:
    return bool(gate.get("authorized_buckets", {}).get(f"{trade_setup.value}:{execution_layer.value}", False))


def _bucket_stats(events: list[Phase1LedgerEvent]) -> dict[str, Any]:
    if not events:
        return {"trades": 0, "wins": 0, "losses": 0, "expectancy": 0.0, "profit_factor": 0.0}
    pnls = [event.pnl or 0.0 for event in events]
    wins = [pnl for pnl in pnls if pnl > 0]
    losses = [pnl for pnl in pnls if pnl < 0]
    return {
        "trades": len(events),
        "wins": len(wins),
        "losses": len(losses),
        "expectancy": round(sum(pnls) / len(events), 4),
        "profit_factor": round(sum(wins) / abs(sum(losses)), 4) if losses else float("inf"),
    }


def _max_drawdown_pct(pnls: list[float]) -> float:
    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for pnl in pnls:
        equity += pnl
        peak = max(peak, equity)
        if peak > 0:
            max_drawdown = max(max_drawdown, ((peak - equity) / peak) * 100)
    return max_drawdown


def _bucket_authorized(setup_bucket: dict[str, Any], layer_bucket: dict[str, Any], gate: dict[str, Any]) -> bool:
    min_trades = gate.get("min_trades_per_enabled_bucket", 10)
    min_pf = gate.get("min_profit_factor", 1.25)
    min_expectancy = gate.get("min_expectancy_per_trade", 0.01)
    return (
        setup_bucket.get("trades", 0) >= min_trades
        and layer_bucket.get("trades", 0) >= min_trades
        and setup_bucket.get("profit_factor", 0.0) >= min_pf
        and layer_bucket.get("profit_factor", 0.0) >= min_pf
        and setup_bucket.get("expectancy", 0.0) > min_expectancy
        and layer_bucket.get("expectancy", 0.0) > min_expectancy
    )


def _reason_counts(events: list[Phase1LedgerEvent]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for event in events:
        reason = event.exit_reason or "unknown"
        counts[reason] = counts.get(reason, 0) + 1
    return counts
