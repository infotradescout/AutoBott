from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from .execution_broker import AlpacaExecutionBroker
from .execution_journal import append_execution_outcome, append_order_submission
from .execution_models import BrokerEnvironment, ExecutionOrder, OrderSide, OrderType, TradeIntent
from .runtime_control import load_runtime_state


def _normalize_bool(value: str | None, *, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class PositionMonitorRules:
    enabled: bool = True
    profit_target_pct: float = 0.18
    stop_loss_pct: float = 0.22
    max_contracts_per_option: int = 1
    exit_limit_price_factor: float = 0.98
    trim_limit_price_factor: float = 0.90


def load_position_monitor_rules() -> PositionMonitorRules:
    return PositionMonitorRules(
        enabled=_normalize_bool(os.getenv("AUTOBOTT_POSITION_MONITOR_ENABLED"), default=True),
        profit_target_pct=float(os.getenv("AUTOBOTT_EXIT_PROFIT_TARGET_PCT", "0.18")),
        stop_loss_pct=float(os.getenv("AUTOBOTT_EXIT_STOP_LOSS_PCT", "0.22")),
        max_contracts_per_option=int(os.getenv("AUTOBOTT_MAX_CONTRACTS_PER_OPTION", "1")),
        exit_limit_price_factor=float(os.getenv("AUTOBOTT_EXIT_LIMIT_PRICE_FACTOR", "0.98")),
        trim_limit_price_factor=float(os.getenv("AUTOBOTT_TRIM_LIMIT_PRICE_FACTOR", "0.90")),
    )


def run_position_monitor(
    *,
    broker: AlpacaExecutionBroker | None = None,
    rules: PositionMonitorRules | None = None,
    journal_path: str | None = None,
) -> dict[str, Any]:
    resolved_rules = rules or load_position_monitor_rules()
    if not resolved_rules.enabled:
        return {"ok": True, "enabled": False, "checked": 0, "actions": []}
    runtime_state = load_runtime_state()
    if runtime_state.kill_switch_enabled or not runtime_state.execution_enabled:
        return {
            "ok": True,
            "enabled": True,
            "blocked": True,
            "reason": "kill_switch_enabled" if runtime_state.kill_switch_enabled else "execution_disabled",
            "checked": 0,
            "actions": [],
        }

    resolved_broker = broker or AlpacaExecutionBroker()
    if not hasattr(resolved_broker, "list_open_positions"):
        return {"ok": True, "enabled": True, "checked": 0, "actions": []}
    positions = resolved_broker.list_open_positions()
    actions: list[dict[str, Any]] = []
    for position in positions:
        action = _monitor_action(position, resolved_rules)
        if action is None:
            continue
        try:
            order = _submit_monitor_exit(
                position,
                action=action,
                broker=resolved_broker,
                rules=resolved_rules,
                journal_path=journal_path,
            )
            action["submitted"] = True
            action["broker_order_id"] = order.broker_order_id
            action["state"] = order.state.value
        except Exception as exc:
            action["submitted"] = False
            action["error"] = str(exc)
        actions.append(action)
    return {
        "ok": True,
        "enabled": True,
        "checked": len(positions),
        "actions": actions,
    }


def _monitor_action(position: dict[str, Any], rules: PositionMonitorRules) -> dict[str, Any] | None:
    symbol = str(position.get("symbol") or "").upper()
    if not symbol:
        return None
    side = str(position.get("side") or "long").lower()
    if side != "long":
        return None
    qty = int(float(position.get("qty") or 0))
    if qty <= 0:
        return None
    current_price = float(position.get("current_price") or position.get("avg_entry_price") or 0.0)
    if current_price <= 0:
        return None
    unrealized_plpc = float(position.get("unrealized_plpc") or 0.0)
    if qty > rules.max_contracts_per_option:
        return {
            "reason": "trim_excess_contracts",
            "symbol": symbol,
            "quantity": qty - rules.max_contracts_per_option,
            "unrealized_plpc": unrealized_plpc,
            "current_price": current_price,
        }
    if unrealized_plpc >= rules.profit_target_pct:
        return {
            "reason": "profit_target",
            "symbol": symbol,
            "quantity": qty,
            "unrealized_plpc": unrealized_plpc,
            "current_price": current_price,
        }
    if unrealized_plpc <= -abs(rules.stop_loss_pct):
        return {
            "reason": "stop_loss",
            "symbol": symbol,
            "quantity": qty,
            "unrealized_plpc": unrealized_plpc,
            "current_price": current_price,
        }
    return None


def _submit_monitor_exit(
    position: dict[str, Any],
    *,
    action: dict[str, Any],
    broker: AlpacaExecutionBroker,
    rules: PositionMonitorRules,
    journal_path: str | None,
) -> ExecutionOrder:
    symbol = action["symbol"]
    order_type = _monitor_order_type(action)
    limit_factor = rules.trim_limit_price_factor if action["reason"] in {"trim_excess_contracts", "stop_loss"} else rules.exit_limit_price_factor
    limit_price = max(0.01, round(float(action["current_price"]) * limit_factor, 2))
    intent = TradeIntent(
        symbol=str(position.get("underlying") or _underlying_from_option_symbol(symbol) or symbol),
        option_symbol=symbol,
        side=OrderSide.SELL_TO_CLOSE,
        quantity=int(action["quantity"]),
        limit_price=limit_price,
        generated_at=datetime.now(tz=UTC),
        environment=broker.config.environment if hasattr(broker, "config") else BrokerEnvironment.PAPER,
        order_type=order_type,
        decision_id=f"monitor-{symbol}",
        thesis_id=f"monitor:{symbol}:{action['reason']}",
        metadata={
            "position_monitor": True,
            "exit_reason": action["reason"],
            "unrealized_plpc": action["unrealized_plpc"],
        },
    )
    order = broker.submit_order(intent, open_positions=0)
    append_order_submission(order, journal_path=journal_path)
    append_execution_outcome(
        decision_id=intent.decision_id,
        thesis_id=intent.thesis_id,
        symbol=symbol,
        disposition="position_monitor_exit_submitted",
        detail=action["reason"],
        payload={
            "quantity": intent.quantity,
            "limit_price": intent.limit_price,
            "unrealized_plpc": action["unrealized_plpc"],
            "state": order.state.value,
            "broker_order_id": order.broker_order_id,
        },
        journal_path=journal_path,
    )
    return order


def _monitor_order_type(action: dict[str, Any]) -> OrderType:
    if action["reason"] in {"trim_excess_contracts", "stop_loss"}:
        return OrderType.MARKET
    return OrderType.LIMIT


def _underlying_from_option_symbol(symbol: str) -> str | None:
    stripped = symbol.strip().upper()
    for index, char in enumerate(stripped):
        if char in {"C", "P"} and index >= 6:
            expiry = stripped[index - 6 : index]
            suffix = stripped[index + 1 :]
            if expiry.isdigit() and suffix.isdigit():
                return stripped[: index - 6]
    return None
