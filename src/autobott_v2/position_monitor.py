from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .execution_broker import AlpacaExecutionBroker
from .execution_journal import append_execution_outcome, append_order_submission
from .execution_models import BrokerEnvironment, ExecutionOrder, OrderSide, OrderType, TradeIntent
from .runtime_control import load_runtime_state
from .runtime_paths import data_root


def _normalize_bool(value: str | None, *, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class PositionMonitorRules:
    enabled: bool = True
    take_profit_pct: float = 0.30
    take_profit_limit_price_factor: float = 1.10
    take_profit_reprice_factor: float = 1.03
    trailing_activation_pct: float = 0.15
    trailing_drawdown_pct: float = 0.10
    stop_loss_pct: float = 0.22
    max_contracts_per_option: int = 1
    trim_limit_price_factor: float = 0.90


def load_position_monitor_rules() -> PositionMonitorRules:
    return PositionMonitorRules(
        enabled=_normalize_bool(os.getenv("AUTOBOTT_POSITION_MONITOR_ENABLED"), default=True),
        take_profit_pct=float(os.getenv("AUTOBOTT_EXIT_TAKE_PROFIT_PCT", "0.30")),
        take_profit_limit_price_factor=float(os.getenv("AUTOBOTT_TAKE_PROFIT_LIMIT_PRICE_FACTOR", "1.10")),
        take_profit_reprice_factor=float(os.getenv("AUTOBOTT_TAKE_PROFIT_REPRICE_FACTOR", "1.03")),
        trailing_activation_pct=float(os.getenv("AUTOBOTT_EXIT_TRAILING_ACTIVATION_PCT", "0.15")),
        trailing_drawdown_pct=float(os.getenv("AUTOBOTT_EXIT_TRAILING_DRAWDOWN_PCT", "0.10")),
        stop_loss_pct=float(os.getenv("AUTOBOTT_EXIT_STOP_LOSS_PCT", "0.22")),
        max_contracts_per_option=int(os.getenv("AUTOBOTT_MAX_CONTRACTS_PER_OPTION", "1")),
        trim_limit_price_factor=float(os.getenv("AUTOBOTT_TRIM_LIMIT_PRICE_FACTOR", "0.90")),
    )


def trailing_peak_state_path() -> Path:
    return data_root() / "execution" / "trailing_peaks.json"


def _load_trailing_peaks(*, state_path: str | Path | None = None) -> dict[str, float]:
    path = Path(state_path) if state_path is not None else trailing_peak_state_path()
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}
    return {str(symbol): float(value) for symbol, value in payload.items()}


def _save_trailing_peaks(peaks: dict[str, float], *, state_path: str | Path | None = None) -> None:
    path = Path(state_path) if state_path is not None else trailing_peak_state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(peaks, indent=2, sort_keys=True), encoding="utf-8")


def run_position_monitor(
    *,
    broker: AlpacaExecutionBroker | None = None,
    rules: PositionMonitorRules | None = None,
    journal_path: str | None = None,
    trailing_state_path: str | Path | None = None,
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
    pending_exits = _pending_exit_orders_by_symbol(resolved_broker)
    peaks = _load_trailing_peaks(state_path=trailing_state_path)
    open_symbols: set[str] = set()
    actions: list[dict[str, Any]] = []
    for position in positions:
        symbol = str(position.get("symbol") or "").upper()
        if symbol:
            open_symbols.add(symbol)
        action = _monitor_action(position, resolved_rules, peaks)
        if action is None:
            continue
        pending_exit = pending_exits.get(action["symbol"])
        if action["reason"] == "take_profit" and pending_exit is not None:
            actions.append(
                _handle_pending_take_profit_exit(
                    pending_exit,
                    action=action,
                    broker=resolved_broker,
                    rules=resolved_rules,
                    journal_path=journal_path,
                )
            )
            continue
        try:
            if action["reason"] in {"stop_loss", "trailing_stop"} and pending_exit is not None and hasattr(resolved_broker, "cancel_order"):
                resolved_broker.cancel_order(str(pending_exit.get("id") or pending_exit.get("broker_order_id")))
                action["canceled_pending_exit_order_id"] = pending_exit.get("id") or pending_exit.get("broker_order_id")
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
    _save_trailing_peaks(
        {symbol: value for symbol, value in peaks.items() if symbol in open_symbols},
        state_path=trailing_state_path,
    )
    return {
        "ok": True,
        "enabled": True,
        "checked": len(positions),
        "actions": actions,
    }


def _monitor_action(
    position: dict[str, Any],
    rules: PositionMonitorRules,
    peaks: dict[str, float],
) -> dict[str, Any] | None:
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

    peak_plpc = max(peaks.get(symbol, unrealized_plpc), unrealized_plpc)
    peaks[symbol] = peak_plpc

    if qty > rules.max_contracts_per_option:
        return {
            "reason": "trim_excess_contracts",
            "symbol": symbol,
            "quantity": qty - rules.max_contracts_per_option,
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
    if unrealized_plpc >= rules.take_profit_pct:
        return {
            "reason": "take_profit",
            "symbol": symbol,
            "quantity": qty,
            "unrealized_plpc": unrealized_plpc,
            "current_price": current_price,
            "peak_unrealized_plpc": peak_plpc,
        }
    if peak_plpc >= rules.trailing_activation_pct and unrealized_plpc <= peak_plpc - rules.trailing_drawdown_pct:
        return {
            "reason": "trailing_stop",
            "symbol": symbol,
            "quantity": qty,
            "unrealized_plpc": unrealized_plpc,
            "current_price": current_price,
            "peak_unrealized_plpc": peak_plpc,
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
    is_take_profit = action["reason"] == "take_profit"
    limit_price = _exit_limit_price(float(action["current_price"]), rules=rules, take_profit=is_take_profit)
    order_type = OrderType.LIMIT if is_take_profit else OrderType.MARKET
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
            "exit_order_style": "rich_limit" if is_take_profit else "urgent_market",
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
            "order_type": intent.order_type.value,
            "unrealized_plpc": action["unrealized_plpc"],
            "state": order.state.value,
            "broker_order_id": order.broker_order_id,
        },
        journal_path=journal_path,
    )
    return order


def _pending_exit_orders_by_symbol(broker: Any) -> dict[str, dict[str, Any]]:
    if not hasattr(broker, "list_orders"):
        return {}
    try:
        orders = broker.list_orders(status="open", limit=100, direction="desc")
    except Exception:
        return {}
    pending: dict[str, dict[str, Any]] = {}
    for order in orders:
        symbol = str(order.get("symbol") or "").upper()
        side = str(order.get("side") or "").lower()
        status = str(order.get("status") or "").lower()
        if not symbol or side != "sell" or status not in {"new", "accepted", "partially_filled", "pending_new", "pending_replace"}:
            continue
        pending.setdefault(symbol, order)
    return pending


def _handle_pending_take_profit_exit(
    pending_exit: dict[str, Any],
    *,
    action: dict[str, Any],
    broker: AlpacaExecutionBroker,
    rules: PositionMonitorRules,
    journal_path: str | None,
) -> dict[str, Any]:
    order_id = str(pending_exit.get("id") or pending_exit.get("broker_order_id") or "")
    current_limit = _float_or_none(pending_exit.get("limit_price"))
    target_limit = _exit_limit_price(float(action["current_price"]), rules=rules, take_profit=True, reprice=True)
    result = {
        **action,
        "reason": "take_profit_exit_already_pending",
        "submitted": False,
        "broker_order_id": order_id,
        "existing_limit_price": current_limit,
        "target_limit_price": target_limit,
    }
    if not order_id or current_limit is None or target_limit >= current_limit or not hasattr(broker, "replace_order"):
        return result
    try:
        payload = broker.replace_order(order_id, limit_price=target_limit)
        result["reason"] = "take_profit_exit_repriced"
        result["replaced"] = True
        result["broker_order_id"] = payload.get("id") or order_id
        result["new_limit_price"] = target_limit
        append_execution_outcome(
            decision_id=f"monitor-{action['symbol']}",
            thesis_id=f"monitor:{action['symbol']}:take_profit_reprice",
            symbol=action["symbol"],
            disposition="position_monitor_exit_repriced",
            detail="take_profit",
            payload={
                "old_limit_price": current_limit,
                "new_limit_price": target_limit,
                "unrealized_plpc": action["unrealized_plpc"],
                "broker_order_id": result["broker_order_id"],
            },
            journal_path=journal_path,
        )
    except Exception as exc:
        result["replaced"] = False
        result["error"] = str(exc)
    return result


def _exit_limit_price(
    current_price: float,
    *,
    rules: PositionMonitorRules,
    take_profit: bool,
    reprice: bool = False,
) -> float:
    if take_profit:
        factor = rules.take_profit_reprice_factor if reprice else rules.take_profit_limit_price_factor
    else:
        factor = rules.trim_limit_price_factor
    return max(0.01, round(current_price * factor, 2))


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _underlying_from_option_symbol(symbol: str) -> str | None:
    stripped = symbol.strip().upper()
    for index, char in enumerate(stripped):
        if char in {"C", "P"} and index >= 6:
            expiry = stripped[index - 6 : index]
            suffix = stripped[index + 1 :]
            if expiry.isdigit() and suffix.isdigit():
                return stripped[: index - 6]
    return None
