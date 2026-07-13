from __future__ import annotations

import json
import os
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .execution_broker import AlpacaExecutionBroker
from .execution_journal import append_execution_outcome, append_order_submission
from .execution_models import BrokerEnvironment, ExecutionOrder, OrderSide, OrderType, TradeIntent
from .position_store import load_open_positions
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
    take_profit_tighten_pct: float = 0.50
    take_profit_harvest_pct: float = 0.80
    take_profit_force_exit_pct: float = 1.20
    take_profit_limit_price_factor: float = 0.98
    take_profit_reprice_factor: float = 0.97
    take_profit_tight_limit_price_factor: float = 0.97
    take_profit_harvest_limit_price_factor: float = 0.95
    trailing_activation_pct: float = 0.15
    trailing_drawdown_pct: float = 0.10
    stop_loss_pct: float = 0.22
    max_contracts_per_option: int = 1
    trim_limit_price_factor: float = 0.90
    runner_take_profit_pct: float = 1.00
    runner_take_profit_tighten_pct: float = 1.50
    runner_take_profit_harvest_pct: float = 2.00
    runner_take_profit_force_exit_pct: float = 3.00
    runner_trailing_activation_pct: float = 0.50
    runner_trailing_drawdown_pct: float = 0.25
    runner_stop_loss_pct: float = 0.70


def load_position_monitor_rules() -> PositionMonitorRules:
    return PositionMonitorRules(
        enabled=_normalize_bool(os.getenv("AUTOBOTT_POSITION_MONITOR_ENABLED"), default=True),
        take_profit_pct=float(os.getenv("AUTOBOTT_EXIT_TAKE_PROFIT_PCT", "0.30")),
        take_profit_tighten_pct=float(os.getenv("AUTOBOTT_EXIT_TAKE_PROFIT_TIGHTEN_PCT", "0.50")),
        take_profit_harvest_pct=float(os.getenv("AUTOBOTT_EXIT_TAKE_PROFIT_HARVEST_PCT", "0.80")),
        take_profit_force_exit_pct=float(os.getenv("AUTOBOTT_EXIT_TAKE_PROFIT_FORCE_EXIT_PCT", "1.20")),
        take_profit_limit_price_factor=float(os.getenv("AUTOBOTT_TAKE_PROFIT_LIMIT_PRICE_FACTOR", "0.98")),
        take_profit_reprice_factor=float(os.getenv("AUTOBOTT_TAKE_PROFIT_REPRICE_FACTOR", "0.97")),
        take_profit_tight_limit_price_factor=float(os.getenv("AUTOBOTT_TAKE_PROFIT_TIGHT_LIMIT_PRICE_FACTOR", "0.97")),
        take_profit_harvest_limit_price_factor=float(os.getenv("AUTOBOTT_TAKE_PROFIT_HARVEST_LIMIT_PRICE_FACTOR", "0.95")),
        trailing_activation_pct=float(os.getenv("AUTOBOTT_EXIT_TRAILING_ACTIVATION_PCT", "0.15")),
        trailing_drawdown_pct=float(os.getenv("AUTOBOTT_EXIT_TRAILING_DRAWDOWN_PCT", "0.10")),
        stop_loss_pct=float(os.getenv("AUTOBOTT_EXIT_STOP_LOSS_PCT", "0.22")),
        max_contracts_per_option=int(os.getenv("AUTOBOTT_MAX_CONTRACTS_PER_OPTION", "1")),
        trim_limit_price_factor=float(os.getenv("AUTOBOTT_TRIM_LIMIT_PRICE_FACTOR", "0.90")),
        runner_take_profit_pct=float(os.getenv("AUTOBOTT_RUNNER_EXIT_TAKE_PROFIT_PCT", "1.00")),
        runner_take_profit_tighten_pct=float(os.getenv("AUTOBOTT_RUNNER_EXIT_TIGHTEN_PCT", "1.50")),
        runner_take_profit_harvest_pct=float(os.getenv("AUTOBOTT_RUNNER_EXIT_HARVEST_PCT", "2.00")),
        runner_take_profit_force_exit_pct=float(os.getenv("AUTOBOTT_RUNNER_EXIT_FORCE_PCT", "3.00")),
        runner_trailing_activation_pct=float(os.getenv("AUTOBOTT_RUNNER_EXIT_TRAILING_ACTIVATION_PCT", "0.50")),
        runner_trailing_drawdown_pct=float(os.getenv("AUTOBOTT_RUNNER_EXIT_TRAILING_DRAWDOWN_PCT", "0.25")),
        runner_stop_loss_pct=float(os.getenv("AUTOBOTT_RUNNER_EXIT_STOP_LOSS_PCT", "0.70")),
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
    position_store_path: str | Path | None = None,
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
    pending_orders = _pending_orders_by_symbol(resolved_broker)
    try:
        stored_positions = load_open_positions(store_path=position_store_path)
    except Exception:
        stored_positions = []
    stored_by_symbol = {position.option_symbol.upper(): position for position in stored_positions}
    peaks = _load_trailing_peaks(state_path=trailing_state_path)
    open_symbols: set[str] = set()
    actions: list[dict[str, Any]] = []
    actions.extend(_cancel_over_cap_pending_entries(pending_orders, broker=resolved_broker))
    for position in positions:
        symbol = str(position.get("symbol") or "").upper()
        if symbol:
            open_symbols.add(symbol)
        stored_position = stored_by_symbol.get(symbol)
        leg_role = stored_position.leg_role if stored_position is not None else None
        action = _monitor_action(position, _rules_for_leg(resolved_rules, leg_role), peaks, leg_role=leg_role)
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
            if action["reason"] in {"stop_loss", "trailing_stop"} and hasattr(resolved_broker, "cancel_order"):
                canceled_ids = _cancel_pending_orders_for_symbol(
                    action["symbol"],
                    pending_orders,
                    broker=resolved_broker,
                )
                if canceled_ids:
                    action["canceled_pending_order_ids"] = canceled_ids
                    action["canceled_pending_exit_order_id"] = canceled_ids[0]
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
    *,
    leg_role: str | None = None,
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
            "leg_role": leg_role,
        }
    if unrealized_plpc <= -abs(rules.stop_loss_pct):
        return {
            "reason": "stop_loss",
            "symbol": symbol,
            "quantity": qty,
            "unrealized_plpc": unrealized_plpc,
            "current_price": current_price,
            "leg_role": leg_role,
        }
    if peak_plpc >= rules.trailing_activation_pct and unrealized_plpc <= peak_plpc - rules.trailing_drawdown_pct:
        return {
            "reason": "trailing_stop",
            "symbol": symbol,
            "quantity": qty,
            "unrealized_plpc": unrealized_plpc,
            "current_price": current_price,
            "peak_unrealized_plpc": peak_plpc,
            "leg_role": leg_role,
        }
    if unrealized_plpc >= rules.take_profit_pct:
        tier = _take_profit_tier(unrealized_plpc, rules)
        return {
            "reason": "take_profit",
            "symbol": symbol,
            "quantity": qty,
            "unrealized_plpc": unrealized_plpc,
            "current_price": current_price,
            "peak_unrealized_plpc": peak_plpc,
            "take_profit_tier": tier,
            "leg_role": leg_role,
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
    limit_price = _exit_limit_price(
        float(action["current_price"]),
        rules=rules,
        take_profit=is_take_profit,
        unrealized_plpc=float(action.get("unrealized_plpc") or 0.0),
    )
    force_profit_exit = action.get("take_profit_tier") == "force_exit"
    order_type = OrderType.MARKET if force_profit_exit or not is_take_profit else OrderType.LIMIT
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
            "exit_order_style": "urgent_market" if order_type is OrderType.MARKET else "profit_ladder_limit",
            "take_profit_tier": action.get("take_profit_tier"),
            "unrealized_plpc": action["unrealized_plpc"],
            "leg_role": action.get("leg_role"),
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
            "take_profit_tier": action.get("take_profit_tier"),
            "unrealized_plpc": action["unrealized_plpc"],
            "state": order.state.value,
            "broker_order_id": order.broker_order_id,
        },
        journal_path=journal_path,
    )
    return order


def _rules_for_leg(rules: PositionMonitorRules, leg_role: str | None) -> PositionMonitorRules:
    if leg_role != "runner":
        return rules
    return replace(
        rules,
        take_profit_pct=rules.runner_take_profit_pct,
        take_profit_tighten_pct=rules.runner_take_profit_tighten_pct,
        take_profit_harvest_pct=rules.runner_take_profit_harvest_pct,
        take_profit_force_exit_pct=rules.runner_take_profit_force_exit_pct,
        trailing_activation_pct=rules.runner_trailing_activation_pct,
        trailing_drawdown_pct=rules.runner_trailing_drawdown_pct,
        stop_loss_pct=rules.runner_stop_loss_pct,
    )


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


def _pending_orders_by_symbol(broker: Any) -> dict[str, list[dict[str, Any]]]:
    if not hasattr(broker, "list_orders"):
        return {}
    try:
        orders = broker.list_orders(status="open", limit=100, direction="desc")
    except Exception:
        return {}
    pending: dict[str, list[dict[str, Any]]] = {}
    for order in orders:
        symbol = str(order.get("symbol") or "").upper()
        status = str(order.get("status") or "").lower()
        if not symbol or status not in {"new", "accepted", "partially_filled", "pending_new", "pending_replace"}:
            continue
        pending.setdefault(symbol, []).append(order)
    return pending


def _cancel_pending_orders_for_symbol(
    symbol: str,
    pending_orders: dict[str, list[dict[str, Any]]],
    *,
    broker: AlpacaExecutionBroker,
) -> list[str]:
    canceled: list[str] = []
    for order in pending_orders.get(symbol, []):
        order_id = str(order.get("id") or order.get("broker_order_id") or "")
        if not order_id:
            continue
        broker.cancel_order(order_id)
        canceled.append(order_id)
    return canceled


def _cancel_over_cap_pending_entries(
    pending_orders: dict[str, list[dict[str, Any]]],
    *,
    broker: AlpacaExecutionBroker,
) -> list[dict[str, Any]]:
    if not hasattr(broker, "cancel_order"):
        return []
    max_position_cost = _float_or_none(getattr(getattr(broker, "config", None), "max_position_cost", None))
    if max_position_cost is None or max_position_cost <= 0:
        return []
    actions: list[dict[str, Any]] = []
    for symbol, orders in pending_orders.items():
        for order in orders:
            side = str(order.get("side") or "").lower()
            if side != "buy":
                continue
            order_id = str(order.get("id") or order.get("broker_order_id") or "")
            if not order_id:
                continue
            estimated_notional = _pending_entry_notional(order)
            if estimated_notional is None or estimated_notional <= max_position_cost:
                continue
            try:
                broker.cancel_order(order_id)
                actions.append(
                    {
                        "reason": "pending_entry_over_cost_cap_canceled",
                        "symbol": symbol,
                        "broker_order_id": order_id,
                        "estimated_notional": estimated_notional,
                        "max_position_cost": max_position_cost,
                    }
                )
            except Exception as exc:
                actions.append(
                    {
                        "reason": "pending_entry_over_cost_cap_cancel_failed",
                        "symbol": symbol,
                        "broker_order_id": order_id,
                        "estimated_notional": estimated_notional,
                        "max_position_cost": max_position_cost,
                        "error": str(exc),
                    }
                )
    return actions


def _pending_entry_notional(order: dict[str, Any]) -> float | None:
    qty = _float_or_none(order.get("qty") or order.get("quantity"))
    filled_qty = _float_or_none(order.get("filled_qty"))
    limit_price = _float_or_none(order.get("limit_price"))
    if qty is None or limit_price is None:
        return None
    remaining_qty = max(0.0, qty - (filled_qty or 0.0))
    return round(limit_price * remaining_qty * 100.0, 2)


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
    target_limit = _exit_limit_price(
        float(action["current_price"]),
        rules=rules,
        take_profit=True,
        reprice=True,
        unrealized_plpc=float(action.get("unrealized_plpc") or 0.0),
    )
    if action.get("take_profit_tier") == "force_exit":
        try:
            if order_id and hasattr(broker, "cancel_order"):
                broker.cancel_order(order_id)
            order = _submit_forced_take_profit_exit(
                action=action,
                broker=broker,
                rules=rules,
                journal_path=journal_path,
            )
            return {
                **action,
                "reason": "take_profit_force_exit_submitted",
                "submitted": True,
                "canceled_pending_exit_order_id": order_id or None,
                "broker_order_id": order.broker_order_id,
                "state": order.state.value,
            }
        except Exception as exc:
            return {
                **action,
                "reason": "take_profit_force_exit_failed",
                "submitted": False,
                "canceled_pending_exit_order_id": order_id or None,
                "error": str(exc),
            }
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
                "take_profit_tier": action.get("take_profit_tier"),
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
    unrealized_plpc: float = 0.0,
    reprice: bool = False,
) -> float:
    if take_profit:
        factor = _take_profit_limit_factor(
            unrealized_plpc=unrealized_plpc,
            rules=rules,
            reprice=reprice,
        )
    else:
        factor = rules.trim_limit_price_factor
    return max(0.01, round(current_price * factor, 2))


def _take_profit_tier(unrealized_plpc: float, rules: PositionMonitorRules) -> str:
    if unrealized_plpc >= rules.take_profit_force_exit_pct:
        return "force_exit"
    if unrealized_plpc >= rules.take_profit_harvest_pct:
        return "harvest"
    if unrealized_plpc >= rules.take_profit_tighten_pct:
        return "tighten"
    return "initial"


def _take_profit_limit_factor(
    *,
    unrealized_plpc: float,
    rules: PositionMonitorRules,
    reprice: bool,
) -> float:
    if reprice:
        return min(rules.take_profit_reprice_factor, 0.99)
    tier = _take_profit_tier(unrealized_plpc, rules)
    if tier == "harvest":
        return min(rules.take_profit_harvest_limit_price_factor, 0.99)
    if tier == "tighten":
        return min(rules.take_profit_tight_limit_price_factor, 0.99)
    return min(rules.take_profit_limit_price_factor, 0.99)


def _submit_forced_take_profit_exit(
    *,
    action: dict[str, Any],
    broker: AlpacaExecutionBroker,
    rules: PositionMonitorRules,
    journal_path: str | None,
) -> ExecutionOrder:
    position = {
        "symbol": action["symbol"],
        "underlying": _underlying_from_option_symbol(action["symbol"]) or action["symbol"],
    }
    return _submit_monitor_exit(position, action=action, broker=broker, rules=rules, journal_path=journal_path)


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
