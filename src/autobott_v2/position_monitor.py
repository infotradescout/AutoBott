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
    trailing_activation_pct: float = 0.15
    trailing_drawdown_pct: float = 0.10
    stop_loss_pct: float = 0.22
    max_contracts_per_option: int = 1
    trim_limit_price_factor: float = 0.90


def load_position_monitor_rules() -> PositionMonitorRules:
    return PositionMonitorRules(
        enabled=_normalize_bool(os.getenv("AUTOBOTT_POSITION_MONITOR_ENABLED"), default=True),
        take_profit_pct=float(os.getenv("AUTOBOTT_EXIT_TAKE_PROFIT_PCT", "0.30")),
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
    limit_price = max(0.01, round(float(action["current_price"]) * rules.trim_limit_price_factor, 2))
    intent = TradeIntent(
        symbol=str(position.get("underlying") or _underlying_from_option_symbol(symbol) or symbol),
        option_symbol=symbol,
        side=OrderSide.SELL_TO_CLOSE,
        quantity=int(action["quantity"]),
        limit_price=limit_price,
        generated_at=datetime.now(tz=UTC),
        environment=broker.config.environment if hasattr(broker, "config") else BrokerEnvironment.PAPER,
        order_type=OrderType.MARKET,
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


def _underlying_from_option_symbol(symbol: str) -> str | None:
    stripped = symbol.strip().upper()
    for index, char in enumerate(stripped):
        if char in {"C", "P"} and index >= 6:
            expiry = stripped[index - 6 : index]
            suffix = stripped[index + 1 :]
            if expiry.isdigit() and suffix.isdigit():
                return stripped[: index - 6]
    return None
