from __future__ import annotations

import json
import os
from math import isfinite
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from .execution_broker import AlpacaExecutionBroker
from .execution_journal import append_execution_outcome, append_order_submission
from .execution_models import BrokerEnvironment, ExecutionOrder, OrderSide, OrderType, TradeIntent
from .hosted_policy import (
    HOSTED_EXIT_MIN_DTE,
    HOSTED_POLICY_VERSION,
    active_build_sha,
    is_hosted_paper_runtime,
)
from .pair_lifecycle import (
    PairAction,
    PairLegMark,
    PairLifecycleRules,
    PairLifecycleState,
    evaluate_pair_lifecycle,
)
from .position_store import OpenPosition, load_open_positions, save_open_positions
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
    pair_max_loss_pct: float = 0.35
    funded_runner_trailing_activation_pct: float = 0.75
    funded_runner_trailing_drawdown_pct: float = 0.35
    funded_runner_catastrophic_stop_loss_pct: float = 0.90
    runner_funding_buffer_dollars: float = 0.0
    pending_entry_max_age_seconds: int = 180
    exit_min_dte: int = -1


def load_position_monitor_rules() -> PositionMonitorRules:
    if is_hosted_paper_runtime():
        return PositionMonitorRules(exit_min_dte=HOSTED_EXIT_MIN_DTE)
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
        pair_max_loss_pct=float(os.getenv("AUTOBOTT_PAIR_MAX_LOSS_PCT", "0.35")),
        funded_runner_trailing_activation_pct=float(os.getenv("AUTOBOTT_FUNDED_RUNNER_TRAILING_ACTIVATION_PCT", "0.75")),
        funded_runner_trailing_drawdown_pct=float(os.getenv("AUTOBOTT_FUNDED_RUNNER_TRAILING_DRAWDOWN_PCT", "0.35")),
        funded_runner_catastrophic_stop_loss_pct=float(os.getenv("AUTOBOTT_FUNDED_RUNNER_CATASTROPHIC_STOP_PCT", "0.90")),
        runner_funding_buffer_dollars=max(0.0, float(os.getenv("AUTOBOTT_RUNNER_FUNDING_BUFFER_DOLLARS", "0"))),
        pending_entry_max_age_seconds=max(30, int(os.getenv("AUTOBOTT_PENDING_ENTRY_MAX_AGE_SECONDS", "180"))),
        exit_min_dte=max(-1, int(os.getenv("AUTOBOTT_EXIT_MIN_DTE", "-1"))),
    )


def trailing_peak_state_path() -> Path:
    return data_root() / "execution" / "trailing_peaks.json"


def pair_lifecycle_state_path() -> Path:
    return data_root() / "execution" / "pair_lifecycle_state.json"


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


def _load_pair_states(*, state_path: str | Path | None = None) -> dict[str, dict[str, Any]]:
    path = Path(state_path) if state_path is not None else pair_lifecycle_state_path()
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _save_pair_states(states: dict[str, dict[str, Any]], *, state_path: str | Path | None = None) -> None:
    path = Path(state_path) if state_path is not None else pair_lifecycle_state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(states, indent=2, sort_keys=True), encoding="utf-8")


def run_position_monitor(
    *,
    broker: AlpacaExecutionBroker | None = None,
    rules: PositionMonitorRules | None = None,
    journal_path: str | None = None,
    trailing_state_path: str | Path | None = None,
    position_store_path: str | Path | None = None,
    pair_state_path: str | Path | None = None,
) -> dict[str, Any]:
    resolved_rules = rules or load_position_monitor_rules()
    if not resolved_rules.enabled:
        return {"ok": True, "enabled": False, "checked": 0, "actions": []}
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
    broker_by_symbol = {
        str(position.get("symbol") or "").upper(): position
        for position in positions
        if str(position.get("symbol") or "").strip()
    }
    peaks = _load_trailing_peaks(state_path=trailing_state_path)
    pair_states = _load_pair_states(state_path=pair_state_path)
    _reconcile_pair_funding(
        pair_states,
        broker=resolved_broker,
        broker_by_symbol=broker_by_symbol,
        stored_positions=stored_positions,
        pending_exits=pending_exits,
    )
    pending_exits = pending_exits or {}
    pair_actions, pair_managed_symbols = _build_pair_actions(
        broker_by_symbol=broker_by_symbol,
        stored_positions=stored_positions,
        peaks=peaks,
        pair_states=pair_states,
        rules=resolved_rules,
    )
    open_symbols: set[str] = set()
    actions: list[dict[str, Any]] = []
    stale_entry_actions = _cancel_stale_pending_entries(
        resolved_broker,
        max_age_seconds=resolved_rules.pending_entry_max_age_seconds,
    )
    actions.extend(stale_entry_actions)
    pending_orders = _without_canceled_orders(pending_orders, stale_entry_actions)
    over_cap_actions = _cancel_over_cap_pending_entries(pending_orders, broker=resolved_broker)
    actions.extend(over_cap_actions)
    pending_orders = _without_canceled_orders(pending_orders, over_cap_actions)
    for position in positions:
        symbol = str(position.get("symbol") or "").upper()
        if symbol:
            open_symbols.add(symbol)
        stored_position = stored_by_symbol.get(symbol)
        leg_role = stored_position.leg_role if stored_position is not None else None
        hard_action = _hard_safety_action(position, resolved_rules, leg_role=leg_role)
        pair_action = pair_actions.get(symbol) if symbol in pair_managed_symbols else None
        rules_action = (
            pair_action
            if symbol in pair_managed_symbols
            else _monitor_action(position, _rules_for_leg(resolved_rules, leg_role), peaks, leg_role=leg_role)
        )
        action = _position_cost_cap_action(position, broker=resolved_broker, leg_role=leg_role) or hard_action or rules_action
        if action is None:
            continue
        if stored_position is not None:
            action["trade_group_id"] = stored_position.trade_group_id
            action["entry_decision_id"] = stored_position.decision_id
            action["paired_option_symbol"] = stored_position.paired_option_symbol
            action["entry_policy_version"] = stored_position.entry_policy_version
            action["entry_build_sha"] = stored_position.entry_build_sha
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
            if action["reason"] in {
                "stop_loss",
                "trailing_stop",
                "dte_floor",
                "position_cost_cap_breached",
                "pair_max_loss_reached",
                "funded_runner_trailing_drawdown",
                "funded_runner_catastrophic_stop",
                "unfunded_runner_stop_loss",
            } and hasattr(resolved_broker, "cancel_order"):
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
            if action["reason"] == "primary_profit_funds_runner" and stored_position is not None and stored_position.trade_group_id:
                group_state = pair_states.setdefault(stored_position.trade_group_id, {})
                group_state["funding_exit_submitted"] = True
                group_state["funding_exit_order_id"] = order.broker_order_id
                group_state["funding_exit_status"] = order.state.value
                if order.broker_order_id:
                    group_state.setdefault("funding_exit_orders", {})[order.broker_order_id] = {
                        "status": order.state.value,
                        "reconciled": False,
                    }
                group_state["primary_profit_at_exit_submission"] = float(action.get("primary_pnl") or 0.0)
                group_state["runner_cost"] = float(action.get("runner_cost") or 0.0)
                group_state["runner_symbol"] = stored_position.paired_option_symbol
        except Exception as exc:
            action["submitted"] = False
            action["error"] = str(exc)
        actions.append(action)
    _save_pair_states(pair_states, state_path=pair_state_path)
    _save_trailing_peaks(
        {symbol: value for symbol, value in peaks.items() if symbol in open_symbols},
        state_path=trailing_state_path,
    )
    retained_store_symbols = open_symbols | {
        symbol
        for symbol, orders in pending_orders.items()
        if any(str(order.get("side") or "").lower() == "buy" for order in orders)
    }
    retained_positions = [
        position for position in stored_positions if position.option_symbol.upper() in retained_store_symbols
    ]
    if len(retained_positions) != len(stored_positions):
        save_open_positions(retained_positions, store_path=position_store_path)
    return {
        "ok": True,
        "enabled": True,
        "checked": len(positions),
        "actions": actions,
        "position_store_pruned": len(stored_positions) - len(retained_positions),
        "pair_groups_managed": len({
            position.trade_group_id
            for position in stored_positions
            if position.trade_group_id and position.option_symbol.upper() in pair_managed_symbols
        }),
    }


def _pair_rules(rules: PositionMonitorRules) -> PairLifecycleRules:
    return PairLifecycleRules(
        funding_buffer_dollars=rules.runner_funding_buffer_dollars,
        max_pair_loss_pct=rules.pair_max_loss_pct,
        unfunded_runner_stop_loss_pct=rules.runner_stop_loss_pct,
        funded_runner_trailing_activation_pct=rules.funded_runner_trailing_activation_pct,
        funded_runner_trailing_drawdown_pct=rules.funded_runner_trailing_drawdown_pct,
        catastrophic_runner_stop_loss_pct=rules.funded_runner_catastrophic_stop_loss_pct,
    ).validate()


def _build_pair_actions(
    *,
    broker_by_symbol: dict[str, dict[str, Any]],
    stored_positions: list[OpenPosition],
    peaks: dict[str, float],
    pair_states: dict[str, dict[str, Any]],
    rules: PositionMonitorRules,
) -> tuple[dict[str, dict[str, Any]], set[str]]:
    groups: dict[str, dict[str, OpenPosition]] = {}
    for stored in stored_positions:
        if not stored.trade_group_id or stored.leg_role not in {"primary", "runner"}:
            continue
        groups.setdefault(stored.trade_group_id, {})[stored.leg_role] = stored

    actions: dict[str, dict[str, Any]] = {}
    managed: set[str] = set()
    resolved_pair_rules = _pair_rules(rules)
    for group_id, group in groups.items():
        primary_store = group.get("primary")
        runner_store = group.get("runner")
        if runner_store is None:
            continue
        state_payload = pair_states.get(group_id, {})
        primary_symbol = primary_store.option_symbol.upper() if primary_store else state_payload.get("primary_symbol")
        runner_symbol = runner_store.option_symbol.upper()
        primary_position = broker_by_symbol.get(primary_symbol or "")
        runner_position = broker_by_symbol.get(runner_symbol)

        if runner_position is None:
            continue
        runner_return = float(runner_position.get("unrealized_plpc") or 0.0)
        runner_peak = max(peaks.get(runner_symbol, runner_return), runner_return)
        peaks[runner_symbol] = runner_peak
        runner_mark = _pair_leg_mark(runner_position, runner_store, peak_return_pct=runner_peak)
        if runner_mark is None:
            continue

        if primary_position is not None and primary_store is not None:
            primary_return = float(primary_position.get("unrealized_plpc") or 0.0)
            primary_peak = max(peaks.get(primary_symbol or "", primary_return), primary_return)
            if primary_symbol:
                peaks[primary_symbol] = primary_peak
            primary_mark = _pair_leg_mark(primary_position, primary_store, peak_return_pct=primary_peak)
            if primary_mark is None:
                continue
            decision = evaluate_pair_lifecycle(
                primary=primary_mark,
                runner=runner_mark,
                state=PairLifecycleState(
                    primary_realized_pnl=float(state_payload.get("primary_realized_pnl") or 0.0),
                    runner_entry_cost=_float_or_none(state_payload.get("runner_cost")),
                ),
                rules=resolved_pair_rules,
            )
            managed.update({primary_symbol or "", runner_symbol})
            if (
                decision.action is PairAction.EXIT_PRIMARY
                and primary_symbol
                and not state_payload.get("funding_exit_submitted")
                and not state_payload.get("funding_exit_blocked")
            ):
                actions[primary_symbol] = _pair_action_payload(
                    symbol=primary_symbol,
                    position=primary_position,
                    stored=primary_store,
                    decision=decision,
                )
            elif decision.action is PairAction.EXIT_BOTH:
                if primary_symbol:
                    actions[primary_symbol] = _pair_action_payload(
                        symbol=primary_symbol,
                        position=primary_position,
                        stored=primary_store,
                        decision=decision,
                    )
                actions[runner_symbol] = _pair_action_payload(
                    symbol=runner_symbol,
                    position=runner_position,
                    stored=runner_store,
                    decision=decision,
                )
            elif decision.action is PairAction.EXIT_RUNNER:
                actions[runner_symbol] = _pair_action_payload(
                    symbol=runner_symbol,
                    position=runner_position,
                    stored=runner_store,
                    decision=decision,
                )
            continue

        decision = evaluate_pair_lifecycle(
            primary=None,
            runner=runner_mark,
            state=PairLifecycleState(
                primary_open=False,
                runner_open=True,
                primary_realized_pnl=float(state_payload.get("primary_realized_pnl") or 0.0),
                runner_entry_cost=_float_or_none(state_payload.get("runner_cost")),
                legacy_runner_protection=bool(state_payload.get("legacy_runner_protection")),
            ),
            rules=resolved_pair_rules,
        )
        managed.add(runner_symbol)
        if decision.action is PairAction.EXIT_RUNNER:
            actions[runner_symbol] = _pair_action_payload(
                symbol=runner_symbol,
                position=runner_position,
                stored=runner_store,
                decision=decision,
            )
    managed.discard("")
    return actions, managed


def _pair_leg_mark(
    position: dict[str, Any],
    stored: OpenPosition,
    *,
    peak_return_pct: float | None,
) -> PairLegMark | None:
    entry_price = _float_or_none(position.get("avg_entry_price")) or stored.entry_limit_price
    current_price = _float_or_none(position.get("current_price")) or entry_price
    quantity = int(float(position.get("qty") or stored.quantity or 0))
    if entry_price <= 0 or current_price < 0 or quantity <= 0:
        return None
    return PairLegMark(
        entry_price=entry_price,
        current_price=current_price,
        quantity=quantity,
        peak_return_pct=peak_return_pct,
    )


def _pair_action_payload(
    *,
    symbol: str,
    position: dict[str, Any],
    stored: OpenPosition,
    decision: Any,
) -> dict[str, Any]:
    return {
        "reason": decision.reason,
        "symbol": symbol,
        "quantity": int(float(position.get("qty") or stored.quantity)),
        "unrealized_plpc": float(position.get("unrealized_plpc") or 0.0),
        "current_price": float(position.get("current_price") or position.get("avg_entry_price") or stored.entry_limit_price),
        "leg_role": stored.leg_role,
        "pair_entry_cost": decision.pair_entry_cost,
        "pair_mark_value": decision.pair_mark_value,
        "pair_pnl": decision.pair_pnl,
        "pair_return_pct": decision.pair_return_pct,
        "primary_pnl": decision.primary_pnl,
        "runner_pnl": decision.runner_pnl,
        "runner_cost": decision.runner_cost,
        "runner_funded": decision.runner_funded,
        "funding_surplus": decision.funding_surplus,
    }


_TERMINAL_FUNDING_STATUSES = {"filled", "canceled", "cancelled", "rejected", "expired", "replaced"}


def _finite_number(value: Any) -> float | None:
    number = _float_or_none(value)
    return number if number is not None and isfinite(number) else None


def _capture_pair_entry(
    payload: dict[str, Any], role: str, stored: OpenPosition | None, position: dict[str, Any] | None, broker: Any,
) -> None:
    if stored is not None:
        payload[f"{role}_symbol"] = stored.option_symbol.upper()
        payload[f"{role}_entry_order_id"] = stored.broker_order_id
    symbol = payload.get(f"{role}_symbol")
    entry_order_id = payload.get(f"{role}_entry_order_id")
    entry_qty = _finite_number(payload.get(f"{role}_entry_filled_qty")) or 0.0
    position_qty = _finite_number((position or {}).get("qty")) or 0.0
    exited_qty = float(payload.get("primary_exit_filled_qty") or 0.0) if role == "primary" else 0.0
    has_basis = bool(payload.get(f"{role}_entry_filled_avg_price") and entry_qty > 0)
    unresolved_exit = role == "primary" and "funding_order_unresolved:" in str(payload.get("funding_reconciliation_error") or "")
    if has_basis and entry_qty >= position_qty + exited_qty and not unresolved_exit:
        return
    # Entry limits are intentions, not execution costs. Prefer the entry order's
    # fills, with the broker's actual position basis as a recovery source.
    entry = None
    if hasattr(broker, "get_order") and entry_order_id:
        try:
            candidate = broker.get_order(entry_order_id)
            if (
                str(candidate.get("symbol") or "").upper() == symbol
                and str(candidate.get("side") or "").lower() == "buy"
            ):
                entry = candidate
        except Exception:
            pass
    price = _finite_number((entry or {}).get("filled_avg_price"))
    quantity = _finite_number((entry or {}).get("filled_qty"))
    if price is None or price <= 0 or quantity is None or quantity <= 0:
        if has_basis:
            return
        price = _finite_number((position or {}).get("avg_entry_price"))
        quantity = _finite_number((position or {}).get("qty"))
    if price is not None and price > 0 and quantity is not None and quantity > 0:
        payload[f"{role}_entry_filled_avg_price"] = price
        payload[f"{role}_entry_filled_qty"] = quantity
        payload[f"{role}_entry_cost"] = round(price * quantity * 100.0, 8)
        if role == "runner":
            payload["runner_cost"] = payload["runner_entry_cost"]


def _reconcile_pair_funding(
    pair_states: dict[str, dict[str, Any]],
    *,
    broker: Any,
    broker_by_symbol: dict[str, dict[str, Any]],
    stored_positions: list[OpenPosition],
    pending_exits: dict[str, dict[str, Any]] | None,
) -> None:
    groups: dict[str, dict[str, OpenPosition]] = {}
    for stored in stored_positions:
        if stored.trade_group_id and stored.leg_role in {"primary", "runner"}:
            groups.setdefault(stored.trade_group_id, {})[stored.leg_role] = stored
    for group_id, group in groups.items():
        payload = pair_states.setdefault(group_id, {})
        if payload.get("runner_funded") and not payload.get("funding_verified"):
            payload["legacy_runner_protection"] = True
        for role, stored in group.items():
            _capture_pair_entry(payload, role, stored, broker_by_symbol.get(stored.option_symbol.upper()), broker)
        primary_symbol = str(payload.get("primary_symbol") or "").upper()
        if "primary" not in group and payload.get("primary_entry_order_id"):
            _capture_pair_entry(payload, "primary", None, broker_by_symbol.get(primary_symbol), broker)
        orders = payload.setdefault("funding_exit_orders", {})
        old_id = payload.get("funding_exit_order_id")
        if old_id:
            orders.setdefault(str(old_id), {"reconciled": False})
        pending = (pending_exits or {}).get(primary_symbol)
        if pending is not None:
            pending_id = str(pending.get("id") or pending.get("broker_order_id") or "")
            if pending_id:
                orders.setdefault(pending_id, {"reconciled": False})
                payload["funding_exit_order_id"] = pending_id

        # An old latch without an order identity cannot safely prove cancellation
        # or funding. Keep it retryable for reconciliation, without another sell.
        unidentified_submission = bool(payload.get("funding_exit_submitted") and not orders)
        blocked = pending_exits is None or unidentified_submission
        errors: list[str] = []
        if pending_exits is None:
            errors.append("open_orders_unavailable")
        if payload.get("funding_exit_submitted") and not orders:
            errors.append("funding_order_identity_unavailable")
        entry_price = _finite_number(payload.get("primary_entry_filled_avg_price"))
        entry_qty = _finite_number(payload.get("primary_entry_filled_qty"))
        for order_id, record in list(orders.items()):
            if record.get("reconciled") and record.get("status") in _TERMINAL_FUNDING_STATUSES:
                continue
            try:
                order = broker.get_order(order_id)
                if (
                    str(order.get("id") or "") != order_id
                    or str(order.get("symbol") or "").upper() != primary_symbol
                    or str(order.get("side") or "").lower() != "sell"
                ):
                    raise ValueError("funding_order_identity_mismatch")
                status = str(order.get("status") or "").lower()
                quantity = _finite_number(order.get("filled_qty"))
                price = _finite_number(order.get("filled_avg_price"))
                if quantity is None or quantity < 0 or (status == "filled" and quantity <= 0):
                    raise ValueError("funding_fill_quantity_unavailable")
                if quantity < float(record.get("filled_qty") or 0.0):
                    raise ValueError("funding_fill_quantity_regressed")
                if quantity > 0 and (
                    price is None or price <= 0 or entry_price is None or entry_qty is None or quantity > entry_qty
                ):
                    raise ValueError("funding_fill_basis_unavailable")
                if status == "replaced" and not order.get("replaced_by"):
                    raise ValueError("funding_replacement_identity_unavailable")
                record.update(
                    status=status,
                    filled_qty=quantity,
                    filled_avg_price=price,
                    realized_pnl=(price - entry_price) * quantity * 100.0 if quantity > 0 else 0.0,
                    reconciled=True,
                )
                if status == "replaced" and order.get("replaced_by"):
                    replacement = str(order["replaced_by"])
                    record["replaced_by"] = replacement
                    orders.setdefault(replacement, {"reconciled": False})
                    payload["funding_exit_order_id"] = replacement
                if order_id == payload.get("funding_exit_order_id"):
                    payload["funding_exit_status"] = status
            except Exception:
                blocked = True
                errors.append(f"funding_order_unresolved:{order_id}")
        # The broker interface does not establish whether a replacement inherits
        # its predecessor's partial fills. Never sum those ambiguous successors.
        ambiguous_successors: set[str] = set()
        for record in orders.values():
            if record.get("replaced_by") and float(record.get("filled_qty") or 0.0) > 0:
                successor = str(record["replaced_by"])
                while successor and successor not in ambiguous_successors:
                    ambiguous_successors.add(successor)
                    successor = str(orders.get(successor, {}).get("replaced_by") or "")
        if ambiguous_successors:
            blocked = True
            errors.append("partial_funding_replacement_requires_reconciliation")
        verified_records = [record for order_id, record in orders.items() if order_id not in ambiguous_successors]
        filled_quantity = sum(float(record.get("filled_qty") or 0.0) for record in verified_records)
        realized = sum(float(record.get("realized_pnl") or 0.0) for record in verified_records)
        if ambiguous_successors:
            # A successor may offset predecessor profit with realized losses.
            # Keep the observed amount diagnostic until net chain P/L is known.
            payload["funding_ambiguous_predecessor_pnl"] = round(realized, 8)
            realized = 0.0
        else:
            payload.pop("funding_ambiguous_predecessor_pnl", None)
        valid_fills = bool(
            not ambiguous_successors and filled_quantity > 0 and entry_qty is not None and filled_quantity <= entry_qty
        )
        if entry_qty is not None and filled_quantity > entry_qty:
            blocked = True
            errors.append("funding_fill_quantity_exceeds_entry")
            realized = 0.0
        for record in orders.values():
            if not record.get("reconciled") or record.get("status") not in _TERMINAL_FUNDING_STATUSES:
                blocked = True
        position_qty = _finite_number(broker_by_symbol.get(primary_symbol, {}).get("qty")) or 0.0
        if entry_qty is not None and position_qty > max(0.0, entry_qty - filled_quantity):
            blocked = True
            errors.append("primary_position_fill_snapshot_pending")
        payload["primary_exit_filled_qty"] = filled_quantity
        payload["primary_realized_pnl"] = round(realized, 8)
        payload["funding_verified"] = valid_fills
        runner_cost = _finite_number(payload.get("runner_cost")) or 0.0
        payload["runner_funded"] = valid_fills and runner_cost > 0 and realized + 1e-8 >= runner_cost
        payload["funding_exit_submitted"] = unidentified_submission or any(
            not record.get("reconciled") or record.get("status") not in _TERMINAL_FUNDING_STATUSES
            for record in orders.values()
        )
        payload["funding_exit_blocked"] = blocked
        if valid_fills and all(record.get("reconciled") for record in orders.values()):
            payload.pop("legacy_runner_protection", None)
        if payload["runner_funded"]:
            payload.setdefault("funded_at", _monitor_now().isoformat())
        if errors:
            payload["funding_reconciliation_error"] = ";".join(errors)
        else:
            payload.pop("funding_reconciliation_error", None)


def _hard_safety_action(
    position: dict[str, Any],
    rules: PositionMonitorRules,
    *,
    leg_role: str | None,
) -> dict[str, Any] | None:
    symbol = str(position.get("symbol") or "").upper()
    if not symbol:
        return None
    qty = int(float(position.get("qty") or 0))
    current_price = float(position.get("current_price") or position.get("avg_entry_price") or 0.0)
    unrealized_plpc = float(position.get("unrealized_plpc") or 0.0)
    expiration = _option_expiration(symbol)
    dte = (expiration - _monitor_now().date()).days if expiration is not None else None
    if rules.exit_min_dte >= 0 and dte is not None and dte <= rules.exit_min_dte:
        return {
            "reason": "dte_floor",
            "symbol": symbol,
            "quantity": qty,
            "unrealized_plpc": unrealized_plpc,
            "current_price": current_price,
            "dte": dte,
            "expiration": expiration.isoformat(),
            "leg_role": leg_role,
        }
    if qty > rules.max_contracts_per_option:
        return {
            "reason": "trim_excess_contracts",
            "symbol": symbol,
            "quantity": qty - rules.max_contracts_per_option,
            "unrealized_plpc": unrealized_plpc,
            "current_price": current_price,
            "leg_role": leg_role,
        }
    return None


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

    expiration = _option_expiration(symbol)
    dte = (expiration - _monitor_now().date()).days if expiration is not None else None
    if rules.exit_min_dte >= 0 and dte is not None and dte <= rules.exit_min_dte:
        return {
            "reason": "dte_floor",
            "symbol": symbol,
            "quantity": qty,
            "unrealized_plpc": unrealized_plpc,
            "current_price": current_price,
            "dte": dte,
            "expiration": expiration.isoformat(),
            "leg_role": leg_role,
        }

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


def _position_cost_cap_action(
    position: dict[str, Any],
    *,
    broker: AlpacaExecutionBroker,
    leg_role: str | None,
) -> dict[str, Any] | None:
    config = getattr(broker, "config", None)
    effective_limit = (
        config.effective_max_position_cost()
        if config is not None and hasattr(config, "effective_max_position_cost")
        else getattr(config, "max_position_cost", None)
    )
    max_position_cost = _float_or_none(effective_limit)
    if max_position_cost is None or max_position_cost <= 0:
        return None
    symbol = str(position.get("symbol") or "").upper()
    side = str(position.get("side") or "long").lower()
    quantity = int(float(position.get("qty") or 0))
    average_entry = _float_or_none(position.get("avg_entry_price"))
    current_price = _float_or_none(position.get("current_price")) or average_entry
    if not symbol or side != "long" or quantity <= 0 or average_entry is None or average_entry <= 0:
        return None
    per_contract_notional = round(average_entry * 100, 2)
    filled_notional = round(per_contract_notional * quantity, 2)
    if per_contract_notional <= max_position_cost:
        return None
    return {
        "reason": "position_cost_cap_breached",
        "symbol": symbol,
        "quantity": quantity,
        "unrealized_plpc": float(position.get("unrealized_plpc") or 0.0),
        "current_price": current_price,
        "average_entry_price": average_entry,
        "filled_notional": filled_notional,
        "max_position_cost": max_position_cost,
        "leg_role": leg_role,
    }


def _submit_monitor_exit(
    position: dict[str, Any],
    *,
    action: dict[str, Any],
    broker: AlpacaExecutionBroker,
    rules: PositionMonitorRules,
    journal_path: str | None,
) -> ExecutionOrder:
    symbol = action["symbol"]
    is_take_profit = action["reason"] in {"take_profit", "primary_profit_funds_runner"}
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
            "trade_group_id": action.get("trade_group_id"),
            "entry_decision_id": action.get("entry_decision_id"),
            "paired_option_symbol": action.get("paired_option_symbol"),
            "entry_policy_version": action.get("entry_policy_version"),
            "entry_build_sha": action.get("entry_build_sha"),
            "pair_pnl": action.get("pair_pnl"),
            "runner_funded": action.get("runner_funded"),
            "exit_policy_version": HOSTED_POLICY_VERSION if is_hosted_paper_runtime() else "local-default",
            "build_sha": active_build_sha(),
        },
    )
    order = broker.submit_order(intent, open_positions=0)
    try:
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
                "pair_pnl": action.get("pair_pnl"),
                "runner_funded": action.get("runner_funded"),
                "state": order.state.value,
                "broker_order_id": order.broker_order_id,
            },
            journal_path=journal_path,
        )
    except Exception:
        pass
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


def _pending_exit_orders_by_symbol(broker: Any) -> dict[str, dict[str, Any]] | None:
    if not hasattr(broker, "list_orders"):
        return None
    try:
        orders = broker.list_orders(status="open", limit=100, direction="desc")
    except Exception:
        return None
    pending: dict[str, dict[str, Any]] = {}
    for order in orders:
        symbol = str(order.get("symbol") or "").upper()
        side = str(order.get("side") or "").lower()
        status = str(order.get("status") or "").lower()
        if not symbol or side != "sell" or status in _TERMINAL_FUNDING_STATUSES:
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
        try:
            broker.cancel_order(order_id)
        except Exception as exc:
            normalized = str(exc).strip().lower()
            if not any(token in normalized for token in ("already canceled", "already cancelled", "not found", "404")):
                raise
        canceled.append(order_id)
    return canceled


def _without_canceled_orders(
    pending_orders: dict[str, list[dict[str, Any]]],
    actions: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    canceled_ids = {
        str(action.get("broker_order_id") or "")
        for action in actions
        if str(action.get("reason") or "").endswith("_canceled")
    }
    if not canceled_ids:
        return pending_orders
    return {
        symbol: [
            order
            for order in orders
            if str(order.get("id") or order.get("broker_order_id") or "") not in canceled_ids
        ]
        for symbol, orders in pending_orders.items()
    }


def _cancel_over_cap_pending_entries(
    pending_orders: dict[str, list[dict[str, Any]]],
    *,
    broker: AlpacaExecutionBroker,
) -> list[dict[str, Any]]:
    if not hasattr(broker, "cancel_order"):
        return []
    config = getattr(broker, "config", None)
    effective_limit = (
        config.effective_max_position_cost()
        if config is not None and hasattr(config, "effective_max_position_cost")
        else getattr(config, "max_position_cost", None)
    )
    max_position_cost = _float_or_none(effective_limit)
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


def _cancel_stale_pending_entries(
    broker: AlpacaExecutionBroker,
    *,
    max_age_seconds: int,
) -> list[dict[str, Any]]:
    if not hasattr(broker, "list_orders") or not hasattr(broker, "cancel_order"):
        return []
    try:
        try:
            orders = broker.list_orders(status="open", limit=100, direction="desc", nested=True)
        except TypeError:
            orders = broker.list_orders(status="open", limit=100, direction="desc")
    except Exception:
        return []
    now = _monitor_now()
    actions: list[dict[str, Any]] = []
    for order in orders:
        if not str(order.get("client_order_id") or "").startswith("autobott-"):
            continue
        order_class = str(order.get("order_class") or "").lower()
        side = str(order.get("side") or "").lower()
        if order_class != "mleg" and side != "buy":
            continue
        status = str(order.get("status") or "").lower()
        if status not in {"new", "accepted", "partially_filled", "pending_new", "pending_replace"}:
            continue
        submitted_at = _datetime_or_none(order.get("submitted_at") or order.get("created_at"))
        if submitted_at is None:
            continue
        age_seconds = max(0.0, (now - submitted_at).total_seconds())
        if age_seconds < max_age_seconds:
            continue
        order_id = str(order.get("id") or order.get("broker_order_id") or "")
        if not order_id:
            continue
        legs = [leg for leg in order.get("legs") or [] if isinstance(leg, dict)]
        symbols = [str(leg.get("symbol") or "").upper() for leg in legs if leg.get("symbol")]
        if not symbols and order.get("symbol"):
            symbols = [str(order["symbol"]).upper()]
        try:
            broker.cancel_order(order_id)
            actions.append(
                {
                    "reason": "stale_atomic_entry_canceled" if order_class == "mleg" else "stale_linked_entry_canceled",
                    "broker_order_id": order_id,
                    "symbols": symbols,
                    "age_seconds": round(age_seconds, 1),
                    "max_age_seconds": max_age_seconds,
                }
            )
        except Exception as exc:
            actions.append(
                {
                    "reason": "stale_atomic_entry_cancel_failed" if order_class == "mleg" else "stale_linked_entry_cancel_failed",
                    "broker_order_id": order_id,
                    "symbols": symbols,
                    "age_seconds": round(age_seconds, 1),
                    "max_age_seconds": max_age_seconds,
                    "error": str(exc),
                }
            )
    return actions


def _monitor_now() -> datetime:
    return datetime.now(UTC)


def _datetime_or_none(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


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


def _option_expiration(symbol: str) -> date | None:
    stripped = symbol.strip().upper()
    for index, char in enumerate(stripped):
        if char not in {"C", "P"} or index < 6:
            continue
        expiry = stripped[index - 6 : index]
        suffix = stripped[index + 1 :]
        if not expiry.isdigit() or not suffix.isdigit():
            continue
        try:
            return date(2000 + int(expiry[:2]), int(expiry[2:4]), int(expiry[4:6]))
        except ValueError:
            return None
    return None
