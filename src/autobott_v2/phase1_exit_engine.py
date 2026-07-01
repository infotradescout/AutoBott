from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time
from typing import Any

from .phase1_models import ExecutionLayer, Phase1LedgerEvent


@dataclass(frozen=True)
class ExitRules:
    tactical_profit_target_pct: float = 0.35
    tactical_stop_loss_pct: float = 0.35
    tactical_eod_flatten_time: time = time(15, 45)
    rider_profit_target_pct: float = 0.60
    rider_stop_loss_pct: float = 0.40
    rider_min_dte: int = 5
    max_exit_quote_age_seconds: int = 30
    fill_model: str = "realistic_mid_penalty"


@dataclass(frozen=True)
class ExitDecision:
    exit_action: str
    exit_reason: str | None
    exit_fill_price: float | None
    exit_option_bid: float | None
    exit_option_ask: float | None
    exit_option_mid: float | None
    exit_spread_pct: float | None
    exit_fill_model: str | None
    exit_underlying_price: float | None
    option_return_pct: float | None
    pnl_dollars: float | None
    hold_minutes: int | None


def evaluate_exit(
    open_position: Phase1LedgerEvent,
    latest_snapshot: dict[str, Any],
    *,
    quote_age_seconds: int = 0,
    rules: ExitRules | None = None,
) -> ExitDecision:
    rules = rules or ExitRules()
    if open_position.selected_contract is None or open_position.entry_fill_price is None:
        return ExitDecision("hold", None, None, None, None, None, None, None, None, None, None, None)

    option_quote = _option_quote(latest_snapshot, open_position.selected_contract.option_symbol)
    if option_quote is None:
        return _unresolved("missing_exit_quote")

    bid = option_quote["bid"]
    ask = option_quote["ask"]
    snapshot_time = _parse_datetime(latest_snapshot["timestamp"])
    eod_trigger = open_position.execution_layer == ExecutionLayer.TACTICAL and snapshot_time.timetz().replace(tzinfo=None) >= rules.tactical_eod_flatten_time
    if quote_age_seconds > rules.max_exit_quote_age_seconds:
        return _unresolved("exit_rejected_stale_quote" if not eod_trigger else "eod_flatten_stale_quote")
    if bid <= 0 or ask <= 0 or ask < bid:
        return _unresolved("invalid_exit_quote" if not eod_trigger else "eod_flatten_invalid_quote")

    mid = round((bid + ask) / 2, 4)
    spread_pct = round((ask - bid) / mid, 4) if mid > 0 else None
    exit_fill_price, exit_fill_model = _exit_fill(bid, ask, mid, rules)
    option_return_pct = round((exit_fill_price - open_position.entry_fill_price) / open_position.entry_fill_price, 4)
    pnl_dollars = round(exit_fill_price - open_position.entry_fill_price, 4)
    exit_underlying_price = latest_snapshot["underlying_quote"]["last"]
    hold_minutes = int((snapshot_time - open_position.timestamp).total_seconds() // 60)
    dte = (open_position.selected_contract.expiration - snapshot_time.date()).days

    if open_position.execution_layer == ExecutionLayer.TACTICAL:
        if option_return_pct >= rules.tactical_profit_target_pct:
            return _decision("close", "profit_target", exit_fill_price, bid, ask, mid, spread_pct, exit_fill_model, exit_underlying_price, option_return_pct, pnl_dollars, hold_minutes)
        if option_return_pct <= -rules.tactical_stop_loss_pct:
            return _decision("close", "stop_loss", exit_fill_price, bid, ask, mid, spread_pct, exit_fill_model, exit_underlying_price, option_return_pct, pnl_dollars, hold_minutes)
        if snapshot_time.timetz().replace(tzinfo=None) >= rules.tactical_eod_flatten_time:
            return _decision("close", "eod_flatten", exit_fill_price, bid, ask, mid, spread_pct, exit_fill_model, exit_underlying_price, option_return_pct, pnl_dollars, hold_minutes)
    else:
        if option_return_pct >= rules.rider_profit_target_pct:
            return _decision("close", "profit_target", exit_fill_price, bid, ask, mid, spread_pct, exit_fill_model, exit_underlying_price, option_return_pct, pnl_dollars, hold_minutes)
        if option_return_pct <= -rules.rider_stop_loss_pct:
            return _decision("close", "stop_loss", exit_fill_price, bid, ask, mid, spread_pct, exit_fill_model, exit_underlying_price, option_return_pct, pnl_dollars, hold_minutes)
        if dte < rules.rider_min_dte:
            return _decision("close", "dte_floor", exit_fill_price, bid, ask, mid, spread_pct, exit_fill_model, exit_underlying_price, option_return_pct, pnl_dollars, hold_minutes)

    return ExitDecision("hold", None, None, None, None, None, None, None, None, None, None, None)


def _decision(
    action: str,
    reason: str,
    fill_price: float,
    bid: float,
    ask: float,
    mid: float,
    spread_pct: float | None,
    exit_fill_model: str,
    underlying_price: float | None,
    option_return_pct: float,
    pnl_dollars: float,
    hold_minutes: int,
) -> ExitDecision:
    return ExitDecision(
        exit_action=action,
        exit_reason=reason,
        exit_fill_price=fill_price,
        exit_option_bid=bid,
        exit_option_ask=ask,
        exit_option_mid=mid,
        exit_spread_pct=spread_pct,
        exit_fill_model=exit_fill_model,
        exit_underlying_price=underlying_price,
        option_return_pct=option_return_pct,
        pnl_dollars=pnl_dollars,
        hold_minutes=hold_minutes,
    )


def _option_quote(snapshot: dict[str, Any], option_symbol: str) -> dict[str, Any] | None:
    for contract in snapshot["option_chain"]:
        if contract["option_symbol"] == option_symbol:
            return contract
    return None


def _parse_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _exit_fill(bid: float, ask: float, mid: float, rules: ExitRules) -> tuple[float, str]:
    if rules.fill_model == "optimistic_mid":
        return round(mid, 4), "mid"
    if rules.fill_model in {"conservative", "stress"}:
        return round(bid, 4), "bid"
    penalty = (ask - bid) * 0.10
    return round(max(bid, mid - penalty), 4), "mid_minus_slippage"


def _unresolved(reason: str) -> ExitDecision:
    return ExitDecision("unresolved", reason, None, None, None, None, None, None, None, None, None, None)
