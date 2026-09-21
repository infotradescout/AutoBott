"""Read-only capacity diagnostics, not authority to submit an order."""
from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
import os
from pathlib import Path
import sqlite3
import threading
import time
from typing import Any

from .execution_config import PORTFOLIO_PREMIUM_ENVELOPE, load_alpaca_execution_config, portfolio_mode_enabled
from .portfolio_budget import BudgetBlocked, _number, _quantity, read_budget_snapshot
from .runtime_paths import data_root


_LOCK = threading.Lock()
_CACHE: tuple[float, bool, dict] | None = None


def _unobserved_reservations(path: Path, snapshot: Any) -> tuple[int, int]:
    if not path.exists():
        return 0, 0
    if path.is_symlink():
        raise BudgetBlocked("portfolio_ledger_symlink_refused")
    connection = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=2)
    try:
        rows = connection.execute("SELECT client,symbol,quantity,price,cents FROM reservations WHERE scope=? AND state IN ('reserved','attempted')", (snapshot.account_scope,)).fetchall()
    finally:
        connection.close()
    cents = count = 0
    for client, symbol, qty, price, amount in rows:
        order = snapshot.order_by_client.get(client)
        if order is not None:
            if (order.get("symbol") != symbol or order.get("side") != "buy" or order.get("type") != "limit"
                    or _quantity(order.get("qty")) != Decimal(qty)
                    or _number(order.get("limit_price"), "portfolio_receipt_price_invalid") > Decimal(price)):
                raise BudgetBlocked("portfolio_reservation_receipt_conflict")
        else:
            cents += amount
            count += 1
    return cents, count


def capacity_status(*, broker: Any = None, ledger_path: Path | None = None) -> dict:
    """Fresh provider reads and a read-only SQLite connection; never creates a file."""
    from .execution_broker import AlpacaExecutionBroker
    config = load_alpaca_execution_config().validate() if broker is None else broker.config
    active = config.portfolio_premium_limit is not None
    cap = config.portfolio_premium_limit if active else PORTFOLIO_PREMIUM_ENVELOPE
    if broker is None:
        # Strict response parsing for the observation client does not change
        # deployed settings or enable order placement anywhere.
        broker = AlpacaExecutionBroker(replace(config, portfolio_premium_limit=cap))
    snapshot = read_budget_snapshot(broker)
    uncertain, count = _unobserved_reservations(ledger_path or data_root() / "execution" / "premium_reservations.sqlite3", snapshot)
    used = snapshot.held_cents + snapshot.working_buy_cents + uncertain
    return {"ok": True, "observed_at": snapshot.observed_at.isoformat(), "allocation_enabled": active,
        "premium_budget_dollars": cap, "held_premium_dollars": snapshot.held_cents / 100,
        "working_buy_dollars": snapshot.working_buy_cents / 100, "uncertain_submission_dollars": uncertain / 100,
        "uncertain_reservations": count, "budget_remaining_dollars": max(0, cap - used / 100),
        "broker_available_cash_dollars": snapshot.available_cents / 100,
        "committed_option_symbols": len(snapshot.active_symbols),
        "configured_position_limit": config.effective_max_open_positions(),
        "per_leg_limit_dollars": config.max_position_cost, "daily_loss_entry_guard_dollars": config.max_daily_loss,
        "new_pairs_per_cycle": config.effective_max_new_entry_attempts_per_loop(),
        "risk_basis": "max_held_cost_or_mark_plus_working_buys_and_uncertain_submissions",
        "fees_included": False, "future_loss_guaranteed": False}


def optional_capacity_status() -> dict | None:
    active = portfolio_mode_enabled()
    observe = os.getenv("AUTOBOTT_PORTFOLIO_BUDGET_OBSERVE", "").strip().lower() in {"true", "1", "yes", "on"}
    if not active and not observe:
        return None
    global _CACHE
    with _LOCK:
        now = time.monotonic()
        if _CACHE is not None and _CACHE[1] == active and now - _CACHE[0] < 20:
            return {**deepcopy(_CACHE[2]), "cached": True}
        try:
            result = capacity_status()
        except BudgetBlocked as exc:
            result = {"ok": False, "allocation_enabled": active, "reason": str(exc), "observed_at": datetime.now(UTC).isoformat()}
        except Exception as exc:
            result = {"ok": False, "allocation_enabled": active, "reason": "portfolio_status_dependency_unavailable",
                      "error_type": type(exc).__name__, "observed_at": datetime.now(UTC).isoformat()}
        _CACHE = (time.monotonic(), active, deepcopy(result))
        return {**result, "cached": False}
