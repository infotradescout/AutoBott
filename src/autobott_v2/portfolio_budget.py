"""Paper-only premium admission; never submits or cancels orders.

Premium capacity is not a guaranteed realized-loss limit. Fees, exercise and
external account activity remain separate. Uncertain submissions stay reserved
until exact broker-order evidence accounts for them.
"""
from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR
from hashlib import sha256
import json
from pathlib import Path
import re
import sqlite3
from typing import Any, Callable, Iterator
from uuid import uuid4


class BudgetBlocked(ValueError):
    """Fixed public reason code, without credentials or raw account payloads."""


def _number(value: Any, reason: str, *, positive: bool = False) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        raise BudgetBlocked(reason) from None
    if isinstance(value, bool) or not result.is_finite() or result < 0 or (positive and result <= 0):
        raise BudgetBlocked(reason)
    return result


def _quantity(value: Any) -> Decimal:
    result = _number(value, "portfolio_quantity_invalid")
    if result != result.to_integral_value():
        raise BudgetBlocked("portfolio_quantity_invalid")
    return result


def _cents(value: Decimal, *, ceiling: bool = True) -> int:
    return int((value * 100).to_integral_value(rounding=ROUND_CEILING if ceiling else ROUND_FLOOR))


def _root(symbol: str) -> str:
    match = re.fullmatch(r"([A-Z][A-Z0-9.]*?)\d{6}[CP]\d{8}", symbol)
    if not match:
        raise BudgetBlocked("portfolio_option_identity_invalid")
    root = match[1]
    return "VOLATILITY" if root in {"VIX", "VIXW", "VXX", "UVXY"} else root


@dataclass(frozen=True)
class BudgetSnapshot:
    account_scope: str
    held_cents: int
    working_buy_cents: int
    available_cents: int
    active_symbols: frozenset[str]
    order_by_client: dict[str, dict]
    observed_at: datetime


def _positions(rows: Any) -> dict[str, dict]:
    if not isinstance(rows, list):
        raise BudgetBlocked("portfolio_positions_unavailable")
    result = {}
    for row in rows:
        if not isinstance(row, dict) or row.get("asset_class") != "us_option" or row.get("side") != "long":
            raise BudgetBlocked("portfolio_requires_long_options_only")
        symbol = row.get("symbol")
        if not isinstance(symbol, str) or symbol in result:
            raise BudgetBlocked("portfolio_position_identity_ambiguous")
        _root(symbol)
        qty = _quantity(row.get("qty"))
        if qty <= 0:
            raise BudgetBlocked("portfolio_quantity_invalid")
        cost = _number(row.get("cost_basis"), "portfolio_basis_unavailable")
        mark = _number(row.get("market_value"), "portfolio_mark_unavailable")
        result[symbol] = {"qty": qty, "cost": cost, "mark": mark}
    return result


def read_budget_snapshot(broker: Any) -> BudgetSnapshot:
    """Require account identity, complete fill reads and stable held quantities."""
    from .trade_outcomes import match_broker_order_lots
    environment = getattr(broker.config.environment, "value", broker.config.environment)
    if (environment != "paper" or broker.config.allow_live_trading
            or broker.config.trading_base_url.rstrip("/") != "https://paper-api.alpaca.markets"):
        raise BudgetBlocked("portfolio_paper_only_required")
    account = broker.get_account()
    if not isinstance(account, dict) or not isinstance(account.get("id"), str) or not account["id"]:
        raise BudgetBlocked("portfolio_account_identity_unavailable")
    if account.get("status") != "ACTIVE" or account.get("currency") != "USD":
        raise BudgetBlocked("portfolio_account_not_active_usd")
    if account.get("trading_blocked") is not False:
        raise BudgetBlocked("portfolio_account_trading_status_unavailable")
    buying_power = _number(account.get("options_buying_power"), "portfolio_buying_power_unavailable")
    cash = _number(account.get("cash"), "portfolio_cash_unavailable")
    before = _positions(broker.list_open_positions())
    orders = broker.list_order_history(status="all")
    if not isinstance(orders, list) or any(not isinstance(row, dict) for row in orders):
        raise BudgetBlocked("portfolio_order_history_unavailable")
    after = _positions(broker.list_open_positions())
    if ({s: (r["qty"], r["cost"]) for s, r in before.items()}
            != {s: (r["qty"], r["cost"]) for s, r in after.items()}):
        raise BudgetBlocked("portfolio_positions_changed_during_read")
    account_after = broker.get_account()
    if not isinstance(account_after, dict) or account_after.get("id") != account["id"]:
        raise BudgetBlocked("portfolio_account_changed_during_read")
    if (account_after.get("trading_blocked") is not False or account_after.get("status") != "ACTIVE"
            or account_after.get("currency") != "USD"):
        raise BudgetBlocked("portfolio_account_not_active_usd")
    buying_power = min(buying_power, _number(account_after.get("options_buying_power"), "portfolio_buying_power_unavailable"))
    cash = min(cash, _number(account_after.get("cash"), "portfolio_cash_unavailable"))
    try:
        matched = match_broker_order_lots(orders, include_nonterminal_fills=True)
    except (ValueError, TypeError, KeyError, OverflowError):
        raise BudgetBlocked("portfolio_fill_history_invalid") from None
    quantities: dict[str, Decimal] = {}
    lot_cost: dict[str, Decimal] = {}
    working = 0
    active_symbols = set(after)
    for row in matched["pending"]:
        if row.get("pending_kind") == "open_filled_buy":
            symbol = row["symbol"]
            qty = _quantity(row["remaining_filled_qty"])
            if not row.get("broker_order_id"):
                raise BudgetBlocked("portfolio_open_lot_identity_unavailable")
            quantities[symbol] = quantities.get(symbol, Decimal(0)) + qty
            lot_cost[symbol] = lot_cost.get(symbol, Decimal(0)) + qty * 100 * _number(row["filled_avg_price"], "portfolio_fill_price_invalid")
    if quantities != {s: r["qty"] for s, r in after.items()}:
        raise BudgetBlocked("portfolio_fills_and_positions_disagree")
    by_client: dict[str, dict] = {}
    pending_sells: dict[str, Decimal] = {}
    # done_for_day can resume next session; it is not a final risk release.
    terminal = {"filled", "canceled", "cancelled", "expired", "replaced", "rejected"}
    for row in orders:
        client = row.get("client_order_id")
        if isinstance(client, str) and client:
            if client in by_client and by_client[client] != row:
                raise BudgetBlocked("portfolio_order_identity_ambiguous")
            by_client[client] = row
        if str(row.get("status") or "").lower() in terminal:
            continue
        if row.get("order_class") not in (None, "", "simple") or row.get("legs"):
            raise BudgetBlocked("portfolio_complex_open_order_unpriced")
        if row.get("side") == "sell":
            if row.get("position_intent") not in (None, "sell_to_close"):
                raise BudgetBlocked("portfolio_short_order_unsupported")
            symbol = str(row.get("symbol") or "")
            _root(symbol)
            remaining = _quantity(row.get("qty")) - _quantity(row.get("filled_qty"))
            if remaining < 0:
                raise BudgetBlocked("portfolio_filled_quantity_exceeds_order")
            pending_sells[symbol] = pending_sells.get(symbol, Decimal(0)) + remaining
            if pending_sells[symbol] > after.get(symbol, {}).get("qty", Decimal(0)):
                raise BudgetBlocked("portfolio_pending_sell_not_covered")
            continue
        if row.get("side") != "buy" or row.get("position_intent") not in (None, "buy_to_open"):
            raise BudgetBlocked("portfolio_open_order_unclassified")
        symbol = str(row.get("symbol") or "")
        _root(symbol)
        if row.get("type") != "limit":
            raise BudgetBlocked("portfolio_working_buy_has_no_price_cap")
        quantity, filled = _quantity(row.get("qty")), _quantity(row.get("filled_qty"))
        if filled > quantity:
            raise BudgetBlocked("portfolio_filled_quantity_exceeds_order")
        price = _number(row.get("limit_price"), "portfolio_working_limit_invalid", positive=True)
        working += _cents((quantity - filled) * price * 100)
        active_symbols.add(symbol)
    held = sum(_cents(max(r["cost"], r["mark"], before[s]["mark"], lot_cost.get(s, Decimal(0)))) for s, r in after.items())
    return BudgetSnapshot("alpaca:paper:" + account["id"], held, working,
        _cents(min(buying_power, cash), ceiling=False), frozenset(active_symbols), by_client, datetime.now(UTC))


def _order_spec(intent: Any) -> dict:
    if getattr(intent.environment, "value", intent.environment) != "paper":
        raise BudgetBlocked("portfolio_paper_only_required")
    if getattr(intent.side, "value", intent.side) != "buy_to_open":
        raise BudgetBlocked("portfolio_open_buy_required")
    if getattr(intent.order_type, "value", intent.order_type) != "limit":
        raise BudgetBlocked("portfolio_entry_requires_limit_order")
    qty = _quantity(intent.quantity)
    price = _number(intent.limit_price, "portfolio_limit_price_invalid", positive=True)
    if qty <= 0 or price.as_tuple().exponent < -2:
        raise BudgetBlocked("portfolio_order_precision_invalid")
    symbol = str(intent.option_symbol)
    _root(symbol)
    identity = str(intent.decision_id or intent.thesis_id or "")
    if not identity:
        raise BudgetBlocked("portfolio_decision_identity_required")
    content = {"decision": identity, "symbol": symbol, "quantity": str(qty.normalize()), "limit": str(price.normalize())}
    key = sha256(json.dumps(content, sort_keys=True).encode()).hexdigest()
    return {"key": key, "symbol": symbol, "quantity": str(qty), "limit": str(price),
            "cents": _cents(qty * price * 100), "client": "autobott-" + str(uuid4())}


class PremiumLedger:
    """SQLite serializes processes; reservations commit before broker requests."""
    def __init__(self, path: Path):
        self.path = Path(path)

    def _connect(self):
        if self.path.is_symlink():
            raise BudgetBlocked("portfolio_ledger_symlink_refused")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path, timeout=10)
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("CREATE TABLE IF NOT EXISTS reservations (scope TEXT NOT NULL, key TEXT NOT NULL, client TEXT NOT NULL, symbol TEXT NOT NULL, quantity TEXT NOT NULL, price TEXT NOT NULL, cents INTEGER NOT NULL CHECK(cents>0), state TEXT NOT NULL, PRIMARY KEY(scope,key), UNIQUE(scope,client))")
        connection.execute("CREATE TABLE IF NOT EXISTS released_reservations (scope TEXT NOT NULL, key TEXT NOT NULL, client TEXT NOT NULL, symbol TEXT NOT NULL, quantity TEXT NOT NULL, price TEXT NOT NULL, cents INTEGER NOT NULL CHECK(cents>0), state TEXT NOT NULL, PRIMARY KEY(scope,client))")
        return connection

    def reserve(self, intents: tuple[Any, ...], *, limit_dollars: Any,
                read_snapshot: Callable[[], BudgetSnapshot], max_legs: int) -> tuple[dict, list[dict]]:
        specs = [_order_spec(intent) for intent in intents]
        if not specs or len({s["symbol"] for s in specs}) != len(specs):
            raise BudgetBlocked("portfolio_duplicate_entry_contract")
        cap = _cents(_number(limit_dollars, "portfolio_budget_invalid", positive=True), ceiling=False)
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            snapshot = read_snapshot()
            if (not isinstance(snapshot, BudgetSnapshot) or not snapshot.account_scope.startswith("alpaca:paper:")
                    or any(type(n) is not int or n < 0 for n in (snapshot.held_cents, snapshot.working_buy_cents, snapshot.available_cents))):
                raise BudgetBlocked("portfolio_snapshot_invalid")
            rows = connection.execute("SELECT key,client,symbol,quantity,price,cents,state FROM reservations WHERE scope=? AND state IN ('reserved','attempted')", (snapshot.account_scope,)).fetchall()
            uncertain = 0
            reserved_symbols = set()
            for key, client, symbol, quantity, price, cents, state in rows:
                order = snapshot.order_by_client.get(client)
                if order is not None:
                    if (order.get("symbol") != symbol or str(order.get("side")) != "buy"
                            or _quantity(order.get("qty")) != Decimal(quantity) or order.get("type") != "limit"
                            or _number(order.get("limit_price"), "portfolio_receipt_price_invalid") > Decimal(price)):
                        raise BudgetBlocked("portfolio_reservation_receipt_conflict")
                    connection.execute("UPDATE reservations SET state='broker_observed' WHERE scope=? AND key=?", (snapshot.account_scope, key))
                else:
                    uncertain += cents
                    reserved_symbols.add(symbol)
            active = set(snapshot.active_symbols) | reserved_symbols
            if {_root(s["symbol"]) for s in specs} & {_root(s) for s in active}:
                raise BudgetBlocked("portfolio_underlying_already_reserved")
            required = sum(s["cents"] for s in specs)
            used = snapshot.held_cents + snapshot.working_buy_cents + uncertain
            if used + required > cap:
                raise BudgetBlocked("portfolio_premium_budget_exceeded")
            if required + uncertain > snapshot.available_cents:
                raise BudgetBlocked("portfolio_available_cash_exceeded")
            if type(max_legs) is not int or max_legs <= 0 or len(active) + len(specs) > max_legs:
                raise BudgetBlocked("portfolio_operational_position_limit")
            for spec in specs:
                previous = connection.execute(
                    "SELECT scope,key,client,symbol,quantity,price,cents,state FROM reservations WHERE scope=? AND key=?",
                    (snapshot.account_scope, spec["key"])).fetchone()
                if previous is not None:
                    if previous[7] != "not_submitted":
                        raise BudgetBlocked("portfolio_entry_already_reserved")
                    # Only a proven unattempted release can be retried. Preserve
                    # its record and mint a new token to invalidate stale callers.
                    connection.execute("INSERT INTO released_reservations VALUES(?,?,?,?,?,?,?,?)", previous)
                    connection.execute(
                        "UPDATE reservations SET client=?,symbol=?,quantity=?,price=?,cents=?,state='reserved' WHERE scope=? AND key=? AND state='not_submitted'",
                        (spec["client"], spec["symbol"], spec["quantity"], spec["limit"], spec["cents"], snapshot.account_scope, spec["key"]))
                else:
                    try:
                        connection.execute("INSERT INTO reservations(scope,key,client,symbol,quantity,price,cents,state) VALUES(?,?,?,?,?,?,?,?)",
                            (snapshot.account_scope, spec["key"], spec["client"], spec["symbol"], spec["quantity"], spec["limit"], spec["cents"], "reserved"))
                    except sqlite3.IntegrityError:
                        raise BudgetBlocked("portfolio_entry_already_reserved") from None
            connection.commit()
            receipt = {"version": "paper_premium_budget.v1", "account_scope_hash": sha256(snapshot.account_scope.encode()).hexdigest(),
                "cap_dollars": cap / 100, "held_dollars": snapshot.held_cents / 100,
                "working_buy_dollars": snapshot.working_buy_cents / 100,
                "uncertain_submission_dollars": uncertain / 100, "reserved_dollars": required / 100,
                "remaining_dollars": (cap - used - required) / 100}
            for spec in specs:
                spec["scope"] = snapshot.account_scope
            return receipt, specs
        finally:
            connection.close()

    def mark_attempted(self, spec: dict) -> None:
        connection = self._connect()
        try:
            with connection:
                cursor = connection.execute("UPDATE reservations SET state='attempted' WHERE scope=? AND key=? AND client=? AND state='reserved'", (spec["scope"], spec["key"], spec["client"]))
                if cursor.rowcount != 1:
                    raise BudgetBlocked("portfolio_reservation_not_available")
        finally:
            connection.close()

    def release_unattempted(self, specs: list[dict]) -> None:
        connection = self._connect()
        try:
            with connection:
                for spec in specs:
                    connection.execute("UPDATE reservations SET state='not_submitted' WHERE scope=? AND key=? AND client=? AND state='reserved'", (spec["scope"], spec["key"], spec["client"]))
        finally:
            connection.close()


_GROUP: ContextVar[Any] = ContextVar("autobott_premium_group", default=None)


def enabled(broker: Any) -> bool:
    return getattr(broker.config, "portfolio_premium_limit", None) is not None


def _ledger() -> PremiumLedger:
    from .runtime_paths import data_root
    return PremiumLedger(data_root() / "execution" / "premium_reservations.sqlite3")


@contextmanager
def reserve_pair(broker: Any, intents: tuple[Any, ...]) -> Iterator[dict | None]:
    if not enabled(broker):
        yield None
        return
    if _GROUP.get() is not None:
        raise BudgetBlocked("portfolio_nested_group_refused")
    ledger = _ledger()
    receipt, specs = ledger.reserve(intents, limit_dollars=broker.config.portfolio_premium_limit,
        read_snapshot=lambda: read_budget_snapshot(broker), max_legs=broker.config.effective_max_open_positions())
    token = _GROUP.set((id(broker), ledger, {s["key"]: s for s in specs}))
    try:
        yield receipt
    finally:
        _GROUP.reset(token)
        try:
            ledger.release_unattempted(specs)
        except (OSError, sqlite3.Error, BudgetBlocked):
            print('AUTOBOTT_PREMIUM_LEDGER {"cleanup":"unconfirmed","reservations_retained":true}', flush=True)


def bounded_intent(intent: Any) -> Any:
    from .execution_models import OrderType
    if getattr(intent.side, "value", intent.side) != "buy_to_open":
        return intent
    return replace(intent, order_type=OrderType.LIMIT,
        metadata={**intent.metadata, "premium_budget_price_cap": intent.limit_price})


def prepare_order(broker: Any, order: Any) -> Any:
    """Apply a bounded price and a durable one-use client ID before submission."""
    if not enabled(broker) or getattr(order.intent.side, "value", order.intent.side) != "buy_to_open":
        return order
    order = replace(order, intent=bounded_intent(order.intent))
    spec = _order_spec(order.intent)
    group = _GROUP.get()
    if group is not None:
        owner, ledger, specs = group
        if owner != id(broker) or spec["key"] not in specs:
            raise BudgetBlocked("portfolio_group_intent_mismatch")
        spec = specs[spec["key"]]
    else:
        ledger = _ledger()
        _, specs = ledger.reserve((order.intent,), limit_dollars=broker.config.portfolio_premium_limit,
            read_snapshot=lambda: read_budget_snapshot(broker), max_legs=broker.config.effective_max_open_positions())
        spec = specs[0]
    ledger.mark_attempted(spec)
    return replace(order, client_order_id=spec["client"])


def submit_budgeted_pair(decision: Any, pair: Any, **kwargs: Any) -> Any:
    """Reserve both legs before invoking the unchanged native pair submitter."""
    from .execution_orchestrator import build_trade_intent_from_decision, submit_core_runner_to_broker, ExecutionRejectedError
    from .execution_models import OrderType
    broker = kwargs.get("broker")
    if broker is None or not enabled(broker):
        return submit_core_runner_to_broker(decision, pair, **kwargs)
    group_id = f"core-runner:{decision.decision_id}"
    intents = tuple(build_trade_intent_from_decision(decision, quantity=1,
        environment=broker.config.environment, max_position_cost=broker.config.effective_max_position_cost(),
        contract=contract, leg_role=role, trade_group_id=group_id, paired_option_symbol=other.option_symbol,
        order_type=OrderType.LIMIT) for contract, role, other in (
            (pair.primary, "primary", pair.runner), (pair.runner, "runner", pair.primary)))
    try:
        with reserve_pair(broker, intents):
            return submit_core_runner_to_broker(decision, pair, **kwargs)
    except BudgetBlocked as exc:
        raise ExecutionRejectedError(str(exc)) from exc
