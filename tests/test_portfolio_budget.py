from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest

from autobott_v2 import portfolio_budget as budget
from autobott_v2.execution_models import BrokerEnvironment, ExecutionOrder, ExecutionState, OrderSide, OrderType, TradeIntent


NOW = datetime(2026, 9, 21, 19, tzinfo=UTC)


def intent(index=0, dollars=100):
    return TradeIntent(symbol=f"TEST{index}", option_symbol=f"TEST{index}261002C00100000",
        side=OrderSide.BUY_TO_OPEN, quantity=1, limit_price=dollars / 100, generated_at=NOW,
        order_type=OrderType.LIMIT, decision_id=f"decision-{index}")


def snapshot(*, held=0, pending=0, available=1000000, scope="alpaca:paper:test", symbols=(), orders=None):
    return budget.BudgetSnapshot(scope, held, pending, available, frozenset(symbols), orders or {}, NOW)


def reserve(ledger, intents, *, snap=None, cap=1000):
    return ledger.reserve(tuple(intents), limit_dollars=cap, read_snapshot=lambda: snap or snapshot(), max_legs=60)


def test_more_than_six_small_entries_fit_without_raising_total_premium(tmp_path):
    ledger = budget.PremiumLedger(tmp_path / "budget.sqlite")
    receipts = [reserve(ledger, [intent(i, 50)])[0] for i in range(20)]
    assert receipts[-1]["remaining_dollars"] == 0
    assert receipts[-1]["uncertain_submission_dollars"] == 950
    with pytest.raises(budget.BudgetBlocked, match="portfolio_premium_budget_exceeded"):
        reserve(ledger, [intent(21, 1)])


def test_restart_keeps_uncertain_orders_reserved_and_does_not_replay(tmp_path):
    path = tmp_path / "budget.sqlite"
    ledger = budget.PremiumLedger(path)
    _, specs = reserve(ledger, [intent(1, 800)])
    ledger.mark_attempted(specs[0])
    restarted = budget.PremiumLedger(path)
    with pytest.raises(budget.BudgetBlocked, match="portfolio_premium_budget_exceeded"):
        reserve(restarted, [intent(2, 201)])
    with pytest.raises(budget.BudgetBlocked, match="portfolio_underlying_already_reserved"):
        reserve(restarted, [intent(1, 100)])
    receipt, _ = reserve(restarted, [intent(2, 200)])
    assert receipt["remaining_dollars"] == 0


def test_pair_is_reserved_in_full_before_either_leg_can_be_attempted(tmp_path):
    ledger = budget.PremiumLedger(tmp_path / "budget.sqlite")
    primary = intent(0, 800)
    runner = replace(primary, option_symbol="TEST0261002C00105000", limit_price=3)
    with pytest.raises(budget.BudgetBlocked, match="portfolio_premium_budget_exceeded"):
        reserve(ledger, [primary, runner])
    receipt, specs = reserve(ledger, [intent(1, 1000)])
    assert receipt["remaining_dollars"] == 0
    assert len(specs) == 1


def test_competing_database_connections_cannot_overspend(tmp_path):
    path = tmp_path / "budget.sqlite"
    def attempt(index):
        try:
            return reserve(budget.PremiumLedger(path), [intent(index, 200)])[0]
        except budget.BudgetBlocked as exc:
            assert str(exc) == "portfolio_premium_budget_exceeded"
            return None
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(attempt, range(12)))
    assert sum(row is not None for row in results) == 5
    assert sum(row["reserved_dollars"] for row in results if row) == 1000


def test_cash_budget_is_not_margin_buying_power(tmp_path):
    ledger = budget.PremiumLedger(tmp_path / "budget.sqlite")
    with pytest.raises(budget.BudgetBlocked, match="portfolio_available_cash_exceeded"):
        reserve(ledger, [intent(1, 101)], snap=snapshot(available=10000))


def test_working_buys_and_held_premium_both_consume_budget(tmp_path):
    ledger = budget.PremiumLedger(tmp_path / "budget.sqlite")
    with pytest.raises(budget.BudgetBlocked, match="portfolio_premium_budget_exceeded"):
        reserve(ledger, [intent(1, 201)], snap=snapshot(held=50000, pending=30000))


def test_unattempted_can_release_but_attempted_never_releases_on_cleanup(tmp_path):
    ledger = budget.PremiumLedger(tmp_path / "budget.sqlite")
    _, first = reserve(ledger, [intent(0, 600)])
    ledger.release_unattempted(first)
    _, second = reserve(ledger, [intent(1, 700)])
    ledger.mark_attempted(second[0])
    ledger.release_unattempted(second)
    with pytest.raises(budget.BudgetBlocked, match="portfolio_premium_budget_exceeded"):
        reserve(ledger, [intent(2, 301)])


def test_account_scope_does_not_borrow_another_accounts_reservations(tmp_path):
    ledger = budget.PremiumLedger(tmp_path / "budget.sqlite")
    reserve(ledger, [intent(1, 1000)], snap=snapshot(scope="alpaca:paper:first"))
    receipt, _ = reserve(ledger, [intent(1, 1000)], snap=snapshot(scope="alpaca:paper:second"))
    assert receipt["uncertain_submission_dollars"] == 0


def test_exact_broker_receipt_replaces_reservation_not_real_exposure(tmp_path):
    ledger = budget.PremiumLedger(tmp_path / "budget.sqlite")
    _, specs = reserve(ledger, [intent(0, 800)])
    ledger.mark_attempted(specs[0])
    order = {"symbol": specs[0]["symbol"], "side": "buy", "qty": "1", "type": "limit", "limit_price": "8", "status": "filled"}
    observed = snapshot(held=80000, orders={specs[0]["client"]: order}, symbols=[specs[0]["symbol"]])
    receipt, _ = reserve(ledger, [intent(1, 200)], snap=observed)
    assert receipt["uncertain_submission_dollars"] == 0
    assert receipt["held_dollars"] == 800
    assert receipt["remaining_dollars"] == 0


@pytest.mark.parametrize("field,value", [("symbol", "SPY261002C00100000"), ("qty", "2"), ("type", "market"), ("limit_price", "9")])
def test_conflicting_receipt_does_not_free_reservation(tmp_path, field, value):
    ledger = budget.PremiumLedger(tmp_path / "budget.sqlite")
    _, specs = reserve(ledger, [intent(0, 800)])
    order = {"symbol": specs[0]["symbol"], "side": "buy", "qty": "1", "type": "limit", "limit_price": "8", "status": "filled", field: value}
    with pytest.raises(budget.BudgetBlocked, match="portfolio_reservation_receipt_conflict"):
        reserve(ledger, [intent(1, 200)], snap=snapshot(orders={specs[0]["client"]: order}))


@pytest.mark.parametrize("price", [float("nan"), float("inf"), -1, 0, True, 1.001])
def test_invalid_order_money_never_reserves(tmp_path, price):
    ledger = budget.PremiumLedger(tmp_path / "budget.sqlite")
    with pytest.raises(budget.BudgetBlocked):
        reserve(ledger, [replace(intent(), limit_price=price)])


def test_uncapped_market_order_is_not_allowed_to_consume_capped_capacity(tmp_path):
    ledger = budget.PremiumLedger(tmp_path / "budget.sqlite")
    with pytest.raises(budget.BudgetBlocked, match="portfolio_entry_requires_limit_order"):
        reserve(ledger, [replace(intent(), order_type=OrderType.MARKET)])


def fixture_broker(*, mark="120", qty="1", pending=None):
    account = {"id": "test", "status": "ACTIVE", "currency": "USD", "trading_blocked": False,
               "cash": "10000", "options_buying_power": "20000"}
    position = {"symbol": "OLD261002C00100000", "asset_class": "us_option", "side": "long", "qty": qty,
                "cost_basis": str(Decimal(qty) * 100), "market_value": mark}
    fill = {"id": "old-fill", "client_order_id": "old-client", "symbol": position["symbol"], "side": "buy",
            "status": "filled", "qty": qty, "filled_qty": qty, "filled_avg_price": "1", "filled_at": NOW.isoformat(),
            "type": "limit", "limit_price": "1", "asset_class": "us_option"}
    rows = [fill] + ([pending] if pending else [])
    return SimpleNamespace(config=SimpleNamespace(environment=BrokerEnvironment.PAPER, allow_live_trading=False,
        trading_base_url="https://paper-api.alpaca.markets"), get_account=lambda: dict(account),
        list_open_positions=lambda: [dict(position)], list_order_history=lambda **kwargs: rows)


@pytest.mark.parametrize("mark,expected", [("20", 10000), ("120", 12000)])
def test_funded_runner_and_losses_never_disappear_from_premium_budget(mark, expected):
    result = budget.read_budget_snapshot(fixture_broker(mark=mark))
    assert result.held_cents == expected
    assert result.available_cents == 1000000


def test_pending_exit_does_not_create_available_capacity():
    sell = {"id": "exit", "symbol": "OLD261002C00100000", "side": "sell", "status": "pending_cancel",
            "qty": "1", "filled_qty": "0", "type": "limit", "limit_price": "1.2", "position_intent": "sell_to_close"}
    result = budget.read_budget_snapshot(fixture_broker(pending=sell))
    assert result.held_cents == 12000
    assert result.working_buy_cents == 0


def test_partial_buy_counts_filled_premium_and_unfilled_limit_quantity():
    pending = {"id": "partial", "client_order_id": "partial-client", "symbol": "OLD261002C00100000", "side": "buy",
               "status": "partially_filled", "qty": "3", "filled_qty": "1", "filled_avg_price": "1",
               "filled_at": NOW.isoformat(), "type": "limit", "limit_price": "1", "position_intent": "buy_to_open"}
    broker = fixture_broker(qty="2", mark="200")
    old = broker.list_order_history()[0]
    old.update(qty="1", filled_qty="1")
    broker.list_order_history = lambda **kwargs: [old, pending]
    result = budget.read_budget_snapshot(broker)
    assert result.held_cents == 20000
    assert result.working_buy_cents == 20000


def test_missing_position_or_fill_data_cannot_be_treated_as_zero():
    broker = fixture_broker()
    broker.list_order_history = lambda **kwargs: []
    with pytest.raises(budget.BudgetBlocked, match="portfolio_fills_and_positions_disagree"):
        budget.read_budget_snapshot(broker)


def test_pair_context_reuses_precommitted_ids_and_cannot_claim_twice(tmp_path, monkeypatch):
    ledger = budget.PremiumLedger(tmp_path / "budget.sqlite")
    monkeypatch.setattr(budget, "_ledger", lambda: ledger)
    monkeypatch.setattr(budget, "read_budget_snapshot", lambda broker: snapshot())
    broker = SimpleNamespace(config=SimpleNamespace(portfolio_premium_limit=1000, effective_max_open_positions=lambda: 60))
    primary = intent(0, 500)
    runner = replace(primary, option_symbol="TEST0261002C00105000", limit_price=1)
    order = lambda item: ExecutionOrder("internal", "unused", item, ExecutionState.APPROVED)
    with budget.reserve_pair(broker, (primary, runner)) as receipt:
        prepared = budget.prepare_order(broker, order(primary))
        assert prepared.client_order_id != "unused"
        assert receipt["reserved_dollars"] == 600
        with pytest.raises(budget.BudgetBlocked, match="portfolio_reservation_not_available"):
            budget.prepare_order(broker, order(primary))
    next_receipt, _ = reserve(ledger, [intent(1, 500)])
    assert next_receipt["uncertain_submission_dollars"] == 500
    assert next_receipt["remaining_dollars"] == 0
