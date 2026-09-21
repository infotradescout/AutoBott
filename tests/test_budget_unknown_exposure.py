import pytest

from autobott_v2 import portfolio_budget as budget
from test_portfolio_budget import fixture_broker


def sell(symbol="OLD261002C00100000", quantity="1", order_id="pending-exit"):
    return {"id": order_id, "symbol": symbol, "side": "sell", "status": "pending_new",
            "qty": quantity, "filled_qty": "0", "type": "limit", "limit_price": "1.2"}


def test_unknown_pending_sell_with_no_held_contract_blocks_budget():
    broker = fixture_broker(pending=sell(symbol="OTHER261002C00100000"))
    with pytest.raises(budget.BudgetBlocked, match="portfolio_pending_sell_not_covered"):
        budget.read_budget_snapshot(broker)


def test_pending_sell_cannot_exceed_held_quantity():
    broker = fixture_broker(pending=sell(quantity="2"))
    with pytest.raises(budget.BudgetBlocked, match="portfolio_pending_sell_not_covered"):
        budget.read_budget_snapshot(broker)


def test_two_pending_exits_cannot_both_claim_the_same_held_contract():
    broker = fixture_broker(pending=sell(order_id="first"))
    orders = broker.list_order_history()
    orders.append(sell(order_id="second"))
    with pytest.raises(budget.BudgetBlocked, match="portfolio_pending_sell_not_covered"):
        budget.read_budget_snapshot(broker)
