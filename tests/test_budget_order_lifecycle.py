import pytest

from autobott_v2 import portfolio_budget as budget
from test_portfolio_budget import fixture_broker


@pytest.mark.parametrize("state,charged", [
    ("new", 20000), ("accepted", 20000), ("pending_cancel", 20000),
    ("pending_replace", 20000), ("done_for_day", 20000),
    ("canceled", 0), ("expired", 0), ("rejected", 0),
])
def test_only_final_unfilled_order_states_release_premium(state, charged):
    pending = {"id": "working", "client_order_id": "working-client", "symbol": "NEW261002C00100000",
        "side": "buy", "status": state, "qty": "2", "filled_qty": "0",
        "type": "limit", "limit_price": "1", "position_intent": "buy_to_open"}
    result = budget.read_budget_snapshot(fixture_broker(pending=pending))
    assert result.working_buy_cents == charged
    assert result.held_cents == 12000


def test_done_for_day_uncovered_sell_still_blocks_new_capacity():
    pending = {"id": "working-exit", "symbol": "NEW261002C00100000", "side": "sell",
        "status": "done_for_day", "qty": "1", "filled_qty": "0", "type": "limit", "limit_price": "1"}
    with pytest.raises(budget.BudgetBlocked, match="portfolio_pending_sell_not_covered"):
        budget.read_budget_snapshot(fixture_broker(pending=pending))
