from __future__ import annotations

from datetime import datetime, timezone

from autobott_v2.execution_models import BrokerEnvironment, ExecutionOrder, ExecutionState, OrderSide, TradeIntent
from autobott_v2.position_store import load_open_positions, upsert_open_position_from_order


def test_upsert_open_position_from_order_round_trips(tmp_path) -> None:
    order = ExecutionOrder(
        order_id="order-1",
        client_order_id="client-1",
        intent=TradeIntent(
            symbol="AAPL",
            option_symbol="AAPL260117C00190000",
            side=OrderSide.BUY_TO_OPEN,
            quantity=1,
            limit_price=2.5,
            generated_at=datetime(2026, 7, 1, 15, 30, tzinfo=timezone.utc),
            environment=BrokerEnvironment.PAPER,
            take_profit_price=3.75,
            stop_loss_price=1.75,
            decision_id="decision-123",
            thesis_id="thesis-123",
        ),
        state=ExecutionState.SUBMITTED,
        submitted_at=datetime(2026, 7, 1, 15, 31, tzinfo=timezone.utc),
        broker_order_id="alpaca-order-1",
    )
    path = tmp_path / "open_positions.json"
    upsert_open_position_from_order(order, store_path=path)
    rows = load_open_positions(store_path=path)
    assert len(rows) == 1
    assert rows[0].broker_order_id == "alpaca-order-1"
    assert rows[0].status == "submitted"
