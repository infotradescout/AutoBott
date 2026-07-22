from __future__ import annotations

from datetime import datetime, timezone

from autobott_v2.execution_journal import load_execution_journal
from autobott_v2.execution_models import BrokerEnvironment, ExecutionOrder, ExecutionState, OrderSide, TradeIntent
from autobott_v2.execution_reconciler import reconcile_open_positions
from autobott_v2.position_store import load_open_positions, upsert_open_position_from_order


class FakeBroker:
    def get_order(self, broker_order_id: str):
        if broker_order_id == "alpaca-order-1":
            return {
                "id": broker_order_id,
                "client_order_id": "client-1",
                "status": "filled",
                "submitted_at": "2026-07-01T15:31:00Z",
            }
        return {}


def test_reconcile_open_positions_updates_status_and_journals(tmp_path) -> None:
    store_path = tmp_path / "open_positions.json"
    journal_path = tmp_path / "execution_orders.jsonl"
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
            metadata={
                "trade_group_id": "core-runner:decision-123",
                "leg_role": "runner",
                "paired_option_symbol": "AAPL260117C00185000",
                "policy_version": "hosted-vix-profit-v1",
                "build_sha": "entry-build-sha",
            },
        ),
        state=ExecutionState.SUBMITTED,
        submitted_at=datetime(2026, 7, 1, 15, 31, tzinfo=timezone.utc),
        broker_order_id="alpaca-order-1",
    )
    upsert_open_position_from_order(order, store_path=store_path)
    summary = reconcile_open_positions(FakeBroker(), store_path=str(store_path), journal_path=str(journal_path))
    rows = load_open_positions(store_path=store_path)
    assert summary.checked == 1
    assert summary.updated == 1
    assert rows[0].status == "filled"
    assert rows[0].trade_group_id == "core-runner:decision-123"
    assert rows[0].leg_role == "runner"
    assert rows[0].paired_option_symbol == "AAPL260117C00185000"
    assert rows[0].entry_policy_version == "hosted-vix-profit-v1"
    assert rows[0].entry_build_sha == "entry-build-sha"
    assert journal_path.exists() is True
    journal_rows = load_execution_journal(journal_path=journal_path)
    metadata = journal_rows[0]["payload"]["intent"]["metadata"]
    assert metadata["policy_version"] == "hosted-vix-profit-v1"
    assert metadata["build_sha"] == "entry-build-sha"
