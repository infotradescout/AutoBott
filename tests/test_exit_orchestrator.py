from __future__ import annotations

import json
from datetime import UTC, datetime

from autobott_v2.execution_config import AlpacaExecutionConfig
from autobott_v2.execution_models import BrokerEnvironment, ExecutionOrder, ExecutionState, OrderSide, TradeIntent
from autobott_v2.exit_orchestrator import build_exit_intent_from_position, cancel_open_order, replace_open_order, submit_exit_for_position
from autobott_v2.position_store import OpenPosition, load_open_positions, save_open_positions
from autobott_v2.runtime_control import default_runtime_state, save_runtime_state


def _config() -> AlpacaExecutionConfig:
    return AlpacaExecutionConfig(
        environment=BrokerEnvironment.PAPER,
        api_key="paper-key",
        secret_key="paper-secret",
        trading_base_url="https://paper-api.alpaca.markets",
        data_base_url="https://data.alpaca.markets",
        allow_live_trading=False,
        allow_order_placement=True,
        max_position_cost=1000.0,
        max_daily_loss=500.0,
        max_open_positions=3,
    )


def _position() -> OpenPosition:
    return OpenPosition(
        broker_order_id="alpaca-entry-1",
        decision_id="decision-123",
        symbol="AAPL",
        option_symbol="AAPL260117C00190000",
        quantity=1,
        entry_limit_price=2.5,
        entry_submitted_at=datetime(2026, 7, 1, 15, 31, tzinfo=UTC),
        take_profit_price=3.75,
        stop_loss_price=1.75,
        status="filled",
    )


class FakeBroker:
    def __init__(self) -> None:
        self.config = _config()

    def submit_order(self, intent, *, current_daily_realized_pnl=0.0, open_positions=0):
        return ExecutionOrder(
            order_id="exit-order-1",
            client_order_id="client-exit-1",
            intent=intent,
            state=ExecutionState.SUBMITTED,
            submitted_at=datetime(2026, 7, 1, 16, 0, tzinfo=UTC),
            broker_order_id="alpaca-exit-1",
        )

    def cancel_order(self, broker_order_id: str):
        return {"id": broker_order_id, "status": "canceled", "symbol": "AAPL260117C00190000", "qty": "1", "limit_price": "2.50"}

    def replace_order(self, broker_order_id: str, *, limit_price: float):
        return {"id": broker_order_id, "status": "new", "symbol": "AAPL260117C00190000", "qty": "1", "limit_price": f"{limit_price:.2f}"}


def test_build_exit_intent_from_position_creates_sell_to_close() -> None:
    intent = build_exit_intent_from_position(_position(), limit_price=3.1)
    assert intent.side is OrderSide.SELL_TO_CLOSE
    assert intent.limit_price == 3.1


def test_submit_exit_for_position_marks_position_closing(tmp_path) -> None:
    save_runtime_state(default_runtime_state())
    store_path = tmp_path / "open_positions.json"
    journal_path = tmp_path / "journal.jsonl"
    save_open_positions([_position()], store_path=store_path)
    order = submit_exit_for_position(
        _position(),
        broker=FakeBroker(),
        limit_price=3.1,
        journal_path=str(journal_path),
        store_path=str(store_path),
    )
    positions = load_open_positions(store_path=store_path)
    assert order.broker_order_id == "alpaca-exit-1"
    assert positions[0].status == "closing:alpaca-exit-1"


def test_cancel_and_replace_open_order_append_journal(tmp_path) -> None:
    journal_path = tmp_path / "journal.jsonl"
    broker = FakeBroker()
    canceled = cancel_open_order(broker_order_id="alpaca-entry-1", broker=broker, journal_path=str(journal_path))
    replaced = replace_open_order(broker_order_id="alpaca-entry-1", broker=broker, limit_price=2.75, journal_path=str(journal_path))
    rows = [json.loads(line) for line in journal_path.read_text(encoding="utf-8").splitlines()]
    assert canceled["status"] == "canceled"
    assert replaced["limit_price"] == "2.75"
    assert len(rows) == 2
