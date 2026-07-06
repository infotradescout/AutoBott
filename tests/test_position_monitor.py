from __future__ import annotations

from datetime import UTC, datetime

from autobott_v2.execution_config import AlpacaExecutionConfig
from autobott_v2.execution_models import BrokerEnvironment, ExecutionOrder, ExecutionState
from autobott_v2.position_monitor import PositionMonitorRules, run_position_monitor
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
        max_open_positions=25,
    )


class FakeBroker:
    def __init__(self, positions):
        self.config = _config()
        self.positions = positions
        self.submitted = []

    def list_open_positions(self):
        return self.positions

    def submit_order(self, intent, *, current_daily_realized_pnl=0.0, open_positions=0):
        self.submitted.append(intent)
        return ExecutionOrder(
            order_id=f"exit-{len(self.submitted)}",
            client_order_id=f"client-exit-{len(self.submitted)}",
            intent=intent,
            state=ExecutionState.SUBMITTED,
            submitted_at=datetime(2026, 7, 6, 15, 45, tzinfo=UTC),
            broker_order_id=f"alpaca-exit-{len(self.submitted)}",
        )


def _position(**overrides):
    base = {
        "symbol": "QQQ260708P00726000",
        "side": "long",
        "qty": "1",
        "current_price": "6.20",
        "avg_entry_price": "5.00",
        "unrealized_plpc": "0.24",
    }
    return base | overrides


def test_position_monitor_closes_profit_target(tmp_path) -> None:
    save_runtime_state(default_runtime_state())
    broker = FakeBroker([_position()])

    result = run_position_monitor(
        broker=broker,
        rules=PositionMonitorRules(profit_target_pct=0.18, stop_loss_pct=0.22, max_contracts_per_option=1),
        journal_path=str(tmp_path / "journal.jsonl"),
    )

    assert result["actions"][0]["reason"] == "profit_target"
    assert broker.submitted[0].option_symbol == "QQQ260708P00726000"
    assert broker.submitted[0].quantity == 1


def test_position_monitor_trims_excess_contracts_before_profit_loss(tmp_path) -> None:
    save_runtime_state(default_runtime_state())
    broker = FakeBroker([_position(qty="4", unrealized_plpc="-0.05")])

    result = run_position_monitor(
        broker=broker,
        rules=PositionMonitorRules(profit_target_pct=0.18, stop_loss_pct=0.22, max_contracts_per_option=1),
        journal_path=str(tmp_path / "journal.jsonl"),
    )

    assert result["actions"][0]["reason"] == "trim_excess_contracts"
    assert broker.submitted[0].quantity == 3


def test_position_monitor_closes_stop_loss(tmp_path) -> None:
    save_runtime_state(default_runtime_state())
    broker = FakeBroker([_position(unrealized_plpc="-0.25")])

    result = run_position_monitor(
        broker=broker,
        rules=PositionMonitorRules(profit_target_pct=0.18, stop_loss_pct=0.22, max_contracts_per_option=1),
        journal_path=str(tmp_path / "journal.jsonl"),
    )

    assert result["actions"][0]["reason"] == "stop_loss"
    assert broker.submitted[0].quantity == 1
