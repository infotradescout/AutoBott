from __future__ import annotations

from datetime import UTC, datetime

from autobott_v2.execution_config import AlpacaExecutionConfig
from autobott_v2.execution_models import BrokerEnvironment, ExecutionOrder, ExecutionState
from autobott_v2.position_monitor import PositionMonitorRules, run_position_monitor
from autobott_v2.runtime_control import default_runtime_state, save_runtime_state


def _config(*, max_position_cost: float = 1000.0) -> AlpacaExecutionConfig:
    return AlpacaExecutionConfig(
        environment=BrokerEnvironment.PAPER,
        api_key="paper-key",
        secret_key="paper-secret",
        trading_base_url="https://paper-api.alpaca.markets",
        data_base_url="https://data.alpaca.markets",
        allow_live_trading=False,
        allow_order_placement=True,
        max_position_cost=max_position_cost,
        max_daily_loss=500.0,
        max_open_positions=25,
    )


class FakeBroker:
    def __init__(self, positions, orders=None, *, max_position_cost: float = 1000.0):
        self.config = _config(max_position_cost=max_position_cost)
        self.positions = positions
        self.orders = orders or []
        self.submitted = []
        self.replaced = []
        self.canceled = []

    def list_open_positions(self):
        return self.positions

    def list_orders(self, *, status="open", limit=100, direction="desc"):
        return self.orders

    def replace_order(self, broker_order_id, *, limit_price):
        self.replaced.append({"id": broker_order_id, "limit_price": limit_price})
        return {"id": broker_order_id, "status": "new", "limit_price": str(limit_price)}

    def cancel_order(self, broker_order_id):
        self.canceled.append(broker_order_id)
        return {"id": broker_order_id, "status": "canceled"}

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


def test_position_monitor_holds_a_winner_that_has_not_reversed(tmp_path) -> None:
    save_runtime_state(default_runtime_state())
    broker = FakeBroker([_position(unrealized_plpc="0.24")])

    result = run_position_monitor(
        broker=broker,
        rules=PositionMonitorRules(take_profit_pct=0.30, trailing_activation_pct=0.15, trailing_drawdown_pct=0.10, stop_loss_pct=0.22, max_contracts_per_option=1),
        journal_path=str(tmp_path / "journal.jsonl"),
        trailing_state_path=str(tmp_path / "trailing_peaks.json"),
    )

    assert result["actions"] == []
    assert broker.submitted == []


def test_position_monitor_sells_once_a_winner_reverses_from_its_peak(tmp_path) -> None:
    save_runtime_state(default_runtime_state())
    trailing_state_path = str(tmp_path / "trailing_peaks.json")
    rules = PositionMonitorRules(take_profit_pct=0.30, trailing_activation_pct=0.15, trailing_drawdown_pct=0.10, stop_loss_pct=0.22, max_contracts_per_option=1)
    journal_path = str(tmp_path / "journal.jsonl")

    # First cycle: position peaks at +28%, well above activation, but hasn't reversed yet.
    peak_broker = FakeBroker([_position(unrealized_plpc="0.28")])
    peak_result = run_position_monitor(broker=peak_broker, rules=rules, journal_path=journal_path, trailing_state_path=trailing_state_path)
    assert peak_result["actions"] == []

    # Second cycle: it has given back more than the 10pt trail from its +28% peak.
    reversal_broker = FakeBroker([_position(unrealized_plpc="0.18")])
    result = run_position_monitor(broker=reversal_broker, rules=rules, journal_path=journal_path, trailing_state_path=trailing_state_path)

    assert result["actions"][0]["reason"] == "trailing_stop"
    assert result["actions"][0]["peak_unrealized_plpc"] == 0.28
    assert reversal_broker.submitted[0].option_symbol == "QQQ260708P00726000"
    assert reversal_broker.submitted[0].order_type.value == "market"


def test_position_monitor_takes_profit_on_large_winner_before_reversal(tmp_path) -> None:
    save_runtime_state(default_runtime_state())
    broker = FakeBroker([_position(unrealized_plpc="0.72")])

    result = run_position_monitor(
        broker=broker,
        rules=PositionMonitorRules(take_profit_pct=0.30, trailing_activation_pct=0.15, trailing_drawdown_pct=0.10, stop_loss_pct=0.22, max_contracts_per_option=1),
        journal_path=str(tmp_path / "journal.jsonl"),
        trailing_state_path=str(tmp_path / "trailing_peaks.json"),
    )

    assert result["actions"][0]["reason"] == "take_profit"
    assert result["actions"][0]["unrealized_plpc"] == 0.72
    assert result["actions"][0]["take_profit_tier"] == "tighten"
    assert broker.submitted[0].option_symbol == "QQQ260708P00726000"
    assert broker.submitted[0].order_type.value == "limit"
    assert broker.submitted[0].limit_price == 6.01
    assert broker.submitted[0].metadata["exit_order_style"] == "profit_ladder_limit"
    assert broker.submitted[0].metadata["take_profit_tier"] == "tighten"


def test_position_monitor_takes_profit_at_stricter_default_threshold(tmp_path) -> None:
    save_runtime_state(default_runtime_state())
    broker = FakeBroker([_position(unrealized_plpc="0.35")])

    result = run_position_monitor(
        broker=broker,
        rules=PositionMonitorRules(),
        journal_path=str(tmp_path / "journal.jsonl"),
        trailing_state_path=str(tmp_path / "trailing_peaks.json"),
    )

    assert result["actions"][0]["reason"] == "take_profit"
    assert result["actions"][0]["unrealized_plpc"] == 0.35
    assert result["actions"][0]["take_profit_tier"] == "initial"
    assert broker.submitted[0].order_type.value == "limit"
    assert broker.submitted[0].limit_price == 6.08


def test_position_monitor_tightens_exit_ladder_for_harvest_winner(tmp_path) -> None:
    save_runtime_state(default_runtime_state())
    broker = FakeBroker([_position(unrealized_plpc="0.88")])

    result = run_position_monitor(
        broker=broker,
        rules=PositionMonitorRules(),
        journal_path=str(tmp_path / "journal.jsonl"),
        trailing_state_path=str(tmp_path / "trailing_peaks.json"),
    )

    assert result["actions"][0]["reason"] == "take_profit"
    assert result["actions"][0]["take_profit_tier"] == "harvest"
    assert broker.submitted[0].order_type.value == "limit"
    assert broker.submitted[0].limit_price == 5.89


def test_position_monitor_caps_stale_rich_profit_factor_below_current_price(tmp_path) -> None:
    save_runtime_state(default_runtime_state())
    broker = FakeBroker([_position(unrealized_plpc="0.35")])

    run_position_monitor(
        broker=broker,
        rules=PositionMonitorRules(take_profit_limit_price_factor=1.10),
        journal_path=str(tmp_path / "journal.jsonl"),
        trailing_state_path=str(tmp_path / "trailing_peaks.json"),
    )

    assert broker.submitted[0].limit_price == 6.14


def test_position_monitor_force_exits_extreme_winner(tmp_path) -> None:
    save_runtime_state(default_runtime_state())
    broker = FakeBroker([_position(unrealized_plpc="1.25")])

    result = run_position_monitor(
        broker=broker,
        rules=PositionMonitorRules(),
        journal_path=str(tmp_path / "journal.jsonl"),
        trailing_state_path=str(tmp_path / "trailing_peaks.json"),
    )

    assert result["actions"][0]["reason"] == "take_profit"
    assert result["actions"][0]["take_profit_tier"] == "force_exit"
    assert broker.submitted[0].order_type.value == "market"
    assert broker.submitted[0].metadata["exit_order_style"] == "urgent_market"


def test_position_monitor_does_not_duplicate_pending_take_profit_exit(tmp_path) -> None:
    save_runtime_state(default_runtime_state())
    broker = FakeBroker(
        [_position(unrealized_plpc="0.35")],
        orders=[
            {
                "id": "pending-exit-1",
                "symbol": "QQQ260708P00726000",
                "side": "sell",
                "status": "new",
                "limit_price": "6.00",
            }
        ],
    )

    result = run_position_monitor(
        broker=broker,
        rules=PositionMonitorRules(),
        journal_path=str(tmp_path / "journal.jsonl"),
        trailing_state_path=str(tmp_path / "trailing_peaks.json"),
    )

    assert result["actions"][0]["reason"] == "take_profit_exit_already_pending"
    assert result["actions"][0]["broker_order_id"] == "pending-exit-1"
    assert broker.submitted == []
    assert broker.replaced == []


def test_position_monitor_reprices_rich_take_profit_exit_downward(tmp_path) -> None:
    save_runtime_state(default_runtime_state())
    broker = FakeBroker(
        [_position(unrealized_plpc="0.35")],
        orders=[
            {
                "id": "pending-exit-1",
                "symbol": "QQQ260708P00726000",
                "side": "sell",
                "status": "new",
                "limit_price": "6.82",
            }
        ],
    )

    result = run_position_monitor(
        broker=broker,
        rules=PositionMonitorRules(),
        journal_path=str(tmp_path / "journal.jsonl"),
        trailing_state_path=str(tmp_path / "trailing_peaks.json"),
    )

    assert result["actions"][0]["reason"] == "take_profit_exit_repriced"
    assert broker.replaced == [{"id": "pending-exit-1", "limit_price": 6.01}]
    assert broker.submitted == []


def test_position_monitor_trailing_stop_overrides_take_profit_after_big_giveback(tmp_path) -> None:
    save_runtime_state(default_runtime_state())
    trailing_state_path = str(tmp_path / "trailing_peaks.json")
    journal_path = str(tmp_path / "journal.jsonl")
    rules = PositionMonitorRules()

    peak_broker = FakeBroker([_position(unrealized_plpc="0.72")])
    run_position_monitor(broker=peak_broker, rules=rules, journal_path=journal_path, trailing_state_path=trailing_state_path)

    reversal_broker = FakeBroker(
        [_position(unrealized_plpc="0.35")],
        orders=[
            {
                "id": "pending-exit-1",
                "symbol": "QQQ260708P00726000",
                "side": "sell",
                "status": "new",
                "limit_price": "6.01",
            }
        ],
    )
    result = run_position_monitor(broker=reversal_broker, rules=rules, journal_path=journal_path, trailing_state_path=trailing_state_path)

    assert result["actions"][0]["reason"] == "trailing_stop"
    assert result["actions"][0]["peak_unrealized_plpc"] == 0.72
    assert result["actions"][0]["canceled_pending_exit_order_id"] == "pending-exit-1"
    assert reversal_broker.canceled == ["pending-exit-1"]
    assert reversal_broker.submitted[0].order_type.value == "market"


def test_position_monitor_cancels_pending_profit_exit_before_force_exit(tmp_path) -> None:
    save_runtime_state(default_runtime_state())
    broker = FakeBroker(
        [_position(unrealized_plpc="1.25")],
        orders=[
            {
                "id": "pending-exit-1",
                "symbol": "QQQ260708P00726000",
                "side": "sell",
                "status": "new",
                "limit_price": "6.82",
            }
        ],
    )

    result = run_position_monitor(
        broker=broker,
        rules=PositionMonitorRules(),
        journal_path=str(tmp_path / "journal.jsonl"),
        trailing_state_path=str(tmp_path / "trailing_peaks.json"),
    )

    assert result["actions"][0]["reason"] == "take_profit_force_exit_submitted"
    assert result["actions"][0]["take_profit_tier"] == "force_exit"
    assert result["actions"][0]["canceled_pending_exit_order_id"] == "pending-exit-1"
    assert broker.canceled == ["pending-exit-1"]
    assert broker.submitted[0].order_type.value == "market"


def test_position_monitor_cancels_pending_profit_exit_before_urgent_stop(tmp_path) -> None:
    save_runtime_state(default_runtime_state())
    broker = FakeBroker(
        [_position(unrealized_plpc="-0.25")],
        orders=[
            {
                "id": "pending-exit-1",
                "symbol": "QQQ260708P00726000",
                "side": "sell",
                "status": "new",
                "limit_price": "6.82",
            }
        ],
    )

    result = run_position_monitor(
        broker=broker,
        rules=PositionMonitorRules(),
        journal_path=str(tmp_path / "journal.jsonl"),
        trailing_state_path=str(tmp_path / "trailing_peaks.json"),
    )

    assert result["actions"][0]["reason"] == "stop_loss"
    assert result["actions"][0]["canceled_pending_exit_order_id"] == "pending-exit-1"
    assert broker.canceled == ["pending-exit-1"]
    assert broker.submitted[0].order_type.value == "market"


def test_position_monitor_cancels_pending_buy_before_urgent_stop(tmp_path) -> None:
    save_runtime_state(default_runtime_state())
    broker = FakeBroker(
        [_position(unrealized_plpc="-0.25")],
        orders=[
            {
                "id": "pending-buy-1",
                "symbol": "QQQ260708P00726000",
                "side": "buy",
                "status": "new",
                "limit_price": "6.82",
            }
        ],
    )

    result = run_position_monitor(
        broker=broker,
        rules=PositionMonitorRules(),
        journal_path=str(tmp_path / "journal.jsonl"),
        trailing_state_path=str(tmp_path / "trailing_peaks.json"),
    )

    assert result["actions"][0]["reason"] == "stop_loss"
    assert result["actions"][0]["canceled_pending_order_ids"] == ["pending-buy-1"]
    assert broker.canceled == ["pending-buy-1"]
    assert broker.submitted[0].order_type.value == "market"


def test_position_monitor_cancels_pending_entry_over_cost_cap_without_positions(tmp_path) -> None:
    save_runtime_state(default_runtime_state())
    broker = FakeBroker(
        [],
        orders=[
            {
                "id": "pending-buy-1",
                "symbol": "MRK260717P00125000",
                "side": "buy",
                "status": "new",
                "qty": "1",
                "filled_qty": "0",
                "limit_price": "1.91",
            },
            {
                "id": "pending-buy-2",
                "symbol": "T260724P00021000",
                "side": "buy",
                "status": "new",
                "qty": "1",
                "filled_qty": "0",
                "limit_price": "0.70",
            },
        ],
        max_position_cost=100.0,
    )

    result = run_position_monitor(
        broker=broker,
        rules=PositionMonitorRules(),
        journal_path=str(tmp_path / "journal.jsonl"),
        trailing_state_path=str(tmp_path / "trailing_peaks.json"),
    )

    assert result["checked"] == 0
    assert result["actions"] == [
        {
            "reason": "pending_entry_over_cost_cap_canceled",
            "symbol": "MRK260717P00125000",
            "broker_order_id": "pending-buy-1",
            "estimated_notional": 191.0,
            "max_position_cost": 100.0,
        }
    ]
    assert broker.canceled == ["pending-buy-1"]
    assert broker.submitted == []


def test_position_monitor_trims_excess_contracts_before_profit_loss(tmp_path) -> None:
    save_runtime_state(default_runtime_state())
    broker = FakeBroker([_position(qty="4", unrealized_plpc="-0.05")])

    result = run_position_monitor(
        broker=broker,
        rules=PositionMonitorRules(take_profit_pct=0.30, trailing_activation_pct=0.15, trailing_drawdown_pct=0.10, stop_loss_pct=0.22, max_contracts_per_option=1),
        journal_path=str(tmp_path / "journal.jsonl"),
        trailing_state_path=str(tmp_path / "trailing_peaks.json"),
    )

    assert result["actions"][0]["reason"] == "trim_excess_contracts"
    assert broker.submitted[0].quantity == 3
    assert broker.submitted[0].order_type.value == "market"


def test_position_monitor_closes_stop_loss(tmp_path) -> None:
    save_runtime_state(default_runtime_state())
    broker = FakeBroker([_position(unrealized_plpc="-0.25")])

    result = run_position_monitor(
        broker=broker,
        rules=PositionMonitorRules(take_profit_pct=0.30, trailing_activation_pct=0.15, trailing_drawdown_pct=0.10, stop_loss_pct=0.22, max_contracts_per_option=1),
        journal_path=str(tmp_path / "journal.jsonl"),
        trailing_state_path=str(tmp_path / "trailing_peaks.json"),
    )

    assert result["actions"][0]["reason"] == "stop_loss"
    assert broker.submitted[0].quantity == 1
    assert broker.submitted[0].order_type.value == "market"
