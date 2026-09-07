from __future__ import annotations

import json
from datetime import UTC, datetime

from autobott_v2.execution_config import AlpacaExecutionConfig
from autobott_v2.execution_models import BrokerEnvironment, ExecutionOrder, ExecutionState, OrderSide
from autobott_v2.position_monitor import PositionMonitorRules, run_position_monitor
from autobott_v2.position_store import OpenPosition, save_open_positions
from autobott_v2.runtime_control import default_runtime_state, save_runtime_state


PRIMARY = "VIX261016C00017000"
RUNNER = "VIX261016C00020000"
GROUP = "core-runner:decision-1"


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
        self.canceled = []

    def list_open_positions(self):
        return self.positions

    def list_orders(self, *, status="open", limit=100, direction="desc"):
        return []

    def cancel_order(self, broker_order_id):
        self.canceled.append(broker_order_id)
        return {"id": broker_order_id, "status": "canceled"}

    def submit_order(self, intent, *, current_daily_realized_pnl=0.0, open_positions=0):
        self.submitted.append(intent)
        return ExecutionOrder(
            order_id=f"exit-{len(self.submitted)}",
            client_order_id=f"exit-client-{len(self.submitted)}",
            intent=intent,
            state=ExecutionState.SUBMITTED,
            submitted_at=datetime(2026, 9, 8, 15, 0, tzinfo=UTC),
            broker_order_id=f"exit-broker-{len(self.submitted)}",
        )


def _broker_position(symbol: str, *, entry: float, current: float) -> dict:
    return {
        "symbol": symbol,
        "side": "long",
        "qty": "1",
        "avg_entry_price": str(entry),
        "current_price": str(current),
        "unrealized_plpc": str((current - entry) / entry),
    }


def _stored_positions():
    opened = datetime(2026, 9, 8, 14, 0, tzinfo=UTC)
    return [
        OpenPosition(
            broker_order_id="primary-order",
            decision_id="decision-1",
            symbol="VIX",
            option_symbol=PRIMARY,
            quantity=1,
            entry_limit_price=0.70,
            entry_submitted_at=opened,
            take_profit_price=1.05,
            stop_loss_price=0.39,
            status="filled",
            trade_group_id=GROUP,
            leg_role="primary",
            paired_option_symbol=RUNNER,
        ),
        OpenPosition(
            broker_order_id="runner-order",
            decision_id="decision-1",
            symbol="VIX",
            option_symbol=RUNNER,
            quantity=1,
            entry_limit_price=0.25,
            entry_submitted_at=opened,
            take_profit_price=0.50,
            stop_loss_price=0.08,
            status="filled",
            trade_group_id=GROUP,
            leg_role="runner",
            paired_option_symbol=PRIMARY,
        ),
    ]


def _run(tmp_path, broker, *, trailing=None, pair_state=None):
    save_runtime_state(default_runtime_state())
    store_path = tmp_path / "open_positions.json"
    trail_path = tmp_path / "trailing.json"
    pair_path = tmp_path / "pair_state.json"
    save_open_positions(_stored_positions(), store_path=store_path)
    if trailing is not None:
        trail_path.write_text(json.dumps(trailing), encoding="utf-8")
    if pair_state is not None:
        pair_path.write_text(json.dumps(pair_state), encoding="utf-8")
    return run_position_monitor(
        broker=broker,
        rules=PositionMonitorRules(exit_min_dte=-1),
        journal_path=str(tmp_path / "journal.jsonl"),
        trailing_state_path=trail_path,
        position_store_path=store_path,
        pair_state_path=pair_path,
    )


def test_primary_above_old_30pct_target_holds_until_it_pays_runner(tmp_path) -> None:
    broker = FakeBroker([
        _broker_position(PRIMARY, entry=0.70, current=0.92),
        _broker_position(RUNNER, entry=0.25, current=0.25),
    ])

    result = _run(tmp_path, broker)

    assert result["pair_groups_managed"] == 1
    assert result["actions"] == []
    assert broker.submitted == []


def test_primary_is_harvested_when_profit_covers_runner_cost(tmp_path) -> None:
    broker = FakeBroker([
        _broker_position(PRIMARY, entry=0.70, current=0.95),
        _broker_position(RUNNER, entry=0.25, current=0.34),
    ])

    result = _run(tmp_path, broker)

    assert len(result["actions"]) == 1
    action = result["actions"][0]
    assert action["symbol"] == PRIMARY
    assert action["reason"] == "primary_profit_funds_runner"
    assert action["primary_pnl"] == 25.0
    assert action["runner_cost"] == 25.0
    assert action["runner_funded"] is True
    assert broker.submitted[0].side is OrderSide.SELL_TO_CLOSE
    assert broker.submitted[0].option_symbol == PRIMARY


def test_combined_pair_loss_exits_both_legs(tmp_path) -> None:
    broker = FakeBroker([
        _broker_position(PRIMARY, entry=0.70, current=0.40),
        _broker_position(RUNNER, entry=0.25, current=0.10),
    ])

    result = _run(tmp_path, broker)

    pair_exits = [action for action in result["actions"] if action["reason"] == "pair_max_loss_reached"]
    assert {action["symbol"] for action in pair_exits} == {PRIMARY, RUNNER}
    assert {intent.option_symbol for intent in broker.submitted} == {PRIMARY, RUNNER}


def test_funded_runner_is_not_sold_at_old_fixed_100pct_target(tmp_path) -> None:
    broker = FakeBroker([
        _broker_position(RUNNER, entry=0.25, current=0.50),
    ])
    state = {
        GROUP: {
            "runner_funded": True,
            "primary_realized_pnl_estimate": 30.0,
            "runner_cost": 25.0,
            "runner_symbol": RUNNER,
        }
    }

    result = _run(tmp_path, broker, trailing={RUNNER: 1.0}, pair_state=state)

    assert result["pair_groups_managed"] == 1
    assert result["actions"] == []
    assert broker.submitted == []


def test_funded_runner_exits_after_large_drawdown_from_peak(tmp_path) -> None:
    broker = FakeBroker([
        _broker_position(RUNNER, entry=0.25, current=0.41),
    ])
    state = {
        GROUP: {
            "runner_funded": True,
            "primary_realized_pnl_estimate": 30.0,
            "runner_cost": 25.0,
            "runner_symbol": RUNNER,
        }
    }

    result = _run(tmp_path, broker, trailing={RUNNER: 1.25}, pair_state=state)

    assert len(result["actions"]) == 1
    assert result["actions"][0]["reason"] == "funded_runner_trailing_drawdown"
    assert broker.submitted[0].option_symbol == RUNNER
