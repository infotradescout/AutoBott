"""Synthetic regression proof only. No broker client or external order."""
from datetime import UTC, datetime
import pytest

from autobott_v2.execution_models import OrderSide, OrderType
from autobott_v2.position_monitor import _pair_leg_mark
from test_position_monitor_pair_lifecycle import (
    FakeBroker, PRIMARY, RUNNER, GROUP, _broker_position, _stored_positions, _run,
)


@pytest.mark.parametrize("zero", [0.0, "0.0", "-0.0"])
@pytest.mark.parametrize("prices,expected", [
    ((0.0, 0.25), {PRIMARY: "pair_max_loss_reached", RUNNER: "pair_max_loss_reached"}),
    ((0.70, 0.0), {RUNNER: "unfunded_runner_stop_loss"}),
    ((0.0, 0.0), {PRIMARY: "pair_max_loss_reached", RUNNER: "pair_max_loss_reached"}),
])
def test_zero_pair_marks_reach_existing_loss_policy(tmp_path, prices, expected, zero):
    positions = [_broker_position(PRIMARY, entry=.70, current=prices[0]),
                 _broker_position(RUNNER, entry=.25, current=prices[1])]
    for position, price in zip(positions, prices):
        if price == 0:
            position["current_price"] = zero
    broker = FakeBroker(positions)
    result = _run(tmp_path, broker)
    assert {a["symbol"]: a["reason"] for a in result["actions"]} == expected
    for action in result["actions"]:
        if float(next(p["current_price"] for p in positions if p["symbol"] == action["symbol"])) == 0:
            assert action["current_price"] == 0
    assert len(broker.submitted) == len(expected)
    assert all(i.side is OrderSide.SELL_TO_CLOSE and i.order_type is OrderType.MARKET for i in broker.submitted)


@pytest.mark.parametrize("funded", [False, True])
def test_zero_lone_runner_reaches_existing_protection(tmp_path, funded):
    broker = FakeBroker([_broker_position(RUNNER, entry=.25, current=0)])
    state = {"runner_cost": 25}
    if funded:
        # Funding is proved by entry and exit fills, not a hand-written P/L latch.
        broker.orders["primary-order"] = {"id": "primary-order", "symbol": PRIMARY, "side": "buy",
                                           "status": "filled", "filled_qty": "1", "filled_avg_price": ".70"}
        broker.orders["funded-exit"] = {"id": "funded-exit", "symbol": PRIMARY, "side": "sell",
                                        "status": "filled", "filled_qty": "1", "filled_avg_price": "1.00"}
        state.update(funding_exit_order_id="funded-exit", funding_exit_orders={"funded-exit": {}})
    result = _run(tmp_path, broker, pair_state={GROUP: state})
    assert len(result["actions"]) == 1
    expected = "funded_runner_catastrophic_stop" if funded else "unfunded_runner_stop_loss"
    assert result["actions"][0]["reason"] == expected
    assert result["actions"][0]["current_price"] == 0
    assert result["actions"][0]["runner_funded"] is funded


def test_positive_pair_and_threshold_policy_are_unchanged(tmp_path):
    broker = FakeBroker([_broker_position(PRIMARY, entry=.70, current=.49),
                         _broker_position(RUNNER, entry=.25, current=.25)])
    result = _run(tmp_path, broker)
    assert result["actions"] == []
    assert broker.submitted == []


def test_missing_mark_fallback_is_explicitly_outside_this_patch():
    stored = _stored_positions()[0]
    position = _broker_position(PRIMARY, entry=.70, current=.49)
    position.pop("current_price")
    mark = _pair_leg_mark(position, stored, peak_return_pct=None)
    assert mark.current_price == .70
