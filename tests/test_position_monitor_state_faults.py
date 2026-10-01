"""Actual-monitor synthetic proof of quantity/state fault containment."""
from dataclasses import replace
from datetime import UTC, datetime
import json
from pathlib import Path

import pytest

import autobott_v2.position_monitor as monitor
from autobott_v2.execution_models import OrderSide, OrderType
from test_position_monitor import FakeBroker
from test_position_monitor_standalone_zero import _run
from test_position_monitor_pair_lifecycle import (
    FakeBroker as PairBroker, PRIMARY, RUNNER, GROUP, _broker_position,
    _run as _run_pair,
)

BAD = "SPY261016P00500000"
LOSER = "QQQ261016P00726000"
OTHER = "IWM261016P00100000"


def _position(symbol=BAD, **overrides):
    return {"symbol": symbol, "side": "long", "qty": "1", "avg_entry_price": "5",
            "current_price": "5.5", "unrealized_plpc": ".10", **overrides}


class RecordingBroker(FakeBroker):
    def submit_order(self, intent, **kwargs):
        order = super().submit_order(intent, **kwargs)
        self.orders.append({"id": order.broker_order_id, "client_order_id": order.client_order_id,
                            "symbol": intent.option_symbol, "side": "sell", "status": "new",
                            "qty": str(intent.quantity), "filled_qty": "0", "type": intent.order_type.value})
        return order


def _broker(**overrides):
    return RecordingBroker([_position(**overrides),
                            _position(LOSER, current_price="3.5", unrealized_plpc="-.30")])


def _reasons(result):
    return [issue["reason"] for issue in result["exit_protection"]["issues"]]


@pytest.mark.parametrize("qty", ["broken", "", None, "nan", "inf", "-inf", "0", "-1", "1.5", True, False, {}, 10**1000])
def test_invalid_quantity_is_held_and_valid_stop_continues(tmp_path, qty):
    broker = _broker(qty=qty, current_price="3.5", unrealized_plpc="-.30")
    broker.orders.append({"id": "existing-bad-exit", "symbol": BAD, "side": "sell", "status": "new"})
    result = _run(tmp_path, broker)
    assert [(a["symbol"], a["reason"]) for a in result["actions"]] == [(LOSER, "stop_loss")]
    assert [i.option_symbol for i in broker.submitted] == [LOSER]
    assert broker.canceled == []
    assert "invalid_exit_quantity: qty" in _reasons(result)
    assert any(issue["symbol"] == BAD and issue["status"] == "pending"
               for issue in result["exit_protection"]["issues"])
    assert result["ok"] is False


@pytest.mark.parametrize("qty", ["1", "1.0", "1e0", "2", 2, 2.0])
def test_valid_integral_quantities_preserve_market_stop_sizing(tmp_path, qty):
    broker = _broker(qty=qty, current_price="3.5", unrealized_plpc="-.30")
    result = _run(tmp_path, broker, monitor.PositionMonitorRules(exit_min_dte=-1, max_contracts_per_option=4))
    assert [i.quantity for i in broker.submitted] == [int(float(qty)), 1]
    assert all(i.side is OrderSide.SELL_TO_CLOSE and i.order_type is OrderType.MARKET for i in broker.submitted)
    assert not any("invalid_exit_quantity" in reason for reason in _reasons(result))


@pytest.mark.parametrize("value", ["broken", None, {}, [], float("nan"), float("inf"), float("-inf"), True])
def test_bad_peak_value_is_preserved_across_ticks_and_does_not_invent_trailing(tmp_path, value):
    path = tmp_path / "peaks.json"
    path.write_text(json.dumps({BAD: value, OTHER: .10}))
    broker = _broker()
    broker.positions.insert(1, _position(OTHER))
    first = _run(tmp_path, broker)
    broker.positions[1].update(current_price="6.25", unrealized_plpc=".25")
    second = _run(tmp_path, broker)
    for result in (first, second):
        assert "invalid_trailing_peak" in _reasons(result)
        assert result["ok"] is False
    saved = json.loads(path.read_text())
    assert json.dumps(saved[BAD], sort_keys=True) == json.dumps(value, sort_keys=True)
    assert saved[OTHER] == .25
    assert len(broker.submitted) == 1  # Existing pending loser is not duplicated.
    broker.positions[1].update(current_price="5.5", unrealized_plpc=".10")
    third = _run(tmp_path, broker)
    assert [(a["symbol"], a["reason"]) for a in third["actions"] if a.get("submitted")] == [(OTHER, "trailing_stop")]
    assert [i.option_symbol for i in broker.submitted] == [LOSER, OTHER]
    assert "invalid_trailing_peak" in _reasons(third)


@pytest.mark.parametrize("raw", ["{broken", "null", "[]", "42", '"scalar"'])
def test_global_peak_corruption_preserves_bytes_and_remains_visible(tmp_path, raw):
    path = tmp_path / "peaks.json"
    path.write_text(raw)
    broker = _broker()
    for _ in range(2):
        result = _run(tmp_path, broker)
        assert result["ok"] is False
        assert result["exit_protection"]["status"] == "attention_required"
        assert path.read_text() == raw
    assert [i.option_symbol for i in broker.submitted] == [LOSER]


def test_unreadable_peak_file_is_not_overwritten(tmp_path, monkeypatch):
    path = tmp_path / "peaks.json"
    original = json.dumps({BAD: .72})
    path.write_text(original)
    read = Path.read_text

    def unreadable(self, *args, **kwargs):
        if self == path:
            raise PermissionError("synthetic read failure")
        return read(self, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", unreadable)
    broker = _broker()
    for _ in range(2):
        result = _run(tmp_path, broker)
        assert "trailing_state_unreadable" in _reasons(result)
        assert read(path) == original
    assert [i.option_symbol for i in broker.submitted] == [LOSER]


@pytest.mark.parametrize("peak", [-.25, 0.0, "0.0"])
def test_finite_negative_and_zero_peaks_are_valid_history(tmp_path, peak):
    (tmp_path / "peaks.json").write_text(json.dumps({BAD: peak}))
    result = _run(tmp_path, _broker())
    assert not any("trailing" in reason for reason in _reasons(result))
    assert result["ok"] is True


def test_missing_peak_file_is_normal_initialization(tmp_path):
    result = _run(tmp_path, _broker())
    assert result["ok"] is True
    assert json.loads((tmp_path / "peaks.json").read_text())[BAD] == .10


def test_bad_untracked_peak_survives_pruning_with_visible_concern(tmp_path):
    path = tmp_path / "peaks.json"
    path.write_text(json.dumps({"OLD": "broken", OTHER: .72}))
    broker = _broker()
    broker.positions.insert(1, _position(OTHER, current_price="7.5", unrealized_plpc=".50"))
    result = _run(tmp_path, broker)
    assert json.loads(path.read_text())["OLD"] == "broken"
    assert "invalid_trailing_peak_for_untracked_position" in _reasons(result)
    assert {(a["symbol"], a["reason"]) for a in result["actions"]} == {(OTHER, "trailing_stop"), (LOSER, "stop_loss")}


def test_explicit_valid_state_replacement_recovers_without_duplicate_exit(tmp_path):
    path = tmp_path / "peaks.json"
    path.write_text(json.dumps({BAD: "broken"}))
    broker = _broker()
    assert _run(tmp_path, broker)["ok"] is False
    path.write_text(json.dumps({BAD: 0.0, LOSER: 0.0}))  # Explicit local fixture repair, never automatic.
    result = _run(tmp_path, broker)
    assert result["ok"] is True
    assert result["exit_protection"]["status"] == "awaiting_fill"
    assert not any("invalid_trailing" in reason for reason in _reasons(result))
    assert [i.option_symbol for i in broker.submitted] == [LOSER]


@pytest.mark.parametrize("reason", ["stop_loss", "dte_floor", "trim_excess_contracts", "position_cost_cap_breached", "take_profit"])
def test_peak_independent_policy_survives_bad_history(tmp_path, monkeypatch, reason):
    (tmp_path / "peaks.json").write_text(json.dumps({BAD: float("inf")}))
    position = _position()
    rules = monitor.PositionMonitorRules(exit_min_dte=-1)
    if reason == "stop_loss":
        position.update(current_price="0", unrealized_plpc="-1")
    elif reason == "dte_floor":
        monkeypatch.setattr(monitor, "_monitor_now", lambda: datetime(2026, 10, 16, tzinfo=UTC))
        rules = replace(rules, exit_min_dte=0)
    elif reason == "trim_excess_contracts":
        position["qty"] = "2"
    elif reason == "position_cost_cap_breached":
        position.update(avg_entry_price="12", current_price="12", unrealized_plpc="0")
    else:
        position.update(current_price="6.75", unrealized_plpc=".35")
    broker = RecordingBroker([position])
    result = _run(tmp_path, broker, rules)
    assert [(a["symbol"], a["reason"]) for a in result["actions"]] == [(BAD, reason)]
    assert broker.submitted[0].order_type is (OrderType.LIMIT if reason == "take_profit" else OrderType.MARKET)
    assert "invalid_trailing_peak" in _reasons(result)


@pytest.mark.parametrize("reason", ["pair_max_loss_reached", "primary_profit_funds_runner", "unfunded_runner_stop_loss", "funded_runner_catastrophic_stop"])
def test_pair_peak_independent_policy_survives_bad_history(tmp_path, reason):
    primary_price = .10 if reason == "pair_max_loss_reached" else 1.0
    runner_price = .01 if reason.endswith("stop_loss") or reason.endswith("catastrophic_stop") else .25
    positions = [_broker_position(PRIMARY, entry=.70, current=primary_price),
                 _broker_position(RUNNER, entry=.25, current=runner_price)]
    state = None
    if reason in {"unfunded_runner_stop_loss", "funded_runner_catastrophic_stop"}:
        positions = positions[1:]
    broker = PairBroker(positions)
    if reason == "funded_runner_catastrophic_stop":
        broker.orders["primary-order"] = {"id": "primary-order", "symbol": PRIMARY, "side": "buy", "status": "filled",
                                           "filled_qty": "1", "filled_avg_price": ".70"}
        broker.orders["funded-exit"] = {"id": "funded-exit", "symbol": PRIMARY, "side": "sell", "status": "filled",
                                        "filled_qty": "1", "filled_avg_price": "1.00"}
        state = {GROUP: {"funding_exit_order_id": "funded-exit", "funding_exit_orders": {"funded-exit": {}}}}
    result = _run_pair(tmp_path, broker, trailing={PRIMARY: "broken", RUNNER: float("inf")}, pair_state=state)
    assert {a["reason"] for a in result["actions"]} == {reason}
    assert len(broker.submitted) == (2 if reason == "pair_max_loss_reached" else 1)
    assert not any("invalid_pair_exit_observation" in value for value in _reasons(result))
    assert result["pair_groups_managed"] == 1


@pytest.mark.parametrize("failure", ["write", "replace"])
def test_peak_persistence_failure_keeps_original_and_reports_attention(tmp_path, monkeypatch, failure):
    path = tmp_path / "peaks.json"
    original = json.dumps({BAD: .05})
    path.write_text(original)
    write, replace_file = Path.write_text, Path.replace

    def failed_write(self, *args, **kwargs):
        if self.name.startswith(".peaks.json."):
            raise OSError("synthetic persistence failure")
        return write(self, *args, **kwargs)

    def failed_replace(self, target):
        if target == path:
            raise OSError("synthetic persistence failure")
        return replace_file(self, target)

    monkeypatch.setattr(Path, "write_text" if failure == "write" else "replace",
                        failed_write if failure == "write" else failed_replace)
    broker = _broker()
    result = _run(tmp_path, broker)
    assert [i.option_symbol for i in broker.submitted] == [LOSER]
    assert path.read_text() == original
    assert "trailing_state_persistence_failed" in _reasons(result)
    assert result["ok"] is False
    assert list(tmp_path.glob(".peaks.json.*.tmp")) == []
