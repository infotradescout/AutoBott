"""Zero marks reach real monitor exits without claiming liquidity or fills."""
from dataclasses import replace
import json

import pytest

import autobott_v2.position_monitor as monitor
from autobott_v2.execution_broker import AlpacaExecutionBroker
from autobott_v2.execution_models import ExecutionState, OrderSide, OrderType
from autobott_v2.position_monitor import PositionMonitorRules, run_position_monitor
from test_position_monitor import FakeBroker, _config


SYMBOL = "QQQ261016P00726000"


def _position(mark="0", **overrides):
    return {"symbol": SYMBOL, "side": "long", "qty": "1", "avg_entry_price": "5",
            "current_price": mark, "unrealized_plpc": "-1", **overrides}


def _run(tmp_path, broker, rules=None):
    return run_position_monitor(
        broker=broker, rules=rules or PositionMonitorRules(exit_min_dte=-1),
        journal_path=str(tmp_path / "journal.jsonl"),
        trailing_state_path=tmp_path / "peaks.json",
        position_store_path=tmp_path / "positions.json",
        pair_state_path=tmp_path / "pairs.json")


@pytest.mark.parametrize("mark", ["0", 0, 0.0, "0.00"])
def test_zero_standalone_stop_preserves_observation_and_pending_state(tmp_path, mark):
    class JournalCheckingBroker(FakeBroker):
        def submit_order(self, intent, **kwargs):
            rows = [json.loads(line) for line in (tmp_path / "journal.jsonl").read_text().splitlines()]
            assert any(row.get("payload", {}).get("exit_status") == "attempting"
                       and row.get("payload", {}).get("client_order_id") == intent.metadata["exit_client_order_id"]
                       for row in rows)
            return super().submit_order(intent, **kwargs)

    broker = JournalCheckingBroker([_position(mark)])
    result = _run(tmp_path, broker)
    action, = result["actions"]
    assert action["reason"] == "stop_loss"
    assert action["current_price"] == 0
    assert action["exit_status"] == "pending"
    assert action["state"] == "submitted"
    assert action["submitted"] is True
    intent, = broker.submitted
    assert intent.side is OrderSide.SELL_TO_CLOSE
    assert intent.order_type is OrderType.MARKET
    assert intent.quantity == 1
    assert intent.limit_price == 0.01  # Internal reference floor, never a fill.
    assert broker.positions == [_position(mark)]


@pytest.mark.parametrize("mark", ["0", 0.0])
def test_zero_standalone_trailing_reversal_uses_existing_peak(tmp_path, mark):
    (tmp_path / "peaks.json").write_text(json.dumps({SYMBOL: 0.28}))
    broker = FakeBroker([_position(mark, unrealized_plpc="-0.10")])
    action, = _run(tmp_path, broker)["actions"]
    assert action["reason"] == "trailing_stop"
    assert action["peak_unrealized_plpc"] == 0.28
    assert action["current_price"] == 0
    assert broker.submitted[0].order_type is OrderType.MARKET


@pytest.mark.parametrize("mark", ["0", 0.0])
@pytest.mark.parametrize("reason", ["dte_floor", "trim_excess_contracts"])
def test_zero_hard_safety_preserves_quantity_and_market_policy(tmp_path, monkeypatch, mark, reason):
    from datetime import UTC, datetime
    monkeypatch.setattr(monitor, "_monitor_now", lambda: datetime(2026, 10, 16, tzinfo=UTC))
    qty = "3" if reason == "trim_excess_contracts" else "1"
    rules = PositionMonitorRules(exit_min_dte=0 if reason == "dte_floor" else -1)
    broker = FakeBroker([_position(mark, qty=qty, unrealized_plpc="0")])
    action, = _run(tmp_path, broker, rules)["actions"]
    assert action["reason"] == reason
    assert action["current_price"] == 0
    assert broker.submitted[0].quantity == (2 if reason == "trim_excess_contracts" else 1)
    assert broker.submitted[0].order_type is OrderType.MARKET


@pytest.mark.parametrize("mark", ["0", 0.0])
@pytest.mark.parametrize("plpc", ["0.35", "0.85", "1.25"])
def test_zero_mark_cannot_fabricate_profit_limit_or_force_profit(tmp_path, mark, plpc):
    broker = FakeBroker([_position(mark, unrealized_plpc=plpc)])
    assert _run(tmp_path, broker)["actions"] == []
    assert broker.submitted == []


@pytest.mark.parametrize("mark", ["-0.01", -0.01, "nan", float("nan"), "inf", float("inf"), "-inf", "bad", ""])
@pytest.mark.parametrize("hard", [False, True])
def test_invalid_mark_never_admits_standalone_loss_or_trim(tmp_path, mark, hard):
    broker = FakeBroker([_position(mark, qty="2" if hard else "1")])
    result = _run(tmp_path, broker)
    assert result["actions"] == []
    assert result["ok"] is False
    assert result["exit_protection"]["status"] == "attention_required"
    assert result["exit_protection"]["issues"][0]["reason"] == "invalid_exit_observation"
    assert broker.submitted == []


@pytest.mark.parametrize("plpc", ["0.35", "1.25"])
def test_zero_with_inconsistent_profit_and_old_peak_is_held(tmp_path, plpc):
    (tmp_path / "peaks.json").write_text(json.dumps({SYMBOL: 2.0}))
    broker = FakeBroker([_position("0", unrealized_plpc=plpc)])
    result = _run(tmp_path, broker)
    assert result["actions"] == []
    assert result["ok"] is False
    assert result["exit_protection"]["blocked_count"] == 1
    assert broker.submitted == []


@pytest.mark.parametrize("plpc", ["nan", float("nan"), "inf", float("inf"), "-inf", "bad"])
@pytest.mark.parametrize("hard", [False, True])
def test_invalid_pnl_never_admits_zero_standalone_exit(tmp_path, plpc, hard):
    broker = FakeBroker([_position(unrealized_plpc=plpc, qty="2" if hard else "1")])
    result = _run(tmp_path, broker)
    assert result["actions"] == []
    assert result["ok"] is False
    assert result["exit_protection"]["status"] == "attention_required"
    assert broker.submitted == []


@pytest.mark.parametrize("present", [True, False])
def test_absent_mark_entry_fallback_is_explicitly_unchanged(tmp_path, present):
    position = _position(None)
    if not present:
        del position["current_price"]
    broker = FakeBroker([position])
    action, = _run(tmp_path, broker)["actions"]
    assert action["reason"] == "stop_loss"
    assert action["current_price"] == 5
    assert broker.submitted[0].order_type is OrderType.MARKET


@pytest.mark.parametrize("plpc,reason,order_type", [
    ("-0.3", "stop_loss", OrderType.MARKET),
    ("0.35", "take_profit", OrderType.LIMIT),
    ("1.25", "take_profit", OrderType.MARKET),
])
def test_positive_mark_actions_keep_existing_policy(tmp_path, plpc, reason, order_type):
    broker = FakeBroker([_position("6.2", unrealized_plpc=plpc)])
    action, = _run(tmp_path, broker)["actions"]
    assert action["reason"] == reason
    assert action["current_price"] == 6.2
    assert broker.submitted[0].order_type is order_type


def test_zero_stop_keeps_unconfirmed_cancellation_uncertain(tmp_path):
    class UnconfirmedBroker(FakeBroker):
        def cancel_order(self, broker_order_id):
            self.canceled.append(broker_order_id)
            return {"id": broker_order_id, "status": "pending_cancel"}

    broker = UnconfirmedBroker([_position()], orders=[{
        "id": "pending-sell", "symbol": SYMBOL, "side": "sell", "status": "new",
        "qty": "1", "filled_qty": "0", "type": "limit", "limit_price": "5"}])
    action, = _run(tmp_path, broker)["actions"]
    assert action["reason"] == "stop_loss"
    assert action["exit_status"] == "uncertain"
    assert action["submitted"] is False
    assert broker.submitted == []
    assert broker.canceled == ["pending-sell"]


@pytest.mark.parametrize("state,expected", [
    (ExecutionState.REJECTED, "rejected"),
    (ExecutionState.PARTIALLY_FILLED, "partially_filled"),
    (ExecutionState.FILLED, "broker_reported_filled"),
])
def test_zero_exit_acknowledgment_never_claims_confirmed_flat(tmp_path, state, expected):
    class ResultBroker(FakeBroker):
        def submit_order(self, intent, **kwargs):
            return replace(super().submit_order(intent, **kwargs), state=state)

    broker = ResultBroker([_position()])
    action, = _run(tmp_path, broker)["actions"]
    assert action["exit_status"] == expected
    assert action["submitted"] is (state is not ExecutionState.REJECTED)
    assert broker.positions == [_position()]


@pytest.mark.parametrize("enabled", [True, False])
def test_actual_alpaca_market_payload_has_no_limit_and_order_hold_precedes_post(tmp_path, monkeypatch, enabled):
    broker = AlpacaExecutionBroker(replace(_config(), allow_order_placement=enabled))
    monkeypatch.setattr(broker, "list_open_positions", lambda: [_position()])
    monkeypatch.setattr(broker, "list_orders", lambda **kwargs: [])
    posts = []

    def request(method, path, *, payload=None):
        assert method == "POST" and path == "/v2/orders"
        posts.append(payload)
        return {"id": "mock-alpaca-exit", "status": "new", "client_order_id": payload["client_order_id"]}

    monkeypatch.setattr(broker, "_request_json", request)
    result = _run(tmp_path, broker)
    action, = result["actions"]
    if enabled:
        payload, = posts
        assert payload["type"] == "market"
        assert payload["position_intent"] == "sell_to_close"
        assert payload["qty"] == "1"
        assert "limit_price" not in payload
        assert payload["client_order_id"] == action["client_order_id"]
        assert action["exit_status"] == "pending"
    else:
        assert posts == []
        assert action["submitted"] is False
        assert action["error"] == "risk_check_not_approved"
        assert result["exit_protection"]["status"] == "attention_required"
