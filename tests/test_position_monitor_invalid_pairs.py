"""Actual-monitor offline proof: one invalid pair must not stop independent exits."""
from dataclasses import replace
from datetime import UTC, datetime
import json

import pytest

import autobott_v2.position_monitor as monitor
from autobott_v2.execution_models import OrderSide, OrderType
from autobott_v2.execution_journal import append_monitor_exit_event
from autobott_v2.position_store import save_open_positions
from test_position_monitor_pair_lifecycle import (
    FakeBroker, PRIMARY, RUNNER, _broker_position, _stored_positions, _run,
)


LOSER = "QQQ261016P00726000"
INVALID_INPUTS = [
    (field, value)
    for field, values in (
        ("current_price", ["broken", "", "nan", "inf", "-inf", "-0.01"]),
        ("avg_entry_price", ["broken", "", "nan", "inf", "-1", "0"]),
        ("qty", ["broken", "", "nan", "inf", "-1", "0", "1.5"]),
        ("unrealized_plpc", ["broken", "", "nan", "inf", "-inf"]),
    )
    for value in values
]


def _broker(symbol, field, value, *, primary_mark=.45, lone=False):
    pair = [_broker_position(PRIMARY, entry=.70, current=primary_mark),
            _broker_position(RUNNER, entry=.25, current=.25)]
    broker = FakeBroker(pair)
    # Original entry-fill evidence remains separate from a damaged position snapshot.
    broker.positions = [dict(row) for row in pair if not lone or row["symbol"] == RUNNER]
    for row in broker.positions:
        if row["symbol"] == symbol:
            row[field] = value
    broker.positions.append(_broker_position(LOSER, entry=5, current=3.5))
    return broker


def _blocked(result):
    return {issue["symbol"] for issue in result["exit_protection"]["issues"]
            if issue["status"] == "blocked"}


@pytest.mark.parametrize("symbol", [PRIMARY, RUNNER])
@pytest.mark.parametrize("field,value", INVALID_INPUTS)
def test_invalid_pair_inputs_are_held_without_interrupting_independent_stop(tmp_path, symbol, field, value):
    broker = _broker(symbol, field, value)
    reads = []
    original_read = broker.get_order

    def read(order_id):
        reads.append(order_id)
        return original_read(order_id)

    broker.get_order = read
    peaks = {PRIMARY: .1, RUNNER: .1}
    result = _run(tmp_path, broker, trailing=peaks)
    assert [(a["symbol"], a["reason"]) for a in result["actions"]] == [(LOSER, "stop_loss")]
    assert [intent.option_symbol for intent in broker.submitted] == [LOSER]
    assert broker.submitted[0].side is OrderSide.SELL_TO_CLOSE
    assert broker.submitted[0].order_type is OrderType.MARKET
    assert broker.canceled == []
    assert reads == ["primary-order", "runner-order"]  # Existing entry reconciliation only.
    assert result["ok"] is False
    assert result["exit_protection"]["status"] == "attention_required"
    assert _blocked(result) == {PRIMARY, RUNNER}
    assert all(field in issue["reason"] for issue in result["exit_protection"]["issues"]
               if issue["status"] == "blocked")
    saved = json.loads((tmp_path / "trailing.json").read_text())
    assert {symbol: saved[symbol] for symbol in peaks} == peaks


def test_invalid_peer_does_not_apply_old_standalone_profit_target(tmp_path):
    broker = _broker(RUNNER, "current_price", "nan", primary_mark=.945)
    result = _run(tmp_path, broker)
    assert [a["symbol"] for a in result["actions"]] == [LOSER]
    assert _blocked(result) == {PRIMARY, RUNNER}
    assert all(intent.option_symbol != PRIMARY for intent in broker.submitted)


@pytest.mark.parametrize("field,value", INVALID_INPUTS)
def test_invalid_lone_runner_is_visible_and_independent_stop_continues(tmp_path, field, value):
    result = _run(tmp_path, _broker(RUNNER, field, value, lone=True))
    assert [(a["symbol"], a["reason"]) for a in result["actions"]] == [(LOSER, "stop_loss")]
    assert _blocked(result) == {RUNNER}


@pytest.mark.parametrize("reason", ["dte_floor", "trim_excess_contracts", "position_cost_cap_breached"])
def test_evaluable_hard_safeguard_survives_invalid_peer(tmp_path, monkeypatch, reason):
    broker = _broker(RUNNER, "unrealized_plpc", "broken")
    broker.positions = [row for row in broker.positions if row["symbol"] != LOSER]
    primary = broker.positions[0]
    rules = monitor.PositionMonitorRules(exit_min_dte=-1)
    if reason == "dte_floor":
        monkeypatch.setattr(monitor, "_monitor_now", lambda: datetime(2026, 10, 16, tzinfo=UTC))
        rules = replace(rules, exit_min_dte=0)
    elif reason == "trim_excess_contracts":
        primary["qty"] = "2"
    else:
        primary.update(avg_entry_price="12", current_price="12", unrealized_plpc="0")
    result = _run(tmp_path, broker, rules=rules)
    assert [(a["symbol"], a["reason"]) for a in result["actions"]] == [(PRIMARY, reason)]
    assert [i.option_symbol for i in broker.submitted] == [PRIMARY]
    assert broker.submitted[0].quantity == 1
    assert broker.submitted[0].order_type is OrderType.MARKET
    assert _blocked(result) == {PRIMARY, RUNNER}


def test_invalid_pair_does_not_stop_another_valid_pair(tmp_path):
    broker = _broker(RUNNER, "qty", "broken")
    other_primary, other_runner = "VIX261016C00018000", "VIX261016C00021000"
    stores = _stored_positions()
    stores += [replace(stores[0], option_symbol=other_primary, paired_option_symbol=other_runner,
                       trade_group_id="other-group", broker_order_id="other-primary-entry"),
               replace(stores[1], option_symbol=other_runner, paired_option_symbol=other_primary,
                       trade_group_id="other-group", broker_order_id="other-runner-entry")]
    save_open_positions(stores, store_path=tmp_path / "open_positions.json")
    broker.positions += [_broker_position(other_primary, entry=.70, current=.10),
                         _broker_position(other_runner, entry=.25, current=.05)]
    result = _run(tmp_path, broker)
    assert {(a["symbol"], a["reason"]) for a in result["actions"]} == {
        (LOSER, "stop_loss"), (other_primary, "pair_max_loss_reached"),
        (other_runner, "pair_max_loss_reached")}
    assert _blocked(result) == {PRIMARY, RUNNER}
    assert len(broker.submitted) == 3


def test_valid_next_observation_recovers_pair_without_duplicate_independent_exit(tmp_path):
    broker = _broker(PRIMARY, "current_price", "nan")
    assert _blocked(_run(tmp_path, broker)) == {PRIMARY, RUNNER}
    broker.positions[0]["current_price"] = ".45"
    result = _run(tmp_path, broker)
    assert _blocked(result) == set()
    assert result["exit_protection"]["status"] == "awaiting_fill"
    assert result["pair_groups_managed"] == 1
    assert len(broker.submitted) == 1
    assert broker.canceled == []


def test_invalid_pair_retains_existing_unresolved_exit_evidence(tmp_path):
    broker = _broker(PRIMARY, "qty", "broken")
    append_monitor_exit_event({"symbol": PRIMARY, "reason": "pair_max_loss_reached",
                               "exit_status": "uncertain", "entry_broker_order_id": "primary-order"},
                              journal_path=str(tmp_path / "journal.jsonl"))
    result = _run(tmp_path, broker)
    assert _blocked(result) == {PRIMARY, RUNNER}
    assert any(issue["symbol"] == PRIMARY and issue["status"] == "uncertain"
               for issue in result["exit_protection"]["issues"])
    assert [intent.option_symbol for intent in broker.submitted] == [LOSER]
