import pytest

from autobott_v2.execution_models import ExecutionState, OrderSide
from autobott_v2.execution_journal import append_monitor_exit_event, append_execution_outcome, load_execution_journal
from autobott_v2.jsonl_retention import compact_jsonl_tail
from autobott_v2.position_monitor import PositionMonitorRules
from test_position_monitor_pair_lifecycle import FakeBroker, PRIMARY, RUNNER, _broker_position, _run


def losing_broker():
    return FakeBroker([_broker_position(PRIMARY, entry=.70, current=.20),
                       _broker_position(RUNNER, entry=.25, current=.10)])


@pytest.mark.parametrize("outcome,expected", [
    ("uncertain", "attention_required"), ("attempting", "attention_required"),
    ("rejected", "attention_required"), ("blocked", "attention_required"),
    ("pending", "attention_required"), ("broker_reported_filled", "awaiting_reconciliation"),
])
def test_existing_exit_evidence_survives_no_new_trigger_without_execution(tmp_path, outcome, expected):
    broker = FakeBroker([_broker_position(PRIMARY, entry=.70, current=.70),
                         _broker_position(RUNNER, entry=.25, current=.25)])
    append_monitor_exit_event({"symbol": PRIMARY, "reason": "pair_max_loss_reached", "exit_status": outcome,
                               "entry_broker_order_id": "primary-order"}, journal_path=tmp_path / "journal.jsonl")
    before = (tmp_path / "journal.jsonl").read_bytes()
    result = _run(tmp_path, broker)
    assert result["actions"] == []  # Existing observations are not new trading actions.
    assert result["exit_protection"]["status"] == expected
    assert result["exit_protection"]["issues"][0]["symbol"] == PRIMARY
    assert broker.submitted == broker.canceled == []
    assert (tmp_path / "journal.jsonl").read_bytes() == before
    broker.positions = []
    assert _run(tmp_path, broker)["exit_protection"]["status"] == "monitoring"


@pytest.mark.parametrize("state,count", [("new", "pending_count"), ("partially_filled", "partially_filled_count")])
def test_existing_open_sell_without_fresh_trigger_stays_visible(tmp_path, state, count):
    broker = FakeBroker([_broker_position(PRIMARY, entry=.70, current=.70),
                         _broker_position(RUNNER, entry=.25, current=.25)])
    broker.orders["existing-exit"] = {"id": "existing-exit", "symbol": PRIMARY, "side": "sell", "status": state}
    result = _run(tmp_path, broker)
    assert result["actions"] == []
    assert result["exit_protection"]["status"] == "awaiting_fill"
    assert result["exit_protection"][count] == 1
    assert broker.submitted == broker.canceled == []


def test_old_holding_receipt_and_unavailable_inventory_do_not_create_false_recovery(tmp_path):
    broker = FakeBroker([_broker_position(PRIMARY, entry=.70, current=.70),
                         _broker_position(RUNNER, entry=.25, current=.25)])
    append_monitor_exit_event({"symbol": PRIMARY, "reason": "stop_loss", "exit_status": "uncertain",
                               "entry_broker_order_id": "previous-holding"}, journal_path=tmp_path / "journal.jsonl")
    assert _run(tmp_path, broker)["exit_protection"]["status"] == "monitoring"
    broker.open_orders_error = True
    result = _run(tmp_path, broker)
    assert result["ok"] is False
    assert result["exit_protection"]["status"] == "unavailable"
    assert broker.submitted == broker.canceled == []


def test_accepted_exit_without_identity_is_visible_as_uncertain(tmp_path):
    from dataclasses import replace
    broker = losing_broker()
    original_submit = broker.submit_order

    def without_identity(intent, **kwargs):
        return replace(original_submit(intent, **kwargs), broker_order_id=None)

    broker.submit_order = without_identity
    result = _run(tmp_path, broker)
    assert len(broker.submitted) == 2
    assert result["ok"] is False
    assert result["exit_protection"]["status"] == "attention_required"
    assert result["exit_protection"]["uncertain_count"] == 2


@pytest.mark.parametrize("state,count", [(ExecutionState.SUBMITTED, "pending_count"),
                                        (ExecutionState.PARTIALLY_FILLED, "partially_filled_count")])
def test_accepted_exit_waits_for_fill_without_claiming_closed(tmp_path, state, count):
    broker = losing_broker()
    broker.submit_state = state
    result = _run(tmp_path, broker)
    assert result["ok"] is True
    assert result["exit_protection"]["status"] == "awaiting_fill"
    assert result["exit_protection"][count] == 2


@pytest.mark.parametrize("state", [ExecutionState.REJECTED, ExecutionState.FAILED, ExecutionState.CANCELED,
                                 ExecutionState.DRAFT, ExecutionState.APPROVED])
def test_returned_failure_is_attempted_but_not_accepted(tmp_path, state):
    broker = losing_broker()
    broker.submit_state = state
    result = _run(tmp_path, broker)
    assert len(result["actions"]) == 2
    assert all(a["attempted"] and not a["submitted"] and a["exit_status"] == state.value for a in result["actions"])
    assert result["ok"] is False
    assert result["exit_protection"]["status"] == "attention_required"
    assert result["exit_protection"]["failed_count"] == 2
    assert {issue["symbol"] for issue in result["exit_protection"]["issues"]} == {PRIMARY, RUNNER}
    assert all(issue["reason"] for issue in result["exit_protection"]["issues"])
    rows = load_execution_journal(journal_path=tmp_path / "journal.jsonl")
    assert sum(r["payload"].get("disposition") == "position_monitor_exit_not_accepted" for r in rows) == 2


@pytest.mark.parametrize("profit", [False, True])
def test_cancel_ack_is_not_confirmation_and_filled_stale_position_does_not_resell(tmp_path, profit):
    positions = [_broker_position(PRIMARY, entry=.70, current=2 if profit else .20)]
    if not profit:
        positions.append(_broker_position(RUNNER, entry=.25, current=.25))
    broker = FakeBroker(positions)
    broker.orders["existing-sell"] = {"id": "existing-sell", "symbol": PRIMARY, "side": "sell",
                                       "status": "new", "type": "limit", "filled_qty": "0"}

    def pending_cancel(order_id):
        broker.canceled.append(order_id)
        broker.orders[order_id]["status"] = "pending_cancel"
        return {"id": order_id, "status": "canceled"}  # Transport acknowledgment only.

    broker.cancel_order = pending_cancel
    first = _run(tmp_path, broker)
    assert not any(i.option_symbol == PRIMARY for i in broker.submitted)
    primary = next(a for a in first["actions"] if a["symbol"] == PRIMARY)
    assert primary["exit_status"] == "uncertain"
    broker.orders["existing-sell"].update(status="filled", filled_qty="1", filled_avg_price=".20")
    second = _run(tmp_path, broker)
    assert not any(i.option_symbol == PRIMARY for i in broker.submitted)
    assert next(a for a in second["actions"] if a["symbol"] == PRIMARY)["exit_status"] == "broker_reported_filled"


def test_unknown_open_orders_cannot_cause_blind_exit(tmp_path):
    broker = losing_broker()
    broker.open_orders_error = True
    result = _run(tmp_path, broker)
    assert broker.submitted == []
    assert all(a["error"] == "open_orders_unavailable" for a in result["actions"])


def test_post_uncertainty_survives_restart_without_second_sell(tmp_path):
    broker = losing_broker()

    def uncertain(intent, **kwargs):
        broker.submitted.append(intent)
        raise TimeoutError("synthetic unknown POST result")

    broker.submit_order = uncertain
    first = _run(tmp_path, broker)
    ids = {a["symbol"]: a["client_order_id"] for a in first["actions"]}
    assert len(broker.submitted) == 2
    second = _run(tmp_path, broker)
    assert len(broker.submitted) == 2
    assert {a["symbol"]: a["client_order_id"] for a in second["actions"]} == ids
    assert all(a["exit_status"] == "uncertain" and not a["attempted"] for a in second["actions"])


def test_pending_and_filled_exit_are_reconciled_before_new_sell(tmp_path):
    broker = losing_broker()
    first = _run(tmp_path, broker)
    assert len(broker.submitted) == 2
    _run(tmp_path, broker)
    assert len(broker.submitted) == 2
    for action in first["actions"]:
        broker.orders[action["broker_order_id"]].update(status="filled", filled_qty="1", filled_avg_price=".10")
    last = _run(tmp_path, broker)  # Deliberately stale broker position snapshot.
    assert len(broker.submitted) == 2
    assert all(a["exit_status"] == "broker_reported_filled" for a in last["actions"])
    assert last["exit_protection"]["status"] == "awaiting_reconciliation"
    broker.positions = []
    reconciled = _run(tmp_path, broker)
    assert reconciled["exit_protection"]["status"] == "monitoring"
    assert reconciled["exit_protection"]["reported_fill_count"] == 0


def test_failed_durable_record_blocks_submission(tmp_path, monkeypatch):
    broker = losing_broker()
    monkeypatch.setattr("autobott_v2.position_monitor.append_monitor_exit_event",
                        lambda *_a, **_k: (_ for _ in ()).throw(OSError("synthetic journal failure")))
    result = _run(tmp_path, broker)
    assert broker.submitted == []
    assert not result["ok"]
    assert all(a.get("journal_error") for a in result["actions"])


def test_post_acceptance_journal_error_keeps_accepted_state(tmp_path, monkeypatch):
    broker = losing_broker()
    monkeypatch.setattr("autobott_v2.position_monitor.append_order_submission",
                        lambda *_a, **_k: (_ for _ in ()).throw(OSError("synthetic journal failure")))
    result = _run(tmp_path, broker)
    assert len(broker.submitted) == 2
    assert all(a["submitted"] and a["exit_status"] == "pending" and a.get("journal_error") for a in result["actions"])
    assert not result["ok"]


def test_exit_receipt_survives_tail_compaction(tmp_path, monkeypatch):
    path = tmp_path / "journal.jsonl"
    monkeypatch.setattr("autobott_v2.execution_journal.compact_jsonl_tail",
                        lambda path, **kwargs: compact_jsonl_tail(path, max_bytes=400, retain_bytes=200, **kwargs))
    append_monitor_exit_event({"symbol": PRIMARY, "exit_status": "uncertain", "client_order_id": "synthetic-id"}, journal_path=path)
    for i in range(20):
        append_execution_outcome(decision_id=str(i), thesis_id=None, symbol="synthetic", disposition="scan", journal_path=path)
    rows = load_execution_journal(journal_path=path, max_tail_bytes=100)
    assert rows[0]["event_type"] == "position_monitor_exit_event"
    assert rows[0]["payload"]["client_order_id"] == "synthetic-id"


def test_broker_uses_predeclared_monitor_client_identity(monkeypatch):
    from test_execution_broker import _config, _intent
    from autobott_v2.execution_broker import AlpacaExecutionBroker
    broker = AlpacaExecutionBroker(_config())
    client = "autobott-exit-" + "a" * 32
    calls = []

    def submit(intent, *, client_order_id):
        calls.append(client_order_id)
        return {"id": "synthetic-order", "status": "new", "client_order_id": client_order_id}

    monkeypatch.setattr(broker, "_submit_with_reconciliation", submit)
    intent = _intent(side=OrderSide.SELL_TO_CLOSE, metadata={"position_monitor": True, "exit_client_order_id": client})
    order = broker.submit_order(intent)
    assert calls == [client]
    assert order.client_order_id == client


@pytest.mark.parametrize("profit", [False, True])
def test_pending_sell_without_identity_never_causes_replacement(tmp_path, profit):
    broker = FakeBroker([_broker_position(PRIMARY, entry=.70, current=2 if profit else .20)])
    broker.orders["anonymous"] = {"symbol": PRIMARY, "side": "sell", "status": "new", "filled_qty": "0"}
    result = _run(tmp_path, broker)
    assert broker.submitted == []
    assert result["actions"][0]["exit_status"] == "uncertain"
    assert "exit_cancellation" in result["actions"][0]["error"]


def seed_profit_receipt(tmp_path, broker):
    broker.orders["profit-order"] = {"id": "profit-order", "symbol": PRIMARY, "side": "sell", "status": "new",
                                     "type": "limit", "limit_price": "1.20", "filled_qty": "0"}
    append_monitor_exit_event({"symbol": PRIMARY, "reason": "take_profit", "exit_status": "pending",
                               "broker_order_id": "profit-order", "entry_broker_order_id": "primary-order",
                               "position_quantity": 1}, journal_path=tmp_path / "journal.jsonl")


def test_direct_reconciled_profit_order_cannot_be_lost_by_stale_enumeration(tmp_path):
    broker = FakeBroker([_broker_position(PRIMARY, entry=.70, current=.20)])
    seed_profit_receipt(tmp_path, broker)
    broker.list_orders = lambda **kwargs: []
    def unconfirmed(order_id):
        broker.canceled.append(order_id)
        broker.orders[order_id]["status"] = "pending_cancel"
    broker.cancel_order = unconfirmed
    result = _run(tmp_path, broker)
    assert broker.canceled == ["profit-order"]
    assert broker.submitted == []
    assert result["actions"][0]["broker_order_id"] == "profit-order"
    assert result["actions"][0]["exit_status"] == "uncertain"


def test_unavailable_enumeration_preserves_profit_identity_across_restart(tmp_path):
    broker = FakeBroker([_broker_position(PRIMARY, entry=.70, current=.20)])
    seed_profit_receipt(tmp_path, broker)
    broker.open_orders_error = True
    first = _run(tmp_path, broker)
    assert first["actions"][0]["exit_status"] == "uncertain"
    assert first["actions"][0]["broker_order_id"] == "profit-order"
    broker.open_orders_error = False
    broker.list_orders = lambda **kwargs: []
    broker.order_read_errors.add("profit-order")
    second = _run(tmp_path, broker)
    assert second["actions"][0]["broker_order_id"] == "profit-order"
    assert second["actions"][0]["exit_status"] == "uncertain"
    assert broker.submitted == []
    broker.order_read_errors.clear()
    third = _run(tmp_path, broker)
    assert broker.canceled == ["profit-order"]
    assert len(broker.submitted) == 1
    assert third["actions"][0]["submitted"]


def test_funding_exit_receipt_prevents_duplicate_after_missing_pair_state_save(tmp_path):
    broker = FakeBroker([_broker_position(PRIMARY, entry=.70, current=1.1),
                         _broker_position(RUNNER, entry=.25, current=.25)])
    first = _run(tmp_path, broker)
    assert first["actions"][0]["reason"] == "primary_profit_funds_runner"
    assert len(broker.submitted) == 1
    (tmp_path / "pair_state.json").write_text("{}", encoding="utf-8")  # Crash before pair-state publication.
    broker.list_orders = lambda **kwargs: []  # Enumeration cannot reconstruct the missing funding latch.
    second = _run(tmp_path, broker)
    assert len(broker.submitted) == 1
    assert second["actions"][0]["broker_order_id"] == first["actions"][0]["broker_order_id"]
    assert second["actions"][0]["exit_status"] == "pending"


@pytest.mark.parametrize("lost_response", [False, True])
def test_profit_replacement_receipt_and_chain_allow_later_loss_exit(tmp_path, lost_response):
    broker = FakeBroker([_broker_position(PRIMARY, entry=.70, current=.98)])
    seed_profit_receipt(tmp_path, broker)
    def replace_order(order_id, *, limit_price):
        rows = load_execution_journal(journal_path=tmp_path / "journal.jsonl")
        assert rows[-1]["event_type"] == "position_monitor_exit_event"
        assert rows[-1]["payload"]["exit_status"] == "uncertain"
        assert rows[-1]["payload"]["broker_order_id"] == order_id
        broker.orders[order_id].update(status="replaced", replaced_by="successor")
        broker.orders["successor"] = {"id": "successor", "symbol": PRIMARY, "side": "sell", "status": "new",
                                      "type": "limit", "limit_price": str(limit_price), "filled_qty": "0"}
        if lost_response:
            raise TimeoutError("synthetic unknown PATCH result")
        return dict(broker.orders["successor"])
    broker.replace_order = replace_order
    first = _run(tmp_path, broker)
    assert first["actions"][0]["replace_attempted"]
    assert first["actions"][0]["replaced"] is (not lost_response)
    rows = load_execution_journal(journal_path=tmp_path / "journal.jsonl")
    assert rows[-1 if lost_response else -2]["event_type"] == "position_monitor_exit_event"
    broker.positions[0] = _broker_position(PRIMARY, entry=.70, current=.20)
    broker.list_orders = lambda **kwargs: []  # Direct chain reconciliation must recover the successor.
    second = _run(tmp_path, broker)
    assert broker.canceled == ["successor"]
    assert len(broker.submitted) == 1
    assert second["actions"][0]["submitted"]


def test_replacement_chain_partial_fill_blocks_stale_remaining_quantity(tmp_path):
    broker = FakeBroker([_broker_position(PRIMARY, entry=.70, current=.20)])
    seed_profit_receipt(tmp_path, broker)
    broker.orders["profit-order"].update(status="replaced", replaced_by="successor", filled_qty="1")
    broker.orders["successor"] = {"id": "successor", "symbol": PRIMARY, "side": "sell", "status": "new", "filled_qty": "0"}
    result = _run(tmp_path, broker)
    assert broker.submitted == []
    assert result["actions"][0]["exit_status"] == "uncertain"
    assert result["actions"][0]["error"] == "prior_exit_position_fill_requires_reconciliation"


def test_failed_durable_reprice_record_blocks_patch(tmp_path, monkeypatch):
    broker = FakeBroker([_broker_position(PRIMARY, entry=.70, current=.98)])
    seed_profit_receipt(tmp_path, broker)
    broker.replace_order = lambda *_a, **_kw: pytest.fail("PATCH preceded durable receipt")
    monkeypatch.setattr("autobott_v2.position_monitor.append_monitor_exit_event",
                        lambda *_a, **_kw: (_ for _ in ()).throw(OSError("synthetic write failure")))
    result = _run(tmp_path, broker)
    assert result["actions"][0]["journal_error"]


def test_successful_patch_preserves_predecessor_fills_and_original_quantity(tmp_path):
    broker = FakeBroker([_broker_position(PRIMARY, entry=.30, current=.42)])
    broker.positions[0]["qty"] = "2"
    broker.orders["primary-order"]["filled_qty"] = "2"
    seed_profit_receipt(tmp_path, broker)
    append_monitor_exit_event({"symbol": PRIMARY, "reason": "take_profit", "exit_status": "pending",
                               "broker_order_id": "profit-order", "entry_broker_order_id": "primary-order",
                               "position_quantity": 2}, journal_path=tmp_path / "journal.jsonl")
    def replace_order(order_id, *, limit_price):
        broker.orders[order_id].update(status="replaced", replaced_by="successor", filled_qty="1")
        broker.orders["successor"] = {"id": "successor", "symbol": PRIMARY, "side": "sell", "status": "new",
                                      "limit_price": str(limit_price), "filled_qty": "0"}
        return dict(broker.orders["successor"])
    broker.replace_order = replace_order
    rules = PositionMonitorRules(exit_min_dte=-1, max_contracts_per_option=2)
    first = _run(tmp_path, broker, rules=rules)
    assert first["actions"][0]["replaced"]
    assert first["actions"][0]["replacement_from_order_id"] == "profit-order"
    broker.positions[0].update(current_price=".10", unrealized_plpc="-.67")
    broker.list_orders = lambda **kwargs: []
    stale = _run(tmp_path, broker, rules=rules)
    assert stale["actions"][0]["error"] == "prior_exit_position_fill_requires_reconciliation"
    assert broker.submitted == []
    broker.positions[0]["qty"] = "1"
    reconciled = _run(tmp_path, broker, rules=rules)
    assert len(broker.submitted) == 1
    assert broker.submitted[0].quantity == 1
    assert reconciled["actions"][0]["submitted"]
    assert "replacement_from_order_id" not in reconciled["actions"][0]


def test_excess_quantity_trim_confirms_existing_sell_cancel_before_submission(tmp_path):
    broker = FakeBroker([_broker_position(PRIMARY, entry=.30, current=.42)])
    broker.positions[0]["qty"] = "2"
    seed_profit_receipt(tmp_path, broker)
    broker.orders["profit-order"]["status"] = "pending_cancel"
    broker.cancel_order = lambda order_id: {"id": order_id, "status": "canceled"}
    result = _run(tmp_path, broker)
    assert result["actions"][0]["reason"] == "trim_excess_contracts"
    assert result["actions"][0]["exit_status"] == "uncertain"
    assert broker.submitted == []


def test_held_heartbeat_receipts_are_bounded_but_new_attempts_are_retained(tmp_path):
    path = tmp_path / "journal.jsonl"
    action = {"symbol": PRIMARY, "exit_status": "uncertain", "client_order_id": "attempt-1", "attempted": False}
    for price in range(100):
        append_monitor_exit_event({**action, "current_price": price / 100}, journal_path=path)
    append_monitor_exit_event({**action, "client_order_id": "attempt-2"}, journal_path=path)
    append_monitor_exit_event({**action, "client_order_id": "attempt-2", "exit_status": "pending"}, journal_path=path)
    rows = load_execution_journal(journal_path=path)
    assert len(rows) == 3
    assert [r["payload"]["client_order_id"] for r in rows] == ["attempt-1", "attempt-2", "attempt-2"]
