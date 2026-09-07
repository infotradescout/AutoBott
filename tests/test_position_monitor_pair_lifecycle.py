from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

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
        self.orders = {
            ("primary-order" if position["symbol"] == PRIMARY else "runner-order"): {
                "id": "primary-order" if position["symbol"] == PRIMARY else "runner-order",
                "symbol": position["symbol"], "side": "buy", "status": "filled",
                "filled_qty": position["qty"], "filled_avg_price": position["avg_entry_price"],
            }
            for position in positions
        }
        self.order_read_errors = set()
        self.open_orders_error = False
        self.submit_state = ExecutionState.SUBMITTED

    def list_open_positions(self):
        return self.positions

    def list_orders(self, *, status="open", limit=100, direction="desc"):
        if self.open_orders_error:
            raise RuntimeError("broker unavailable")
        return [
            dict(order) for order in self.orders.values()
            if order["status"] not in {"filled", "canceled", "rejected", "expired"}
        ]

    def get_order(self, broker_order_id):
        if broker_order_id in self.order_read_errors:
            raise RuntimeError("broker unavailable")
        return dict(self.orders[broker_order_id])

    def cancel_order(self, broker_order_id):
        self.canceled.append(broker_order_id)
        self.orders[broker_order_id]["status"] = "canceled"
        return {"id": broker_order_id, "status": "canceled"}

    def submit_order(self, intent, *, current_daily_realized_pnl=0.0, open_positions=0):
        self.submitted.append(intent)
        broker_id = f"exit-broker-{len(self.submitted)}"
        self.orders[broker_id] = {
            "id": broker_id, "symbol": intent.option_symbol, "side": "sell",
            "status": "new" if self.submit_state is ExecutionState.SUBMITTED else self.submit_state.value,
            "filled_qty": "0", "filled_avg_price": None,
        }
        return ExecutionOrder(
            order_id=f"exit-{len(self.submitted)}",
            client_order_id=f"exit-client-{len(self.submitted)}",
            intent=intent,
            state=self.submit_state,
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


def _run(tmp_path, broker, *, trailing=None, pair_state=None, rules=None, monitor=run_position_monitor):
    save_runtime_state(default_runtime_state())
    store_path = tmp_path / "open_positions.json"
    trail_path = tmp_path / "trailing.json"
    pair_path = tmp_path / "pair_state.json"
    if not store_path.exists():
        save_open_positions(_stored_positions(), store_path=store_path)
    if trailing is not None:
        trail_path.write_text(json.dumps(trailing), encoding="utf-8")
    if pair_state is not None:
        pair_path.write_text(json.dumps(pair_state), encoding="utf-8")
    return monitor(
        broker=broker,
        rules=rules or PositionMonitorRules(exit_min_dte=-1),
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
    assert action["runner_funded"] is False
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


def _funding_broker():
    return FakeBroker([
        _broker_position(PRIMARY, entry=0.70, current=0.95),
        _broker_position(RUNNER, entry=0.25, current=0.34),
    ])


def _state(tmp_path):
    return json.loads((tmp_path / "pair_state.json").read_text())[GROUP]


@pytest.mark.parametrize("status", ["new", "accepted", "pending_cancel", "suspended", "done_for_day"])
def test_pending_exit_is_reconciled_without_duplicate_sells(tmp_path, status) -> None:
    from autobott_v2.position_monitor_v2 import run_position_monitor as run_v2

    broker = _funding_broker()
    _run(tmp_path, broker)
    broker.orders["exit-broker-1"]["status"] = status

    result = _run(tmp_path, broker, monitor=run_v2)

    assert result["actions"] == []
    assert len(broker.submitted) == 1
    assert _state(tmp_path)["funding_exit_order_id"] == "exit-broker-1"
    assert not _state(tmp_path)["runner_funded"]


@pytest.mark.parametrize("status", ["canceled", "rejected", "expired"])
def test_terminal_unfilled_funding_exit_can_retry(tmp_path, status) -> None:
    broker = _funding_broker()
    _run(tmp_path, broker)
    broker.orders["exit-broker-1"]["status"] = status

    result = _run(tmp_path, broker)

    assert len(broker.submitted) == 2
    assert result["actions"][0]["broker_order_id"] == "exit-broker-2"
    assert not _state(tmp_path)["runner_funded"]
    assert _state(tmp_path)["primary_realized_pnl"] == 0


def test_rejected_submission_response_is_reconciled_and_retried(tmp_path) -> None:
    broker = _funding_broker()
    broker.submit_state = ExecutionState.REJECTED
    _run(tmp_path, broker)
    broker.submit_state = ExecutionState.SUBMITTED

    _run(tmp_path, broker)

    assert len(broker.submitted) == 2
    assert _state(tmp_path)["funding_exit_orders"]["exit-broker-1"]["status"] == "rejected"


def test_funding_uses_actual_exit_fill_and_entry_basis(tmp_path) -> None:
    broker = _funding_broker()
    _run(tmp_path, broker)
    assert broker.submitted[0].limit_price == 0.93
    broker.orders["exit-broker-1"].update(status="filled", filled_qty="1", filled_avg_price="0.93")
    broker.positions = [_broker_position(RUNNER, entry=0.25, current=0.05)]

    result = _run(tmp_path, broker)

    assert _state(tmp_path)["primary_realized_pnl"] == 23.0
    assert _state(tmp_path)["primary_entry_filled_avg_price"] == 0.70
    assert _state(tmp_path)["primary_entry_filled_qty"] == 1
    assert _state(tmp_path)["funding_verified"]
    assert not _state(tmp_path)["runner_funded"]
    assert result["actions"][0]["reason"] == "unfunded_runner_stop_loss"


def test_filled_primary_funds_runner_before_first_missing_primary_decision(tmp_path) -> None:
    broker = _funding_broker()
    _run(tmp_path, broker)
    broker.orders["exit-broker-1"].update(status="filled", filled_qty="1", filled_avg_price="1.00")
    broker.positions = [_broker_position(RUNNER, entry=0.25, current=0.50)]

    first = _run(tmp_path, broker)
    second = _run(tmp_path, broker)

    assert first["actions"] == second["actions"] == []
    assert first["pair_groups_managed"] == second["pair_groups_managed"] == 1
    assert len(broker.submitted) == 1
    assert _state(tmp_path)["runner_funded"]
    assert _state(tmp_path)["funding_verified"]
    assert _state(tmp_path)["primary_realized_pnl"] == 30.0


def test_restart_preserves_pending_exit_and_original_entry_fills(tmp_path) -> None:
    broker = _funding_broker()
    broker.positions[0]["avg_entry_price"] = "0.68"
    broker.orders["primary-order"]["filled_avg_price"] = "0.68"
    _run(tmp_path, broker)
    restored = _funding_broker()
    restored.orders = dict(broker.orders)
    restored.orders["exit-broker-1"].update(status="filled", filled_qty="1", filled_avg_price="0.94")
    restored.positions = [_broker_position(RUNNER, entry=0.25, current=0.50)]

    result = _run(tmp_path, restored)

    assert result["actions"] == []
    assert restored.submitted == []
    assert _state(tmp_path)["primary_realized_pnl"] == 26.0
    assert _state(tmp_path)["runner_funded"]


def test_partial_fills_are_cumulative_and_canceled_remainder_retries_once(tmp_path) -> None:
    broker = _funding_broker()
    broker.positions[0] = {**_broker_position(PRIMARY, entry=0.70, current=0.83), "qty": "2"}
    broker.orders["primary-order"]["filled_qty"] = "2"
    rules = PositionMonitorRules(exit_min_dte=-1, max_contracts_per_option=2)
    _run(tmp_path, broker, rules=rules)
    broker.orders["exit-broker-1"].update(status="partially_filled", filled_qty="1", filled_avg_price="0.85")
    broker.positions[0]["qty"] = "1"

    _run(tmp_path, broker, rules=rules)
    _run(tmp_path, broker, rules=rules)
    assert len(broker.submitted) == 1
    assert _state(tmp_path)["primary_realized_pnl"] == 15.0
    assert not _state(tmp_path)["runner_funded"]
    broker.orders["exit-broker-1"]["status"] = "canceled"
    _run(tmp_path, broker, rules=rules)
    assert len(broker.submitted) == 2
    assert broker.submitted[1].quantity == 1
    broker.orders["exit-broker-2"].update(status="filled", filled_qty="1", filled_avg_price="0.80")
    broker.positions = [_broker_position(RUNNER, entry=0.25, current=0.50)]

    result = _run(tmp_path, broker, rules=rules)

    assert result["actions"] == []
    assert _state(tmp_path)["primary_exit_filled_qty"] == 2
    assert _state(tmp_path)["primary_realized_pnl"] == 25.0
    assert _state(tmp_path)["runner_funded"]


def test_filled_order_with_stale_primary_position_does_not_sell_again(tmp_path) -> None:
    broker = _funding_broker()
    _run(tmp_path, broker)
    broker.orders["exit-broker-1"].update(status="filled", filled_qty="1", filled_avg_price="1.00")

    result = _run(tmp_path, broker)

    assert result["actions"] == []
    assert len(broker.submitted) == 1
    assert _state(tmp_path)["funding_exit_blocked"]
    assert _state(tmp_path)["runner_funded"]


def test_missing_latch_recovers_existing_pending_sell_identity(tmp_path) -> None:
    broker = _funding_broker()
    _run(tmp_path, broker)
    (tmp_path / "pair_state.json").unlink()

    result = _run(tmp_path, broker)

    assert result["actions"] == []
    assert len(broker.submitted) == 1
    assert _state(tmp_path)["funding_exit_order_id"] == "exit-broker-1"


def test_unknown_order_read_blocks_duplicates_then_retries_after_confirmed_cancel(tmp_path) -> None:
    broker = _funding_broker()
    _run(tmp_path, broker)
    broker.orders["exit-broker-1"]["status"] = "canceled"
    broker.order_read_errors.add("exit-broker-1")

    result = _run(tmp_path, broker)

    assert result["actions"] == []
    assert len(broker.submitted) == 1
    assert not _state(tmp_path)["runner_funded"]
    broker.order_read_errors.clear()
    _run(tmp_path, broker)
    assert len(broker.submitted) == 2


def test_unknown_open_order_read_does_not_create_a_permanent_latch(tmp_path) -> None:
    broker = _funding_broker()
    broker.open_orders_error = True

    result = _run(tmp_path, broker)

    assert result["actions"] == []
    assert broker.submitted == []
    broker.open_orders_error = False
    _run(tmp_path, broker)
    assert len(broker.submitted) == 1


def test_stale_latch_without_order_identity_never_promotes_from_absence(tmp_path) -> None:
    broker = FakeBroker([_broker_position(RUNNER, entry=0.25, current=0.50)])
    state = {GROUP: {"funding_exit_submitted": True, "primary_profit_at_exit_submission": 30.0}}

    first = _run(tmp_path, broker, pair_state=state)
    second = _run(tmp_path, broker)

    assert first["actions"] == second["actions"] == []
    assert not _state(tmp_path)["runner_funded"]
    assert not _state(tmp_path)["funding_verified"]
    assert _state(tmp_path)["primary_realized_pnl"] == 0.0


def test_unverified_legacy_runner_retains_protection_without_funding_claim(tmp_path) -> None:
    broker = FakeBroker([_broker_position(RUNNER, entry=0.25, current=0.05)])
    state = {GROUP: {"runner_funded": True, "primary_realized_pnl_estimate": 30.0}}

    result = _run(tmp_path, broker, pair_state=state)

    assert result["actions"] == []
    assert _state(tmp_path)["legacy_runner_protection"]
    assert not _state(tmp_path)["runner_funded"]
    assert not _state(tmp_path)["funding_verified"]
    broker.positions = [_broker_position(RUNNER, entry=0.25, current=0.02)]
    result = _run(tmp_path, broker)
    assert result["actions"][0]["reason"] == "funded_runner_catastrophic_stop"
    assert not result["actions"][0]["runner_funded"]


def test_pending_funding_exit_does_not_disable_pair_hard_stop(tmp_path) -> None:
    broker = _funding_broker()
    _run(tmp_path, broker)
    broker.positions = [
        _broker_position(PRIMARY, entry=0.70, current=0.40),
        _broker_position(RUNNER, entry=0.25, current=0.10),
    ]

    result = _run(tmp_path, broker)

    assert {action["symbol"] for action in result["actions"]} == {PRIMARY, RUNNER}
    assert {action["reason"] for action in result["actions"]} == {"pair_max_loss_reached"}
    assert "exit-broker-1" in broker.canceled


def test_done_for_day_funding_order_resumes_and_fills_without_duplicate(tmp_path) -> None:
    broker = _funding_broker()
    _run(tmp_path, broker)
    broker.orders["exit-broker-1"]["status"] = "done_for_day"
    _run(tmp_path, broker)
    broker.orders["exit-broker-1"]["status"] = "new"
    _run(tmp_path, broker)
    broker.orders["exit-broker-1"].update(status="filled", filled_qty="1", filled_avg_price="1.00")
    broker.positions = [_broker_position(RUNNER, entry=0.25, current=0.50)]

    result = _run(tmp_path, broker)

    assert result["actions"] == []
    assert len(broker.submitted) == 1
    assert _state(tmp_path)["runner_funded"]


def test_adopted_partial_exit_recovers_missing_original_entry_quantity(tmp_path) -> None:
    broker = _funding_broker()
    broker.orders["primary-order"]["filled_qty"] = "2"
    broker.orders["prior-exit"] = {
        "id": "prior-exit", "symbol": PRIMARY, "side": "sell", "status": "partially_filled",
        "filled_qty": "1", "filled_avg_price": "0.85",
    }
    broker.order_read_errors.add("primary-order")
    rules = PositionMonitorRules(exit_min_dte=-1, max_contracts_per_option=2)
    _run(tmp_path, broker, rules=rules)
    assert broker.submitted == []
    broker.order_read_errors.clear()
    broker.orders["prior-exit"]["status"] = "canceled"

    result = _run(tmp_path, broker, rules=rules)

    assert _state(tmp_path)["primary_entry_filled_qty"] == 2
    assert _state(tmp_path)["primary_realized_pnl"] == 15.0
    assert len(result["actions"]) == 1
    assert broker.submitted[0].quantity == 1


def test_regressed_partial_fill_read_keeps_verified_profit_and_blocks_retry(tmp_path) -> None:
    broker = _funding_broker()
    _run(tmp_path, broker)
    broker.orders["exit-broker-1"].update(status="partially_filled", filled_qty="1", filled_avg_price="0.95")
    broker.positions = [_broker_position(RUNNER, entry=0.25, current=0.50)]
    _run(tmp_path, broker)
    broker.orders["exit-broker-1"].update(status="canceled", filled_qty="0", filled_avg_price=None)

    result = _run(tmp_path, broker)

    assert result["actions"] == []
    assert len(broker.submitted) == 1
    assert _state(tmp_path)["primary_realized_pnl"] == 25.0
    assert _state(tmp_path)["funding_exit_blocked"]


def test_missing_entry_basis_recovers_after_primary_store_is_pruned(tmp_path) -> None:
    broker = FakeBroker([_broker_position(RUNNER, entry=0.25, current=0.34)])
    broker.orders["primary-order"] = {
        "id": "primary-order", "symbol": PRIMARY, "side": "buy", "status": "filled",
        "filled_qty": "1", "filled_avg_price": "0.70",
    }
    broker.orders["prior-exit"] = {
        "id": "prior-exit", "symbol": PRIMARY, "side": "sell", "status": "filled",
        "filled_qty": "1", "filled_avg_price": "1.00",
    }
    broker.order_read_errors.add("primary-order")
    state = {GROUP: {"funding_exit_order_id": "prior-exit", "funding_exit_submitted": True}}
    first = _run(tmp_path, broker, pair_state=state)
    assert first["position_store_pruned"] == 1
    assert not _state(tmp_path)["runner_funded"]
    broker.order_read_errors.clear()
    broker.positions = [_broker_position(RUNNER, entry=0.25, current=0.05)]

    result = _run(tmp_path, broker)

    assert result["actions"] == []
    assert broker.submitted == []
    assert _state(tmp_path)["runner_funded"]
    assert _state(tmp_path)["primary_realized_pnl"] == 30.0


def test_partial_replacement_chain_never_double_counts_inherited_fills(tmp_path) -> None:
    broker = _funding_broker()
    broker.positions[0]["qty"] = "2"
    broker.orders["primary-order"]["filled_qty"] = "2"
    rules = PositionMonitorRules(exit_min_dte=-1, max_contracts_per_option=2)
    _run(tmp_path, broker, rules=rules)
    broker.orders["exit-broker-1"].update(
        status="replaced", filled_qty="1", filled_avg_price="0.85", replaced_by="successor",
    )
    broker.orders["successor"] = {
        "id": "successor", "symbol": PRIMARY, "side": "sell", "status": "filled",
        "filled_qty": "2", "filled_avg_price": "0.85",
    }
    broker.positions = [_broker_position(RUNNER, entry=0.25, current=0.50)]
    _run(tmp_path, broker, rules=rules)

    result = _run(tmp_path, broker, rules=rules)

    assert result["actions"] == []
    assert len(broker.submitted) == 1
    assert _state(tmp_path)["primary_realized_pnl"] == 0.0
    assert _state(tmp_path)["funding_ambiguous_predecessor_pnl"] == 15.0
    assert not _state(tmp_path)["funding_verified"]
    assert not _state(tmp_path)["runner_funded"]
    assert _state(tmp_path)["funding_exit_blocked"]
    assert "partial_funding_replacement_requires_reconciliation" in _state(tmp_path)["funding_reconciliation_error"]


def test_profitable_predecessor_cannot_fund_runner_with_ambiguous_successor_losses(tmp_path) -> None:
    broker = _funding_broker()
    broker.positions[0]["qty"] = "2"
    broker.orders["primary-order"]["filled_qty"] = "2"
    rules = PositionMonitorRules(exit_min_dte=-1, max_contracts_per_option=2)
    _run(tmp_path, broker, rules=rules)
    broker.orders["exit-broker-1"].update(
        status="replaced", filled_qty="1", filled_avg_price="1.00", replaced_by="successor",
    )
    broker.orders["successor"] = {
        "id": "successor", "symbol": PRIMARY, "side": "sell", "status": "filled",
        "filled_qty": "2", "filled_avg_price": "0.60",
    }
    broker.positions = [_broker_position(RUNNER, entry=0.25, current=0.34)]
    _run(tmp_path, broker, rules=rules)
    broker.positions = [_broker_position(RUNNER, entry=0.25, current=0.05)]

    result = _run(tmp_path, broker, rules=rules)

    assert _state(tmp_path)["funding_ambiguous_predecessor_pnl"] == 30.0
    assert _state(tmp_path)["primary_realized_pnl"] == 0.0
    assert not _state(tmp_path)["funding_verified"]
    assert not _state(tmp_path)["runner_funded"]
    assert _state(tmp_path)["funding_exit_blocked"]
    assert result["actions"][0]["reason"] == "unfunded_runner_stop_loss"
    assert not result["actions"][0]["runner_funded"]
    assert sum(intent.option_symbol == PRIMARY for intent in broker.submitted) == 1
