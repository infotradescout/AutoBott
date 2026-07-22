from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from autobott_v2.execution_cycle import BrokerOrderState, CycleLifecycleState, ExecutionCycle, ManagedOrder


def test_canceled_exit_is_never_counted_as_proceeds() -> None:
    order = ManagedOrder("order-1", "call", "exit", submitted_limit=4.0, requested_quantity=2)
    order.apply_broker_update(state=BrokerOrderState.CANCELED, filled_quantity=0)
    assert order.confirmed_proceeds == 0.0


def test_working_exit_is_never_counted_as_proceeds() -> None:
    order = ManagedOrder("order-1", "call", "exit", submitted_limit=4.0, requested_quantity=2)
    order.apply_broker_update(state=BrokerOrderState.WORKING, filled_quantity=0)
    assert order.confirmed_proceeds == 0.0


def test_only_broker_confirmed_fill_counts_as_proceeds() -> None:
    order = ManagedOrder("order-1", "call", "exit", submitted_limit=4.0, requested_quantity=2)
    order.apply_broker_update(state=BrokerOrderState.FILLED, filled_quantity=2, fill_price=3.95)
    assert order.confirmed_proceeds == 790.0


def test_partial_fill_requires_positive_quantity() -> None:
    order = ManagedOrder("order-1", "call", "entry", requested_quantity=2)
    try:
        order.apply_broker_update(state=BrokerOrderState.PARTIALLY_FILLED, filled_quantity=0)
    except ValueError as exc:
        assert str(exc) == "partial_fill_requires_positive_quantity"
    else:
        raise AssertionError("partial fill without quantity must fail")


def test_partial_fill_proceeds_survive_cancel() -> None:
    order = ManagedOrder("order-1", "call", "exit", requested_quantity=2)
    order.apply_broker_update(state=BrokerOrderState.PARTIALLY_FILLED, filled_quantity=1, fill_price=3.0)
    order.apply_broker_update(state=BrokerOrderState.CANCELED, filled_quantity=1)
    assert order.confirmed_quantity == 1
    assert order.confirmed_proceeds == 300.0


def test_partial_fill_proceeds_survive_replacement() -> None:
    original = ManagedOrder("order-1", "call", "exit", requested_quantity=2, replaced_by_order_id="order-2")
    replacement = ManagedOrder("order-2", "call", "exit", requested_quantity=1)
    original.apply_broker_update(state=BrokerOrderState.REPLACED, filled_quantity=1, fill_price=3.0)
    replacement.apply_broker_update(state=BrokerOrderState.FILLED, filled_quantity=1, fill_price=2.5)
    assert original.confirmed_proceeds + replacement.confirmed_proceeds == 550.0


def test_cycle_lifecycle_rejects_skipped_transitions() -> None:
    now = datetime.now(UTC)
    cycle = ExecutionCycle("test", "paper", now, now, now + timedelta(minutes=1), 1000, now + timedelta(days=1), {})
    with pytest.raises(ValueError, match="invalid_cycle_transition:DRAFT->ACTIVE"):
        cycle.transition(CycleLifecycleState.ACTIVE, actor="test", reason="skip")
    cycle.transition(CycleLifecycleState.PREFLIGHT_REQUIRED, actor="test", reason="begin")
    cycle.transition(CycleLifecycleState.PREFLIGHT_VALIDATED, actor="test", reason="passed")
    assert cycle.lifecycle_state is CycleLifecycleState.PREFLIGHT_VALIDATED
    assert len(cycle.audit_events) == 2
