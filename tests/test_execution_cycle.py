from __future__ import annotations

from autobott_v2.execution_cycle import BrokerOrderState, ManagedOrder


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
