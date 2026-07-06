from __future__ import annotations

from datetime import datetime, timezone

import pytest

from autobott_v2.execution_models import (
    BrokerEnvironment,
    ExecutionRiskControls,
    ExecutionState,
    OrderSide,
    TradeIntent,
    build_execution_order,
    validate_trade_intent,
)


def _controls(**overrides) -> ExecutionRiskControls:
    base = ExecutionRiskControls(
        max_position_cost=1_000.0,
        max_daily_loss=500.0,
        max_open_positions=3,
        allow_live_trading=False,
        allow_order_placement=True,
        allowed_environments=(BrokerEnvironment.PAPER,),
    )
    values = base.__dict__ | overrides
    return ExecutionRiskControls(**values)


def _intent(**overrides) -> TradeIntent:
    base = TradeIntent(
        symbol="AAPL",
        option_symbol="AAPL260117C00190000",
        side=OrderSide.BUY_TO_OPEN,
        quantity=1,
        limit_price=2.5,
        generated_at=datetime(2026, 7, 1, 15, 30, tzinfo=timezone.utc),
        decision_id="decision-123",
        take_profit_price=3.75,
        stop_loss_price=1.75,
    )
    values = base.__dict__ | overrides
    return TradeIntent(**values)


def test_validate_trade_intent_approves_valid_paper_trade() -> None:
    result = validate_trade_intent(_intent(), _controls())
    assert result.approved is True
    assert result.reasons == ()
    assert result.estimated_notional == 250.0


def test_validate_trade_intent_blocks_live_when_disabled() -> None:
    result = validate_trade_intent(
        _intent(environment=BrokerEnvironment.LIVE),
        _controls(),
    )
    assert result.approved is False
    assert "environment_not_allowed" in result.reasons
    assert "live_trading_disabled" in result.reasons


def test_validate_trade_intent_blocks_when_order_placement_disabled() -> None:
    result = validate_trade_intent(
        _intent(),
        _controls(allow_order_placement=False),
    )
    assert result.approved is False
    assert result.reasons == ("order_placement_disabled",)


def test_validate_trade_intent_enforces_position_cost_limit() -> None:
    result = validate_trade_intent(
        _intent(quantity=10, limit_price=2.5),
        _controls(max_position_cost=2_000.0),
    )
    assert result.approved is False
    assert "position_cost_exceeds_limit" in result.reasons


def test_validate_trade_intent_allows_large_sell_to_close_exit() -> None:
    result = validate_trade_intent(
        _intent(side=OrderSide.SELL_TO_CLOSE, quantity=10, limit_price=2.5),
        _controls(max_position_cost=2_000.0),
    )

    assert result.approved is True
    assert "position_cost_exceeds_limit" not in result.reasons


def test_validate_trade_intent_enforces_daily_loss_lock() -> None:
    result = validate_trade_intent(
        _intent(),
        _controls(),
        current_daily_realized_pnl=-500.0,
    )
    assert result.approved is False
    assert result.reasons == ("daily_loss_limit_reached",)


def test_validate_trade_intent_enforces_exit_price_relationships() -> None:
    result = validate_trade_intent(
        _intent(take_profit_price=2.0, stop_loss_price=3.0),
        _controls(),
    )
    assert result.approved is False
    assert "take_profit_must_exceed_entry" in result.reasons
    assert "stop_loss_must_be_below_entry" in result.reasons


def test_build_execution_order_requires_approved_risk_check() -> None:
    intent = _intent()
    risk_check = validate_trade_intent(intent, _controls())

    order = build_execution_order(intent, risk_check)

    assert order.intent == intent
    assert order.state is ExecutionState.APPROVED
    assert order.client_order_id.startswith("autobott-")


def test_build_execution_order_rejects_unapproved_risk_check() -> None:
    intent = _intent()
    risk_check = validate_trade_intent(intent, _controls(allow_order_placement=False))

    with pytest.raises(ValueError, match="risk_check_not_approved"):
        build_execution_order(intent, risk_check)


def test_build_execution_order_requires_decision_or_thesis_id() -> None:
    intent = _intent(decision_id=None, thesis_id=None)
    risk_check = validate_trade_intent(intent, _controls())

    with pytest.raises(ValueError, match="decision_or_thesis_id_required"):
        build_execution_order(intent, risk_check)
