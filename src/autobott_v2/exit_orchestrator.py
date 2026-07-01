from __future__ import annotations

from datetime import UTC, datetime

from .execution_broker import AlpacaExecutionBroker
from .execution_journal import append_order_submission, append_risk_check
from .execution_models import BrokerEnvironment, ExecutionOrder, ExecutionState, OrderSide, RiskCheckResult, TradeIntent
from .position_store import OpenPosition, load_open_positions, save_open_positions
from .runtime_control import load_runtime_state


def build_exit_intent_from_position(
    position: OpenPosition,
    *,
    limit_price: float,
    environment: BrokerEnvironment = BrokerEnvironment.PAPER,
) -> TradeIntent:
    if limit_price <= 0:
        raise ValueError("limit_price_must_be_positive")
    return TradeIntent(
        symbol=position.symbol,
        option_symbol=position.option_symbol,
        side=OrderSide.SELL_TO_CLOSE,
        quantity=position.quantity,
        limit_price=limit_price,
        generated_at=datetime.now(tz=UTC),
        environment=environment,
        decision_id=position.decision_id,
        thesis_id=position.decision_id,
        metadata={"source_broker_order_id": position.broker_order_id, "exit": True},
    )


def submit_exit_for_position(
    position: OpenPosition,
    *,
    broker: AlpacaExecutionBroker,
    limit_price: float,
    current_daily_realized_pnl: float = 0.0,
    open_positions: int | None = None,
    journal_path: str | None = None,
    store_path: str | None = None,
) -> ExecutionOrder:
    runtime_state = load_runtime_state()
    if runtime_state.kill_switch_enabled:
        raise ValueError("kill_switch_enabled")
    if not runtime_state.execution_enabled:
        raise ValueError("execution_disabled")
    if broker.config.environment is BrokerEnvironment.LIVE and not runtime_state.live_mode_enabled:
        raise ValueError("live_mode_not_enabled")

    intent = build_exit_intent_from_position(position, limit_price=limit_price, environment=broker.config.environment)
    risk_check = RiskCheckResult(
        approved=True,
        reasons=(),
        estimated_notional=round(intent.quantity * intent.limit_price * 100, 2),
        normalized_limit_price=round(intent.limit_price, 2),
    )
    append_risk_check(intent, risk_check, journal_path=journal_path)
    order = broker.submit_order(
        intent,
        current_daily_realized_pnl=current_daily_realized_pnl,
        open_positions=max(0, (open_positions if open_positions is not None else 1) - 1),
    )
    append_order_submission(order, journal_path=journal_path)
    _mark_position_closing(position.broker_order_id, order, store_path=store_path)
    return order


def cancel_open_order(*, broker_order_id: str, broker: AlpacaExecutionBroker, journal_path: str | None = None) -> dict:
    payload = broker.cancel_order(broker_order_id)
    order = ExecutionOrder(
        order_id=payload.get("client_order_id") or broker_order_id,
        client_order_id=payload.get("client_order_id") or broker_order_id,
        intent=TradeIntent(
            symbol=payload.get("symbol", ""),
            option_symbol=payload.get("symbol", ""),
            side=OrderSide.BUY_TO_OPEN,
            quantity=int(payload.get("qty", 0) or 0),
            limit_price=float(payload.get("limit_price", 0) or 0),
            generated_at=datetime.now(tz=UTC),
            environment=broker.config.environment,
            thesis_id=broker_order_id,
        ),
        state=ExecutionState.CANCELED,
        submitted_at=datetime.now(tz=UTC),
        broker_order_id=broker_order_id,
    )
    append_order_submission(order, journal_path=journal_path)
    return payload


def replace_open_order(
    *,
    broker_order_id: str,
    broker: AlpacaExecutionBroker,
    limit_price: float,
    journal_path: str | None = None,
) -> dict:
    payload = broker.replace_order(broker_order_id, limit_price=limit_price)
    order = ExecutionOrder(
        order_id=payload.get("client_order_id") or broker_order_id,
        client_order_id=payload.get("client_order_id") or broker_order_id,
        intent=TradeIntent(
            symbol=payload.get("symbol", ""),
            option_symbol=payload.get("symbol", ""),
            side=OrderSide.BUY_TO_OPEN,
            quantity=int(payload.get("qty", 0) or 0),
            limit_price=float(payload.get("limit_price", limit_price) or limit_price),
            generated_at=datetime.now(tz=UTC),
            environment=broker.config.environment,
            thesis_id=broker_order_id,
        ),
        state=ExecutionState.SUBMITTED,
        submitted_at=datetime.now(tz=UTC),
        broker_order_id=broker_order_id,
    )
    append_order_submission(order, journal_path=journal_path)
    return payload


def _mark_position_closing(source_broker_order_id: str, exit_order: ExecutionOrder, *, store_path: str | None = None) -> None:
    positions = load_open_positions(store_path=store_path)
    updated = []
    for position in positions:
        if position.broker_order_id == source_broker_order_id:
            updated.append(
                OpenPosition(
                    broker_order_id=position.broker_order_id,
                    decision_id=position.decision_id,
                    symbol=position.symbol,
                    option_symbol=position.option_symbol,
                    quantity=position.quantity,
                    entry_limit_price=position.entry_limit_price,
                    entry_submitted_at=position.entry_submitted_at,
                    take_profit_price=position.take_profit_price,
                    stop_loss_price=position.stop_loss_price,
                    status=f"closing:{exit_order.broker_order_id or exit_order.order_id}",
                )
            )
        else:
            updated.append(position)
    save_open_positions(updated, store_path=store_path)
