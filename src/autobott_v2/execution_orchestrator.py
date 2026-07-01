from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from .execution_broker import AlpacaExecutionBroker
from .execution_config import AlpacaExecutionConfig
from .execution_journal import append_order_submission, append_risk_check
from .execution_models import (
    BrokerEnvironment,
    ExecutionOrder,
    OrderSide,
    TradeIntent,
    validate_trade_intent,
)
from .position_store import upsert_open_position_from_order
from .phase1_models import DecisionCard, DecisionStatus, ExecutionLayer
from .runtime_control import load_runtime_state


@dataclass(frozen=True)
class OrderPlan:
    intent: TradeIntent
    quantity: int


def build_trade_intent_from_decision(
    decision: DecisionCard,
    *,
    quantity: int = 1,
    environment: BrokerEnvironment = BrokerEnvironment.PAPER,
) -> TradeIntent:
    if decision.decision is not DecisionStatus.TRADE_CANDIDATE:
        raise ValueError("decision_not_trade_candidate")
    if decision.selected_contract is None:
        raise ValueError("decision_missing_selected_contract")
    if decision.execution_layer is ExecutionLayer.NONE:
        raise ValueError("decision_missing_execution_layer")

    contract = decision.selected_contract
    side = OrderSide.BUY_TO_OPEN
    return TradeIntent(
        symbol=decision.ticker,
        option_symbol=contract.option_symbol,
        side=side,
        quantity=quantity,
        limit_price=contract.mid,
        generated_at=decision.timestamp,
        environment=environment,
        take_profit_price=contract.target_exit_mid,
        stop_loss_price=contract.stop_exit_mid,
        decision_id=decision.decision_id,
        thesis_id=f"{decision.ticker}:{decision.trade_setup.value}:{decision.execution_layer.value}",
        metadata={
            "trade_setup": decision.trade_setup.value,
            "execution_layer": decision.execution_layer.value,
            "confidence_score": decision.confidence_score,
            "reason_codes": list(decision.reason_codes),
        },
    )


def submit_decision_to_broker(
    decision: DecisionCard,
    *,
    broker: AlpacaExecutionBroker | None = None,
    config: AlpacaExecutionConfig | None = None,
    quantity: int = 1,
    current_daily_realized_pnl: float = 0.0,
    open_positions: int = 0,
    journal_path: str | None = None,
) -> ExecutionOrder:
    resolved_broker = broker or AlpacaExecutionBroker(config)
    runtime_state = load_runtime_state()
    if runtime_state.kill_switch_enabled:
        raise ValueError("kill_switch_enabled")
    if not runtime_state.execution_enabled:
        raise ValueError("execution_disabled")
    if resolved_broker.config.environment is BrokerEnvironment.LIVE and not runtime_state.live_mode_enabled:
        raise ValueError("live_mode_not_enabled")
    intent = build_trade_intent_from_decision(
        decision,
        quantity=quantity,
        environment=resolved_broker.config.environment,
    )
    risk_check = validate_trade_intent(
        intent,
        resolved_broker.config.risk_controls(),
        current_daily_realized_pnl=current_daily_realized_pnl,
        open_positions=open_positions,
    )
    append_risk_check(intent, risk_check, journal_path=journal_path)
    if not risk_check.approved:
        raise ValueError("risk_check_not_approved")
    order = resolved_broker.submit_order(
        intent,
        current_daily_realized_pnl=current_daily_realized_pnl,
        open_positions=open_positions,
    )
    append_order_submission(order, journal_path=journal_path)
    upsert_open_position_from_order(order)
    return order
