from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from typing import Callable

from .core_runner import CoreRunnerPair
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
from .phase1_models import DecisionCard, DecisionStatus, ExecutionLayer, SelectedContract
from .runtime_control import load_runtime_state


@dataclass(frozen=True)
class OrderPlan:
    intent: TradeIntent
    quantity: int


class ExecutionRejectedError(ValueError):
    def __init__(self, reason: str, *, detail: str | None = None, reasons: tuple[str, ...] = ()) -> None:
        self.reason = reason
        self.detail = detail or reason
        self.reasons = reasons or (reason,)
        super().__init__(self.detail)


def build_trade_intent_from_decision(
    decision: DecisionCard,
    *,
    quantity: int = 1,
    environment: BrokerEnvironment = BrokerEnvironment.PAPER,
    max_position_cost: float | None = None,
    contract: SelectedContract | None = None,
    leg_role: str = "primary",
    trade_group_id: str | None = None,
    paired_option_symbol: str | None = None,
) -> TradeIntent:
    if decision.decision is not DecisionStatus.TRADE_CANDIDATE:
        raise ValueError("decision_not_trade_candidate")
    if decision.selected_contract is None and contract is None:
        raise ValueError("decision_missing_selected_contract")
    if decision.execution_layer is ExecutionLayer.NONE:
        raise ValueError("decision_missing_execution_layer")

    selected_contract = contract or decision.selected_contract
    if selected_contract is None:
        raise ValueError("decision_missing_selected_contract")
    side = OrderSide.BUY_TO_OPEN
    return TradeIntent(
        symbol=decision.ticker,
        option_symbol=selected_contract.option_symbol,
        side=side,
        quantity=quantity,
        limit_price=_entry_limit_price(
            selected_contract,
            quantity=quantity,
            environment=environment,
            max_position_cost=max_position_cost,
        ),
        generated_at=decision.timestamp,
        environment=environment,
        take_profit_price=selected_contract.target_exit_mid,
        stop_loss_price=selected_contract.stop_exit_mid,
        decision_id=decision.decision_id,
        thesis_id=f"{decision.ticker}:{decision.trade_setup.value}:{decision.execution_layer.value}",
        metadata={
            "trade_setup": decision.trade_setup.value,
            "execution_layer": decision.execution_layer.value,
            "confidence_score": decision.confidence_score,
            "reason_codes": list(decision.reason_codes),
            "trade_group_id": trade_group_id,
            "leg_role": leg_role,
            "paired_option_symbol": paired_option_symbol,
        },
    )


def submit_core_runner_to_broker(
    decision: DecisionCard,
    pair: CoreRunnerPair,
    *,
    broker: AlpacaExecutionBroker | None = None,
    config: AlpacaExecutionConfig | None = None,
    current_daily_realized_pnl: float = 0.0,
    open_positions: int = 0,
    journal_path: str | None = None,
    on_submission_attempt: Callable[[TradeIntent], None] | None = None,
) -> tuple[ExecutionOrder, ExecutionOrder]:
    """Submit one primary plus one distinct runner within one debit budget."""

    resolved_broker = broker or AlpacaExecutionBroker(config)
    runtime_state = load_runtime_state()
    if runtime_state.kill_switch_enabled:
        raise ExecutionRejectedError("kill_switch_enabled")
    if not runtime_state.execution_enabled:
        raise ExecutionRejectedError("execution_disabled")
    if resolved_broker.config.environment is BrokerEnvironment.LIVE:
        raise ExecutionRejectedError("core_runner_live_not_validated")
    _validate_pair(pair)

    trade_group_id = f"core-runner:{decision.decision_id}"
    primary_intent = build_trade_intent_from_decision(
        decision,
        quantity=1,
        environment=resolved_broker.config.environment,
        max_position_cost=resolved_broker.config.max_position_cost,
        contract=pair.primary,
        leg_role="primary",
        trade_group_id=trade_group_id,
        paired_option_symbol=pair.runner.option_symbol,
    )
    runner_intent = build_trade_intent_from_decision(
        decision,
        quantity=1,
        environment=resolved_broker.config.environment,
        max_position_cost=resolved_broker.config.max_position_cost,
        contract=pair.runner,
        leg_role="runner",
        trade_group_id=trade_group_id,
        paired_option_symbol=pair.primary.option_symbol,
    )
    actual_group_notional = round(primary_intent.estimated_notional + runner_intent.estimated_notional, 2)
    if actual_group_notional > pair.max_group_cost:
        raise ExecutionRejectedError(
            "core_runner_group_cost_exceeds_budget",
            detail=(
                "core_runner_group_cost_exceeds_budget: "
                f"actual_group_notional={actual_group_notional} budget={pair.max_group_cost}"
            ),
        )

    controls = resolved_broker.config.risk_controls()
    primary_risk = validate_trade_intent(
        primary_intent,
        controls,
        current_daily_realized_pnl=current_daily_realized_pnl,
        open_positions=open_positions,
    )
    runner_risk = validate_trade_intent(
        runner_intent,
        controls,
        current_daily_realized_pnl=current_daily_realized_pnl,
        open_positions=open_positions + 1,
    )
    append_risk_check(primary_intent, primary_risk, journal_path=journal_path)
    append_risk_check(runner_intent, runner_risk, journal_path=journal_path)
    rejected = tuple(primary_risk.reasons + runner_risk.reasons)
    if rejected:
        raise ExecutionRejectedError(rejected[0], detail=", ".join(rejected), reasons=rejected)

    if on_submission_attempt is not None:
        on_submission_attempt(primary_intent)
    primary_order = resolved_broker.submit_order(
        primary_intent,
        current_daily_realized_pnl=current_daily_realized_pnl,
        open_positions=open_positions,
    )
    append_order_submission(primary_order, journal_path=journal_path)
    upsert_open_position_from_order(primary_order)
    try:
        runner_order = resolved_broker.submit_order(
            runner_intent,
            current_daily_realized_pnl=current_daily_realized_pnl,
            open_positions=open_positions + 1,
        )
    except Exception as exc:
        if primary_order.broker_order_id and hasattr(resolved_broker, "cancel_order"):
            try:
                resolved_broker.cancel_order(primary_order.broker_order_id)
            except Exception:
                pass
        raise ExecutionRejectedError(
            "runner_submission_failed",
            detail=str(exc),
            reasons=("runner_submission_failed",),
        ) from exc
    append_order_submission(runner_order, journal_path=journal_path)
    upsert_open_position_from_order(runner_order)
    return primary_order, runner_order


def _validate_pair(pair: CoreRunnerPair) -> None:
    primary = pair.primary
    runner = pair.runner
    if primary.option_symbol == runner.option_symbol:
        raise ExecutionRejectedError("runner_must_use_distinct_contract")
    if _option_type_value(primary.option_type) != _option_type_value(runner.option_type):
        raise ExecutionRejectedError("runner_must_match_primary_direction")
    if primary.expiration != runner.expiration:
        raise ExecutionRejectedError("runner_must_match_primary_expiration")
    if runner.ask >= primary.ask or runner.mid >= primary.mid:
        raise ExecutionRejectedError("runner_must_be_cheaper_than_primary")
    if _option_type_value(primary.option_type) == "call" and runner.strike <= primary.strike:
        raise ExecutionRejectedError("call_runner_must_use_higher_strike")
    if _option_type_value(primary.option_type) == "put" and runner.strike >= primary.strike:
        raise ExecutionRejectedError("put_runner_must_use_lower_strike")


def _option_type_value(value: object) -> str:
    enum_value = getattr(value, "value", value)
    return str(enum_value).lower()


def submit_decision_to_broker(
    decision: DecisionCard,
    *,
    broker: AlpacaExecutionBroker | None = None,
    config: AlpacaExecutionConfig | None = None,
    quantity: int = 1,
    current_daily_realized_pnl: float = 0.0,
    open_positions: int = 0,
    journal_path: str | None = None,
    on_submission_attempt: Callable[[TradeIntent], None] | None = None,
) -> ExecutionOrder:
    resolved_broker = broker or AlpacaExecutionBroker(config)
    runtime_state = load_runtime_state()
    if runtime_state.kill_switch_enabled:
        raise ExecutionRejectedError("kill_switch_enabled")
    if not runtime_state.execution_enabled:
        raise ExecutionRejectedError("execution_disabled")
    if resolved_broker.config.environment is BrokerEnvironment.LIVE and not runtime_state.live_mode_enabled:
        raise ExecutionRejectedError("live_mode_not_enabled")
    intent = build_trade_intent_from_decision(
        decision,
        quantity=quantity,
        environment=resolved_broker.config.environment,
        max_position_cost=resolved_broker.config.max_position_cost,
    )
    risk_check = validate_trade_intent(
        intent,
        resolved_broker.config.risk_controls(),
        current_daily_realized_pnl=current_daily_realized_pnl,
        open_positions=open_positions,
    )
    append_risk_check(intent, risk_check, journal_path=journal_path)
    if not risk_check.approved:
        raise ExecutionRejectedError(
            risk_check.reasons[0] if risk_check.reasons else "risk_check_not_approved",
            detail=", ".join(risk_check.reasons) if risk_check.reasons else "risk_check_not_approved",
            reasons=risk_check.reasons,
        )
    if on_submission_attempt is not None:
        on_submission_attempt(intent)
    order = resolved_broker.submit_order(
        intent,
        current_daily_realized_pnl=current_daily_realized_pnl,
        open_positions=open_positions,
    )
    append_order_submission(order, journal_path=journal_path)
    upsert_open_position_from_order(order)
    return order


def _entry_limit_price(
    contract: object,
    *,
    quantity: int,
    environment: BrokerEnvironment,
    max_position_cost: float | None,
) -> float:
    style = (os.getenv("AUTOBOTT_ENTRY_LIMIT_STYLE") or "marketable").strip().lower()
    mid = float(getattr(contract, "mid"))
    ask = float(getattr(contract, "ask", mid) or mid)
    if environment is not BrokerEnvironment.PAPER or style in {"mid", "passive"}:
        return round(mid, 2)
    limit_price = ask if style in {"marketable", "ask", "aggressive"} else mid
    if style in {"marketable", "aggressive"}:
        limit_price += _entry_limit_extra()
    if max_position_cost is not None and max_position_cost > 0 and quantity > 0:
        limit_price = min(limit_price, max_position_cost / (quantity * 100.0))
    return max(0.01, round(limit_price, 2))


def _entry_limit_extra() -> float:
    value = os.getenv("AUTOBOTT_ENTRY_LIMIT_EXTRA")
    if value is None or not value.strip():
        return 0.0
    return max(0.0, float(value))
