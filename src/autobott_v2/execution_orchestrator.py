from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Callable

from .core_runner import CoreRunnerPair
from .execution_broker import AlpacaExecutionBroker
from .execution_config import AlpacaExecutionConfig
from .execution_journal import append_order_submission, append_risk_check
from .execution_models import (
    BrokerEnvironment,
    ExecutionOrder,
    ExecutionState,
    OrderSide,
    OrderType,
    TradeIntent,
    validate_trade_intent,
)
from .hosted_policy import HOSTED_POLICY_VERSION, active_build_sha, is_hosted_paper_runtime, is_volatility_symbol
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
    order_type: OrderType | None = None,
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
        order_type=order_type or _entry_order_type(environment),
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
            "strategy_version": "phase1-human-loop-v1",
            "policy_version": HOSTED_POLICY_VERSION if is_hosted_paper_runtime() else "local-default",
            "build_sha": active_build_sha(),
            "setup_event_id": _setup_event_id(decision),
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
    """Submit one primary plus one distinct runner as an atomic paper order."""

    resolved_broker = broker or AlpacaExecutionBroker(config)
    runtime_state = load_runtime_state()
    if runtime_state.kill_switch_enabled:
        raise ExecutionRejectedError("kill_switch_enabled")
    if not runtime_state.execution_enabled:
        raise ExecutionRejectedError("execution_disabled")
    if resolved_broker.config.environment is BrokerEnvironment.LIVE:
        raise ExecutionRejectedError("core_runner_live_not_validated")
    _validate_pair(pair)

    atomic_mleg_required = _core_runner_atomic_mleg_required()
    pair_order_type = OrderType.LIMIT if atomic_mleg_required else _entry_order_type(resolved_broker.config.environment)
    trade_group_id = f"core-runner:{decision.decision_id}"
    primary_intent = build_trade_intent_from_decision(
        decision,
        quantity=1,
        environment=resolved_broker.config.environment,
        max_position_cost=resolved_broker.config.effective_max_position_cost(),
        contract=pair.primary,
        leg_role="primary",
        trade_group_id=trade_group_id,
        paired_option_symbol=pair.runner.option_symbol,
        order_type=pair_order_type,
    )
    runner_intent = build_trade_intent_from_decision(
        decision,
        quantity=1,
        environment=resolved_broker.config.environment,
        max_position_cost=resolved_broker.config.effective_max_position_cost(),
        contract=pair.runner,
        leg_role="runner",
        trade_group_id=trade_group_id,
        paired_option_symbol=pair.primary.option_symbol,
        order_type=pair_order_type,
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
    _append_risk_check_safely(primary_intent, primary_risk, journal_path=journal_path)
    _append_risk_check_safely(runner_intent, runner_risk, journal_path=journal_path)
    rejected = tuple(primary_risk.reasons + runner_risk.reasons)
    if rejected:
        raise ExecutionRejectedError(rejected[0], detail=", ".join(rejected), reasons=rejected)

    if atomic_mleg_required and not hasattr(resolved_broker, "submit_mleg_order"):
        raise ExecutionRejectedError("core_runner_atomic_submission_unavailable")
    if not atomic_mleg_required and not hasattr(resolved_broker, "submit_order"):
        raise ExecutionRejectedError("core_runner_paired_submission_unavailable")
    if on_submission_attempt is not None:
        on_submission_attempt(primary_intent)

    if atomic_mleg_required:
        try:
            submitted_orders = resolved_broker.submit_mleg_order(
                (primary_intent, runner_intent),
                current_daily_realized_pnl=current_daily_realized_pnl,
                open_positions=open_positions,
            )
        except Exception as exc:
            raise ExecutionRejectedError(
                "core_runner_atomic_submission_failed",
                detail=str(exc),
                reasons=("core_runner_atomic_submission_failed",),
            ) from exc
        if len(submitted_orders) != 2:
            raise ExecutionRejectedError("core_runner_atomic_response_invalid")
    else:
        # Paper collection must not depend on Alpaca Level-3 MLEG approval.
        # Keep both contracts linked by trade_group_id while submitting them
        # as ordinary buy-to-open orders supported by the paper account.
        simple_orders: list[ExecutionOrder] = []
        try:
            for index, intent in enumerate((primary_intent, runner_intent)):
                simple_orders.append(
                    resolved_broker.submit_order(
                        intent,
                        current_daily_realized_pnl=current_daily_realized_pnl,
                        open_positions=open_positions + index,
                    )
                )
        except Exception as exc:
            compensation: dict[str, str | None] | None = None
            # A pair is the unit of risk. If the runner POST fails after the
            # primary was accepted, cancel the primary or immediately flatten
            # any filled quantity instead of leaving an accidental single leg.
            for order in simple_orders:
                _persist_submitted_order_safely(order, journal_path=journal_path)
                try:
                    compensation = _neutralize_partial_pair_order(
                        order,
                        broker=resolved_broker,
                        journal_path=journal_path,
                    )
                except Exception as compensation_exc:
                    compensation = {
                        "action": "compensation_failed",
                        "broker_order_id": order.broker_order_id,
                        "error": str(compensation_exc),
                    }
            reason = (
                "core_runner_paired_submission_partial_failure"
                if simple_orders
                else "core_runner_paired_submission_failed"
            )
            detail = str(exc)
            if compensation is not None:
                detail = f"{detail}; compensation={compensation}"
            raise ExecutionRejectedError(reason, detail=detail, reasons=(reason,)) from exc
        submitted_orders = tuple(simple_orders)

    primary_order, runner_order = submitted_orders
    for order in submitted_orders:
        _persist_submitted_order_safely(order, journal_path=journal_path)
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


def _core_runner_atomic_mleg_required() -> bool:
    if is_hosted_paper_runtime():
        return False
    value = os.getenv("AUTOBOTT_CORE_RUNNER_ATOMIC_MLEG_REQUIRED")
    if value is not None:
        return value.strip().lower() in {"1", "true", "yes", "on"}
    data_root = (os.getenv("AUTOBOTT_DATA_ROOT") or "").rstrip("/")
    return not data_root.startswith("/var/data/autobott")


def _option_type_value(value: object) -> str:
    enum_value = getattr(value, "value", value)
    return str(enum_value).lower()


def _setup_event_id(decision: DecisionCard) -> str:
    completed_bar_bucket = decision.timestamp.astimezone(UTC).replace(minute=0, second=0, microsecond=0)
    learning_symbol = "VOLATILITY" if is_volatility_symbol(decision.ticker) else decision.ticker.upper()
    return ":".join(
        (
            learning_symbol,
            completed_bar_bucket.isoformat(),
            decision.trade_setup.value,
            decision.direction.bias.value,
        )
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
        max_position_cost=resolved_broker.config.effective_max_position_cost(),
    )
    risk_check = validate_trade_intent(
        intent,
        resolved_broker.config.risk_controls(),
        current_daily_realized_pnl=current_daily_realized_pnl,
        open_positions=open_positions,
    )
    _append_risk_check_safely(intent, risk_check, journal_path=journal_path)
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
    _persist_submitted_order_safely(order, journal_path=journal_path)
    return order


def _entry_limit_price(
    contract: object,
    *,
    quantity: int,
    environment: BrokerEnvironment,
    max_position_cost: float | None,
) -> float:
    mid = float(getattr(contract, "mid"))
    ask = float(getattr(contract, "ask", mid) or mid)
    if environment is BrokerEnvironment.PAPER and is_hosted_paper_runtime():
        # Market orders still need a valuation for pre-trade debit controls.
        # Use the observed ask; retained limit-style/extra env values must not
        # manufacture a cost-cap rejection for the hosted paper service.
        return max(0.01, round(ask, 2))
    style = (os.getenv("AUTOBOTT_ENTRY_LIMIT_STYLE") or "marketable").strip().lower()
    if environment is not BrokerEnvironment.PAPER or style in {"mid", "passive"}:
        return round(mid, 2)
    limit_price = ask if style in {"marketable", "ask", "aggressive"} else mid
    if style in {"marketable", "aggressive"}:
        limit_price += _entry_limit_extra()
    # Never distort a live quote to squeeze it under an affordability limit.
    # Execution risk controls either approve the real notional or reject it;
    # paper mode can explicitly disable that risk limit altogether.
    return max(0.01, round(limit_price, 2))


def _entry_order_type(environment: BrokerEnvironment) -> OrderType:
    """Use fill-producing market orders for the fake-money collection lane.

    Alpaca's Basic options feed is indicative rather than the executable OPRA
    quote. A limit derived from that feed can sit unfilled even when the paper
    account and session are healthy. Market orders are supported for options
    during regular market hours and give the learning loop an actual fill.
    Live-money entries remain limit-only.
    """

    if environment is not BrokerEnvironment.PAPER:
        return OrderType.LIMIT
    if is_hosted_paper_runtime():
        return OrderType.MARKET
    configured = (os.getenv("AUTOBOTT_PAPER_ENTRY_ORDER_TYPE") or "market").strip().lower()
    return OrderType.LIMIT if configured == "limit" else OrderType.MARKET


def _neutralize_partial_pair_order(
    order: ExecutionOrder,
    *,
    broker: AlpacaExecutionBroker,
    journal_path: str | None,
) -> dict[str, str | None]:
    """Cancel an accepted orphan or market-close its filled quantity."""

    broker_order_id = order.broker_order_id
    cancel_error: str | None = None
    cancel_status = ""
    if broker_order_id and hasattr(broker, "cancel_order"):
        try:
            canceled = broker.cancel_order(broker_order_id)
            cancel_status = str((canceled or {}).get("status") or "").lower()
        except Exception as exc:
            cancel_error = str(exc)

    filled_quantity = order.intent.quantity if order.state is ExecutionState.FILLED else 0
    status = cancel_status
    if broker_order_id and hasattr(broker, "get_order"):
        try:
            payload = broker.get_order(broker_order_id)
            status = str(payload.get("status") or "").lower()
            filled_quantity = max(filled_quantity, int(float(payload.get("filled_qty") or 0)))
        except Exception:
            pass
    if filled_quantity <= 0:
        # Market paper orders normally fill before a second POST can fail. If
        # the broker cannot report status, inspect the live position before a
        # sell-to-close so we never create an invalid naked exit.
        if hasattr(broker, "list_open_positions"):
            try:
                for position in broker.list_open_positions():
                    if str(position.get("symbol") or "").upper() == order.intent.option_symbol.upper():
                        filled_quantity = int(float(position.get("qty") or 0))
                        break
            except Exception:
                pass
    if filled_quantity <= 0 and status in {"canceled", "cancelled", "rejected", "expired"}:
        return {"action": "canceled_primary", "broker_order_id": broker_order_id}
    if filled_quantity <= 0:
        return {
            "action": "cancel_requested",
            "broker_order_id": broker_order_id,
            "error": cancel_error,
        }

    exit_intent = TradeIntent(
        symbol=order.intent.symbol,
        option_symbol=order.intent.option_symbol,
        side=OrderSide.SELL_TO_CLOSE,
        quantity=filled_quantity,
        limit_price=max(0.01, order.intent.limit_price),
        generated_at=datetime.now(tz=order.intent.generated_at.tzinfo),
        environment=order.intent.environment,
        order_type=OrderType.MARKET,
        decision_id=order.intent.decision_id,
        thesis_id=order.intent.thesis_id,
        metadata={
            **order.intent.metadata,
            "compensating_exit": True,
            "exit_reason": "runner_submission_failed",
            "source_broker_order_id": broker_order_id,
        },
    )
    exit_order = broker.submit_order(exit_intent, current_daily_realized_pnl=0.0, open_positions=0)
    try:
        append_order_submission(exit_order, journal_path=journal_path)
    except Exception:
        pass
    return {
        "action": "market_close_primary",
        "broker_order_id": broker_order_id,
        "exit_broker_order_id": exit_order.broker_order_id,
    }


def _append_risk_check_safely(intent: TradeIntent, risk_check: object, *, journal_path: str | None) -> None:
    try:
        append_risk_check(intent, risk_check, journal_path=journal_path)
    except Exception:
        pass


def _persist_submitted_order_safely(order: ExecutionOrder, *, journal_path: str | None) -> None:
    try:
        append_order_submission(order, journal_path=journal_path)
    except Exception:
        pass
    try:
        upsert_open_position_from_order(order)
    except Exception:
        pass


def _entry_limit_extra() -> float:
    value = os.getenv("AUTOBOTT_ENTRY_LIMIT_EXTRA")
    if value is None or not value.strip():
        return 0.0
    return max(0.0, float(value))
