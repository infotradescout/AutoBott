from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any
from uuid import uuid4


class BrokerEnvironment(str, Enum):
    PAPER = "paper"
    LIVE = "live"


class OrderSide(str, Enum):
    BUY_TO_OPEN = "buy_to_open"
    SELL_TO_CLOSE = "sell_to_close"


class OrderType(str, Enum):
    LIMIT = "limit"
    MARKET = "market"


class ExecutionState(str, Enum):
    DRAFT = "draft"
    APPROVED = "approved"
    SUBMITTED = "submitted"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELED = "canceled"
    REJECTED = "rejected"
    FAILED = "failed"


@dataclass(frozen=True)
class ExecutionRiskControls:
    max_position_cost: float | None
    max_daily_loss: float
    max_open_positions: int
    allow_live_trading: bool = False
    allow_order_placement: bool = False
    allowed_environments: tuple[BrokerEnvironment, ...] = (BrokerEnvironment.PAPER,)


@dataclass(frozen=True)
class TradeIntent:
    symbol: str
    option_symbol: str
    side: OrderSide
    quantity: int
    limit_price: float
    generated_at: datetime
    environment: BrokerEnvironment = BrokerEnvironment.PAPER
    order_type: OrderType = OrderType.LIMIT
    take_profit_price: float | None = None
    stop_loss_price: float | None = None
    decision_id: str | None = None
    thesis_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def estimated_notional(self) -> float:
        return round(self.quantity * self.limit_price * 100, 2)


@dataclass(frozen=True)
class RiskCheckResult:
    approved: bool
    reasons: tuple[str, ...]
    estimated_notional: float
    normalized_limit_price: float


@dataclass(frozen=True)
class ExecutionOrder:
    order_id: str
    client_order_id: str
    intent: TradeIntent
    state: ExecutionState
    submitted_at: datetime | None = None
    broker_order_id: str | None = None


def validate_trade_intent(
    intent: TradeIntent,
    controls: ExecutionRiskControls,
    *,
    current_daily_realized_pnl: float = 0.0,
    open_positions: int = 0,
) -> RiskCheckResult:
    reasons: list[str] = []
    normalized_limit_price = round(intent.limit_price, 2)
    estimated_notional = round(intent.quantity * normalized_limit_price * 100, 2)

    if not controls.allow_order_placement:
        reasons.append("order_placement_disabled")
    if intent.environment not in controls.allowed_environments:
        reasons.append("environment_not_allowed")
    if intent.environment is BrokerEnvironment.LIVE and not controls.allow_live_trading:
        reasons.append("live_trading_disabled")
    if not intent.symbol.strip():
        reasons.append("symbol_required")
    if not intent.option_symbol.strip():
        reasons.append("option_symbol_required")
    if intent.quantity <= 0:
        reasons.append("quantity_must_be_positive")
    if normalized_limit_price <= 0:
        reasons.append("limit_price_must_be_positive")
    if (
        intent.side is OrderSide.BUY_TO_OPEN
        and controls.max_position_cost is not None
        and estimated_notional > controls.max_position_cost
    ):
        reasons.append("position_cost_exceeds_limit")
    if open_positions >= controls.max_open_positions:
        reasons.append("max_open_positions_reached")
    if current_daily_realized_pnl <= -abs(controls.max_daily_loss):
        reasons.append("daily_loss_limit_reached")
    if intent.side is OrderSide.BUY_TO_OPEN:
        if intent.take_profit_price is not None and intent.take_profit_price <= normalized_limit_price:
            reasons.append("take_profit_must_exceed_entry")
        if intent.stop_loss_price is not None and intent.stop_loss_price >= normalized_limit_price:
            reasons.append("stop_loss_must_be_below_entry")

    return RiskCheckResult(
        approved=not reasons,
        reasons=tuple(reasons),
        estimated_notional=estimated_notional,
        normalized_limit_price=normalized_limit_price,
    )


def build_execution_order(intent: TradeIntent, risk_check: RiskCheckResult) -> ExecutionOrder:
    if not risk_check.approved:
        raise ValueError("risk_check_not_approved")
    if not intent.decision_id and not intent.thesis_id:
        raise ValueError("decision_or_thesis_id_required")
    order_id = str(uuid4())
    return ExecutionOrder(
        order_id=order_id,
        client_order_id=f"autobott-{order_id}",
        intent=intent,
        state=ExecutionState.APPROVED,
    )
