from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class DecisionReasonCode(str, Enum):
    APPROVED_PAPER_ORDER = "approved_paper_order"
    REJECT_MISSING_ACCOUNT_STATE = "reject_missing_account_state"
    REJECT_MISSING_MARKET_STATE = "reject_missing_market_state"
    REJECT_MISSING_TIMESTAMP = "reject_missing_timestamp"
    REJECT_MISSING_STRATEGY_IDENTITY = "reject_missing_strategy_identity"
    REJECT_INSUFFICIENT_BUYING_POWER = "reject_insufficient_buying_power"
    REJECT_POSITION_SIZE_TOO_LARGE = "reject_position_size_too_large"
    REJECT_MAX_LOSS_EXCEEDED = "reject_max_loss_exceeded"


@dataclass(frozen=True)
class MarketState:
    symbol: str
    timestamp: datetime | None
    last_price: float
    volatility_regime: str


@dataclass(frozen=True)
class TradingSignal:
    signal_id: str
    strategy_id: str
    symbol: str
    side: str
    confidence: float
    timestamp: datetime | None
    expected_entry_price: float


@dataclass(frozen=True)
class AccountState:
    equity: float
    buying_power: float
    realized_pnl: float


@dataclass(frozen=True)
class RiskRules:
    max_position_fraction: float
    max_units_per_trade: int
    max_daily_loss: float


@dataclass(frozen=True)
class PaperOrder:
    order_id: str
    symbol: str
    side: str
    units: int
    expected_fill_price: float
    notional: float
    created_at: datetime


@dataclass(frozen=True)
class PaperPosition:
    symbol: str
    side: str
    units: int
    avg_price: float
    opened_at: datetime


@dataclass(frozen=True)
class TradeDecision:
    decision_id: str
    accepted: bool
    reason_code: DecisionReasonCode
    reason_detail: str
    paper_order: PaperOrder | None
    replay_payload: dict[str, Any] = field(default_factory=dict)

    def to_json_dict(self) -> dict[str, Any]:
        order_dict = None
        if self.paper_order is not None:
            order_dict = {
                "order_id": self.paper_order.order_id,
                "symbol": self.paper_order.symbol,
                "side": self.paper_order.side,
                "units": self.paper_order.units,
                "expected_fill_price": self.paper_order.expected_fill_price,
                "notional": self.paper_order.notional,
                "created_at": self.paper_order.created_at.isoformat(),
            }
        return {
            "decision_id": self.decision_id,
            "accepted": self.accepted,
            "reason_code": self.reason_code.value,
            "reason_detail": self.reason_detail,
            "paper_order": order_dict,
            "replay_payload": self.replay_payload,
        }
