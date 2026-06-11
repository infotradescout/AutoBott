from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Any


class RegimeLabel(str, Enum):
    TREND = "trend"
    RANGE = "range"
    VOLATILITY_EXPANSION = "volatility_expansion"
    VOLATILITY_COMPRESSION = "volatility_compression"
    RISK_ON = "risk_on"
    RISK_OFF = "risk_off"


class DirectionBias(str, Enum):
    BULLISH = "bullish"
    BEARISH = "bearish"
    NEUTRAL = "neutral"


class OptionType(str, Enum):
    CALL = "call"
    PUT = "put"


class DecisionStatus(str, Enum):
    TRADE_CANDIDATE = "TRADE_CANDIDATE"
    NO_TRADE = "NO_TRADE"
    BLOCKED_BY_REGIME = "BLOCKED_BY_REGIME"
    BLOCKED_BY_VOLATILITY = "BLOCKED_BY_VOLATILITY"
    BLOCKED_BY_SPREAD = "BLOCKED_BY_SPREAD"
    BLOCKED_BY_RISK = "BLOCKED_BY_RISK"


@dataclass(frozen=True)
class ForwardOutcomes:
    after_5m: dict[str, Any] | None = None
    after_15m: dict[str, Any] | None = None
    after_30m: dict[str, Any] | None = None
    after_1h: dict[str, Any] | None = None


@dataclass(frozen=True)
class MarketBar:
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int


@dataclass(frozen=True)
class OptionContractSnapshot:
    option_symbol: str
    underlying: str
    expiration: date
    strike: float
    option_type: OptionType
    bid: float
    ask: float
    last: float | None
    volume: int
    open_interest: int
    delta: float
    theta: float
    vega: float
    implied_volatility: float

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2

    @property
    def spread_pct(self) -> float:
        return (self.ask - self.bid) / self.mid if self.mid > 0 else 1.0


@dataclass(frozen=True)
class MarketContext:
    spy_bars: list[MarketBar] = field(default_factory=list)
    qqq_bars: list[MarketBar] = field(default_factory=list)
    vix_bars: list[MarketBar] = field(default_factory=list)
    blackout_event: bool = False
    event_labels: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class DecisionInput:
    ticker: str
    timestamp: datetime
    market_bars: list[MarketBar]
    option_chain: list[OptionContractSnapshot]
    context: MarketContext
    iv_history: list[float] = field(default_factory=list)


@dataclass(frozen=True)
class Phase1Rules:
    min_bars: int = 30
    min_direction_score: float = 0.25
    min_volatility_score: float = -0.10
    min_confidence: float = 0.45
    min_dte: int = 7
    max_dte: int = 45
    max_strike_distance_pct: float = 0.08
    max_spread_pct: float = 0.18
    min_open_interest: int = 100
    min_contract_volume: int = 10
    min_abs_delta: float = 0.25
    max_abs_delta: float = 0.70
    min_vega: float = 0.01
    max_theta_abs: float = 0.20


@dataclass(frozen=True)
class RegimeResult:
    primary: RegimeLabel
    labels: list[RegimeLabel]
    score: float
    explanation: str


@dataclass(frozen=True)
class DirectionResult:
    bias: DirectionBias
    score: float
    momentum: float
    relative_strength: float
    volume_confirmation: float
    failed_breakout: bool
    explanation: str


@dataclass(frozen=True)
class VolatilityResult:
    score: float
    iv_percentile: float | None
    iv_realized_ratio: float | None
    iv_crush_risk: bool
    event_risk: bool
    explanation: str


@dataclass(frozen=True)
class ContractScore:
    contract: OptionContractSnapshot
    score: float
    reasons: list[str]


@dataclass(frozen=True)
class SelectedContract:
    option_symbol: str
    option_type: OptionType
    expiration: date
    strike: float
    bid: float
    ask: float
    mid: float
    spread_pct: float
    open_interest: int
    volume: int
    delta: float
    theta: float
    vega: float
    implied_volatility: float

    @classmethod
    def from_contract(cls, contract: OptionContractSnapshot) -> "SelectedContract":
        return cls(
            option_symbol=contract.option_symbol,
            option_type=contract.option_type,
            expiration=contract.expiration,
            strike=contract.strike,
            bid=contract.bid,
            ask=contract.ask,
            mid=contract.mid,
            spread_pct=contract.spread_pct,
            open_interest=contract.open_interest,
            volume=contract.volume,
            delta=contract.delta,
            theta=contract.theta,
            vega=contract.vega,
            implied_volatility=contract.implied_volatility,
        )


@dataclass(frozen=True)
class DecisionCard:
    ticker: str
    timestamp: datetime
    regime: RegimeResult
    direction: DirectionResult
    volatility: VolatilityResult
    selected_contract: SelectedContract | None
    decision: DecisionStatus
    blocked_reason: str | None
    confidence_score: float
    explanation: str
    forward_outcomes: ForwardOutcomes = field(default_factory=ForwardOutcomes)

    def to_json_dict(self) -> dict[str, Any]:
        return _json_safe(asdict(self))


def _json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    return value
