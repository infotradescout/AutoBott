from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Any

PHASE1_DECISION_CARD_SCHEMA_VERSION = "phase1_decision_card.v1"


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


class TradeSetup(str, Enum):
    BULLISH_CONTINUATION = "bullish_continuation"
    BEARISH_CONTINUATION = "bearish_continuation"
    LATE_CYCLE_BULLISH_REVERSAL = "late_cycle_bullish_reversal"
    LATE_CYCLE_BEARISH_REVERSAL = "late_cycle_bearish_reversal"
    NO_TRADE = "no_trade"


class ExecutionLayer(str, Enum):
    TACTICAL = "tactical"
    RIDER = "rider"
    BOTH = "both"
    NONE = "none"


class CycleStatus(str, Enum):
    UNKNOWN = "unknown"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class LegRole(str, Enum):
    TACTICAL = "tactical"
    RIDER = "rider"


class LifecycleStatus(str, Enum):
    REJECTED = "rejected"
    OPEN = "open"
    CLOSED = "closed"
    UNRESOLVED = "unresolved"


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
    volume_available: bool = True

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
class CycleProfile:
    median_valley_to_peak_bars: int | None = None
    median_peak_to_valley_bars: int | None = None
    bars_since_last_valley: int | None = None
    bars_since_last_peak: int | None = None
    expected_holding_days: int | None = None
    cycle_confidence: CycleStatus = CycleStatus.UNKNOWN
    last_pivot_type: str = "unknown"


@dataclass(frozen=True)
class DecisionInput:
    ticker: str
    timestamp: datetime
    market_bars: list[MarketBar]
    option_chain: list[OptionContractSnapshot]
    context: MarketContext
    iv_history: list[float] = field(default_factory=list)
    cycle_profile: CycleProfile = field(default_factory=CycleProfile)


@dataclass(frozen=True)
class Phase1Rules:
    min_bars: int = 30
    min_direction_score: float = 0.25
    min_volatility_score: float = -0.10
    min_confidence: float = 0.45
    min_dte: int = 7
    max_dte: int = 45
    intraday_min_dte: int = 1
    intraday_max_dte: int = 3
    rider_min_dte: int = 7
    rider_max_dte: int = 30
    max_strike_distance_pct: float = 0.08
    max_contract_mid: float = 1.00
    max_spread_pct: float = 0.18
    min_open_interest: int = 100
    min_contract_volume: int = 10
    min_abs_delta: float = 0.25
    max_abs_delta: float = 0.70
    intraday_min_abs_delta: float = 0.45
    intraday_max_abs_delta: float = 0.65
    min_vega: float = 0.01
    max_theta_abs: float = 0.20
    target_profit_pct: float = 0.50
    stop_loss_pct: float = 0.45
    min_reward_risk_ratio: float = 0.65
    reversal_min_move_pct: float = 0.012
    reversal_upper_range_position: float = 0.70
    reversal_lower_range_position: float = 0.30
    # Volatility instruments rise during risk-off by design. This exemption
    # only removes the contradictory regime veto; every direction, volatility,
    # liquidity, confidence, DTE, and contract-quality gate still applies.
    risk_off_bullish_exempt_symbols: tuple[str, ...] = ()


@dataclass(frozen=True)
class CycleAssessment:
    status: CycleStatus
    trend_score: int
    bars_since_last_valley: int | None
    bars_since_last_peak: int | None
    median_valley_to_peak_bars: int | None
    median_peak_to_valley_bars: int | None
    late_up_cycle: bool
    late_down_cycle: bool
    late_cycle: bool
    bearish_confirmation: bool
    bullish_confirmation: bool
    last_pivot_type: str
    reason: str
    explanation: str


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
    reward_risk_ratio: float
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
    contract_score: float
    reward_risk_ratio: float
    target_exit_mid: float
    stop_exit_mid: float
    exit_rule: str
    score_reasons: list[str]
    volume_available: bool = True

    @classmethod
    def from_score(cls, score: ContractScore, rules: Phase1Rules) -> "SelectedContract":
        contract = score.contract
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
            contract_score=score.score,
            reward_risk_ratio=score.reward_risk_ratio,
            target_exit_mid=round(contract.mid * (1 + rules.target_profit_pct), 4),
            stop_exit_mid=round(contract.mid * (1 - rules.stop_loss_pct), 4),
            exit_rule=f"take_profit_at_{int(rules.target_profit_pct * 100)}pct_gain_or_stop_at_{int(rules.stop_loss_pct * 100)}pct_loss_on_mid",
            score_reasons=score.reasons,
            volume_available=contract.volume_available,
        )


@dataclass(frozen=True)
class DecisionCard:
    schema_version: str
    decision_id: str
    ticker: str
    timestamp: datetime
    regime: RegimeResult
    direction: DirectionResult
    cycle: CycleAssessment
    volatility: VolatilityResult
    selected_contract: SelectedContract | None
    tactical_contract: SelectedContract | None
    rider_contract: SelectedContract | None
    trade_setup: TradeSetup
    execution_layer: ExecutionLayer
    decision: DecisionStatus
    blocked_reason: str | None
    reason_codes: list[str]
    confidence_score: float
    explanation: str
    forward_outcomes: ForwardOutcomes = field(default_factory=ForwardOutcomes)

    def to_json_dict(self) -> dict[str, Any]:
        return _json_safe(asdict(self))


@dataclass(frozen=True)
class Phase1LedgerEvent:
    schema_version: str
    decision_id: str
    parent_decision_id: str | None
    leg_role: LegRole | None
    ticker: str
    timestamp: datetime
    trade_setup: TradeSetup
    execution_layer: ExecutionLayer
    cycle_confidence: CycleStatus
    selected_contract: SelectedContract | None
    filled: bool
    lifecycle_status: LifecycleStatus
    entry_fill_model: str
    entry_underlying_price: float | None
    entry_option_bid: float | None
    entry_option_ask: float | None
    entry_option_mid: float | None
    entry_spread_pct: float | None
    entry_fill_price: float | None
    exit_option_bid: float | None
    exit_option_ask: float | None
    exit_option_mid: float | None
    exit_spread_pct: float | None
    exit_fill_model: str | None
    exit_fill_price: float | None
    exit_reason: str | None
    option_return_pct: float | None
    pnl: float | None
    max_favorable_excursion: float | None
    max_adverse_excursion: float | None
    hold_minutes: int | None
    contract_volume: int | None
    contract_open_interest: int | None
    quote_age_seconds: int | None
    underlying_price_at_exit: float | None

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
