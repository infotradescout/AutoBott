from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime, time, timedelta, timezone, tzinfo
from enum import Enum
from pathlib import Path
from typing import Any, Protocol
from uuid import uuid4

from .execution_cycle import CycleLifecycleState, ExecutionCycle
from .phase1_snapshot_capture import _market_timezone_info
from .runtime_paths import data_root
from .strategy_registry import StrategyDefinition, register_strategy


VIX_STRATEGY_ID = "vix_paired_options"


def _eastern_timezone(reference: date) -> tzinfo:
    return _market_timezone_info("America/New_York", reference)


def _central_timezone(reference: date) -> tzinfo:
    eastern = _eastern_timezone(reference)
    offset = eastern.utcoffset(datetime.combine(reference, time(12, 0))) or timedelta(hours=-5)
    return timezone(offset - timedelta(hours=1), name="America/Chicago")


class VixProduct(str, Enum):
    VIX = "VIX"
    VIXW = "VIXW"


class SettlementType(str, Enum):
    AM = "AM"
    PM = "PM"


class TradingSession(str, Enum):
    REGULAR = "REGULAR"
    GLOBAL = "GLOBAL"
    CURB = "CURB"
    CLOSED = "CLOSED"


class VixBrokerAdapter(Protocol):
    """Capability boundary for a future broker with actual VIX/VIXW support."""

    def get_account(self) -> dict[str, Any]: ...
    def get_option_chain(self, product: VixProduct, expiration: date) -> list[dict[str, Any]]: ...
    def get_session_status(self, at: datetime) -> dict[str, Any]: ...
    def get_quote(self, option_symbol: str) -> dict[str, Any]: ...
    def preview_order(self, order: dict[str, Any]) -> dict[str, Any]: ...
    def submit_order(self, order: dict[str, Any]) -> dict[str, Any]: ...
    def cancel_order(self, broker_order_id: str) -> dict[str, Any]: ...
    def replace_order(self, broker_order_id: str, order: dict[str, Any]) -> dict[str, Any]: ...
    def get_order(self, broker_order_id: str) -> dict[str, Any]: ...
    def get_fills(self, broker_order_id: str) -> list[dict[str, Any]]: ...
    def get_positions(self) -> list[dict[str, Any]]: ...


@dataclass(frozen=True)
class VixStrategyConfig:
    enabled: bool = True
    preferred_entry_min: float = 17.0
    preferred_entry_max: float = 17.99
    enabled_entry_min: float = 16.0
    enabled_entry_max: float = 19.99
    minimum_full_trading_sessions_remaining: int | None = None
    maximum_days_to_expiration: int | None = None
    regular_hours_only: bool = True
    accepted_products: tuple[VixProduct, ...] = (VixProduct.VIX, VixProduct.VIXW)
    strike_selection_method: str = "explicit_reviewed_strikes"
    maximum_combined_debit: float | None = None
    maximum_cycle_allocation: float | None = None
    first_leg_exit_target_pct: float | None = None
    second_leg_management_rule: str | None = None
    maximum_additions: int | None = None
    maximum_additional_capital: float | None = None
    addition_sizing: int | None = None
    addition_trigger: str | None = None
    require_first_leg_exit_before_addition: bool = True
    mandatory_exit_buffer_minutes: int = 60
    settlement_trading_authorized: bool = False

    def to_json_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["accepted_products"] = [item.value for item in self.accepted_products]
        return payload


@dataclass(frozen=True)
class VixPreflightRequest:
    spot_vix: float
    product: VixProduct
    call_product: VixProduct
    put_product: VixProduct
    call_expiration: date
    put_expiration: date
    settlement_type: SettlementType
    intended_session: TradingSession
    actual_timestamp: datetime
    call_strike: float
    put_strike: float
    call_quantity: int
    put_quantity: int
    call_debit: float
    put_debit: float
    account_id: str = "paper"
    existing_cycle_ids: tuple[str, ...] = ()
    overlapping_expirations: tuple[date, ...] = ()
    client_request_id: str | None = None
    prior_client_request_ids: tuple[str, ...] = ()
    requested_override_codes: tuple[str, ...] = ()
    override_actor: str | None = None
    expected_settlement_type: SettlementType = SettlementType.AM

    @property
    def expiration(self) -> date:
        return self.call_expiration

    @property
    def combined_debit(self) -> float:
        return round(self.call_debit + self.put_debit, 4)

    @property
    def maximum_cycle_loss(self) -> float:
        return round((self.call_debit * self.call_quantity + self.put_debit * self.put_quantity) * 100, 2)


@dataclass(frozen=True)
class PreflightIssue:
    code: str
    message: str
    category: str
    overridable: bool = False


@dataclass
class VixPreflightResult:
    request: VixPreflightRequest
    issues: list[PreflightIssue]
    warnings: list[PreflightIssue]
    actual_session: TradingSession
    calendar_days_to_expiration: int
    full_regular_sessions_remaining: int
    final_tradable_timestamp: datetime
    automatic_exit_deadline: datetime
    override_audit: list[dict[str, Any]] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not self.issues

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "product": self.request.product.value,
            "spot_vix": self.request.spot_vix,
            "call_expiration": self.request.call_expiration.isoformat(),
            "put_expiration": self.request.put_expiration.isoformat(),
            "settlement_type": self.request.settlement_type.value,
            "intended_session": self.request.intended_session.value,
            "actual_session": self.actual_session.value,
            "calendar_days_to_expiration": self.calendar_days_to_expiration,
            "full_regular_sessions_remaining": self.full_regular_sessions_remaining,
            "final_tradable_timestamp": self.final_tradable_timestamp.astimezone(UTC).isoformat(),
            "automatic_exit_deadline": self.automatic_exit_deadline.astimezone(UTC).isoformat(),
            "combined_debit": self.request.combined_debit,
            "maximum_cycle_loss": self.request.maximum_cycle_loss,
            "issues": [asdict(issue) for issue in self.issues],
            "warnings": [asdict(issue) for issue in self.warnings],
            "override_audit": self.override_audit,
        }


@dataclass
class VixPairedCycle:
    execution_cycle: ExecutionCycle
    preflight: VixPreflightResult
    call_status: str = "PLANNED"
    put_status: str = "PLANNED"
    first_move: str | None = None
    first_leg_sold: str | None = None
    remaining_leg: str | None = None
    additions: list[dict[str, Any]] = field(default_factory=list)
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    maximum_drawdown: float = 0.0
    execution_deviations: list[dict[str, Any]] = field(default_factory=list)

    @property
    def combined_cycle_pnl(self) -> float:
        return round(self.realized_pnl + self.unrealized_pnl, 2)

    def to_json_dict(self) -> dict[str, Any]:
        payload = self.execution_cycle.to_json_dict()
        payload.update(
            {
                "preflight": self.preflight.to_json_dict(),
                "call_status": self.call_status,
                "put_status": self.put_status,
                "first_move": self.first_move,
                "first_leg_sold": self.first_leg_sold,
                "remaining_leg": self.remaining_leg,
                "additions": self.additions,
                "realized_pnl": self.realized_pnl,
                "unrealized_pnl": self.unrealized_pnl,
                "combined_cycle_pnl": self.combined_cycle_pnl,
                "maximum_drawdown": self.maximum_drawdown,
                "execution_deviations": self.execution_deviations,
            }
        )
        return payload

    def add_opposite_leg(
        self,
        *,
        leg: str,
        quantity: int,
        debit: float,
        reason: str,
        config: VixStrategyConfig | None = None,
        actor: str = "operator",
    ) -> dict[str, Any]:
        config = config or VixStrategyConfig()
        if leg not in {"call", "put"}:
            raise ValueError("addition_leg_invalid")
        if config.maximum_additions is None or config.maximum_additional_capital is None or config.maximum_cycle_allocation is None:
            raise ValueError("addition_configuration_incomplete")
        if len(self.additions) >= config.maximum_additions:
            raise ValueError("maximum_additions_reached")
        if config.require_first_leg_exit_before_addition and not self.first_leg_sold:
            raise ValueError("first_leg_exit_required_before_addition")
        if leg == self.first_leg_sold:
            raise ValueError("addition_must_target_opposite_leg")
        if quantity <= 0 or debit <= 0:
            raise ValueError("addition_quantity_and_debit_required")
        incremental_capital = round(quantity * debit * 100, 2)
        if incremental_capital > config.maximum_additional_capital:
            raise ValueError("maximum_additional_capital_exceeded")
        if self.execution_cycle.capital_committed + incremental_capital > config.maximum_cycle_allocation:
            raise ValueError("maximum_cycle_allocation_exceeded")
        addition = {
            "addition_id": f"addition-{uuid4()}",
            "leg": leg,
            "quantity": quantity,
            "intended_debit": debit,
            "capital": incremental_capital,
            "reason": reason,
            "state": "PLANNED",
            "created_at": datetime.now(UTC).isoformat(),
        }
        self.additions.append(addition)
        self.execution_cycle.record_event("opposite_leg_addition_planned", reason, actor=actor, payload=addition)
        self.execution_cycle.lifecycle_state = CycleLifecycleState.REBALANCE_ELIGIBLE
        self.execution_cycle.next_required_action = "review_addition_order"
        return addition


def vix_cycle_analytics(cycle: VixPairedCycle) -> dict[str, Any]:
    addition_capital = round(sum(float(item.get("capital") or 0.0) for item in cycle.additions), 2)
    capital_committed = round(cycle.execution_cycle.capital_committed + addition_capital, 2)
    return {
        "strategy_performance": {
            "combined_cycle_pnl": cycle.combined_cycle_pnl,
            "realized_pnl": cycle.realized_pnl,
            "unrealized_pnl": cycle.unrealized_pnl,
            "capital_committed": capital_committed,
            "maximum_drawdown": cycle.maximum_drawdown,
            "addition_count": len(cycle.additions),
            "profitability_claim": "measured_only_from_confirmed_fills_and_current_quotes",
        },
        "execution_quality": {
            "deviation_count": len(cycle.execution_deviations),
            "deviations": list(cycle.execution_deviations),
            "intended_session": cycle.preflight.request.intended_session.value,
            "actual_session": cycle.preflight.actual_session.value,
            "intended_expiration": cycle.preflight.request.call_expiration.isoformat(),
            "selected_expiration": cycle.preflight.request.put_expiration.isoformat(),
            "unknown_deviation_costs_are_estimated": False,
        },
    }


def classify_vix_session(at: datetime) -> TradingSession:
    localized = at.astimezone(_eastern_timezone(at.date()))
    if localized.weekday() >= 5:
        return TradingSession.CLOSED
    current = localized.time().replace(tzinfo=None)
    if time(9, 30) <= current <= time(16, 15):
        return TradingSession.REGULAR
    if time(16, 15) < current <= time(17, 0):
        return TradingSession.CURB
    if current >= time(20, 15) or current <= time(9, 25):
        return TradingSession.GLOBAL
    return TradingSession.CLOSED


def previous_business_day(value: date, *, holidays: set[date] | None = None) -> date:
    holidays = holidays or set()
    cursor = value - timedelta(days=1)
    while cursor.weekday() >= 5 or cursor in holidays:
        cursor -= timedelta(days=1)
    return cursor


def vix_final_tradable_timestamp(expiration: date, *, holidays: set[date] | None = None) -> datetime:
    last_day = previous_business_day(expiration, holidays=holidays)
    return datetime.combine(last_day, time(16, 0), tzinfo=_central_timezone(last_day))


def full_regular_sessions_remaining(start: datetime, expiration: date, *, holidays: set[date] | None = None) -> int:
    holidays = holidays or set()
    final_day = previous_business_day(expiration, holidays=holidays)
    cursor = start.astimezone(_eastern_timezone(start.date())).date() + timedelta(days=1)
    count = 0
    while cursor <= final_day:
        if cursor.weekday() < 5 and cursor not in holidays:
            count += 1
        cursor += timedelta(days=1)
    return count


def validate_vix_preflight(
    request: VixPreflightRequest,
    config: VixStrategyConfig | None = None,
    *,
    holidays: set[date] | None = None,
) -> VixPreflightResult:
    config = config or VixStrategyConfig()
    actual_session = classify_vix_session(request.actual_timestamp)
    final_tradable = vix_final_tradable_timestamp(request.expiration, holidays=holidays)
    automatic_exit = final_tradable - timedelta(minutes=config.mandatory_exit_buffer_minutes)
    days = (request.expiration - request.actual_timestamp.astimezone(_eastern_timezone(request.actual_timestamp.date())).date()).days
    sessions = full_regular_sessions_remaining(request.actual_timestamp, request.expiration, holidays=holidays)
    issues: list[PreflightIssue] = []
    warnings: list[PreflightIssue] = []

    def block(code: str, message: str, category: str, *, overridable: bool = False) -> None:
        if code in request.requested_override_codes and overridable and request.override_actor:
            warnings.append(PreflightIssue(code, message, category, overridable=True))
            return
        issues.append(PreflightIssue(code, message, category, overridable=overridable))

    required_config = {
        "minimum_full_trading_sessions_remaining": config.minimum_full_trading_sessions_remaining,
        "maximum_days_to_expiration": config.maximum_days_to_expiration,
        "maximum_combined_debit": config.maximum_combined_debit,
        "maximum_cycle_allocation": config.maximum_cycle_allocation,
        "first_leg_exit_target_pct": config.first_leg_exit_target_pct,
        "second_leg_management_rule": config.second_leg_management_rule,
        "maximum_additions": config.maximum_additions,
        "maximum_additional_capital": config.maximum_additional_capital,
        "addition_sizing": config.addition_sizing,
        "addition_trigger": config.addition_trigger,
    }
    missing_config = sorted(key for key, value in required_config.items() if value is None or value == "")
    if missing_config:
        block("strategy_configuration_incomplete", f"Configure before entry: {', '.join(missing_config)}.", "strategy")

    if not config.enabled:
        block("strategy_disabled", "VIX paired-options strategy is disabled.", "strategy")
    if request.product not in config.accepted_products:
        block("wrong_underlying", f"{request.product.value} is not an accepted configured product.", "contract")
    if request.call_product is not request.product or request.put_product is not request.product:
        block("vix_vixw_mismatch", "Call and put must use the exact reviewed VIX or VIXW product.", "contract")
    if request.call_expiration != request.put_expiration:
        block("mismatched_expirations", "Call and put expirations must match.", "expiration")
    if request.settlement_type is not request.expected_settlement_type:
        block("wrong_settlement_assumption", "Selected settlement type differs from the reviewed contract settlement.", "expiration")
    if request.settlement_type is not SettlementType.AM:
        block("unsupported_settlement_type", "VIX/VIXW cycles default to AM settlement and require explicit correction.", "expiration")
    if config.regular_hours_only and actual_session is not TradingSession.REGULAR:
        block("extended_hours_entry_blocked", f"Actual session is {actual_session.value}; regular session is required.", "session", overridable=True)
    if request.intended_session is not actual_session:
        block("intended_actual_session_mismatch", "Intended and actual trading sessions differ.", "session", overridable=True)
    if config.minimum_full_trading_sessions_remaining is not None and sessions < config.minimum_full_trading_sessions_remaining:
        block("too_few_trading_sessions", f"Only {sessions} full regular sessions remain; {config.minimum_full_trading_sessions_remaining} required.", "expiration")
    if config.maximum_days_to_expiration is not None and days > config.maximum_days_to_expiration:
        block("expiration_too_distant", f"Expiration is {days} days away; configured maximum is {config.maximum_days_to_expiration}.", "expiration", overridable=True)
    if request.call_strike <= 0 or request.put_strike <= 0:
        block("invalid_strike", "Both reviewed strikes must be positive.", "contract")
    if request.call_quantity <= 0 or request.put_quantity <= 0:
        block("invalid_quantity", "Both legs require a positive quantity.", "risk")
    if request.call_quantity != request.put_quantity:
        block("quantity_mismatch", "Paired entry quantities must match unless explicitly overridden.", "risk", overridable=True)
    if config.maximum_combined_debit is not None and request.combined_debit > config.maximum_combined_debit:
        block("combined_debit_exceeded", "Combined per-pair debit exceeds strategy configuration.", "risk", overridable=True)
    if config.maximum_cycle_allocation is not None and request.maximum_cycle_loss > config.maximum_cycle_allocation:
        block("cycle_capital_exceeded", "Maximum cycle loss exceeds approved cycle allocation.", "risk")
    if request.existing_cycle_ids:
        block("duplicate_cycle", "An equivalent active cycle already exists.", "duplicate")
    if request.expiration in request.overlapping_expirations:
        block("overlapping_exposure", "Open exposure already exists for this expiration.", "risk", overridable=True)
    if request.client_request_id and request.client_request_id in request.prior_client_request_ids:
        block("duplicate_order", "Client request ID has already been used.", "duplicate")
    if not (config.enabled_entry_min <= request.spot_vix <= config.enabled_entry_max):
        block("spot_outside_enabled_range", "Spot VIX is outside the enabled entry range.", "strategy", overridable=True)
    elif not (config.preferred_entry_min <= request.spot_vix <= config.preferred_entry_max):
        warnings.append(PreflightIssue("spot_outside_preferred_range", "Spot VIX is enabled but outside the preferred 17s range.", "strategy", True))

    override_audit = [
        {
            "code": issue.code,
            "actor": request.override_actor,
            "timestamp": datetime.now(UTC).isoformat(),
            "detail": issue.message,
        }
        for issue in warnings
        if issue.code in request.requested_override_codes
    ]
    return VixPreflightResult(request, issues, warnings, actual_session, days, sessions, final_tradable, automatic_exit, override_audit)


def create_vix_cycle(request: VixPreflightRequest, config: VixStrategyConfig | None = None) -> VixPairedCycle:
    config = config or VixStrategyConfig()
    preflight = validate_vix_preflight(request, config)
    now = request.actual_timestamp.astimezone(UTC)
    execution = ExecutionCycle(
        cycle_id=f"vix-{uuid4()}",
        strategy_id=VIX_STRATEGY_ID,
        account_id=request.account_id,
        intent_timestamp=now,
        entry_window_start=now,
        entry_window_end=now + timedelta(minutes=15),
        maximum_approved_exposure=float(config.maximum_cycle_allocation or 0.0),
        exit_deadline=preflight.automatic_exit_deadline,
        strategy_payload={
            "product": request.product.value,
            "spot_vix_at_decision": request.spot_vix,
            "expiration": request.expiration.isoformat(),
            "calendar_days_to_expiration": preflight.calendar_days_to_expiration,
            "full_regular_sessions_remaining": preflight.full_regular_sessions_remaining,
            "settlement_type": request.settlement_type.value,
            "final_tradable_timestamp": preflight.final_tradable_timestamp.astimezone(UTC).isoformat(),
            "automatic_exit_deadline": preflight.automatic_exit_deadline.astimezone(UTC).isoformat(),
            "intended_session": request.intended_session.value,
            "actual_session": preflight.actual_session.value,
            "call_strike": request.call_strike,
            "put_strike": request.put_strike,
            "call_quantity": request.call_quantity,
            "put_quantity": request.put_quantity,
            "call_debit": request.call_debit,
            "put_debit": request.put_debit,
            "combined_debit": request.combined_debit,
            "first_leg_profit_target_pct": config.first_leg_exit_target_pct,
            "second_leg_management_rule": config.second_leg_management_rule,
            "maximum_additions": config.maximum_additions,
            "maximum_cycle_capital": config.maximum_cycle_allocation,
            "configuration_source": "default",
            "client_request_id": request.client_request_id,
        },
        lifecycle_state=CycleLifecycleState.PREFLIGHT_VALIDATED if preflight.passed else CycleLifecycleState.PREFLIGHT_BLOCKED,
        capital_committed=0.0,
        risk_policy_result={"passed": preflight.passed, "issues": [asdict(issue) for issue in preflight.issues]},
        next_required_action="review_and_submit_entry" if preflight.passed else "correct_preflight_issues",
    )
    execution.record_event("preflight_completed", "VIX paired-options preflight completed.", payload=preflight.to_json_dict())
    for override in preflight.override_audit:
        execution.record_event("manual_override", override["detail"], actor=str(override["actor"]), payload=override)
    return VixPairedCycle(execution, preflight)


def vix_cycle_store_path() -> Path:
    return data_root() / "vix_trader" / "cycles.jsonl"


def append_vix_cycle(cycle: VixPairedCycle, *, path: str | Path | None = None) -> Path:
    target = Path(path) if path is not None else vix_cycle_store_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(cycle.to_json_dict(), sort_keys=True) + "\n")
    return target


def load_vix_cycles(*, path: str | Path | None = None, limit: int = 100) -> list[dict[str, Any]]:
    target = Path(path) if path is not None else vix_cycle_store_path()
    if not target.exists():
        return []
    rows = [json.loads(line) for line in target.read_text(encoding="utf-8").splitlines() if line.strip()]
    return rows[-max(0, limit):]


def vix_strategy_status() -> dict[str, Any]:
    config = VixStrategyConfig()
    cycles = load_vix_cycles(limit=20)
    active = [row for row in cycles if row.get("lifecycle_state") not in {"CLOSED", "RECONCILED"}]
    return {
        "ok": True,
        "strategy_id": VIX_STRATEGY_ID,
        "name": "VIX Trader",
        "mode": "simulation_and_preflight_only",
        "broker_execution_supported": False,
        "broker_blocker": "current Alpaca adapter does not expose actual Cboe VIX/VIXW index options",
        "profitability_status": "unproven",
        "config": config.to_json_dict(),
        "cycle_count": len(cycles),
        "active_cycle_count": len(active),
        "cycles": list(reversed(cycles)),
    }


VIX_STRATEGY = register_strategy(
    StrategyDefinition(
        strategy_id=VIX_STRATEGY_ID,
        name="VIX Trader",
        category="options",
        enabled=True,
        supported_underlying_types=("VIX", "VIXW"),
        configuration_schema={key: type(value).__name__ for key, value in VixStrategyConfig().to_json_dict().items()},
        preflight_validator=validate_vix_preflight,
        lifecycle_handler=create_vix_cycle,
        risk_policy_extensions=("paired_debit_cap", "expiration_overlap", "settlement_deadline", "addition_cap"),
        analytics_definitions=("combined_cycle_pnl", "maximum_drawdown", "execution_deviations"),
        strategy_screens=("workspace", "preflight", "cycle_detail", "performance", "execution_quality"),
        simulation_supported=True,
        broker_execution_supported=False,
    )
)
