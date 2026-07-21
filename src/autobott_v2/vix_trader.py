from __future__ import annotations

import json
import math
import os
import time as time_module
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime, time, timedelta, tzinfo
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


@dataclass(frozen=True)
class VixContractMetadata:
    option_symbol: str
    product: VixProduct
    option_type: str
    expiration: date
    strike: float
    settlement_type: SettlementType
    source: str
    observed_at: datetime

    @property
    def authoritative(self) -> bool:
        return self.source in {"broker", "exchange"}


class CboeCalendar(Protocol):
    authoritative: bool

    def session_at(self, at: datetime) -> TradingSession: ...
    def final_tradable_timestamp(self, expiration: date) -> datetime: ...
    def full_regular_sessions_remaining(self, start: datetime, expiration: date) -> int: ...
    def covers(self, start: date, end: date) -> bool: ...


@dataclass(frozen=True)
class AuthoritativeCboeCalendar:
    """Explicit Cboe calendar snapshot; callers must load current holiday exceptions."""

    holidays: frozenset[date]
    early_closes: dict[date, time] = field(default_factory=dict)
    source: str = "cboe_published_schedule"
    source_url: str = "https://www.cboe.com/about/hours/us-options/"
    coverage_start: date = date.min
    coverage_end: date = date.max
    published_at: datetime | None = None
    authoritative: bool = True

    def covers(self, start: date, end: date) -> bool:
        return self.coverage_start <= start <= end <= self.coverage_end

    def _is_trading_day(self, value: date) -> bool:
        return value.weekday() < 5 and value not in self.holidays

    def _previous_trading_day(self, value: date) -> date:
        cursor = value - timedelta(days=1)
        while not self._is_trading_day(cursor):
            cursor -= timedelta(days=1)
        return cursor

    def session_at(self, at: datetime) -> TradingSession:
        localized = at.astimezone(_eastern_timezone(at.date()))
        day = localized.date()
        current = localized.time().replace(tzinfo=None)
        if self._is_trading_day(day):
            regular_close = self.early_closes.get(day, time(16, 15))
            if time(9, 30) <= current <= regular_close:
                return TradingSession.REGULAR
            if day not in self.early_closes and time(16, 15) < current <= time(17, 0):
                return TradingSession.CURB
            if current <= time(9, 25):
                return TradingSession.GLOBAL
        next_day = day + timedelta(days=1)
        evening_gth_day = day.weekday() in {0, 1, 2, 3, 6}
        if evening_gth_day and current >= time(20, 15) and self._is_trading_day(next_day):
            return TradingSession.GLOBAL
        return TradingSession.CLOSED

    def final_tradable_timestamp(self, expiration: date) -> datetime:
        last_day = self._previous_trading_day(expiration)
        close = self.early_closes.get(last_day, time(17, 0))
        return datetime.combine(last_day, close, tzinfo=_eastern_timezone(last_day))

    def full_regular_sessions_remaining(self, start: datetime, expiration: date) -> int:
        final_day = self._previous_trading_day(expiration)
        cursor = start.astimezone(_eastern_timezone(start.date())).date() + timedelta(days=1)
        count = 0
        while cursor <= final_day:
            if self._is_trading_day(cursor):
                count += 1
            cursor += timedelta(days=1)
        return count


@dataclass(frozen=True)
class UnavailableCboeCalendar:
    authoritative: bool = False
    source: str = "unavailable"
    source_url: str = ""
    coverage_start: date | None = None
    coverage_end: date | None = None
    published_at: datetime | None = None

    def session_at(self, _at: datetime) -> TradingSession:
        return TradingSession.CLOSED

    def final_tradable_timestamp(self, expiration: date) -> datetime:
        return datetime.combine(expiration, time(0, 0), tzinfo=UTC)

    def full_regular_sessions_remaining(self, _start: datetime, _expiration: date) -> int:
        return 0

    def covers(self, _start: date, _end: date) -> bool:
        return False


def cboe_calendar_path() -> Path:
    return data_root() / "vix_trader" / "cboe_calendar.json"


def load_cboe_calendar(*, path: str | Path | None = None) -> CboeCalendar:
    target = Path(path) if path is not None else cboe_calendar_path()
    if not target.exists():
        return UnavailableCboeCalendar()
    payload = json.loads(target.read_text(encoding="utf-8"))
    source = str(payload.get("source") or "")
    source_url = str(payload.get("source_url") or "")
    if (
        not source.startswith("cboe")
        or "cboe.com/" not in source_url.lower()
        or not payload.get("published_at")
        or not payload.get("coverage_start")
        or not payload.get("coverage_end")
    ):
        return UnavailableCboeCalendar()
    holidays = frozenset(date.fromisoformat(str(item)[:10]) for item in payload.get("holidays", []))
    early_closes = {
        date.fromisoformat(str(day)[:10]): time.fromisoformat(str(close))
        for day, close in (payload.get("early_closes") or {}).items()
    }
    published_at = datetime.fromisoformat(str(payload["published_at"]).replace("Z", "+00:00"))
    if published_at.tzinfo is None:
        return UnavailableCboeCalendar()
    return AuthoritativeCboeCalendar(
        holidays=holidays,
        early_closes=early_closes,
        source=source,
        source_url=source_url,
        coverage_start=date.fromisoformat(str(payload["coverage_start"])[:10]),
        coverage_end=date.fromisoformat(str(payload["coverage_end"])[:10]),
        published_at=published_at,
    )


class VixBrokerAdapter(Protocol):
    """Capability boundary for a future broker with actual VIX/VIXW support."""

    def get_account(self) -> dict[str, Any]: ...
    def get_option_chain(self, product: VixProduct, expiration: date) -> list[dict[str, Any]]: ...
    def get_contract_metadata(self, option_symbol: str) -> VixContractMetadata: ...
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

    def missing_required_fields(self) -> list[str]:
        required = {
            "minimum_full_trading_sessions_remaining": self.minimum_full_trading_sessions_remaining,
            "maximum_days_to_expiration": self.maximum_days_to_expiration,
            "maximum_combined_debit": self.maximum_combined_debit,
            "maximum_cycle_allocation": self.maximum_cycle_allocation,
            "first_leg_exit_target_pct": self.first_leg_exit_target_pct,
            "second_leg_management_rule": self.second_leg_management_rule,
            "maximum_additions": self.maximum_additions,
            "maximum_additional_capital": self.maximum_additional_capital,
            "addition_sizing": self.addition_sizing,
            "addition_trigger": self.addition_trigger,
        }
        return sorted(key for key, value in required.items() if value is None or value == "")

    def validation_errors(self) -> list[str]:
        errors: list[str] = []
        numeric_values = {
            "preferred_entry_min": self.preferred_entry_min,
            "preferred_entry_max": self.preferred_entry_max,
            "enabled_entry_min": self.enabled_entry_min,
            "enabled_entry_max": self.enabled_entry_max,
        }
        optional_positive = {
            "maximum_combined_debit": self.maximum_combined_debit,
            "maximum_cycle_allocation": self.maximum_cycle_allocation,
            "maximum_additional_capital": self.maximum_additional_capital,
            "first_leg_exit_target_pct": self.first_leg_exit_target_pct,
        }
        for name, value in {**numeric_values, **optional_positive}.items():
            if value is not None and (not math.isfinite(value) or value <= 0):
                errors.append(f"{name}_must_be_positive_finite")
        for name, value in {
            "minimum_full_trading_sessions_remaining": self.minimum_full_trading_sessions_remaining,
            "maximum_days_to_expiration": self.maximum_days_to_expiration,
            "addition_sizing": self.addition_sizing,
            "mandatory_exit_buffer_minutes": self.mandatory_exit_buffer_minutes,
        }.items():
            if value is not None and value <= 0:
                errors.append(f"{name}_must_be_positive")
        if self.maximum_additions is not None and self.maximum_additions < 0:
            errors.append("maximum_additions_must_be_nonnegative")
        if self.preferred_entry_min > self.preferred_entry_max:
            errors.append("preferred_entry_range_inverted")
        if self.enabled_entry_min > self.enabled_entry_max:
            errors.append("enabled_entry_range_inverted")
        if self.preferred_entry_min < self.enabled_entry_min or self.preferred_entry_max > self.enabled_entry_max:
            errors.append("preferred_entry_range_outside_enabled_range")
        if self.first_leg_exit_target_pct is not None and self.first_leg_exit_target_pct > 1:
            errors.append("first_leg_exit_target_pct_above_one")
        if not self.accepted_products:
            errors.append("accepted_products_required")
        return sorted(set(errors))


def vix_strategy_config_path() -> Path:
    return data_root() / "vix_trader" / "config.json"


def _bool_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def vix_strategy_config_from_dict(payload: dict[str, Any]) -> VixStrategyConfig:
    defaults = VixStrategyConfig()
    accepted = payload.get("accepted_products", [item.value for item in defaults.accepted_products])
    return VixStrategyConfig(
        enabled=_bool_value(payload.get("enabled", defaults.enabled)),
        preferred_entry_min=float(payload.get("preferred_entry_min", defaults.preferred_entry_min)),
        preferred_entry_max=float(payload.get("preferred_entry_max", defaults.preferred_entry_max)),
        enabled_entry_min=float(payload.get("enabled_entry_min", defaults.enabled_entry_min)),
        enabled_entry_max=float(payload.get("enabled_entry_max", defaults.enabled_entry_max)),
        minimum_full_trading_sessions_remaining=int(payload["minimum_full_trading_sessions_remaining"]) if payload.get("minimum_full_trading_sessions_remaining") is not None else None,
        maximum_days_to_expiration=int(payload["maximum_days_to_expiration"]) if payload.get("maximum_days_to_expiration") is not None else None,
        regular_hours_only=_bool_value(payload.get("regular_hours_only", defaults.regular_hours_only)),
        accepted_products=tuple(VixProduct(str(item).upper()) for item in accepted),
        strike_selection_method=str(payload.get("strike_selection_method", defaults.strike_selection_method)),
        maximum_combined_debit=float(payload["maximum_combined_debit"]) if payload.get("maximum_combined_debit") is not None else None,
        maximum_cycle_allocation=float(payload["maximum_cycle_allocation"]) if payload.get("maximum_cycle_allocation") is not None else None,
        first_leg_exit_target_pct=float(payload["first_leg_exit_target_pct"]) if payload.get("first_leg_exit_target_pct") is not None else None,
        second_leg_management_rule=str(payload["second_leg_management_rule"]) if payload.get("second_leg_management_rule") else None,
        maximum_additions=int(payload["maximum_additions"]) if payload.get("maximum_additions") is not None else None,
        maximum_additional_capital=float(payload["maximum_additional_capital"]) if payload.get("maximum_additional_capital") is not None else None,
        addition_sizing=int(payload["addition_sizing"]) if payload.get("addition_sizing") is not None else None,
        addition_trigger=str(payload["addition_trigger"]) if payload.get("addition_trigger") else None,
        require_first_leg_exit_before_addition=_bool_value(payload.get("require_first_leg_exit_before_addition", defaults.require_first_leg_exit_before_addition)),
        mandatory_exit_buffer_minutes=int(payload.get("mandatory_exit_buffer_minutes", defaults.mandatory_exit_buffer_minutes)),
        settlement_trading_authorized=_bool_value(payload.get("settlement_trading_authorized", defaults.settlement_trading_authorized)),
    )


_VIX_ENV_FIELDS = {
    "AUTOBOTT_VIX_MIN_FULL_SESSIONS": "minimum_full_trading_sessions_remaining",
    "AUTOBOTT_VIX_MAX_DTE": "maximum_days_to_expiration",
    "AUTOBOTT_VIX_MAX_COMBINED_DEBIT": "maximum_combined_debit",
    "AUTOBOTT_VIX_MAX_CYCLE_ALLOCATION": "maximum_cycle_allocation",
    "AUTOBOTT_VIX_FIRST_LEG_TARGET_PCT": "first_leg_exit_target_pct",
    "AUTOBOTT_VIX_SECOND_LEG_RULE": "second_leg_management_rule",
    "AUTOBOTT_VIX_MAX_ADDITIONS": "maximum_additions",
    "AUTOBOTT_VIX_MAX_ADDITIONAL_CAPITAL": "maximum_additional_capital",
    "AUTOBOTT_VIX_ADDITION_SIZING": "addition_sizing",
    "AUTOBOTT_VIX_ADDITION_TRIGGER": "addition_trigger",
}


def load_vix_strategy_config(*, path: str | Path | None = None, environ: dict[str, str] | None = None) -> VixStrategyConfig:
    target = Path(path) if path is not None else vix_strategy_config_path()
    payload: dict[str, Any] = {}
    if target.exists():
        payload.update(json.loads(target.read_text(encoding="utf-8")))
    source_env = environ if environ is not None else os.environ
    for env_name, field_name in _VIX_ENV_FIELDS.items():
        if source_env.get(env_name) not in {None, ""}:
            payload[field_name] = source_env[env_name]
    return vix_strategy_config_from_dict(payload)


def save_vix_strategy_config(config: VixStrategyConfig, *, path: str | Path | None = None) -> Path:
    if config.validation_errors():
        raise ValueError("invalid_vix_strategy_config:" + ",".join(config.validation_errors()))
    target = Path(path) if path is not None else vix_strategy_config_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(".tmp")
    temporary.write_text(json.dumps(config.to_json_dict(), indent=2, sort_keys=True), encoding="utf-8")
    temporary.replace(target)
    return target


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
    call_contract: VixContractMetadata | None = None
    put_contract: VixContractMetadata | None = None
    timestamp_source: str = "server"

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
    equity_curve: list[float] = field(default_factory=list)
    execution_deviations: list[dict[str, Any]] = field(default_factory=list)

    @property
    def accounting(self) -> dict[str, float]:
        return _derived_cycle_accounting(self.execution_cycle)

    @property
    def combined_cycle_pnl(self) -> float:
        return self.accounting["combined_cycle_pnl"]

    @property
    def maximum_drawdown(self) -> float:
        peak = 0.0
        drawdown = 0.0
        for value in self.equity_curve:
            peak = max(peak, value)
            drawdown = max(drawdown, peak - value)
        return round(drawdown, 2)

    def apply_market_estimates(self, estimates: dict[str, float], *, source: str) -> None:
        if source not in {"broker", "exchange"}:
            raise ValueError("authoritative_quote_source_required")
        if any(not math.isfinite(float(value)) or float(value) < 0 for value in estimates.values()):
            raise ValueError("invalid_market_estimate")
        self.execution_cycle.current_market_estimates = {str(key): float(value) for key, value in estimates.items()}
        self.equity_curve.append(self.combined_cycle_pnl)

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
                "realized_pnl": self.accounting["realized_pnl"],
                "unrealized_pnl": self.accounting["unrealized_pnl"],
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
        trigger_condition_met: bool,
        trigger_evidence: dict[str, Any],
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
        if not trigger_condition_met or not trigger_evidence:
            raise ValueError("configured_addition_trigger_not_proven")
        if quantity != config.addition_sizing:
            raise ValueError("addition_sizing_mismatch")
        if quantity <= 0 or debit <= 0:
            raise ValueError("addition_quantity_and_debit_required")
        incremental_capital = round(quantity * debit * 100, 2)
        if incremental_capital > config.maximum_additional_capital:
            raise ValueError("maximum_additional_capital_exceeded")
        cumulative_addition_capital = sum(float(item.get("capital") or 0.0) for item in self.additions) + incremental_capital
        if cumulative_addition_capital > config.maximum_additional_capital:
            raise ValueError("cumulative_additional_capital_exceeded")
        if self.execution_cycle.capital_committed + cumulative_addition_capital > config.maximum_cycle_allocation:
            raise ValueError("maximum_cycle_allocation_exceeded")
        addition = {
            "addition_id": f"addition-{uuid4()}",
            "leg": leg,
            "quantity": quantity,
            "intended_debit": debit,
            "capital": incremental_capital,
            "reason": reason,
            "configured_trigger": config.addition_trigger,
            "trigger_evidence": trigger_evidence,
            "state": "PLANNED",
            "created_at": datetime.now(UTC).isoformat(),
        }
        self.additions.append(addition)
        self.execution_cycle.record_event("opposite_leg_addition_planned", reason, actor=actor, payload=addition)
        if self.execution_cycle.lifecycle_state is CycleLifecycleState.FIRST_LEG_EXITED:
            self.execution_cycle.transition(CycleLifecycleState.REBALANCE_ELIGIBLE, actor=actor, reason=reason)
        elif self.execution_cycle.lifecycle_state is not CycleLifecycleState.REBALANCE_ELIGIBLE:
            raise ValueError("cycle_not_rebalance_eligible")
        self.execution_cycle.next_required_action = "review_addition_order"
        return addition


def vix_cycle_analytics(cycle: VixPairedCycle) -> dict[str, Any]:
    accounting = cycle.accounting
    return {
        "strategy_performance": {
            "combined_cycle_pnl": accounting["combined_cycle_pnl"],
            "realized_pnl": accounting["realized_pnl"],
            "unrealized_pnl": accounting["unrealized_pnl"],
            "realized_proceeds": accounting["realized_proceeds"],
            "open_value": accounting["open_value"],
            "capital_committed": accounting["capital_committed"],
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


def _derived_cycle_accounting(cycle: ExecutionCycle) -> dict[str, float]:
    entry_cost_by_leg: dict[str, float] = {}
    entry_qty_by_leg: dict[str, int] = {}
    exit_proceeds_by_leg: dict[str, float] = {}
    exit_qty_by_leg: dict[str, int] = {}
    for order in cycle.orders:
        if order.purpose in {"entry", "addition", "rebalance"}:
            entry_cost_by_leg[order.leg_id] = entry_cost_by_leg.get(order.leg_id, 0.0) + order.confirmed_cost
            entry_qty_by_leg[order.leg_id] = entry_qty_by_leg.get(order.leg_id, 0) + order.confirmed_quantity
        elif order.purpose in {"exit", "reduce"}:
            exit_proceeds_by_leg[order.leg_id] = exit_proceeds_by_leg.get(order.leg_id, 0.0) + order.confirmed_proceeds
            exit_qty_by_leg[order.leg_id] = exit_qty_by_leg.get(order.leg_id, 0) + order.confirmed_quantity
    realized_cost = 0.0
    remaining_cost = 0.0
    for leg_id, entry_qty in entry_qty_by_leg.items():
        average_cost = entry_cost_by_leg.get(leg_id, 0.0) / entry_qty if entry_qty else 0.0
        exited = min(entry_qty, exit_qty_by_leg.get(leg_id, 0))
        realized_cost += average_cost * exited
        remaining_cost += average_cost * max(0, entry_qty - exited)
    realized_proceeds = round(sum(exit_proceeds_by_leg.values()), 2)
    open_value = cycle.current_open_value
    capital_committed = round(sum(entry_cost_by_leg.values()), 2)
    realized_pnl = round(realized_proceeds - realized_cost, 2)
    unrealized_pnl = round(open_value - remaining_cost, 2)
    return {
        "capital_committed": capital_committed,
        "realized_proceeds": realized_proceeds,
        "open_value": open_value,
        "realized_pnl": realized_pnl,
        "unrealized_pnl": unrealized_pnl,
        "combined_cycle_pnl": round(realized_proceeds + open_value - capital_committed, 2),
    }


def classify_vix_session(at: datetime, *, calendar: CboeCalendar | None = None) -> TradingSession:
    return (calendar or UnavailableCboeCalendar()).session_at(at)


def vix_final_tradable_timestamp(expiration: date, *, calendar: CboeCalendar | None = None) -> datetime:
    return (calendar or UnavailableCboeCalendar()).final_tradable_timestamp(expiration)


def full_regular_sessions_remaining(start: datetime, expiration: date, *, calendar: CboeCalendar | None = None) -> int:
    return (calendar or UnavailableCboeCalendar()).full_regular_sessions_remaining(start, expiration)


def validate_vix_preflight(
    request: VixPreflightRequest,
    config: VixStrategyConfig | None = None,
    *,
    calendar: CboeCalendar | None = None,
    authorized_override_actor: str | None = None,
    allowed_override_codes: set[str] | None = None,
    require_evidence: bool | None = None,
) -> VixPreflightResult:
    config_was_provided = config is not None
    if require_evidence is None:
        require_evidence = not config_was_provided

    evidence_resolution = None
    if require_evidence:
        from .vix_evidence import resolve_vix_strategy_config

        evidence_resolution = resolve_vix_strategy_config()
        config = evidence_resolution.config or VixStrategyConfig()
    else:
        config = config or VixStrategyConfig()

    resolved_calendar = calendar or UnavailableCboeCalendar()
    actual_session = resolved_calendar.session_at(request.actual_timestamp)
    final_tradable = resolved_calendar.final_tradable_timestamp(request.expiration)
    automatic_exit = final_tradable - timedelta(minutes=config.mandatory_exit_buffer_minutes)
    days = (request.expiration - request.actual_timestamp.astimezone(_eastern_timezone(request.actual_timestamp.date())).date()).days
    sessions = resolved_calendar.full_regular_sessions_remaining(request.actual_timestamp, request.expiration)
    issues: list[PreflightIssue] = []
    warnings: list[PreflightIssue] = []

    def block(code: str, message: str, category: str, *, overridable: bool = False) -> None:
        can_override = (
            code in request.requested_override_codes
            and overridable
            and authorized_override_actor is not None
            and code in (allowed_override_codes or set())
        )
        if can_override:
            warnings.append(PreflightIssue(code, message, category, overridable=True))
            return
        issues.append(PreflightIssue(code, message, category, overridable=overridable))

    if require_evidence and evidence_resolution is not None and evidence_resolution.config is None:
        reasons = ", ".join(evidence_resolution.blocking_reasons) or "insufficient_evidence"
        block(
            "strategy_evidence_insufficient",
            f"No VIX parameter set has proven itself yet ({reasons}).",
            "strategy",
        )
    missing_config = config.missing_required_fields()
    if missing_config and not require_evidence:
        block("strategy_configuration_incomplete", f"Candidate configuration incomplete: {', '.join(missing_config)}.", "strategy")
    invalid_config = config.validation_errors()
    if invalid_config:
        block("strategy_configuration_invalid", f"Invalid strategy configuration: {', '.join(invalid_config)}.", "strategy")

    if not resolved_calendar.authoritative:
        block("authoritative_calendar_required", "Current Cboe holiday and early-close calendar is unavailable.", "calendar")
    elif not resolved_calendar.covers(request.actual_timestamp.date(), request.expiration):
        block("calendar_coverage_incomplete", "Authoritative Cboe calendar does not cover the decision-through-expiration window.", "calendar")
    if request.timestamp_source not in {"server", "broker"}:
        block("untrusted_timestamp", "Preflight time must come from the server or broker.", "session")
    if request.requested_override_codes and not authorized_override_actor:
        block("unauthorized_override_request", "Override identity must come from authenticated server context.", "override")
    if not request.client_request_id or not request.client_request_id.strip():
        block("client_request_id_required", "A unique client request ID is required.", "duplicate")

    if not config.enabled:
        block("strategy_disabled", "VIX paired-options strategy is disabled.", "strategy")
    if request.product not in config.accepted_products:
        block("wrong_underlying", f"{request.product.value} is not an accepted configured product.", "contract")
    if request.call_product is not request.product or request.put_product is not request.product:
        block("vix_vixw_mismatch", "Call and put must use the exact reviewed VIX or VIXW product.", "contract")
    _validate_contract_metadata(request, request.call_contract, expected_type="call", block=block)
    _validate_contract_metadata(request, request.put_contract, expected_type="put", block=block)
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
    if not all(math.isfinite(value) and value > 0 for value in (request.call_strike, request.put_strike)):
        block("invalid_strike", "Both reviewed strikes must be positive.", "contract")
    if request.call_quantity <= 0 or request.put_quantity <= 0:
        block("invalid_quantity", "Both legs require a positive quantity.", "risk")
    if request.call_quantity != request.put_quantity:
        block("quantity_mismatch", "Paired entry quantities must match unless explicitly overridden.", "risk", overridable=True)
    if not all(math.isfinite(value) and value > 0 for value in (request.call_debit, request.put_debit, request.combined_debit)):
        block("invalid_debit", "Call and put debit values must be positive finite numbers.", "risk")
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
            "actor": authorized_override_actor,
            "timestamp": datetime.now(UTC).isoformat(),
            "detail": issue.message,
        }
        for issue in warnings
        if issue.code in request.requested_override_codes and issue.code in (allowed_override_codes or set())
    ]
    return VixPreflightResult(request, issues, warnings, actual_session, days, sessions, final_tradable, automatic_exit, override_audit)


def _validate_contract_metadata(
    request: VixPreflightRequest,
    contract: VixContractMetadata | None,
    *,
    expected_type: str,
    block: Any,
) -> None:
    if contract is None or not contract.authoritative:
        block("authoritative_contract_metadata_required", f"Authoritative {expected_type} contract metadata is required.", "contract")
        return
    described_product = request.call_product if expected_type == "call" else request.put_product
    described_expiration = request.call_expiration if expected_type == "call" else request.put_expiration
    described_strike = request.call_strike if expected_type == "call" else request.put_strike
    if contract.option_type.lower() != expected_type:
        block("contract_type_mismatch", f"Authoritative contract is not a {expected_type}.", "contract")
    if contract.product is not described_product or contract.product is not request.product:
        block("contract_product_mismatch", "Authoritative contract root does not match the reviewed VIX/VIXW product.", "contract")
    if contract.expiration != described_expiration:
        block("contract_expiration_mismatch", "Authoritative contract expiration differs from the reviewed expiration.", "contract")
    if not math.isclose(contract.strike, described_strike, rel_tol=0.0, abs_tol=1e-9):
        block("contract_strike_mismatch", "Authoritative contract strike differs from the reviewed strike.", "contract")
    if contract.settlement_type is not request.settlement_type:
        block("contract_settlement_mismatch", "Authoritative settlement metadata differs from the reviewed settlement.", "expiration")


def create_vix_cycle(
    request: VixPreflightRequest,
    config: VixStrategyConfig | None = None,
    *,
    calendar: CboeCalendar | None = None,
    authorized_override_actor: str | None = None,
    allowed_override_codes: set[str] | None = None,
) -> VixPairedCycle:
    from .vix_evidence import resolve_vix_strategy_config, vix_strategy_fingerprint

    require_evidence = config is None
    configuration_source = "explicit_candidate"
    fingerprint = None
    if require_evidence:
        evidence_resolution = resolve_vix_strategy_config()
        config = evidence_resolution.config or VixStrategyConfig()
        configuration_source = evidence_resolution.source if evidence_resolution.config is not None else "none"
        fingerprint = evidence_resolution.fingerprint
    else:
        fingerprint = vix_strategy_fingerprint(config) if not config.missing_required_fields() else None
    preflight = validate_vix_preflight(
        request,
        None if require_evidence else config,
        calendar=calendar,
        authorized_override_actor=authorized_override_actor,
        allowed_override_codes=allowed_override_codes,
        require_evidence=require_evidence,
    )
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
            "configuration_source": configuration_source,
            "configuration_fingerprint": fingerprint,
            "strategy_configuration": config.to_json_dict() if not config.missing_required_fields() else None,
            "client_request_id": request.client_request_id,
        },
        lifecycle_state=CycleLifecycleState.PREFLIGHT_VALIDATED if preflight.passed else CycleLifecycleState.PREFLIGHT_BLOCKED,
        risk_policy_result={"passed": preflight.passed, "issues": [asdict(issue) for issue in preflight.issues]},
        next_required_action="review_and_submit_entry" if preflight.passed else "correct_preflight_issues",
    )
    execution.record_event("preflight_completed", "VIX paired-options preflight completed.", payload=preflight.to_json_dict())
    for override in preflight.override_audit:
        execution.record_event("manual_override", override["detail"], actor=str(override["actor"]), payload=override)
    return VixPairedCycle(execution, preflight)


def vix_cycle_store_path() -> Path:
    return data_root() / "vix_trader" / "cycles.jsonl"


ACTIVE_EXPOSURE_STATES = {
    CycleLifecycleState.ENTRY_SUBMITTED.value,
    CycleLifecycleState.ENTRY_PARTIALLY_FILLED.value,
    CycleLifecycleState.ACTIVE.value,
    CycleLifecycleState.FIRST_LEG_EXIT_WORKING.value,
    CycleLifecycleState.FIRST_LEG_EXITED.value,
    CycleLifecycleState.REBALANCE_ELIGIBLE.value,
    CycleLifecycleState.REBALANCE_SUBMITTED.value,
    CycleLifecycleState.REBALANCED.value,
    CycleLifecycleState.EXIT_REQUIRED.value,
    CycleLifecycleState.CLOSING.value,
    CycleLifecycleState.EXIT_CANCELED.value,
    CycleLifecycleState.EXIT_REPLACEMENT_REQUIRED.value,
}


@contextmanager
def _cycle_store_lock(target: Path, *, timeout_seconds: float = 5.0):
    lock_path = target.with_suffix(target.suffix + ".lock")
    deadline = time_module.monotonic() + timeout_seconds
    descriptor: int | None = None
    while descriptor is None:
        try:
            descriptor = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            if time_module.monotonic() >= deadline:
                raise TimeoutError("vix_cycle_store_lock_timeout")
            time_module.sleep(0.01)
    try:
        os.write(descriptor, str(os.getpid()).encode("ascii"))
        yield
    finally:
        os.close(descriptor)
        lock_path.unlink(missing_ok=True)


def append_vix_cycle(cycle: VixPairedCycle, *, path: str | Path | None = None) -> Path:
    target = Path(path) if path is not None else vix_cycle_store_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = cycle.to_json_dict()
    request_id = str((payload.get("strategy_payload") or {}).get("client_request_id") or "").strip()
    if not request_id:
        raise ValueError("client_request_id_required")
    expiration = str((payload.get("strategy_payload") or {}).get("expiration") or "")
    with _cycle_store_lock(target):
        existing = load_vix_cycles(path=target, limit=100_000)
        if any(str((row.get("strategy_payload") or {}).get("client_request_id") or "") == request_id for row in existing):
            raise ValueError("duplicate_client_request_id")
        if payload.get("lifecycle_state") in ACTIVE_EXPOSURE_STATES and any(
            row.get("lifecycle_state") in ACTIVE_EXPOSURE_STATES
            and str((row.get("strategy_payload") or {}).get("expiration") or "") == expiration
            for row in existing
        ):
            raise ValueError("overlapping_active_expiration")
        with target.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, sort_keys=True) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
    return target


def load_vix_cycles(*, path: str | Path | None = None, limit: int = 100) -> list[dict[str, Any]]:
    target = Path(path) if path is not None else vix_cycle_store_path()
    if not target.exists():
        return []
    rows = [json.loads(line) for line in target.read_text(encoding="utf-8").splitlines() if line.strip()]
    return rows[-max(0, limit):]


def vix_strategy_status() -> dict[str, Any]:
    from .vix_evidence import resolve_vix_strategy_config
    from .vix_robinhood_mirror import build_robinhood_mirror_report, paper_vix_operating_config

    ceilings = load_vix_strategy_config()
    resolution = resolve_vix_strategy_config(ceilings=ceilings)
    operating = paper_vix_operating_config()
    config_payload = operating["config"]
    calendar = load_cboe_calendar()
    cycles = load_vix_cycles(limit=20)
    active = [row for row in cycles if row.get("lifecycle_state") in ACTIVE_EXPOSURE_STATES]
    mirror = build_robinhood_mirror_report(limit=50)
    if mirror["open_count"] > 0:
        next_action = "mirror_open_vix_actions_on_robinhood"
    elif operating.get("proven"):
        next_action = "paper_next_vix_entry_for_robinhood_mirror"
    else:
        next_action = "paper_trade_vix_and_review_robinhood_mirror_report"
    return {
        "ok": True,
        "strategy_id": VIX_STRATEGY_ID,
        "name": "VIX Trader",
        "mode": "paper_trading_with_robinhood_reporting",
        "product_intent": "Paper the VIX paired trade in AutoBott; copy the same trade on Robinhood for real money.",
        "broker_execution_supported": False,
        "broker_blocker": "AutoBott does not submit live VIX orders; real-money venue is manual Robinhood mirroring.",
        "profitability_status": operating.get("profitability_status") or resolution.profitability_status,
        "config": config_payload,
        "configuration_complete": True,
        "configuration_source": operating.get("source"),
        "configuration_fingerprint": operating.get("fingerprint"),
        "missing_configuration": [],
        "configuration_valid": not ceilings.validation_errors(),
        "configuration_errors": ceilings.validation_errors(),
        "evidence": resolution.to_json_dict(),
        "operating": operating,
        "operator_ceilings": ceilings.to_json_dict(),
        "next_action": next_action,
        "robinhood_mirror": {
            "open_count": mirror["open_count"],
            "closed_count": mirror["closed_count"],
            "action_count": len(mirror["robinhood_action_queue"]),
            "performance_report": mirror["performance_report"],
        },
        "calendar": {
            "authoritative": calendar.authoritative,
            "source": calendar.source,
            "source_url": calendar.source_url,
            "published_at": calendar.published_at.astimezone(UTC).isoformat() if calendar.published_at else None,
            "coverage_start": calendar.coverage_start.isoformat() if calendar.coverage_start else None,
            "coverage_end": calendar.coverage_end.isoformat() if calendar.coverage_end else None,
        },
        "cycle_count": len(cycles),
        "active_cycle_count": len(active),
        "cycles": list(reversed(cycles)),
        "alpaca_paper_isolated": True,
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
