from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any
from uuid import uuid4


class CycleLifecycleState(str, Enum):
    DRAFT = "DRAFT"
    PREFLIGHT_REQUIRED = "PREFLIGHT_REQUIRED"
    PREFLIGHT_VALIDATED = "PREFLIGHT_VALIDATED"
    ENTRY_READY = "ENTRY_READY"
    ENTRY_SUBMITTED = "ENTRY_SUBMITTED"
    ENTRY_PARTIALLY_FILLED = "ENTRY_PARTIALLY_FILLED"
    ACTIVE = "ACTIVE"
    FIRST_LEG_EXIT_WORKING = "FIRST_LEG_EXIT_WORKING"
    FIRST_LEG_EXITED = "FIRST_LEG_EXITED"
    REBALANCE_ELIGIBLE = "REBALANCE_ELIGIBLE"
    REBALANCE_SUBMITTED = "REBALANCE_SUBMITTED"
    REBALANCED = "REBALANCED"
    EXIT_REQUIRED = "EXIT_REQUIRED"
    CLOSING = "CLOSING"
    CLOSED = "CLOSED"
    RECONCILED = "RECONCILED"
    PREFLIGHT_BLOCKED = "PREFLIGHT_BLOCKED"
    ORDER_REJECTED = "ORDER_REJECTED"
    EXIT_CANCELED = "EXIT_CANCELED"
    EXIT_REPLACEMENT_REQUIRED = "EXIT_REPLACEMENT_REQUIRED"
    SESSION_BLOCKED = "SESSION_BLOCKED"
    EXPIRATION_BLOCKED = "EXPIRATION_BLOCKED"
    RISK_BLOCKED = "RISK_BLOCKED"
    MANUAL_REVIEW_REQUIRED = "MANUAL_REVIEW_REQUIRED"
    RECONCILIATION_MISMATCH = "RECONCILIATION_MISMATCH"


class BrokerOrderState(str, Enum):
    CREATED = "CREATED"
    SUBMITTED = "SUBMITTED"
    ACCEPTED = "ACCEPTED"
    WORKING = "WORKING"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCEL_REQUESTED = "CANCEL_REQUESTED"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    REPLACED = "REPLACED"


ALLOWED_CYCLE_TRANSITIONS: dict[CycleLifecycleState, set[CycleLifecycleState]] = {
    CycleLifecycleState.DRAFT: {CycleLifecycleState.PREFLIGHT_REQUIRED},
    CycleLifecycleState.PREFLIGHT_REQUIRED: {
        CycleLifecycleState.PREFLIGHT_VALIDATED,
        CycleLifecycleState.PREFLIGHT_BLOCKED,
        CycleLifecycleState.SESSION_BLOCKED,
        CycleLifecycleState.EXPIRATION_BLOCKED,
        CycleLifecycleState.RISK_BLOCKED,
    },
    CycleLifecycleState.PREFLIGHT_VALIDATED: {CycleLifecycleState.ENTRY_READY, CycleLifecycleState.MANUAL_REVIEW_REQUIRED},
    CycleLifecycleState.ENTRY_READY: {CycleLifecycleState.ENTRY_SUBMITTED, CycleLifecycleState.RISK_BLOCKED},
    CycleLifecycleState.ENTRY_SUBMITTED: {
        CycleLifecycleState.ENTRY_PARTIALLY_FILLED,
        CycleLifecycleState.ACTIVE,
        CycleLifecycleState.ORDER_REJECTED,
        CycleLifecycleState.MANUAL_REVIEW_REQUIRED,
    },
    CycleLifecycleState.ENTRY_PARTIALLY_FILLED: {
        CycleLifecycleState.ACTIVE,
        CycleLifecycleState.ORDER_REJECTED,
        CycleLifecycleState.MANUAL_REVIEW_REQUIRED,
    },
    CycleLifecycleState.ACTIVE: {CycleLifecycleState.FIRST_LEG_EXIT_WORKING, CycleLifecycleState.EXIT_REQUIRED},
    CycleLifecycleState.FIRST_LEG_EXIT_WORKING: {
        CycleLifecycleState.FIRST_LEG_EXITED,
        CycleLifecycleState.EXIT_CANCELED,
        CycleLifecycleState.EXIT_REPLACEMENT_REQUIRED,
        CycleLifecycleState.ORDER_REJECTED,
    },
    CycleLifecycleState.EXIT_CANCELED: {CycleLifecycleState.EXIT_REPLACEMENT_REQUIRED, CycleLifecycleState.MANUAL_REVIEW_REQUIRED},
    CycleLifecycleState.EXIT_REPLACEMENT_REQUIRED: {CycleLifecycleState.FIRST_LEG_EXIT_WORKING, CycleLifecycleState.MANUAL_REVIEW_REQUIRED},
    CycleLifecycleState.FIRST_LEG_EXITED: {CycleLifecycleState.REBALANCE_ELIGIBLE, CycleLifecycleState.EXIT_REQUIRED},
    CycleLifecycleState.REBALANCE_ELIGIBLE: {CycleLifecycleState.REBALANCE_SUBMITTED, CycleLifecycleState.EXIT_REQUIRED},
    CycleLifecycleState.REBALANCE_SUBMITTED: {CycleLifecycleState.REBALANCED, CycleLifecycleState.ORDER_REJECTED},
    CycleLifecycleState.REBALANCED: {CycleLifecycleState.EXIT_REQUIRED, CycleLifecycleState.CLOSING},
    CycleLifecycleState.EXIT_REQUIRED: {CycleLifecycleState.CLOSING, CycleLifecycleState.MANUAL_REVIEW_REQUIRED},
    CycleLifecycleState.CLOSING: {CycleLifecycleState.CLOSED, CycleLifecycleState.EXIT_REPLACEMENT_REQUIRED},
    CycleLifecycleState.CLOSED: {CycleLifecycleState.RECONCILED, CycleLifecycleState.RECONCILIATION_MISMATCH},
    CycleLifecycleState.RECONCILIATION_MISMATCH: {CycleLifecycleState.RECONCILED, CycleLifecycleState.MANUAL_REVIEW_REQUIRED},
}


@dataclass(frozen=True)
class AuditEvent:
    event_type: str
    timestamp: datetime
    actor: str
    detail: str
    payload: dict[str, Any] = field(default_factory=dict)

    def to_json_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["timestamp"] = self.timestamp.astimezone(UTC).isoformat()
        return payload


@dataclass(frozen=True)
class BrokerFill:
    fill_id: str
    quantity: int
    price: float
    timestamp: datetime

    def __post_init__(self) -> None:
        if self.quantity <= 0:
            raise ValueError("fill_quantity_must_be_positive")
        if self.price <= 0:
            raise ValueError("fill_price_must_be_positive")

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "fill_id": self.fill_id,
            "quantity": self.quantity,
            "price": self.price,
            "timestamp": self.timestamp.astimezone(UTC).isoformat(),
        }


@dataclass
class ManagedOrder:
    order_id: str
    leg_id: str
    purpose: str
    state: BrokerOrderState = BrokerOrderState.CREATED
    intended_price: float | None = None
    submitted_limit: float | None = None
    broker_confirmed_fill_price: float | None = None
    requested_quantity: int = 0
    filled_quantity: int = 0
    broker_order_id: str | None = None
    replaced_by_order_id: str | None = None
    submitted_at: datetime | None = None
    confirmed_at: datetime | None = None
    fills: list[BrokerFill] = field(default_factory=list)

    @property
    def confirmed_quantity(self) -> int:
        return sum(fill.quantity for fill in self.fills)

    @property
    def confirmed_value(self) -> float:
        return round(sum(fill.quantity * fill.price * 100 for fill in self.fills), 2)

    @property
    def confirmed_proceeds(self) -> float:
        if self.purpose not in {"exit", "reduce"}:
            return 0.0
        return self.confirmed_value

    @property
    def confirmed_cost(self) -> float:
        if self.purpose not in {"entry", "addition", "rebalance"}:
            return 0.0
        return self.confirmed_value

    def apply_broker_update(
        self,
        *,
        state: BrokerOrderState,
        filled_quantity: int = 0,
        fill_price: float | None = None,
        broker_order_id: str | None = None,
        confirmed_at: datetime | None = None,
    ) -> None:
        if filled_quantity < self.filled_quantity:
            raise ValueError("filled_quantity_cannot_decrease")
        if state is BrokerOrderState.FILLED and (filled_quantity <= 0 or fill_price is None):
            raise ValueError("filled_order_requires_broker_fill")
        if state is BrokerOrderState.PARTIALLY_FILLED and filled_quantity <= 0:
            raise ValueError("partial_fill_requires_positive_quantity")
        incremental_quantity = filled_quantity - self.confirmed_quantity
        if incremental_quantity > 0:
            if fill_price is None or fill_price <= 0:
                raise ValueError("new_fill_requires_positive_price")
            self.fills.append(
                BrokerFill(
                    fill_id=f"{broker_order_id or self.broker_order_id or self.order_id}:{filled_quantity}",
                    quantity=incremental_quantity,
                    price=fill_price,
                    timestamp=confirmed_at or datetime.now(UTC),
                )
            )
        self.state = state
        self.filled_quantity = self.confirmed_quantity
        if fill_price is not None:
            self.broker_confirmed_fill_price = fill_price
        self.broker_order_id = broker_order_id or self.broker_order_id
        self.confirmed_at = confirmed_at or datetime.now(UTC)

    def to_json_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["state"] = self.state.value
        payload["fills"] = [fill.to_json_dict() for fill in self.fills]
        for key in ("submitted_at", "confirmed_at"):
            value = getattr(self, key)
            payload[key] = value.astimezone(UTC).isoformat() if value else None
        payload["confirmed_proceeds"] = self.confirmed_proceeds
        return payload


@dataclass
class ExecutionCycle:
    strategy_id: str
    account_id: str
    intent_timestamp: datetime
    entry_window_start: datetime
    entry_window_end: datetime
    maximum_approved_exposure: float
    exit_deadline: datetime
    strategy_payload: dict[str, Any]
    cycle_id: str = field(default_factory=lambda: str(uuid4()))
    lifecycle_state: CycleLifecycleState = CycleLifecycleState.DRAFT
    orders: list[ManagedOrder] = field(default_factory=list)
    current_market_estimates: dict[str, float] = field(default_factory=dict)
    risk_policy_result: dict[str, Any] = field(default_factory=dict)
    next_required_action: str = "run_preflight"
    audit_events: list[AuditEvent] = field(default_factory=list)

    def record_event(self, event_type: str, detail: str, *, actor: str = "system", payload: dict[str, Any] | None = None) -> None:
        self.audit_events.append(AuditEvent(event_type, datetime.now(UTC), actor, detail, payload or {}))

    @property
    def realized_proceeds(self) -> float:
        return round(sum(order.confirmed_proceeds for order in self.orders), 2)

    @property
    def capital_committed(self) -> float:
        return round(sum(order.confirmed_cost for order in self.orders), 2)

    @property
    def current_open_value(self) -> float:
        entry_qty: dict[str, int] = {}
        exit_qty: dict[str, int] = {}
        for order in self.orders:
            if order.purpose in {"entry", "addition", "rebalance"}:
                entry_qty[order.leg_id] = entry_qty.get(order.leg_id, 0) + order.confirmed_quantity
            elif order.purpose in {"exit", "reduce"}:
                exit_qty[order.leg_id] = exit_qty.get(order.leg_id, 0) + order.confirmed_quantity
        return round(
            sum(max(0, quantity - exit_qty.get(leg_id, 0)) * float(self.current_market_estimates.get(leg_id, 0.0)) * 100 for leg_id, quantity in entry_qty.items()),
            2,
        )

    def transition(self, target: CycleLifecycleState, *, actor: str, reason: str) -> None:
        allowed = ALLOWED_CYCLE_TRANSITIONS.get(self.lifecycle_state, set())
        if target not in allowed:
            raise ValueError(f"invalid_cycle_transition:{self.lifecycle_state.value}->{target.value}")
        previous = self.lifecycle_state
        self.lifecycle_state = target
        self.record_event(
            "lifecycle_transition",
            reason,
            actor=actor,
            payload={"from": previous.value, "to": target.value},
        )

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "cycle_id": self.cycle_id,
            "strategy_id": self.strategy_id,
            "account_id": self.account_id,
            "lifecycle_state": self.lifecycle_state.value,
            "intent_timestamp": self.intent_timestamp.astimezone(UTC).isoformat(),
            "entry_window": {
                "start": self.entry_window_start.astimezone(UTC).isoformat(),
                "end": self.entry_window_end.astimezone(UTC).isoformat(),
            },
            "orders": [order.to_json_dict() for order in self.orders],
            "realized_proceeds": self.realized_proceeds,
            "current_open_value": self.current_open_value,
            "capital_committed": self.capital_committed,
            "maximum_approved_exposure": self.maximum_approved_exposure,
            "risk_policy_result": self.risk_policy_result,
            "next_required_action": self.next_required_action,
            "exit_deadline": self.exit_deadline.astimezone(UTC).isoformat(),
            "strategy_payload": self.strategy_payload,
            "audit_events": [event.to_json_dict() for event in self.audit_events],
        }
