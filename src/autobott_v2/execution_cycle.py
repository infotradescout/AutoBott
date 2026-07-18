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

    @property
    def confirmed_proceeds(self) -> float:
        if self.purpose not in {"exit", "reduce"} or self.state is not BrokerOrderState.FILLED:
            return 0.0
        return round(float(self.broker_confirmed_fill_price or 0.0) * self.filled_quantity * 100, 2)

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
        self.state = state
        self.filled_quantity = filled_quantity
        self.broker_confirmed_fill_price = fill_price
        self.broker_order_id = broker_order_id or self.broker_order_id
        self.confirmed_at = confirmed_at or datetime.now(UTC)

    def to_json_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["state"] = self.state.value
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
    realized_proceeds: float = 0.0
    current_open_value: float = 0.0
    capital_committed: float = 0.0
    risk_policy_result: dict[str, Any] = field(default_factory=dict)
    next_required_action: str = "run_preflight"
    audit_events: list[AuditEvent] = field(default_factory=list)

    def record_event(self, event_type: str, detail: str, *, actor: str = "system", payload: dict[str, Any] | None = None) -> None:
        self.audit_events.append(AuditEvent(event_type, datetime.now(UTC), actor, detail, payload or {}))

    def recalculate_confirmed_proceeds(self) -> float:
        self.realized_proceeds = round(sum(order.confirmed_proceeds for order in self.orders), 2)
        return self.realized_proceeds

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
