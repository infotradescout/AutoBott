from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from .execution_journal import append_order_submission
from .execution_models import BrokerEnvironment, ExecutionOrder, ExecutionState, OrderSide, TradeIntent
from .position_store import load_open_positions, save_open_positions


class BrokerOrderStatusReader(Protocol):
    def get_order(self, broker_order_id: str) -> dict[str, Any]:
        ...


@dataclass(frozen=True)
class ReconciliationSummary:
    checked: int
    updated: int
    unchanged: int
    missing: int


def reconcile_open_positions(
    broker: BrokerOrderStatusReader,
    *,
    store_path: str | None = None,
    journal_path: str | None = None,
) -> ReconciliationSummary:
    positions = load_open_positions(store_path=store_path)
    updated_positions = []
    updated = 0
    unchanged = 0
    missing = 0

    for position in positions:
        payload = broker.get_order(position.broker_order_id)
        if not payload:
            updated_positions.append(position)
            missing += 1
            continue
        next_state = _map_alpaca_status(payload.get("status"))
        submitted_at = _parse_dt(payload.get("submitted_at")) or position.entry_submitted_at
        next_position = type(position)(
            broker_order_id=position.broker_order_id,
            decision_id=position.decision_id,
            symbol=position.symbol,
            option_symbol=position.option_symbol,
            quantity=position.quantity,
            entry_limit_price=position.entry_limit_price,
            entry_submitted_at=submitted_at,
            take_profit_price=position.take_profit_price,
            stop_loss_price=position.stop_loss_price,
            status=next_state.value,
            trade_group_id=position.trade_group_id,
            leg_role=position.leg_role,
            paired_option_symbol=position.paired_option_symbol,
            entry_policy_version=position.entry_policy_version,
            entry_build_sha=position.entry_build_sha,
        )
        if next_position.status != position.status:
            updated += 1
            append_order_submission(
                ExecutionOrder(
                    order_id=payload.get("client_order_id") or position.broker_order_id,
                    client_order_id=payload.get("client_order_id") or position.broker_order_id,
                    intent=TradeIntent(
                        symbol=position.symbol,
                        option_symbol=position.option_symbol,
                        side=OrderSide.BUY_TO_OPEN,
                        quantity=position.quantity,
                        limit_price=position.entry_limit_price,
                        generated_at=position.entry_submitted_at or datetime.now(tz=UTC),
                        environment=BrokerEnvironment.PAPER,
                        take_profit_price=position.take_profit_price,
                        stop_loss_price=position.stop_loss_price,
                        decision_id=position.decision_id,
                        thesis_id=position.decision_id,
                        metadata={
                            "trade_group_id": position.trade_group_id,
                            "leg_role": position.leg_role,
                            "paired_option_symbol": position.paired_option_symbol,
                            "policy_version": position.entry_policy_version,
                            "build_sha": position.entry_build_sha,
                        },
                    ),
                    state=next_state,
                    submitted_at=submitted_at,
                    broker_order_id=position.broker_order_id,
                ),
                journal_path=journal_path,
            )
        else:
            unchanged += 1
        updated_positions.append(next_position)

    save_open_positions(updated_positions, store_path=store_path)
    return ReconciliationSummary(len(positions), updated, unchanged, missing)


def _map_alpaca_status(status: str | None) -> ExecutionState:
    normalized = (status or "").strip().lower()
    return {
        "new": ExecutionState.SUBMITTED,
        "accepted": ExecutionState.SUBMITTED,
        "partially_filled": ExecutionState.PARTIALLY_FILLED,
        "filled": ExecutionState.FILLED,
        "canceled": ExecutionState.CANCELED,
        "rejected": ExecutionState.REJECTED,
    }.get(normalized, ExecutionState.FAILED)


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
