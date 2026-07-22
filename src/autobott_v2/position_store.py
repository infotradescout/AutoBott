from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .execution_models import ExecutionOrder
from .runtime_paths import data_root


def position_store_path() -> Path:
    return data_root() / "execution" / "open_positions.json"


@dataclass(frozen=True)
class OpenPosition:
    broker_order_id: str
    decision_id: str | None
    symbol: str
    option_symbol: str
    quantity: int
    entry_limit_price: float
    entry_submitted_at: datetime | None
    take_profit_price: float | None
    stop_loss_price: float | None
    status: str
    trade_group_id: str | None = None
    leg_role: str | None = None
    paired_option_symbol: str | None = None
    # Persist the policy that opened the position. Exit-time runtime policy is
    # intentionally separate so legacy positions are never relabeled.
    entry_policy_version: str | None = None
    entry_build_sha: str | None = None

    def to_json_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["entry_submitted_at"] = self.entry_submitted_at.astimezone(UTC).isoformat() if self.entry_submitted_at else None
        return payload


def load_open_positions(*, store_path: str | Path | None = None) -> list[OpenPosition]:
    path = Path(store_path) if store_path is not None else position_store_path()
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    positions: list[OpenPosition] = []
    for row in payload:
        submitted_at = row.get("entry_submitted_at")
        positions.append(
            OpenPosition(
                broker_order_id=str(row["broker_order_id"]),
                decision_id=row.get("decision_id"),
                symbol=str(row["symbol"]),
                option_symbol=str(row["option_symbol"]),
                quantity=int(row["quantity"]),
                entry_limit_price=float(row["entry_limit_price"]),
                entry_submitted_at=datetime.fromisoformat(submitted_at.replace("Z", "+00:00")).astimezone(UTC) if submitted_at else None,
                take_profit_price=row.get("take_profit_price"),
                stop_loss_price=row.get("stop_loss_price"),
                status=str(row.get("status", "open")),
                trade_group_id=row.get("trade_group_id"),
                leg_role=row.get("leg_role"),
                paired_option_symbol=row.get("paired_option_symbol"),
                entry_policy_version=row.get("entry_policy_version"),
                entry_build_sha=row.get("entry_build_sha"),
            )
        )
    return positions


def save_open_positions(positions: list[OpenPosition], *, store_path: str | Path | None = None) -> Path:
    path = Path(store_path) if store_path is not None else position_store_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps([item.to_json_dict() for item in positions], indent=2, sort_keys=True), encoding="utf-8")
    return path


def upsert_open_position_from_order(order: ExecutionOrder, *, store_path: str | Path | None = None) -> Path:
    if not order.broker_order_id:
        raise ValueError("broker_order_id_required")
    positions = load_open_positions(store_path=store_path)
    updated = [
        position
        for position in positions
        if not (
            position.broker_order_id == order.broker_order_id
            and position.option_symbol == order.intent.option_symbol
        )
    ]
    updated.append(
        OpenPosition(
            broker_order_id=order.broker_order_id,
            decision_id=order.intent.decision_id,
            symbol=order.intent.symbol,
            option_symbol=order.intent.option_symbol,
            quantity=order.intent.quantity,
            entry_limit_price=order.intent.limit_price,
            entry_submitted_at=order.submitted_at,
            take_profit_price=order.intent.take_profit_price,
            stop_loss_price=order.intent.stop_loss_price,
            status=order.state.value,
            trade_group_id=order.intent.metadata.get("trade_group_id"),
            leg_role=order.intent.metadata.get("leg_role"),
            paired_option_symbol=order.intent.metadata.get("paired_option_symbol"),
            entry_policy_version=order.intent.metadata.get("policy_version"),
            entry_build_sha=order.intent.metadata.get("build_sha"),
        )
    )
    return save_open_positions(updated, store_path=store_path)
