from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from typing import Protocol

from .execution_config import AlpacaExecutionConfig, require_alpaca_execution_config
from .execution_models import (
    ExecutionOrder,
    ExecutionState,
    OrderSide,
    OrderType,
    TradeIntent,
    build_execution_order,
    validate_trade_intent,
)


class BrokerAdapter(Protocol):
    def submit_order(self, intent: TradeIntent, *, current_daily_realized_pnl: float = 0.0, open_positions: int = 0) -> ExecutionOrder:
        ...

    def get_order(self, broker_order_id: str) -> dict:
        ...

    def cancel_order(self, broker_order_id: str) -> dict:
        ...

    def replace_order(self, broker_order_id: str, *, limit_price: float) -> dict:
        ...

    def list_orders(self, *, status: str = "open", limit: int = 100, direction: str = "desc") -> list[dict]:
        ...


class AlpacaExecutionBroker:
    def __init__(self, config: AlpacaExecutionConfig | None = None) -> None:
        self.config = (config or require_alpaca_execution_config()).validate()

    def submit_order(
        self,
        intent: TradeIntent,
        *,
        current_daily_realized_pnl: float = 0.0,
        open_positions: int = 0,
    ) -> ExecutionOrder:
        risk_check = validate_trade_intent(
            intent,
            self.config.risk_controls(),
            current_daily_realized_pnl=current_daily_realized_pnl,
            open_positions=open_positions,
        )
        order = build_execution_order(intent, risk_check)

        payload = self._submit_alpaca_order(order.intent)
        return ExecutionOrder(
            order_id=order.order_id,
            client_order_id=order.client_order_id,
            intent=order.intent,
            state=_map_alpaca_status(payload.get("status")),
            submitted_at=_parse_dt(payload.get("submitted_at")),
            broker_order_id=payload.get("id"),
        )

    def _submit_alpaca_order(self, intent: TradeIntent) -> dict:
        side = "buy" if intent.side is OrderSide.BUY_TO_OPEN else "sell"
        request_payload = {
            "symbol": intent.option_symbol,
            "qty": str(intent.quantity),
            "side": side,
            "type": "limit" if intent.order_type is OrderType.LIMIT else "market",
            "time_in_force": "day",
        }
        if intent.order_type is OrderType.LIMIT:
            request_payload["limit_price"] = f"{intent.limit_price:.2f}"

        return self._request_json("POST", "/v2/orders", payload=request_payload)

    def get_order(self, broker_order_id: str) -> dict:
        return self._request_json("GET", f"/v2/orders/{broker_order_id}")

    def cancel_order(self, broker_order_id: str) -> dict:
        response = self._request_json("DELETE", f"/v2/orders/{broker_order_id}")
        if response:
            return response
        return {"id": broker_order_id, "status": "canceled"}

    def replace_order(self, broker_order_id: str, *, limit_price: float) -> dict:
        if limit_price <= 0:
            raise ValueError("limit_price_must_be_positive")
        return self._request_json(
            "PATCH",
            f"/v2/orders/{broker_order_id}",
            payload={"limit_price": f"{limit_price:.2f}"},
        )

    def list_open_positions(self) -> list[dict]:
        payload = self._request_json("GET", "/v2/positions")
        return payload if isinstance(payload, list) else []

    def list_orders(self, *, status: str = "open", limit: int = 100, direction: str = "desc") -> list[dict]:
        query = urllib.parse.urlencode(
            {
                "status": status,
                "limit": str(limit),
                "direction": direction,
                "nested": "false",
            }
        )
        payload = self._request_json("GET", f"/v2/orders?{query}")
        return payload if isinstance(payload, list) else []

    def _request_json(self, method: str, path: str, *, payload: dict | None = None) -> dict | list:
        headers = {
            "APCA-API-KEY-ID": str(self.config.api_key),
            "APCA-API-SECRET-KEY": str(self.config.secret_key),
            "Accept": "application/json",
        }
        data = None
        if payload is not None:
            headers["Content-Type"] = "application/json"
            data = json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(
            f"{self.config.trading_base_url}{path}",
            data=data,
            headers=headers,
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                body = response.read().decode("utf-8").strip()
                return json.loads(body) if body else {}
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace").strip()
            detail = body
            try:
                parsed = json.loads(body) if body else {}
                detail = parsed.get("message") or parsed
            except json.JSONDecodeError:
                pass
            raise ValueError(f"alpaca_http_{exc.code}: {detail}") from exc


def _map_alpaca_status(status: str | None) -> ExecutionState:
    normalized = (status or "").strip().lower()
    return {
        "new": ExecutionState.SUBMITTED,
        "accepted": ExecutionState.SUBMITTED,
        "accepted_for_bidding": ExecutionState.SUBMITTED,
        "pending_new": ExecutionState.SUBMITTED,
        "pending_replace": ExecutionState.SUBMITTED,
        "pending_cancel": ExecutionState.SUBMITTED,
        "stopped": ExecutionState.SUBMITTED,
        "calculated": ExecutionState.SUBMITTED,
        "partially_filled": ExecutionState.PARTIALLY_FILLED,
        "filled": ExecutionState.FILLED,
        "canceled": ExecutionState.CANCELED,
        "expired": ExecutionState.CANCELED,
        "replaced": ExecutionState.CANCELED,
        "rejected": ExecutionState.REJECTED,
        "suspended": ExecutionState.REJECTED,
    }.get(normalized, ExecutionState.FAILED)


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
