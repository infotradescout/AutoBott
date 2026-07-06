from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from autobott_v2.execution_broker import AlpacaExecutionBroker, _map_alpaca_status
from autobott_v2.execution_config import AlpacaExecutionConfig
from autobott_v2.execution_models import BrokerEnvironment, ExecutionState, OrderSide, TradeIntent


def _config(**overrides) -> AlpacaExecutionConfig:
    base = AlpacaExecutionConfig(
        environment=BrokerEnvironment.PAPER,
        api_key="paper-key",
        secret_key="paper-secret",
        trading_base_url="https://paper-api.alpaca.markets",
        data_base_url="https://data.alpaca.markets",
        allow_live_trading=False,
        allow_order_placement=True,
        max_position_cost=1000.0,
        max_daily_loss=500.0,
        max_open_positions=3,
    )
    values = base.__dict__ | overrides
    return AlpacaExecutionConfig(**values)


def _intent(**overrides) -> TradeIntent:
    base = TradeIntent(
        symbol="AAPL",
        option_symbol="AAPL260117C00190000",
        side=OrderSide.BUY_TO_OPEN,
        quantity=1,
        limit_price=2.5,
        generated_at=datetime(2026, 7, 1, 15, 30, tzinfo=timezone.utc),
        decision_id="decision-123",
        take_profit_price=3.75,
        stop_loss_price=1.75,
    )
    values = base.__dict__ | overrides
    return TradeIntent(**values)


class _FakeResponse:
    def __init__(self, payload: dict) -> None:
        self.payload = payload

    def read(self) -> bytes:
        return json.dumps(self.payload).encode("utf-8")

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


def test_submit_order_returns_submitted_execution_order(monkeypatch) -> None:
    broker = AlpacaExecutionBroker(_config())
    captured = {}

    def _fake_urlopen(request, timeout=30):
        captured["url"] = request.full_url
        captured["method"] = request.get_method()
        captured["body"] = json.loads(request.data.decode("utf-8"))
        return _FakeResponse(
            {
                "id": "alpaca-order-1",
                "status": "accepted",
                "submitted_at": "2026-07-01T15:31:00Z",
            }
        )

    monkeypatch.setattr("urllib.request.urlopen", _fake_urlopen)

    order = broker.submit_order(_intent())

    assert order.state is ExecutionState.SUBMITTED
    assert order.broker_order_id == "alpaca-order-1"
    assert captured["url"] == "https://paper-api.alpaca.markets/v2/orders"
    assert captured["method"] == "POST"
    assert captured["body"]["symbol"] == "AAPL260117C00190000"
    assert "legs" not in captured["body"]
    assert captured["body"]["limit_price"] == "2.50"


def test_map_alpaca_intermediate_order_statuses_as_submitted() -> None:
    assert _map_alpaca_status("pending_new") is ExecutionState.SUBMITTED
    assert _map_alpaca_status("accepted_for_bidding") is ExecutionState.SUBMITTED
    assert _map_alpaca_status("pending_replace") is ExecutionState.SUBMITTED


def test_submit_order_fails_closed_on_risk_rejection() -> None:
    broker = AlpacaExecutionBroker(_config(allow_order_placement=False))

    with pytest.raises(ValueError, match="risk_check_not_approved"):
        broker.submit_order(_intent())


def test_cancel_and_replace_order_use_order_endpoints(monkeypatch) -> None:
    broker = AlpacaExecutionBroker(_config())
    calls = []

    def _fake_urlopen(request, timeout=30):
        calls.append(
            {
                "url": request.full_url,
                "method": request.get_method(),
                "body": json.loads(request.data.decode("utf-8")) if request.data else None,
            }
        )
        if request.get_method() == "DELETE":
            return _FakeResponse({})
        return _FakeResponse({"id": "alpaca-order-1", "status": "new", "limit_price": "2.75"})

    monkeypatch.setattr("urllib.request.urlopen", _fake_urlopen)

    canceled = broker.cancel_order("alpaca-order-1")
    replaced = broker.replace_order("alpaca-order-1", limit_price=2.75)

    assert canceled["status"] == "canceled"
    assert replaced["status"] == "new"
    assert calls[0]["url"].endswith("/v2/orders/alpaca-order-1")
    assert calls[0]["method"] == "DELETE"
    assert calls[1]["method"] == "PATCH"
    assert calls[1]["body"] == {"limit_price": "2.75"}
