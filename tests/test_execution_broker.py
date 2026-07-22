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


class _FakeClock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


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
    assert captured["body"]["client_order_id"] == order.client_order_id
    assert "legs" not in captured["body"]
    assert captured["body"]["limit_price"] == "2.50"
    assert captured["body"]["position_intent"] == "buy_to_open"


def test_submit_order_reconciles_ambiguous_post_timeout(monkeypatch) -> None:
    broker = AlpacaExecutionBroker(_config())
    monkeypatch.setattr(
        broker,
        "_submit_alpaca_order",
        lambda *args, **kwargs: (_ for _ in ()).throw(TimeoutError("post timed out")),
    )
    monkeypatch.setattr(
        broker,
        "_get_order_by_client_order_id",
        lambda client_order_id: {
            "id": "alpaca-reconciled",
            "client_order_id": client_order_id,
            "status": "filled",
            "submitted_at": "2026-07-01T15:31:00Z",
        },
    )

    order = broker.submit_order(_intent())

    assert order.state is ExecutionState.FILLED
    assert order.broker_order_id == "alpaca-reconciled"
    assert order.client_order_id.startswith("autobott-")


def test_hosted_paper_paces_burst_order_mutations(monkeypatch) -> None:
    monkeypatch.setenv("RENDER", "true")
    clock = _FakeClock()
    monkeypatch.setattr("autobott_v2.execution_broker.time.monotonic", clock.monotonic)
    monkeypatch.setattr("autobott_v2.execution_broker.time.sleep", clock.sleep)
    broker = AlpacaExecutionBroker(_config())
    post_times: list[float] = []

    def _fake_urlopen(request, timeout=30):
        post_times.append(clock.now)
        body = json.loads(request.data.decode("utf-8"))
        return _FakeResponse(
            {
                "id": f"alpaca-order-{len(post_times)}",
                "client_order_id": body["client_order_id"],
                "status": "accepted",
                "submitted_at": "2026-07-01T15:31:00Z",
            }
        )

    monkeypatch.setattr("urllib.request.urlopen", _fake_urlopen)

    broker.submit_order(_intent())
    broker.submit_order(_intent(option_symbol="MSFT260117C00400000"))

    assert post_times == [0.0, 0.75]
    assert clock.sleeps == [0.75]


def test_hosted_paper_retries_429_with_same_client_id_after_reconciliation(monkeypatch) -> None:
    monkeypatch.setenv("RENDER", "true")
    clock = _FakeClock()
    monkeypatch.setattr("autobott_v2.execution_broker.time.monotonic", clock.monotonic)
    monkeypatch.setattr("autobott_v2.execution_broker.time.sleep", clock.sleep)
    broker = AlpacaExecutionBroker(_config())
    submitted_client_ids: list[str] = []
    reconciliation_attempts = 0

    def _fake_request_once(method, path, *, payload=None):
        assert method == "POST"
        submitted_client_ids.append(payload["client_order_id"])
        if len(submitted_client_ids) == 1:
            raise ValueError("alpaca_http_429: rate limit exceeded")
        raise ValueError("alpaca_http_422: client_order_id must be unique")

    def _fake_reconcile(client_order_id):
        nonlocal reconciliation_attempts
        reconciliation_attempts += 1
        if reconciliation_attempts == 1:
            raise RuntimeError("order_not_found")
        return {
            "id": "alpaca-late-order",
            "client_order_id": client_order_id,
            "status": "accepted",
            "submitted_at": "2026-07-01T15:31:00Z",
        }

    monkeypatch.setattr(broker, "_request_json_once", _fake_request_once)
    monkeypatch.setattr(broker, "_get_order_by_client_order_id", _fake_reconcile)

    order = broker.submit_order(_intent())

    assert submitted_client_ids[0] == submitted_client_ids[1]
    assert reconciliation_attempts == 2
    assert order.broker_order_id == "alpaca-late-order"
    assert order.state is ExecutionState.SUBMITTED
    assert clock.sleeps == [1.0]


def test_list_order_history_pages_until_complete(monkeypatch) -> None:
    broker = AlpacaExecutionBroker(_config())
    first = [
        {"id": f"order-{index}", "submitted_at": f"2026-07-22T15:{index % 60:02d}:00Z"}
        for index in range(500)
    ]
    second = [{"id": "old-order", "submitted_at": "2026-07-21T15:00:00Z"}]
    pages = [first, second]
    monkeypatch.setattr(broker, "list_orders", lambda **kwargs: pages.pop(0))

    rows = broker.list_order_history()

    assert len(rows) == 501
    assert rows[-1]["id"] == "old-order"


def test_submit_mleg_order_sends_one_atomic_two_leg_request(monkeypatch) -> None:
    broker = AlpacaExecutionBroker(_config())
    captured = {}
    primary = _intent(
        option_symbol="AAPL260117C00195000",
        limit_price=0.70,
        take_profit_price=1.05,
        stop_loss_price=0.38,
    )
    runner = _intent(
        option_symbol="AAPL260117C00200000",
        limit_price=0.25,
        take_profit_price=0.50,
        stop_loss_price=0.08,
    )

    def _fake_urlopen(request, timeout=30):
        captured["url"] = request.full_url
        captured["method"] = request.get_method()
        captured["body"] = json.loads(request.data.decode("utf-8"))
        return _FakeResponse(
            {
                "id": "alpaca-mleg-1",
                "status": "accepted",
                "submitted_at": "2026-07-01T15:31:00Z",
                "legs": [
                    {"id": "alpaca-leg-primary", "symbol": primary.option_symbol, "status": "accepted"},
                    {"id": "alpaca-leg-runner", "symbol": runner.option_symbol, "status": "accepted"},
                ],
            }
        )

    monkeypatch.setattr("urllib.request.urlopen", _fake_urlopen)

    orders = broker.submit_mleg_order((primary, runner))

    assert [order.broker_order_id for order in orders] == ["alpaca-leg-primary", "alpaca-leg-runner"]
    assert captured["url"] == "https://paper-api.alpaca.markets/v2/orders"
    assert captured["method"] == "POST"
    assert captured["body"]["order_class"] == "mleg"
    assert captured["body"]["qty"] == "1"
    assert captured["body"]["limit_price"] == "0.95"
    assert "symbol" not in captured["body"]
    assert "side" not in captured["body"]
    assert captured["body"]["legs"] == [
        {
            "symbol": primary.option_symbol,
            "ratio_qty": "1",
            "side": "buy",
            "position_intent": "buy_to_open",
        },
        {
            "symbol": runner.option_symbol,
            "ratio_qty": "1",
            "side": "buy",
            "position_intent": "buy_to_open",
        },
    ]


def test_list_orders_can_request_nested_mleg_parents(monkeypatch) -> None:
    broker = AlpacaExecutionBroker(_config())
    captured = {}

    def _fake_urlopen(request, timeout=30):
        captured["url"] = request.full_url
        return _FakeResponse([])

    monkeypatch.setattr("urllib.request.urlopen", _fake_urlopen)

    assert broker.list_orders(status="open", nested=True) == []
    assert "nested=true" in captured["url"]


def test_submit_sell_to_close_marks_position_intent(monkeypatch) -> None:
    broker = AlpacaExecutionBroker(_config())
    captured = {}

    def _fake_urlopen(request, timeout=30):
        captured["body"] = json.loads(request.data.decode("utf-8"))
        return _FakeResponse(
            {
                "id": "alpaca-order-1",
                "status": "accepted",
                "submitted_at": "2026-07-01T15:31:00Z",
            }
        )

    monkeypatch.setattr("urllib.request.urlopen", _fake_urlopen)

    broker.submit_order(_intent(side=OrderSide.SELL_TO_CLOSE))

    assert captured["body"]["side"] == "sell"
    assert captured["body"]["position_intent"] == "sell_to_close"


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


def test_list_orders_uses_open_orders_endpoint(monkeypatch) -> None:
    broker = AlpacaExecutionBroker(_config())
    captured = {}

    def _fake_urlopen(request, timeout=30):
        captured["url"] = request.full_url
        captured["method"] = request.get_method()
        return _FakeResponse([{"id": "alpaca-order-1", "symbol": "AAPL260117C00190000"}])

    monkeypatch.setattr("urllib.request.urlopen", _fake_urlopen)

    orders = broker.list_orders(status="open", limit=25, direction="asc")

    assert orders == [{"id": "alpaca-order-1", "symbol": "AAPL260117C00190000"}]
    assert captured["method"] == "GET"
    assert captured["url"] == "https://paper-api.alpaca.markets/v2/orders?status=open&limit=25&direction=asc&nested=false"
