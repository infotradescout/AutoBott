import json
from io import BytesIO
from datetime import datetime, timezone

from autobott_v2.phase1_alpaca_client import AlpacaPaperClient
from autobott_v2.phase1_alpaca_config import AlpacaPaperConfig


def _config() -> AlpacaPaperConfig:
    return AlpacaPaperConfig(
        env="paper",
        api_key="paper-key",
        secret_key="paper-secret",
        trading_base_url="https://paper-api.alpaca.markets",
        data_base_url="https://data.alpaca.markets",
        live_trading_enabled=False,
        paper_only=True,
        allow_order_placement=True,
    )


class _FakeResponse:
    def __init__(self, payload) -> None:
        self._body = json.dumps(payload).encode("utf-8")

    def read(self) -> bytes:
        return self._body

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, *exc_info) -> None:
        return None


def test_get_positions_returns_list(monkeypatch) -> None:
    client = AlpacaPaperClient(_config())
    captured = {}

    def fake_urlopen(request, timeout=30):
        captured["url"] = request.full_url
        return _FakeResponse(
            [{"symbol": "SPY260703C00600000", "qty": "1", "unrealized_pl": "50.00"}]
        )

    monkeypatch.setattr("autobott_v2.phase1_alpaca_client.urllib.request.urlopen", fake_urlopen)

    positions = client.get_positions()

    assert positions == [{"symbol": "SPY260703C00600000", "qty": "1", "unrealized_pl": "50.00"}]
    assert captured["url"].endswith("/v2/positions")


def test_get_orders_passes_status_and_limit(monkeypatch) -> None:
    client = AlpacaPaperClient(_config())
    captured = {}

    def fake_urlopen(request, timeout=30):
        captured["url"] = request.full_url
        return _FakeResponse([{"symbol": "AAPL", "status": "filled"}])

    monkeypatch.setattr("autobott_v2.phase1_alpaca_client.urllib.request.urlopen", fake_urlopen)

    orders = client.get_orders(status="all", limit=10)

    assert orders == [{"symbol": "AAPL", "status": "filled"}]
    assert "status=all" in captured["url"]
    assert "limit=10" in captured["url"]


def test_get_stock_bars_follows_pagination(monkeypatch) -> None:
    client = AlpacaPaperClient(_config())
    urls = []
    responses = [
        {"bars": {"AAPL": [{"t": "2024-06-03T13:30:00Z", "c": 190.0}]}, "next_page_token": "next-token"},
        {"bars": {"SPY": [{"t": "2024-06-03T13:30:00Z", "c": 520.0}]}},
    ]

    def fake_urlopen(request, timeout=30):
        urls.append(request.full_url)
        return _FakeResponse(responses[len(urls) - 1])

    monkeypatch.setattr("autobott_v2.phase1_alpaca_client.urllib.request.urlopen", fake_urlopen)

    bars = client.get_stock_bars(
        ["AAPL", "SPY"],
        start=datetime(2024, 6, 3, tzinfo=timezone.utc),
        end=datetime(2024, 6, 4, tzinfo=timezone.utc),
        timeframe="15Min",
        limit=1000,
    )

    assert bars["AAPL"][0]["c"] == 190.0
    assert bars["SPY"][0]["c"] == 520.0
    assert "page_token=next-token" in urls[1]


def test_get_positions_ignores_non_list_payload(monkeypatch) -> None:
    client = AlpacaPaperClient(_config())

    def fake_urlopen(request, timeout=30):
        return _FakeResponse({"unexpected": "shape"})

    monkeypatch.setattr("autobott_v2.phase1_alpaca_client.urllib.request.urlopen", fake_urlopen)

    assert client.get_positions() == []
