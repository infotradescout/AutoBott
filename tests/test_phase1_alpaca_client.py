import json
from io import BytesIO
from datetime import datetime, timezone

import pytest

from autobott_v2.phase1_alpaca_client import AlpacaPaperClient, _clear_option_contract_metadata_cache
from autobott_v2.phase1_alpaca_config import AlpacaPaperConfig


@pytest.fixture(autouse=True)
def _clear_contract_cache() -> None:
    _clear_option_contract_metadata_cache()


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


def test_get_option_chain_joins_contract_open_interest(monkeypatch) -> None:
    client = AlpacaPaperClient(_config())
    urls = []
    option_symbol = "VXX260717C00050000"

    def fake_urlopen(request, timeout=30):
        urls.append(request.full_url)
        if "/v2/options/contracts" in request.full_url:
            return _FakeResponse(
                {
                    "option_contracts": [
                        {
                            "symbol": option_symbol,
                            "expiration_date": "2026-07-17",
                            "strike_price": "50",
                            "type": "call",
                            "tradable": True,
                            "open_interest": "6168",
                            "open_interest_date": "2026-07-15",
                        }
                    ]
                }
            )
        return _FakeResponse(
            {
                "snapshots": {
                    option_symbol: {
                        "latestQuote": {"bp": 0.74, "ap": 0.80},
                        "greeks": {"delta": 0.31},
                    }
                }
            }
        )

    monkeypatch.setattr("autobott_v2.phase1_alpaca_client.urllib.request.urlopen", fake_urlopen)

    chain = client.get_option_chain_snapshots("VXX")

    assert chain[option_symbol]["open_interest"] == "6168"
    assert chain[option_symbol]["details"] == {
        "expiration_date": "2026-07-17",
        "strike_price": "50",
        "type": "call",
    }
    assert any("underlying_symbols=VXX" in url for url in urls)
    assert any("/v1beta1/options/snapshots/VXX" in url for url in urls)


def test_get_option_chain_drops_active_but_nontradable_contracts(monkeypatch) -> None:
    client = AlpacaPaperClient(_config())
    tradable = "VXX260717C00050000"
    nontradable = "VXX1260717C00050000"

    def fake_urlopen(request, timeout=30):
        if "/v2/options/contracts" in request.full_url:
            return _FakeResponse(
                {
                    "option_contracts": [
                        {"symbol": tradable, "tradable": True, "open_interest": "500"},
                        {"symbol": nontradable, "tradable": False, "open_interest": "900"},
                    ]
                }
            )
        return _FakeResponse(
            {
                "snapshots": {
                    tradable: {"latestQuote": {"bp": 0.74, "ap": 0.80}},
                    nontradable: {"latestQuote": {"bp": 0.70, "ap": 0.75}},
                }
            }
        )

    monkeypatch.setattr("autobott_v2.phase1_alpaca_client.urllib.request.urlopen", fake_urlopen)

    chain = client.get_option_chain_snapshots("VXX")

    assert set(chain) == {tradable}
    assert chain[tradable]["tradable"] is True


def test_get_latest_option_quotes_uses_multi_quote_endpoint(monkeypatch) -> None:
    client = AlpacaPaperClient(_config())
    option_symbol = "VXX260717C00050000"
    captured = {}

    def fake_urlopen(request, timeout=30):
        captured["url"] = request.full_url
        return _FakeResponse({"quotes": {option_symbol: {"bp": 0.76, "ap": 0.82}}})

    monkeypatch.setattr("autobott_v2.phase1_alpaca_client.urllib.request.urlopen", fake_urlopen)

    quotes = client.get_latest_option_quotes([option_symbol])

    assert quotes[option_symbol]["ap"] == 0.82
    assert "/v1beta1/options/quotes/latest" in captured["url"]
    assert f"symbols={option_symbol}" in captured["url"]


def test_option_contract_metadata_is_cached_across_cycle_clients(monkeypatch) -> None:
    calls = []

    def fake_urlopen(request, timeout=30):
        calls.append(request.full_url)
        return _FakeResponse(
            {
                "option_contracts": [
                    {
                        "symbol": "VXX260717C00050000",
                        "tradable": True,
                        "open_interest": "6168",
                    }
                ]
            }
        )

    monkeypatch.setattr("autobott_v2.phase1_alpaca_client.urllib.request.urlopen", fake_urlopen)

    for _ in range(2):
        AlpacaPaperClient(_config())._get_option_contract_metadata(
            "VXX",
            expiration_date_gte="2026-07-17",
            expiration_date_lte="2026-08-15",
        )

    assert len(calls) == 1
    assert "limit=10000" in calls[0]


def test_option_chain_follows_more_than_ten_pages(monkeypatch) -> None:
    client = AlpacaPaperClient(_config())
    symbols = [f"VXX260717C{index:08d}" for index in range(11)]
    metadata = {symbol: {"symbol": symbol, "tradable": True, "open_interest": "500"} for symbol in symbols}
    monkeypatch.setattr(client, "_get_option_contract_metadata", lambda *args, **kwargs: metadata)
    calls = []

    def fake_get_json(_base_url, _path, params=None):
        index = len(calls)
        calls.append(dict(params or {}))
        payload = {"snapshots": {symbols[index]: {"latestQuote": {"bp": 0.7, "ap": 0.8}}}}
        if index < 10:
            payload["next_page_token"] = f"page-{index + 1}"
        return payload

    monkeypatch.setattr(client, "_get_json_with_retry", fake_get_json)

    chain = client.get_option_chain_snapshots("VXX")

    assert len(calls) == 11
    assert set(chain) == set(symbols)
    assert calls[-1]["page_token"] == "page-10"


def test_get_positions_ignores_non_list_payload(monkeypatch) -> None:
    client = AlpacaPaperClient(_config())

    def fake_urlopen(request, timeout=30):
        return _FakeResponse({"unexpected": "shape"})

    monkeypatch.setattr("autobott_v2.phase1_alpaca_client.urllib.request.urlopen", fake_urlopen)

    assert client.get_positions() == []
