import pytest

from autobott_v2.execution_config import load_alpaca_execution_config
from autobott_v2.execution_models import BrokerEnvironment


def test_execution_config_defaults_to_paper(monkeypatch) -> None:
    monkeypatch.setenv("ALPACA_ENV", "paper")
    monkeypatch.setenv("ALPACA_API_KEY_ID", "paper-key")
    monkeypatch.setenv("ALPACA_API_SECRET_KEY", "paper-secret")

    config = load_alpaca_execution_config().validate()

    assert config.environment is BrokerEnvironment.PAPER
    assert config.allow_live_trading is False
    assert config.trading_base_url == "https://paper-api.alpaca.markets"


def test_execution_config_rejects_live_without_explicit_enable(monkeypatch) -> None:
    monkeypatch.setenv("ALPACA_ENV", "live")
    monkeypatch.setenv("ALPACA_API_KEY_ID", "live-key")
    monkeypatch.setenv("ALPACA_API_SECRET_KEY", "live-secret")
    monkeypatch.setenv("ALPACA_TRADING_BASE_URL", "https://api.alpaca.markets")
    monkeypatch.delenv("AUTOBOTT_LIVE_TRADING_ENABLED", raising=False)

    with pytest.raises(ValueError, match="live_trading_disabled"):
        load_alpaca_execution_config().validate()


def test_execution_config_allows_live_only_when_explicitly_enabled(monkeypatch) -> None:
    monkeypatch.setenv("ALPACA_ENV", "live")
    monkeypatch.setenv("ALPACA_API_KEY_ID", "live-key")
    monkeypatch.setenv("ALPACA_API_SECRET_KEY", "live-secret")
    monkeypatch.setenv("ALPACA_TRADING_BASE_URL", "https://api.alpaca.markets")
    monkeypatch.setenv("AUTOBOTT_LIVE_TRADING_ENABLED", "true")
    monkeypatch.setenv("AUTOBOTT_ALLOW_ORDER_PLACEMENT", "true")

    config = load_alpaca_execution_config().validate()

    assert config.environment is BrokerEnvironment.LIVE
    assert config.allow_live_trading is True
    assert config.allow_order_placement is True
