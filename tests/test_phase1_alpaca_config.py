import pytest

from autobott_v2.phase1_alpaca_config import load_alpaca_paper_config


def test_alpaca_config_rejects_non_paper_env(monkeypatch) -> None:
    monkeypatch.setenv("ALPACA_ENV", "live")
    monkeypatch.setenv("ALPACA_API_KEY_ID", "paper-key")
    monkeypatch.setenv("ALPACA_API_SECRET_KEY", "paper-secret")
    monkeypatch.setenv("ALPACA_TRADING_BASE_URL", "https://paper-api.alpaca.markets")
    monkeypatch.setenv("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets")

    with pytest.raises(ValueError, match="alpaca_env_not_paper"):
        load_alpaca_paper_config().validate()


def test_alpaca_config_rejects_live_url(monkeypatch) -> None:
    monkeypatch.setenv("ALPACA_ENV", "paper")
    monkeypatch.setenv("ALPACA_API_KEY_ID", "paper-key")
    monkeypatch.setenv("ALPACA_API_SECRET_KEY", "paper-secret")
    monkeypatch.setenv("ALPACA_TRADING_BASE_URL", "https://api.alpaca.markets")
    monkeypatch.setenv("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets")

    with pytest.raises(ValueError, match="alpaca_trading_base_url_not_paper"):
        load_alpaca_paper_config().validate()
