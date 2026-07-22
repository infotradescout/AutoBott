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


def test_alpaca_config_allows_order_placement_flag_in_paper_mode(monkeypatch) -> None:
    monkeypatch.setenv("ALPACA_ENV", "paper")
    monkeypatch.setenv("ALPACA_API_KEY_ID", "paper-key")
    monkeypatch.setenv("ALPACA_API_SECRET_KEY", "paper-secret")
    monkeypatch.setenv("ALPACA_TRADING_BASE_URL", "https://paper-api.alpaca.markets")
    monkeypatch.setenv("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets")
    monkeypatch.setenv("AUTOBOTT_ALLOW_ORDER_PLACEMENT", "true")

    config = load_alpaca_paper_config().validate()

    assert config.allow_order_placement is True


def test_hosted_alpaca_config_forces_paper_despite_poisoned_render_env(monkeypatch) -> None:
    monkeypatch.setenv("RENDER", "true")
    monkeypatch.setenv("ALPACA_ENV", "live")
    monkeypatch.setenv("ALPACA_API_KEY_ID", "retained-key")
    monkeypatch.setenv("ALPACA_API_SECRET_KEY", "retained-secret")
    monkeypatch.setenv("ALPACA_TRADING_BASE_URL", "https://api.alpaca.markets")
    monkeypatch.setenv("ALPACA_DATA_BASE_URL", "https://stale.invalid")
    monkeypatch.setenv("AUTOBOTT_LIVE_TRADING_ENABLED", "true")
    monkeypatch.setenv("AUTOBOTT_PAPER_ONLY", "false")
    monkeypatch.setenv("AUTOBOTT_ALLOW_ORDER_PLACEMENT", "false")

    config = load_alpaca_paper_config().validate()

    assert config.env == "paper"
    assert config.api_key == "retained-key"
    assert config.secret_key == "retained-secret"
    assert config.trading_base_url == "https://paper-api.alpaca.markets"
    assert config.data_base_url == "https://data.alpaca.markets"
    assert config.live_trading_enabled is False
    assert config.paper_only is True
    assert config.allow_order_placement is True
