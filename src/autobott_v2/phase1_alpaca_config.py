from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any


def _normalize_bool(value: str | None, *, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class AlpacaPaperConfig:
    env: str
    api_key: str | None
    secret_key: str | None
    trading_base_url: str
    data_base_url: str
    live_trading_enabled: bool
    paper_only: bool
    allow_order_placement: bool

    def validate(self) -> "AlpacaPaperConfig":
        if self.env != "paper":
            raise ValueError("alpaca_env_not_paper")
        if "paper-api.alpaca.markets" not in self.trading_base_url.lower():
            raise ValueError("alpaca_trading_base_url_not_paper")
        if self.live_trading_enabled:
            raise ValueError("autobott_live_trading_must_be_disabled")
        if not self.paper_only:
            raise ValueError("autobott_paper_only_must_be_enabled")
        if not self.api_key or not self.secret_key:
            raise ValueError("alpaca_credentials_missing")
        return self

    def redacted_dict(self) -> dict[str, Any]:
        return {
            "env": self.env,
            "api_key": _redact_secret(self.api_key),
            "secret_key": _redact_secret(self.secret_key),
            "trading_base_url": self.trading_base_url,
            "data_base_url": self.data_base_url,
            "live_trading_enabled": self.live_trading_enabled,
            "paper_only": self.paper_only,
            "allow_order_placement": self.allow_order_placement,
        }


def load_alpaca_paper_config() -> AlpacaPaperConfig:
    return AlpacaPaperConfig(
        env=(os.getenv("ALPACA_ENV", "") or "").strip().lower(),
        api_key=os.getenv("ALPACA_API_KEY_ID"),
        secret_key=os.getenv("ALPACA_API_SECRET_KEY"),
        trading_base_url=(os.getenv("ALPACA_TRADING_BASE_URL") or "https://paper-api.alpaca.markets").rstrip("/"),
        data_base_url=(os.getenv("ALPACA_DATA_BASE_URL") or "https://data.alpaca.markets").rstrip("/"),
        live_trading_enabled=_normalize_bool(os.getenv("AUTOBOTT_LIVE_TRADING_ENABLED"), default=False),
        paper_only=_normalize_bool(os.getenv("AUTOBOTT_PAPER_ONLY"), default=True),
        allow_order_placement=_normalize_bool(os.getenv("AUTOBOTT_ALLOW_ORDER_PLACEMENT"), default=False),
    )


def require_alpaca_paper_config() -> AlpacaPaperConfig:
    return load_alpaca_paper_config().validate()


def _redact_secret(value: str | None) -> str | None:
    if value is None:
        return None
    if len(value) <= 4:
        return "*" * len(value)
    return f"{value[:2]}{'*' * (len(value) - 4)}{value[-2:]}"
