from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class AlpacaReadOnlyConfig:
    api_key: str | None
    secret_key: str | None
    base_url: str | None
    data_url: str | None
    paper: bool

    @property
    def has_credentials(self) -> bool:
        return bool(self.api_key and self.secret_key)


def load_alpaca_read_only_config() -> AlpacaReadOnlyConfig:
    """Load the old bot's common Alpaca env names without enabling execution."""
    api_key = os.getenv("APCA_API_KEY_ID") or os.getenv("ALPACA_API_KEY")
    secret_key = os.getenv("APCA_API_SECRET_KEY") or os.getenv("ALPACA_SECRET_KEY")
    base_url = os.getenv("APCA_API_BASE_URL") or os.getenv("ALPACA_BASE_URL")
    data_url = os.getenv("APCA_API_DATA_URL") or os.getenv("ALPACA_DATA_URL")
    paper_raw = os.getenv("ALPACA_PAPER", "true").strip().lower()

    return AlpacaReadOnlyConfig(
        api_key=api_key,
        secret_key=secret_key,
        base_url=base_url,
        data_url=data_url,
        paper=paper_raw not in {"0", "false", "no"},
    )
