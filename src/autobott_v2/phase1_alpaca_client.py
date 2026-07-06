from __future__ import annotations

import json
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from typing import Any

from .phase1_alpaca_config import AlpacaPaperConfig, require_alpaca_paper_config


class AlpacaPaperClient:
    def __init__(self, config: AlpacaPaperConfig | None = None) -> None:
        self.config = (config or require_alpaca_paper_config()).validate()

    def get_account(self) -> dict[str, Any]:
        return self._get_json(self.config.trading_base_url, "/v2/account")

    def get_stock_bars(
        self,
        symbols: list[str],
        *,
        start: datetime,
        end: datetime,
        timeframe: str = "1Min",
        limit: int = 35,
        feed: str = "iex",
    ) -> dict[str, list[dict[str, Any]]]:
        payload = self._get_json(
            self.config.data_base_url,
            "/v2/stocks/bars",
            {
                "symbols": ",".join(symbols),
                "timeframe": timeframe,
                "start": _isoformat_z(start),
                "end": _isoformat_z(end),
                "limit": str(limit),
                "sort": "asc",
                "feed": feed,
            },
        )
        bars = payload.get("bars", {})
        return {symbol.upper(): list(rows) for symbol, rows in bars.items()}

    def get_latest_stock_quotes(self, symbols: list[str]) -> dict[str, dict[str, Any]]:
        payload = self._get_json(
            self.config.data_base_url,
            "/v2/stocks/quotes/latest",
            {"symbols": ",".join(symbols)},
        )
        quotes = payload.get("quotes", {})
        return {symbol.upper(): dict(row) for symbol, row in quotes.items()}

    def get_option_chain_snapshots(self, symbol: str) -> dict[str, dict[str, Any]]:
        payload = self._get_json(
            self.config.data_base_url,
            f"/v1beta1/options/snapshots/{symbol.upper()}",
            {"feed": "indicative"},
        )
        snapshots = payload.get("snapshots") or payload.get("option_snapshots") or {}
        return {option_symbol: dict(row) for option_symbol, row in snapshots.items()}

    def get_positions(self) -> list[dict[str, Any]]:
        payload = self._get_json(self.config.trading_base_url, "/v2/positions")
        return list(payload) if isinstance(payload, list) else []

    def get_orders(
        self,
        *,
        status: str = "all",
        limit: int = 50,
        direction: str = "desc",
    ) -> list[dict[str, Any]]:
        payload = self._get_json(
            self.config.trading_base_url,
            "/v2/orders",
            {"status": status, "limit": str(limit), "direction": direction, "nested": "false"},
        )
        return list(payload) if isinstance(payload, list) else []

    def _get_json(
        self,
        base_url: str,
        path: str,
        params: dict[str, str] | None = None,
    ) -> Any:
        query = urllib.parse.urlencode(params or {})
        suffix = f"?{query}" if query else ""
        request = urllib.request.Request(
            f"{base_url}{path}{suffix}",
            headers={
                "APCA-API-KEY-ID": str(self.config.api_key),
                "APCA-API-SECRET-KEY": str(self.config.secret_key),
                "Accept": "application/json",
            },
            method="GET",
        )
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))


def _isoformat_z(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
