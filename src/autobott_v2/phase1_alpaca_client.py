from __future__ import annotations

import json
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, datetime, timedelta
from typing import Any

from .hosted_policy import HOSTED_TACTICAL_MIN_DTE, is_hosted_paper_runtime
from .phase1_alpaca_config import AlpacaPaperConfig, require_alpaca_paper_config


_OPTION_CONTRACT_METADATA_CACHE: dict[tuple[str, str, str, str, str], dict[str, dict[str, Any]]] = {}
_OPTION_CONTRACT_METADATA_CACHE_LOCK = threading.Lock()


class _AlpacaResponseDecodeError(ValueError):
    """A successful Alpaca response whose body cannot be decoded as JSON."""

    def __init__(self, path: str, reason: str) -> None:
        super().__init__(f"alpaca_response_invalid_json:{path}:{reason}")
        self.path = path
        self.reason = reason


class AlpacaPaperClient:
    def __init__(self, config: AlpacaPaperConfig | None = None) -> None:
        self.config = (config or require_alpaca_paper_config()).validate()
        # One client instance is reused for a complete trading cycle. Cache the
        # repeated SPY/QQQ/VIXY context reads so a 25-symbol scan does not spend
        # most of Alpaca's request budget downloading identical bars.
        self._stock_bars_cache: dict[tuple[Any, ...], dict[str, list[dict[str, Any]]]] = {}
        self._latest_stock_quote_cache: dict[str, dict[str, Any]] = {}
        self._account_cache: dict[str, Any] | None = None

    def get_account(self) -> dict[str, Any]:
        if self._account_cache is None:
            self._account_cache = self._get_json_with_retry(self.config.trading_base_url, "/v2/account")
        return dict(self._account_cache)

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
        cache_key = (
            tuple(sorted(symbol.upper() for symbol in symbols)),
            _isoformat_z(start),
            _isoformat_z(end),
            timeframe,
            limit,
            feed,
        )
        cached = self._stock_bars_cache.get(cache_key)
        if cached is not None:
            return {symbol: list(rows) for symbol, rows in cached.items()}
        bars: dict[str, list[dict[str, Any]]] = {}
        base_params = {
            "symbols": ",".join(symbols),
            "timeframe": timeframe,
            "start": _isoformat_z(start),
            "end": _isoformat_z(end),
            "limit": str(limit),
            "sort": "asc",
            "feed": feed,
        }
        page_token: str | None = None
        for _ in range(20):
            params = dict(base_params)
            if page_token:
                params["page_token"] = page_token
            payload = self._get_json_with_retry(self.config.data_base_url, "/v2/stocks/bars", params)
            for symbol, rows in payload.get("bars", {}).items():
                bars.setdefault(symbol.upper(), []).extend(list(rows))
            page_token = payload.get("next_page_token")
            if not page_token:
                break
        self._stock_bars_cache[cache_key] = {symbol: list(rows) for symbol, rows in bars.items()}
        return bars

    def get_latest_stock_quotes(self, symbols: list[str]) -> dict[str, dict[str, Any]]:
        normalized = [symbol.upper() for symbol in symbols]
        missing = [symbol for symbol in normalized if symbol not in self._latest_stock_quote_cache]
        if not missing:
            return {symbol: dict(self._latest_stock_quote_cache[symbol]) for symbol in normalized}
        payload = self._get_json_with_retry(
            self.config.data_base_url,
            "/v2/stocks/quotes/latest",
            {"symbols": ",".join(missing)},
        )
        quotes = payload.get("quotes", {})
        self._latest_stock_quote_cache.update({symbol.upper(): dict(row) for symbol, row in quotes.items()})
        return {
            symbol: dict(self._latest_stock_quote_cache[symbol])
            for symbol in normalized
            if symbol in self._latest_stock_quote_cache
        }

    def get_option_chain_snapshots(self, symbol: str) -> dict[str, dict[str, Any]]:
        # Without an expiration window, Alpaca's snapshot endpoint defaults to
        # only the nearest (often same-day) expiration, which the decision
        # engine's minimum-DTE rule always excludes. Request the window the
        # engine actually trades so a full chain comes back.
        normalized_symbol = symbol.upper()
        if normalized_symbol in {"VIX", "VIXW"}:
            self._require_paper_index_option_capability()
        underlying_symbol, root_symbol = _option_chain_request_symbols(normalized_symbol)
        today = datetime.now(UTC).date()
        min_dte = HOSTED_TACTICAL_MIN_DTE if is_hosted_paper_runtime() else 1
        base_params = {
            "feed": "indicative",
            "limit": "1000",
            "expiration_date_gte": (today + timedelta(days=min_dte)).isoformat(),
            "expiration_date_lte": (today + timedelta(days=45)).isoformat(),
        }
        contract_metadata = self._get_option_contract_metadata(
            underlying_symbol,
            expiration_date_gte=base_params["expiration_date_gte"],
            expiration_date_lte=base_params["expiration_date_lte"],
            root_symbol=root_symbol,
        )
        if root_symbol is not None:
            base_params["root_symbol"] = root_symbol
        snapshots: dict[str, dict[str, Any]] = {}
        page_token: str | None = None
        seen_page_tokens: set[str] = set()
        while True:
            params = dict(base_params)
            if page_token:
                params["page_token"] = page_token
            payload = self._get_json_with_retry(
                self.config.data_base_url,
                f"/v1beta1/options/snapshots/{underlying_symbol}",
                params,
            )
            page_snapshots = payload.get("snapshots") or payload.get("option_snapshots") or {}
            snapshots.update({option_symbol.upper(): dict(row) for option_symbol, row in page_snapshots.items()})
            next_page_token = payload.get("next_page_token")
            if not next_page_token:
                break
            if next_page_token in seen_page_tokens:
                raise ValueError("option_chain_pagination_token_cycle")
            seen_page_tokens.add(next_page_token)
            page_token = next_page_token
        return {
            option_symbol: _merge_option_contract_metadata(snapshot, contract_metadata.get(option_symbol))
            for option_symbol, snapshot in snapshots.items()
            if option_symbol in contract_metadata
        }

    def _require_paper_index_option_capability(self) -> None:
        account = self.get_account()
        if bool(account.get("trading_blocked")) or bool(account.get("account_blocked")):
            raise ValueError("vix_index_options_account_blocked")
        raw_level = account.get("options_trading_level")
        if raw_level is None:
            raw_level = account.get("options_approved_level")
        if raw_level is not None and int(raw_level) < 2:
            raise ValueError("vix_index_options_level_insufficient")

    def get_latest_option_quotes(self, option_symbols: list[str]) -> dict[str, dict[str, Any]]:
        symbols = [symbol.strip().upper() for symbol in option_symbols if symbol.strip()]
        if not symbols:
            return {}
        if len(symbols) > 100:
            raise ValueError("latest_option_quotes_symbol_limit_exceeded")
        payload = self._get_json_with_retry(
            self.config.data_base_url,
            "/v1beta1/options/quotes/latest",
            {"symbols": ",".join(symbols), "feed": "indicative"},
        )
        return {
            str(option_symbol).upper(): dict(quote)
            for option_symbol, quote in (payload.get("quotes") or {}).items()
        }

    def _get_option_contract_metadata(
        self,
        symbol: str,
        *,
        expiration_date_gte: str,
        expiration_date_lte: str,
        root_symbol: str | None = None,
    ) -> dict[str, dict[str, Any]]:
        """Fetch contract fields that are absent from the market-data snapshot.

        Alpaca's chain snapshot contains quotes, trades, and Greeks. Open
        interest and canonical contract details live on the trading API's
        option-contract endpoint, so the two responses must be joined before
        liquidity rules can evaluate a live chain.
        """

        base_params = {
            "underlying_symbols": symbol.upper(),
            "status": "active",
            "expiration_date_gte": expiration_date_gte,
            "expiration_date_lte": expiration_date_lte,
            "limit": "10000",
        }
        if symbol.upper() in {"VIX", "VIXW"}:
            base_params["style"] = "european"
        if root_symbol is not None:
            base_params["root_symbol"] = root_symbol
        cache_key = (
            self.config.trading_base_url,
            symbol.upper(),
            expiration_date_gte,
            expiration_date_lte,
            root_symbol or "",
        )
        with _OPTION_CONTRACT_METADATA_CACHE_LOCK:
            cached = _OPTION_CONTRACT_METADATA_CACHE.get(cache_key)
        if cached is not None:
            return cached
        metadata: dict[str, dict[str, Any]] = {}
        page_token: str | None = None
        seen_page_tokens: set[str] = set()
        while True:
            params = dict(base_params)
            if page_token:
                params["page_token"] = page_token
            payload = self._get_json_with_retry(
                self.config.trading_base_url,
                "/v2/options/contracts",
                params,
            )
            for contract in payload.get("option_contracts") or []:
                if contract.get("tradable") is not True:
                    continue
                if symbol.upper() in {"VIX", "VIXW"}:
                    if str(contract.get("style") or "").lower() != "european":
                        continue
                    contract_root = str(contract.get("root_symbol") or "").upper()
                    if root_symbol is not None and contract_root != root_symbol:
                        continue
                    if root_symbol is None and contract_root and contract_root not in {"VIX", "VIXW"}:
                        continue
                option_symbol = str(contract.get("symbol") or "").upper()
                if option_symbol:
                    metadata[option_symbol] = dict(contract)
            next_page_token = payload.get("next_page_token")
            if not next_page_token:
                break
            if next_page_token in seen_page_tokens:
                raise ValueError("option_contract_pagination_token_cycle")
            seen_page_tokens.add(next_page_token)
            page_token = next_page_token
        if not metadata:
            raise ValueError(f"option_contract_metadata_empty:{symbol.upper()}")
        with _OPTION_CONTRACT_METADATA_CACHE_LOCK:
            _OPTION_CONTRACT_METADATA_CACHE[cache_key] = metadata
        return metadata

    def get_positions(self) -> list[dict[str, Any]]:
        payload = self._get_json_with_retry(self.config.trading_base_url, "/v2/positions")
        return list(payload) if isinstance(payload, list) else []

    def get_orders(
        self,
        *,
        status: str = "all",
        limit: int = 50,
        direction: str = "desc",
    ) -> list[dict[str, Any]]:
        payload = self._get_json_with_retry(
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
            body = response.read()
            if not body or not body.strip():
                raise _AlpacaResponseDecodeError(path, "empty_body")
            try:
                decoded = body.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise _AlpacaResponseDecodeError(path, "invalid_utf8") from exc
            try:
                return json.loads(decoded)
            except json.JSONDecodeError as exc:
                reason = f"malformed_json_line_{exc.lineno}_column_{exc.colno}"
                raise _AlpacaResponseDecodeError(path, reason) from exc

    def _get_json_with_retry(
        self,
        base_url: str,
        path: str,
        params: dict[str, str] | None = None,
    ) -> Any:
        for attempt in range(3):
            try:
                return self._get_json(base_url, path, params)
            except urllib.error.HTTPError as exc:
                if exc.code != 429 and exc.code < 500:
                    raise
                if attempt == 2:
                    raise
            except (urllib.error.URLError, TimeoutError):
                if attempt == 2:
                    raise
            except _AlpacaResponseDecodeError as exc:
                if attempt == 2:
                    raise RuntimeError(
                        f"alpaca_response_invalid_json:{path}:{exc.reason}:attempts=3"
                    ) from exc
            time.sleep(0.25 * (2**attempt))
        raise RuntimeError("alpaca_request_retry_exhausted")


def _clear_option_contract_metadata_cache() -> None:
    with _OPTION_CONTRACT_METADATA_CACHE_LOCK:
        _OPTION_CONTRACT_METADATA_CACHE.clear()


def _option_chain_request_symbols(symbol: str) -> tuple[str, str | None]:
    """Return Alpaca's underlying path symbol and optional contract root.

    VIX weekly contracts use the ``VIXW`` root but remain options on the VIX
    index. Alpaca's chain endpoint is keyed by underlying, so a VIXW-only
    request must use the VIX path and narrow the response by root symbol.
    """

    normalized = symbol.strip().upper()
    if normalized == "VIXW":
        return "VIX", "VIXW"
    return normalized, None


def _isoformat_z(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _merge_option_contract_metadata(
    snapshot: dict[str, Any],
    metadata: dict[str, Any] | None,
) -> dict[str, Any]:
    merged = dict(snapshot)
    if not metadata:
        return merged
    details = dict(merged.get("details") or merged.get("option_details") or {})
    details.update(
        {
            "expiration_date": metadata.get("expiration_date"),
            "strike_price": metadata.get("strike_price"),
            "type": metadata.get("type"),
        }
    )
    merged["details"] = {key: value for key, value in details.items() if value is not None}
    merged["open_interest"] = metadata.get("open_interest") or 0
    merged["open_interest_date"] = metadata.get("open_interest_date")
    merged["tradable"] = metadata.get("tradable") is True
    return merged
