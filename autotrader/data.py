"""Market data fetching for stocks and options."""

from __future__ import annotations

from datetime import date, datetime, timedelta
import time
from typing import Any

import pandas as pd
import pytz
import requests

import config

_EARNINGS_SKIP_SYMBOLS = {str(s).upper() for s in config.EARNINGS_SKIP_SYMBOLS}

# Options contract data is ONLY available on the live API endpoint.
# Paper trading accounts submit orders via paper-api but must fetch
# contracts, quotes, and chain data from the live API.
_LIVE_TRADE_BASE_URL = "https://api.alpaca.markets"


def _yfinance_ticker(symbol: str):
    import yfinance as yf

    return yf.Ticker(symbol)


def _yfinance_fallback_enabled() -> bool:
    return bool(getattr(config, "ENABLE_YFINANCE_FALLBACK", True))


class AlpacaDataClient:
    _shared_rate_limit: dict[str, Any] = {
        "recent_429_count": 0,
        "last_429_at_epoch": 0.0,
        "cooldown_until_epoch": 0.0,
        "cooldown_seconds": 0.0,
        "degraded": False,
        "last_error": "",
        "last_cooldown_skip_log_epoch": 0.0,
    }

    def __init__(self, api_key: str, secret_key: str, paper: bool = True):
        self.api_key = api_key
        self.secret_key = secret_key
        self.paper = paper
        self.base_url = config.ALPACA_PAPER_BASE_URL if paper else "https://api.alpaca.markets"
        self.data_base_url = config.ALPACA_DATA_BASE_URL
        self.trade_session = requests.Session()
        self.trade_session.headers.update(
            {
                "APCA-API-KEY-ID": api_key,
                "APCA-API-SECRET-KEY": secret_key,
                "accept": "application/json",
            }
        )
        # Options contract/quote lookups always use the live endpoint.
        # When running in paper mode, use ALPACA_LIVE_API_KEY / ALPACA_LIVE_SECRET_KEY
        # if provided, because paper keys are rejected (401) by the live endpoint.
        live_api_key = str(getattr(config, "ALPACA_LIVE_API_KEY", "") or "").strip()
        live_secret_key = str(getattr(config, "ALPACA_LIVE_SECRET_KEY", "") or "").strip()
        options_key = live_api_key if (paper and live_api_key) else api_key
        options_secret = live_secret_key if (paper and live_secret_key) else secret_key
        if paper:
            if live_api_key:
                print(f"[data] Paper mode: using LIVE key ({live_api_key[:6]}...) for options endpoint.")
            else:
                print("[data] WARNING: Paper mode but ALPACA_LIVE_API_KEY not set — options endpoint will 401.")
        self.options_session = requests.Session()
        self.options_session.headers.update(
            {
                "APCA-API-KEY-ID": options_key,
                "APCA-API-SECRET-KEY": options_secret,
                "accept": "application/json",
            }
        )
        self.data_session = requests.Session()
        self.data_session.headers.update(
            {
                "APCA-API-KEY-ID": api_key,
                "APCA-API-SECRET-KEY": secret_key,
                "accept": "application/json",
            }
        )
        # Options contract/quote data is ONLY available on the live API endpoint.
        # Paper trading accounts must also use the live endpoint for contract lookups.
        # Always include the live endpoint; for paper accounts it is the primary source.
        if paper:
            self._option_contract_base_candidates = [_LIVE_TRADE_BASE_URL]
        else:
            self._option_contract_base_candidates = list(
                dict.fromkeys([_LIVE_TRADE_BASE_URL, self.base_url])
            )
        self._bars_cache: dict[tuple[Any, ...], tuple[float, pd.DataFrame]] = {}
        self._bars_cache_ttl_seconds = max(1.0, float(getattr(config, "DATA_BARS_CACHE_TTL_SECONDS", 12.0) or 12.0))
        self._alpaca_backoff_until_ts = 0.0
        self._alpaca_backoff_seconds = max(1.0, float(getattr(config, "ALPACA_429_BACKOFF_SECONDS", 2.0) or 2.0))
        self._alpaca_cooldown_seconds = max(
            self._alpaca_backoff_seconds,
            float(getattr(config, "ALPACA_429_COOLDOWN_SECONDS", 90.0) or 90.0),
        )

    @classmethod
    def get_rate_limit_status(cls) -> dict[str, Any]:
        now = time.time()
        shared = dict(cls._shared_rate_limit)
        cooldown_until = float(shared.get("cooldown_until_epoch", 0.0) or 0.0)
        remaining = max(0.0, cooldown_until - now)
        shared["cooldown_remaining_seconds"] = round(remaining, 2)
        shared["data_source_degraded"] = bool(shared.get("degraded", False) and remaining > 0)
        return shared

    def _alpaca_cooldown_active(self) -> bool:
        return float(self._shared_rate_limit.get("cooldown_until_epoch", 0.0) or 0.0) > time.time()

    def _log_cooldown_skip_once(self, context: str) -> None:
        now = time.time()
        last = float(self._shared_rate_limit.get("last_cooldown_skip_log_epoch", 0.0) or 0.0)
        if now - last < 15.0:
            return
        self._shared_rate_limit["last_cooldown_skip_log_epoch"] = now
        remaining = max(0.0, float(self._shared_rate_limit.get("cooldown_until_epoch", 0.0) or 0.0) - now)
        print(f"[data] Alpaca cooldown active ({remaining:.1f}s). Skipping Alpaca fetch for {context}.")

    def _bars_cache_get(self, key: tuple[Any, ...]) -> pd.DataFrame | None:
        cached = self._bars_cache.get(key)
        if cached is None:
            return None
        expires_at, frame = cached
        if time.time() >= expires_at:
            self._bars_cache.pop(key, None)
            return None
        return frame.copy()

    def _bars_cache_set(self, key: tuple[Any, ...], frame: pd.DataFrame) -> None:
        self._bars_cache[key] = (time.time() + self._bars_cache_ttl_seconds, frame.copy())

    def _request_json(
        self,
        *,
        session: requests.Session,
        url: str,
        params: dict[str, Any],
        timeout: int = 15,
        attempts: int = 3,
    ) -> dict[str, Any]:
        last_exc: Exception | None = None
        endpoint = str(url.split("?", 1)[0] if "?" in str(url) else url)
        for attempt in range(1, max(1, attempts) + 1):
            shared_cooldown_until = float(self._shared_rate_limit.get("cooldown_until_epoch", 0.0) or 0.0)
            if shared_cooldown_until > time.time():
                remaining = shared_cooldown_until - time.time()
                raise requests.HTTPError(f"429 cooldown active for {endpoint} ({remaining:.1f}s remaining)")
            wait_seconds = self._alpaca_backoff_until_ts - time.time()
            if wait_seconds > 0:
                time.sleep(min(wait_seconds, 3.0))
            try:
                resp = session.get(url, params=params, timeout=timeout)
                if resp.status_code == 429:
                    self._alpaca_backoff_until_ts = time.time() + self._alpaca_backoff_seconds
                    retry_after_raw = resp.headers.get("Retry-After")
                    try:
                        retry_after = float(retry_after_raw) if retry_after_raw is not None else self._alpaca_backoff_seconds
                    except (TypeError, ValueError):
                        retry_after = self._alpaca_backoff_seconds
                    cooldown_seconds = max(self._alpaca_cooldown_seconds, self._alpaca_backoff_seconds, retry_after)
                    cooldown_until = time.time() + cooldown_seconds
                    self._shared_rate_limit.update(
                        {
                            "recent_429_count": int(self._shared_rate_limit.get("recent_429_count", 0) or 0) + 1,
                            "last_429_at_epoch": time.time(),
                            "cooldown_until_epoch": cooldown_until,
                            "cooldown_seconds": float(cooldown_seconds),
                            "degraded": True,
                            "last_error": f"429 Too Many Requests for {endpoint}",
                        }
                    )
                    time.sleep(min(max(self._alpaca_backoff_seconds, retry_after), 3.0))
                    raise requests.HTTPError(f"429 Too Many Requests for {url}", response=resp)
                resp.raise_for_status()
                if float(self._shared_rate_limit.get("cooldown_until_epoch", 0.0) or 0.0) <= time.time():
                    self._shared_rate_limit["degraded"] = False
                return resp.json() if resp.content else {}
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                self._shared_rate_limit["last_error"] = str(exc)[:200]
                if attempt >= attempts:
                    break
                time.sleep(min(1.0 * attempt, 3.0))
        if last_exc is not None:
            raise last_exc
        return {}

    def get_stock_bars(
        self,
        symbol: str,
        limit: int,
        timeframe: str = "5m",
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> pd.DataFrame:
        """
        Fetch OHLCV bars using Alpaca first, then yfinance fallback.
        timeframe: "1m", "5m", "15m", "30m", "1h", "1d", etc.
        limit: max number of rows to return (most recent).
        """
        timeframe_map = {
            "1m": "1Min",
            "5m": "5Min",
            "15m": "15Min",
            "30m": "30Min",
            "60m": "1Hour",
            "1h": "1Hour",
            "1d": "1Day",
        }
        tf = str(timeframe or "5m").strip().lower()
        if tf.endswith("min"):
            tf = f"{tf[:-3]}m"
        alpaca_tf = timeframe_map.get(tf)
        tz_et = pytz.timezone(config.EASTERN_TZ)

        cache_key = ("stock_bars", str(symbol).upper(), str(alpaca_tf or tf), int(limit))
        cached = self._bars_cache_get(cache_key)
        if cached is not None and not cached.empty:
            return cached

        # Primary source: Alpaca bars API (keeps volume basis aligned with intraday feed)
        if self._alpaca_cooldown_active():
            self._log_cooldown_skip_once(f"get_stock_bars:{symbol}:{timeframe}")
            return pd.DataFrame()
        if alpaca_tf:
            try:
                now_utc = datetime.now(pytz.UTC)
                if tf in ("1d",):
                    lookback_days = max(45, int(limit * 2))
                else:
                    lookback_days = 10
                start_utc = (now_utc - timedelta(days=lookback_days)).isoformat().replace("+00:00", "Z")
                end_utc = now_utc.isoformat().replace("+00:00", "Z")
                body = self._request_json(
                    session=self.data_session,
                    url=f"{self.data_base_url}/v2/stocks/bars",
                    params={
                        "symbols": symbol,
                        "timeframe": alpaca_tf,
                        "start": start_utc,
                        "end": end_utc,
                        "limit": max(50, int(limit) + 25),
                        "adjustment": "raw",
                        "feed": "iex",
                    },
                    timeout=15,
                )
                bars_map = body.get("bars", {}) if isinstance(body, dict) else {}
                rows = bars_map.get(symbol, []) if isinstance(bars_map, dict) else []
                if rows:
                    df = pd.DataFrame(rows)
                    if not df.empty and {"t", "o", "h", "l", "c", "v"}.issubset(set(df.columns)):
                        df = df.rename(
                            columns={
                                "t": "timestamp",
                                "o": "open",
                                "h": "high",
                                "l": "low",
                                "c": "close",
                                "v": "volume",
                            }
                        )
                        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert(tz_et)
                        df = df.tail(limit).reset_index(drop=True)
                        result = df[["timestamp", "open", "high", "low", "close", "volume"]]
                        self._bars_cache_set(cache_key, result)
                        return result
            except Exception as exc:  # noqa: BLE001
                print(f"[data] get_stock_bars Alpaca failed for {symbol} tf={timeframe}: {exc}")

        # Fallback: yfinance
        if not _yfinance_fallback_enabled():
            return pd.DataFrame()
        try:
            # Choose a safe period based on timeframe
            yf_interval = {
                "1m": "1m",
                "2m": "2m",
                "5m": "5m",
                "15m": "15m",
                "30m": "30m",
                "60m": "60m",
                "90m": "90m",
                "1h": "1h",
                "1d": "1d",
            }.get(tf, tf)

            if yf_interval in ("1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h"):
                period = "5d"  # yfinance allows up to 60d for minute-level, 5d is safe
            else:
                period = f"{limit + 10}d"

            ticker = _yfinance_ticker(symbol)
            df = ticker.history(period=period, interval=yf_interval, auto_adjust=True)

            if df is None or df.empty:
                return pd.DataFrame()

            # Normalize column names to lowercase
            df.columns = [c.lower() for c in df.columns]

            # Ensure we have required columns
            required = {"open", "high", "low", "close", "volume"}
            if not required.issubset(set(df.columns)):
                return pd.DataFrame()

            # Reset index - yfinance sets datetime as the index
            df = df.reset_index()
            # Rename the index column to 'timestamp'
            ts_col = [c for c in df.columns if "date" in c.lower() or c == "datetime" or c == "index"]
            if ts_col:
                df = df.rename(columns={ts_col[0]: "timestamp"})

            # Ensure timestamp is timezone-aware and in ET
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert(tz_et)

            # Keep only the most recent `limit` rows
            df = df.tail(limit).reset_index(drop=True)
            return df[["timestamp", "open", "high", "low", "close", "volume"]]

        except Exception as exc:  # noqa: BLE001
            print(f"[data] get_stock_bars yfinance failed for {symbol} tf={timeframe}: {exc}")
            return pd.DataFrame()

    def get_stock_daily_bars(self, symbol: str, limit: int = 30) -> pd.DataFrame:
        """Fetch daily OHLCV bars using yfinance."""
        return self.get_stock_bars(symbol=symbol, limit=limit, timeframe="1d")

    def get_intraday_bars_since_open(
        self,
        symbol: str,
        now_et: datetime,
        limit: int = 120,
        bar_timeframe: str | None = None,
    ) -> pd.DataFrame:
        """
        Fetch 5-minute intraday bars from market open (9:30 ET) until now.
        Uses Alpaca data bars first, with yfinance fallback.
        """
        tz_et = pytz.timezone(config.EASTERN_TZ)
        today = now_et.date()
        market_open = tz_et.localize(datetime(today.year, today.month, today.day, 9, 30, 0))
        minutes_since_open = max(0, int((now_et - market_open).total_seconds() // 60))
        normalized_tf = str(bar_timeframe or "").strip().lower()
        if normalized_tf in ("1min", "1m"):
            timeframe = "1Min"
        elif normalized_tf in ("5min", "5m"):
            timeframe = "5Min"
        else:
            use_one_minute = minutes_since_open < 5
            timeframe = "1Min" if use_one_minute else "5Min"

        cache_key = (
            "intraday_since_open",
            str(symbol).upper(),
            str(timeframe),
            now_et.strftime("%Y-%m-%d %H:%M"),
            int(limit),
        )
        cached = self._bars_cache_get(cache_key)
        if cached is not None and not cached.empty:
            return cached

        # Primary source: Alpaca bars API (more stable intraday for live trading loops)
        if self._alpaca_cooldown_active():
            self._log_cooldown_skip_once(f"get_intraday_bars_since_open:{symbol}")
            return pd.DataFrame()
        try:
            start_utc = market_open.astimezone(pytz.UTC).isoformat().replace("+00:00", "Z")
            end_utc = now_et.astimezone(pytz.UTC).isoformat().replace("+00:00", "Z")
            body = self._request_json(
                session=self.data_session,
                url=f"{self.data_base_url}/v2/stocks/bars",
                params={
                    "symbols": symbol,
                    "timeframe": timeframe,
                    "start": start_utc,
                    "end": end_utc,
                    "limit": max(limit, 500),
                    "adjustment": "raw",
                    "feed": "iex",
                },
                timeout=15,
            )
            bars_map = body.get("bars", {}) if isinstance(body, dict) else {}
            rows = bars_map.get(symbol, []) if isinstance(bars_map, dict) else []
            if rows:
                df = pd.DataFrame(rows)
                if not df.empty and {"t", "o", "h", "l", "c", "v"}.issubset(set(df.columns)):
                    df = df.rename(
                        columns={
                            "t": "timestamp",
                            "o": "open",
                            "h": "high",
                            "l": "low",
                            "c": "close",
                            "v": "volume",
                        }
                    )
                    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert(tz_et)
                    df = df[(df["timestamp"] >= market_open) & (df["timestamp"] <= now_et)]
                    df = df.tail(limit).reset_index(drop=True)
                    if not df.empty:
                        result = df[["timestamp", "open", "high", "low", "close", "volume"]]
                        self._bars_cache_set(cache_key, result)
                        return result
        except Exception as exc:  # noqa: BLE001
            print(f"[data] get_intraday_bars_since_open Alpaca failed for {symbol}: {exc}")

        # Fallback: yfinance
        if not _yfinance_fallback_enabled():
            return pd.DataFrame()
        try:
            ticker = _yfinance_ticker(symbol)
            interval = "1m" if timeframe == "1Min" else "5m"
            df = ticker.history(period="5d", interval=interval, auto_adjust=True)

            if df is None or df.empty:
                return pd.DataFrame()

            df.columns = [c.lower() for c in df.columns]
            df = df.reset_index()
            ts_col = [c for c in df.columns if "date" in c.lower() or c == "datetime" or c == "index"]
            if ts_col:
                df = df.rename(columns={ts_col[0]: "timestamp"})

            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert(tz_et)

            # Filter to today's session from 9:30 ET onwards
            df = df[df["timestamp"] >= market_open]
            df = df[df["timestamp"] <= now_et]
            df = df.tail(limit).reset_index(drop=True)

            if df.empty:
                return pd.DataFrame()

            return df[["timestamp", "open", "high", "low", "close", "volume"]]

        except Exception as exc:  # noqa: BLE001
            print(f"[data] get_intraday_bars_since_open yfinance failed for {symbol}: {exc}")
            return pd.DataFrame()

    def get_intraday_bars_window(
        self,
        symbol: str,
        start_et: datetime,
        end_et: datetime,
        limit: int = 120,
    ) -> pd.DataFrame:
        """
        Fetch intraday bars between explicit ET timestamps.
        Useful for premarket scans where we cannot anchor to 9:30 ET session open.
        """
        tz_et = pytz.timezone(config.EASTERN_TZ)
        if start_et.tzinfo is None:
            start_et = tz_et.localize(start_et)
        if end_et.tzinfo is None:
            end_et = tz_et.localize(end_et)
        if end_et <= start_et:
            return pd.DataFrame()

        minutes_span = max(1, int((end_et - start_et).total_seconds() // 60))
        timeframe = "1Min" if minutes_span <= 20 else "5Min"

        cache_key = (
            "intraday_window",
            str(symbol).upper(),
            start_et.strftime("%Y-%m-%d %H:%M"),
            end_et.strftime("%Y-%m-%d %H:%M"),
            int(limit),
        )
        cached = self._bars_cache_get(cache_key)
        if cached is not None and not cached.empty:
            return cached

        # Primary source: Alpaca bars API
        if self._alpaca_cooldown_active():
            self._log_cooldown_skip_once(f"get_intraday_bars_window:{symbol}")
            return pd.DataFrame()
        try:
            start_utc = start_et.astimezone(pytz.UTC).isoformat().replace("+00:00", "Z")
            end_utc = end_et.astimezone(pytz.UTC).isoformat().replace("+00:00", "Z")
            body = self._request_json(
                session=self.data_session,
                url=f"{self.data_base_url}/v2/stocks/bars",
                params={
                    "symbols": symbol,
                    "timeframe": timeframe,
                    "start": start_utc,
                    "end": end_utc,
                    "limit": max(limit, 500),
                    "adjustment": "raw",
                    "feed": "iex",
                },
                timeout=15,
            )
            bars_map = body.get("bars", {}) if isinstance(body, dict) else {}
            rows = bars_map.get(symbol, []) if isinstance(bars_map, dict) else []
            if rows:
                df = pd.DataFrame(rows)
                if not df.empty and {"t", "o", "h", "l", "c", "v"}.issubset(set(df.columns)):
                    df = df.rename(
                        columns={
                            "t": "timestamp",
                            "o": "open",
                            "h": "high",
                            "l": "low",
                            "c": "close",
                            "v": "volume",
                        }
                    )
                    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert(tz_et)
                    df = df[(df["timestamp"] >= start_et) & (df["timestamp"] <= end_et)]
                    df = df.tail(limit).reset_index(drop=True)
                    if not df.empty:
                        result = df[["timestamp", "open", "high", "low", "close", "volume"]]
                        self._bars_cache_set(cache_key, result)
                        return result
        except Exception as exc:  # noqa: BLE001
            print(f"[data] get_intraday_bars_window Alpaca failed for {symbol}: {exc}")

        # Fallback: yfinance
        if not _yfinance_fallback_enabled():
            return pd.DataFrame()
        try:
            ticker = _yfinance_ticker(symbol)
            interval = "1m" if timeframe == "1Min" else "5m"
            df = ticker.history(period="5d", interval=interval, auto_adjust=True, prepost=True)

            if df is None or df.empty:
                return pd.DataFrame()

            df.columns = [c.lower() for c in df.columns]
            df = df.reset_index()
            ts_col = [c for c in df.columns if "date" in c.lower() or c == "datetime" or c == "index"]
            if ts_col:
                df = df.rename(columns={ts_col[0]: "timestamp"})

            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert(tz_et)
            df = df[(df["timestamp"] >= start_et) & (df["timestamp"] <= end_et)]
            df = df.tail(limit).reset_index(drop=True)
            if df.empty:
                return pd.DataFrame()
            return df[["timestamp", "open", "high", "low", "close", "volume"]]
        except Exception as exc:  # noqa: BLE001
            print(f"[data] get_intraday_bars_window yfinance failed for {symbol}: {exc}")
            return pd.DataFrame()

    def get_latest_stock_price(self, symbol: str) -> float | None:
        """
        Get latest stock price.
        Preference order:
        1) Alpaca latest trade (IEX)
        2) Alpaca quote midpoint (IEX)
        3) yfinance fallback
        """
        try:
            trade_price = self.get_latest_stock_trade_price(symbol)
            if trade_price is not None and trade_price > 0:
                return float(trade_price)
        except Exception as exc:  # noqa: BLE001
            print(f"[data] latest trade lookup failed for {symbol}: {exc}")

        try:
            quote = self.get_latest_stock_quote(symbol)
            bid = quote.get("bid")
            ask = quote.get("ask")
            if bid is not None and ask is not None and bid > 0 and ask > 0:
                return float((bid + ask) / 2.0)
            if ask is not None and ask > 0:
                return float(ask)
            if bid is not None and bid > 0:
                return float(bid)
        except Exception as exc:  # noqa: BLE001
            print(f"[data] latest quote lookup failed for {symbol}: {exc}")

        try:
            if not _yfinance_fallback_enabled():
                return None
            ticker = _yfinance_ticker(symbol)
            info = ticker.fast_info
            price = float(info.last_price)
            return price if price and price > 0 else None
        except Exception:
            return None

    def get_latest_stock_quote(self, symbol: str) -> dict[str, float | None]:
        try:
            resp = self.data_session.get(
                f"{self.data_base_url}/v2/stocks/quotes/latest",
                params={"symbols": symbol, "feed": "iex"},
                timeout=15,
            )
            resp.raise_for_status()
            body = resp.json()
            quote_map = body.get("quotes", {}) if isinstance(body, dict) else {}
            quote = quote_map.get(symbol) if isinstance(quote_map, dict) else None
            if not quote:
                return {"bid": None, "ask": None, "bid_size": None, "ask_size": None}
            return {
                "bid": float(quote.get("bp")) if quote.get("bp") is not None else None,
                "ask": float(quote.get("ap")) if quote.get("ap") is not None else None,
                "bid_size": float(quote.get("bs")) if quote.get("bs") is not None else None,
                "ask_size": float(quote.get("as")) if quote.get("as") is not None else None,
            }
        except Exception:
            return {"bid": None, "ask": None, "bid_size": None, "ask_size": None}

    def get_latest_stock_trade_price(self, symbol: str) -> float | None:
        try:
            resp = self.data_session.get(
                f"{self.data_base_url}/v2/stocks/trades/latest",
                params={"symbols": symbol, "feed": "iex"},
                timeout=15,
            )
            resp.raise_for_status()
            body = resp.json()
            trade_map = body.get("trades", {}) if isinstance(body, dict) else {}
            trade = trade_map.get(symbol) if isinstance(trade_map, dict) else None
            if not trade:
                return None
            price = trade.get("p")
            return float(price) if price is not None else None
        except Exception:
            return None

    def get_option_contracts(
        self,
        underlying_symbol: str,
        contract_type: str,
        expiration_date_gte: date,
        expiration_date_lte: date,
    ) -> list[dict[str, Any]]:
        params = {
            "underlying_symbols": underlying_symbol,
            "type": contract_type,
            "expiration_date_gte": expiration_date_gte.isoformat(),
            "expiration_date_lte": expiration_date_lte.isoformat(),
            "limit": 200,  # fetch full chain in one call to avoid per-contract enrichment
        }
        errors_by_base: list[str] = []
        for base in self._option_contract_base_candidates:
            try:
                resp = self.options_session.get(
                    f"{base}/v2/options/contracts",
                    params=params,
                    timeout=15,
                )
                resp.raise_for_status()
                body = resp.json()
                contracts = body.get("option_contracts", []) or body.get("contracts", []) or []
                if contracts:
                    return contracts
            except Exception as exc:  # noqa: BLE001
                errors_by_base.append(f"{base}: {exc}")
                continue
        if errors_by_base:
            print(
                f"[data] get_option_contracts failed for {underlying_symbol} "
                f"({contract_type}) across endpoints: {' | '.join(errors_by_base)}"
            )
        return []

    def get_option_contract(self, option_symbol: str) -> dict[str, Any]:
        errors_by_base: list[str] = []
        for base in self._option_contract_base_candidates:
            try:
                resp = self.options_session.get(
                    f"{base}/v2/options/contracts/{option_symbol}",
                    timeout=15,
                )
                resp.raise_for_status()
                body = resp.json()
                payload = body.get("option_contract", body)
                if isinstance(payload, dict) and payload:
                    return payload
            except Exception as exc:  # noqa: BLE001
                errors_by_base.append(f"{base}: {exc}")
                continue
        if errors_by_base:
            print(f"[data] get_option_contract failed for {option_symbol}: {' | '.join(errors_by_base)}")
        return {}

    def get_latest_option_ask(self, option_symbol: str) -> float | None:
        quote = self.get_latest_option_quote(option_symbol)
        ask = quote.get("ask")
        return float(ask) if ask is not None else None

    def get_latest_option_bid(self, option_symbol: str) -> float | None:
        quote = self.get_latest_option_quote(option_symbol)
        bid = quote.get("bid")
        return float(bid) if bid is not None else None

    def get_latest_option_quote(self, option_symbol: str) -> dict[str, float | None]:
        last_exc: Exception | None = None
        try:
            resp = self.data_session.get(
                f"{self.data_base_url}/v1beta1/options/quotes/latest",
                params={"symbols": option_symbol, "feed": "indicative"},
                timeout=15,
            )
            resp.raise_for_status()
            body = resp.json()
            quote_map = body.get("quotes", {}) if isinstance(body, dict) else {}
            quote = quote_map.get(option_symbol) if isinstance(quote_map, dict) else None
            if quote:
                bid = quote.get("bp")
                ask = quote.get("ap")
                return {
                    "bid": float(bid) if bid is not None else None,
                    "ask": float(ask) if ask is not None else None,
                }
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
        if last_exc is not None:
            print(f"[data] get_latest_option_quote failed for {option_symbol}: {last_exc}")
        return {"bid": None, "ask": None}

    def get_top_movers(self, top: int = 20) -> tuple[list[str], list[str]]:
        # Alpaca's movers endpoint takes market_type in the URL path, not query params.
        # Also cap "top" to avoid 400s on unsupported large values.
        capped_top = max(1, min(int(top or 20), 50))
        endpoint = f"{self.data_base_url}/v1beta1/screener/stocks/movers"

        last_exc: Exception | None = None
        for params in ({"top": capped_top}, {}):
            try:
                resp = self.data_session.get(
                    endpoint,
                    params=params,
                    timeout=15,
                )
                resp.raise_for_status()
                body = resp.json()
                gainers = [item.get("symbol", "") for item in body.get("gainers", []) if item.get("symbol")]
                losers = [item.get("symbol", "") for item in body.get("losers", []) if item.get("symbol")]
                return gainers, losers
            except Exception as exc:  # noqa: BLE001
                last_exc = exc

        if last_exc is not None:
            raise last_exc
        return [], []

    def get_all_optionable_tickers(self, *, max_count: int | None = None) -> list[str]:
        """
        Return tradable option-enabled US equity symbols from Alpaca assets.

        The endpoint is paginated, so this method follows `next_page_token`.
        """
        cap = int(max_count) if (max_count is not None and int(max_count) > 0) else 0
        params: dict[str, Any] = {
            "status": "active",
            "asset_class": "us_equity",
            "tradable": "true",
        }
        symbols: list[str] = []
        seen: set[str] = set()
        next_page_token: str | None = None

        while True:
            request_params = dict(params)
            if next_page_token:
                request_params["page_token"] = next_page_token
            resp = self.trade_session.get(
                f"{self.base_url}/v2/assets",
                params=request_params,
                timeout=15,
            )
            resp.raise_for_status()
            payload = resp.json()
            items = payload.get("assets", []) if isinstance(payload, dict) else []
            for item in items:
                if not isinstance(item, dict):
                    continue
                symbol = str(item.get("symbol", "") or "").strip().upper()
                if not symbol or symbol in seen:
                    continue
                if not bool(item.get("tradable", True)):
                    continue
                if not bool(item.get("options_enabled", False)):
                    continue
                seen.add(symbol)
                symbols.append(symbol)
                if cap and len(symbols) >= cap:
                    return symbols
            next_page_token = payload.get("next_page_token") if isinstance(payload, dict) else None
            if not next_page_token:
                break

        return symbols

    def get_asset(self, symbol: str) -> dict[str, Any]:
        resp = self.trade_session.get(f"{self.base_url}/v2/assets/{symbol}", timeout=15)
        resp.raise_for_status()
        return resp.json()

    def has_earnings_within_days(self, symbol: str, days: int, now_et: datetime) -> bool:
        """
        Returns True if the symbol has an earnings announcement from today through
        `days` calendar days forward.

        This intentionally blocks the report date but not the day after. Once the
        announcement is in the past, post-earnings momentum is eligible again.
        Uses yfinance as the data source - free, no API key required.
        Wraps in try/except so a lookup failure never blocks a trade.
        """
        if str(symbol).upper() in _EARNINGS_SKIP_SYMBOLS:
            return False
        if not _yfinance_fallback_enabled():
            return False
        try:
            ticker = _yfinance_ticker(symbol)
            cal = ticker.calendar  # dict with keys like 'Earnings Date'
            if cal is None:
                return False

            # yfinance returns a dict; earnings date may be a list or single Timestamp
            earnings_dates = cal.get("Earnings Date") or cal.get("earnings_date") or []
            if not isinstance(earnings_dates, (list, tuple)):
                earnings_dates = [earnings_dates]

            window_end = now_et.date() + timedelta(days=days)
            for ed in earnings_dates:
                if ed is None:
                    continue
                # Convert to date whether it's a Timestamp, datetime, or date
                if hasattr(ed, "date"):
                    ed_date = ed.date()
                else:
                    ed_date = ed
                if now_et.date() <= ed_date <= window_end:
                    return True
            return False
        except Exception:
            # If yfinance fails, do NOT block the trade - log silently
            return False

    def has_high_impact_news(
        self,
        symbol: str,
        now_et: datetime,
        lookback_minutes: int,
        keywords: tuple[str, ...],
    ) -> tuple[bool, str]:
        """
        Returns (blocked, reason) if recent high-impact headlines are detected.
        Uses yfinance news feed as a lightweight proxy.
        """
        if not _yfinance_fallback_enabled():
            return False, ""
        try:
            ticker = _yfinance_ticker(symbol)
            news_items = getattr(ticker, "news", None) or []
            if not isinstance(news_items, list):
                return False, ""
            cutoff = now_et - timedelta(minutes=max(1, lookback_minutes))
            keyword_set = tuple(k.lower() for k in keywords if k)
            for item in news_items[:25]:
                if not isinstance(item, dict):
                    continue
                published_raw = item.get("providerPublishTime")
                if published_raw is None:
                    continue
                try:
                    published_dt = datetime.fromtimestamp(int(published_raw), tz=pytz.UTC).astimezone(
                        pytz.timezone(config.EASTERN_TZ)
                    )
                except Exception:
                    continue
                if published_dt < cutoff:
                    continue
                title = str(item.get("title", "") or "")
                lower_title = title.lower()
                hit = next((k for k in keyword_set if k and k in lower_title), "")
                if hit:
                    return True, f"recent news keyword '{hit}' ({title[:80]})"
            return False, ""
        except Exception:
            return False, ""
