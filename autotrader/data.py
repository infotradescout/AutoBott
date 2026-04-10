"""Market data fetching for stocks and options."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd
import pytz
import requests
import yfinance as yf

import config

_EARNINGS_SKIP_SYMBOLS = {str(s).upper() for s in config.EARNINGS_SKIP_SYMBOLS}

# Options contract data is ONLY available on the live API endpoint.
# Paper trading accounts submit orders via paper-api but must fetch
# contracts, quotes, and chain data from the live API.
_LIVE_TRADE_BASE_URL = "https://api.alpaca.markets"


class AlpacaDataClient:
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
        self.options_session = requests.Session()
        self.options_session.headers.update(
            {
                "APCA-API-KEY-ID": api_key,
                "APCA-API-SECRET-KEY": secret_key,
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

    def get_stock_bars(
        self,
        symbol: str,
        limit: int,
        timeframe: str = "5m",
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> pd.DataFrame:
        """
        Fetch OHLCV bars using yfinance.
        timeframe: yfinance interval string - "5m", "1d", "1h", etc.
        limit: max number of rows to return (most recent).
        """
        try:
            # Choose a safe period based on timeframe
            if timeframe in ("1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h"):
                period = "5d"  # yfinance allows up to 60d for minute-level, 5d is safe
            else:
                period = f"{limit + 10}d"

            ticker = yf.Ticker(symbol)
            df = ticker.history(period=period, interval=timeframe, auto_adjust=True)

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
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert(
                pytz.timezone(config.EASTERN_TZ)
            )

            # Keep only the most recent `limit` rows
            df = df.tail(limit).reset_index(drop=True)
            return df[["timestamp", "open", "high", "low", "close", "volume"]]

        except Exception:
            return pd.DataFrame()

    def get_stock_daily_bars(self, symbol: str, limit: int = 30) -> pd.DataFrame:
        """Fetch daily OHLCV bars using yfinance."""
        return self.get_stock_bars(symbol=symbol, limit=limit, timeframe="1d")

    def get_intraday_bars_since_open(self, symbol: str, now_et: datetime, limit: int = 120) -> pd.DataFrame:
        """
        Fetch 5-minute intraday bars from market open (9:30 ET) until now.
        Uses Alpaca data bars first, with yfinance fallback.
        """
        tz_et = pytz.timezone(config.EASTERN_TZ)
        today = now_et.date()
        market_open = tz_et.localize(datetime(today.year, today.month, today.day, 9, 30, 0))

        # Primary source: Alpaca bars API (more stable intraday for live trading loops)
        try:
            start_utc = market_open.astimezone(pytz.UTC).isoformat().replace("+00:00", "Z")
            end_utc = now_et.astimezone(pytz.UTC).isoformat().replace("+00:00", "Z")
            resp = self.data_session.get(
                f"{self.data_base_url}/v2/stocks/bars",
                params={
                    "symbols": symbol,
                    "timeframe": "5Min",
                    "start": start_utc,
                    "end": end_utc,
                    "limit": max(limit, 500),
                    "adjustment": "raw",
                    "feed": "iex",
                },
                timeout=15,
            )
            resp.raise_for_status()
            body = resp.json()
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
                        return df[["timestamp", "open", "high", "low", "close", "volume"]]
        except Exception:
            pass

        # Fallback: yfinance
        try:
            ticker = yf.Ticker(symbol)
            df = ticker.history(period="5d", interval="5m", auto_adjust=True)

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

        except Exception:
            return pd.DataFrame()

    def get_latest_stock_price(self, symbol: str) -> float | None:
        """Get the latest stock price using yfinance. Free, no API key."""
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.fast_info
            price = float(info.last_price)
            return price if price and price > 0 else None
        except Exception:
            return None

    def get_latest_stock_quote(self, symbol: str) -> dict[str, float | None]:
        try:
            resp = self.data_session.get(
                f"{self.data_base_url}/v2/stocks/quotes/latest",
                params={"symbols": symbol},
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
                params={"symbols": symbol},
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
        # Options contract data must come from the live API endpoint.
        # Paper API does not serve options chain data.
        params = {
            "underlying_symbols": underlying_symbol,
            "type": contract_type,
            "expiration_date_gte": expiration_date_gte.isoformat(),
            "expiration_date_lte": expiration_date_lte.isoformat(),
        }
        resp = self.options_session.get(
            f"{_LIVE_TRADE_BASE_URL}/v2/options/contracts",
            params=params,
            timeout=15,
        )
        resp.raise_for_status()
        body = resp.json()
        return body.get("option_contracts", []) or body.get("contracts", []) or []

    def get_option_contract(self, option_symbol: str) -> dict[str, Any]:
        # Always use live endpoint for contract detail lookups.
        resp = self.options_session.get(
            f"{_LIVE_TRADE_BASE_URL}/v2/options/contracts/{option_symbol}",
            timeout=15,
        )
        resp.raise_for_status()
        body = resp.json()
        return body.get("option_contract", body)

    def get_latest_option_ask(self, option_symbol: str) -> float | None:
        quote = self.get_latest_option_quote(option_symbol)
        ask = quote.get("ask")
        return float(ask) if ask is not None else None

    def get_latest_option_quote(self, option_symbol: str) -> dict[str, float | None]:
        # Option quotes also require the live endpoint.
        resp = self.options_session.get(
            f"{_LIVE_TRADE_BASE_URL}/v2/options/quotes/latest",
            params={"symbols": option_symbol},
            timeout=15,
        )
        resp.raise_for_status()
        body = resp.json()
        quote_map = body.get("quotes", {})
        quote = quote_map.get(option_symbol)
        if not quote:
            return {"bid": None, "ask": None}
        bid = quote.get("bp")
        ask = quote.get("ap")
        return {
            "bid": float(bid) if bid is not None else None,
            "ask": float(ask) if ask is not None else None,
        }

    def get_top_movers(self, top: int = 20) -> tuple[list[str], list[str]]:
        params = {"top": top, "market_type": "stocks"}
        resp = self.data_session.get(
            f"{self.data_base_url}/v1beta1/screener/stocks/movers",
            params=params,
            timeout=15,
        )
        resp.raise_for_status()
        body = resp.json()
        gainers = [item.get("symbol", "") for item in body.get("gainers", []) if item.get("symbol")]
        losers = [item.get("symbol", "") for item in body.get("losers", []) if item.get("symbol")]
        return gainers, losers

    def get_asset(self, symbol: str) -> dict[str, Any]:
        resp = self.trade_session.get(f"{self.base_url}/v2/assets/{symbol}", timeout=15)
        resp.raise_for_status()
        return resp.json()

    def has_earnings_within_days(self, symbol: str, days: int, now_et: datetime) -> bool:
        """
        Returns True if the symbol has an earnings announcement within `days` calendar days.
        Uses yfinance as the data source - free, no API key required.
        Wraps in try/except so a lookup failure never blocks a trade.
        """
        if str(symbol).upper() in _EARNINGS_SKIP_SYMBOLS:
            return False
        try:
            import yfinance as yf

            ticker = yf.Ticker(symbol)
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
        try:
            ticker = yf.Ticker(symbol)
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
