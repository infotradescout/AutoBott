"""Intraday scanner for options-tradable momentum names."""

from __future__ import annotations

import math
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytz

import config
from data import AlpacaDataClient
from risk import is_at_or_after

_DEFAULT_SCANNER: "IntradayScanner | None" = None
SCAN_LOG_PATH = Path(__file__).resolve().parent / "scan_log.csv"
SCAN_LOG_COLUMNS = ["timestamp", "symbol", "result", "direction", "rvol", "rsi", "roc", "iv_rank", "reason"]


def calculate_rsi(closes: pd.Series, period: int = 14) -> float:
    delta = closes.diff()
    gains = delta.clip(lower=0)
    losses = -delta.clip(upper=0)
    avg_gain = gains.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = losses.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, pd.NA)
    rsi = 100 - (100 / (1 + rs))
    last = rsi.iloc[-1]
    return float(last) if pd.notna(last) else float("nan")


def calculate_roc(closes: pd.Series, period: int = 10) -> float:
    if len(closes) <= period:
        return float("nan")
    prev = float(closes.iloc[-period - 1])
    curr = float(closes.iloc[-1])
    if prev == 0:
        return float("nan")
    return ((curr - prev) / prev) * 100


def calculate_vwap(bars_df: pd.DataFrame) -> float:
    if bars_df.empty:
        return float("nan")
    typical = (bars_df["high"] + bars_df["low"] + bars_df["close"]) / 3
    volume = bars_df["volume"]
    denom = volume.sum()
    if denom <= 0:
        return float("nan")
    return float((typical * volume).sum() / denom)


def calculate_atr(symbol: str, daily_bars_df: pd.DataFrame, period: int = 14) -> float:
    if daily_bars_df is None or daily_bars_df.empty or len(daily_bars_df) < period + 1:
        return float("nan")
    high = daily_bars_df["high"].astype(float)
    low = daily_bars_df["low"].astype(float)
    close = daily_bars_df["close"].astype(float)
    prev_close = close.shift(1)
    tr = pd.concat([(high - low), (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
    atr = tr.rolling(period).mean()
    return float(atr.iloc[-1]) if pd.notna(atr.iloc[-1]) else float("nan")


def calculate_rvol(symbol: str, today_volume: float, daily_bars_df: pd.DataFrame, minutes_since_open: int) -> float:
    if daily_bars_df is None or daily_bars_df.empty or len(daily_bars_df) < 20:
        return float("nan")
    avg_daily_volume = float(daily_bars_df["volume"].tail(20).astype(float).mean())
    if avg_daily_volume <= 0:
        return float("nan")
    scaled = avg_daily_volume * max(minutes_since_open, 1) / 390
    if scaled <= 0:
        return float("nan")
    return float(today_volume / scaled)


def _scan_failure(reason: str) -> dict[str, Any]:
    return {"failed": True, "reason": reason}


def _safe_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _add_trading_days(start: date, days: int) -> date:
    cursor = start
    count = 0
    while count < days:
        cursor += timedelta(days=1)
        if cursor.weekday() < 5:
            count += 1
    return cursor


def _calculate_iv_rank_from_contracts(
    contracts: list[dict[str, Any]],
    price: float,
    data_client: "AlpacaDataClient | None" = None,
    symbol: str | None = None,
) -> tuple[float | None, float | None]:
    """
    Returns (atm_iv, iv_rank_pct).

    iv_rank_pct is a 0-100 score:
      - Uses 52-week daily bars of the underlying to compute rolling 30-day
        annualized realized volatility, then ranks the ATM IV against that
        historical RV range as a proxy for true IV Rank.
      - Falls back to 50.0 (neutral) if historical data is unavailable.
    """
    # Step 1: find ATM IV from today's chain
    closest_iv: float | None = None
    closest_dist = float("inf")
    for contract in contracts:
        strike = _safe_float(contract.get("strike_price"))
        iv = _safe_float(contract.get("implied_volatility")) or _safe_float(contract.get("iv"))
        if iv is None or strike is None:
            continue
        dist = abs(strike - price)
        if dist < closest_dist:
            closest_dist = dist
            closest_iv = iv

    if closest_iv is None:
        return None, None

    # Step 2: try to compute IV rank from 52-week HV history
    if data_client is not None and symbol is not None:
        try:
            daily_df = data_client.get_stock_daily_bars(symbol, limit=260)  # ~52 weeks
            if daily_df is not None and len(daily_df) >= 30:
                closes = daily_df["close"].astype(float)
                # 30-day rolling annualized realized volatility
                log_returns = (closes / closes.shift(1)).apply(lambda x: x**0.5 if x > 0 else float("nan"))
                log_ret = np.log(closes / closes.shift(1))
                rv_series = log_ret.rolling(30).std() * (252**0.5)
                rv_clean = rv_series.dropna()
                if len(rv_clean) >= 10:
                    rv_min = float(rv_clean.min())
                    rv_max = float(rv_clean.max())
                    if rv_max > rv_min:
                        iv_rank = ((closest_iv - rv_min) / (rv_max - rv_min)) * 100
                        iv_rank = max(0.0, min(100.0, iv_rank))
                        return closest_iv, round(iv_rank, 2)
        except Exception:
            pass  # fall through to neutral default

    # Fallback: return neutral rank so IV check doesn't block the trade
    return closest_iv, 50.0


def scan_ticker(
    symbol: str,
    bars_df: pd.DataFrame,
    daily_bars_df: pd.DataFrame,
    today_volume: float,
    data_client: AlpacaDataClient | None = None,
) -> dict | None:
    active_client = data_client or (_DEFAULT_SCANNER.data_client if _DEFAULT_SCANNER is not None else None)
    if active_client is None:
        return None
    details = _scan_ticker_details(
        symbol=symbol,
        bars_df=bars_df,
        daily_bars_df=daily_bars_df,
        today_volume=today_volume,
        data_client=active_client,
    )
    if details.get("failed"):
        return None
    return details


def _scan_ticker_details(
    symbol: str,
    bars_df: pd.DataFrame,
    daily_bars_df: pd.DataFrame,
    today_volume: float,
    data_client: AlpacaDataClient,
) -> dict[str, Any]:
    if bars_df is None or bars_df.empty or len(bars_df) < 30:
        return _scan_failure("insufficient intraday bars")
    if daily_bars_df is None or daily_bars_df.empty or len(daily_bars_df) < 20:
        return _scan_failure("insufficient daily bars")

    bars = bars_df.copy()
    closes = bars["close"].astype(float)
    price = float(closes.iloc[-1])

    if price < config.MIN_SHARE_PRICE:
        return _scan_failure(f"price ${price:.2f} below ${config.MIN_SHARE_PRICE}")
    if price > config.MAX_SHARE_PRICE:
        return _scan_failure(f"price ${price:.2f} above ${config.MAX_SHARE_PRICE}")

    ts_last = bars["timestamp"].iloc[-1]
    ts_last = ts_last if isinstance(ts_last, datetime) else pd.to_datetime(ts_last).to_pydatetime()
    if ts_last.tzinfo is None:
        ts_last = pytz.UTC.localize(ts_last)
    ts_last_et = ts_last.astimezone(pytz.timezone(config.EASTERN_TZ))
    market_open = ts_last_et.replace(hour=9, minute=30, second=0, microsecond=0)
    minutes_since_open = max(1, int((ts_last_et - market_open).total_seconds() // 60))
    now_et = ts_last_et

    try:
        if data_client.has_earnings_within_days(symbol, config.EARNINGS_LOOKAHEAD_DAYS, now_et=now_et):
            return _scan_failure(f"earnings within {config.EARNINGS_LOOKAHEAD_DAYS} days")
    except Exception as exc:  # noqa: BLE001
        if config.EARNINGS_CHECK_STRICT:
            return _scan_failure(f"earnings check failed: {exc}")

    rvol = calculate_rvol(
        symbol=symbol,
        today_volume=today_volume,
        daily_bars_df=daily_bars_df,
        minutes_since_open=minutes_since_open,
    )
    if math.isnan(rvol):
        return _scan_failure("rvol unavailable")
    if rvol < config.RVOL_MIN:
        return _scan_failure(f"RVOL {rvol:.1f}x (too low)")

    atr = calculate_atr(symbol=symbol, daily_bars_df=daily_bars_df, period=14)
    if math.isnan(atr) or price <= 0:
        return _scan_failure("ATR unavailable")
    atr_pct = (atr / price) * 100
    if atr_pct < config.ATR_PCT_MIN:
        return _scan_failure(f"ATR {atr_pct:.2f}% (too low)")

    vwap = calculate_vwap(bars)
    if math.isnan(vwap) or vwap <= 0:
        return _scan_failure("VWAP unavailable")
    distance_pct = abs(price - vwap) / vwap * 100
    if distance_pct <= config.VWAP_NEUTRAL_BAND_PCT:
        return _scan_failure(f"price near VWAP ({distance_pct:.2f}%)")

    last3 = closes.tail(3)
    above_vwap = price > vwap and (last3 > vwap).all()
    below_vwap = price < vwap and (last3 < vwap).all()
    if not above_vwap and not below_vwap:
        return _scan_failure("VWAP direction not clean")
    direction = "call" if above_vwap else "put"

    roc = calculate_roc(closes, period=config.ROC_PERIOD)
    if math.isnan(roc):
        return _scan_failure("ROC unavailable")
    if direction == "call" and roc <= config.ROC_BULL_MIN:
        return _scan_failure(f"ROC {roc:+.2f}% too weak for call")
    if direction == "put" and roc >= config.ROC_BEAR_MAX:
        return _scan_failure(f"ROC {roc:+.2f}% too weak for put")

    ema9 = closes.ewm(span=9, adjust=False).mean()
    ema21 = closes.ewm(span=21, adjust=False).mean()
    if len(ema9) < 4 or len(ema21) < 4:
        return _scan_failure("EMA unavailable")
    ema_bull = ema9.iloc[-1] > ema21.iloc[-1] and ema9.iloc[-1] > ema9.iloc[-4] and ema21.iloc[-1] > ema21.iloc[-4]
    ema_bear = ema9.iloc[-1] < ema21.iloc[-1] and ema9.iloc[-1] < ema9.iloc[-4] and ema21.iloc[-1] < ema21.iloc[-4]
    if direction == "call" and not ema_bull:
        return _scan_failure("EMA not bullish")
    if direction == "put" and not ema_bear:
        return _scan_failure("EMA not bearish")

    rsi = calculate_rsi(closes, period=14)
    if math.isnan(rsi):
        return _scan_failure("RSI unavailable")
    if direction == "call" and not (50 <= rsi <= 72):
        return _scan_failure(f"RSI {rsi:.0f} outside call range")
    if direction == "put" and not (28 <= rsi <= 50):
        return _scan_failure(f"RSI {rsi:.0f} outside put range")

    expiry_gte = _add_trading_days(now_et.date(), config.MIN_DTE_TRADING_DAYS)
    expiry_lte = _add_trading_days(now_et.date(), config.MAX_DTE_TRADING_DAYS)
    try:
        chain = data_client.get_option_contracts(
            underlying_symbol=symbol,
            contract_type=direction,
            expiration_date_gte=expiry_gte,
            expiration_date_lte=expiry_lte,
        )
    except Exception as exc:  # noqa: BLE001
        return _scan_failure(f"IV data unavailable: {exc}")
    if not chain:
        return _scan_failure("no option chain for IV check")

    iv_value, iv_rank = _calculate_iv_rank_from_contracts(
        chain, price=price, data_client=data_client, symbol=symbol
    )
    if iv_rank is None:
        return _scan_failure("IV rank unavailable")
    if iv_rank > config.IV_RANK_MAX:
        return _scan_failure(f"IV Rank {iv_rank:.0f}% too high")
    if iv_rank < config.IV_RANK_MIN:
        return _scan_failure(f"IV Rank {iv_rank:.0f}% too low")

    above_below = "Above VWAP" if direction == "call" else "Below VWAP"
    return {
        "symbol": symbol,
        "direction": direction,
        "rvol": round(rvol, 2),
        "atr_pct": round(atr_pct, 2),
        "rsi": round(rsi, 2),
        "roc": round(roc, 2),
        "vwap": round(vwap, 4),
        "price": round(price, 4),
        "iv": round(iv_value, 4) if iv_value is not None else None,
        "iv_rank": round(iv_rank, 2),
        "reason": (
            f"RVOL {rvol:.1f}x | {above_below} | EMA {'bullish' if direction == 'call' else 'bearish'} | "
            f"ROC {roc:+.2f}% | IVR {iv_rank:.0f}%"
        ),
    }


class IntradayScanner:
    def __init__(self, data_client: AlpacaDataClient):
        self.data_client = data_client
        self.tz = pytz.timezone(config.EASTERN_TZ)
        self.last_failures: list[dict[str, str]] = []

    def build_watchlist(self) -> list[str]:
        base = list(config.CORE_TICKERS)
        gainers: list[str] = []
        losers: list[str] = []
        try:
            gainers, losers = self.data_client.get_top_movers(top=config.SCREENER_TOP_N)
        except Exception as exc:  # noqa: BLE001
            print(f"[{self._ts()}] Movers endpoint unavailable ({exc}). Using core tickers only.")

        candidates = []
        candidates.extend(base)
        candidates.extend(gainers[: config.MOVER_SYMBOLS_PER_SIDE])
        candidates.extend(losers[: config.MOVER_SYMBOLS_PER_SIDE])

        deduped: list[str] = []
        seen: set[str] = set()
        for sym in candidates:
            if not sym:
                continue
            usym = sym.upper()
            if usym not in seen:
                seen.add(usym)
                deduped.append(usym)

        filtered: list[str] = []
        for sym in deduped:
            try:
                price = self.data_client.get_latest_stock_price(sym)
                if price is None or price < config.MIN_SHARE_PRICE or price > config.MAX_SHARE_PRICE:
                    continue
                asset = self.data_client.get_asset(sym)
                if not asset.get("tradable", True):
                    continue
                if not asset.get("options_enabled", False):
                    continue
                filtered.append(sym)
            except Exception:
                continue
            time.sleep(config.RATE_LIMIT_SLEEP_SECONDS)

        return filtered

    def run_scan(self, watchlist: list[str]) -> list[dict]:
        now_et = datetime.now(self.tz)
        passed: list[dict] = []
        failed: list[dict[str, str]] = []

        for symbol in watchlist:
            try:
                bars_df = self.data_client.get_intraday_bars_since_open(
                    symbol=symbol,
                    now_et=now_et,
                    limit=config.SCAN_INTRADAY_BARS,
                )
                if bars_df.empty:
                    failed.append({"symbol": symbol, "reason": "no intraday bars"})
                    continue
                daily_df = self.data_client.get_stock_daily_bars(symbol, limit=config.SCAN_DAILY_BARS)
                today_volume = float(bars_df["volume"].astype(float).sum())
                details = _scan_ticker_details(
                    symbol=symbol,
                    bars_df=bars_df,
                    daily_bars_df=daily_df,
                    today_volume=today_volume,
                    data_client=self.data_client,
                )
                if details.get("failed"):
                    failed.append({"symbol": symbol, "reason": details["reason"]})
                else:
                    passed.append(details)
            except Exception as exc:  # noqa: BLE001
                failed.append({"symbol": symbol, "reason": f"scan error: {exc}"})
            time.sleep(config.RATE_LIMIT_SLEEP_SECONDS)

        passed.sort(key=lambda item: float(item["rvol"]), reverse=True)
        self.last_failures = failed
        self._print_summary(now_et, len(watchlist), passed, failed)
        return passed

    def _print_summary(self, now_et: datetime, total: int, passed: list[dict], failed: list[dict[str, str]]) -> None:
        print(f"[{now_et.strftime('%H:%M ET')}] SCAN RESULTS - {len(passed)} of {total} tickers passed")
        for item in passed:
            vwap_side = "Above VWAP" if item["direction"] == "call" else "Below VWAP"
            print(
                f"  + {item['symbol']:<5} | {item['direction'].upper():<4} | RVOL {item['rvol']:.1f}x | "
                f"RSI {item['rsi']:.0f} | ROC {item['roc']:+.1f}% | IVR {item['iv_rank']:.0f}% | {vwap_side}"
            )
        for item in failed[:8]:
            print(f"  - {item['symbol']:<5} | failed: {item['reason']}")
        self._write_scan_log(now_et, passed, failed)

    def _write_scan_log(self, now_et: datetime, passed: list[dict], failed: list[dict[str, str]]) -> None:
        import csv

        write_header = not SCAN_LOG_PATH.exists()
        with SCAN_LOG_PATH.open("a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=SCAN_LOG_COLUMNS)
            if write_header:
                writer.writeheader()
            ts_str = now_et.strftime("%Y-%m-%d %H:%M:%S %Z")
            for item in passed:
                writer.writerow(
                    {
                        "timestamp": ts_str,
                        "symbol": item["symbol"],
                        "result": "pass",
                        "direction": item.get("direction", ""),
                        "rvol": item.get("rvol", ""),
                        "rsi": item.get("rsi", ""),
                        "roc": item.get("roc", ""),
                        "iv_rank": item.get("iv_rank", ""),
                        "reason": item.get("reason", ""),
                    }
                )
            for item in failed:
                writer.writerow(
                    {
                        "timestamp": ts_str,
                        "symbol": item["symbol"],
                        "result": "fail",
                        "direction": "",
                        "rvol": "",
                        "rsi": "",
                        "roc": "",
                        "iv_rank": "",
                        "reason": item.get("reason", ""),
                    }
                )

    def _ts(self) -> str:
        return datetime.now(self.tz).strftime("%Y-%m-%d %H:%M:%S %Z")


def initialize_scanner(data_client: AlpacaDataClient) -> None:
    global _DEFAULT_SCANNER
    _DEFAULT_SCANNER = IntradayScanner(data_client)


def build_watchlist() -> list[str]:
    if _DEFAULT_SCANNER is None:
        raise RuntimeError("Scanner not initialized. Call initialize_scanner(data_client) first.")
    return _DEFAULT_SCANNER.build_watchlist()


def run_scan(watchlist: list[str]) -> list[dict]:
    if _DEFAULT_SCANNER is None:
        raise RuntimeError("Scanner not initialized. Call initialize_scanner(data_client) first.")
    return _DEFAULT_SCANNER.run_scan(watchlist)


def should_build_watchlist(now_et: datetime) -> bool:
    return is_at_or_after(now_et, config.SCAN_MORNING_TIME)
