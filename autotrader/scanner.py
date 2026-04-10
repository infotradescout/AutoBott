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
_CATALYST_MODE_ACTIVE = False
_CATALYST_MODE_REASON = ""
SCAN_LOG_PATH = Path(config.SCAN_LOG_CSV_PATH)
SCAN_LOG_COLUMNS = [
    "timestamp",
    "symbol",
    "result",
    "direction",
    "rvol",
    "rsi",
    "roc",
    "iv_rank",
    "regime_score",
    "signal_score",
    "flow_score",
    "htf_reason",
    "reason",
]
OBSERVATION_LOG_PATH = Path(config.OBSERVATION_LOG_CSV_PATH)
OBSERVATION_LOG_COLS = [
    "date",
    "symbol",
    "open_range_high",
    "open_range_low",
    "open_range_pct",
    "early_rvol",
    "early_volume",
    "hot",
]


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


def set_catalyst_mode(active: bool, reason: str = "") -> None:
    global _CATALYST_MODE_ACTIVE, _CATALYST_MODE_REASON
    _CATALYST_MODE_ACTIVE = bool(active)
    _CATALYST_MODE_REASON = reason or ""


def _historical_regime_score(daily_bars_df: pd.DataFrame) -> tuple[float, str]:
    if daily_bars_df is None or daily_bars_df.empty or len(daily_bars_df) < 30:
        return 0.0, "insufficient daily history"

    closes = daily_bars_df["close"].astype(float)
    highs = daily_bars_df["high"].astype(float)
    lows = daily_bars_df["low"].astype(float)
    vols = daily_bars_df["volume"].astype(float)
    last_close = float(closes.iloc[-1])
    if last_close <= 0:
        return 0.0, "invalid daily close"

    sma20 = float(closes.tail(20).mean())
    sma50 = float(closes.tail(50).mean()) if len(closes) >= 50 else float(closes.mean())
    trend_strength_pct = abs((sma20 - sma50) / sma50 * 100) if sma50 > 0 else 0.0

    log_ret = np.log(closes / closes.shift(1))
    rv20 = float(log_ret.tail(20).std() * (252**0.5) * 100) if len(log_ret.dropna()) >= 20 else 0.0
    range20 = float((((highs - lows) / closes.replace(0, np.nan)) * 100).tail(20).mean())
    avg_vol20 = float(vols.tail(20).mean())

    score = 0.0
    if trend_strength_pct >= 0.5:
        score += 1.0
    if trend_strength_pct >= 1.0:
        score += 1.0
    if 12.0 <= rv20 <= 65.0:
        score += 1.0
    if range20 >= 1.0:
        score += 1.0
    if avg_vol20 >= 1_000_000:
        score += 1.0

    reason = (
        f"trend={trend_strength_pct:.2f}% rv20={rv20:.1f}% "
        f"range20={range20:.2f}% vol20={avg_vol20:,.0f}"
    )
    return round(score, 2), reason


def _combined_signal_score(
    rvol: float,
    atr_pct: float,
    roc: float,
    iv_rank: float,
    regime_score: float,
    ema_aligned: bool = True,
) -> float:
    score = 0.0
    score += min(2.0, max(0.0, rvol / 2.0))
    score += min(1.5, max(0.0, atr_pct / 2.0))
    score += min(1.5, max(0.0, abs(roc) / 1.5))
    iv_center_distance = abs(iv_rank - 40.0)
    score += max(0.0, 1.0 - (iv_center_distance / 40.0))
    score += min(5.0, max(0.0, regime_score))
    # EMA alignment is a bonus, not a blocker
    if ema_aligned:
        score += 1.0
    return round(score, 2)


def _htf_trend_confirmation(
    symbol: str,
    direction: str,
    data_client: AlpacaDataClient,
) -> tuple[bool, str]:
    bars = data_client.get_stock_bars(
        symbol=symbol,
        limit=max(20, int(config.HTF_LOOKBACK_BARS)),
        timeframe=str(config.HTF_TIMEFRAME),
    )
    if bars is None or bars.empty or len(bars) < 12:
        return False, "insufficient HTF bars"

    closes = bars["close"].astype(float)
    ema9 = closes.ewm(span=9, adjust=False).mean()
    ema21 = closes.ewm(span=21, adjust=False).mean()
    if len(ema9) < 6 or len(ema21) < 6:
        return False, "HTF EMA unavailable"

    last_ema9 = float(ema9.iloc[-1])
    last_ema21 = float(ema21.iloc[-1])
    prev_ema21 = float(ema21.iloc[-4])
    slope_pct = ((last_ema21 - prev_ema21) / prev_ema21 * 100) if prev_ema21 != 0 else 0.0

    if direction == "call":
        ok = last_ema9 > last_ema21 and slope_pct >= 0
    else:
        ok = last_ema9 < last_ema21 and slope_pct <= 0
    relation = "above" if last_ema9 > last_ema21 else "below"
    reason = f"HTF ema9 {relation} ema21, ema21_slope={slope_pct:+.2f}%"
    return ok, reason


def _order_flow_score(symbol: str, data_client: AlpacaDataClient) -> float | None:
    quote = data_client.get_latest_stock_quote(symbol)
    bid = _safe_float(quote.get("bid"))
    ask = _safe_float(quote.get("ask"))
    bid_size = _safe_float(quote.get("bid_size"))
    ask_size = _safe_float(quote.get("ask_size"))
    if bid is None or ask is None or bid <= 0 or ask <= 0 or ask <= bid:
        return None

    size_imbalance = 0.0
    if bid_size is not None and ask_size is not None and (bid_size + ask_size) > 0:
        size_imbalance = (bid_size - ask_size) / (bid_size + ask_size)

    trade_price = data_client.get_latest_stock_trade_price(symbol)
    mid = (bid + ask) / 2
    spread = ask - bid
    trade_vs_mid = 0.0
    if trade_price is not None and spread > 0:
        trade_vs_mid = (trade_price - mid) / (spread / 2)
        trade_vs_mid = max(-1.0, min(1.0, trade_vs_mid))

    score = (0.6 * size_imbalance) + (0.4 * trade_vs_mid)
    return round(float(score), 4)


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

    if data_client is not None and symbol is not None:
        try:
            daily_df = data_client.get_stock_daily_bars(symbol, limit=260)
            if daily_df is not None and len(daily_df) >= 30:
                closes = daily_df["close"].astype(float)
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
            pass

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
    if bars_df is None or bars_df.empty or len(bars_df) < config.SCAN_MIN_BARS:
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
    if not is_at_or_after(now_et, config.RVOL_IGNORE_AFTER):
        effective_rvol_min = float(config.CATALYST_RELAXED_RVOL_MIN) if _CATALYST_MODE_ACTIVE else float(config.RVOL_MIN)
        if is_at_or_after(now_et, config.RVOL_RELAX_AFTER):
            effective_rvol_min = min(effective_rvol_min, float(config.RVOL_RELAXED_MIN))
        if rvol < effective_rvol_min:
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
    vwap_band = getattr(config, "VWAP_NEUTRAL_BAND_PCT", 0.05)
    distance_pct = abs(price - vwap) / vwap * 100
    if distance_pct <= vwap_band:
        return _scan_failure(f"price near VWAP ({distance_pct:.2f}%)")

    above_vwap = price > vwap
    below_vwap = price < vwap
    direction = "call" if above_vwap else "put"

    htf_reason = ""
    if config.ENABLE_HTF_CONFIRM:
        htf_ok, htf_reason = _htf_trend_confirmation(symbol=symbol, direction=direction, data_client=data_client)
        if not htf_ok:
            return _scan_failure(f"HTF trend mismatch: {htf_reason}")

    flow_score: float | None = None
    if config.ENABLE_ORDER_FLOW_FILTER:
        flow_score = _order_flow_score(symbol=symbol, data_client=data_client)
        if flow_score is None:
            return _scan_failure("order flow unavailable")
        threshold = float(config.MIN_FLOW_SCORE)
        if direction == "call" and flow_score < threshold:
            return _scan_failure(f"order flow weak for call ({flow_score:+.2f})")
        if direction == "put" and flow_score > -threshold:
            return _scan_failure(f"order flow weak for put ({flow_score:+.2f})")

    roc = calculate_roc(closes, period=config.ROC_PERIOD)
    if config.ENABLE_ROC_FILTER:
        if math.isnan(roc):
            return _scan_failure("ROC unavailable")
        if direction == "call" and roc <= config.ROC_BULL_MIN:
            return _scan_failure(f"ROC {roc:+.2f}% too weak for call")
        if direction == "put" and roc >= config.ROC_BEAR_MAX:
            return _scan_failure(f"ROC {roc:+.2f}% too weak for put")
    elif math.isnan(roc):
        roc = 0.0

    # EMA crossover — soft scored check, not a hard blocker.
    # EMA9 lags on gap-and-hold moves; removing hard fail lets those signals through
    # while still rewarding aligned setups via signal_score bonus.
    ema9 = closes.ewm(span=9, adjust=False).mean()
    ema21 = closes.ewm(span=21, adjust=False).mean()
    ema_aligned = False
    ema_note = "EMA N/A"
    if len(ema9) >= 3 and len(ema21) >= 3:
        ema_bull = ema9.iloc[-1] > ema21.iloc[-1]
        ema_bear = ema9.iloc[-1] < ema21.iloc[-1]
        if direction == "call":
            ema_aligned = ema_bull
            ema_note = "EMA bullish" if ema_bull else "EMA not yet crossed (scored)"
        else:
            ema_aligned = ema_bear
            ema_note = "EMA bearish" if ema_bear else "EMA not yet crossed (scored)"

    rsi_period = 14
    rsi = calculate_rsi(closes, period=rsi_period)
    if math.isnan(rsi):
        rsi_period = min(14, max(int(config.RSI_EARLY_MIN_PERIOD), len(closes) - 1))
        if rsi_period >= 2:
            rsi = calculate_rsi(closes, period=rsi_period)
    if math.isnan(rsi):
        if is_at_or_after(now_et, config.RSI_STRICT_AFTER_TIME) and not (
            _CATALYST_MODE_ACTIVE and config.CATALYST_DISABLE_RSI
        ):
            return _scan_failure("RSI unavailable")
        rsi = 50.0
    if config.ENABLE_RSI_FILTER and not (_CATALYST_MODE_ACTIVE and config.CATALYST_DISABLE_RSI):
        if direction == "call" and not (float(config.RSI_CALL_MIN) <= rsi <= float(config.RSI_CALL_MAX)):
            return _scan_failure(
                f"RSI {rsi:.0f} outside call range ({float(config.RSI_CALL_MIN):.0f}-{float(config.RSI_CALL_MAX):.0f})"
            )
        if direction == "put" and not (float(config.RSI_PUT_MIN) <= rsi <= float(config.RSI_PUT_MAX)):
            return _scan_failure(
                f"RSI {rsi:.0f} outside put range ({float(config.RSI_PUT_MIN):.0f}-{float(config.RSI_PUT_MAX):.0f})"
            )

    expiry_gte = _add_trading_days(now_et.date(), config.MIN_DTE_TRADING_DAYS)
    expiry_lte = _add_trading_days(now_et.date(), config.MAX_DTE_TRADING_DAYS)
    iv_value: float | None = None
    iv_rank: float | None = None
    try:
        chain = data_client.get_option_contracts(
            underlying_symbol=symbol,
            contract_type=direction,
            expiration_date_gte=expiry_gte,
            expiration_date_lte=expiry_lte,
        )
    except Exception:
        chain = []

    if chain:
        iv_value, iv_rank = _calculate_iv_rank_from_contracts(
            chain, price=price, data_client=data_client, symbol=symbol
        )

    if iv_rank is None:
        iv_rank = 50.0

    effective_iv_rank_max = (
        float(config.CATALYST_RELAXED_IV_RANK_MAX) if _CATALYST_MODE_ACTIVE else float(config.IV_RANK_MAX)
    )
    if iv_rank > effective_iv_rank_max:
        return _scan_failure(f"IV Rank {iv_rank:.0f}% too high (max {effective_iv_rank_max:.0f}%)")

    if config.ENABLE_NEWS_EVENT_BLOCK:
        blocked, news_reason = data_client.has_high_impact_news(
            symbol=symbol,
            now_et=now_et,
            lookback_minutes=int(config.NEWS_LOOKBACK_MINUTES),
            keywords=tuple(config.NEWS_BLOCK_KEYWORDS),
        )
        if blocked:
            return _scan_failure(f"news/event block: {news_reason}")

    regime_score, regime_reason = _historical_regime_score(daily_bars_df)
    if config.ENABLE_HISTORICAL_REGIME_SCORE and regime_score < float(config.MIN_HISTORICAL_REGIME_SCORE):
        return _scan_failure(
            f"historical regime score {regime_score:.2f} below {config.MIN_HISTORICAL_REGIME_SCORE:.2f}"
        )

    signal_score = _combined_signal_score(
        rvol=float(rvol),
        atr_pct=float(atr_pct),
        roc=float(roc),
        iv_rank=float(iv_rank),
        regime_score=float(regime_score),
        ema_aligned=ema_aligned,
    )
    effective_min_signal_score = (
        float(config.CATALYST_RELAXED_MIN_SIGNAL_SCORE) if _CATALYST_MODE_ACTIVE else float(config.MIN_SIGNAL_SCORE)
    )
    if config.ENABLE_SIGNAL_SCORING and signal_score < effective_min_signal_score:
        return _scan_failure(f"signal score {signal_score:.2f} below {effective_min_signal_score:.2f}")

    above_below = "Above VWAP" if direction == "call" else "Below VWAP"
    return {
        "symbol": symbol,
        "direction": direction,
        "rvol": round(rvol, 2),
        "atr_pct": round(atr_pct, 2),
        "rsi": round(rsi, 2),
        "rsi_period": rsi_period,
        "roc": round(roc, 2),
        "vwap": round(vwap, 4),
        "price": round(price, 4),
        "iv": round(iv_value, 4) if iv_value is not None else None,
        "iv_rank": round(iv_rank, 2),
        "regime_score": round(regime_score, 2),
        "signal_score": round(signal_score, 2),
        "flow_score": round(flow_score, 4) if flow_score is not None else None,
        "htf_reason": htf_reason,
        "reason": (
            f"RVOL {rvol:.1f}x | {above_below} | {ema_note} | "
            f"ROC {roc:+.2f}% | IVR {iv_rank:.0f}% | Regime {regime_score:.2f} | "
            f"Flow {(flow_score if flow_score is not None else 0):+.2f} | Score {signal_score:.2f}"
        ) + (f" | Catalyst {_CATALYST_MODE_REASON}" if _CATALYST_MODE_ACTIVE and _CATALYST_MODE_REASON else ""),
        "regime_reason": regime_reason,
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
                        "regime_score": item.get("regime_score", ""),
                        "signal_score": item.get("signal_score", ""),
                        "flow_score": item.get("flow_score", ""),
                        "htf_reason": item.get("htf_reason", ""),
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
                        "regime_score": "",
                        "signal_score": "",
                        "flow_score": "",
                        "htf_reason": "",
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


def run_observation_phase(watchlist: list[str], data_client: AlpacaDataClient, now_et: datetime) -> list[str]:
    """
    Run from 9:30-10:00 ET. Calculates opening range and early RVOL for each ticker.
    Returns a hot list sorted by RVOL descending and saves observations to CSV.
    """
    today_str = now_et.date().isoformat()
    observations: list[dict[str, Any]] = []

    for symbol in watchlist:
        try:
            bars_df = data_client.get_intraday_bars_since_open(symbol=symbol, now_et=now_et, limit=60)
            if bars_df is None or bars_df.empty or len(bars_df) < 3:
                time.sleep(config.RATE_LIMIT_SLEEP_SECONDS)
                continue

            daily_df = data_client.get_stock_daily_bars(symbol, limit=25)
            or_high = float(bars_df["high"].max())
            or_low = float(bars_df["low"].min())
            or_pct = ((or_high - or_low) / or_low * 100) if or_low > 0 else 0.0

            today_vol = float(bars_df["volume"].astype(float).sum())
            minutes_elapsed = max(1, len(bars_df) * 5)
            rvol = (
                calculate_rvol(symbol, today_vol, daily_df, minutes_elapsed)
                if daily_df is not None and not daily_df.empty
                else float("nan")
            )
            is_hot = (not math.isnan(rvol)) and rvol >= config.RVOL_MIN and or_pct >= config.ATR_PCT_MIN

            observations.append(
                {
                    "date": today_str,
                    "symbol": symbol,
                    "open_range_high": round(or_high, 4),
                    "open_range_low": round(or_low, 4),
                    "open_range_pct": round(or_pct, 2),
                    "early_rvol": round(rvol, 2) if not math.isnan(rvol) else "",
                    "early_volume": int(today_vol),
                    "hot": "yes" if is_hot else "no",
                }
            )
        except Exception:
            pass
        time.sleep(config.RATE_LIMIT_SLEEP_SECONDS)

    if observations:
        import csv

        write_header = not OBSERVATION_LOG_PATH.exists()
        with OBSERVATION_LOG_PATH.open("a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=OBSERVATION_LOG_COLS)
            if write_header:
                writer.writeheader()
            writer.writerows(observations)

    hot = [
        o["symbol"]
        for o in sorted(
            [o for o in observations if o["hot"] == "yes"],
            key=lambda x: float(x["early_rvol"]) if x["early_rvol"] else 0,
            reverse=True,
        )
    ]
    print(f"[Observation] {len(hot)} hot tickers: {hot}")
    return hot
