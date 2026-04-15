"""Intraday scanner for options-tradable momentum names."""

from __future__ import annotations

import math
import re
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytz

import config
from data import AlpacaDataClient
from intraday_profiles import PROFILES, enrich_signal_for_profile, is_profile_window_open
from risk import is_at_or_after

_DEFAULT_SCANNER: "IntradayScanner | None" = None
_CATALYST_MODE_ACTIVE = False
_CATALYST_MODE_REASON = ""
SCAN_LOG_PATH = Path(config.SCAN_LOG_CSV_PATH)
SCAN_LOG_COLUMNS = [
    "timestamp",
    "symbol",
    "strategy_profile",
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
_SCAN_SYMBOL_RE = re.compile(r"^[A-Z][A-Z.]{0,5}$")
_CORE_LIQUID_PROFILE_SYMBOLS = {
    "SPY", "QQQ", "IWM", "DIA", "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA", "AMD",
}


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


_STAGE_PREFIXES = frozenset({
    "hard_block",
    "universe_reject",
    "universe rejected",  # legacy spelling from run_scan callers
    "setup_reject",
    "setup rejected",     # legacy spelling
    "execution_reject",
    "profile_miss",
})


def _scan_failure(reason: str, *, stage: str = "setup_reject") -> dict[str, Any]:
    """Build a failure dict with a hierarchical stage label.

    Stages (from highest to lowest severity):
      hard_block       — earnings window, no data, price range, junk symbol
      universe_reject  — upstream checks before bar fetch (set by run_scan callers)
      setup_reject     — signal quality (RSI, ROC, ATR, score, VWAP movement)
      execution_reject — HTF trend, order flow
      profile_miss     — no named or generic profile matched
    """
    text = str(reason or "").strip()
    if not text:
        text = "unknown"
    # Pass through already-labelled strings unchanged.
    if ":" in text:
        prefix = text.split(":", 1)[0].strip()
        if prefix in _STAGE_PREFIXES:
            return {"failed": True, "reason": text}
    return {"failed": True, "reason": f"{stage}: {text}"}


def _is_obvious_junk_symbol(symbol: str) -> bool:
    sym = str(symbol or "").upper().strip()
    if not sym:
        return True
    if not _SCAN_SYMBOL_RE.match(sym):
        return True
    if len(sym) == 5 and sym[-1] in {"W", "R", "U"}:
        return True
    if "." in sym:
        suffix = sym.split(".", 1)[1]
        if suffix in {"W", "WS", "WT", "WTS", "R", "RT", "U", "UN", "UNIT"}:
            return True
    return False


def _ensure_scan_log_header() -> None:
    if not SCAN_LOG_PATH.exists():
        return
    try:
        import csv

        with SCAN_LOG_PATH.open("r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            existing_columns = list(reader.fieldnames or [])
            rows = list(reader)
        if existing_columns == SCAN_LOG_COLUMNS:
            return
        with SCAN_LOG_PATH.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=SCAN_LOG_COLUMNS)
            writer.writeheader()
            for row in rows:
                payload = {key: row.get(key, "") for key in SCAN_LOG_COLUMNS}
                writer.writerow(payload)
    except Exception as exc:  # noqa: BLE001
        print(f"[scanner] scan log header migration failed: {exc}")


def _profile_signals_for_candidate(
    *,
    base_signal: dict[str, Any],
    bars_df: pd.DataFrame,
    now_et: datetime,
    catalyst_mode_active: bool,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Evaluate all named profiles then fall back to generic continuation.

    Architecture:
      1. Named profiles (priority 1-4): tried in order.  A name matches if
         (a) symbol is eligible, (b) time window is open, (c) score gate passes,
         (d) profile logic passes.  Symbol eligibility tolerates empty-symbols
         tuples (universal) and always accepts permissive_core names.
      2. generic_intraday_continuation (priority 5): fires for permissive_core
         names ONLY when no named profile matched.  It is the safety net so that
         liquid names are *never* rejected solely because the named profiles do
         not fit.

    Returns (passed_list, rejected_reasons).  passed_list will contain at most
    one entry per profile (the best one is selected upstream in run_scan).
    """
    symbol = str(base_signal.get("symbol", "") or "").upper()
    rvol = float(base_signal.get("rvol", 0.0) or 0.0)
    roc = float(base_signal.get("roc", 0.0) or 0.0)
    signal_score = float(base_signal.get("signal_score", 0.0) or 0.0)
    direction = str(base_signal.get("direction", "") or "").lower()
    price = float(base_signal.get("price", 0.0) or 0.0)
    vwap = float(base_signal.get("vwap", 0.0) or 0.0)

    closes = bars_df["close"].astype(float)
    last2_roc = 0.0
    if len(closes) >= 3:
        prev2 = float(closes.iloc[-3] or 0.0)
        curr = float(closes.iloc[-1] or 0.0)
        if prev2 != 0:
            last2_roc = ((curr - prev2) / prev2) * 100.0
    distance_from_vwap_pct = 0.0
    if vwap > 0:
        distance_from_vwap_pct = abs(price - vwap) / vwap * 100.0

    permissive_core = set(str(s).upper() for s in getattr(config, "CORE_TICKERS", [])) | _CORE_LIQUID_PROFILE_SYMBOLS
    is_core = symbol in permissive_core

    passed: list[dict[str, Any]] = []
    rejected: list[str] = []

    # ── Named profiles (priority 1-4) ─────────────────────────────────────
    named_profiles = sorted(
        (p for p in PROFILES.values() if p.name != "generic_intraday_continuation"),
        key=lambda p: int(p.priority),
    )
    for profile in named_profiles:
        profile_symbols = set(profile.symbols)
        # Symbol gate: empty symbols = universal; core names bypass symbol list.
        if profile_symbols and symbol not in profile_symbols and not is_core:
            rejected.append(f"{profile.name}:symbol")
            continue
        # Time gate: hard-block non-core names outside window;
        # for core names, a closed time window is a soft skip (tried next profile).
        if not is_profile_window_open(now_et, profile):
            rejected.append(f"{profile.name}:time")
            continue
        # Score gate
        if signal_score < float(profile.min_signal_score):
            rejected.append(f"{profile.name}:score")
            continue

        profile_ok = False
        profile_reason = ""
        if profile.name == "open_drive_momentum":
            profile_ok = rvol >= 0.70 and abs(roc) >= 0.06 and distance_from_vwap_pct >= 0.02
            profile_reason = "open-drive momentum"
        elif profile.name == "vwap_continuation":
            profile_ok = distance_from_vwap_pct <= 1.20 and rvol >= 0.55 and abs(roc) >= 0.03
            profile_reason = "vwap continuation"
        elif profile.name == "reversal_snapback":
            if distance_from_vwap_pct >= 0.20 and abs(last2_roc) >= 0.10:
                if price > vwap and last2_roc < 0:
                    base_signal = dict(base_signal)
                    base_signal["direction"] = "put"
                elif price < vwap and last2_roc > 0:
                    base_signal = dict(base_signal)
                    base_signal["direction"] = "call"
                profile_ok = base_signal.get("direction") in ("call", "put")
            profile_reason = "reversal snapback"
        elif profile.name == "catalyst_impulse":
            profile_ok = (catalyst_mode_active and rvol >= 0.55) or (rvol >= 1.20 and abs(roc) >= 0.10)
            profile_reason = "catalyst impulse"

        if not profile_ok:
            rejected.append(f"{profile.name}:logic")
            continue

        enriched = enrich_signal_for_profile(base_signal, profile)
        enriched["reason"] = f"{enriched.get('reason', '')} | {profile_reason}".strip()
        passed.append(enriched)

    # ── Generic fallback (priority 5) ─────────────────────────────────────
    # Only fires for core/permissive names that matched nothing above.
    # Criteria: price is directional (clear VWAP side + any ROC signal).
    if not passed and is_core:
        generic = PROFILES.get("generic_intraday_continuation")
        if generic is not None:
            generic_ok = False
            generic_reason = ""
            if signal_score >= float(generic.min_signal_score):
                has_direction = direction in ("call", "put")
                has_roc = abs(roc) >= 0.015  # any measurable momentum
                has_vwap_side = distance_from_vwap_pct >= 0.01  # not pinned to VWAP
                generic_ok = has_direction and (has_roc or has_vwap_side)
                if generic_ok:
                    generic_reason = (
                        f"generic continuation | {direction.upper()} | "
                        f"ROC {roc:+.2f}% | VWAP dist {distance_from_vwap_pct:.2f}%"
                    )
                else:
                    rejected.append("generic_intraday_continuation:logic")
            else:
                rejected.append(f"generic_intraday_continuation:score({signal_score:.2f}<{generic.min_signal_score})")

            if generic_ok:
                enriched = enrich_signal_for_profile(base_signal, generic)
                enriched["reason"] = f"{enriched.get('reason', '')} | {generic_reason}".strip()
                passed.append(enriched)

    return passed, rejected


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
        except Exception as exc:
            print(f"[scanner] IV rank fallback for {symbol} failed: {exc}")

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
    force_relaxed_rvol: bool = False,
) -> dict[str, Any]:
    if bars_df is None or bars_df.empty:
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
    opening_relax = bool(config.ENABLE_OPENING_ENTRY_RELAX) and (
        minutes_since_open <= int(config.OPENING_ENTRY_RELAX_MINUTES)
    )
    base_min_bars = max(1, int(config.SCAN_MIN_BARS))
    bar_interval_minutes = 1
    if len(bars_df) >= 2:
        ts_series = pd.to_datetime(bars_df["timestamp"], errors="coerce")
        deltas = ts_series.diff().dt.total_seconds().dropna()
        if not deltas.empty:
            median_delta_sec = float(deltas.median())
            if median_delta_sec > 0:
                bar_interval_minutes = max(1, int(round(median_delta_sec / 60.0)))
    completed_bars = max(1, minutes_since_open // max(1, bar_interval_minutes))
    min_bars_required = 1 if opening_relax else min(base_min_bars, completed_bars)
    if len(bars_df) < min_bars_required:
        return _scan_failure(f"insufficient intraday bars ({len(bars_df)}/{min_bars_required})")

    try:
        if data_client.has_earnings_within_days(symbol, config.EARNINGS_LOOKAHEAD_DAYS, now_et=now_et):
            return _scan_failure(f"earnings within {config.EARNINGS_LOOKAHEAD_DAYS} days", stage="hard_block")
    except Exception as exc:  # noqa: BLE001
        if config.EARNINGS_CHECK_STRICT:
            return _scan_failure(f"earnings check failed: {exc}", stage="hard_block")

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
        if opening_relax:
            effective_rvol_min = min(effective_rvol_min, float(config.OPENING_RVOL_MIN))
        if is_at_or_after(now_et, config.RVOL_RELAX_AFTER):
            effective_rvol_min = min(effective_rvol_min, float(config.RVOL_RELAXED_MIN))
        if force_relaxed_rvol:
            effective_rvol_min = min(effective_rvol_min, 0.05)
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
    vwap_neutral = distance_pct <= vwap_band

    above_vwap = price > vwap

    # Simple direction model: anchor to VWAP side, confirm with momentum,
    # and use EMA only as a light tiebreaker to keep responsiveness high.
    roc_early = calculate_roc(closes, period=config.ROC_PERIOD)
    ema9_pre = closes.ewm(span=9, adjust=False).mean()
    ema21_pre = closes.ewm(span=21, adjust=False).mean()

    vwap_vote = 1.0 if above_vwap else -1.0
    roc_vote = 0.0 if math.isnan(roc_early) or abs(roc_early) < 0.01 else (1.0 if roc_early > 0 else -1.0)
    ema_vote = 0.0
    if len(ema9_pre) >= 3 and len(ema21_pre) >= 3:
        ema_vote = 1.0 if ema9_pre.iloc[-1] > ema21_pre.iloc[-1] else -1.0

    movement_force_min = float(getattr(config, "MOVEMENT_FORCE_MIN_PCT", 0.03) or 0.03)
    weak_vwap_mult = float(getattr(config, "MOVEMENT_WEAK_VWAP_MULT", 1.5) or 1.5)
    if not math.isnan(roc_early) and abs(roc_early) < movement_force_min and distance_pct < (vwap_band * weak_vwap_mult):
        return _scan_failure(f"movement too weak (ROC {roc_early:+.2f}%, VWAP dist {distance_pct:.2f}%)")

    vwap_weight = 0.50 if vwap_neutral else 1.00
    direction_bias = (vwap_weight * vwap_vote) + (1.10 * roc_vote) + (0.40 * ema_vote)
    direction_score = max(-1.0, min(1.0, direction_bias / 2.2))
    direction = "call" if direction_bias >= 0 else "put"

    # If momentum is clearly opposite, follow momentum rather than reject outright.
    if not math.isnan(roc_early) and abs(float(roc_early)) >= 0.15:
        if roc_early > 0:
            direction = "call"
        elif roc_early < 0:
            direction = "put"

    direction_votes: list[tuple[str, float, float]] = [
        ("vwap", vwap_weight, vwap_vote),
        ("roc", 1.1, roc_vote),
        ("ema", 0.4, ema_vote),
    ]

    htf_reason = ""
    if config.ENABLE_HTF_CONFIRM:
        htf_ok, htf_reason = _htf_trend_confirmation(symbol=symbol, direction=direction, data_client=data_client)
        if not htf_ok:
            return _scan_failure(f"HTF trend mismatch: {htf_reason}", stage="execution_reject")

    flow_score: float | None = None
    if config.ENABLE_ORDER_FLOW_FILTER:
        flow_score = _order_flow_score(symbol=symbol, data_client=data_client)
        if flow_score is None:
            return _scan_failure("order flow unavailable", stage="execution_reject")
        threshold = float(config.MIN_FLOW_SCORE)
        if direction == "call" and flow_score < threshold:
            return _scan_failure(f"order flow weak for call ({flow_score:+.2f})", stage="execution_reject")
        if direction == "put" and flow_score > -threshold:
            return _scan_failure(f"order flow weak for put ({flow_score:+.2f})", stage="execution_reject")

    # ROC and EMA are already computed above in the direction vote block.
    # Re-use those values here for downstream filters and scoring.
    roc = roc_early if not math.isnan(roc_early) else 0.0
    if config.ENABLE_ROC_FILTER and not math.isnan(roc_early):
        weak_floor = max(0.01, movement_force_min / 2.0)
        if abs(roc) < weak_floor and distance_pct < (vwap_band * 2.0):
            return _scan_failure(f"ROC {roc:+.2f}% too weak for active move")

    ema9 = ema9_pre
    ema21 = ema21_pre
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
    vote_log = ", ".join(f"{n}={'+' if v > 0 else ''}{v:.0f}" for n, _, v in direction_votes)
    return {
        "symbol": symbol,
        "direction": direction,
        "direction_score": round(direction_score, 3),
        "direction_votes": vote_log,
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
            f"RVOL {rvol:.1f}x | {above_below} | Dir {direction_score:+.2f} [{vote_log}] | {ema_note} | "
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
        # symbol -> {until: datetime, bucket: str, reason: str}
        self._reject_cooldowns: dict[str, dict[str, Any]] = {}

    def _next_session_open(self, now_et: datetime) -> datetime:
        candidate = now_et
        if (now_et.hour > 9) or (now_et.hour == 9 and now_et.minute >= 30):
            candidate = now_et + timedelta(days=1)
        while candidate.weekday() >= 5:
            candidate += timedelta(days=1)
        naive_open = datetime.combine(candidate.date(), datetime.min.time()).replace(hour=9, minute=30)
        return self.tz.localize(naive_open)

    def _cooldown_for_reject(self, reason: str, now_et: datetime) -> tuple[str, datetime] | None:
        text = str(reason or "").strip().lower()
        if not text:
            return None

        # No cooldown for live setup logic: keep checking every scan.
        no_cooldown_tokens = (
            "setup_reject: movement too weak",
            "setup_reject: roc",
            "setup_reject: signal score",
            "near vwap",
            "momentum opposes setup",
        )
        if any(token in text for token in no_cooldown_tokens):
            return None

        hard_tokens = (
            "hard_block: earnings",
            "hard_block: manual deny",
            "hard_block: manual block",
            "hard_block: news/event block",
            "hard_block: explicit event/news block",
        )
        if any(token in text for token in hard_tokens):
            return ("long", self._next_session_open(now_et))

        medium_tokens = (
            "no valid contract",
            "spread too wide",
            "chain lookup fails",
            "chain lookup failed",
            "option chain unavailable",
            "contract selection failed",
        )
        if any(token in text for token in medium_tokens):
            minutes = int(getattr(config, "REJECT_COOLDOWN_MEDIUM_MINUTES", 30) or 30)
            return ("medium", now_et + timedelta(minutes=max(15, min(60, minutes))))

        short_tokens = (
            "no premarket bars",
            "no intraday bars",
            "insufficient bars",
            "quote unavailable",
            "no latest stock price",
        )
        if any(token in text for token in short_tokens):
            minutes = int(getattr(config, "REJECT_COOLDOWN_SHORT_MINUTES", 3) or 3)
            return ("short", now_et + timedelta(minutes=max(1, min(5, minutes))))

        return None

    def _active_reject_cooldown(self, symbol: str, now_et: datetime) -> dict[str, Any] | None:
        key = str(symbol or "").upper()
        if not key:
            return None
        item = self._reject_cooldowns.get(key)
        if item is None:
            return None
        until = item.get("until")
        if not isinstance(until, datetime) or until <= now_et:
            self._reject_cooldowns.pop(key, None)
            return None
        return item

    def _record_reject(self, failed: list[dict[str, str]], symbol: str, reason: str, now_et: datetime) -> None:
        failed.append({"symbol": symbol, "reason": reason})
        cooldown = self._cooldown_for_reject(reason, now_et)
        if cooldown is None:
            return
        bucket, until = cooldown
        self._reject_cooldowns[str(symbol or "").upper()] = {
            "bucket": bucket,
            "until": until,
            "reason": reason,
        }

    def _clear_reject_cooldown(self, symbol: str) -> None:
        self._reject_cooldowns.pop(str(symbol or "").upper(), None)

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

    def run_scan(
        self,
        watchlist: list[str],
        *,
        now_et: datetime | None = None,
        premarket_mode: bool = False,
    ) -> list[dict]:
        now_et = now_et or datetime.now(self.tz)
        passed: list[dict] = []
        failed: list[dict[str, str]] = []
        cached_inputs: list[tuple[str, pd.DataFrame, pd.DataFrame, float]] = []
        lookback_minutes = max(5, int(getattr(config, "PREMARKET_LOOKBACK_MINUTES", 75)))

        for symbol in watchlist:
            try:
                active_cd = self._active_reject_cooldown(symbol, now_et)
                if active_cd is not None:
                    until_et = active_cd["until"].astimezone(self.tz)
                    failed.append(
                        {
                            "symbol": symbol,
                            "reason": (
                                f"cooldown_skip:{active_cd['bucket']} until {until_et.strftime('%Y-%m-%d %H:%M %Z')}"
                            ),
                        }
                    )
                    continue

                if _is_obvious_junk_symbol(symbol):
                    self._record_reject(failed, symbol, "universe rejected: junk/warrant-like symbol", now_et)
                    continue

                latest_price = self.data_client.get_latest_stock_price(symbol)
                if latest_price is None:
                    self._record_reject(failed, symbol, "universe rejected: no latest stock price", now_et)
                    continue
                if latest_price < float(config.MIN_SHARE_PRICE) or latest_price > float(config.MAX_SHARE_PRICE):
                    self._record_reject(
                        failed,
                        symbol,
                        (
                            f"universe rejected: price ${latest_price:.2f} outside "
                            f"${float(config.MIN_SHARE_PRICE):.2f}-${float(config.MAX_SHARE_PRICE):.2f}"
                        ),
                        now_et,
                    )
                    continue

                if premarket_mode:
                    window_start = now_et - timedelta(minutes=lookback_minutes)
                    bars_df = self.data_client.get_intraday_bars_window(
                        symbol=symbol,
                        start_et=window_start,
                        end_et=now_et,
                        limit=config.SCAN_INTRADAY_BARS,
                    )
                else:
                    bars_df = self.data_client.get_intraday_bars_since_open(
                        symbol=symbol,
                        now_et=now_et,
                        limit=config.SCAN_INTRADAY_BARS,
                        bar_timeframe="1Min",
                    )
                if bars_df.empty:
                    no_bars_reason = "universe rejected: no premarket bars" if premarket_mode else "universe rejected: no intraday bars"
                    self._record_reject(failed, symbol, no_bars_reason, now_et)
                    continue
                if len(bars_df) < 2:
                    self._record_reject(failed, symbol, "universe rejected: insufficient bars (need >= 2)", now_et)
                    continue
                daily_df = self.data_client.get_stock_daily_bars(symbol, limit=config.SCAN_DAILY_BARS)
                today_volume = float(bars_df["volume"].astype(float).sum())
                cached_inputs.append((symbol, bars_df, daily_df, today_volume))
                details = _scan_ticker_details(
                    symbol=symbol,
                    bars_df=bars_df,
                    daily_bars_df=daily_df,
                    today_volume=today_volume,
                    data_client=self.data_client,
                    force_relaxed_rvol=premarket_mode,
                )
                if details.get("failed"):
                    self._record_reject(failed, symbol, details["reason"], now_et)
                else:
                    profile_signals, rejected = _profile_signals_for_candidate(
                        base_signal=details,
                        bars_df=bars_df,
                        now_et=now_et,
                        catalyst_mode_active=_CATALYST_MODE_ACTIVE,
                    )
                    if not profile_signals:
                        _permissive_core = (
                            set(str(s).upper() for s in getattr(config, "CORE_TICKERS", []))
                            | _CORE_LIQUID_PROFILE_SYMBOLS
                        )
                        stage = "profile_miss" if symbol in _permissive_core else "setup_reject"
                        self._record_reject(
                            failed,
                            symbol,
                            f"{stage}: no profile matched ({', '.join(rejected)})",
                            now_et,
                        )
                    else:
                        for profile_signal in profile_signals:
                            if premarket_mode:
                                profile_signal["reason"] = (
                                    f"{profile_signal.get('reason', '')} | Premarket prep"
                                ).strip()
                            passed.append(profile_signal)
                        self._clear_reject_cooldown(symbol)
            except Exception as exc:  # noqa: BLE001
                self._record_reject(failed, symbol, f"setup_reject: scan error: {exc}", now_et)
            time.sleep(config.RATE_LIMIT_SLEEP_SECONDS)

        rvol_fail_count = sum(1 for item in failed if "rvol" in str(item.get("reason", "")).lower())
        failopen_triggered = False
        if not passed and failed and (rvol_fail_count / max(1, len(failed))) >= 0.70:
            failopen_triggered = True
            retry_passed: list[dict] = []
            retry_failed: list[dict[str, str]] = []
            for symbol, bars_df, daily_df, today_volume in cached_inputs:
                try:
                    details = _scan_ticker_details(
                        symbol=symbol,
                        bars_df=bars_df,
                        daily_bars_df=daily_df,
                        today_volume=today_volume,
                        data_client=self.data_client,
                        force_relaxed_rvol=True,
                    )
                    if details.get("failed"):
                        self._record_reject(retry_failed, symbol, details["reason"], now_et)
                    else:
                        profile_signals, rejected = _profile_signals_for_candidate(
                            base_signal=details,
                            bars_df=bars_df,
                            now_et=now_et,
                            catalyst_mode_active=_CATALYST_MODE_ACTIVE,
                        )
                        if not profile_signals:
                            _permissive_core = (
                                set(str(s).upper() for s in getattr(config, "CORE_TICKERS", []))
                                | _CORE_LIQUID_PROFILE_SYMBOLS
                            )
                            stage = "profile_miss" if symbol in _permissive_core else "setup_reject"
                            self._record_reject(
                                retry_failed,
                                symbol,
                                f"{stage}: no profile matched ({', '.join(rejected)})",
                                now_et,
                            )
                        else:
                            for profile_signal in profile_signals:
                                profile_signal["reason"] = (
                                    f"{profile_signal.get('reason', '')} | RVOL fail-open"
                                ).strip()
                                retry_passed.append(profile_signal)
                            self._clear_reject_cooldown(symbol)
                except Exception as exc:  # noqa: BLE001
                    self._record_reject(retry_failed, symbol, f"setup_reject: scan error: {exc}", now_et)
                time.sleep(config.RATE_LIMIT_SLEEP_SECONDS)
            passed = retry_passed
            failed = retry_failed

        by_symbol: dict[str, dict[str, Any]] = {}
        for item in passed:
            symbol = str(item.get("symbol", "") or "")
            current = by_symbol.get(symbol)
            if current is None:
                by_symbol[symbol] = item
                continue
            left = (
                float(item.get("signal_score", 0.0) or 0.0),
                -int(item.get("profile_priority", 99) or 99),
                float(item.get("rvol", 0.0) or 0.0),
            )
            right = (
                float(current.get("signal_score", 0.0) or 0.0),
                -int(current.get("profile_priority", 99) or 99),
                float(current.get("rvol", 0.0) or 0.0),
            )
            if left > right:
                by_symbol[symbol] = item
        passed = list(by_symbol.values())

        if premarket_mode:
            passed.sort(
                key=lambda item: (
                    float(item.get("signal_score", 0.0) or 0.0),
                    float(item.get("rvol", 0.0) or 0.0),
                ),
                reverse=True,
            )
        else:
            passed.sort(
                key=lambda item: (
                    float(item.get("signal_score", 0.0) or 0.0),
                    float(item.get("rvol", 0.0) or 0.0),
                ),
                reverse=True,
            )
        self.last_failures = failed
        if failopen_triggered:
            print(f"[{now_et.strftime('%H:%M ET')}] RVOL fail-open engaged: widespread low RVOL detected.")
        self._print_summary(now_et, len(watchlist), passed, failed)
        return passed

    def _print_summary(self, now_et: datetime, total: int, passed: list[dict], failed: list[dict[str, str]]) -> None:
        print(f"[{now_et.strftime('%H:%M ET')}] SCAN RESULTS - {len(passed)} of {total} tickers passed")
        for item in passed:
            vwap_side = "Above VWAP" if item["direction"] == "call" else "Below VWAP"
            print(
                f"  + {item['symbol']:<5} | {str(item.get('strategy_profile', 'base')):<18} | "
                f"{item['direction'].upper():<4} | RVOL {item['rvol']:.1f}x | "
                f"RSI {item['rsi']:.0f} | ROC {item['roc']:+.1f}% | IVR {item['iv_rank']:.0f}% | {vwap_side}"
            )
        for item in failed[:8]:
            print(f"  - {item['symbol']:<5} | failed: {item['reason']}")
        self._write_scan_log(now_et, passed, failed)

    def _write_scan_log(self, now_et: datetime, passed: list[dict], failed: list[dict[str, str]]) -> None:
        import csv

        _ensure_scan_log_header()
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
                        "strategy_profile": item.get("strategy_profile", ""),
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
                        "strategy_profile": "",
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


def run_scan(
    watchlist: list[str],
    *,
    now_et: datetime | None = None,
    premarket_mode: bool = False,
) -> list[dict]:
    if _DEFAULT_SCANNER is None:
        raise RuntimeError("Scanner not initialized. Call initialize_scanner(data_client) first.")
    return _DEFAULT_SCANNER.run_scan(
        watchlist,
        now_et=now_et,
        premarket_mode=premarket_mode,
    )


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
        except Exception as exc:
            print(f"[Observation] {symbol}: observation step failed: {exc}")
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
