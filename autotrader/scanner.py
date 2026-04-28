"""Intraday scanner for options-tradable momentum names."""

from __future__ import annotations

import math
import re
import time
import csv
from collections import deque
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytz

try:
    from autotrader import config
except ImportError:
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
    "volatility_score",
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
_LEARNING_PROFILE_CACHE: dict[str, Any] = {
    "updated_at": None,
    "profile": None,
}


def compute_direction_from_votes(
    votes: list[tuple[str, float, float]],
) -> tuple[str, float, float, int]:
    """Aggregate weighted direction votes into (direction, score, abs_score, aligned_count).

    `votes` is a list of (name, weight, vote) where vote is +1/-1 (or any signed
    float). Returns:
      direction      : "call" if weighted_vote >= 0 else "put"
      score          : signed normalized score in [-1, 1]
      abs_score      : abs(score)
      aligned_count  : count of votes whose sign matches the chosen direction
    Pure function; no config dependencies, safe for unit tests.
    """
    if not votes:
        return "call", 0.0, 0.0, 0
    weighted = sum(float(weight) * float(vote) for _name, weight, vote in votes)
    total_weight = sum(abs(float(weight)) for _name, weight, _vote in votes)
    direction = "call" if weighted >= 0 else "put"
    abs_score = (abs(weighted) / total_weight) if total_weight > 0 else 0.0
    score = abs_score if direction == "call" else -abs_score
    aligned = 0
    for _name, _weight, vote in votes:
        if direction == "call" and float(vote) > 0:
            aligned += 1
        elif direction == "put" and float(vote) < 0:
            aligned += 1
    return direction, score, abs_score, aligned


def _default_learning_profile() -> dict[str, Any]:
    return {
        "move_threshold_mult": 1.0,
        "rvol_min_mult": 1.0,
        "rsi_expand_points": 0.0,
        "shares": {
            "day": {"rsi": 0.0, "weak_move": 0.0, "rvol": 0.0},
            "week": {"rsi": 0.0, "weak_move": 0.0, "rvol": 0.0},
            "month": {"rsi": 0.0, "weak_move": 0.0, "rvol": 0.0},
        },
    }


def _parse_scan_ts(raw: str) -> datetime | None:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        if text.endswith(" ET"):
            naive = datetime.strptime(text[:-3].strip(), "%Y-%m-%d %H:%M:%S")
            return pytz.timezone(config.EASTERN_TZ).localize(naive)
        if text.endswith(" EDT"):
            naive = datetime.strptime(text[:-4].strip(), "%Y-%m-%d %H:%M:%S")
            return pytz.timezone(config.EASTERN_TZ).localize(naive)
        if text.endswith(" EST"):
            naive = datetime.strptime(text[:-4].strip(), "%Y-%m-%d %H:%M:%S")
            return pytz.timezone(config.EASTERN_TZ).localize(naive)
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return pytz.timezone(config.EASTERN_TZ).localize(parsed)
        return parsed.astimezone(pytz.timezone(config.EASTERN_TZ))
    except Exception:
        return None


def _reject_feature_counts(rows: list[dict[str, str]]) -> tuple[int, dict[str, int]]:
    total = 0
    counts = {"rsi": 0, "weak_move": 0, "rvol": 0}
    for row in rows:
        reason = str(row.get("reason", "") or "").lower()
        if "fail" not in str(row.get("result", "") or "").lower() and "setup_reject" not in reason:
            continue
        total += 1
        if "setup_reject: rsi" in reason:
            counts["rsi"] += 1
        if (
            "setup_reject: movement too weak" in reason
            or "setup_reject: price direction too weak" in reason
            or "setup_reject: roc" in reason
        ):
            counts["weak_move"] += 1
        if "setup_reject: rvol" in reason:
            counts["rvol"] += 1
    return total, counts


def _build_learning_profile(now_et: datetime) -> dict[str, Any]:
    profile = _default_learning_profile()
    if not SCAN_LOG_PATH.exists():
        return profile

    lookback_rows = int(getattr(config, "LEARNING_SCAN_LOG_MAX_ROWS", 12000) or 12000)
    if lookback_rows < 1000:
        lookback_rows = 1000

    day_cutoff = now_et - timedelta(days=1)
    week_cutoff = now_et - timedelta(days=7)
    month_cutoff = now_et - timedelta(days=30)

    try:
        with SCAN_LOG_PATH.open("r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(deque(reader, maxlen=lookback_rows))
    except Exception:
        return profile

    day_rows: list[dict[str, str]] = []
    week_rows: list[dict[str, str]] = []
    month_rows: list[dict[str, str]] = []
    for row in rows:
        ts = _parse_scan_ts(str(row.get("timestamp", "") or ""))
        if ts is None:
            continue
        ts_et = ts.astimezone(pytz.timezone(config.EASTERN_TZ))
        if ts_et >= month_cutoff:
            month_rows.append(row)
        if ts_et >= week_cutoff:
            week_rows.append(row)
        if ts_et >= day_cutoff:
            day_rows.append(row)

    day_total, day_counts = _reject_feature_counts(day_rows)
    week_total, week_counts = _reject_feature_counts(week_rows)
    month_total, month_counts = _reject_feature_counts(month_rows)

    def _share(total: int, count: int) -> float:
        if total <= 0:
            return 0.0
        return float(count) / float(total)

    shares = {
        "day": {
            "rsi": _share(day_total, day_counts["rsi"]),
            "weak_move": _share(day_total, day_counts["weak_move"]),
            "rvol": _share(day_total, day_counts["rvol"]),
        },
        "week": {
            "rsi": _share(week_total, week_counts["rsi"]),
            "weak_move": _share(week_total, week_counts["weak_move"]),
            "rvol": _share(week_total, week_counts["rvol"]),
        },
        "month": {
            "rsi": _share(month_total, month_counts["rsi"]),
            "weak_move": _share(month_total, month_counts["weak_move"]),
            "rvol": _share(month_total, month_counts["rvol"]),
        },
    }

    move_threshold_mult = 1.0
    if shares["day"]["weak_move"] >= 0.22:
        move_threshold_mult = 0.70
    elif shares["day"]["weak_move"] >= 0.15:
        move_threshold_mult = 0.82
    elif shares["week"]["weak_move"] >= 0.12:
        move_threshold_mult = 0.90

    rvol_min_mult = 1.0
    if shares["day"]["rvol"] >= 0.28:
        rvol_min_mult = 0.75
    elif shares["day"]["rvol"] >= 0.20:
        rvol_min_mult = 0.88
    elif shares["week"]["rvol"] >= 0.18:
        rvol_min_mult = 0.92

    rsi_expand_points = 0.0
    if shares["day"]["rsi"] >= 0.20:
        rsi_expand_points = 8.0
    elif shares["day"]["rsi"] >= 0.14:
        rsi_expand_points = 5.0
    elif shares["week"]["rsi"] >= 0.11:
        rsi_expand_points = 3.0

    profile["move_threshold_mult"] = float(max(0.60, min(1.0, move_threshold_mult)))
    profile["rvol_min_mult"] = float(max(0.70, min(1.0, rvol_min_mult)))
    profile["rsi_expand_points"] = float(max(0.0, min(10.0, rsi_expand_points)))
    profile["shares"] = shares
    return profile


def _learning_profile(now_et: datetime) -> dict[str, Any]:
    cache_seconds = int(getattr(config, "LEARNING_REFRESH_SECONDS", 300) or 300)
    updated_at = _LEARNING_PROFILE_CACHE.get("updated_at")
    if isinstance(updated_at, datetime):
        age = (now_et - updated_at).total_seconds()
        if age >= 0 and age < max(30, cache_seconds):
            cached = _LEARNING_PROFILE_CACHE.get("profile")
            if isinstance(cached, dict):
                return cached

    profile = _build_learning_profile(now_et)
    _LEARNING_PROFILE_CACHE["updated_at"] = now_et
    _LEARNING_PROFILE_CACHE["profile"] = profile
    return profile


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
    if len(closes) < 2:
        return float("nan")
    # Early-session fallback: if requested lookback is longer than available bars,
    # use the longest safe lookback instead of returning NaN.
    if len(closes) <= period:
        period = len(closes) - 1
    if period < 1:
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
            profile_ok = rvol >= 0.20 and abs(roc) >= 0.01 and distance_from_vwap_pct >= 0.005
            profile_reason = "open-drive momentum"
        elif profile.name == "vwap_continuation":
            profile_ok = distance_from_vwap_pct <= 3.0 and rvol >= 0.20 and abs(roc) >= 0.01
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
            profile_ok = rvol >= 0.20 and abs(roc) >= 0.01
            profile_reason = "catalyst impulse"

        if not profile_ok:
            rejected.append(f"{profile.name}:logic")
            continue

        enriched = enrich_signal_for_profile(base_signal, profile)
        enriched["reason"] = f"{enriched.get('reason', '')} | {profile_reason}".strip()
        passed.append(enriched)

    # ── Generic fallback (priority 5) ─────────────────────────────────────
    # Universal safety net: fires for ANY symbol that passed the scanner but
    # didn't match a named profile. Previously restricted to core names only;
    # now open to all symbols so movers and non-core tickers can trade.
    if not passed:
        generic = PROFILES.get("generic_intraday_continuation")
        if generic is not None:
            generic_ok = False
            generic_reason = ""
            if signal_score >= float(generic.min_signal_score):
                has_direction = direction in ("call", "put")
                has_roc = abs(roc) >= 0.05
                has_vwap_side = distance_from_vwap_pct >= 0.03
                generic_ok = has_direction and has_roc and has_vwap_side
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
    volatility_score: float,
    flow_score: float | None = None,
    ema_aligned: bool = True,
) -> float:
    vol_weight = float(getattr(config, "VOLATILITY_PRIORITY_WEIGHT", 3.0) or 3.0)
    trend_weight = float(getattr(config, "TREND_PRIORITY_WEIGHT", 1.0) or 1.0)
    flow_weight = float(getattr(config, "FLOW_PRIORITY_WEIGHT", 1.0) or 1.0)

    trend_component = 0.0
    trend_component += min(2.0, max(0.0, abs(roc) / 1.5))
    trend_component += min(5.0, max(0.0, regime_score))
    if ema_aligned:
        trend_component += 1.0

    # Keep flow bounded so it cannot dominate volatility.
    flow_component = 0.0
    if flow_score is not None:
        flow_component = min(1.0, max(0.0, (float(flow_score) + 1.0) / 2.0))

    score = (vol_weight * float(volatility_score)) + (trend_weight * trend_component) + (flow_weight * flow_component)
    return round(score, 2)


def _volatility_priority_score(rvol: float, atr_pct: float, iv_rank: float | None) -> float:
    rvol_component = min(4.0, max(0.0, float(rvol) * 1.2))
    atr_component = min(4.0, max(0.0, float(atr_pct) * 1.1))
    iv_component = 0.0
    if iv_rank is not None:
        iv_component = min(2.0, max(0.0, float(iv_rank) / 50.0))
    return round(rvol_component + atr_component + iv_component, 2)


# Cache HTF results for 5 minutes to avoid one API call per ticker per loop.
# Without this, 150 tickers × ~1s per HTF call = 150s per loop → loop stale.
_HTF_CACHE: dict[str, tuple[float, bool, str]] = {}  # symbol -> (expiry_ts, ok, reason)
_HTF_CACHE_TTL_SECONDS = 300  # 5 minutes


def _htf_trend_confirmation(
    symbol: str,
    direction: str,
    data_client: AlpacaDataClient,
) -> tuple[bool, str]:
    import time as _time
    cache_key = f"{symbol}:{direction}"
    cached = _HTF_CACHE.get(cache_key)
    if cached is not None and _time.monotonic() < cached[0]:
        return cached[1], cached[2] + " (cached)"
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

    # Soft tolerance avoids rejecting good setups when HTF is flat/noisy.
    gap_pct = ((last_ema9 - last_ema21) / last_ema21 * 100) if last_ema21 != 0 else 0.0
    slope_tol = float(getattr(config, "HTF_SLOPE_TOLERANCE_PCT", 0.12) or 0.12)
    gap_tol = float(getattr(config, "HTF_EMA_GAP_TOLERANCE_PCT", 0.05) or 0.05)
    if direction == "call":
        ok = gap_pct >= -gap_tol and slope_pct >= -slope_tol
    else:
        ok = gap_pct <= gap_tol and slope_pct <= slope_tol
    relation = "above" if last_ema9 > last_ema21 else "below"
    reason = f"HTF ema9 {relation} ema21, ema_gap={gap_pct:+.2f}%, ema21_slope={slope_pct:+.2f}%"
    # Write to cache so subsequent tickers in the same loop use the cached result.
    import time as _time
    _HTF_CACHE[f"{symbol}:{direction}"] = (_time.monotonic() + _HTF_CACHE_TTL_SECONDS, ok, reason)
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
            - Returns None when historical data is unavailable.
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

    return closest_iv, None


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
    bar_interval_minutes = 1
    if len(bars_df) >= 2:
        ts_series = pd.to_datetime(bars_df["timestamp"], errors="coerce")
        deltas = ts_series.diff().dt.total_seconds().dropna()
        if not deltas.empty:
            median_delta_sec = float(deltas.median())
            if median_delta_sec > 0:
                bar_interval_minutes = max(1, int(round(median_delta_sec / 60.0)))
    now_et_actual = datetime.now(pytz.timezone(config.EASTERN_TZ))
    bar_age_seconds = max(0.0, (now_et_actual - ts_last_et).total_seconds())
    # Reject delayed intraday feeds before any signal logic runs.
    # Allow up to roughly two bar intervals (+ buffer) to tolerate feed jitter.
    configured_stale_limit = float(getattr(config, "STALE_BAR_MAX_AGE_SECONDS", 0) or 0)
    dynamic_stale_limit = float(max(120, (bar_interval_minutes * 120) + 30))
    stale_limit_seconds = max(dynamic_stale_limit, configured_stale_limit)
    if bar_age_seconds > stale_limit_seconds:
        return _scan_failure(
            f"stale intraday bars ({int(bar_age_seconds)}s old; limit {int(stale_limit_seconds)}s)"
        )
    market_open = now_et_actual.replace(hour=9, minute=30, second=0, microsecond=0)
    minutes_since_open = max(1, int((now_et_actual - market_open).total_seconds() // 60))
    now_et = now_et_actual
    learning = _learning_profile(now_et)
    opening_relax = bool(config.ENABLE_OPENING_ENTRY_RELAX) and (
        minutes_since_open <= int(config.OPENING_ENTRY_RELAX_MINUTES)
    )
    base_min_bars = max(1, int(config.SCAN_MIN_BARS))
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
        # Apply learning multiplier without re-imposing a 1.0x hard floor; let
        # CATALYST/OPENING/RELAX/force_relaxed_rvol pathways actually relax.
        effective_rvol_min = max(0.50, effective_rvol_min * float(learning.get("rvol_min_mult", 1.0) or 1.0))
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

    # Direction model: use recent price direction first, then validate with momentum.
    roc_period = max(2, int(getattr(config, "ROC_PERIOD", 10) or 10))
    fast_roc_period = max(2, int(getattr(config, "DIRECTION_FAST_ROC_PERIOD", 5) or 5))
    roc_early = calculate_roc(closes, period=roc_period)
    roc_fast = calculate_roc(closes, period=fast_roc_period)
    ema9_pre = closes.ewm(span=9, adjust=False).mean()
    ema21_pre = closes.ewm(span=21, adjust=False).mean()

    movement_force_min = float(getattr(config, "MOVEMENT_FORCE_MIN_PCT", 0.03) or 0.03)
    movement_force_min *= float(learning.get("move_threshold_mult", 1.0) or 1.0)
    weak_vwap_mult = float(getattr(config, "MOVEMENT_WEAK_VWAP_MULT", 1.5) or 1.5)
    direction_roc = roc_fast if not math.isnan(roc_fast) else roc_early
    if math.isnan(direction_roc):
        return _scan_failure("momentum unavailable")

    price_dir_lookback = min(len(closes) - 1, max(1, int(getattr(config, "DIRECTION_PRICE_LOOKBACK_BARS", 3) or 3)))
    start_price = float(closes.iloc[-price_dir_lookback - 1])
    if start_price <= 0:
        return _scan_failure("direction unavailable")
    price_change_pct = ((price - start_price) / start_price) * 100

    if abs(direction_roc) < movement_force_min and distance_pct < (vwap_band * weak_vwap_mult):
        return _scan_failure(f"movement too weak (ROC {direction_roc:+.2f}%, VWAP dist {distance_pct:.2f}%)")

    price_dir_min = float(getattr(config, "DIRECTION_PRICE_MIN_PCT", max(0.005, movement_force_min / 2.0)) or 0.005)
    if abs(price_change_pct) < price_dir_min and abs(direction_roc) < (movement_force_min * 1.2):
        return _scan_failure(f"price direction too weak ({price_change_pct:+.2f}%)")

    price_sign = 1.0 if price_change_pct > 0 else -1.0
    momentum_sign = 1.0 if direction_roc > 0 else -1.0
    conflict_roc_min = float(
        getattr(config, "DIRECTION_CONFLICT_ROC_MIN_PCT", max(0.01, movement_force_min / 2.0))
        or max(0.01, movement_force_min / 2.0)
    )
    direction_conflict = price_sign != momentum_sign and abs(direction_roc) >= conflict_roc_min
    if direction_conflict and bool(getattr(config, "DIRECTION_CONFLICT_HARD_REJECT", False)):
        return _scan_failure(
            f"direction conflict (price {price_change_pct:+.2f}% vs ROC {direction_roc:+.2f}%)"
        )

    vwap_vote = 1.0 if above_vwap else -1.0
    ema_trend_vote = 1.0 if float(ema9_pre.iloc[-1]) >= float(ema21_pre.iloc[-1]) else -1.0
    roc_slow_vote = momentum_sign if math.isnan(roc_early) else (1.0 if roc_early >= 0 else -1.0)
    direction_votes: list[tuple[str, float, float]] = [
        ("price", 1.0, price_sign),
        ("momentum_fast", 0.9, momentum_sign),
        ("momentum_slow", 0.6, roc_slow_vote),
        ("ema_trend", 0.6, ema_trend_vote),
        ("vwap_side", 0.3, vwap_vote),
    ]

    direction, direction_score, _direction_score_abs, aligned_count = compute_direction_from_votes(
        direction_votes
    )

    min_aligned_votes = int(getattr(config, "DIRECTION_MIN_ALIGNED_VOTES", 3) or 3)
    min_aligned_votes = max(1, min(len(direction_votes), min_aligned_votes))
    if aligned_count < min_aligned_votes:
        return _scan_failure(
            f"direction alignment {aligned_count}/{len(direction_votes)} below {min_aligned_votes}"
        )

    if direction_conflict:
        conflict_mult = float(getattr(config, "DIRECTION_CONFLICT_SCORE_MULT", 0.55) or 0.55)
        direction_score *= max(0.1, min(1.0, conflict_mult))

    conviction_min = float(getattr(config, "DIRECTION_CONVICTION_MIN", 0.25) or 0.25)
    if abs(direction_score) < conviction_min:
        return _scan_failure(
            f"direction conviction {abs(direction_score):.2f} below {conviction_min:.2f}"
        )

    htf_reason = ""
    if config.ENABLE_HTF_CONFIRM:
        htf_ok, htf_reason = _htf_trend_confirmation(symbol=symbol, direction=direction, data_client=data_client)
        if not htf_ok:
            if bool(getattr(config, "HTF_MISMATCH_HARD_REJECT", False)):
                return _scan_failure(f"HTF trend mismatch: {htf_reason}", stage="execution_reject")
            direction_score *= 0.75

    flow_score: float | None = None
    if config.ENABLE_ORDER_FLOW_FILTER:
        flow_score = _order_flow_score(symbol=symbol, data_client=data_client)
        # Do not hard-reject when quote-size flow is unavailable; this would
        # suppress too many otherwise-valid setups in thin or noisy quote moments.
        if flow_score is None:
            flow_score = 0.0
        threshold = float(config.MIN_FLOW_SCORE)
        # Reject only when flow is explicitly opposite enough, not just weak/neutral.
        if direction == "call" and flow_score < -threshold:
            return _scan_failure(f"order flow weak for call ({flow_score:+.2f})", stage="execution_reject")
        if direction == "put" and flow_score > threshold:
            return _scan_failure(f"order flow weak for put ({flow_score:+.2f})", stage="execution_reject")

    # Re-use momentum ROC for downstream filters and scoring.
    roc = direction_roc
    if config.ENABLE_ROC_FILTER and not math.isnan(roc):
        weak_floor = float(getattr(config, "ROC_ACTIVE_MOVE_MIN_PCT", max(0.005, movement_force_min / 2.0)) or 0.005)
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
        rsi_expand = float(learning.get("rsi_expand_points", 0.0) or 0.0)
        call_min = max(1.0, float(config.RSI_CALL_MIN) - rsi_expand)
        call_max = min(99.0, float(config.RSI_CALL_MAX) + rsi_expand)
        put_min = max(1.0, float(config.RSI_PUT_MIN) - rsi_expand)
        put_max = min(99.0, float(config.RSI_PUT_MAX) + rsi_expand)
        if direction == "call" and not (call_min <= rsi <= call_max):
            return _scan_failure(
                f"RSI {rsi:.0f} outside call range ({call_min:.0f}-{call_max:.0f})"
            )
        if direction == "put" and not (put_min <= rsi <= put_max):
            return _scan_failure(
                f"RSI {rsi:.0f} outside put range ({put_min:.0f}-{put_max:.0f})"
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

    iv_rank_for_score = 50.0 if iv_rank is None else float(iv_rank)

    effective_iv_rank_max = (
        float(config.CATALYST_RELAXED_IV_RANK_MAX) if _CATALYST_MODE_ACTIVE else float(config.IV_RANK_MAX)
    )
    if iv_rank is not None and iv_rank > effective_iv_rank_max:
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

    volatility_score = _volatility_priority_score(
        rvol=float(rvol),
        atr_pct=float(atr_pct),
        iv_rank=(float(iv_rank) if iv_rank is not None else None),
    )

    signal_score = _combined_signal_score(
        rvol=float(rvol),
        atr_pct=float(atr_pct),
        roc=float(roc),
        iv_rank=iv_rank_for_score,
        regime_score=float(regime_score),
        volatility_score=float(volatility_score),
        flow_score=flow_score,
        ema_aligned=ema_aligned,
    )

    ivr_reason_text = f"IVR {iv_rank:.0f}%" if iv_rank is not None else "IVR N/A"
    effective_min_signal_score = (
        float(config.CATALYST_RELAXED_MIN_SIGNAL_SCORE) if _CATALYST_MODE_ACTIVE else float(config.MIN_SIGNAL_SCORE)
    )
    if config.ENABLE_SIGNAL_SCORING and signal_score < effective_min_signal_score:
        return _scan_failure(f"signal score {signal_score:.2f} below {effective_min_signal_score:.2f}")

    above_below = "Above VWAP" if direction == "call" else "Below VWAP"
    conflict_note = (
        f" | conflict price {price_change_pct:+.2f}% vs ROC {direction_roc:+.2f}%"
        if direction_conflict
        else ""
    )
    day_shares = learning.get("shares", {}).get("day", {}) if isinstance(learning, dict) else {}
    learn_note = (
        f" | Learn d(rsi={float(day_shares.get('rsi', 0.0))*100:.0f}%"
        f",weak={float(day_shares.get('weak_move', 0.0))*100:.0f}%"
        f",rvol={float(day_shares.get('rvol', 0.0))*100:.0f}%)"
    )
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
        "iv_rank": round(float(iv_rank), 2) if iv_rank is not None else None,
        "regime_score": round(regime_score, 2),
        "volatility_score": round(volatility_score, 2),
        "signal_score": round(signal_score, 2),
        "flow_score": round(flow_score, 4) if flow_score is not None else None,
        "htf_reason": htf_reason,
        "reason": (
            f"RVOL {rvol:.1f}x | {above_below} | Dir {direction_score:+.2f} [{vote_log}] | {ema_note} | "
            f"ROC {roc:+.2f}% | {ivr_reason_text} | Vol {volatility_score:.2f} | Regime {regime_score:.2f} | "
            f"Flow {(flow_score if flow_score is not None else 0):+.2f} | Score {signal_score:.2f}{conflict_note}{learn_note}"
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
            "setup_reject: direction conflict",
            "setup_reject: price direction too weak",
            "setup_reject: rsi",
            "near vwap",
            "momentum opposes setup",
        )
        if any(token in text for token in no_cooldown_tokens):
            return None

        long_tokens = (
            "hard_block: manual deny",
            "hard_block: manual block",
        )
        if any(token in text for token in long_tokens):
            return ("long", self._next_session_open(now_et))

        event_tokens = (
            "hard_block: earnings",
            "hard_block: news/event block",
            "hard_block: explicit event/news block",
        )
        if any(token in text for token in event_tokens):
            minutes = int(getattr(config, "REJECT_COOLDOWN_EVENT_MINUTES", 20) or 20)
            return ("event", now_et + timedelta(minutes=max(5, min(30, minutes))))

        medium_tokens = (
            "no valid contract",
            "spread too wide",
            "chain lookup fails",
            "chain lookup failed",
            "option chain unavailable",
            "contract selection failed",
        )
        if any(token in text for token in medium_tokens):
            minutes = int(getattr(config, "REJECT_COOLDOWN_MEDIUM_MINUTES", 15) or 15)
            return ("medium", now_et + timedelta(minutes=max(5, min(30, minutes))))

        short_tokens = (
            "no premarket bars",
            "no intraday bars",
            "insufficient bars",
            "quote unavailable",
            "no latest stock price",
        )
        if any(token in text for token in short_tokens):
            minutes = int(getattr(config, "REJECT_COOLDOWN_SHORT_MINUTES", 2) or 2)
            return ("short", now_et + timedelta(minutes=max(1, min(3, minutes))))

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
        base = list(dict.fromkeys(list(config.TICKERS) + list(config.CORE_TICKERS)))
        gainers: list[str] = []
        losers: list[str] = []
        if bool(getattr(config, "AUTO_EXPAND_UNIVERSE_WITH_MOVERS", True)):
            movers_top = int(getattr(config, "UNIVERSE_MOVER_TOP", getattr(config, "SCREENER_TOP_N", 20)) or 20)
            try:
                gainers, losers = self.data_client.get_top_movers(top=movers_top)
            except Exception as exc:  # noqa: BLE001
                print(f"[{self._ts()}] Movers endpoint unavailable ({exc}). Using core tickers only.")

        candidates = []
        # Prefer live movers first to avoid wasting cycles on sleepy symbols.
        # Static base symbols are only used as fallback when mover coverage is thin.
        per_side = int(getattr(config, "MOVER_SYMBOLS_PER_SIDE", 10) or 10)
        candidates.extend(gainers[:per_side])
        candidates.extend(losers[:per_side])
        min_mover_candidates = max(20, int(getattr(config, "MIN_MOVER_CANDIDATES_BEFORE_BASE_FALLBACK", 40) or 40))
        if len(candidates) < min_mover_candidates:
            candidates.extend(base)

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

        max_tickers = max(1, int(getattr(config, "UNIVERSE_MAX_TICKERS", len(filtered)) or len(filtered)))
        return filtered[:max_tickers]

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
                float(item.get("volatility_score", 0.0) or 0.0),
                float(item.get("signal_score", 0.0) or 0.0),
                -int(item.get("profile_priority", 99) or 99),
                float(item.get("rvol", 0.0) or 0.0),
            )
            right = (
                float(current.get("volatility_score", 0.0) or 0.0),
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
                    float(item.get("volatility_score", 0.0) or 0.0),
                    float(item.get("signal_score", 0.0) or 0.0),
                    float(item.get("rvol", 0.0) or 0.0),
                ),
                reverse=True,
            )
        else:
            passed.sort(
                key=lambda item: (
                    float(item.get("volatility_score", 0.0) or 0.0),
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
            ivr_print = f"{float(item['iv_rank']):.0f}%" if item.get("iv_rank") is not None else "N/A"
            print(
                f"  + {item['symbol']:<5} | {str(item.get('strategy_profile', 'base')):<18} | "
                f"{item['direction'].upper():<4} | RVOL {item['rvol']:.1f}x | "
                f"RSI {item['rsi']:.0f} | ROC {item['roc']:+.1f}% | IVR {ivr_print} | "
                f"VOL {float(item.get('volatility_score', 0.0) or 0.0):.2f} | {vwap_side}"
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
                        "volatility_score": item.get("volatility_score", ""),
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
                        "volatility_score": "",
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
