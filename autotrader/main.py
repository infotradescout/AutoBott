"""Entry point for the intraday options autotrader."""

from __future__ import annotations

import csv
import time
from datetime import date, datetime, timedelta
import re
import math

import pytz
import yfinance as yf

from env_config import get_required_env, load_runtime_env
load_runtime_env()

import config
from alerts import AlertManager
from broker import AlpacaBroker
from data import AlpacaDataClient
from feature_flags import is_enabled
from logger import TradeLogger
from options import select_atm_option_contract_with_reason
from risk import (
    can_open_new_positions,
    is_at_or_after,
    position_matches_ticker,
)
from scanner import initialize_scanner, run_observation_phase, run_scan, set_catalyst_mode
from session_rules import (
    premarket_scan_decision,
    should_force_same_day_exit,
    should_trigger_stop_loss,
)
from strategy_profiles import get_profile_overrides, normalize_profile_name
from state_store import load_bot_state, save_bot_state
from trading_control import load_trading_control, set_manual_stop
from watchlist_control import load_watchlist_control


def ts(now_et: datetime | None = None) -> str:
    now_et = now_et or datetime.now(pytz.timezone(config.EASTERN_TZ))
    return now_et.strftime("%Y-%m-%d %H:%M:%S %Z")


def ts_ct(now_ct: datetime | None = None) -> str:
    now_ct = now_ct or datetime.now(pytz.timezone(config.CENTRAL_TZ))
    return now_ct.strftime("%Y-%m-%d %H:%M:%S %Z")


def position_qty_as_int(qty_value) -> int:
    try:
        return int(float(qty_value))
    except (TypeError, ValueError):
        return 0


def _prune_recent_entries(entry_times: list[datetime], now_et: datetime, days: int = 5) -> list[datetime]:
    threshold = now_et.timestamp() - (days * 24 * 60 * 60)
    return [dt for dt in entry_times if dt.timestamp() >= threshold]


def _closed_market_sleep_seconds(clock, *, preopen_ready_minutes: int = 0) -> int:
    next_open = getattr(clock, "next_open", None)
    min_sleep = int(config.CLOSED_MIN_SLEEP_SECONDS)
    max_sleep = int(config.CLOSED_MAX_SLEEP_SECONDS)
    if next_open is None:
        return max(min_sleep, int(config.LOOP_INTERVAL_SECONDS))

    if next_open.tzinfo is None:
        next_open = pytz.utc.localize(next_open)

    now_utc = datetime.now(pytz.utc)
    seconds_until_open = int((next_open - now_utc).total_seconds())
    if seconds_until_open <= 0:
        return min_sleep

    if preopen_ready_minutes > 0:
        preopen_seconds = int(preopen_ready_minutes) * 60
        if seconds_until_open > preopen_seconds:
            return max(min_sleep, min(max_sleep, seconds_until_open - preopen_seconds))
    return max(min_sleep, min(max_sleep, seconds_until_open))


def _seconds_until_next_open(clock) -> int | None:
    next_open = getattr(clock, "next_open", None)
    if next_open is None:
        return None
    if next_open.tzinfo is None:
        next_open = pytz.utc.localize(next_open)

    now_utc = datetime.now(pytz.utc)
    return int((next_open - now_utc).total_seconds())


def _minutes_from_hhmm(value: str) -> int:
    raw = str(value or "").strip()
    parts = raw.split(":", 1)
    if len(parts) != 2:
        raise ValueError(f"invalid HH:MM value: {value!r}")
    hour = int(parts[0])
    minute = int(parts[1])
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise ValueError(f"invalid HH:MM value: {value!r}")
    return hour * 60 + minute


def _closed_market_lead_minutes() -> int:
    lead_minutes = max(0, int(config.PREOPEN_READY_MINUTES))
    if not bool(getattr(config, "ENABLE_PREMARKET_OPENING_SIGNALS", False)):
        return lead_minutes

    try:
        premarket_start_minutes = _minutes_from_hhmm(str(getattr(config, "PREMARKET_SIGNAL_WINDOW_START", "")))
        entry_open_minutes = _minutes_from_hhmm(str(getattr(config, "NO_NEW_TRADES_BEFORE", config.MARKET_OPEN)))
    except ValueError:
        return lead_minutes

    if premarket_start_minutes >= entry_open_minutes:
        return lead_minutes
    return max(lead_minutes, entry_open_minutes - premarket_start_minutes)


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None:
            return pytz.timezone(config.EASTERN_TZ).localize(parsed)
        return parsed
    except Exception:
        return None


def _week_key(day: date) -> str:
    year, week, _weekday = day.isocalendar()
    return f"{year}-W{week:02d}"


def _slippage_pct(reference_price: float, current_price: float) -> float:
    if reference_price <= 0 or current_price <= 0:
        return 0.0
    return ((current_price - reference_price) / reference_price) * 100.0


def _quote_midpoint(bid: float | None, ask: float | None) -> float | None:
    bid_value = float(bid or 0.0)
    ask_value = float(ask or 0.0)
    if bid_value > 0 and ask_value > 0 and ask_value >= bid_value:
        return (bid_value + ask_value) / 2.0
    if ask_value > 0:
        return ask_value
    if bid_value > 0:
        return bid_value
    return None


def _quote_spread_pct(bid: float | None, ask: float | None) -> float:
    bid_value = float(bid or 0.0)
    ask_value = float(ask or 0.0)
    midpoint = _quote_midpoint(bid_value, ask_value)
    if midpoint is None or midpoint <= 0 or ask_value < bid_value or bid_value <= 0:
        return 0.0
    return ((ask_value - bid_value) / midpoint) * 100.0


def _safe_signal_float(value, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return float(default)
    if math.isnan(parsed) or math.isinf(parsed):
        return float(default)
    return parsed


def _signal_volatility_profile(signal: dict) -> dict[str, float | str | int]:
    atr_pct = _safe_signal_float(signal.get("atr_pct"), 0.0)
    rvol = _safe_signal_float(signal.get("rvol"), 0.0)
    iv_rank_raw = signal.get("iv_rank")
    iv_rank = _safe_signal_float(iv_rank_raw, -1.0)
    iv_available = iv_rank >= 0.0

    score = 0
    atr_high = float(getattr(config, "VOL_RISK_ATR_PCT_HIGH", 2.0) or 2.0)
    atr_extreme = float(getattr(config, "VOL_RISK_ATR_PCT_EXTREME", 3.0) or 3.0)
    rvol_high = float(getattr(config, "VOL_RISK_RVOL_HIGH", 1.8) or 1.8)
    rvol_extreme = float(getattr(config, "VOL_RISK_RVOL_EXTREME", 2.8) or 2.8)
    iv_high = float(getattr(config, "VOL_RISK_IV_RANK_HIGH", 70.0) or 70.0)
    iv_extreme = float(getattr(config, "VOL_RISK_IV_RANK_EXTREME", 85.0) or 85.0)

    if atr_pct >= atr_high:
        score += 1
    if atr_pct >= atr_extreme:
        score += 1
    if rvol >= rvol_high:
        score += 1
    if rvol >= rvol_extreme:
        score += 1
    if iv_available and iv_rank >= iv_high:
        score += 1
    if iv_available and iv_rank >= iv_extreme:
        score += 1

    score_high = int(getattr(config, "VOL_RISK_SCORE_HIGH", 3) or 3)
    score_extreme = int(getattr(config, "VOL_RISK_SCORE_EXTREME", 5) or 5)
    label = "normal"
    if score >= score_extreme:
        label = "extreme"
    elif score >= score_high:
        label = "high"

    stop_loss_mult = 1.0
    premium_cap_mult = 1.0
    opening_premium_cap_mult = 1.0
    if bool(getattr(config, "ENABLE_VOLATILITY_ADAPTIVE_RISK", True)):
        if label == "extreme":
            stop_loss_mult = float(getattr(config, "VOL_STOP_LOSS_MULT_EXTREME", 1.35) or 1.35)
            premium_cap_mult = float(getattr(config, "VOL_PREMIUM_CAP_MULT_EXTREME", 0.70) or 0.70)
            opening_premium_cap_mult = float(getattr(config, "VOL_OPEN_PREMIUM_CAP_MULT_EXTREME", 0.75) or 0.75)
        elif label == "high":
            stop_loss_mult = float(getattr(config, "VOL_STOP_LOSS_MULT_HIGH", 1.20) or 1.20)
            premium_cap_mult = float(getattr(config, "VOL_PREMIUM_CAP_MULT_HIGH", 0.85) or 0.85)
            opening_premium_cap_mult = float(getattr(config, "VOL_OPEN_PREMIUM_CAP_MULT_HIGH", 0.90) or 0.90)

    return {
        "label": label,
        "score": int(score),
        "atr_pct": round(float(atr_pct), 4),
        "rvol": round(float(rvol), 4),
        "iv_rank": round(float(iv_rank), 4) if iv_available else -1.0,
        "iv_available": int(iv_available),
        "stop_loss_mult": float(stop_loss_mult),
        "premium_cap_mult": float(premium_cap_mult),
        "opening_premium_cap_mult": float(opening_premium_cap_mult),
    }


def _option_quote_snapshot(data_client: AlpacaDataClient, option_symbol: str) -> dict[str, float | None]:
    quote = data_client.get_latest_option_quote(option_symbol)
    bid_raw = quote.get("bid")
    ask_raw = quote.get("ask")
    bid = float(bid_raw) if bid_raw is not None else None
    ask = float(ask_raw) if ask_raw is not None else None
    midpoint = _quote_midpoint(bid, ask)
    return {
        "bid": bid,
        "ask": ask,
        "midpoint": midpoint,
        "spread_pct": round(_quote_spread_pct(bid, ask), 4),
    }


def _runtime_entry_max_quote_spread_pct(
    now_et: datetime,
    *,
    strategy_profile: str | None = None,
    spread_override_pct: float | None = None,
) -> float:
    base = float(getattr(config, "ENTRY_MAX_QUOTE_SPREAD_PCT", getattr(config, "MAX_OPTION_SPREAD_PCT", 30.0)))
    opening_base = float(getattr(config, "OPENING_ENTRY_MAX_QUOTE_SPREAD_PCT", base))

    try:
        entry_open_minutes = _minutes_from_hhmm(str(getattr(config, "NO_NEW_TRADES_BEFORE", config.MARKET_OPEN)))
        now_minutes = (now_et.hour * 60) + now_et.minute
        minutes_since_open = max(0, now_minutes - entry_open_minutes)
    except ValueError:
        minutes_since_open = 10**6

    if bool(getattr(config, "ENABLE_OPENING_ENTRY_RELAX", False)) and minutes_since_open <= int(getattr(config, "OPENING_ENTRY_RELAX_MINUTES", 0) or 0):
        base = max(base, opening_base)

    if spread_override_pct is not None:
        try:
            return max(1.0, float(spread_override_pct))
        except (TypeError, ValueError):
            pass

    if not is_enabled("FEATURE_STRATEGY_PROFILES", False):
        return base
    overrides = get_profile_overrides(strategy_profile)
    override = overrides.get("entry_max_quote_spread_pct")
    if override is None:
        return base
    return float(override)


def _runtime_entry_blocked_hours_et(*, strategy_profile: str | None = None) -> set[int]:
    raw_hours = getattr(config, "ENTRY_BLOCKED_HOURS_ET", ())
    if is_enabled("FEATURE_STRATEGY_PROFILES", False):
        overrides = get_profile_overrides(strategy_profile)
        override = overrides.get("entry_blocked_hours_et")
        if override is not None:
            raw_hours = override

    hours: set[int] = set()
    if not isinstance(raw_hours, (list, tuple, set)):
        return hours
    for value in raw_hours:
        try:
            hour = int(value)
        except (TypeError, ValueError):
            continue
        if 0 <= hour <= 23:
            hours.add(hour)
    return hours


def _is_entry_hour_blocked(now_et: datetime, *, strategy_profile: str | None = None) -> bool:
    return now_et.hour in _runtime_entry_blocked_hours_et(strategy_profile=strategy_profile)


def _is_in_opening_strict_window(now_et: datetime) -> bool:
    try:
        entry_open_minutes = _minutes_from_hhmm(str(getattr(config, "NO_NEW_TRADES_BEFORE", config.MARKET_OPEN)))
    except ValueError:
        return False
    now_minutes = (now_et.hour * 60) + now_et.minute
    window_minutes = max(0, int(getattr(config, "OPENING_STRICT_WINDOW_MINUTES", 10) or 10))
    return entry_open_minutes <= now_minutes < (entry_open_minutes + window_minutes)


def _current_open_premium_usd(option_positions: list, open_trade_meta: dict[str, dict]) -> float:
    total = 0.0
    seen_symbols: set[str] = set()
    for pos in option_positions:
        symbol = str(getattr(pos, "symbol", "") or "")
        qty = position_qty_as_int(getattr(pos, "qty", 0))
        if not symbol or qty <= 0:
            continue
        seen_symbols.add(symbol)
        meta = open_trade_meta.get(symbol, {})
        entry_price = float(meta.get("entry_price", getattr(pos, "avg_entry_price", 0) or 0) or 0)
        if entry_price > 0:
            total += entry_price * qty * 100.0

    for symbol, meta in open_trade_meta.items():
        if symbol in seen_symbols:
            continue
        qty = int(meta.get("qty", 0) or 0)
        entry_price = float(meta.get("entry_price", 0) or 0)
        if qty > 0 and entry_price > 0:
            total += entry_price * qty * 100.0
    return round(total, 4)


def _direction_exposure_counts(option_positions: list, open_trade_meta: dict[str, dict]) -> tuple[int, int]:
    call_symbols: set[str] = set()
    put_symbols: set[str] = set()

    def _add_symbol(symbol: str, direction: str) -> None:
        direction_lc = str(direction or "").lower()
        if direction_lc == "call":
            call_symbols.add(symbol)
        elif direction_lc == "put":
            put_symbols.add(symbol)

    seen_symbols: set[str] = set()
    for pos in option_positions:
        symbol = str(getattr(pos, "symbol", "") or "")
        qty = position_qty_as_int(getattr(pos, "qty", 0))
        if not symbol or qty <= 0:
            continue
        seen_symbols.add(symbol)
        meta = open_trade_meta.get(symbol, {})
        direction = str(meta.get("direction", "") or "")
        if direction not in ("call", "put"):
            _parsed_ticker, direction = _parse_option_symbol(symbol)
        _add_symbol(symbol, direction)

    for symbol, meta in open_trade_meta.items():
        if symbol in seen_symbols:
            continue
        qty = int(meta.get("qty", 0) or 0)
        if qty <= 0:
            continue
        direction = str(meta.get("direction", "") or "")
        if direction not in ("call", "put"):
            _parsed_ticker, direction = _parse_option_symbol(symbol)
        _add_symbol(symbol, direction)

    return len(call_symbols), len(put_symbols)


def _opening_entry_quality_ok(signal: dict[str, Any], now_et: datetime) -> tuple[bool, str]:
    if not _is_in_opening_strict_window(now_et):
        return True, ""
    try:
        direction_score = abs(float(signal.get("direction_score", 0.0) or 0.0))
        rvol = float(signal.get("rvol", 0.0) or 0.0)
        roc = abs(float(signal.get("roc", 0.0) or 0.0))
        price = float(signal.get("price", 0.0) or 0.0)
        vwap = float(signal.get("vwap", 0.0) or 0.0)
    except (TypeError, ValueError):
        return False, "opening quality parse failure"

    min_dir = float(getattr(config, "OPENING_STRICT_MIN_DIRECTION_SCORE", 0.55) or 0.55)
    min_rvol = float(getattr(config, "OPENING_STRICT_MIN_RVOL", 1.2) or 1.2)
    min_roc = float(getattr(config, "OPENING_STRICT_MIN_ROC_PCT", 0.18) or 0.18)
    min_vwap_dist = float(getattr(config, "OPENING_STRICT_MIN_VWAP_DISTANCE_PCT", 0.08) or 0.08)

    vwap_dist = 0.0
    if vwap > 0 and price > 0:
        vwap_dist = abs(price - vwap) / vwap * 100.0

    if direction_score < min_dir:
        return False, f"opening direction conviction too weak ({direction_score:.2f}<{min_dir:.2f})"
    if rvol < min_rvol:
        return False, f"opening RVOL too weak ({rvol:.2f}<{min_rvol:.2f})"
    if roc < min_roc:
        return False, f"opening ROC too weak ({roc:.2f}%<{min_roc:.2f}%)"
    if vwap_dist < min_vwap_dist:
        return False, f"opening VWAP distance too shallow ({vwap_dist:.2f}%<{min_vwap_dist:.2f}%)"
    return True, ""


def _fast_start_entry_quality_ok(signal: dict[str, Any], now_et: datetime) -> tuple[bool, str]:
    """Require setups that should work quickly, not slow drifts."""
    try:
        signal_score = float(signal.get("signal_score", 0.0) or 0.0)
        direction_score = abs(float(signal.get("direction_score", 0.0) or 0.0))
        rvol = float(signal.get("rvol", 0.0) or 0.0)
        roc_pct = abs(float(signal.get("roc", 0.0) or 0.0))
        price = float(signal.get("price", 0.0) or 0.0)
        vwap = float(signal.get("vwap", 0.0) or 0.0)
    except (TypeError, ValueError):
        return False, "fast-start parse failure"

    min_signal = float(getattr(config, "FAST_START_MIN_SIGNAL_SCORE", 7.0) or 7.0)
    min_direction = float(getattr(config, "FAST_START_MIN_DIRECTION_SCORE", 0.68) or 0.68)
    min_rvol = float(getattr(config, "FAST_START_MIN_RVOL", 1.25) or 1.25)
    min_abs_roc = float(getattr(config, "FAST_START_MIN_ABS_ROC_PCT", 0.16) or 0.16)
    min_vwap_dist = float(getattr(config, "FAST_START_MIN_VWAP_DISTANCE_PCT", 0.10) or 0.10)

    if _is_in_opening_strict_window(now_et):
        min_signal = max(min_signal, float(getattr(config, "OPENING_FAST_START_MIN_SIGNAL_SCORE", min_signal) or min_signal))
        min_direction = max(min_direction, float(getattr(config, "OPENING_FAST_START_MIN_DIRECTION_SCORE", min_direction) or min_direction))
        min_rvol = max(min_rvol, float(getattr(config, "OPENING_FAST_START_MIN_RVOL", min_rvol) or min_rvol))
        min_abs_roc = max(min_abs_roc, float(getattr(config, "OPENING_FAST_START_MIN_ABS_ROC_PCT", min_abs_roc) or min_abs_roc))
        min_vwap_dist = max(min_vwap_dist, float(getattr(config, "OPENING_FAST_START_MIN_VWAP_DISTANCE_PCT", min_vwap_dist) or min_vwap_dist))

    if is_at_or_after(now_et, config.RVOL_IGNORE_AFTER):
        min_rvol = 0.0
    elif is_at_or_after(now_et, config.RVOL_RELAX_AFTER):
        min_rvol = min(min_rvol, float(getattr(config, "RVOL_RELAXED_MIN", min_rvol) or min_rvol))

    vwap_dist = 0.0
    if vwap > 0 and price > 0:
        vwap_dist = abs(price - vwap) / vwap * 100.0

    if signal_score < min_signal:
        return False, f"signal score too weak ({signal_score:.2f}<{min_signal:.2f})"
    if direction_score < min_direction:
        return False, f"direction conviction too weak ({direction_score:.2f}<{min_direction:.2f})"
    if min_rvol > 0 and rvol < min_rvol:
        return False, f"RVOL too weak ({rvol:.2f}<{min_rvol:.2f})"
    if roc_pct < min_abs_roc:
        return False, f"ROC too weak ({roc_pct:.2f}%<{min_abs_roc:.2f}%)"
    if vwap_dist < min_vwap_dist:
        return False, f"VWAP distance too shallow ({vwap_dist:.2f}%<{min_vwap_dist:.2f}%)"
    return True, ""


def _premium_cap_quality_override_ok(
    *,
    signal: dict[str, Any],
    entry_quote: dict[str, float | None],
    now_et: datetime,
) -> tuple[bool, str]:
    """Allow expensive entries only when conviction and execution quality are exceptional."""
    if not bool(getattr(config, "ENABLE_PREMIUM_CAP_QUALITY_OVERRIDE", True)):
        return False, "premium quality override disabled"

    try:
        signal_score = float(signal.get("signal_score", 0.0) or 0.0)
        direction_score = abs(float(signal.get("direction_score", 0.0) or 0.0))
        rvol = float(signal.get("rvol", 0.0) or 0.0)
        spread_pct = float(entry_quote.get("spread_pct") or 0.0)
    except (TypeError, ValueError):
        return False, "premium override parse failure"

    min_signal_score = float(getattr(config, "EXPENSIVE_TRADE_MIN_SIGNAL_SCORE", 8.0) or 8.0)
    min_direction_score = float(getattr(config, "EXPENSIVE_TRADE_MIN_DIRECTION_SCORE", 0.75) or 0.75)
    min_rvol = float(getattr(config, "EXPENSIVE_TRADE_MIN_RVOL", 1.8) or 1.8)
    max_spread_pct = float(getattr(config, "EXPENSIVE_TRADE_MAX_SPREAD_PCT", 8.0) or 8.0)

    if _is_in_opening_strict_window(now_et):
        opening_min_signal = float(getattr(config, "OPENING_EXPENSIVE_TRADE_MIN_SIGNAL_SCORE", min_signal_score) or min_signal_score)
        min_signal_score = max(min_signal_score, opening_min_signal)

    if signal_score < min_signal_score:
        return False, f"signal score {signal_score:.2f}<{min_signal_score:.2f}"
    if direction_score < min_direction_score:
        return False, f"direction score {direction_score:.2f}<{min_direction_score:.2f}"
    if rvol < min_rvol:
        return False, f"RVOL {rvol:.2f}<{min_rvol:.2f}"
    if spread_pct > max_spread_pct:
        return False, f"spread {spread_pct:.2f}%>{max_spread_pct:.2f}%"
    return True, "high-conviction override"


def _entry_quote_spread_gate(
    *,
    option_symbol: str,
    entry_quote: dict[str, float | None],
    now_et: datetime,
    strategy_profile: str | None = None,
    spread_override_pct: float | None = None,
) -> tuple[bool, str]:
    ask_price = float(entry_quote.get("ask") or 0.0)
    if ask_price <= 0:
        return False, f"no option ask for {option_symbol}"

    spread_pct = float(entry_quote.get("spread_pct") or 0.0)
    max_spread_pct = _runtime_entry_max_quote_spread_pct(
        now_et,
        strategy_profile=strategy_profile,
        spread_override_pct=spread_override_pct,
    )
    if spread_pct > max_spread_pct:
        return False, f"live spread {spread_pct:.2f}% > max {max_spread_pct:.2f}%"

    return True, ""


def _buy_fill_slippage_vs_ask_pct(ask_price: float | None, fill_price: float | None) -> float:
    ask_value = float(ask_price or 0.0)
    fill_value = float(fill_price or 0.0)
    if ask_value <= 0 or fill_value <= 0:
        return 0.0
    return ((fill_value - ask_value) / ask_value) * 100.0


def _sell_fill_slippage_vs_bid_pct(bid_price: float | None, fill_price: float | None) -> float:
    bid_value = float(bid_price or 0.0)
    fill_value = float(fill_price or 0.0)
    if bid_value <= 0 or fill_value <= 0:
        return 0.0
    return ((bid_value - fill_value) / bid_value) * 100.0


def _paper_execution_friction_usd(qty: int, sides: int = 2) -> float:
    contracts = max(0, int(qty))
    side_count = max(0, int(sides))
    return contracts * side_count * float(getattr(config, "PAPER_EXECUTION_FRICTION_PER_CONTRACT", 0.0) or 0.0)


def _conservative_executable_pnl(
    *,
    entry_ask_price: float | None,
    exit_bid_price: float | None,
    qty: int,
) -> tuple[float, float]:
    entry_value = float(entry_ask_price or 0.0)
    exit_value = float(exit_bid_price or 0.0)
    contracts = max(0, int(qty))
    if entry_value <= 0 or exit_value <= 0 or contracts <= 0:
        return 0.0, 0.0
    gross_usd = (exit_value - entry_value) * contracts * 100.0
    net_usd = gross_usd - _paper_execution_friction_usd(contracts, sides=2)
    basis = entry_value * contracts * 100.0
    net_pct = (net_usd / basis) * 100.0 if basis > 0 else 0.0
    return round(net_usd, 2), round(net_pct, 4)


def _is_runner_eligible(
    symbol: str,
    ticker: str,
    meta: dict,
    data_client: AlpacaDataClient,
    now_et: datetime,
) -> bool:
    """
    Determine if a profitable trade should be promoted to 'runner mode'
    (allowed to run for larger gains) vs immediately banked.
    
    A runner is eligible if:
    1. Momentum signature is still strong (trend continuing)
    2. Price structure remains aligned (no reversal signals)
    3. Spread/liquidity acceptable (from meta)
    
    For now, we check EMA alignment and early reversal signals.
    """
    if not isinstance(meta, dict):
        return False
    
    try:
        trade_direction = str(meta.get("direction", "") or "").lower()
        if trade_direction not in ("call", "put"):
            return False
        
        # Get recent bars
        bars = data_client.get_intraday_bars_since_open(
            symbol=ticker, now_et=now_et, limit=12
        )
        if bars is None or bars.empty or len(bars) < 5:
            return False
        
        closes = bars["close"].astype(float)
        
        # Check 1: EMA alignment (9 above 21 for calls, below for puts)
        ema9 = closes.ewm(span=9, adjust=False).mean()
        ema21 = closes.ewm(span=21, adjust=False).mean()
        
        ema_aligned = False
        if len(ema9) >= 2 and len(ema21) >= 2:
            if trade_direction == "call" and ema9.iloc[-1] > ema21.iloc[-1]:
                ema_aligned = True
            elif trade_direction == "put" and ema9.iloc[-1] < ema21.iloc[-1]:
                ema_aligned = True
        
        # Check 2: Early reversal signals (only 1 out of 3 is OK; 2+ means don't run)
        from scanner import calculate_vwap
        
        # Signal A: ROC of last 2 bars
        roc_reversed = False
        if len(closes) >= 3:
            prev2 = float(closes.iloc[-3])
            curr = float(closes.iloc[-1])
            last2_roc = ((curr - prev2) / prev2) * 100 if prev2 > 0 else 0
            reversal_roc_threshold = float(getattr(config, "REVERSAL_ROC_THRESHOLD_PCT", 0.30))
            if trade_direction == "call" and last2_roc <= -reversal_roc_threshold:
                roc_reversed = True
            elif trade_direction == "put" and last2_roc >= reversal_roc_threshold:
                roc_reversed = True
        
        # Signal B: VWAP flip
        vwap_flipped = False
        vwap = calculate_vwap(bars)
        if vwap and not (vwap != vwap):  # not nan
            curr_price = float(closes.iloc[-1])
            if trade_direction == "call" and curr_price < vwap:
                vwap_flipped = True
            elif trade_direction == "put" and curr_price > vwap:
                vwap_flipped = True
        
        # Count reversal signals
        reversal_count = sum([roc_reversed, vwap_flipped])
        
        # Runner is eligible if:
        # - EMA aligned AND
        # - Fewer than 2 reversal signals (0 or 1 is OK; 2+ means don't trust it)
        return ema_aligned and reversal_count < 2
        
    except Exception:  # noqa: BLE001
        # Conservatively assume not runner eligible if any error
        return False


def _apply_profit_protection(meta: dict, plpc: float, now_et: datetime = None) -> None:
    """
    Apply profit protection: at +3%, move the stop floor to breakeven or slight green.
    This prevents a green trade from turning red while still allowing upside.
    
    Called once when profit first reaches +3%.
    """
    if not isinstance(meta, dict):
        return
    
    # Only apply once
    if meta.get("profit_protection_applied"):
        return
    
    if plpc >= 0.03:  # +3% threshold
        # Move stop floor to breakeven-ish (with slight cushion)
        current_floor = float(meta.get("stop_floor_plpc", 0) or 0)
        if current_floor < -0.005:  # Only move if stop is deeper than we want
            meta["stop_floor_plpc"] = -0.005  # Protect to -0.5% (slight green)
            meta["profit_protection_applied"] = True


def _runner_near_close_blocked(now_et: datetime) -> bool:
    cutoff = str(getattr(config, "RUNNER_DISABLE_AFTER_ET", getattr(config, "NO_NEW_TRADES_AFTER", "14:30")) or "14:30")
    try:
        return is_at_or_after(now_et, cutoff)
    except Exception:
        return False


def _trade_state_from_meta(meta: dict[str, Any]) -> str:
    raw = str(meta.get("trade_state", "") or "").strip().lower()
    if raw in {"unproven", "protected", "bank_or_qualify", "runner"}:
        return raw
    if bool(meta.get("runner_mode")):
        return "runner"
    return "unproven"


def _await_order_fill(
    broker: AlpacaBroker,
    *,
    order_id: str,
    requested_qty: int,
    now_et: datetime,
    label: str,
    poll_seconds: int,
    max_wait_seconds: int,
) -> tuple[int, float | None, str, bool]:
    deadline = time.time() + max_wait_seconds
    observed_filled = 0
    observed_avg_price: float | None = None
    last_status = ""
    non_fill_terminal = {"canceled", "cancelled", "rejected", "expired", "done_for_day", "stopped", "suspended"}

    while time.time() < deadline:
        try:
            status_order = broker.get_order_status(order_id)
        except Exception as exc:  # noqa: BLE001
            print(f"[{ts(now_et)}] {label}: order status error for {order_id}: {exc}")
            time.sleep(poll_seconds)
            continue

        last_status = str(getattr(status_order, "status", "") or "").lower()
        filled_qty = position_qty_as_int(getattr(status_order, "filled_qty", 0))
        observed_filled = max(observed_filled, filled_qty)
        avg_price_raw = getattr(status_order, "filled_avg_price", None)
        try:
            avg_price = float(avg_price_raw) if avg_price_raw is not None else None
        except (TypeError, ValueError):
            avg_price = None
        if avg_price is not None and avg_price > 0:
            observed_avg_price = avg_price
        if observed_filled > 0 and last_status in ("filled", "partially_filled"):
            return min(max(0, int(requested_qty)), observed_filled), observed_avg_price, last_status, False
        if last_status in non_fill_terminal:
            break
        time.sleep(poll_seconds)

    if observed_filled > 0:
        return min(max(0, int(requested_qty)), observed_filled), observed_avg_price, last_status, False
    is_still_open = last_status not in non_fill_terminal and last_status not in ("", "filled", "partially_filled")
    return 0, None, last_status, is_still_open


def _execute_limit_entry(
    *,
    broker: AlpacaBroker,
    data_client: AlpacaDataClient,
    option_symbol: str,
    qty: int,
    now_et: datetime,
    label: str,
    initial_quote: dict[str, float | None] | None = None,
) -> dict[str, object]:
    quote_snapshot = dict(initial_quote or _option_quote_snapshot(data_client, option_symbol))
    ask_price = float(quote_snapshot.get("ask") or 0.0)
    if ask_price <= 0:
        return {"filled": False, "status": "no_ask", "attempts": 0}

    max_attempts = max(1, int(getattr(config, "ENTRY_LIMIT_ATTEMPTS", 1) or 1))
    attempt_quotes = [quote_snapshot]
    if max_attempts > 1:
        retry_quote = _option_quote_snapshot(data_client, option_symbol)
        retry_ask = float(retry_quote.get("ask") or 0.0)
        if retry_ask > 0:
            attempt_quotes.append(retry_quote)
        else:
            attempt_quotes.append(quote_snapshot)
    attempt_quotes = attempt_quotes[:max_attempts]

    for attempt_index, submit_quote in enumerate(attempt_quotes, start=1):
        submit_ask = float(submit_quote.get("ask") or 0.0)
        if submit_ask <= 0:
            continue
        if attempt_index == 1:
            limit_price = round(submit_ask, 2)
            wait_seconds = max(1, int(config.ENTRY_ORDER_STATUS_WAIT_SECONDS))
        else:
            retry_pct = max(0.0, float(getattr(config, "ENTRY_RETRY_LIMIT_PCT", 0.02) or 0.02))
            limit_price = round(max(submit_ask, submit_ask * (1.0 + retry_pct)), 2)
            wait_seconds = max(1, int(config.ENTRY_RETRY_STATUS_WAIT_SECONDS))

        submit_ts = time.time()
        order = broker.place_option_limit_buy(option_symbol, qty, limit_price)
        filled_qty, fill_price, status, still_open = _await_order_fill(
            broker,
            order_id=str(getattr(order, "id", "") or ""),
            requested_qty=qty,
            now_et=now_et,
            label=f"{label} attempt {attempt_index}",
            poll_seconds=1,
            max_wait_seconds=wait_seconds,
        )
        fill_seconds = max(0.0, time.time() - submit_ts)
        if filled_qty > 0:
            return {
                "filled": True,
                "status": status,
                "filled_qty": filled_qty,
                "filled_price": fill_price,
                "attempts": attempt_index,
                "submit_bid": submit_quote.get("bid"),
                "submit_ask": submit_quote.get("ask"),
                "submit_midpoint": submit_quote.get("midpoint"),
                "submit_spread_pct": submit_quote.get("spread_pct"),
                "intended_limit": limit_price,
                "fill_seconds": round(fill_seconds, 3),
                "fill_slippage_vs_ask_pct": round(_buy_fill_slippage_vs_ask_pct(submit_quote.get("ask"), fill_price), 4),
                "order_id": str(getattr(order, "id", "") or ""),
            }

        order_id = str(getattr(order, "id", "") or "")
        if still_open:
            return {
                "filled": False,
                "status": status or "pending_open",
                "attempts": attempt_index,
                "submit_bid": submit_quote.get("bid"),
                "submit_ask": submit_quote.get("ask"),
                "submit_midpoint": submit_quote.get("midpoint"),
                "submit_spread_pct": submit_quote.get("spread_pct"),
                "intended_limit": limit_price,
                "fill_seconds": round(fill_seconds, 3),
                "order_id": order_id,
                "pending_open": True,
            }

    if bool(getattr(config, "ENABLE_ENTRY_MARKET_FALLBACK", True)):
        market_wait_seconds = max(1, int(getattr(config, "ENTRY_MARKET_FALLBACK_WAIT_SECONDS", 3) or 3))
        try:
            market_order = broker.place_option_market_buy(option_symbol, qty)
            market_order_id = str(getattr(market_order, "id", "") or "")
            if market_order_id:
                filled_qty, fill_price, status, still_open = _await_order_fill(
                    broker,
                    order_id=market_order_id,
                    requested_qty=qty,
                    now_et=now_et,
                    label=f"{label} market-fallback",
                    poll_seconds=1,
                    max_wait_seconds=market_wait_seconds,
                )
                if filled_qty > 0:
                    return {
                        "filled": True,
                        "status": status,
                        "filled_qty": filled_qty,
                        "filled_price": fill_price,
                        "attempts": len(attempt_quotes) + 1,
                        "submit_bid": None,
                        "submit_ask": None,
                        "submit_midpoint": None,
                        "submit_spread_pct": None,
                        "intended_limit": None,
                        "fill_seconds": None,
                        "fill_slippage_vs_ask_pct": None,
                        "order_id": market_order_id,
                        "fallback": "market",
                    }
                if still_open:
                    try:
                        broker.cancel_order(market_order_id)
                    except Exception as exc:  # noqa: BLE001
                        print(f"[{ts(now_et)}] {label}: cancel market fallback order {market_order_id} failed: {exc}")
        except Exception as exc:  # noqa: BLE001
            print(f"[{ts(now_et)}] {label}: market fallback submit failed: {exc}")

    return {"filled": False, "status": "not_filled", "attempts": len(attempt_quotes)}


def _position_plpc_snapshot(pos) -> float | None:
    """
    Best-effort normalized unrealized P&L % for a long option position.
    Returns None when a usable value is unavailable.
    """
    try:
        pos_current = float(getattr(pos, "current_price", 0) or 0)
        pos_entry = float(getattr(pos, "avg_entry_price", 0) or 0)
        if pos_entry > 0 and pos_current > 0:
            return (pos_current - pos_entry) / pos_entry
    except (TypeError, ValueError):
        pass

    try:
        raw_plpc = float(getattr(pos, "unrealized_plpc", 0) or 0)
        if math.isfinite(raw_plpc):
            return raw_plpc
    except (TypeError, ValueError):
        pass

    qty = position_qty_as_int(getattr(pos, "qty", 0))
    try:
        pos_entry = float(getattr(pos, "avg_entry_price", 0) or 0)
        unrealized_pl = float(getattr(pos, "unrealized_pl", 0) or 0)
        if qty > 0 and pos_entry > 0:
            basis = pos_entry * qty * 100.0
            if basis > 0:
                derived = unrealized_pl / basis
                if math.isfinite(derived):
                    return derived
    except (TypeError, ValueError):
        pass
    return None


def _live_option_mark_and_plpc(
    data_client: AlpacaDataClient,
    option_symbol: str,
    entry_price: float,
) -> tuple[float | None, float | None]:
    if entry_price <= 0:
        return None, None
    try:
        quote = _option_quote_snapshot(data_client, option_symbol)
        bid = float(quote.get("bid") or 0.0)
        if bid <= 0:
            return None, None
        return bid, ((bid - entry_price) / entry_price)
    except Exception as exc:  # noqa: BLE001
        print(f"[main] live option quote unavailable for {option_symbol}: {exc}")
        return None, None


def _order_reject_reason(order) -> str:
    for field in ("rejected_reason", "cancel_reject_reason", "failed_at"):
        value = getattr(order, field, None)
        if value:
            return f"{field}={value}"
    return ""


def _is_news_block_day(now_et: datetime) -> bool:
    return now_et.date().isoformat() in set(config.NEWS_BLOCK_DATES_ET)


def _fetch_vix_level() -> float | None:
    try:
        ticker = yf.Ticker("^VIX")
        fast = getattr(ticker, "fast_info", None)
        if fast is not None:
            price = getattr(fast, "last_price", None)
            if price is None and isinstance(fast, dict):
                price = fast.get("last_price")
            if price is not None:
                value = float(price)
                if value > 0:
                    return value
    except Exception as exc:  # noqa: BLE001
        print(f"[main] VIX lookup failed: {exc}")
    return None


def _parse_trade_meta_entry_time(meta: dict) -> datetime | None:
    raw = str(meta.get("entry_time_iso", "") or "").strip()
    if not raw:
        # Backward-compatible fallback for older runtime records that only had
        # "timestamp" in "%Y-%m-%d %H:%M:%S %Z" format.
        raw = str(meta.get("timestamp", "") or "").strip()
        if not raw:
            return None
        for fmt in ("%Y-%m-%d %H:%M:%S %Z", "%Y-%m-%d %H:%M:%S"):
            try:
                parsed = datetime.strptime(raw, fmt)
                if parsed.tzinfo is None:
                    parsed = pytz.timezone(config.EASTERN_TZ).localize(parsed)
                return parsed.astimezone(pytz.timezone(config.EASTERN_TZ))
            except Exception:
                continue
        return None
    try:
        parsed = datetime.fromisoformat(raw)
        if parsed.tzinfo is None:
            return pytz.timezone(config.EASTERN_TZ).localize(parsed)
        return parsed.astimezone(pytz.timezone(config.EASTERN_TZ))
    except Exception:
        return None


def _is_in_anti_churn_window(entry_time: datetime | None, now_et: datetime) -> bool:
    """
    Return True while a position is inside the minimum hold window.

    This is intentionally stronger than a discretionary anti-churn helper:
    strategy exits wait until the minimum hold has elapsed so the bot does not
    scalp in and out during early noise.
    """
    if entry_time is None:
        return False
    try:
        hold_minutes = float(getattr(config, "ANTI_CHURN_HOLD_MINUTES", 3) or 3)
        elapsed = (now_et - entry_time).total_seconds() / 60.0
        return elapsed < hold_minutes
    except Exception:
        return False


def _minimum_hold_blocks_exit(exit_reason: str | None, entry_time: datetime | None, now_et: datetime) -> bool:
    if not exit_reason or not _is_in_anti_churn_window(entry_time, now_et):
        return False
    bypass_reasons = {
        str(item).strip().lower()
        for item in getattr(config, "MIN_HOLD_EXIT_BYPASS_REASONS", ())
        if str(item).strip()
    }
    return str(exit_reason or "").strip().lower() not in bypass_reasons


def _parse_state_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None:
            return pytz.timezone(config.EASTERN_TZ).localize(parsed)
        return parsed.astimezone(pytz.timezone(config.EASTERN_TZ))
    except Exception:
        return None


def _looks_like_auth_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return "401" in text or "unauthorized" in text or "authorization required" in text


def _latest_5m_move_pct(data_client: AlpacaDataClient, symbol: str, now_et: datetime) -> float | None:
    try:
        bars = data_client.get_intraday_bars_since_open(symbol=symbol, now_et=now_et, limit=3)
        if bars is None or bars.empty or len(bars) < 2:
            return None
        prev_close = float(bars["close"].iloc[-2])
        last_close = float(bars["close"].iloc[-1])
        if prev_close <= 0:
            return None
        return ((last_close - prev_close) / prev_close) * 100.0
    except Exception:
        return None


_OPTION_SYMBOL_RE = re.compile(r"^([A-Z.]+)\d{6}([CP])\d{8}$")
_SCAN_SYMBOL_RE = re.compile(r"^[A-Z][A-Z.]{0,5}$")


def _parse_option_symbol(option_symbol: str) -> tuple[str, str]:
    symbol = str(option_symbol or "").upper().strip()
    match = _OPTION_SYMBOL_RE.match(symbol)
    if not match:
        return "", ""
    ticker = match.group(1)
    cp = match.group(2)
    direction = "call" if cp == "C" else "put"
    return ticker, direction


def _looks_like_junk_scan_symbol(symbol: str, *, protected: set[str]) -> bool:
    sym = str(symbol or "").upper().strip()
    if not sym:
        return True
    if sym in protected:
        return False
    if not _SCAN_SYMBOL_RE.match(sym):
        return True
    if len(sym) > 5 and "." not in sym:
        return True

    # Common warrant/right/unit tails that pollute mover feeds.
    if len(sym) == 5 and sym[-1] in {"W", "R", "U"}:
        return True
    if "." in sym:
        suffix = sym.split(".", 1)[1]
        if suffix in {"W", "WS", "WT", "WTS", "R", "RT", "U", "UN", "UNIT"}:
            return True
    return False


def _filter_mover_candidates(
    data_client: AlpacaDataClient,
    symbols: list[str],
    *,
    protected: set[str],
) -> list[str]:
    kept: list[str] = []
    for raw in symbols:
        sym = str(raw or "").upper().strip()
        if not sym:
            continue
        if _looks_like_junk_scan_symbol(sym, protected=protected):
            continue
        if sym in protected:
            kept.append(sym)
            continue

        try:
            asset = data_client.get_asset(sym)
            if not bool(asset.get("tradable", False)):
                continue
            if not bool(asset.get("options_enabled", False)):
                continue
            if str(asset.get("status", "active") or "active").lower() != "active":
                continue
            price = data_client.get_latest_stock_price(sym)
            if price is None:
                continue
            if price < float(config.MIN_SHARE_PRICE) or price > float(config.MAX_SHARE_PRICE):
                continue
            kept.append(sym)
        except Exception:
            continue
    return list(dict.fromkeys(kept))


def _is_valid_long_direction(direction: str) -> bool:
    return str(direction or "").lower() in ("call", "put")


def _option_symbol_matches_direction(option_symbol: str, direction: str) -> bool:
    _ticker, parsed_direction = _parse_option_symbol(option_symbol)
    if not parsed_direction:
        return False
    return parsed_direction == str(direction or "").lower()


def _enum_text(value) -> str:
    try:
        value = getattr(value, "value", value)
    except Exception:
        pass
    text = str(value or "").strip().lower()
    if "." in text:
        text = text.split(".")[-1]
    return text


def _as_et_datetime(value, tz) -> datetime | None:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return tz.localize(value)
        return value.astimezone(tz)
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return tz.localize(parsed)
        return parsed.astimezone(tz)
    except Exception:
        return None


def _order_fill_price(order) -> float:
    for field in ("filled_avg_price", "average_fill_price", "limit_price"):
        try:
            value = float(getattr(order, field, 0) or 0)
        except (TypeError, ValueError):
            value = 0.0
        if value > 0:
            return value
    return 0.0


_ACTIVE_ENTRY_ORDER_STATUSES = {
    "new",
    "accepted",
    "accepted_for_bidding",
    "pending_new",
    "pending_replace",
    "partially_filled",
}


def _is_active_entry_order_status(status: str) -> bool:
    return _enum_text(status) in _ACTIVE_ENTRY_ORDER_STATUSES


def _alpaca_option_day_pnl_snapshot(broker: AlpacaBroker, now_et: datetime) -> dict[str, object]:
    tz = pytz.timezone(config.EASTERN_TZ)
    filled: list[dict[str, object]] = []
    for order in broker.get_recent_orders(limit=500):
        if _enum_text(getattr(order, "status", "")) != "filled":
            continue
        symbol = str(getattr(order, "symbol", "") or "").upper()
        if not _parse_option_symbol(symbol)[0]:
            continue
        filled_at = _as_et_datetime(
            getattr(order, "filled_at", None) or getattr(order, "submitted_at", None),
            tz,
        )
        if filled_at is None or filled_at.date() != now_et.date():
            continue
        side = _enum_text(getattr(order, "side", ""))
        qty = position_qty_as_int(getattr(order, "filled_qty", None) or getattr(order, "qty", 0))
        price = _order_fill_price(order)
        if side not in {"buy", "sell"} or qty <= 0 or price <= 0:
            continue
        filled.append({"symbol": symbol, "side": side, "qty": qty, "price": price, "filled_at": filled_at})

    filled.sort(key=lambda item: item["filled_at"])
    lots: dict[str, list[dict[str, float]]] = {}
    closed: list[dict[str, object]] = []
    for item in filled:
        symbol = str(item["symbol"])
        qty_remaining = float(item["qty"])
        price = float(item["price"])
        side = str(item["side"])
        if side == "buy":
            lots.setdefault(symbol, []).append(
                {"qty": qty_remaining, "price": price, "filled_at": item["filled_at"]}
            )
            continue

        realized = 0.0
        paired_qty = 0.0
        entry_notional = 0.0
        first_entry_at = None
        open_lots = lots.setdefault(symbol, [])
        while qty_remaining > 0 and open_lots:
            lot = open_lots[0]
            use_qty = min(qty_remaining, float(lot["qty"]))
            lot_price = float(lot["price"])
            lot_time = lot.get("filled_at")
            realized += (price - lot_price) * use_qty * 100.0
            entry_notional += lot_price * use_qty
            if isinstance(lot_time, datetime) and (first_entry_at is None or lot_time < first_entry_at):
                first_entry_at = lot_time
            lot["qty"] = float(lot["qty"]) - use_qty
            qty_remaining -= use_qty
            paired_qty += use_qty
            if float(lot["qty"]) <= 0:
                open_lots.pop(0)
        if paired_qty > 0:
            ticker, direction = _parse_option_symbol(symbol)
            cost_basis = entry_notional * 100.0
            exit_time = item["filled_at"]
            hold_seconds = 0
            if isinstance(first_entry_at, datetime) and isinstance(exit_time, datetime):
                hold_seconds = max(0, int((exit_time - first_entry_at).total_seconds()))
            closed.append(
                {
                    "symbol": symbol,
                    "ticker": ticker,
                    "direction": direction,
                    "qty": round(paired_qty, 4),
                    "entry_price": round((entry_notional / paired_qty), 4) if paired_qty > 0 else 0.0,
                    "exit_price": round(price, 4),
                    "entry_time": first_entry_at.isoformat() if isinstance(first_entry_at, datetime) else "",
                    "exit_time": exit_time.isoformat() if isinstance(exit_time, datetime) else "",
                    "hold_seconds": hold_seconds,
                    "realized_pnl_usd": round(realized, 2),
                    "realized_pnl_pct": round((realized / cost_basis) * 100.0, 4) if cost_basis > 0 else 0.0,
                }
            )

    return {
        "realized_pnl_usd": round(sum(float(item["realized_pnl_usd"]) for item in closed), 2),
        "closed_count": len(closed),
        "filled_option_orders_today": len(filled),
        "closed_trades": closed,
        "losing_tickers": sorted(
            {
                str(item["ticker"])
                for item in closed
                if float(item["realized_pnl_usd"]) < 0 and str(item["ticker"])
            }
        ),
        "open_lot_symbols": sorted([symbol for symbol, symbol_lots in lots.items() if symbol_lots]),
    }


def _alpaca_option_buy_order_counts_by_ticker_today(
    broker: AlpacaBroker,
    now_et: datetime,
    *,
    limit: int = 500,
) -> dict[str, int]:
    tz = pytz.timezone(config.EASTERN_TZ)
    counts: dict[str, int] = {}
    count_canceled = bool(getattr(config, "ALPACA_BUY_ORDER_CAP_COUNTS_CANCELED", True))
    canceled_cooldown_minutes = max(
        0,
        int(getattr(config, "ALPACA_CANCELED_BUY_ORDER_COOLDOWN_MINUTES", 0) or 0),
    )
    ignored_statuses = {"rejected", "expired"}
    if not count_canceled:
        ignored_statuses.add("canceled")
        ignored_statuses.add("cancelled")

    for order in broker.get_recent_orders(limit=max(1, int(limit))):
        if _enum_text(getattr(order, "side", "")) != "buy":
            continue
        status = _enum_text(getattr(order, "status", ""))
        submitted_at = _as_et_datetime(
            getattr(order, "submitted_at", None) or getattr(order, "created_at", None),
            tz,
        )
        if submitted_at is None or submitted_at.date() != now_et.date():
            continue
        if (
            not count_canceled
            and status in {"canceled", "cancelled"}
        ):
            if (
                canceled_cooldown_minutes <= 0
                or (now_et - submitted_at).total_seconds() >= (canceled_cooldown_minutes * 60)
            ):
                continue
        elif status in ignored_statuses:
            continue
        ticker, _direction = _parse_option_symbol(str(getattr(order, "symbol", "") or ""))
        ticker = str(ticker or "").upper()
        if ticker:
            counts[ticker] = int(counts.get(ticker, 0)) + 1
    return counts


def _alpaca_active_option_buy_order_counts_by_ticker_today(
    broker: AlpacaBroker,
    now_et: datetime,
    *,
    limit: int = 500,
) -> dict[str, int]:
    tz = pytz.timezone(config.EASTERN_TZ)
    counts: dict[str, int] = {}
    for order in broker.get_recent_orders(limit=max(1, int(limit))):
        if _enum_text(getattr(order, "side", "")) != "buy":
            continue
        if not _is_active_entry_order_status(getattr(order, "status", "")):
            continue
        submitted_at = _as_et_datetime(
            getattr(order, "submitted_at", None) or getattr(order, "created_at", None),
            tz,
        )
        if submitted_at is None or submitted_at.date() != now_et.date():
            continue
        ticker, _direction = _parse_option_symbol(str(getattr(order, "symbol", "") or ""))
        ticker = str(ticker or "").upper()
        if ticker:
            counts[ticker] = int(counts.get(ticker, 0)) + 1
    return counts


def _alpaca_active_option_buy_order_premium_usd_today(
    broker: AlpacaBroker,
    now_et: datetime,
    *,
    limit: int = 500,
) -> float:
    tz = pytz.timezone(config.EASTERN_TZ)
    total = 0.0
    for order in broker.get_recent_orders(limit=max(1, int(limit))):
        if _enum_text(getattr(order, "side", "")) != "buy":
            continue
        if not _is_active_entry_order_status(getattr(order, "status", "")):
            continue
        submitted_at = _as_et_datetime(
            getattr(order, "submitted_at", None) or getattr(order, "created_at", None),
            tz,
        )
        if submitted_at is None or submitted_at.date() != now_et.date():
            continue
        symbol = str(getattr(order, "symbol", "") or "").upper()
        if not _parse_option_symbol(symbol)[0]:
            continue
        qty = position_qty_as_int(getattr(order, "qty", 0))
        price = _order_fill_price(order)
        if qty <= 0 or price <= 0:
            continue
        total += qty * price * 100.0
    return round(total, 2)


def _cancel_stale_active_entry_buy_orders(
    broker: AlpacaBroker,
    now_et: datetime,
    *,
    limit: int = 500,
) -> int:
    max_age_minutes = max(0.0, float(getattr(config, "ENTRY_RESTING_ORDER_MAX_MINUTES", 0) or 0))
    if max_age_minutes <= 0:
        return 0
    tz = pytz.timezone(config.EASTERN_TZ)
    canceled = 0
    try:
        orders = broker.get_recent_orders(limit=max(1, int(limit)))
    except Exception as exc:  # noqa: BLE001
        print(f"[{ts(now_et)}] Stale entry-order lookup failed: {exc}")
        return 0

    for order in orders:
        if _enum_text(getattr(order, "side", "")) != "buy":
            continue
        if not _is_active_entry_order_status(getattr(order, "status", "")):
            continue
        symbol = str(getattr(order, "symbol", "") or "").upper()
        if not _parse_option_symbol(symbol)[0]:
            continue
        submitted_at = _as_et_datetime(
            getattr(order, "submitted_at", None) or getattr(order, "created_at", None),
            tz,
        )
        if submitted_at is None or submitted_at.date() != now_et.date():
            continue
        age_minutes = (now_et - submitted_at).total_seconds() / 60.0
        if age_minutes < max_age_minutes:
            continue
        order_id = str(getattr(order, "id", "") or "")
        if not order_id:
            continue
        try:
            broker.cancel_order(order_id)
            canceled += 1
            print(
                f"[{ts(now_et)}] Canceled stale entry buy order {order_id} "
                f"{symbol} age={age_minutes:.1f}m max={max_age_minutes:.1f}m."
            )
        except Exception as exc:  # noqa: BLE001
            print(f"[{ts(now_et)}] Cancel stale entry order {order_id} failed: {exc}")
    return canceled


def _cancel_active_entry_buys_for_open_tickers(
    broker: AlpacaBroker,
    option_positions: list,
    now_et: datetime,
    *,
    limit: int = 500,
) -> int:
    open_tickers: set[str] = set()
    for pos in option_positions or []:
        qty = position_qty_as_int(getattr(pos, "qty", 0))
        if qty <= 0:
            continue
        symbol = str(getattr(pos, "symbol", "") or "").upper()
        ticker = str(getattr(pos, "underlying_symbol", "") or "").upper()
        if not ticker:
            ticker, _direction = _parse_option_symbol(symbol)
        if ticker:
            open_tickers.add(ticker.upper())
    if not open_tickers:
        return 0

    tz = pytz.timezone(config.EASTERN_TZ)
    canceled = 0
    try:
        orders = broker.get_recent_orders(limit=max(1, int(limit)))
    except Exception as exc:  # noqa: BLE001
        print(f"[{ts(now_et)}] Duplicate entry-order lookup failed: {exc}")
        return 0

    for order in orders:
        if _enum_text(getattr(order, "side", "")) != "buy":
            continue
        if not _is_active_entry_order_status(getattr(order, "status", "")):
            continue
        symbol = str(getattr(order, "symbol", "") or "").upper()
        ticker, _direction = _parse_option_symbol(symbol)
        ticker = ticker.upper()
        if not ticker or ticker not in open_tickers:
            continue
        submitted_at = _as_et_datetime(
            getattr(order, "submitted_at", None) or getattr(order, "created_at", None),
            tz,
        )
        if submitted_at is None or submitted_at.date() != now_et.date():
            continue
        order_id = str(getattr(order, "id", "") or "")
        if not order_id:
            continue
        try:
            broker.cancel_order(order_id)
            canceled += 1
            print(
                f"[{ts(now_et)}] Canceled duplicate entry buy order {order_id} "
                f"{symbol}: ticker {ticker} already has an open option position."
            )
        except Exception as exc:  # noqa: BLE001
            print(f"[{ts(now_et)}] Cancel duplicate entry order {order_id} failed: {exc}")
    return canceled


def _parse_option_expiry_from_symbol(option_symbol: str) -> date | None:
    symbol = str(option_symbol or "").upper().strip()
    match = _OPTION_SYMBOL_RE.match(symbol)
    if not match:
        return None
    # OCC format includes YYMMDD immediately after root.
    date_chunk = symbol[len(match.group(1)) : len(match.group(1)) + 6]
    try:
        yy = int(date_chunk[0:2])
        mm = int(date_chunk[2:4])
        dd = int(date_chunk[4:6])
        return date(2000 + yy, mm, dd)
    except Exception:
        return None


def _option_expiry_date(meta: dict, option_symbol: str) -> date | None:
    raw = str(meta.get("expiry", "") or "").strip()
    if raw:
        try:
            return date.fromisoformat(raw[:10])
        except Exception as exc:  # noqa: BLE001
            print(f"[main] invalid expiry in trade meta for {option_symbol}: {raw!r} ({exc})")
    return _parse_option_expiry_from_symbol(option_symbol)


def _parse_expiration_text(value: str | None) -> date | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw[:10])
    except Exception:
        return None


def _subtract_trading_days(end_date: date, days: int) -> date:
    cursor = end_date
    remaining = max(0, int(days))
    while remaining > 0:
        cursor -= timedelta(days=1)
        if cursor.weekday() < 5:
            remaining -= 1
    return cursor


def _index_regime_bias(data_client: AlpacaDataClient, now_et: datetime) -> str:
    if not config.ENABLE_INDEX_BIAS_FILTER:
        return "both"
    trend_votes: list[str] = []
    for symbol in ("SPY", "QQQ"):
        try:
            bars = data_client.get_stock_bars(
                symbol=symbol,
                timeframe=config.INDEX_BIAS_TIMEFRAME,
                limit=max(25, int(config.INDEX_BIAS_LOOKBACK)),
            )
            if bars is None or bars.empty or len(bars) < 21:
                continue
            closes = bars["close"].astype(float)
            ema9 = closes.ewm(span=9, adjust=False).mean()
            ema21 = closes.ewm(span=21, adjust=False).mean()
            if ema9.iloc[-1] > ema21.iloc[-1] and ema21.iloc[-1] > ema21.iloc[-2]:
                trend_votes.append("call")
            elif ema9.iloc[-1] < ema21.iloc[-1] and ema21.iloc[-1] < ema21.iloc[-2]:
                trend_votes.append("put")
            else:
                trend_votes.append("both")
        except Exception:
            trend_votes.append("both")
    if trend_votes and all(v == "call" for v in trend_votes):
        return "call"
    if trend_votes and all(v == "put" for v in trend_votes):
        return "put"
    return "both"


def _entry_confirmation_passes(
    data_client: AlpacaDataClient,
    ticker: str,
    direction: str,
    now_et: datetime,
) -> bool:
    if not config.ENABLE_ENTRY_CONFIRMATION:
        return True
    try:
        confirm_bars = max(2, int(getattr(config, "ENTRY_CONFIRM_BARS", 2) or 2))
        momentum_threshold_pct = float(getattr(config, "ENTRY_CONFIRM_MOMENTUM_THRESHOLD_PCT", 0.06) or 0.06)
        opening_strict = _is_in_opening_strict_window(now_et)
        if opening_strict:
            confirm_bars = max(confirm_bars, int(getattr(config, "OPENING_STRICT_CONFIRM_BARS", confirm_bars) or confirm_bars))
            momentum_threshold_pct = max(
                momentum_threshold_pct,
                float(
                    getattr(
                        config,
                        "OPENING_STRICT_CONFIRM_MOMENTUM_THRESHOLD_PCT",
                        momentum_threshold_pct,
                    )
                    or momentum_threshold_pct
                ),
            )

        bars = data_client.get_intraday_bars_since_open(
            symbol=ticker,
            now_et=now_et,
            limit=max(3, confirm_bars),
        )
        if bars is None or bars.empty or len(bars) < max(2, confirm_bars):
            return False
        closes = bars["close"].astype(float)
        last_close = float(closes.iloc[-1])
        prev_close = float(closes.iloc[-2])
        two_bar_ref = float(closes.iloc[-3]) if len(closes) >= 3 else prev_close
        confirm_ref = float(closes.iloc[-confirm_bars]) if len(closes) >= confirm_bars else two_bar_ref
        if two_bar_ref <= 0 or prev_close <= 0 or confirm_ref <= 0:
            return False

        one_bar_move_pct = ((last_close - prev_close) / prev_close) * 100.0
        two_bar_move_pct = ((last_close - two_bar_ref) / two_bar_ref) * 100.0
        confirm_window_move_pct = ((last_close - confirm_ref) / confirm_ref) * 100.0

        if opening_strict:
            if direction == "call":
                return (last_close > prev_close) and (confirm_window_move_pct >= momentum_threshold_pct)
            return (last_close < prev_close) and (confirm_window_move_pct <= -momentum_threshold_pct)

        if direction == "call":
            return (
                last_close > prev_close
                and confirm_window_move_pct >= momentum_threshold_pct
                and one_bar_move_pct >= (momentum_threshold_pct * 0.5)
            )
        return (
            last_close < prev_close
            and confirm_window_move_pct <= -momentum_threshold_pct
            and one_bar_move_pct <= -(momentum_threshold_pct * 0.5)
        )
    except Exception:
        return False


def _hydrate_missing_position_meta(open_trade_meta: dict[str, dict], option_positions: list, now_et: datetime) -> int:
    hydrated = 0
    for pos in option_positions:
        symbol = str(getattr(pos, "symbol", "") or "")
        if not symbol or symbol in open_trade_meta:
            continue
        qty = position_qty_as_int(getattr(pos, "qty", 0))
        if qty <= 0:
            continue
        underlying = str(getattr(pos, "underlying_symbol", "") or "").upper()
        ticker, direction = _parse_option_symbol(symbol)
        if not ticker:
            ticker = underlying
        if not direction:
            direction = "call" if "C" in symbol else "put" if "P" in symbol else ""
        entry_price = float(getattr(pos, "avg_entry_price", 0) or 0)
        open_trade_meta[symbol] = {
            "timestamp": ts(now_et),
            "entry_time_iso": now_et.isoformat(),
            "strategy_profile": "hydrated_external",
            "ticker": ticker,
            "direction": direction,
            "option_symbol": symbol,
            "strike": "",
            "expiry": "",
            "qty": qty,
            "entry_price": entry_price,
            "stop_floor_plpc": -float(config.STOP_LOSS_PCT),
            "stop_loss_usd": float(getattr(config, "STOP_LOSS_USD", 10.0)),
            "immediate_take_profit_pct": float(getattr(config, "IMMEDIATE_TAKE_PROFIT_PCT", 1.0) or 1.0),
            "max_hold_minutes": int(getattr(config, "MAX_HOLD_MINUTES", 90) or 90),
            "trade_state": "unproven",
            "runner_mode": False,
            "max_plpc": 0.0,
            "min_plpc": 0.0,
            "inferred": True,
        }
        hydrated += 1
    return hydrated


def _prune_stale_open_trade_meta(open_trade_meta: dict[str, dict], option_positions: list) -> int:
    live_symbols: set[str] = set()
    for pos in option_positions:
        symbol = str(getattr(pos, "symbol", "") or "")
        qty = position_qty_as_int(getattr(pos, "qty", 0))
        if symbol and qty > 0:
            live_symbols.add(symbol)

    stale_symbols = [symbol for symbol in list(open_trade_meta) if symbol not in live_symbols]
    for symbol in stale_symbols:
        open_trade_meta.pop(symbol, None)
    return len(stale_symbols)


def _detect_catalyst_event(
    data_client: AlpacaDataClient,
    now_et: datetime,
    watchlist: list[str],
) -> tuple[bool, str]:
    idx_threshold = float(config.CATALYST_INDEX_5M_MOVE_PCT)
    spy_move = _latest_5m_move_pct(data_client, "SPY", now_et)
    qqq_move = _latest_5m_move_pct(data_client, "QQQ", now_et)
    if spy_move is not None and qqq_move is not None and abs(spy_move) >= idx_threshold and abs(qqq_move) >= idx_threshold:
        same_dir = (spy_move > 0 and qqq_move > 0) or (spy_move < 0 and qqq_move < 0)
        if same_dir:
            direction = "UP" if spy_move > 0 else "DOWN"
            return True, f"{direction} shock: SPY {spy_move:+.2f}% / QQQ {qqq_move:+.2f}% (5m)"

    breadth_symbols = list(dict.fromkeys((watchlist or [])[:20] + list(config.CORE_TICKERS)[:10]))
    up = 0
    down = 0
    breadth_threshold = float(config.CATALYST_BREADTH_MOVE_PCT)
    for symbol in breadth_symbols:
        move = _latest_5m_move_pct(data_client, symbol, now_et)
        if move is None:
            continue
        if move >= breadth_threshold:
            up += 1
        elif move <= -breadth_threshold:
            down += 1
    required = int(config.CATALYST_BREADTH_MIN_COUNT)
    if up >= required:
        return True, f"Breadth shock UP: {up} names >= +{breadth_threshold:.2f}% (5m)"
    if down >= required:
        return True, f"Breadth shock DOWN: {down} names <= -{breadth_threshold:.2f}% (5m)"
    return False, ""


def _build_scan_universe(data_client: AlpacaDataClient) -> list[str]:
    base = [str(sym).upper() for sym in config.TICKERS if str(sym).strip()]
    core = [str(sym).upper() for sym in config.CORE_TICKERS if str(sym).strip()]
    protected = set(base + core)
    base = list(dict.fromkeys(base + core))
    mover_candidates: list[str] = []
    universe_mode = str(getattr(config, "UNIVERSE_MODE", "core") or "core").strip().lower()
    if universe_mode in {"all", "all_optionable", "all_optionable_assets"}:
        try:
            all_optionable = data_client.get_all_optionable_tickers(
                max_count=max(1, int(getattr(config, "UNIVERSE_MAX_TICKERS", 60) or 60) * 3)
            )
            base.extend(str(sym).upper() for sym in all_optionable if str(sym).strip())
        except Exception as exc:  # noqa: BLE001
            print(f"[{ts()}] All-optionable universe unavailable ({exc}); using core/movers fallback.")
    if config.AUTO_EXPAND_UNIVERSE_WITH_MOVERS or universe_mode == "movers":
        try:
            gainers, losers = data_client.get_top_movers(top=int(config.UNIVERSE_MOVER_TOP))
            mover_candidates.extend(str(sym).upper() for sym in gainers if str(sym).strip())
            mover_candidates.extend(str(sym).upper() for sym in losers if str(sym).strip())
        except Exception as exc:  # noqa: BLE001
            print(f"[{ts()}] Universe expansion skipped (movers unavailable): {exc}")

    mover_candidates = [s for s in list(dict.fromkeys(mover_candidates)) if s not in protected]
    filtered_movers = _filter_mover_candidates(
        data_client,
        mover_candidates,
        protected=protected,
    )
    dropped_count = max(0, len(mover_candidates) - len(filtered_movers))
    if dropped_count > 0:
        print(f"[{ts()}] Universe cleanup removed {dropped_count} junk/non-tradable mover symbols before scan.")

    deduped = list(dict.fromkeys(base + filtered_movers))
    max_tickers = max(1, int(config.UNIVERSE_MAX_TICKERS))
    return deduped[:max_tickers]


def _apply_watchlist_mode(universe: list[str], control: dict) -> list[str]:
    mode = str(control.get("mode", "off") or "off").strip().lower()
    protected = set(str(s).upper() for s in config.CORE_TICKERS)
    tickers = [
        str(s).upper()
        for s in (control.get("tickers") or [])
        if str(s).strip() and not _looks_like_junk_scan_symbol(str(s), protected=protected)
    ]
    ticker_set = set(tickers)
    if mode == "only_listed":
        return tickers
    if mode == "exclude_listed" and ticker_set:
        return [s for s in universe if s not in ticker_set]
    return universe


def _dedupe_signals_by_symbol(signals: list[dict]) -> list[dict]:
    deduped: list[dict] = []
    seen: set[str] = set()
    for signal in signals:
        symbol = str(signal.get("symbol", "") or "").upper()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        deduped.append(signal)
    return deduped


def _signal_sort_key(signal: dict) -> tuple[float, float]:
    try:
        score = float(signal.get("signal_score", 0.0) or 0.0)
    except (TypeError, ValueError):
        score = 0.0
    try:
        rvol = float(signal.get("rvol", 0.0) or 0.0)
    except (TypeError, ValueError):
        rvol = 0.0
    return score, rvol


def _flatten_positions_for_killswitch(broker: AlpacaBroker, now_et: datetime, *, label: str = "KILLSWITCH") -> None:
    poll_seconds = max(1, int(config.EXIT_ORDER_STATUS_POLL_SECONDS))
    max_wait_seconds = max(poll_seconds, int(config.EXIT_ORDER_MAX_WAIT_SECONDS))
    retry_attempts = max(1, int(config.EXIT_CLOSE_RETRY_ATTEMPTS))
    non_fill_terminal = {"canceled", "cancelled", "rejected", "expired", "done_for_day", "stopped", "suspended"}

    def _wait_for_fill(order_id: str, close_qty: int) -> tuple[int, str, bool]:
        deadline = time.time() + max_wait_seconds
        observed_filled = 0
        last_status = ""
        while time.time() < deadline:
            try:
                status_order = broker.get_order_status(order_id)
            except Exception as exc:  # noqa: BLE001
                print(f"[{ts(now_et)}] {label} status error for {order_id}: {exc}")
                time.sleep(poll_seconds)
                continue
            last_status = str(getattr(status_order, "status", "")).lower()
            filled_qty = position_qty_as_int(getattr(status_order, "filled_qty", 0))
            observed_filled = max(observed_filled, filled_qty)
            if observed_filled > 0 and last_status in ("filled", "partially_filled"):
                return min(close_qty, observed_filled), last_status, False
            if last_status in non_fill_terminal:
                break
            time.sleep(poll_seconds)
        if observed_filled > 0:
            return min(close_qty, observed_filled), last_status, False
        is_still_open = last_status not in non_fill_terminal and last_status not in ("", "filled", "partially_filled")
        return 0, last_status, is_still_open

    option_positions = broker.get_open_option_positions()
    for pos in option_positions:
        symbol = str(getattr(pos, "symbol", ""))
        qty = position_qty_as_int(getattr(pos, "qty", 0))
        if qty <= 0:
            continue
        try:
            filled_qty = 0
            for attempt in range(1, retry_attempts + 1):
                order = broker.close_option_market(symbol, qty)
                order_id = str(getattr(order, "id", "") or "")
                if not order_id:
                    print(f"[{ts(now_et)}] {label} {symbol}: close submitted without order id.")
                    break
                filled_qty, status, still_open = _wait_for_fill(order_id, qty)
                if filled_qty > 0:
                    print(f"[{ts(now_et)}] {label} CLOSE {symbol} qty={filled_qty}/{qty}")
                    break
                if still_open:
                    print(f"[{ts(now_et)}] {label} {symbol}: close pending ({status or 'unknown'}).")
                    break
                if attempt < retry_attempts:
                    print(
                        f"[{ts(now_et)}] {label} {symbol}: close attempt {attempt}/{retry_attempts} "
                        f"ended status={status or 'unknown'}, retrying."
                    )
            if filled_qty <= 0:
                print(f"[{ts(now_et)}] {label} {symbol}: close not confirmed.")
        except Exception as exc:  # noqa: BLE001
            print(f"[{ts(now_et)}] {label} close error for {symbol}: {exc}")
        time.sleep(config.RATE_LIMIT_SLEEP_SECONDS)
    try:
        broker.cancel_all_open_orders()
    except Exception as exc:  # noqa: BLE001
        print(f"[{ts(now_et)}] {label} cancel orders error: {exc}")


def main():
    api_key = get_required_env("ALPACA_API_KEY")
    secret_key = get_required_env("ALPACA_SECRET_KEY")

    tz = pytz.timezone(config.EASTERN_TZ)
    broker = AlpacaBroker(api_key, secret_key, paper=config.PAPER)
    data_client = AlpacaDataClient(api_key, secret_key, paper=config.PAPER)
    initialize_scanner(data_client)
    trade_logger = TradeLogger()
    alerts = AlertManager()
    state = load_bot_state()
    open_trade_meta: dict[str, dict] = dict(state.get("open_trade_meta") or {})
    watchlist: list[str] = []
    observation_done = bool(state.get("observation_done", False))
    hot_tickers: list[str] = list(state.get("hot_tickers") or [])
    entry_times_rolling: list[datetime] = [
        dt
        for dt in (_parse_iso_datetime(item) for item in (state.get("entry_times_rolling") or []))
        if dt is not None
    ]
    daily_realized_loss_usd = float(state.get("daily_realized_loss_usd", 0.0) or 0.0)
    weekly_realized_loss_usd = float(state.get("weekly_realized_loss_usd", 0.0) or 0.0)
    consecutive_losses = int(state.get("consecutive_losses", 0) or 0)
    consecutive_loss_cooldown_until = _parse_state_datetime(state.get("consecutive_loss_cooldown_until_iso"))
    consecutive_loss_cooldown_loss_count = int(state.get("consecutive_loss_cooldown_loss_count", 0) or 0)
    consecutive_loss_recovery_announced_count = int(state.get("consecutive_loss_recovery_announced_count", 0) or 0)
    loss_counters_day_raw = state.get("loss_counters_day")
    loss_counters_day = None
    if loss_counters_day_raw:
        try:
            loss_counters_day = date.fromisoformat(str(loss_counters_day_raw))
        except (TypeError, ValueError):
            print(f"[{ts()}] Invalid loss_counters_day in runtime state: {loss_counters_day_raw!r}. Resetting.")
    weekly_loss_key = str(state.get("weekly_loss_key") or _week_key(datetime.now(tz).date()))
    blocked_day_notice = state.get("blocked_day_notice")
    vix_block_notice = state.get("vix_block_notice")
    catalyst_mode_active = bool(state.get("catalyst_mode_active", False))
    catalyst_mode_reason = str(state.get("catalyst_mode_reason", "") or "")
    catalyst_mode_until = _parse_state_datetime(state.get("catalyst_mode_until_iso"))
    ticker_entry_counts: dict[str, int] = {
        str(k): int(v)
        for k, v in dict(state.get("ticker_entry_counts") or {}).items()
    }
    ticker_reentry_armed: dict[str, bool] = {
        str(k): bool(v)
        for k, v in dict(state.get("ticker_reentry_armed") or {}).items()
    }
    ticker_reentry_expected_direction: dict[str, str] = {
        str(k): str(v)
        for k, v in dict(state.get("ticker_reentry_expected_direction") or {}).items()
    }
    ticker_reentries_used: dict[str, int] = {
        str(k): int(v)
        for k, v in dict(state.get("ticker_reentries_used") or {}).items()
    }
    ticker_loss_cooldown_until: dict[str, datetime] = {}
    for _k, _v in dict(state.get("ticker_loss_cooldown_until") or {}).items():
        _dt = _parse_state_datetime(_v)
        if _dt is not None:
            ticker_loss_cooldown_until[str(_k).upper()] = _dt
    ticker_roundtrip_cooldown_until: dict[str, datetime] = {}
    for _k, _v in dict(state.get("ticker_roundtrip_cooldown_until") or {}).items():
        _dt = _parse_state_datetime(_v)
        if _dt is not None:
            ticker_roundtrip_cooldown_until[str(_k).upper()] = _dt
    last_entry_debug: dict = dict(state.get("last_entry_debug") or {})
    last_exit_debug: dict = dict(state.get("last_exit_debug") or {})
    open_position_pl_history: dict[str, list[dict[str, float | str | None]]] = dict(
        state.get("open_position_pl_history") or {}
    )
    premarket_opening_signals: list[dict] = list(state.get("premarket_opening_signals") or [])
    premarket_signals_day = str(state.get("premarket_signals_day", "") or "")
    premarket_scan_runs = int(state.get("premarket_scan_runs", 0) or 0)
    premarket_last_scan_at = _parse_state_datetime(state.get("premarket_last_scan_at_iso"))
    bad_fill_tracker: dict[str, dict] = dict(state.get("bad_fill_tracker") or {})
    watchlist_control_state: dict = dict(state.get("watchlist_control") or {})
    last_trader_heartbeat_et = str(state.get("last_trader_heartbeat_et", "") or "")
    last_alpaca_auth_error_et = str(state.get("last_alpaca_auth_error_et", "") or "")
    last_alpaca_auth_error = str(state.get("last_alpaca_auth_error", "") or "")
    trade_telemetry_day = str(state.get("trade_telemetry_day", "") or "")
    trade_telemetry_closed_count = int(state.get("trade_telemetry_closed_count", 0) or 0)
    trade_telemetry_total_pnl_usd = float(state.get("trade_telemetry_total_pnl_usd", 0.0) or 0.0)
    trade_telemetry_last_close_iso = str(state.get("trade_telemetry_last_close_iso", "") or "")
    trade_telemetry_last_log_error = str(state.get("trade_telemetry_last_log_error", "") or "")
    broker_truth_day_pnl_usd = float(state.get("broker_truth_day_pnl_usd", 0.0) or 0.0)
    broker_truth_closed_count = int(state.get("broker_truth_closed_count", 0) or 0)
    broker_truth_last_error = str(state.get("broker_truth_last_error", "") or "")
    adaptive_loss_active = bool(state.get("adaptive_loss_active", False))
    adaptive_loss_blocked_tickers: set[str] = set(
        str(item).upper()
        for item in (state.get("adaptive_loss_blocked_tickers") or [])
        if str(item).strip()
    )
    adaptive_loss_profile: dict = dict(state.get("adaptive_loss_profile") or {})
    signal_pattern_memory: dict[str, dict] = dict(state.get("signal_pattern_memory") or {})
    opening_entries_today_count = int(state.get("opening_entries_today_count", 0) or 0)
    opening_fresh_premium_deployed_usd = float(state.get("opening_fresh_premium_deployed_usd", 0.0) or 0.0)
    opening_expensive_entries_today_count = int(state.get("opening_expensive_entries_today_count", 0) or 0)
    non_core_entries_today_count = int(state.get("non_core_entries_today_count", 0) or 0)
    next_heartbeat_at = 0.0
    last_heartbeat_persist_at = 0.0
    manual_stop_latched = False
    control_state = load_trading_control()
    strategy_profile = normalize_profile_name(str(control_state.get("strategy_profile", "balanced") or "balanced"))
    dry_run_enabled = bool(control_state.get("dry_run", False)) or is_enabled("FEATURE_DRY_RUN_MODE", False)
    option_expiry_cache: dict[str, date | None] = {}

    def _resolve_option_expiry(symbol: str, meta: dict) -> date | None:
        from_meta = _option_expiry_date(meta, symbol)
        if from_meta is not None:
            option_expiry_cache[symbol] = from_meta
            return from_meta
        if symbol in option_expiry_cache:
            return option_expiry_cache[symbol]
        fetched_expiry: date | None = None
        try:
            contract = data_client.get_option_contract(symbol)
            fetched_expiry = _parse_expiration_text(contract.get("expiration_date")) if isinstance(contract, dict) else None
        except Exception:
            fetched_expiry = None
        option_expiry_cache[symbol] = fetched_expiry
        if fetched_expiry is not None and symbol in open_trade_meta:
            open_trade_meta[symbol]["expiry"] = fetched_expiry.isoformat()
        return fetched_expiry

    def _runtime_stop_loss_usd() -> float:
        base = float(getattr(config, "STOP_LOSS_USD", 10.0))
        if not is_enabled("FEATURE_STRATEGY_PROFILES", False):
            return base
        overrides = get_profile_overrides(strategy_profile)
        override = overrides.get("stop_loss_usd")
        if override is None:
            return base
        return float(override)

    def _runtime_entry_min_signal_score() -> float:
        base = float(getattr(config, "MIN_SIGNAL_SCORE", 5.0))
        if not is_enabled("FEATURE_STRATEGY_PROFILES", False):
            return base
        overrides = get_profile_overrides(strategy_profile)
        override = overrides.get("entry_min_signal_score")
        if override is None:
            return base
        return float(override)

    def _is_bad_fill_blocked(ticker: str, now_et: datetime) -> bool:
        if not is_enabled("FEATURE_BAD_FILL_DETECTOR", False):
            return False
        info = bad_fill_tracker.get(str(ticker).upper())
        if not isinstance(info, dict):
            return False
        until_dt = _parse_state_datetime(info.get("blocked_until_iso"))
        if until_dt is None:
            return False
        return now_et < until_dt

    def _record_bad_fill_event(ticker: str, now_et: datetime, slippage_pct: float) -> None:
        if not is_enabled("FEATURE_BAD_FILL_DETECTOR", False):
            return
        symbol = str(ticker).upper()
        info = bad_fill_tracker.get(symbol)
        if not isinstance(info, dict):
            info = {}
        count = int(info.get("count", 0) or 0) + 1
        info["count"] = count
        info["last_slippage_pct"] = round(float(slippage_pct), 4)
        info["last_seen_iso"] = now_et.isoformat()
        if count >= 2:
            cooldown_minutes = 45
            blocked_until = now_et + timedelta(minutes=cooldown_minutes)
            info["blocked_until_iso"] = blocked_until.isoformat()
            info["count"] = 0
        bad_fill_tracker[symbol] = info

    if catalyst_mode_until and datetime.now(tz) >= catalyst_mode_until:
        catalyst_mode_active = False
        catalyst_mode_reason = ""
        catalyst_mode_until = None
    set_catalyst_mode(catalyst_mode_active, catalyst_mode_reason)

    def _save_runtime_state() -> None:
        save_bot_state(
            {
                "open_trade_meta": open_trade_meta,
                "watchlist": watchlist,
                "observation_done": observation_done,
                "hot_tickers": hot_tickers,
                "entry_times_rolling": [item.isoformat() for item in entry_times_rolling],
                "daily_realized_loss_usd": round(daily_realized_loss_usd, 6),
                "weekly_realized_loss_usd": round(weekly_realized_loss_usd, 6),
                "consecutive_losses": consecutive_losses,
                "consecutive_loss_cooldown_until_iso": (
                    consecutive_loss_cooldown_until.isoformat()
                    if isinstance(consecutive_loss_cooldown_until, datetime)
                    else ""
                ),
                "consecutive_loss_cooldown_loss_count": consecutive_loss_cooldown_loss_count,
                "consecutive_loss_recovery_announced_count": consecutive_loss_recovery_announced_count,
                "loss_counters_day": loss_counters_day.isoformat() if loss_counters_day else None,
                "weekly_loss_key": weekly_loss_key,
                "blocked_day_notice": blocked_day_notice,
                "vix_block_notice": vix_block_notice,
                "catalyst_mode_active": catalyst_mode_active,
                "catalyst_mode_reason": catalyst_mode_reason,
                "catalyst_mode_until_iso": catalyst_mode_until.isoformat() if catalyst_mode_until else "",
                "ticker_entry_counts": ticker_entry_counts,
                "ticker_reentry_armed": ticker_reentry_armed,
                "ticker_reentry_expected_direction": ticker_reentry_expected_direction,
                "ticker_reentries_used": ticker_reentries_used,
                "ticker_loss_cooldown_until": {
                    str(k).upper(): v.isoformat()
                    for k, v in ticker_loss_cooldown_until.items()
                    if isinstance(v, datetime)
                },
                "ticker_roundtrip_cooldown_until": {
                    str(k).upper(): v.isoformat()
                    for k, v in ticker_roundtrip_cooldown_until.items()
                    if isinstance(v, datetime)
                },
                "last_entry_debug": last_entry_debug,
                "last_exit_debug": last_exit_debug,
                "open_position_pl_history": open_position_pl_history,
                "premarket_opening_signals": premarket_opening_signals,
                "premarket_signals_day": premarket_signals_day,
                "premarket_scan_runs": premarket_scan_runs,
                "premarket_last_scan_at_iso": premarket_last_scan_at.isoformat() if premarket_last_scan_at else "",
                "bad_fill_tracker": bad_fill_tracker,
                "watchlist_control": watchlist_control_state,
                "last_trader_heartbeat_et": last_trader_heartbeat_et,
                "last_alpaca_auth_error_et": last_alpaca_auth_error_et,
                "last_alpaca_auth_error": last_alpaca_auth_error,
                "trade_telemetry_day": trade_telemetry_day,
                "trade_telemetry_closed_count": trade_telemetry_closed_count,
                "trade_telemetry_total_pnl_usd": round(trade_telemetry_total_pnl_usd, 6),
                "trade_telemetry_last_close_iso": trade_telemetry_last_close_iso,
                "trade_telemetry_last_log_error": trade_telemetry_last_log_error,
                "broker_truth_day_pnl_usd": round(broker_truth_day_pnl_usd, 6),
                "broker_truth_closed_count": broker_truth_closed_count,
                "broker_truth_last_error": broker_truth_last_error,
                "adaptive_loss_active": adaptive_loss_active,
                "adaptive_loss_blocked_tickers": sorted(adaptive_loss_blocked_tickers),
                "adaptive_loss_profile": adaptive_loss_profile,
                "signal_pattern_memory": signal_pattern_memory,
                "opening_entries_today_count": opening_entries_today_count,
                "opening_fresh_premium_deployed_usd": round(opening_fresh_premium_deployed_usd, 6),
                "opening_expensive_entries_today_count": opening_expensive_entries_today_count,
                "non_core_entries_today_count": non_core_entries_today_count,
            }
        )

    def _touch_heartbeat(*, force: bool = False) -> None:
        nonlocal last_trader_heartbeat_et, last_heartbeat_persist_at
        now_ts = time.time()
        min_interval = max(5, int(config.LOOP_INTERVAL_SECONDS) // 2)
        if (not force) and (now_ts - last_heartbeat_persist_at) < min_interval:
            return
        last_trader_heartbeat_et = datetime.now(tz).isoformat()
        _save_runtime_state()
        last_heartbeat_persist_at = now_ts

    def _consecutive_loss_cooldown_active(now_et: datetime) -> bool:
        nonlocal consecutive_loss_cooldown_until, consecutive_loss_cooldown_loss_count
        nonlocal consecutive_loss_recovery_announced_count
        limit = int(getattr(config, "CONSECUTIVE_LOSS_LIMIT", 0) or 0)
        if limit <= 0 or consecutive_losses < limit:
            consecutive_loss_cooldown_until = None
            consecutive_loss_cooldown_loss_count = 0
            consecutive_loss_recovery_announced_count = 0
            return False

        if bool(getattr(config, "CONSECUTIVE_LOSS_HARD_STOP_REST_OF_DAY", False)):
            _mark_skip("consecutive_loss_limit")
            print(
                f"[{ts(now_et)}] {consecutive_losses} consecutive losses. "
                "Pausing new entries for the rest of the day."
            )
            return True

        cooldown_minutes = max(0, int(getattr(config, "CONSECUTIVE_LOSS_COOLDOWN_MINUTES", 0) or 0))
        if cooldown_minutes <= 0:
            return False

        needs_new_cooldown = (
            consecutive_loss_cooldown_loss_count != consecutive_losses
            or consecutive_loss_cooldown_until is None
        )
        if needs_new_cooldown:
            consecutive_loss_cooldown_until = now_et + timedelta(minutes=cooldown_minutes)
            consecutive_loss_cooldown_loss_count = consecutive_losses
            consecutive_loss_recovery_announced_count = 0
            _mark_skip("consecutive_loss_cooldown")
            print(
                f"[{ts(now_et)}] {consecutive_losses} consecutive losses. "
                f"Cooling down new entries for {cooldown_minutes}m, then recovery mode can trade stricter setups."
            )
            _save_runtime_state()
            return True

        if consecutive_loss_cooldown_until > now_et:
            remaining_seconds = int((consecutive_loss_cooldown_until - now_et).total_seconds())
            _mark_skip("consecutive_loss_cooldown")
            print(
                f"[{ts(now_et)}] {consecutive_losses} consecutive losses. "
                f"Cooldown active for {max(1, remaining_seconds // 60)}m more."
            )
            return True

        if consecutive_loss_recovery_announced_count != consecutive_losses:
            consecutive_loss_recovery_announced_count = consecutive_losses
            print(
                f"[{ts(now_et)}] Consecutive-loss cooldown expired after {consecutive_losses} losses; "
                "recovery mode will allow only stricter loss-throttle setups."
            )
            _save_runtime_state()
        return False

    def _active_ticker_loss_cooldown_until(ticker: str, now_et: datetime) -> datetime | None:
        key = str(ticker or "").upper()
        if not key:
            return None
        until_dt = ticker_loss_cooldown_until.get(key)
        if until_dt is None:
            return None
        if until_dt <= now_et:
            ticker_loss_cooldown_until.pop(key, None)
            return None
        return until_dt

    def _set_ticker_loss_cooldown(ticker: str, now_et: datetime, *, minutes: int, reason: str) -> None:
        key = str(ticker or "").upper()
        if not key:
            return
        cooldown_minutes = max(1, int(minutes))
        until_dt = now_et + timedelta(minutes=cooldown_minutes)
        prior = ticker_loss_cooldown_until.get(key)
        if prior is None or until_dt > prior:
            ticker_loss_cooldown_until[key] = until_dt
        print(
            f"[{ts(now_et)}] {key}: loss cooldown armed for {cooldown_minutes}m "
            f"(until {ts(ticker_loss_cooldown_until[key])}; reason={reason})."
        )

    def _active_ticker_roundtrip_cooldown_until(ticker: str, now_et: datetime) -> datetime | None:
        key = str(ticker or "").upper()
        if not key:
            return None
        until_dt = ticker_roundtrip_cooldown_until.get(key)
        if until_dt is None:
            return None
        if until_dt <= now_et:
            ticker_roundtrip_cooldown_until.pop(key, None)
            return None
        return until_dt

    def _set_ticker_roundtrip_cooldown(ticker: str, now_et: datetime, *, minutes: int, reason: str) -> None:
        key = str(ticker or "").upper()
        if not key:
            return
        cooldown_minutes = max(0, int(minutes))
        if cooldown_minutes <= 0:
            return
        until_dt = now_et + timedelta(minutes=cooldown_minutes)
        prior = ticker_roundtrip_cooldown_until.get(key)
        if prior is None or until_dt > prior:
            ticker_roundtrip_cooldown_until[key] = until_dt
        print(
            f"[{ts(now_et)}] {key}: round-trip diversification cooldown armed for {cooldown_minutes}m "
            f"(until {ts(ticker_roundtrip_cooldown_until[key])}; reason={reason})."
        )

    def _opposite_direction(direction: str) -> str:
        normalized = str(direction or "").strip().lower()
        if normalized == "call":
            return "put"
        if normalized == "put":
            return "call"
        return ""

    def _arm_loss_direction_flip(ticker: str, losing_direction: str, now_et: datetime, *, reason: str) -> str:
        if not bool(getattr(config, "ENABLE_LOSS_DIRECTION_FLIP", True)):
            return ""
        key = str(ticker or "").upper()
        opposite = _opposite_direction(losing_direction)
        if not key or opposite not in {"call", "put"}:
            return ""
        ticker_reentry_armed[key] = True
        ticker_reentry_expected_direction[key] = opposite
        print(
            f"[{ts(now_et)}] {key}: loss direction flip armed. "
            f"Lost on {str(losing_direction).upper()}, next allowed direction is {opposite.upper()} "
            f"(reason={reason})."
        )
        return opposite

    def _pattern_bucket(value, *, step: float, limit: float | None = None) -> str:
        raw = value
        if raw is None or str(raw).strip() == "":
            return "na"
        try:
            number = float(raw)
        except (TypeError, ValueError):
            return "na"
        if math.isnan(number) or math.isinf(number):
            return "na"
        if limit is not None:
            number = max(-float(limit), min(float(limit), number))
        bucket = round(number / float(step)) * float(step)
        if abs(bucket) < (float(step) / 2.0):
            bucket = 0.0
        return f"{bucket:+.2f}"

    def _time_pattern_bucket(value) -> str:
        dt = _parse_state_datetime(str(value or "")) if value else None
        hour = dt.hour if isinstance(dt, datetime) else datetime.now(tz).hour
        if hour < 10:
            return "open"
        if hour < 12:
            return "morning"
        if hour < 14:
            return "midday"
        return "late"

    def _vwap_side_bucket(payload: dict) -> str:
        price = _safe_signal_float(payload.get("price"), 0.0)
        vwap = _safe_signal_float(payload.get("vwap"), 0.0)
        if price <= 0 or vwap <= 0:
            return "na"
        distance_pct = ((price - vwap) / vwap) * 100.0
        if abs(distance_pct) < 0.05:
            return "near"
        return "above" if distance_pct > 0 else "below"

    def _signal_pattern_signature(payload: dict, *, now_et: datetime | None = None) -> str:
        entry_time = payload.get("entry_time") or payload.get("entry_time_iso") or ""
        time_bucket = _time_pattern_bucket(entry_time) if entry_time else _time_pattern_bucket((now_et or datetime.now(tz)).isoformat())
        profile = str(payload.get("strategy_profile", "") or payload.get("profile", "") or "generic").strip().lower()
        parts = [
            f"t={time_bucket}",
            f"p={profile}",
            f"vwap={_vwap_side_bucket(payload)}",
            f"rvol={_pattern_bucket(payload.get('rvol'), step=0.5, limit=8.0)}",
            f"roc={_pattern_bucket(payload.get('roc'), step=0.1, limit=5.0)}",
            f"rsi={_pattern_bucket(payload.get('rsi'), step=5.0, limit=100.0)}",
            f"ds={_pattern_bucket(payload.get('direction_score'), step=0.1, limit=1.0)}",
            f"sig={_pattern_bucket(payload.get('signal_score'), step=0.5, limit=12.0)}",
            f"vol={_pattern_bucket(payload.get('volatility_score'), step=0.5, limit=12.0)}",
            f"flow={_pattern_bucket(payload.get('flow_score'), step=0.25, limit=1.0)}",
            f"atr={_pattern_bucket(payload.get('atr_pct'), step=0.5, limit=10.0)}",
            f"ivr={_pattern_bucket(payload.get('iv_rank'), step=10.0, limit=100.0)}",
        ]
        return "|".join(parts)

    def _signal_pattern_keys(payload: dict, ticker: str, *, now_et: datetime | None = None) -> tuple[str, str, str]:
        signature = str(payload.get("signal_pattern_signature", "") or "").strip()
        if not signature:
            signature = _signal_pattern_signature(payload, now_et=now_et)
        symbol = str(ticker or payload.get("ticker", "") or payload.get("symbol", "") or "").upper()
        ticker_key = f"ticker:{symbol}|{signature}" if symbol else ""
        global_key = f"global|{signature}"
        return signature, ticker_key, global_key

    def _signal_number_summary(payload: dict) -> str:
        return (
            f"vwap={_vwap_side_bucket(payload)}, "
            f"rvol={_pattern_bucket(payload.get('rvol'), step=0.5, limit=8.0)}, "
            f"roc={_pattern_bucket(payload.get('roc'), step=0.1, limit=5.0)}, "
            f"rsi={_pattern_bucket(payload.get('rsi'), step=5.0, limit=100.0)}, "
            f"direction_score={_pattern_bucket(payload.get('direction_score'), step=0.1, limit=1.0)}, "
            f"signal_score={_pattern_bucket(payload.get('signal_score'), step=0.5, limit=12.0)}, "
            f"volatility_score={_pattern_bucket(payload.get('volatility_score'), step=0.5, limit=12.0)}, "
            f"flow={_pattern_bucket(payload.get('flow_score'), step=0.25, limit=1.0)}, "
            f"atr={_pattern_bucket(payload.get('atr_pct'), step=0.5, limit=10.0)}, "
            f"iv_rank={_pattern_bucket(payload.get('iv_rank'), step=10.0, limit=100.0)}"
        )

    def _pattern_memory_explanation(memory: dict, current_direction: str, preferred_direction: str) -> str:
        numbers = str(memory.get("last_signal_numbers", "") or "same signal-number bucket")
        result = str(memory.get("last_result_text", "") or "loss not profit")
        old_choice = str(memory.get("last_choice", "") or current_direction or "").upper()
        better_choice = str(memory.get("last_better_choice", "") or preferred_direction or "").upper()
        current = str(current_direction or "").upper()
        preferred = str(preferred_direction or "").upper()
        return (
            f"Last time when the numbers were {numbers}, the result was {result}, "
            f"and I picked {old_choice} instead of {better_choice} to get that result; "
            f"so now this time I need to pick {preferred} instead of {current}."
        )

    def _trade_row_pnl_usd(trade_row: dict) -> float | None:
        for key in (
            "realized_pnl_usd",
            "pnl_usd",
            "paper_reported_pnl_usd",
            "conservative_executable_pnl_usd",
        ):
            raw = trade_row.get(key)
            if raw is None or str(raw).strip() == "":
                continue
            text = str(raw).replace("$", "").replace(",", "").strip()
            return _safe_signal_float(text, 0.0)

        entry = _safe_signal_float(str(trade_row.get("entry_price", "")).replace("$", "").replace(",", ""), 0.0)
        exit_ = _safe_signal_float(str(trade_row.get("exit_price", "")).replace("$", "").replace(",", ""), 0.0)
        qty = max(1, int(_safe_signal_float(trade_row.get("qty"), 1.0)))
        if entry > 0 and exit_ > 0:
            return (exit_ - entry) * qty * 100.0

        for key in ("realized_pnl_pct", "pnl_pct", "paper_reported_pnl_pct", "conservative_executable_pnl_pct"):
            raw = trade_row.get(key)
            if raw is None or str(raw).strip() == "":
                continue
            pct = _safe_signal_float(str(raw).replace("%", "").replace(",", ""), 0.0)
            if pct < 0:
                return -0.01
            if pct > 0:
                return 0.01
            return 0.0
        return None

    def _record_signal_pattern_outcome(trade_row: dict, now_et: datetime) -> bool:
        if not bool(getattr(config, "ENABLE_SIGNAL_PATTERN_MEMORY", True)):
            return False
        ticker = str(trade_row.get("ticker", "") or trade_row.get("symbol", "") or "").upper()
        direction = str(trade_row.get("direction", "") or "").lower()
        if (not ticker or direction not in {"call", "put"}) and trade_row.get("option_symbol"):
            parsed_ticker, parsed_direction = _parse_option_symbol(str(trade_row.get("option_symbol", "")))
            ticker = ticker or parsed_ticker.upper()
            direction = direction if direction in {"call", "put"} else parsed_direction
        if direction not in {"call", "put"}:
            return False
        pnl = _trade_row_pnl_usd(trade_row)
        if pnl is None:
            return False
        signature, ticker_key, global_key = _signal_pattern_keys(trade_row, ticker, now_et=now_et)
        keys = [key for key in (ticker_key, global_key) if key]
        for key in keys:
            item = signal_pattern_memory.get(key)
            if not isinstance(item, dict):
                item = {
                    "signature": signature,
                    "ticker": ticker if key.startswith("ticker:") else "*",
                    "wins_by_direction": {},
                    "losses_by_direction": {},
                    "samples": 0,
                }
            wins = item.get("wins_by_direction")
            if not isinstance(wins, dict):
                wins = {}
            losses = item.get("losses_by_direction")
            if not isinstance(losses, dict):
                losses = {}
            item["samples"] = int(item.get("samples", 0) or 0) + 1
            if pnl < 0:
                losses[direction] = int(losses.get(direction, 0) or 0) + 1
                preferred = _opposite_direction(direction)
                item["avoid_direction"] = direction
                item["preferred_direction"] = preferred
                item["last_outcome"] = "loss"
                item["last_pnl_usd"] = round(pnl, 2)
                item["last_result_text"] = f"loss ${abs(pnl):.2f} not profit"
                item["last_choice"] = direction.upper()
                item["last_better_choice"] = preferred.upper() if preferred else ""
                item["last_signal_numbers"] = _signal_number_summary(trade_row)
            else:
                wins[direction] = int(wins.get(direction, 0) or 0) + 1
                item["last_outcome"] = "win"
                item["last_pnl_usd"] = round(pnl, 2)
                item["last_result_text"] = f"profit ${pnl:.2f} not loss"
                item["last_choice"] = direction.upper()
                item["last_better_choice"] = direction.upper()
                item["last_signal_numbers"] = _signal_number_summary(trade_row)
                current_preferred = str(item.get("preferred_direction", "") or "").lower()
                if current_preferred not in {"call", "put"}:
                    item["preferred_direction"] = direction
            item["wins_by_direction"] = wins
            item["losses_by_direction"] = losses
            item["updated_at_et"] = now_et.isoformat()
            signal_pattern_memory[key] = item

        if len(signal_pattern_memory) > 1200:
            stale = sorted(
                signal_pattern_memory.items(),
                key=lambda pair: str((pair[1] or {}).get("updated_at_et", "")),
            )
            for key, _item in stale[: len(signal_pattern_memory) - 1000]:
                signal_pattern_memory.pop(key, None)
        return True

    def _pattern_direction_override(signal: dict, ticker: str, current_direction: str, now_et: datetime) -> tuple[str, str, str]:
        if not bool(getattr(config, "ENABLE_SIGNAL_PATTERN_MEMORY", True)):
            return current_direction, "", ""
        direction = str(current_direction or "").lower()
        if direction not in {"call", "put"}:
            return current_direction, "", ""
        _signature, ticker_key, global_key = _signal_pattern_keys(signal, ticker, now_et=now_et)
        keys = [ticker_key]
        if bool(getattr(config, "SIGNAL_PATTERN_MEMORY_USE_GLOBAL", True)):
            keys.append(global_key)
        min_losses = max(1, int(getattr(config, "SIGNAL_PATTERN_MEMORY_MIN_LOSSES", 1) or 1))
        for key in [item for item in keys if item]:
            memory = signal_pattern_memory.get(key)
            if not isinstance(memory, dict):
                continue
            preferred = str(memory.get("preferred_direction", "") or "").lower()
            avoid = str(memory.get("avoid_direction", "") or "").lower()
            losses = memory.get("losses_by_direction")
            wins = memory.get("wins_by_direction")
            if not isinstance(losses, dict):
                losses = {}
            if not isinstance(wins, dict):
                wins = {}
            current_losses = int(losses.get(direction, 0) or 0)
            current_wins = int(wins.get(direction, 0) or 0)
            if (
                preferred in {"call", "put"}
                and preferred != direction
                and avoid == direction
                and current_losses >= min_losses
                and current_losses > current_wins
            ):
                return preferred, key, _pattern_memory_explanation(memory, direction, preferred)
        return direction, "", ""

    def _pre_execution_history_check(signal: dict, ticker: str, direction: str, now_et: datetime) -> tuple[bool, str]:
        if not bool(getattr(config, "ENABLE_PRE_EXECUTION_HISTORY_CHECK", True)):
            return True, ""
        if not bool(getattr(config, "ENABLE_SIGNAL_PATTERN_MEMORY", True)):
            return True, ""
        normalized_direction = str(direction or "").lower()
        if normalized_direction not in {"call", "put"}:
            return False, f"history check blocked invalid direction={direction!r}"

        recalculated_direction, _recheck_key, recalculated_reason = _pattern_direction_override(
            signal,
            ticker,
            normalized_direction,
            now_et,
        )
        if str(recalculated_direction or "").lower() != normalized_direction:
            return (
                False,
                recalculated_reason
                or (
                    f"history check blocked {normalized_direction.upper()} before order; "
                    f"memory now prefers {str(recalculated_direction).upper()} for this setup."
                ),
            )

        _signature, ticker_key, global_key = _signal_pattern_keys(signal, ticker, now_et=now_et)
        keys = [ticker_key]
        if bool(getattr(config, "SIGNAL_PATTERN_MEMORY_USE_GLOBAL", True)):
            keys.append(global_key)
        min_losses = max(1, int(getattr(config, "SIGNAL_PATTERN_MEMORY_MIN_LOSSES", 1) or 1))

        reviewed = False
        pass_detail = ""
        opposite = _opposite_direction(normalized_direction)
        for key in [item for item in keys if item]:
            memory = signal_pattern_memory.get(key)
            if not isinstance(memory, dict):
                continue
            reviewed = True
            losses = memory.get("losses_by_direction")
            wins = memory.get("wins_by_direction")
            if not isinstance(losses, dict):
                losses = {}
            if not isinstance(wins, dict):
                wins = {}
            current_losses = int(losses.get(normalized_direction, 0) or 0)
            current_wins = int(wins.get(normalized_direction, 0) or 0)
            opposite_losses = int(losses.get(opposite, 0) or 0) if opposite else 0
            opposite_wins = int(wins.get(opposite, 0) or 0) if opposite else 0
            avoid = str(memory.get("avoid_direction", "") or "").lower()
            preferred = str(memory.get("preferred_direction", "") or "").lower()

            if current_losses >= min_losses and current_losses > current_wins:
                better_direction = preferred if preferred in {"call", "put"} and preferred != normalized_direction else opposite
                explanation = _pattern_memory_explanation(memory, normalized_direction, better_direction)
                return (
                    False,
                    (
                        f"history check blocked {normalized_direction.upper()} before order: "
                        f"this number bucket has {current_losses} loss(es) vs {current_wins} win(s) "
                        f"for {normalized_direction.upper()}. {explanation}"
                    ),
                )

            if avoid == normalized_direction and preferred in {"call", "put"} and preferred != normalized_direction:
                if current_losses >= min_losses:
                    explanation = _pattern_memory_explanation(memory, normalized_direction, preferred)
                    return (
                        False,
                        (
                            f"history check blocked {normalized_direction.upper()} before order: "
                            f"memory says avoid {normalized_direction.upper()} and prefer {preferred.upper()}. "
                            f"{explanation}"
                        ),
                    )

            if not pass_detail:
                pass_detail = (
                    f"history check passed before order: {normalized_direction.upper()} "
                    f"{current_wins}W/{current_losses}L"
                )
                if opposite:
                    pass_detail += f", {opposite.upper()} {opposite_wins}W/{opposite_losses}L"

        return True, pass_detail if reviewed else ""

    def _parse_trade_history_time(row: dict, fallback: datetime) -> datetime:
        for key in ("exit_time", "timestamp", "entry_time", "entry_time_iso"):
            raw = str(row.get(key, "") or "").strip()
            if not raw:
                continue
            parsed = _parse_state_datetime(raw)
            if parsed is not None:
                return parsed

            suffix_zone = None
            text = raw
            upper = text.upper()
            if upper.endswith((" EDT", " EST")):
                suffix_zone = pytz.timezone(config.EASTERN_TZ)
                text = text[:-4]
            elif upper.endswith((" CDT", " CST")):
                suffix_zone = pytz.timezone(config.CENTRAL_TZ)
                text = text[:-4]

            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%m/%d/%Y %H:%M:%S"):
                try:
                    parsed_naive = datetime.strptime(text, fmt)
                    localized = (suffix_zone or tz).localize(parsed_naive)
                    return localized.astimezone(tz)
                except Exception:
                    continue
        return fallback

    def _seed_signal_pattern_memory_from_trade_history(now_et: datetime) -> None:
        if not bool(getattr(config, "ENABLE_SIGNAL_PATTERN_MEMORY", True)):
            return
        path = config.TRADES_CSV_PATH
        if not path.exists():
            print(f"[{ts(now_et)}] Signal pattern memory: no trade history found at {path}.")
            return
        try:
            with path.open("r", newline="", encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
        except Exception as exc:  # noqa: BLE001
            print(f"[{ts(now_et)}] Signal pattern memory: trade history read failed: {exc}")
            return

        history_limit = max(0, int(getattr(config, "SIGNAL_PATTERN_MEMORY_HISTORY_ROWS", 1000) or 1000))
        if history_limit > 0:
            rows = rows[-history_limit:]

        state_memory = {
            str(key): value
            for key, value in signal_pattern_memory.items()
            if isinstance(value, dict)
        }
        signal_pattern_memory.clear()
        learned = 0
        skipped = 0
        for row in rows:
            if not isinstance(row, dict):
                skipped += 1
                continue
            trade_time = _parse_trade_history_time(row, now_et)
            if _record_signal_pattern_outcome(row, trade_time):
                learned += 1
            else:
                skipped += 1

        state_only = 0
        for key, item in state_memory.items():
            if key not in signal_pattern_memory:
                signal_pattern_memory[key] = item
                state_only += 1

        if learned > 0 or state_only > 0:
            _save_runtime_state()
        print(
            f"[{ts(now_et)}] Signal pattern memory loaded from trade history: "
            f"rows={len(rows)} learned={learned} skipped={skipped} "
            f"patterns={len(signal_pattern_memory)} state_only={state_only}."
        )

    def _blank_loss_profile(day_key: str) -> dict:
        return {
            "day": day_key,
            "source": "",
            "source_closed_count": 0,
            "loss_count": 0,
            "dominant_cause": "",
            "causes": {},
            "ticker_losses": {},
            "min_signal_score": float(getattr(config, "ADAPTIVE_LOSS_MIN_SIGNAL_SCORE", 7.8) or 7.8),
            "min_direction_score": float(getattr(config, "ADAPTIVE_LOSS_MIN_DIRECTION_SCORE", 0.65) or 0.65),
            "max_spread_pct": float(getattr(config, "ADAPTIVE_LOSS_MAX_SPREAD_PCT", 4.0) or 4.0),
            "min_abs_roc_pct": 0.0,
            "min_rvol": 0.0,
            "diagnoses": [],
            "last_diagnosis": {},
            "updated_at_et": "",
        }

    def _underlying_move_between(ticker: str, entry_iso: str, exit_iso: str) -> float | None:
        symbol = str(ticker or "").upper()
        entry_dt = _parse_state_datetime(str(entry_iso or ""))
        exit_dt = _parse_state_datetime(str(exit_iso or ""))
        if not symbol or entry_dt is None or exit_dt is None or exit_dt <= entry_dt:
            return None
        try:
            bars = data_client.get_intraday_bars_window(
                symbol,
                entry_dt - timedelta(minutes=2),
                exit_dt + timedelta(minutes=2),
                limit=80,
            )
            if bars is None or bars.empty or len(bars) < 2:
                return None
            entry_price = float(bars["close"].iloc[0])
            exit_price = float(bars["close"].iloc[-1])
            if entry_price <= 0:
                return None
            return ((exit_price - entry_price) / entry_price) * 100.0
        except Exception as exc:  # noqa: BLE001
            print(f"[{ts()}] {symbol}: loss diagnosis underlying move unavailable: {exc}")
            return None

    def _diagnose_loss_trade(trade: dict, now_et: datetime, *, source: str) -> dict:
        symbol = str(trade.get("option_symbol") or trade.get("symbol") or "").upper()
        ticker = str(trade.get("ticker") or "").upper()
        direction = str(trade.get("direction") or "").lower()
        if (not ticker or not direction) and symbol:
            parsed_ticker, parsed_direction = _parse_option_symbol(symbol)
            ticker = ticker or parsed_ticker.upper()
            direction = direction or parsed_direction
        hold_seconds = int(_safe_signal_float(trade.get("hold_seconds"), 0.0))
        pnl_usd = _safe_signal_float(trade.get("realized_pnl_usd"), 0.0)
        pnl_pct = _safe_signal_float(trade.get("realized_pnl_pct", trade.get("pnl_pct")), 0.0)
        if abs(pnl_pct) <= 1.0 and "pnl_pct" in trade:
            pnl_pct *= 100.0
        entry_spread = _safe_signal_float(
            trade.get("entry_spread_pct", trade.get("contract_spread_pct")),
            0.0,
        )
        exit_spread = _safe_signal_float(trade.get("exit_spread_pct"), 0.0)
        entry_slip = _safe_signal_float(trade.get("entry_fill_slippage_vs_ask_pct"), 0.0)
        exit_slip = _safe_signal_float(trade.get("exit_fill_slippage_vs_bid_pct"), 0.0)
        max_spread = max(entry_spread, exit_spread)
        max_slippage = max(entry_slip, exit_slip)
        underlying_move_pct = _underlying_move_between(
            ticker,
            str(trade.get("entry_time") or trade.get("entry_time_iso") or ""),
            str(trade.get("exit_time") or ""),
        )

        wrong_way_threshold = float(
            getattr(config, "ADAPTIVE_LOSS_WRONG_WAY_UNDERLYING_MOVE_PCT", 0.05) or 0.05
        )
        spread_threshold = float(getattr(config, "ADAPTIVE_LOSS_SPREAD_CAUSE_PCT", 4.0) or 4.0)
        slip_threshold = float(getattr(config, "ADAPTIVE_LOSS_SLIPPAGE_CAUSE_PCT", 2.0) or 2.0)
        quick_seconds = int(getattr(config, "ADAPTIVE_LOSS_QUICK_SECONDS", 180) or 180)
        wrong_way = False
        if underlying_move_pct is not None:
            if direction == "call" and underlying_move_pct <= -wrong_way_threshold:
                wrong_way = True
            elif direction == "put" and underlying_move_pct >= wrong_way_threshold:
                wrong_way = True

        if wrong_way:
            cause = "wrong_direction"
            action = "raise direction conviction and block ticker"
        elif max_spread >= spread_threshold or max_slippage >= slip_threshold:
            cause = "execution_slippage"
            action = "tighten spread cap and cool ticker"
        elif hold_seconds > 0 and hold_seconds <= quick_seconds:
            cause = "rapid_stopout"
            action = "require stronger momentum before next entry"
        elif underlying_move_pct is not None and abs(underlying_move_pct) < wrong_way_threshold:
            cause = "chop_no_followthrough"
            action = "require stronger ROC/RVOL before next entry"
        else:
            cause = "timing_or_decay"
            action = "raise signal quality and cool ticker"

        return {
            "source": source,
            "timestamp_et": now_et.isoformat(),
            "ticker": ticker,
            "symbol": symbol,
            "direction": direction,
            "cause": cause,
            "action": action,
            "hold_seconds": hold_seconds,
            "realized_pnl_usd": round(pnl_usd, 2),
            "realized_pnl_pct": round(pnl_pct, 4),
            "underlying_move_pct": round(float(underlying_move_pct), 4) if underlying_move_pct is not None else None,
            "entry_spread_pct": round(entry_spread, 4),
            "exit_spread_pct": round(exit_spread, 4),
            "max_slippage_pct": round(max_slippage, 4),
            "signal_score": _safe_signal_float(trade.get("signal_score"), 0.0),
            "direction_score": _safe_signal_float(trade.get("direction_score"), 0.0),
        }

    def _build_loss_profile(diagnoses: list[dict], now_et: datetime, *, source: str, closed_count: int) -> dict:
        day_key = now_et.date().isoformat()
        profile = _blank_loss_profile(day_key)
        causes: dict[str, int] = {}
        ticker_losses: dict[str, int] = {}
        for diag in diagnoses:
            cause = str(diag.get("cause", "unknown") or "unknown")
            ticker = str(diag.get("ticker", "") or "").upper()
            causes[cause] = int(causes.get(cause, 0)) + 1
            if ticker:
                ticker_losses[ticker] = int(ticker_losses.get(ticker, 0)) + 1

        loss_count = len(diagnoses)
        wrong_count = int(causes.get("wrong_direction", 0))
        execution_count = int(causes.get("execution_slippage", 0))
        momentum_loss_count = int(causes.get("rapid_stopout", 0)) + int(causes.get("chop_no_followthrough", 0))
        min_signal = float(getattr(config, "ADAPTIVE_LOSS_MIN_SIGNAL_SCORE", 7.8))
        min_signal += loss_count * float(getattr(config, "ADAPTIVE_LOSS_SIGNAL_SCORE_ADD_PER_LOSS", 0.15))
        min_signal = min(float(getattr(config, "ADAPTIVE_LOSS_MAX_SIGNAL_SCORE", 9.2)), min_signal)
        min_direction = float(getattr(config, "ADAPTIVE_LOSS_MIN_DIRECTION_SCORE", 0.65))
        min_direction += wrong_count * float(getattr(config, "ADAPTIVE_LOSS_DIRECTION_ADD_PER_WRONG", 0.05))
        min_direction = min(
            float(getattr(config, "ADAPTIVE_LOSS_MAX_DIRECTION_SCORE", 0.85)),
            min_direction,
        )
        max_spread = float(getattr(config, "ADAPTIVE_LOSS_MAX_SPREAD_PCT", 4.0))
        if execution_count > 0:
            max_spread = min(
                max_spread,
                float(getattr(config, "ADAPTIVE_LOSS_EXECUTION_MAX_SPREAD_PCT", 3.0)),
            )
        momentum_trigger = int(getattr(config, "ADAPTIVE_LOSS_REQUIRE_MOMENTUM_AFTER_LOSSES", 2))
        min_abs_roc = 0.0
        min_rvol = 0.0
        if momentum_loss_count >= momentum_trigger:
            min_abs_roc = float(getattr(config, "ADAPTIVE_LOSS_MIN_ABS_ROC_PCT", 0.12))
            min_rvol = float(getattr(config, "ADAPTIVE_LOSS_MIN_RVOL", 0.50))

        dominant_cause = ""
        if causes:
            dominant_cause = sorted(causes.items(), key=lambda item: (-int(item[1]), str(item[0])))[0][0]
        profile.update(
            {
                "source": source,
                "source_closed_count": int(closed_count),
                "loss_count": loss_count,
                "dominant_cause": dominant_cause,
                "causes": causes,
                "ticker_losses": ticker_losses,
                "min_signal_score": round(min_signal, 4),
                "min_direction_score": round(min_direction, 4),
                "max_spread_pct": round(max_spread, 4),
                "min_abs_roc_pct": round(min_abs_roc, 4),
                "min_rvol": round(min_rvol, 4),
                "diagnoses": diagnoses[-5:],
                "last_diagnosis": diagnoses[-1] if diagnoses else {},
                "updated_at_et": now_et.isoformat(),
            }
        )
        return profile

    def _activate_loss_profile(profile: dict, now_et: datetime) -> None:
        nonlocal adaptive_loss_active, adaptive_loss_profile
        adaptive_loss_profile = profile
        if int(profile.get("loss_count", 0) or 0) <= 0:
            return
        adaptive_loss_active = True
        block_after = max(1, int(getattr(config, "ADAPTIVE_LOSS_BLOCK_TICKER_AFTER_LOSSES", 1) or 1))
        if bool(getattr(config, "ADAPTIVE_BLOCK_LOSING_TICKERS", True)):
            for ticker, count in dict(profile.get("ticker_losses") or {}).items():
                if int(count or 0) >= block_after:
                    adaptive_loss_blocked_tickers.add(str(ticker).upper())
        last = profile.get("last_diagnosis") if isinstance(profile.get("last_diagnosis"), dict) else {}
        if last:
            print(
                f"[{ts(now_et)}] Adaptive diagnosis: {last.get('ticker', '')} "
                f"cause={last.get('cause', 'unknown')} action={last.get('action', '')}. "
                f"Profile gates score>={float(profile.get('min_signal_score', 0) or 0):.2f} "
                f"dir>={float(profile.get('min_direction_score', 0) or 0):.2f} "
                f"spread<={float(profile.get('max_spread_pct', 0) or 0):.2f}%."
            )

    def _refresh_adaptive_loss_profile_from_truth(truth_snapshot: dict, now_et: datetime) -> bool:
        closed_count = int(truth_snapshot.get("closed_count", 0) or 0)
        if (
            isinstance(adaptive_loss_profile, dict)
            and str(adaptive_loss_profile.get("source", "")) == "alpaca_truth"
            and int(adaptive_loss_profile.get("source_closed_count", -1) or -1) == closed_count
        ):
            return False
        losses = [
            item
            for item in list(truth_snapshot.get("closed_trades") or [])
            if _safe_signal_float(item.get("realized_pnl_usd"), 0.0) < 0
        ]
        if not losses:
            return False
        window = max(1, int(getattr(config, "ADAPTIVE_LOSS_CAUSE_WINDOW", 12) or 12))
        diagnoses = [
            _diagnose_loss_trade(item, now_et, source="alpaca_truth")
            for item in losses[-window:]
        ]
        profile = _build_loss_profile(diagnoses, now_et, source="alpaca_truth", closed_count=closed_count)
        _activate_loss_profile(profile, now_et)
        return True

    def _arm_loss_direction_flips_from_truth(truth_snapshot: dict, now_et: datetime) -> bool:
        changed = False
        if not bool(getattr(config, "ENABLE_LOSS_DIRECTION_FLIP", True)):
            return changed
        for item in list(truth_snapshot.get("closed_trades") or []):
            if _safe_signal_float(item.get("realized_pnl_usd"), 0.0) >= 0:
                continue
            ticker = str(item.get("ticker", "") or "").upper()
            direction = str(item.get("direction", "") or "").lower()
            expected = _opposite_direction(direction)
            if not ticker or expected not in {"call", "put"}:
                continue
            if (
                bool(ticker_reentry_armed.get(ticker, False))
                and str(ticker_reentry_expected_direction.get(ticker, "") or "").lower() == expected
            ):
                continue
            _arm_loss_direction_flip(ticker, direction, now_et, reason="alpaca_truth_loss")
            changed = True
        return changed

    def _record_local_loss_diagnosis(trade_row: dict, now_et: datetime) -> dict:
        window = max(1, int(getattr(config, "ADAPTIVE_LOSS_CAUSE_WINDOW", 12) or 12))
        existing = []
        if isinstance(adaptive_loss_profile, dict):
            existing = [
                item for item in list(adaptive_loss_profile.get("diagnoses") or [])
                if isinstance(item, dict)
            ]
        diagnosis = _diagnose_loss_trade(trade_row, now_et, source="local_close")
        profile = _build_loss_profile(
            (existing + [diagnosis])[-window:],
            now_et,
            source="local_close",
            closed_count=int(broker_truth_closed_count or 0),
        )
        _activate_loss_profile(profile, now_et)
        return diagnosis

    def _cooldown_minutes_for_loss_cause(cause: str, fallback_minutes: int) -> int:
        normalized = str(cause or "").lower()
        if normalized == "wrong_direction":
            return max(
                int(fallback_minutes),
                int(getattr(config, "ADAPTIVE_LOSS_WRONG_DIRECTION_COOLDOWN_MINUTES", 90) or 90),
            )
        if normalized == "execution_slippage":
            return max(
                int(fallback_minutes),
                int(getattr(config, "ADAPTIVE_LOSS_EXECUTION_COOLDOWN_MINUTES", 60) or 60),
            )
        if normalized == "rapid_stopout":
            return max(
                int(fallback_minutes),
                int(getattr(config, "ADAPTIVE_LOSS_QUICK_COOLDOWN_MINUTES", 45) or 45),
            )
        if normalized == "chop_no_followthrough":
            return max(
                int(fallback_minutes),
                int(getattr(config, "ADAPTIVE_LOSS_CHOP_COOLDOWN_MINUTES", 30) or 30),
            )
        return int(fallback_minutes)

    def _safe_get_clock(*, phase: str, now_et: datetime, now_ct: datetime):
        nonlocal last_alpaca_auth_error_et, last_alpaca_auth_error
        try:
            return broker.get_clock()
        except Exception as exc:  # noqa: BLE001
            retry_sleep = max(5, int(config.LOOP_INTERVAL_SECONDS))
            if _looks_like_auth_error(exc):
                last_alpaca_auth_error_et = now_et.isoformat()
                last_alpaca_auth_error = str(exc)[:300]
                print(
                    f"[{ts(now_et)} | {ts_ct(now_ct)}] Alpaca auth error during {phase} clock lookup: {exc}. "
                    f"Retrying in {retry_sleep}s."
                )
                alerts.send(
                    "alpaca_auth_error",
                    f"Alpaca auth error during {phase} clock lookup. Retrying in {retry_sleep}s.",
                    level="error",
                    dedupe_key=f"alpaca-auth-{int(time.time() // 60)}",
                )
                _save_runtime_state()
            else:
                print(
                    f"[{ts(now_et)} | {ts_ct(now_ct)}] Clock lookup failed during {phase}: {exc}. "
                    f"Retrying in {retry_sleep}s."
                )
            time.sleep(retry_sleep)
            return None

    def _has_ticker_open_meta(ticker: str) -> bool:
        want = str(ticker or "").upper()
        if not want:
            return False
        for meta in open_trade_meta.values():
            if str(meta.get("ticker", "") or "").upper() == want:
                return True
        return False

    def _attempt_reversal_entry(
        *,
        ticker: str,
        direction: str,
        now_et: datetime,
        reentries_used: int,
    ) -> bool:
        if dry_run_enabled:
            print(f"[{ts(now_et)}] DRY-RUN reversal candidate: {ticker} {direction.upper()} (no order submitted).")
            return False
        if not _is_valid_long_direction(direction):
            return False
        if not is_at_or_after(now_et, config.NO_NEW_TRADES_BEFORE):
            print(f"[{ts(now_et)}] {ticker}: reversal skipped (before entry window).")
            return False
        if is_at_or_after(now_et, config.NO_NEW_TRADES_AFTER):
            print(f"[{ts(now_et)}] {ticker}: reversal skipped (after entry window).")
            return False
        if _is_entry_hour_blocked(now_et, strategy_profile=strategy_profile):
            print(f"[{ts(now_et)}] {ticker}: reversal skipped (hour {now_et.hour:02d}:00 ET blocked by config).")
            return False
        if reentries_used >= int(config.MAX_REENTRIES_PER_TICKER):
            print(
                f"[{ts(now_et)}] {ticker}: reversal skipped "
                f"(max re-entries used {reentries_used}/{int(config.MAX_REENTRIES_PER_TICKER)})."
            )
            return False

        option_positions_now = broker.get_open_option_positions()
        if not can_open_new_positions(len(option_positions_now), config.MAX_POSITIONS):
            print(f"[{ts(now_et)}] {ticker}: reversal skipped (max positions reached).")
            return False
        if _has_ticker_open_meta(ticker):
            print(f"[{ts(now_et)}] {ticker}: reversal skipped (ticker already open in runtime state).")
            return False

        try:
            stock_price = data_client.get_latest_stock_price(ticker)
            if stock_price is None:
                print(f"[{ts(now_et)}] {ticker}: reversal skipped (no stock quote).")
                return False

            contract, contract_reason = select_atm_option_contract_with_reason(
                data_client=data_client,
                underlying_symbol=ticker,
                direction=direction,
                underlying_price=stock_price,
                now_et=now_et,
            )
            if not contract:
                print(f"[{ts(now_et)}] {ticker}: reversal skipped (no eligible contract: {contract_reason}).")
                return False

            option_symbol = str(contract.get("symbol", "") or "")
            if not option_symbol:
                print(f"[{ts(now_et)}] {ticker}: reversal skipped (contract missing symbol).")
                return False
            if not _option_symbol_matches_direction(option_symbol, direction):
                print(
                    f"[{ts(now_et)}] {ticker}: reversal skipped (symbol direction mismatch "
                    f"{option_symbol} vs {direction.upper()})."
                )
                return False

            entry_quote = _option_quote_snapshot(data_client, option_symbol)
            spread_ok, spread_reason = _entry_quote_spread_gate(
                option_symbol=option_symbol,
                entry_quote=entry_quote,
                now_et=now_et,
                strategy_profile=strategy_profile,
            )
            if not spread_ok:
                print(f"[{ts(now_et)}] {ticker}: reversal skipped ({spread_reason}).")
                return False
            ask_price = float(entry_quote.get("ask") or 0.0)

            initial_chain_ask = float(contract.get("ask_price", ask_price) or ask_price)
            pre_submit_slippage = _slippage_pct(initial_chain_ask, ask_price)
            if pre_submit_slippage > config.MAX_ENTRY_SLIPPAGE_PCT:
                retry_quote = _option_quote_snapshot(data_client, option_symbol)
                retry_ok, retry_reason = _entry_quote_spread_gate(
                    option_symbol=option_symbol,
                    entry_quote=retry_quote,
                    now_et=now_et,
                    strategy_profile=strategy_profile,
                )
                retry_ask = float(retry_quote.get("ask") or 0.0)
                if retry_ok and retry_ask > 0:
                    entry_quote = retry_quote
                    ask_price = retry_ask
                    pre_submit_slippage = _slippage_pct(initial_chain_ask, ask_price)
                elif not retry_ok:
                    print(f"[{ts(now_et)}] {ticker}: reversal skipped ({retry_reason}).")
                    return False
            if pre_submit_slippage > (config.MAX_ENTRY_SLIPPAGE_PCT * 3):
                print(
                    f"[{ts(now_et)}] {ticker}: reversal skipped (entry slippage {pre_submit_slippage:.2f}% > "
                    f"hard cap {(config.MAX_ENTRY_SLIPPAGE_PCT * 3):.2f}%)."
                )
                return False

            reversal_signal = {
                "symbol": ticker,
                "ticker": ticker,
                "direction": direction,
                "strategy_profile": "reversal_snapback",
                "entry_time_iso": now_et.isoformat(),
                "price": stock_price,
                "signal_score": 0.0,
                "direction_score": 0.0,
                "rvol": 0.0,
                "rsi": 0.0,
                "roc": 0.0,
                "iv_rank": 0.0,
                "flow_score": 0.0,
                "atr_pct": 0.0,
                "volatility_score": 0.0,
            }
            history_ok, history_reason = _pre_execution_history_check(
                reversal_signal,
                ticker,
                direction,
                now_et,
            )
            if not history_ok:
                print(f"[{ts(now_et)}] {ticker}: reversal skipped ({history_reason}).")
                return False
            if history_reason:
                print(f"[{ts(now_et)}] {ticker}: reversal {history_reason}.")

            qty = 1
            entry_result = _execute_limit_entry(
                broker=broker,
                data_client=data_client,
                option_symbol=option_symbol,
                qty=qty,
                now_et=now_et,
                label=f"REVERSAL ENTRY {ticker}",
                initial_quote=entry_quote,
            )
            if not bool(entry_result.get("filled", False)):
                print(
                    f"[{ts(now_et)}] {ticker}: reversal not filled "
                    f"(status={entry_result.get('status', 'unknown')})."
                )
                return False

            filled_avg_price = float(entry_result.get("filled_price") or 0.0)
            filled_qty = position_qty_as_int(entry_result.get("filled_qty", qty)) or qty
            if filled_qty > 1:
                extra_qty = filled_qty - 1
                try:
                    broker.close_option_market(option_symbol, extra_qty)
                    print(f"[{ts(now_et)}] {ticker}: trimmed reversal fill to 1 contract (closed extra {extra_qty}).")
                except Exception as exc:  # noqa: BLE001
                    print(f"[{ts(now_et)}] {ticker}: failed to trim reversal extra qty {extra_qty}: {exc}")
                filled_qty = 1
            fill_slippage = float(entry_result.get("fill_slippage_vs_ask_pct", 0.0) or 0.0)
            if fill_slippage > config.MAX_FILL_SLIPPAGE_PCT:
                print(
                    f"[{ts(now_et)}] {ticker}: reversal fill slippage {fill_slippage:.2f}% exceeds "
                    f"{config.MAX_FILL_SLIPPAGE_PCT:.2f}%. Closing immediately."
                )
                try:
                    broker.close_option_market(option_symbol, filled_qty)
                except Exception as exc:  # noqa: BLE001
                    print(f"[{ts(now_et)}] {ticker}: reversal slippage close failed: {exc}")
                return False

            prior_entries = int(ticker_entry_counts.get(ticker, 0))
            pattern_signature, pattern_key, pattern_global_key = _signal_pattern_keys(reversal_signal, ticker, now_et=now_et)
            open_trade_meta[option_symbol] = {
                "timestamp": ts(now_et),
                "entry_time_iso": now_et.isoformat(),
                "strategy_profile": "reversal_snapback",
                "ticker": ticker,
                "direction": direction,
                "option_symbol": option_symbol,
                "strike": contract.get("strike_price", ""),
                "expiry": contract.get("expiration_date", ""),
                "qty": filled_qty,
                "entry_price": filled_avg_price or ask_price,
                "signal_score": 0.0,
                "direction_score": 0.0,
                "rvol": 0.0,
                "rsi": 0.0,
                "roc": 0.0,
                "iv_rank": 0.0,
                "price": round(float(stock_price or 0.0), 4),
                "vwap": 0.0,
                "flow_score": 0.0,
                "regime_score": 0.0,
                "contract_spread_pct": round(float(entry_result.get("submit_spread_pct", 0.0) or 0.0), 4),
                "atr_pct": 0.0,
                "volatility_score": 0.0,
                "signal_pattern_signature": pattern_signature,
                "signal_pattern_key": pattern_key,
                "signal_pattern_global_key": pattern_global_key,
                "pattern_direction_override": "",
                "pattern_direction_override_reason": history_reason,
                "pre_execution_history_check": history_reason,
                "entry_bid_submit": entry_result.get("submit_bid"),
                "entry_ask_submit": entry_result.get("submit_ask"),
                "entry_midpoint_submit": entry_result.get("submit_midpoint"),
                "entry_intended_limit": entry_result.get("intended_limit"),
                "entry_filled_price": filled_avg_price or ask_price,
                "entry_spread_pct": entry_result.get("submit_spread_pct"),
                "entry_fill_slippage_vs_ask_pct": entry_result.get("fill_slippage_vs_ask_pct"),
                "entry_fill_seconds": entry_result.get("fill_seconds"),
                "entry_attempts": entry_result.get("attempts", 0),
                "stop_floor_plpc": -float(config.STOP_LOSS_PCT),
                "stop_loss_usd": _runtime_stop_loss_usd(),
                "immediate_take_profit_pct": float(getattr(config, "IMMEDIATE_TAKE_PROFIT_PCT", 1.0) or 1.0),
                "max_hold_minutes": int(getattr(config, "MAX_HOLD_MINUTES", 90) or 90),
                "trade_state": "unproven",
                "runner_mode": False,
                "max_plpc": 0.0,
                "min_plpc": 0.0,
            }
            ticker_entry_counts[ticker] = prior_entries + 1
            ticker_reentries_used[ticker] = reentries_used + 1
            ticker_reentry_armed[ticker] = False
            ticker_reentry_expected_direction[ticker] = ""
            entry_times_rolling.append(now_et)
            _save_runtime_state()
            alerts.send(
                "reversal_entry",
                f"Reversal entry filled: {ticker} {direction.upper()} {option_symbol}",
                dedupe_key=f"reversal-{ticker}-{now_et.strftime('%Y%m%d%H%M')}",
            )
            return True
        except Exception as exc:  # noqa: BLE001
            print(f"[{ts(now_et)}] {ticker}: reversal entry error ({type(exc).__name__}): {exc!r}")
            return False

    def _close_position_with_confirmation(
        *,
        symbol: str,
        qty: int,
        now_et: datetime,
        label: str,
        exit_reason: str | None = None,
        poll_seconds_override: int | None = None,
        max_wait_seconds_override: int | None = None,
        retry_attempts_override: int | None = None,
    ) -> tuple[int, float | None, dict[str, object]]:
        request_qty = max(0, int(qty))
        if request_qty <= 0:
            return 0, None, {}

        if poll_seconds_override is None:
            poll_seconds = max(1, int(config.EXIT_ORDER_STATUS_POLL_SECONDS))
        else:
            poll_seconds = max(1, int(poll_seconds_override))
        if max_wait_seconds_override is None:
            max_wait_seconds = max(poll_seconds, int(config.EXIT_ORDER_MAX_WAIT_SECONDS))
        else:
            max_wait_seconds = max(poll_seconds, int(max_wait_seconds_override))

        critical_exit_reasons = {
            "stop_loss",
            "stop_loss_pct",
            "profit_target",
            "base_win_bank",
            "protected_floor_breach",
            "eod_close",
            "overnight_forced_close",
            "pre_expiry_exit",
            "pre_expiry_exit_overdue",
        }
        is_critical = str(exit_reason or "").lower() in critical_exit_reasons
        wait_seconds = max_wait_seconds
        if poll_seconds_override is None and max_wait_seconds_override is None:
            if is_critical:
                wait_seconds = max(poll_seconds, int(getattr(config, "SMART_EXIT_CRITICAL_WAIT_SECONDS", 3) or 3))
            else:
                wait_seconds = max(poll_seconds, int(getattr(config, "SMART_EXIT_NORMAL_WAIT_SECONDS", 6) or 6))

        execution_meta: dict[str, object] = {
            "attempts": 0,
            "submit_mode": "",
            "submit_bid": None,
            "submit_ask": None,
            "submit_midpoint": None,
            "submit_spread_pct": 0.0,
            "intended_limit": None,
            "fill_seconds": 0.0,
            "fill_slippage_vs_bid_pct": 0.0,
            "used_market_fallback": False,
            "status": "",
        }

        try:
            existing_sells = broker.get_open_orders_for_symbol(symbol=symbol, side="sell")
            for existing in existing_sells:
                existing_order_id = str(getattr(existing, "id", "") or "")
                if not existing_order_id:
                    continue
                try:
                    broker.cancel_order(existing_order_id)
                except Exception as exc:  # noqa: BLE001
                    print(
                        f"[{ts(now_et)}] {label} {symbol} qty={request_qty}: "
                        f"cancel existing close order {existing_order_id} failed: {exc}"
                    )

            quote_snapshot = _option_quote_snapshot(data_client, symbol)
            bid_price = float(quote_snapshot.get("bid") or 0.0)
            ask_price = float(quote_snapshot.get("ask") or 0.0)
            execution_meta["submit_bid"] = quote_snapshot.get("bid")
            execution_meta["submit_ask"] = quote_snapshot.get("ask")
            execution_meta["submit_midpoint"] = quote_snapshot.get("midpoint")
            execution_meta["submit_spread_pct"] = quote_snapshot.get("spread_pct")

            if bid_price > 0:
                spread = max(0.0, ask_price - bid_price) if ask_price > 0 else 0.0
                reprice_pct = float(
                    getattr(
                        config,
                        "SMART_EXIT_CRITICAL_REPRICE_PCT" if is_critical else "SMART_EXIT_NORMAL_REPRICE_PCT",
                        0.10 if is_critical else 0.35,
                    )
                    or (0.10 if is_critical else 0.35)
                )
                limit_price = round(bid_price + (spread * max(0.0, reprice_pct)), 2)
                execution_meta["intended_limit"] = limit_price
                execution_meta["submit_mode"] = "limit"
                execution_meta["attempts"] = 1
                submit_ts = time.time()
                order = broker.place_option_limit_sell(symbol, request_qty, limit_price)
                order_id = str(getattr(order, "id", "") or "")
                if not order_id:
                    print(f"[{ts(now_et)}] {label} {symbol} qty={request_qty}: limit close submitted without order id.")
                    return 0, None, execution_meta
                filled_qty, filled_avg_price, status, still_open = _await_order_fill(
                    broker,
                    order_id=order_id,
                    requested_qty=request_qty,
                    now_et=now_et,
                    label=f"{label} {symbol}",
                    poll_seconds=poll_seconds,
                    max_wait_seconds=wait_seconds,
                )
                execution_meta["fill_seconds"] = round(max(0.0, time.time() - submit_ts), 3)
                execution_meta["status"] = status
                if filled_qty > 0:
                    execution_meta["fill_slippage_vs_bid_pct"] = round(
                        _sell_fill_slippage_vs_bid_pct(bid_price, filled_avg_price),
                        4,
                    )
                    return filled_qty, filled_avg_price, execution_meta
                if still_open:
                    try:
                        broker.cancel_order(order_id)
                    except Exception as exc:  # noqa: BLE001
                        print(f"[{ts(now_et)}] {label} {symbol}: cancel close order {order_id} failed: {exc}")

            if not is_critical:
                print(f"[{ts(now_et)}] {label} {symbol} qty={request_qty}: executable limit exit not filled; will retry next loop.")
                return 0, None, execution_meta

            execution_meta["submit_mode"] = "market"
            execution_meta["attempts"] = int(execution_meta.get("attempts", 0) or 0) + 1
            execution_meta["used_market_fallback"] = True
            submit_ts = time.time()
            order = broker.close_option_market(symbol, request_qty)
            order_id = str(getattr(order, "id", "") or "")
            if not order_id:
                print(f"[{ts(now_et)}] {label} {symbol} qty={request_qty}: market close submitted without order id.")
                return 0, None, execution_meta
            filled_qty, filled_avg_price, status, _still_open = _await_order_fill(
                broker,
                order_id=order_id,
                requested_qty=request_qty,
                now_et=now_et,
                label=f"{label} {symbol} market",
                poll_seconds=poll_seconds,
                max_wait_seconds=max_wait_seconds,
            )
            execution_meta["fill_seconds"] = round(max(0.0, time.time() - submit_ts), 3)
            execution_meta["status"] = status
            if filled_qty > 0:
                execution_meta["fill_slippage_vs_bid_pct"] = round(
                    _sell_fill_slippage_vs_bid_pct(bid_price if bid_price > 0 else None, filled_avg_price),
                    4,
                )
                return filled_qty, filled_avg_price, execution_meta
            print(f"[{ts(now_et)}] {label} {symbol} qty={request_qty}: critical exit not filled.")
            return 0, None, execution_meta
        except Exception as exc:  # noqa: BLE001
            print(f"[{ts(now_et)}] {label} {symbol} qty={request_qty}: close error: {exc}")
            return 0, None, execution_meta

    def _force_normalize_ticker_exposure(option_positions: list, now_et: datetime) -> int:
        ticker_positions: dict[str, list[tuple[str, int]]] = {}
        total_filled = 0
        for pos in option_positions:
            symbol = str(getattr(pos, "symbol", "") or "")
            qty = position_qty_as_int(getattr(pos, "qty", 0))
            if not symbol or qty == 0:
                continue
            if qty < 0:
                cover_qty = abs(qty)
                try:
                    for existing_sell in broker.get_open_orders_for_symbol(symbol=symbol, side="sell"):
                        existing_order_id = str(getattr(existing_sell, "id", "") or "")
                        if existing_order_id:
                            broker.cancel_order(existing_order_id)
                            print(f"[{ts(now_et)}] SHORT_GUARD canceled open sell order {existing_order_id} for {symbol}.")
                except Exception as exc:  # noqa: BLE001
                    print(f"[{ts(now_et)}] SHORT_GUARD sell-order cleanup failed for {symbol}: {exc}")
                if broker.has_open_order_for_symbol(symbol=symbol, side="buy"):
                    print(f"[{ts(now_et)}] SHORT_GUARD {symbol}: buy-to-cover already open.")
                    continue
                try:
                    order = broker.cover_option_market(symbol, cover_qty)
                    order_id = str(getattr(order, "id", "") or "")
                    filled_qty = 0
                    if order_id:
                        filled_qty, _fill_price, status, _still_open = _await_order_fill(
                            broker,
                            order_id=order_id,
                            requested_qty=cover_qty,
                            now_et=now_et,
                            label=f"SHORT_GUARD {symbol}",
                            poll_seconds=1,
                            max_wait_seconds=max(3, int(getattr(config, "STOPLOSS_EXIT_ORDER_MAX_WAIT_SECONDS", 3) or 3)),
                        )
                    if filled_qty > 0:
                        total_filled += filled_qty
                        open_trade_meta.pop(symbol, None)
                        print(
                            f"[{ts(now_et)}] SHORT_GUARD covered short option "
                            f"{symbol} qty={filled_qty}/{cover_qty}."
                        )
                    else:
                        print(f"[{ts(now_et)}] SHORT_GUARD cover pending/not filled for {symbol} qty={cover_qty}.")
                except Exception as exc:  # noqa: BLE001
                    print(f"[{ts(now_et)}] SHORT_GUARD cover failed for {symbol} qty={cover_qty}: {exc}")
                continue
            ticker = str(getattr(pos, "underlying_symbol", "") or "").upper()
            if not ticker:
                parsed_ticker, _parsed_direction = _parse_option_symbol(symbol)
                ticker = parsed_ticker.upper()
            if not ticker:
                continue
            ticker_positions.setdefault(ticker, []).append((symbol, qty))

        close_actions: list[tuple[str, int, str]] = []
        for ticker, entries in ticker_positions.items():
            # Deterministic keep order: largest qty first, then symbol.
            ordered = sorted(entries, key=lambda item: (-int(item[1]), str(item[0])))
            allowance = 1
            for symbol, qty in ordered:
                keep_qty = min(allowance, qty)
                close_qty = max(0, qty - keep_qty)
                allowance = max(0, allowance - keep_qty)
                if allowance == 0 and close_qty == 0:
                    continue
                if allowance == 0 and keep_qty == 0:
                    close_qty = qty
                if close_qty > 0:
                    close_actions.append((symbol, close_qty, ticker))

        for symbol, close_qty, ticker in close_actions:
            filled_qty, _fill_price, _close_meta = _close_position_with_confirmation(
                symbol=symbol,
                qty=close_qty,
                now_et=now_et,
                label="EXPOSURE_GUARD",
                exit_reason="exposure_normalize",
            )
            if filled_qty <= 0:
                continue
            total_filled += filled_qty
            meta = open_trade_meta.get(symbol, {})
            existing_qty = int(meta.get("qty", 0) or 0)
            if existing_qty <= filled_qty:
                open_trade_meta.pop(symbol, None)
            elif symbol in open_trade_meta:
                open_trade_meta[symbol]["qty"] = existing_qty - filled_qty
            print(
                f"[{ts(now_et)}] EXPOSURE_GUARD closed {filled_qty} {symbol} "
                f"to normalize {ticker} to one contract."
            )
        if total_filled > 0:
            _save_runtime_state()
        return total_filled

    mode = "PAPER" if config.PAPER else "LIVE"
    _seed_signal_pattern_memory_from_trade_history(datetime.now(tz))
    try:
        acct = broker.get_account()
        print(
            f"[{ts()} | {ts_ct()}] Account status={getattr(acct, 'status', 'unknown')} "
            f"trading_blocked={getattr(acct, 'trading_blocked', 'unknown')} "
            f"options_level={getattr(acct, 'options_trading_level', 'unknown')} "
            f"options_approved={getattr(acct, 'options_approved_level', 'unknown')}"
        )
        print(
            f"[{ts()} | {ts_ct()}] Account equity={getattr(acct, 'equity', 'unknown')} "
            f"daytrade_count={getattr(acct, 'daytrade_count', 'unknown')}"
        )
    except Exception as exc:  # noqa: BLE001
        print(f"[{ts()} | {ts_ct()}] Account diagnostics unavailable: {exc}")
    initial_control = load_trading_control()
    if bool(initial_control.get("manual_stop", False)):
        reason = str(initial_control.get("reason", "") or "manual_stop")
        print(f"[{ts()} | {ts_ct()}] Startup state: KILLSWITCH ACTIVE ({reason}). Trading will remain paused.")
    else:
        print(f"[{ts()} | {ts_ct()}] Startup state: KILLSWITCH not active.")
    print(f"[{ts()} | {ts_ct()}] Autotrader started in {mode} mode. Waiting for market open.")
    closed_market_lead_minutes = _closed_market_lead_minutes()
    alerts.send(
        "startup",
        f"Autotrader online ({mode}). Closed-market workflow starts {closed_market_lead_minutes}m before open.",
        dedupe_key="startup",
    )

    while True:
        now_et = datetime.now(tz)
        now_ct = datetime.now(pytz.timezone(config.CENTRAL_TZ))
        last_trader_heartbeat_et = datetime.now(tz).isoformat()
        control_state = load_trading_control()
        strategy_profile = normalize_profile_name(str(control_state.get("strategy_profile", "balanced") or "balanced"))
        dry_run_enabled = bool(control_state.get("dry_run", False)) or is_enabled("FEATURE_DRY_RUN_MODE", False)
        manual_stop = bool(control_state.get("manual_stop", False))
        if manual_stop:
            _touch_heartbeat(force=True)
            now_et = datetime.now(tz)
            now_ct = datetime.now(pytz.timezone(config.CENTRAL_TZ))
            if not manual_stop_latched:
                reason = str(control_state.get("reason", "") or "manual_stop")
                print(f"[{ts(now_et)} | {ts_ct(now_ct)}] KILLSWITCH ACTIVE ({reason}). Waiting for manual start.")
                alerts.send(
                    "killswitch_active",
                    f"Kill-switch active. Trading paused until manual start. reason={reason}",
                    level="warning",
                    dedupe_key="killswitch-active",
                )
                manual_stop_latched = True
            time.sleep(max(5, int(config.MANUAL_PAUSE_SLEEP_SECONDS)))
            continue
        if manual_stop_latched:
            now_et = datetime.now(tz)
            now_ct = datetime.now(pytz.timezone(config.CENTRAL_TZ))
            print(f"[{ts(now_et)} | {ts_ct(now_ct)}] KILLSWITCH CLEARED. Auto mode re-enabled.")
            alerts.send(
                "killswitch_cleared",
                "Kill-switch cleared. Trading auto mode re-enabled.",
                dedupe_key=f"killswitch-cleared-{now_et.date().isoformat()}",
            )
            manual_stop_latched = False
        clock = _safe_get_clock(phase="pre-open", now_et=now_et, now_ct=now_ct)
        if clock is None:
            continue
        now_et = datetime.now(tz)
        now_ct = datetime.now(pytz.timezone(config.CENTRAL_TZ))
        seconds_until_open = _seconds_until_next_open(clock)
        preopen_window_seconds = int(closed_market_lead_minutes) * 60
        if clock.is_open or (
            seconds_until_open is not None and 0 < seconds_until_open <= preopen_window_seconds
        ):
            break
        sleep_seconds = _closed_market_sleep_seconds(clock, preopen_ready_minutes=closed_market_lead_minutes)
        next_open = getattr(clock, "next_open", None)
        next_open_ct = ""
        if next_open is not None:
            if next_open.tzinfo is None:
                next_open = pytz.utc.localize(next_open)
            next_open_ct = next_open.astimezone(pytz.timezone(config.CENTRAL_TZ)).strftime("%Y-%m-%d %H:%M:%S %Z")
        if alerts.enabled() and time.time() >= next_heartbeat_at:
            alerts.send(
                "heartbeat",
                f"Heartbeat ({mode}): waiting for open. Next open CT: {next_open_ct or 'unknown'}.",
                dedupe_key=f"heartbeat-{int(time.time() // max(1, config.HEARTBEAT_SECONDS))}",
            )
            next_heartbeat_at = time.time() + max(30, int(config.HEARTBEAT_SECONDS))
        _save_runtime_state()
        print(
            f"[{ts(now_et)} | {ts_ct(now_ct)}] Market closed. "
            f"Next open (CT): {next_open_ct or 'unknown'}. Sleeping {sleep_seconds}s."
        )
        time.sleep(sleep_seconds)

    print(
        f"[{ts()} | {ts_ct()}] Pre-open readiness window reached "
        f"({closed_market_lead_minutes}m before open) or market already open. Starting loop."
    )
    while True:
        now_et = datetime.now(tz)
        now_ct = datetime.now(pytz.timezone(config.CENTRAL_TZ))
        _touch_heartbeat(force=True)
        control_state = load_trading_control()
        strategy_profile = normalize_profile_name(str(control_state.get("strategy_profile", "balanced") or "balanced"))
        dry_run_enabled = bool(control_state.get("dry_run", False)) or is_enabled("FEATURE_DRY_RUN_MODE", False)
        manual_stop = bool(control_state.get("manual_stop", False))
        if manual_stop:
            if bool(getattr(config, "ENABLE_ALPACA_TRUTH_LOSS_GUARD", True)):
                try:
                    truth_snapshot = _alpaca_option_day_pnl_snapshot(broker, now_et)
                    adaptive_before = adaptive_loss_active
                    blocked_before = set(adaptive_loss_blocked_tickers)
                    profile_before = dict(adaptive_loss_profile)
                    broker_truth_day_pnl_usd = float(truth_snapshot.get("realized_pnl_usd", 0.0) or 0.0)
                    broker_truth_closed_count = int(truth_snapshot.get("closed_count", 0) or 0)
                    broker_truth_last_error = ""
                    adapt_after_loss = abs(float(getattr(config, "ALPACA_TRUTH_ADAPT_AFTER_LOSS_USD", 0.0) or 0.0))
                    direction_flip_changed = _arm_loss_direction_flips_from_truth(truth_snapshot, now_et)
                    if adapt_after_loss > 0 and broker_truth_day_pnl_usd <= -adapt_after_loss:
                        adaptive_loss_active = True
                        _refresh_adaptive_loss_profile_from_truth(truth_snapshot, now_et)
                    if (
                        adaptive_loss_active != adaptive_before
                        or adaptive_loss_blocked_tickers != blocked_before
                        or adaptive_loss_profile != profile_before
                        or direction_flip_changed
                    ):
                        _save_runtime_state()
                except Exception as exc:  # noqa: BLE001
                    broker_truth_last_error = str(exc)[:300]
                    _save_runtime_state()
                    print(f"[{ts(now_et)}] Paused truth-loss analysis unavailable: {type(exc).__name__}: {exc!r}")
            if not manual_stop_latched:
                reason = str(control_state.get("reason", "") or "manual_stop")
                print(
                    f"[{ts(now_et)} | {ts_ct(now_ct)}] KILLSWITCH ACTIVE ({reason}). "
                    "Flattening positions and pausing trading."
                )
                alerts.send(
                    "killswitch_active",
                    f"Kill-switch active in live loop. Flattening positions. reason={reason}",
                    level="warning",
                    dedupe_key=f"killswitch-live-{now_et.date().isoformat()}",
                )
                _flatten_positions_for_killswitch(broker, now_et, label="KILLSWITCH")
                manual_stop_latched = True
                _save_runtime_state()
            time.sleep(max(5, int(config.MANUAL_PAUSE_SLEEP_SECONDS)))
            continue
        if manual_stop_latched:
            print(f"[{ts(now_et)} | {ts_ct(now_ct)}] KILLSWITCH CLEARED. Manual start acknowledged.")
            alerts.send(
                "killswitch_cleared",
                "Kill-switch cleared. Trading resumed.",
                dedupe_key=f"killswitch-resume-{now_et.date().isoformat()}",
            )
            manual_stop_latched = False
        week_key_now = _week_key(now_et.date())
        if week_key_now != weekly_loss_key:
            weekly_loss_key = week_key_now
            weekly_realized_loss_usd = 0.0

        clock = _safe_get_clock(phase="market-loop", now_et=now_et, now_ct=now_ct)
        if clock is None:
            continue
        if not clock.is_open:
            sleep_seconds = _closed_market_sleep_seconds(clock, preopen_ready_minutes=closed_market_lead_minutes)
            next_open = getattr(clock, "next_open", None)
            next_open_ct = ""
            if next_open is not None:
                if next_open.tzinfo is None:
                    next_open = pytz.utc.localize(next_open)
                next_open_ct = next_open.astimezone(pytz.timezone(config.CENTRAL_TZ)).strftime("%Y-%m-%d %H:%M:%S %Z")

            if bool(getattr(config, "ENABLE_PREMARKET_OPENING_SIGNALS", False)):
                decision = premarket_scan_decision(
                    now_et,
                    signals_day=premarket_signals_day,
                    last_scan_at=premarket_last_scan_at,
                    scan_runs=premarket_scan_runs,
                    max_runs=max(0, int(getattr(config, "PREMARKET_SCAN_MAX_RUNS", 0))),
                    interval_seconds=max(15, int(getattr(config, "PREMARKET_SCAN_INTERVAL_SECONDS", 120))),
                    window_start=config.PREMARKET_SIGNAL_WINDOW_START,
                    window_end=config.PREMARKET_SIGNAL_WINDOW_END,
                    entry_open_time=config.NO_NEW_TRADES_BEFORE,
                )
                if bool(decision["reset_day"]):
                    premarket_opening_signals = []
                premarket_signals_day = str(decision["today_tag"])
                premarket_scan_runs = int(decision["effective_scan_runs"])
                premarket_last_scan_at = decision["effective_last_scan_at"]
                if bool(decision["should_scan"]):
                    watchlist_control_state = load_watchlist_control()
                    watchlist_mode = str(watchlist_control_state.get("mode", "off") or "off").lower()
                    premarket_watchlist = _build_scan_universe(data_client)
                    premarket_watchlist = _apply_watchlist_mode(premarket_watchlist, watchlist_control_state)
                    print(
                        f"[{ts(now_et)}] Premarket scan on {len(premarket_watchlist)} tickers "
                        f"(watchlist mode={watchlist_mode})."
                    )
                    premarket_signals = run_scan(
                        premarket_watchlist,
                        now_et=now_et,
                        premarket_mode=True,
                    ) if premarket_watchlist else []
                    max_premarket = max(0, int(getattr(config, "PREMARKET_MAX_SIGNALS", 0)))
                    merged_premarket = _dedupe_signals_by_symbol(
                        list(premarket_signals) + list(premarket_opening_signals)
                    )
                    merged_premarket.sort(key=_signal_sort_key, reverse=True)
                    if max_premarket > 0:
                        merged_premarket = merged_premarket[:max_premarket]
                    premarket_opening_signals = merged_premarket
                    premarket_scan_runs += 1
                    premarket_last_scan_at = now_et
                    print(
                        f"[{ts(now_et)}] Premarket opening set prepared: "
                        f"{len(premarket_opening_signals)} signal(s) after run {premarket_scan_runs}."
                    )
                    _save_runtime_state()

            if alerts.enabled() and time.time() >= next_heartbeat_at:
                alerts.send(
                    "heartbeat",
                    f"Heartbeat ({mode}): market closed. Next open CT: {next_open_ct or 'unknown'}.",
                    dedupe_key=f"heartbeat-{int(time.time() // max(1, config.HEARTBEAT_SECONDS))}",
                )
                next_heartbeat_at = time.time() + max(30, int(config.HEARTBEAT_SECONDS))
            _save_runtime_state()
            print(
                f"[{ts(now_et)} | {ts_ct(now_ct)}] Market closed. "
                f"Next open (CT): {next_open_ct or 'unknown'}. Sleeping {sleep_seconds}s."
            )
            time.sleep(sleep_seconds)
            continue
        if loss_counters_day != now_et.date():
            daily_realized_loss_usd = 0.0
            consecutive_losses = 0
            consecutive_loss_cooldown_until = None
            consecutive_loss_cooldown_loss_count = 0
            consecutive_loss_recovery_announced_count = 0
            loss_counters_day = now_et.date()
            watchlist = []
            observation_done = False
            hot_tickers = []
            blocked_day_notice = None
            vix_block_notice = None
            catalyst_mode_active = False
            catalyst_mode_reason = ""
            catalyst_mode_until = None
            ticker_entry_counts = {}
            ticker_reentry_armed = {}
            ticker_reentry_expected_direction = {}
            ticker_reentries_used = {}
            ticker_loss_cooldown_until = {}
            ticker_roundtrip_cooldown_until = {}
            premarket_opening_signals = []
            premarket_signals_day = ""
            premarket_scan_runs = 0
            premarket_last_scan_at = None
            bad_fill_tracker = {}
            trade_telemetry_day = now_et.date().isoformat()
            trade_telemetry_closed_count = 0
            trade_telemetry_total_pnl_usd = 0.0
            trade_telemetry_last_close_iso = ""
            trade_telemetry_last_log_error = ""
            broker_truth_day_pnl_usd = 0.0
            broker_truth_closed_count = 0
            broker_truth_last_error = ""
            adaptive_loss_active = False
            adaptive_loss_blocked_tickers = set()
            adaptive_loss_profile = {}
            opening_entries_today_count = 0
            opening_fresh_premium_deployed_usd = 0.0
            opening_expensive_entries_today_count = 0
            non_core_entries_today_count = 0
            set_catalyst_mode(False, "")

        option_positions = broker.get_open_option_positions()
        stale_meta_count = _prune_stale_open_trade_meta(open_trade_meta, option_positions)
        if stale_meta_count > 0:
            print(f"[{ts(now_et)}] Pruned {stale_meta_count} stale runtime position record(s) not present at Alpaca.")
            _save_runtime_state()
        hydrated_count = _hydrate_missing_position_meta(open_trade_meta, option_positions, now_et)
        if hydrated_count > 0:
            for meta in open_trade_meta.values():
                ticker = str(meta.get("ticker", "") or "").upper()
                if ticker:
                    ticker_entry_counts[ticker] = max(int(ticker_entry_counts.get(ticker, 0)), 1)
            print(f"[{ts(now_et)}] Hydrated {hydrated_count} externally-opened position(s) into runtime state.")
            _save_runtime_state()
        normalized_fills = _force_normalize_ticker_exposure(option_positions, now_et)
        if normalized_fills > 0:
            option_positions = broker.get_open_option_positions()
        open_count = len(option_positions)
        alpaca_truth_ticker_roundtrips: dict[str, int] = {}
        alpaca_buy_orders_by_ticker: dict[str, int] = {}
        alpaca_active_buy_orders_by_ticker: dict[str, int] = {}
        active_entry_order_premium_usd = 0.0
        try:
            duplicate_entry_cancels = _cancel_active_entry_buys_for_open_tickers(broker, option_positions, now_et)
            if duplicate_entry_cancels > 0:
                time.sleep(config.RATE_LIMIT_SLEEP_SECONDS)
            stale_entry_cancels = _cancel_stale_active_entry_buy_orders(broker, now_et)
            if stale_entry_cancels > 0:
                time.sleep(config.RATE_LIMIT_SLEEP_SECONDS)
            alpaca_buy_orders_by_ticker = _alpaca_option_buy_order_counts_by_ticker_today(broker, now_et)
            alpaca_active_buy_orders_by_ticker = _alpaca_active_option_buy_order_counts_by_ticker_today(broker, now_et)
            active_entry_order_premium_usd = _alpaca_active_option_buy_order_premium_usd_today(broker, now_et)
        except Exception as exc:  # noqa: BLE001
            print(f"[{ts(now_et)}] Alpaca buy-order cap lookup unavailable: {type(exc).__name__}: {exc!r}")

        if bool(getattr(config, "ENABLE_ALPACA_TRUTH_LOSS_GUARD", True)):
            try:
                truth_snapshot = _alpaca_option_day_pnl_snapshot(broker, now_et)
                adaptive_before = adaptive_loss_active
                blocked_before = set(adaptive_loss_blocked_tickers)
                profile_before = dict(adaptive_loss_profile)
                broker_truth_day_pnl_usd = float(truth_snapshot.get("realized_pnl_usd", 0.0) or 0.0)
                broker_truth_closed_count = int(truth_snapshot.get("closed_count", 0) or 0)
                broker_truth_last_error = ""
                adapt_after_loss = abs(float(getattr(config, "ALPACA_TRUTH_ADAPT_AFTER_LOSS_USD", 0.0) or 0.0))
                direction_flip_changed = _arm_loss_direction_flips_from_truth(truth_snapshot, now_et)
                if adapt_after_loss > 0 and broker_truth_day_pnl_usd <= -adapt_after_loss:
                    if not adaptive_loss_active:
                        print(
                            f"[{ts(now_et)}] Adaptive loss mode active: "
                            f"Alpaca realized P/L ${broker_truth_day_pnl_usd:.2f}."
                        )
                    adaptive_loss_active = True
                    _refresh_adaptive_loss_profile_from_truth(truth_snapshot, now_et)
                if (
                    adaptive_loss_active != adaptive_before
                    or adaptive_loss_blocked_tickers != blocked_before
                    or adaptive_loss_profile != profile_before
                    or direction_flip_changed
                ):
                    _save_runtime_state()
                truth_loss_limit = abs(
                    float(
                        getattr(
                            config,
                            "ALPACA_TRUTH_DAILY_LOSS_LIMIT_USD",
                            getattr(config, "DAILY_LOSS_LIMIT_USD", 0.0),
                        )
                        or 0.0
                    )
                )
                truth_closed_trades = list(truth_snapshot.get("closed_trades") or [])
                for item in truth_closed_trades:
                    truth_ticker = str(item.get("ticker", "") or "").upper()
                    if truth_ticker:
                        alpaca_truth_ticker_roundtrips[truth_ticker] = (
                            int(alpaca_truth_ticker_roundtrips.get(truth_ticker, 0)) + 1
                        )
                truth_loss_count = sum(
                    1
                    for item in truth_closed_trades
                    if _safe_signal_float(item.get("realized_pnl_usd"), 0.0) < 0
                )
                stop_after_losses = max(0, int(getattr(config, "ALPACA_TRUTH_STOP_AFTER_LOSSES", 0) or 0))
                hard_stop_reasons: list[str] = []
                if truth_loss_limit > 0 and broker_truth_day_pnl_usd <= -truth_loss_limit:
                    hard_stop_reasons.append(
                        f"daily_loss pnl=${broker_truth_day_pnl_usd:.2f} <= -${truth_loss_limit:.2f}"
                    )
                if stop_after_losses > 0 and truth_loss_count >= stop_after_losses:
                    hard_stop_reasons.append(
                        f"loss_count {truth_loss_count} >= {stop_after_losses}"
                    )
                if hard_stop_reasons:
                    reason = "alpaca_truth_hard_stop " + "; ".join(hard_stop_reasons)
                    set_manual_stop(True, reason=reason[:240])
                    print(f"[{ts(now_et)}] ALPACA TRUTH LOSS GUARD hit: {reason}. Flattening and pausing.")
                    alerts.send(
                        "alpaca_truth_daily_loss_guard",
                        f"Alpaca truth loss guard hit: {reason}. Trading paused.",
                        level="error",
                        dedupe_key=f"alpaca-truth-loss-{now_et.date().isoformat()}",
                    )
                    _flatten_positions_for_killswitch(broker, now_et, label="ALPACA_TRUTH_LOSS_GUARD")
                    option_positions = broker.get_open_option_positions()
                    open_trade_meta.clear()
                    open_count = len(option_positions)
                    _save_runtime_state()
                    time.sleep(config.LOOP_INTERVAL_SECONDS)
                    continue
            except Exception as exc:  # noqa: BLE001
                broker_truth_last_error = str(exc)[:300]
                print(f"[{ts(now_et)}] Alpaca truth loss guard unavailable: {type(exc).__name__}: {exc!r}")
                _save_runtime_state()

        if alerts.enabled() and time.time() >= next_heartbeat_at:
            alerts.send(
                "heartbeat",
                (
                    f"Heartbeat ({mode}): market open. Positions={open_count} "
                    f"daily_loss=${daily_realized_loss_usd:.2f} weekly_loss=${weekly_realized_loss_usd:.2f}"
                ),
                dedupe_key=f"heartbeat-{int(time.time() // max(1, config.HEARTBEAT_SECONDS))}",
            )
            next_heartbeat_at = time.time() + max(30, int(config.HEARTBEAT_SECONDS))

        watchlist_control_state = load_watchlist_control()
        watchlist_mode = str(watchlist_control_state.get("mode", "off") or "off").lower()
        watchlist = _build_scan_universe(data_client)
        watchlist = _apply_watchlist_mode(watchlist, watchlist_control_state)
        if hot_tickers:
            watchlist = hot_tickers + [s for s in watchlist if s not in hot_tickers]
        print(
            f"[{ts(now_et)}] Running scan on {len(watchlist)} tickers "
            f"(watchlist mode={watchlist_mode})."
        )

        if catalyst_mode_active and catalyst_mode_until and now_et >= catalyst_mode_until:
            catalyst_mode_active = False
            catalyst_mode_reason = ""
            catalyst_mode_until = None
            set_catalyst_mode(False, "")
            print(f"[{ts(now_et)}] Catalyst mode expired. Returning to normal filters.")

        if config.ENABLE_CATALYST_MODE and not catalyst_mode_active:
            triggered, reason = _detect_catalyst_event(data_client=data_client, now_et=now_et, watchlist=watchlist)
            if triggered:
                catalyst_mode_active = True
                catalyst_mode_reason = reason
                catalyst_mode_until = now_et + timedelta(minutes=int(config.CATALYST_WINDOW_MINUTES))
                set_catalyst_mode(True, reason)
                print(
                    f"[{ts(now_et)}] CATALYST MODE ON ({reason}). "
                    f"Relaxed filters active until {ts(catalyst_mode_until)}."
                )

        if (
            config.OBSERVATION_ENABLED
            and watchlist
            and not observation_done
            and is_at_or_after(now_et, config.SCAN_MORNING_TIME)
            and not is_at_or_after(now_et, config.OBSERVATION_END_TIME)
        ):
            print(f"[{ts(now_et)}] Observation phase: collecting opening range data...")
            hot_tickers = run_observation_phase(watchlist, data_client, now_et)
            watchlist = hot_tickers + [s for s in watchlist if s not in hot_tickers]
            observation_done = True

        if is_at_or_after(now_et, config.OBSERVATION_END_TIME) and not observation_done:
            if not hot_tickers and watchlist:
                hot_tickers = run_observation_phase(watchlist, data_client, now_et)
                watchlist = hot_tickers + [s for s in watchlist if s not in hot_tickers]
            observation_done = True
            print(f"[{ts(now_et)}] Observation phase complete. Hot tickers at front of queue: {hot_tickers}")

        # --- PDT / equity checks (log-only, non-blocking) ---
        try:
            pdt_allowed, pdt_info = broker.pdt_allows_new_day_trade()
        except Exception as exc:  # noqa: BLE001
            pdt_allowed, pdt_info = True, {"reason": "pdt_check_error", "equity": None, "daytrade_count": None}
            print(f"[{ts(now_et)} | {ts_ct(now_ct)}] PDT guard check failed (fail-open): {exc}")
        entry_times_rolling = _prune_recent_entries(entry_times_rolling, now_et, days=5)
        equity = pdt_info.get("equity")
        under_25k = equity is not None and float(equity) < config.PDT_MIN_EQUITY
        local_trade_budget_hit = (
            config.ENFORCE_PDT_GUARD
            and under_25k
            and len(entry_times_rolling) >= config.PDT_MAX_DAY_TRADES_5D
        )
        if under_25k and len(entry_times_rolling) >= config.PDT_MAX_DAY_TRADES_5D:
            print(
                f"[{ts(now_et)}] PDT note: {len(entry_times_rolling)}/{config.PDT_MAX_DAY_TRADES_5D} "
                f"entries in 5 days, equity=${float(equity):.2f} (guard={'ON' if config.ENFORCE_PDT_GUARD else 'OFF'})."
            )

        blocked_day = _is_news_block_day(now_et)
        vix_value = _fetch_vix_level() if config.ENABLE_VIX_GUARD else None
        vix_blocked = False
        if config.ENABLE_VIX_GUARD:
            if vix_value is None:
                vix_blocked = False
                if vix_block_notice != now_et.date().isoformat():
                    vix_block_notice = now_et.date().isoformat()
                    print(f"[{ts(now_et)}] VIX data unavailable; guard fail-open (trading not blocked).")
            else:
                vix_blocked = vix_value < float(config.VIX_MIN) or vix_value > float(config.VIX_MAX)
        if blocked_day:
            if blocked_day_notice != now_et.date().isoformat():
                blocked_day_notice = now_et.date().isoformat()
                notice = (
                    f"[{ts(now_et)}] Event block day ({blocked_day_notice}) configured via "
                    f"NEWS_BLOCK_DATES_ET. Skipping new entries."
                )
                print(notice)
                alerts.send(
                    "event_day_block",
                    f"Autotrader paused new entries for configured event day: {blocked_day_notice}.",
                    dedupe_key=f"event-day-{blocked_day_notice}",
                )
            signals = []
        elif vix_blocked:
            vix_tag = now_et.date().isoformat()
            if vix_block_notice != vix_tag:
                vix_block_notice = vix_tag
                vix_text = "unavailable" if vix_value is None else f"{vix_value:.2f}"
                print(
                    f"[{ts(now_et)}] VIX guard blocking entries: VIX={vix_text}, "
                    f"allowed range {config.VIX_MIN:.2f}-{config.VIX_MAX:.2f}."
                )
                alerts.send(
                    "vix_guard_block",
                    (
                        f"VIX guard active. VIX={vix_text}, "
                        f"range={config.VIX_MIN:.2f}-{config.VIX_MAX:.2f}. New entries paused."
                    ),
                    dedupe_key=f"vix-guard-{vix_tag}",
                )
            signals = []
        else:
            vix_block_notice = None
            signals = run_scan(watchlist) if watchlist else []

        if (
            bool(getattr(config, "ENABLE_PREMARKET_OPENING_SIGNALS", False))
            and premarket_opening_signals
            and premarket_signals_day == now_et.date().isoformat()
            and is_at_or_after(now_et, config.NO_NEW_TRADES_BEFORE)
            and not is_at_or_after(now_et, config.PREMARKET_APPLY_UNTIL)
        ):
            merged = _dedupe_signals_by_symbol(list(premarket_opening_signals) + list(signals))
            print(
                f"[{ts(now_et)}] Applied premarket opening set: "
                f"{len(premarket_opening_signals)} staged + {len(signals)} live -> {len(merged)} unique signals."
            )
            signals = merged
            premarket_opening_signals = []
            _save_runtime_state()

        index_bias = _index_regime_bias(data_client, now_et)
        if index_bias in ("call", "put") and signals:
            before = len(signals)
            signals = [s for s in signals if str(s.get("direction", "")).lower() == index_bias]
            print(f"[{ts(now_et)}] Index bias={index_bias.upper()} filtered signals {before}->{len(signals)}.")
        elif signals:
            print(f"[{ts(now_et)}] Index bias neutral; keeping both call/put signals.")

        entry_min_signal_score = _runtime_entry_min_signal_score()
        if signals:
            before = len(signals)
            filtered_signals: list[dict] = []
            for s in signals:
                floor = float(s.get("profile_min_signal_score", entry_min_signal_score) or entry_min_signal_score)
                if _is_in_opening_strict_window(now_et):
                    floor = max(
                        floor,
                        float(
                            getattr(
                                config,
                                "OPENING_STRICT_MIN_SIGNAL_SCORE",
                                floor,
                            )
                            or floor
                        ),
                    )
                if float(s.get("signal_score", 0) or 0) >= floor:
                    filtered_signals.append(s)
            signals = filtered_signals
            if before != len(signals):
                print(
                    f"[{ts(now_et)}] Entry signal-score gate (runtime floor {entry_min_signal_score:.2f}) "
                    f"filtered signals {before}->{len(signals)} (profile={strategy_profile})."
                )

        if signals and _is_in_opening_strict_window(now_et):
            opening_signal_cap = max(1, int(getattr(config, "OPENING_MAX_SIGNAL_CANDIDATES", len(signals)) or len(signals)))
            if len(signals) > opening_signal_cap:
                signals.sort(
                    key=lambda s: (
                        float(s.get("signal_score", 0.0) or 0.0),
                        float(s.get("rvol", 0.0) or 0.0),
                    ),
                    reverse=True,
                )
                before_opening = len(signals)
                signals = signals[:opening_signal_cap]
                print(
                    f"[{ts(now_et)}] Opening strict shortlist capped signals "
                    f"{before_opening}->{len(signals)} (cap={opening_signal_cap})."
                )

        if not pdt_allowed:
            print(
                f"[{ts(now_et)}] PDT broker flag: no new entries reported. "
                f"equity={float(pdt_info.get('equity') or 0):.2f} "
                f"daytrades_5d={pdt_info.get('daytrade_count')}/{config.PDT_MAX_DAY_TRADES_5D} "
                f"(ENFORCE_PDT_GUARD={config.ENFORCE_PDT_GUARD})"
            )

        entry_debug: dict[str, object] = {
            "loop_ts_et": ts(now_et),
            "watchlist_count": len(watchlist),
            "watchlist_mode": watchlist_mode,
            "watchlist_tickers": list(watchlist_control_state.get("tickers") or []),
            "signal_detected_count": len(signals),
            "scan_pass_count": len(signals),
            "signals_considered": 0,
            "entry_eligible_count": 0,
            "entry_stage4_eligible_count": 0,
            "entry_stage4_reject_count": 0,
            "entry_stage4_reject_reasons": {},
            "entry_stage4_eligible_symbols": [],
            "entry_stage4_rejected_symbols": [],
            "entry_orders_submitted": 0,
            "entries_filled": 0,
            "signal_outcomes": {},
            "skips": {},
            "exceptions": [],
        }

        def _normalize_disposition_reason(reason: str) -> str:
            token = str(reason or "").strip().lower()
            aliases = {
                "premium_per_trade_cap": "premium_cap",
                "entry_confirmation_mismatch": "entry_confirmation",
                "quote_spread_too_wide": "spread_gate",
                "same_direction_exposure_cap": "same_direction_cap",
                "opening_fresh_entry_cap": "opening_limit",
            }
            normalized = aliases.get(token, token)
            normalized = normalized.replace(" ", "_")
            return normalized or "unknown"

        def _set_signal_outcome(*, ticker: str, disposition: str, detail: str = "") -> None:
            outcomes = entry_debug.get("signal_outcomes", {})
            if not isinstance(outcomes, dict):
                outcomes = {}
            symbol_upper = str(ticker or "").upper()
            if not symbol_upper:
                return
            outcomes[symbol_upper] = {
                "disposition": str(disposition or "").strip() or "unknown",
                "detail": str(detail or "").strip(),
            }
            entry_debug["signal_outcomes"] = outcomes

        def _mark_skip(reason: str) -> None:
            skips = entry_debug.get("skips", {})
            if not isinstance(skips, dict):
                skips = {}
            skips[reason] = int(skips.get(reason, 0)) + 1
            entry_debug["skips"] = skips

        def _mark_stage4_reject(*, reason: str, ticker: str, detail: str = "") -> None:
            reject_reasons = entry_debug.get("entry_stage4_reject_reasons", {})
            if not isinstance(reject_reasons, dict):
                reject_reasons = {}
            reject_reasons[reason] = int(reject_reasons.get(reason, 0)) + 1
            entry_debug["entry_stage4_reject_reasons"] = reject_reasons
            entry_debug["entry_stage4_reject_count"] = int(entry_debug.get("entry_stage4_reject_count", 0)) + 1

            rejected_symbols = entry_debug.get("entry_stage4_rejected_symbols", [])
            if not isinstance(rejected_symbols, list):
                rejected_symbols = []
            symbol_upper = str(ticker or "").upper()
            if symbol_upper and symbol_upper not in rejected_symbols:
                rejected_symbols.append(symbol_upper)
            entry_debug["entry_stage4_rejected_symbols"] = rejected_symbols
            _set_signal_outcome(
                ticker=ticker,
                disposition=f"blocked_{_normalize_disposition_reason(reason)}",
                detail=str(detail or reason),
            )

        def _mark_stage4_eligible(*, ticker: str) -> None:
            entry_debug["entry_stage4_eligible_count"] = int(entry_debug.get("entry_stage4_eligible_count", 0)) + 1
            entry_debug["entry_eligible_count"] = int(entry_debug.get("entry_stage4_eligible_count", 0))
            eligible_symbols = entry_debug.get("entry_stage4_eligible_symbols", [])
            if not isinstance(eligible_symbols, list):
                eligible_symbols = []
            symbol_upper = str(ticker or "").upper()
            if symbol_upper and symbol_upper not in eligible_symbols:
                eligible_symbols.append(symbol_upper)
            entry_debug["entry_stage4_eligible_symbols"] = eligible_symbols
            _set_signal_outcome(ticker=ticker, disposition="entry_eligible")

        def _record_entry_exception(ticker: str, exc: Exception) -> None:
            exceptions = entry_debug.get("exceptions", [])
            if not isinstance(exceptions, list):
                exceptions = []
            if len(exceptions) >= 5:
                return
            exceptions.append(
                {
                    "ticker": ticker,
                    "type": type(exc).__name__,
                    "message": str(exc)[:300],
                }
            )
            entry_debug["exceptions"] = exceptions

        opening_entry_attempts_loop = 0
        entry_attempts_loop = 0

        for signal in signals:
            now_et = datetime.now(tz)
            _touch_heartbeat()
            entry_debug["signals_considered"] = int(entry_debug.get("signals_considered", 0)) + 1

            # --- Daily loss limit ---
            if daily_realized_loss_usd >= config.DAILY_LOSS_LIMIT_USD:
                _mark_skip("daily_loss_limit")
                print(
                    f"[{ts(now_et)}] DAILY LOSS LIMIT hit: "
                    f"${daily_realized_loss_usd:.2f} >= ${config.DAILY_LOSS_LIMIT_USD:.2f}. "
                    f"No new entries today."
                )
                alerts.send(
                    "daily_loss_limit",
                    (
                        f"Daily loss limit hit: ${daily_realized_loss_usd:.2f} "
                        f"(limit ${config.DAILY_LOSS_LIMIT_USD:.2f}). New entries paused."
                    ),
                    level="warning",
                    dedupe_key=f"daily-loss-{now_et.date().isoformat()}",
                )
                break

            # --- Weekly loss limit ---
            if weekly_realized_loss_usd >= config.WEEKLY_LOSS_LIMIT_USD:
                _mark_skip("weekly_loss_limit")
                print(
                    f"[{ts(now_et)}] WEEKLY LOSS LIMIT hit: "
                    f"${weekly_realized_loss_usd:.2f} >= ${config.WEEKLY_LOSS_LIMIT_USD:.2f}. "
                    f"No new entries this week."
                )
                alerts.send(
                    "weekly_loss_limit",
                    (
                        f"Weekly loss limit hit: ${weekly_realized_loss_usd:.2f} "
                        f"(limit ${config.WEEKLY_LOSS_LIMIT_USD:.2f}). New entries paused."
                    ),
                    level="warning",
                    dedupe_key=f"weekly-loss-{weekly_loss_key}",
                )
                break

            # --- Consecutive loss recovery brake ---
            if _consecutive_loss_cooldown_active(now_et):
                break

            # --- Net P&L circuit breaker ---
            intraday_net_loss_limit = abs(float(getattr(config, "INTRADAY_NET_LOSS_LIMIT_USD", 0.0) or 0.0))
            if intraday_net_loss_limit > 0 and trade_telemetry_total_pnl_usd <= -intraday_net_loss_limit:
                _mark_skip("intraday_net_loss_limit")
                print(
                    f"[{ts(now_et)}] INTRADAY NET LOSS LIMIT hit: "
                    f"net=${trade_telemetry_total_pnl_usd:.2f} <= -${intraday_net_loss_limit:.2f}. "
                    "Pausing new entries for the rest of the day."
                )
                alerts.send(
                    "intraday_net_loss_limit",
                    (
                        f"Intraday net loss limit hit: net=${trade_telemetry_total_pnl_usd:.2f} "
                        f"(limit -${intraday_net_loss_limit:.2f}). New entries paused."
                    ),
                    level="warning",
                    dedupe_key=f"intraday-net-loss-{now_et.date().isoformat()}",
                )
                break

            # --- Early-red guard ---
            if bool(getattr(config, "EARLY_RED_GUARD_ENABLED", False)):
                early_red_min_closed = max(1, int(getattr(config, "EARLY_RED_GUARD_MIN_CLOSED_TRADES", 4) or 4))
                early_red_max_net_pnl = float(getattr(config, "EARLY_RED_GUARD_MAX_NET_PNL_USD", -0.01) or -0.01)
                if (
                    trade_telemetry_closed_count >= early_red_min_closed
                    and trade_telemetry_total_pnl_usd <= early_red_max_net_pnl
                ):
                    _mark_skip("early_red_guard")
                    print(
                        f"[{ts(now_et)}] EARLY RED GUARD hit: "
                        f"closed={trade_telemetry_closed_count} net=${trade_telemetry_total_pnl_usd:.2f} "
                        f"(threshold <= ${early_red_max_net_pnl:.2f}). "
                        "Pausing new entries for the rest of the day."
                    )
                    alerts.send(
                        "early_red_guard",
                        (
                            f"Early red guard paused entries: {trade_telemetry_closed_count} closed trades, "
                            f"net=${trade_telemetry_total_pnl_usd:.2f} (threshold ${early_red_max_net_pnl:.2f})."
                        ),
                        level="warning",
                        dedupe_key=f"early-red-{now_et.date().isoformat()}",
                    )
                    break

            # --- PDT guard (only blocks if ENFORCE_PDT_GUARD=True) ---
            if local_trade_budget_hit:
                _mark_skip("pdt_local_budget_hit")
                break
            if config.ENFORCE_PDT_GUARD and not pdt_allowed:
                _mark_skip("pdt_broker_block")
                break

            ticker = str(signal["symbol"]).upper()
            direction = str(signal["direction"]).lower()
            _set_signal_outcome(ticker=ticker, disposition="setup_pass")
            pattern_direction, pattern_key, pattern_explanation = _pattern_direction_override(signal, ticker, direction, now_et)
            if pattern_direction != direction:
                previous_direction = direction
                direction = pattern_direction
                signal = dict(signal)
                signal["direction"] = direction
                signal["pattern_direction_override"] = pattern_key
                signal["pattern_direction_override_reason"] = pattern_explanation
                _set_signal_outcome(
                    ticker=ticker,
                    disposition="pattern_direction_override",
                    detail=pattern_explanation or f"{previous_direction}->{direction}",
                )
                print(
                    f"[{ts(now_et)}] {ticker}: {pattern_explanation}"
                )

            if adaptive_loss_active:
                profile = adaptive_loss_profile if isinstance(adaptive_loss_profile, dict) else {}
                ticker_losses = dict(profile.get("ticker_losses") or {})
                block_after = max(1, int(getattr(config, "ADAPTIVE_LOSS_BLOCK_TICKER_AFTER_LOSSES", 1)))
                ticker_loss_count = int(ticker_losses.get(ticker, 0) or 0)
                if ticker in adaptive_loss_blocked_tickers and ticker_loss_count >= block_after:
                    expected_after_loss = str(ticker_reentry_expected_direction.get(ticker, "") or "").lower()
                    allow_direction_flip = (
                        bool(getattr(config, "ENABLE_LOSS_DIRECTION_FLIP", True))
                        and bool(ticker_reentry_armed.get(ticker, False))
                        and expected_after_loss in {"call", "put"}
                        and str(direction or "").lower() == expected_after_loss
                    )
                    if not allow_direction_flip:
                        _mark_skip("adaptive_losing_ticker_block")
                        _mark_stage4_reject(reason="adaptive_losing_ticker_block", ticker=ticker)
                        print(
                            f"[{ts(now_et)}] {ticker}: skip "
                            f"(adaptive loss mode blocked ticker after {ticker_loss_count} same-day loss(es))."
                        )
                        continue
                    print(
                        f"[{ts(now_et)}] {ticker}: adaptive loss block bypassed for "
                        f"direction flip ({expected_after_loss.upper()})."
                    )
                signal_score_now = float(signal.get("signal_score", 0.0) or 0.0)
                direction_score_now = abs(float(signal.get("direction_score", 0.0) or 0.0))
                adaptive_min_signal = max(
                    float(getattr(config, "ADAPTIVE_LOSS_MIN_SIGNAL_SCORE", 7.8)),
                    float(profile.get("min_signal_score", 0.0) or 0.0),
                )
                adaptive_min_signal = min(
                    adaptive_min_signal,
                    float(getattr(config, "ADAPTIVE_LOSS_MAX_SIGNAL_SCORE", adaptive_min_signal)),
                )
                adaptive_min_direction = max(
                    float(getattr(config, "ADAPTIVE_LOSS_MIN_DIRECTION_SCORE", 0.65)),
                    float(profile.get("min_direction_score", 0.0) or 0.0),
                )
                adaptive_min_direction = min(
                    adaptive_min_direction,
                    float(getattr(config, "ADAPTIVE_LOSS_MAX_DIRECTION_SCORE", adaptive_min_direction)),
                )
                if signal_score_now < adaptive_min_signal or direction_score_now < adaptive_min_direction:
                    _mark_skip("adaptive_loss_quality_gate")
                    _mark_stage4_reject(reason="adaptive_loss_quality_gate", ticker=ticker)
                    print(
                        f"[{ts(now_et)}] {ticker}: skip (adaptive loss quality gate; "
                        f"score {signal_score_now:.2f}/{adaptive_min_signal:.2f}, "
                        f"direction {direction_score_now:.2f}/{adaptive_min_direction:.2f})."
                    )
                    continue
                adaptive_min_roc = float(profile.get("min_abs_roc_pct", 0.0) or 0.0)
                adaptive_min_rvol = float(profile.get("min_rvol", 0.0) or 0.0)
                if adaptive_min_roc > 0:
                    adaptive_min_roc = min(
                        adaptive_min_roc,
                        float(getattr(config, "ADAPTIVE_LOSS_MIN_ABS_ROC_PCT", adaptive_min_roc)),
                    )
                if adaptive_min_rvol > 0:
                    adaptive_min_rvol = min(
                        adaptive_min_rvol,
                        float(getattr(config, "ADAPTIVE_LOSS_MIN_RVOL", adaptive_min_rvol)),
                    )
                signal_abs_roc = abs(float(signal.get("roc", 0.0) or 0.0))
                signal_rvol = float(signal.get("rvol", 0.0) or 0.0)
                if (
                    adaptive_min_roc > 0
                    and signal_abs_roc < adaptive_min_roc
                ) or (
                    adaptive_min_rvol > 0
                    and signal_rvol < adaptive_min_rvol
                ):
                    _mark_skip("adaptive_loss_momentum_gate")
                    _mark_stage4_reject(reason="adaptive_loss_momentum_gate", ticker=ticker)
                    print(
                        f"[{ts(now_et)}] {ticker}: skip (adaptive momentum gate; "
                        f"roc {signal_abs_roc:.2f}/{adaptive_min_roc:.2f}, "
                        f"rvol {signal_rvol:.2f}/{adaptive_min_rvol:.2f})."
                    )
                    continue

            # --- Loss throttle (keep trading, but demand stronger setups after losses) ---
            throttle_after_losses = max(1, int(getattr(config, "LOSS_THROTTLE_AFTER_CONSEC_LOSSES", 1) or 1))
            if consecutive_losses >= throttle_after_losses:
                signal_score_now = float(signal.get("signal_score", 0.0) or 0.0)
                volatility_score_now = float(signal.get("volatility_score", 0.0) or 0.0)
                min_signal_add = float(getattr(config, "LOSS_THROTTLE_SIGNAL_SCORE_ADD", 1.0) or 1.0)
                min_volatility_score = float(getattr(config, "LOSS_THROTTLE_MIN_VOLATILITY_SCORE", 6.0) or 6.0)
                throttle_min_signal = _runtime_entry_min_signal_score() + min_signal_add
                if signal_score_now < throttle_min_signal or volatility_score_now < min_volatility_score:
                    _mark_skip("loss_throttle_quality_gate")
                    _mark_stage4_reject(reason="loss_throttle_quality_gate", ticker=ticker)
                    print(
                        f"[{ts(now_et)}] {ticker}: skip (loss throttle active after {consecutive_losses} losses; "
                        f"score {signal_score_now:.2f}/{throttle_min_signal:.2f}, "
                        f"vol {volatility_score_now:.2f}/{min_volatility_score:.2f})."
                    )
                    continue

            active_buy_order_count = sum(int(v) for v in alpaca_active_buy_orders_by_ticker.values())
            max_open_entry_orders = max(0, int(getattr(config, "MAX_OPEN_ENTRY_BUY_ORDERS", 1) or 0))
            if max_open_entry_orders > 0 and active_buy_order_count >= max_open_entry_orders:
                _mark_skip("open_entry_order_cap")
                print(
                    f"[{ts(now_et)}] Open entry-order cap reached "
                    f"({active_buy_order_count}/{max_open_entry_orders}); letting limit buy rest."
                )
                break

            loop_attempt_cap = max(1, int(getattr(config, "MAX_NEW_ENTRY_ATTEMPTS_PER_LOOP", 1) or 1))
            if entry_attempts_loop >= loop_attempt_cap:
                _mark_skip("entry_attempt_cap")
                print(
                    f"[{ts(now_et)}] Entry-attempt cap reached ({entry_attempts_loop}/{loop_attempt_cap}) for this loop."
                )
                break

            if _is_in_opening_strict_window(now_et):
                opening_attempt_cap = max(1, int(getattr(config, "OPENING_MAX_NEW_ENTRY_ATTEMPTS_PER_LOOP", 2) or 2))
                if opening_entry_attempts_loop >= opening_attempt_cap:
                    _mark_skip("opening_entry_attempt_cap")
                    _mark_stage4_reject(reason="opening_entry_attempt_cap", ticker=ticker)
                    print(
                        f"[{ts(now_et)}] Opening entry-attempt cap reached "
                        f"({opening_entry_attempts_loop}/{opening_attempt_cap})."
                    )
                    break

                opening_max_fresh_entries = max(1, int(getattr(config, "OPENING_MAX_FRESH_ENTRIES", 3) or 3))
                if opening_entries_today_count >= opening_max_fresh_entries:
                    _mark_skip("opening_fresh_entry_cap")
                    _mark_stage4_reject(reason="opening_fresh_entry_cap", ticker=ticker)
                    print(
                        f"[{ts(now_et)}] Opening fresh-entry cap reached "
                        f"({opening_entries_today_count}/{opening_max_fresh_entries})."
                    )
                    break

            opening_quality_ok, opening_quality_reason = _opening_entry_quality_ok(signal, now_et)
            if not opening_quality_ok:
                _mark_skip("opening_quality_gate")
                _mark_stage4_reject(reason="opening_quality_gate", ticker=ticker)
                print(f"[{ts(now_et)}] {ticker}: skip ({opening_quality_reason}).")
                continue

            if bool(getattr(config, "ENABLE_FAST_START_ENTRY_QUALITY", True)):
                fast_start_ok, fast_start_reason = _fast_start_entry_quality_ok(signal, now_et)
                if not fast_start_ok:
                    _mark_skip("fast_start_quality_gate")
                    _mark_stage4_reject(reason="fast_start_quality_gate", ticker=ticker, detail=fast_start_reason)
                    print(f"[{ts(now_et)}] {ticker}: skip ({fast_start_reason}).")
                    continue

            if not _is_valid_long_direction(direction):
                _mark_skip("invalid_strategy_direction")
                _mark_stage4_reject(reason="invalid_strategy_direction", ticker=ticker)
                print(f"[{ts(now_et)}] {ticker}: skip (invalid direction={direction!r}; only CALL/PUT allowed).")
                continue

            preferred_core = set(str(s).upper() for s in getattr(config, "PREFERRED_CORE_TICKERS", ()))
            is_non_core = ticker not in preferred_core
            if is_non_core:
                non_core_cap = max(0, int(getattr(config, "MAX_NON_CORE_ENTRIES_PER_DAY", 4)))
                if non_core_entries_today_count >= non_core_cap:
                    _mark_skip("non_core_entry_cap")
                    _mark_stage4_reject(reason="non_core_entry_cap", ticker=ticker)
                    print(
                        f"[{ts(now_et)}] {ticker}: skip (non-core entry cap "
                        f"{non_core_entries_today_count}/{non_core_cap})."
                    )
                    continue
                try:
                    signal_score = float(signal.get("signal_score", 0.0) or 0.0)
                except (TypeError, ValueError):
                    signal_score = 0.0
                non_core_min_signal = float(getattr(config, "NON_CORE_MIN_SIGNAL_SCORE", 9.0) or 9.0)
                if signal_score < non_core_min_signal:
                    _mark_skip("non_core_quality_gate")
                    _mark_stage4_reject(reason="non_core_quality_gate", ticker=ticker)
                    print(
                        f"[{ts(now_et)}] {ticker}: skip (non-core signal score {signal_score:.2f} "
                        f"< {non_core_min_signal:.2f})."
                    )
                    continue

            if _is_bad_fill_blocked(ticker, now_et):
                _mark_skip("bad_fill_cooldown")
                _mark_stage4_reject(reason="bad_fill_cooldown", ticker=ticker)
                print(f"[{ts(now_et)}] {ticker}: skip (bad-fill cooldown active).")
                continue
            roundtrip_cooldown_until = _active_ticker_roundtrip_cooldown_until(ticker, now_et)
            if roundtrip_cooldown_until is not None:
                _mark_skip("ticker_roundtrip_cooldown")
                _mark_stage4_reject(reason="ticker_roundtrip_cooldown", ticker=ticker)
                print(
                    f"[{ts(now_et)}] {ticker}: skip "
                    f"(round-trip diversification cooldown until {ts(roundtrip_cooldown_until)})."
                )
                continue
            truth_roundtrip_cap = max(
                0,
                int(getattr(config, "MAX_ALPACA_TRUTH_ROUNDTRIPS_PER_TICKER_PER_DAY", 1) or 0),
            )
            truth_roundtrip_count = int(alpaca_truth_ticker_roundtrips.get(ticker, 0))
            if truth_roundtrip_cap > 0 and truth_roundtrip_count >= truth_roundtrip_cap:
                _mark_skip("alpaca_truth_ticker_roundtrip_cap")
                _mark_stage4_reject(reason="alpaca_truth_ticker_roundtrip_cap", ticker=ticker)
                print(
                    f"[{ts(now_et)}] {ticker}: skip "
                    f"(Alpaca truth has {truth_roundtrip_count}/{truth_roundtrip_cap} "
                    "closed round-trip(s) today)."
                )
                continue
            loss_cooldown_until = _active_ticker_loss_cooldown_until(ticker, now_et)
            if loss_cooldown_until is not None:
                expected_after_loss = str(ticker_reentry_expected_direction.get(ticker, "") or "").lower()
                allow_direction_flip = (
                    bool(getattr(config, "LOSS_DIRECTION_FLIP_ALLOWS_COOLDOWN_BYPASS", True))
                    and bool(ticker_reentry_armed.get(ticker, False))
                    and expected_after_loss in {"call", "put"}
                    and str(direction or "").lower() == expected_after_loss
                )
                if not allow_direction_flip:
                    _mark_skip("ticker_loss_cooldown")
                    _mark_stage4_reject(reason="ticker_loss_cooldown", ticker=ticker)
                    print(
                        f"[{ts(now_et)}] {ticker}: skip (loss cooldown until {ts(loss_cooldown_until)})."
                    )
                    continue
                print(
                    f"[{ts(now_et)}] {ticker}: loss cooldown bypassed for direction flip "
                    f"({expected_after_loss.upper()})."
                )
            if not is_at_or_after(now_et, config.NO_NEW_TRADES_BEFORE):
                _mark_skip("before_entry_window")
                _mark_stage4_reject(reason="before_entry_window", ticker=ticker)
                print(f"[{ts(now_et)}] Entry window not open yet (before {config.NO_NEW_TRADES_BEFORE} ET).")
                break
            if is_at_or_after(now_et, config.NO_NEW_TRADES_AFTER):
                _mark_skip("after_entry_window")
                _mark_stage4_reject(reason="after_entry_window", ticker=ticker)
                print(f"[{ts(now_et)}] Entry window closed (past {config.NO_NEW_TRADES_AFTER} ET).")
                break
            if _is_entry_hour_blocked(now_et, strategy_profile=strategy_profile):
                _mark_skip("blocked_entry_hour")
                _mark_stage4_reject(reason="blocked_entry_hour", ticker=ticker)
                print(f"[{ts(now_et)}] {ticker}: skip (hour {now_et.hour:02d}:00 ET blocked by config).")
                continue

            has_ticker_position = any(
                position_matches_ticker(
                    str(getattr(p, "symbol", "")),
                    ticker,
                    getattr(p, "underlying_symbol", None),
                )
                for p in option_positions
            )
            if has_ticker_position:
                _mark_skip("existing_option_position")
                _mark_stage4_reject(reason="existing_option_position", ticker=ticker)
                print(f"[{ts(now_et)}] {ticker}: skip (existing option position).")
                continue
            if _has_ticker_open_meta(ticker):
                _mark_skip("existing_ticker_runtime_state")
                _mark_stage4_reject(reason="existing_ticker_runtime_state", ticker=ticker)
                print(f"[{ts(now_et)}] {ticker}: skip (already open in runtime state).")
                continue
            if dry_run_enabled:
                _mark_skip("dry_run_mode")
                _mark_stage4_reject(reason="dry_run_mode", ticker=ticker)
                print(
                    f"[{ts(now_et)}] DRY-RUN entry candidate: {ticker} {str(direction).upper()} "
                    f"score={float(signal.get('signal_score', 0) or 0):.2f} (no order submitted)."
                )
                continue

            prior_entries = int(ticker_entry_counts.get(ticker, 0))
            max_entries_per_ticker = max(1, int(getattr(config, "MAX_ENTRIES_PER_TICKER_PER_DAY", 1) or 1))
            reentries_used = int(ticker_reentries_used.get(ticker, 0))
            reentry_armed = bool(ticker_reentry_armed.get(ticker, False))
            expected_direction = str(ticker_reentry_expected_direction.get(ticker, "") or "").lower()
            alpaca_buy_order_cap = max(
                0,
                int(getattr(config, "MAX_ALPACA_BUY_ORDERS_PER_TICKER_PER_DAY", 1) or 0),
            )
            active_buy_orders_for_ticker = int(alpaca_active_buy_orders_by_ticker.get(ticker, 0))
            if active_buy_orders_for_ticker > 0:
                _mark_skip("ticker_entry_order_open")
                _mark_stage4_reject(reason="ticker_entry_order_open", ticker=ticker)
                print(
                    f"[{ts(now_et)}] {ticker}: skip "
                    f"({active_buy_orders_for_ticker} open buy order(s) already resting)."
                )
                continue
            alpaca_buy_order_count = int(alpaca_buy_orders_by_ticker.get(ticker, 0))
            if alpaca_buy_order_cap > 0 and alpaca_buy_order_count >= alpaca_buy_order_cap:
                _mark_skip("alpaca_buy_order_ticker_cap")
                _mark_stage4_reject(reason="alpaca_buy_order_ticker_cap", ticker=ticker)
                print(
                    f"[{ts(now_et)}] {ticker}: skip "
                    f"(Alpaca has {alpaca_buy_order_count}/{alpaca_buy_order_cap} "
                    "same-day buy order(s) for this ticker)."
                )
                continue
            if _is_in_opening_strict_window(now_et) and prior_entries >= 1:
                _mark_skip("opening_no_reentry")
                _mark_stage4_reject(reason="opening_no_reentry", ticker=ticker)
                print(f"[{ts(now_et)}] {ticker}: skip (opening window disallows re-entry).")
                continue
            if prior_entries >= max_entries_per_ticker:
                if not reentry_armed:
                    _mark_skip("max_entries_per_ticker_reached")
                    _mark_stage4_reject(reason="max_entries_per_ticker_reached", ticker=ticker)
                    print(
                        f"[{ts(now_et)}] {ticker}: skip (max entries reached "
                        f"{prior_entries}/{max_entries_per_ticker}; no stop-loss re-entry armed)."
                    )
                    continue
            if reentry_armed and prior_entries >= max_entries_per_ticker:
                if reentries_used >= int(config.MAX_REENTRIES_PER_TICKER):
                    _mark_skip("max_reentries_used")
                    _mark_stage4_reject(reason="max_reentries_used", ticker=ticker)
                    print(
                        f"[{ts(now_et)}] {ticker}: skip (max re-entries used "
                        f"{reentries_used}/{int(config.MAX_REENTRIES_PER_TICKER)})."
                    )
                    continue
                if expected_direction in ("call", "put") and direction != expected_direction:
                    _mark_skip("waiting_for_reversal_signal")
                    _mark_stage4_reject(reason="waiting_for_reversal_signal", ticker=ticker)
                    print(
                        f"[{ts(now_et)}] {ticker}: waiting for reversal signal "
                        f"({expected_direction.upper()}); got {direction.upper()}."
                    )
                    continue

            if not _entry_confirmation_passes(data_client, ticker, direction, now_et):
                signal_score = 0.0
                try:
                    signal_score = float(signal.get("signal_score", 0) or 0)
                except (TypeError, ValueError):
                    signal_score = 0.0
                if signal_score >= float(getattr(config, "ENTRY_CONFIRM_BYPASS_MIN_SIGNAL_SCORE", 0.0) or 0.0):
                    print(
                        f"[{ts(now_et)}] {ticker}: entry confirmation bypassed "
                        f"(signal_score={signal_score:.2f} >= {float(config.ENTRY_CONFIRM_BYPASS_MIN_SIGNAL_SCORE):.2f})."
                    )
                else:
                    _mark_skip("entry_confirmation_mismatch")
                    _mark_stage4_reject(reason="entry_confirmation_mismatch", ticker=ticker)
                    print(f"[{ts(now_et)}] {ticker}: skip (entry confirmation candle not aligned).")
                    continue

            # Re-check live position count right before placing a new order.
            option_positions = broker.get_open_option_positions()
            open_count = len(option_positions)
            pending_entry_count = sum(int(v) for v in alpaca_active_buy_orders_by_ticker.values())
            effective_open_count = open_count + pending_entry_count
            if _is_in_opening_strict_window(now_et):
                opening_max_concurrent = max(1, int(getattr(config, "OPENING_MAX_CONCURRENT_POSITIONS", 3) or 3))
                if effective_open_count >= opening_max_concurrent:
                    _mark_skip("opening_concurrent_position_cap")
                    _mark_stage4_reject(reason="opening_concurrent_position_cap", ticker=ticker)
                    print(
                        f"[{ts(now_et)}] Opening concurrent-position cap reached "
                        f"({open_count} open + {pending_entry_count} pending/{opening_max_concurrent})."
                    )
                    break
            if not can_open_new_positions(effective_open_count, config.MAX_POSITIONS):
                _mark_skip("max_positions_reached")
                _mark_stage4_reject(reason="max_positions_reached", ticker=ticker)
                print(
                    f"[{ts(now_et)}] Max positions reached "
                    f"({open_count} open + {pending_entry_count} pending/{config.MAX_POSITIONS}). "
                    "Stopping new entries this loop."
                )
                break

            direction_lc = str(direction or "").lower()
            same_dir_cap = max(1, int(getattr(config, "MAX_SAME_DIRECTION_POSITIONS", 2) or 2))
            call_exposure, put_exposure = _direction_exposure_counts(option_positions, open_trade_meta)
            if direction_lc == "call" and call_exposure >= same_dir_cap:
                _mark_skip("same_direction_exposure_cap")
                _mark_stage4_reject(reason="same_direction_exposure_cap", ticker=ticker)
                print(f"[{ts(now_et)}] {ticker}: skip (call exposure cap {call_exposure}/{same_dir_cap}).")
                continue
            if direction_lc == "put" and put_exposure >= same_dir_cap:
                _mark_skip("same_direction_exposure_cap")
                _mark_stage4_reject(reason="same_direction_exposure_cap", ticker=ticker)
                print(f"[{ts(now_et)}] {ticker}: skip (put exposure cap {put_exposure}/{same_dir_cap}).")
                continue

            # Check both live Alpaca positions AND in-memory open_trade_meta to prevent
            # duplicate entries when Alpaca API returns stale data right after an order fill.
            existing_qty_for_ticker = 0
            for existing_pos in option_positions:
                existing_qty = position_qty_as_int(getattr(existing_pos, "qty", 0))
                if existing_qty <= 0:
                    continue
                existing_ticker = str(getattr(existing_pos, "underlying_symbol", "") or "").upper()
                if not existing_ticker:
                    parsed_ticker, _parsed_direction = _parse_option_symbol(str(getattr(existing_pos, "symbol", "") or ""))
                    existing_ticker = parsed_ticker.upper()
                if existing_ticker == ticker:
                    existing_qty_for_ticker += existing_qty
            # Also check in-memory meta (catches positions placed this same loop iteration)
            if existing_qty_for_ticker == 0:
                for sym, m in open_trade_meta.items():
                    if str(m.get("ticker", "") or "").upper() == ticker:
                        existing_qty_for_ticker += int(m.get("qty", 1) or 1)
                        break
            if existing_qty_for_ticker > 0:
                _mark_skip("ticker_position_already_open")
                _mark_stage4_reject(reason="ticker_position_already_open", ticker=ticker)
                print(
                    f"[{ts(now_et)}] {ticker}: skip (existing open position qty={existing_qty_for_ticker}; "
                    "one position per ticker.)"
                )
                continue

            try:
                print(
                    f"[{ts(now_et)}] {ticker}: scanner signal={direction} "
                    f"profile={str(signal.get('strategy_profile', 'generic') or 'generic')}. "
                    f"{signal.get('reason', '')}"
                )
                signal_strategy_profile = str(signal.get("strategy_profile", "") or "generic")
                signal_entry_max_spread = signal.get("entry_max_quote_spread_pct")
                if adaptive_loss_active:
                    adaptive_spread = float(getattr(config, "ADAPTIVE_LOSS_MAX_SPREAD_PCT", 0.0) or 0.0)
                    if isinstance(adaptive_loss_profile, dict):
                        profile_spread = float(adaptive_loss_profile.get("max_spread_pct", 0.0) or 0.0)
                        if profile_spread > 0:
                            adaptive_spread = min(adaptive_spread or profile_spread, profile_spread)
                    if adaptive_spread > 0:
                        try:
                            current_spread_cap = float(signal_entry_max_spread or adaptive_spread)
                        except (TypeError, ValueError):
                            current_spread_cap = adaptive_spread
                        signal_entry_max_spread = min(current_spread_cap, adaptive_spread)
                volatility_profile = _signal_volatility_profile(signal)
                base_signal_stop_loss_usd = float(signal.get("stop_loss_usd", _runtime_stop_loss_usd()) or _runtime_stop_loss_usd())
                signal_stop_loss_usd = round(
                    max(1.0, base_signal_stop_loss_usd * float(volatility_profile["stop_loss_mult"])),
                    2,
                )
                signal_take_profit_pct = float(
                    signal.get("immediate_take_profit_pct", getattr(config, "IMMEDIATE_TAKE_PROFIT_PCT", 1.0))
                    or getattr(config, "IMMEDIATE_TAKE_PROFIT_PCT", 1.0)
                )
                signal_max_hold_minutes = int(
                    signal.get("max_hold_minutes", getattr(config, "MAX_HOLD_MINUTES", 90))
                    or getattr(config, "MAX_HOLD_MINUTES", 90)
                )

                stock_price = data_client.get_latest_stock_price(ticker)
                if stock_price is None:
                    _mark_skip("no_stock_quote")
                    _mark_stage4_reject(reason="no_stock_quote", ticker=ticker)
                    print(f"[{ts(now_et)}] {ticker}: skip (no stock quote).")
                    time.sleep(config.RATE_LIMIT_SLEEP_SECONDS)
                    continue

                contract, contract_reason = select_atm_option_contract_with_reason(
                    data_client=data_client,
                    underlying_symbol=ticker,
                    direction=direction,
                    underlying_price=stock_price,
                    now_et=now_et,
                )
                if not contract:
                    _mark_skip("no_eligible_option_contract")
                    _mark_stage4_reject(reason="no_eligible_option_contract", ticker=ticker)
                    print(f"[{ts(now_et)}] {ticker}: skip (no eligible option contract: {contract_reason}).")
                    time.sleep(config.RATE_LIMIT_SLEEP_SECONDS)
                    continue

                option_symbol = contract["symbol"]
                if not _option_symbol_matches_direction(option_symbol, direction):
                    _mark_skip("contract_direction_mismatch")
                    _mark_stage4_reject(reason="contract_direction_mismatch", ticker=ticker)
                    print(
                        f"[{ts(now_et)}] {ticker}: skip (contract direction mismatch "
                        f"{option_symbol} vs {direction.upper()})."
                    )
                    continue
                entry_quote = _option_quote_snapshot(data_client, option_symbol)
                spread_ok, spread_reason = _entry_quote_spread_gate(
                    option_symbol=option_symbol,
                    entry_quote=entry_quote,
                    now_et=now_et,
                    strategy_profile=strategy_profile,
                    spread_override_pct=signal_entry_max_spread,
                )
                if not spread_ok:
                    reject_reason = "quote_spread_too_wide" if "spread" in spread_reason else "no_option_ask"
                    _mark_skip(reject_reason)
                    _mark_stage4_reject(reason=reject_reason, ticker=ticker)
                    print(f"[{ts(now_et)}] {ticker}: skip ({spread_reason}).")
                    time.sleep(config.RATE_LIMIT_SLEEP_SECONDS)
                    continue
                ask_price = float(entry_quote.get("ask") or 0.0)

                qty = 1
                trade_premium_usd = ask_price * qty * 100.0
                volatility_premium_mult = max(0.1, float(volatility_profile["premium_cap_mult"]))
                volatility_opening_premium_mult = max(0.1, float(volatility_profile["opening_premium_cap_mult"]))
                max_trade_premium_base = float(getattr(config, "MAX_PREMIUM_PER_TRADE_USD", 150.0) or 150.0)
                max_trade_premium = max(25.0, max_trade_premium_base * volatility_premium_mult)
                premium_override_ok = False
                premium_override_reason = ""
                if trade_premium_usd > max_trade_premium:
                    premium_override_ok, premium_override_reason = _premium_cap_quality_override_ok(
                        signal=signal,
                        entry_quote=entry_quote,
                        now_et=now_et,
                    )
                    if not premium_override_ok:
                        _mark_skip("premium_per_trade_cap")
                        _mark_stage4_reject(reason="premium_per_trade_cap", ticker=ticker)
                        print(
                            f"[{ts(now_et)}] {ticker}: skip (premium ${trade_premium_usd:.2f} > "
                            f"per-trade cap ${max_trade_premium:.2f}; {premium_override_reason})."
                        )
                        continue
                    print(
                        f"[{ts(now_et)}] {ticker}: premium override accepted "
                        f"(${trade_premium_usd:.2f} > ${max_trade_premium:.2f}; {premium_override_reason})."
                    )

                total_open_premium = _current_open_premium_usd(option_positions, open_trade_meta)
                committed_open_premium = total_open_premium + float(active_entry_order_premium_usd or 0.0)
                max_total_open_premium_base = float(getattr(config, "MAX_TOTAL_OPEN_PREMIUM_USD", 600.0) or 600.0)
                max_total_open_premium = max(75.0, max_total_open_premium_base * volatility_premium_mult)
                if (committed_open_premium + trade_premium_usd) > max_total_open_premium:
                    _mark_skip("total_open_premium_cap")
                    _mark_stage4_reject(reason="total_open_premium_cap", ticker=ticker)
                    print(
                        f"[{ts(now_et)}] {ticker}: skip (open premium ${total_open_premium:.2f} + "
                        f"pending ${active_entry_order_premium_usd:.2f} + "
                        f"new ${trade_premium_usd:.2f} > cap ${max_total_open_premium:.2f})."
                    )
                    continue

                expensive_symbols = set(str(s).upper() for s in getattr(config, "EXPENSIVE_PREMIUM_SYMBOLS", ()))
                is_expensive_symbol = ticker in expensive_symbols
                if _is_in_opening_strict_window(now_et):
                    opening_premium_cap_base = float(getattr(config, "OPENING_MAX_FRESH_PREMIUM_USD", 300.0) or 300.0)
                    opening_premium_cap = max(75.0, opening_premium_cap_base * volatility_opening_premium_mult)

                    # In opening strict mode, expensive names are blocked unless
                    # they are core names or fit an extra-tight premium budget.
                    if is_expensive_symbol:
                        core_set = set(str(s).upper() for s in getattr(config, "PREFERRED_CORE_TICKERS", ()))
                        tight_opening_expensive_premium = float(
                            getattr(config, "OPENING_EXPENSIVE_MAX_PREMIUM_USD", max_trade_premium) or max_trade_premium
                        )
                        is_core_name = ticker in core_set
                        if not is_core_name and trade_premium_usd > tight_opening_expensive_premium:
                            _mark_skip("opening_expensive_name_gate")
                            _mark_stage4_reject(reason="opening_expensive_name_gate", ticker=ticker)
                            print(
                                f"[{ts(now_et)}] {ticker}: skip (opening expensive-name gate; premium ${trade_premium_usd:.2f} "
                                f"> ${tight_opening_expensive_premium:.2f} and not core)."
                            )
                            continue

                    if (opening_fresh_premium_deployed_usd + trade_premium_usd) > opening_premium_cap:
                        if not premium_override_ok:
                            _mark_skip("opening_fresh_premium_cap")
                            _mark_stage4_reject(reason="opening_fresh_premium_cap", ticker=ticker)
                            print(
                                f"[{ts(now_et)}] {ticker}: skip (opening premium ${opening_fresh_premium_deployed_usd:.2f} + "
                                f"${trade_premium_usd:.2f} > cap ${opening_premium_cap:.2f})."
                            )
                            continue
                        print(
                            f"[{ts(now_et)}] {ticker}: opening premium cap override accepted "
                            f"(${opening_fresh_premium_deployed_usd + trade_premium_usd:.2f} > ${opening_premium_cap:.2f})."
                        )
                    opening_expensive_cap = max(0, int(getattr(config, "OPENING_MAX_EXPENSIVE_ENTRIES", 1)))
                    if is_expensive_symbol and opening_expensive_entries_today_count >= opening_expensive_cap:
                        if not premium_override_ok:
                            _mark_skip("opening_expensive_symbol_cap")
                            _mark_stage4_reject(reason="opening_expensive_symbol_cap", ticker=ticker)
                            print(
                                f"[{ts(now_et)}] {ticker}: skip (opening expensive-name cap "
                                f"{opening_expensive_entries_today_count}/{opening_expensive_cap})."
                            )
                            continue
                        print(
                            f"[{ts(now_et)}] {ticker}: opening expensive-name cap override accepted "
                            f"({opening_expensive_entries_today_count}/{opening_expensive_cap})."
                        )

                if str(volatility_profile.get("label", "normal")) != "normal":
                    iv_rank_text = "n/a"
                    if int(volatility_profile.get("iv_available", 0) or 0):
                        iv_rank_text = f"{float(volatility_profile.get('iv_rank', 0.0) or 0.0):.1f}"
                    print(
                        f"[{ts(now_et)}] {ticker}: volatility={volatility_profile.get('label')} "
                        f"(score={int(volatility_profile.get('score', 0) or 0)}, "
                        f"atr={float(volatility_profile.get('atr_pct', 0.0) or 0.0):.2f}%, "
                        f"rvol={float(volatility_profile.get('rvol', 0.0) or 0.0):.2f}, ivr={iv_rank_text}) "
                        f"-> stop=${signal_stop_loss_usd:.2f}, trade cap=${max_trade_premium:.2f}."
                    )

                initial_chain_ask = float(contract.get("ask_price", ask_price) or ask_price)
                pre_submit_slippage = _slippage_pct(initial_chain_ask, ask_price)
                if pre_submit_slippage > config.MAX_ENTRY_SLIPPAGE_PCT:
                    retry_quote = _option_quote_snapshot(data_client, option_symbol)
                    retry_ok, retry_reason = _entry_quote_spread_gate(
                        option_symbol=option_symbol,
                        entry_quote=retry_quote,
                        now_et=now_et,
                        strategy_profile=strategy_profile,
                        spread_override_pct=signal_entry_max_spread,
                    )
                    retry_ask = float(retry_quote.get("ask") or 0.0)
                    if retry_ok and retry_ask > 0:
                        entry_quote = retry_quote
                        ask_price = retry_ask
                        pre_submit_slippage = _slippage_pct(initial_chain_ask, ask_price)
                    elif not retry_ok:
                        reject_reason = "quote_spread_too_wide" if "spread" in retry_reason else "no_option_ask"
                        _mark_skip(reject_reason)
                        _mark_stage4_reject(reason=reject_reason, ticker=ticker)
                        print(f"[{ts(now_et)}] {ticker}: skip ({retry_reason}).")
                        time.sleep(config.RATE_LIMIT_SLEEP_SECONDS)
                        continue
                if pre_submit_slippage > (config.MAX_ENTRY_SLIPPAGE_PCT * 3):
                    _mark_skip("entry_slippage_too_high")
                    _mark_stage4_reject(reason="entry_slippage_too_high", ticker=ticker)
                    print(
                        f"[{ts(now_et)}] {ticker}: skip (entry slippage {pre_submit_slippage:.2f}% > "
                        f"hard cap {(config.MAX_ENTRY_SLIPPAGE_PCT * 3):.2f}%)."
                    )
                    time.sleep(config.RATE_LIMIT_SLEEP_SECONDS)
                    continue

                history_ok, history_reason = _pre_execution_history_check(signal, ticker, direction, now_et)
                if not history_ok:
                    _mark_skip("pre_execution_history_check")
                    _mark_stage4_reject(reason="pre_execution_history_check", ticker=ticker, detail=history_reason)
                    print(f"[{ts(now_et)}] {ticker}: skip ({history_reason}).")
                    continue
                if history_reason:
                    signal = dict(signal)
                    signal["pre_execution_history_check"] = history_reason
                    print(f"[{ts(now_et)}] {ticker}: {history_reason}.")

                _mark_stage4_eligible(ticker=ticker)
                entry_attempts_loop += 1
                if _is_in_opening_strict_window(now_et):
                    opening_entry_attempts_loop += 1
                entry_result = _execute_limit_entry(
                    broker=broker,
                    data_client=data_client,
                    option_symbol=option_symbol,
                    qty=qty,
                    now_et=now_et,
                    label=f"ENTRY {ticker}",
                    initial_quote=entry_quote,
                )
                entry_debug["entry_orders_submitted"] = int(entry_debug.get("entry_orders_submitted", 0)) + int(entry_result.get("attempts", 0) or 0)
                _set_signal_outcome(
                    ticker=ticker,
                    disposition="order_submitted",
                    detail=str(entry_result.get("status", "submitted") or "submitted"),
                )
                if not bool(entry_result.get("filled", False)):
                    if bool(entry_result.get("pending_open", False)):
                        alpaca_active_buy_orders_by_ticker[ticker] = (
                            int(alpaca_active_buy_orders_by_ticker.get(ticker, 0)) + 1
                        )
                        _mark_skip("entry_order_resting")
                        _set_signal_outcome(
                            ticker=ticker,
                            disposition="order_resting",
                            detail=str(entry_result.get("status", "pending_open") or "pending_open"),
                        )
                        print(
                            f"[{ts(now_et)}] {ticker}: entry limit order resting "
                            f"(status={entry_result.get('status', 'pending_open')}, "
                            f"order={entry_result.get('order_id', '')})."
                        )
                        _save_runtime_state()
                        break
                    _mark_skip("entry_not_filled_after_retry")
                    _set_signal_outcome(
                        ticker=ticker,
                        disposition="order_not_filled",
                        detail=str(entry_result.get("status", "unknown") or "unknown"),
                    )
                    print(
                        f"[{ts(now_et)}] {ticker}: entry not filled "
                        f"(status={entry_result.get('status', 'unknown')}). Skipping."
                    )
                    time.sleep(config.RATE_LIMIT_SLEEP_SECONDS)
                    continue

                filled_avg_price = float(entry_result.get("filled_price") or 0.0)
                filled_qty = position_qty_as_int(entry_result.get("filled_qty", qty)) or qty
                if filled_qty > 1:
                    extra_qty = filled_qty - 1
                    try:
                        trim_order = broker.close_option_market(option_symbol, extra_qty)
                        print(f"[{ts(now_et)}] {ticker}: trimmed fill to 1 contract (closed extra {extra_qty}).")
                    except Exception as exc:  # noqa: BLE001
                        print(f"[{ts(now_et)}] {ticker}: WARNING — failed to trim extra qty {extra_qty}: {exc}. Recording qty=1 anyway.")
                    filled_qty = 1
                fill_slippage = float(entry_result.get("fill_slippage_vs_ask_pct", 0.0) or 0.0)
                if fill_slippage > config.MAX_FILL_SLIPPAGE_PCT:
                    _mark_skip("fill_slippage_too_high")
                    _record_bad_fill_event(ticker, now_et, fill_slippage)
                    print(
                        f"[{ts(now_et)}] {ticker}: fill slippage {fill_slippage:.2f}% exceeds "
                        f"{config.MAX_FILL_SLIPPAGE_PCT:.2f}%. Closing immediately."
                    )
                    alerts.send(
                        "high_fill_slippage",
                        (
                            f"High fill slippage on {option_symbol}: {fill_slippage:.2f}% "
                            f"(limit {config.MAX_FILL_SLIPPAGE_PCT:.2f}%). Position closed."
                        ),
                        level="warning",
                        dedupe_key=f"slippage-{option_symbol}-{now_et.strftime('%Y%m%d%H%M')}",
                    )
                    try:
                        broker.close_option_market(option_symbol, filled_qty)
                    except Exception as exc:  # noqa: BLE001
                        print(f"[{ts(now_et)}] {ticker}: immediate slippage close failed: {exc}")
                    time.sleep(config.RATE_LIMIT_SLEEP_SECONDS)
                    continue

                pattern_signature, pattern_key, pattern_global_key = _signal_pattern_keys(signal, ticker, now_et=now_et)
                open_trade_meta[option_symbol] = {
                    "timestamp": ts(now_et),
                    "entry_time_iso": now_et.isoformat(),
                    "strategy_profile": signal_strategy_profile,
                    "ticker": ticker,
                    "direction": direction,
                    "option_symbol": option_symbol,
                    "strike": contract.get("strike_price", ""),
                    "expiry": contract.get("expiration_date", ""),
                    "qty": filled_qty,
                    "entry_price": filled_avg_price or ask_price,
                    "signal_score": round(float(signal.get("signal_score", 0.0) or 0.0), 4),
                    "direction_score": round(float(signal.get("direction_score", 0.0) or 0.0), 4),
                    "rvol": round(float(signal.get("rvol", 0.0) or 0.0), 4),
                    "rsi": round(float(signal.get("rsi", 0.0) or 0.0), 4),
                    "roc": round(float(signal.get("roc", 0.0) or 0.0), 4),
                    "iv_rank": round(float(signal.get("iv_rank", 0.0) or 0.0), 4),
                    "price": round(float(signal.get("price", 0.0) or 0.0), 4),
                    "vwap": round(float(signal.get("vwap", 0.0) or 0.0), 4),
                    "flow_score": round(float(signal.get("flow_score", 0.0) or 0.0), 4),
                    "regime_score": round(float(signal.get("regime_score", 0.0) or 0.0), 4),
                    "contract_spread_pct": round(float(entry_result.get("submit_spread_pct", 0.0) or 0.0), 4),
                    "atr_pct": round(float(volatility_profile.get("atr_pct", 0.0) or 0.0), 4),
                    "volatility_regime": str(volatility_profile.get("label", "normal") or "normal"),
                    "volatility_score": int(volatility_profile.get("score", 0) or 0),
                    "signal_pattern_signature": pattern_signature,
                    "signal_pattern_key": pattern_key,
                    "signal_pattern_global_key": pattern_global_key,
                    "pattern_direction_override": signal.get("pattern_direction_override", ""),
                    "pattern_direction_override_reason": signal.get("pattern_direction_override_reason", ""),
                    "pre_execution_history_check": signal.get("pre_execution_history_check", ""),
                    "volatility_stop_loss_mult": round(float(volatility_profile.get("stop_loss_mult", 1.0) or 1.0), 4),
                    "volatility_premium_cap_mult": round(float(volatility_profile.get("premium_cap_mult", 1.0) or 1.0), 4),
                    "entry_bid_submit": entry_result.get("submit_bid"),
                    "entry_ask_submit": entry_result.get("submit_ask"),
                    "entry_midpoint_submit": entry_result.get("submit_midpoint"),
                    "entry_intended_limit": entry_result.get("intended_limit"),
                    "entry_filled_price": filled_avg_price or ask_price,
                    "entry_spread_pct": entry_result.get("submit_spread_pct"),
                    "entry_fill_slippage_vs_ask_pct": entry_result.get("fill_slippage_vs_ask_pct"),
                    "entry_fill_seconds": entry_result.get("fill_seconds"),
                    "entry_attempts": entry_result.get("attempts", 0),
                    "stop_floor_plpc": -float(config.STOP_LOSS_PCT),
                    "stop_loss_usd": signal_stop_loss_usd,
                    "immediate_take_profit_pct": signal_take_profit_pct,
                    "max_hold_minutes": signal_max_hold_minutes,
                    "trade_state": "unproven",
                    "runner_mode": False,
                    "max_plpc": 0.0,
                    "min_plpc": 0.0,
                }
                if prior_entries >= 1 and reentry_armed:
                    ticker_reentries_used[ticker] = reentries_used + 1
                    ticker_reentry_armed[ticker] = False
                    ticker_reentry_expected_direction[ticker] = ""
                ticker_entry_counts[ticker] = prior_entries + 1
                open_count += 1
                entry_times_rolling.append(now_et)
                if _is_in_opening_strict_window(now_et):
                    opening_entries_today_count += 1
                    entry_premium_usd = float((filled_avg_price or ask_price) * filled_qty * 100.0)
                    opening_fresh_premium_deployed_usd += entry_premium_usd
                    if ticker in set(str(s).upper() for s in getattr(config, "EXPENSIVE_PREMIUM_SYMBOLS", ())):
                        opening_expensive_entries_today_count += 1
                if ticker not in set(str(s).upper() for s in getattr(config, "PREFERRED_CORE_TICKERS", ())):
                    non_core_entries_today_count += 1
                entry_debug["entries_filled"] = int(entry_debug.get("entries_filled", 0)) + 1
                _set_signal_outcome(ticker=ticker, disposition="order_filled")
                _save_runtime_state()
                time.sleep(config.RATE_LIMIT_SLEEP_SECONDS)
            except Exception as exc:  # noqa: BLE001
                _mark_skip("entry_flow_exception")
                _mark_stage4_reject(reason="entry_flow_exception", ticker=ticker)
                _record_entry_exception(ticker, exc)
                print(
                    f"[{ts(now_et)}] {ticker}: error during entry flow "
                    f"({type(exc).__name__}): {exc!r}"
                )
                time.sleep(config.RATE_LIMIT_SLEEP_SECONDS)

        last_entry_debug = entry_debug
        _save_runtime_state()

        # --- Exit management ---
        option_positions = broker.get_open_option_positions()
        hydrated_count = _hydrate_missing_position_meta(open_trade_meta, option_positions, now_et)
        if hydrated_count > 0:
            print(f"[{ts(now_et)}] Hydrated {hydrated_count} externally-opened position(s) before exit management.")
            _save_runtime_state()
        ticker_total_qty: dict[str, int] = {}
        ticker_first_symbol: dict[str, str] = {}
        for p in option_positions:
            p_symbol = str(getattr(p, "symbol", "") or "")
            p_qty = position_qty_as_int(getattr(p, "qty", 0))
            if p_qty <= 0:
                continue
            p_meta = open_trade_meta.get(p_symbol, {})
            p_ticker = str(p_meta.get("ticker", "") or "").upper()
            if not p_ticker:
                parsed_ticker, _parsed_dir = _parse_option_symbol(p_symbol)
                p_ticker = parsed_ticker.upper()
            if not p_ticker:
                continue
            ticker_total_qty[p_ticker] = int(ticker_total_qty.get(p_ticker, 0)) + p_qty
            if p_ticker not in ticker_first_symbol:
                ticker_first_symbol[p_ticker] = p_symbol
        for pos in option_positions:
            now_et = datetime.now(tz)
            _touch_heartbeat()
            symbol = str(getattr(pos, "symbol", ""))
            qty = position_qty_as_int(getattr(pos, "qty", 0))
            if qty <= 0:
                continue

            meta = open_trade_meta.get(symbol, {})
            entry_price_for_monitor = float(meta.get("entry_price", getattr(pos, "avg_entry_price", 0) or 0) or 0)
            live_mark_price, live_plpc = _live_option_mark_and_plpc(
                data_client=data_client,
                option_symbol=symbol,
                entry_price=entry_price_for_monitor,
            )

            # Calculate P&L from the best available live source.
            plpc = _position_plpc_snapshot(pos)
            if plpc is None:
                plpc = 0.0
            # Override with live quote if available (more real-time than position snapshot).
            if live_plpc is not None and math.isfinite(float(live_plpc)):
                plpc = float(live_plpc)

            unrealized_usd: float | None = None
            if live_mark_price is not None and live_mark_price > 0 and entry_price_for_monitor > 0:
                unrealized_usd = (float(live_mark_price) - float(entry_price_for_monitor)) * qty * 100.0
            else:
                try:
                    pl_raw = float(getattr(pos, "unrealized_pl", 0) or 0)
                    if math.isfinite(pl_raw):
                        unrealized_usd = pl_raw
                except (TypeError, ValueError):
                    unrealized_usd = None
                if unrealized_usd is None and entry_price_for_monitor > 0:
                    unrealized_usd = float(plpc) * float(entry_price_for_monitor) * qty * 100.0

            # --- Exit strategy ---
            # 1. STOP LOSS: exit immediately at/through the per-trade USD loss cap.
            # 2. STATE TRANSITIONS: unproven -> protected -> bank_or_qualify -> runner.
            # 3. REVERSAL EXIT: in protected/runner states, exit on confirmed reversal.

            if meta:
                meta["max_plpc"] = max(float(meta.get("max_plpc", plpc) or plpc), plpc)
                meta["min_plpc"] = min(float(meta.get("min_plpc", plpc) or plpc), plpc)
                # Legacy meta compatibility: ensure a default state exists.
                if not str(meta.get("trade_state", "") or "").strip():
                    meta["trade_state"] = "runner" if bool(meta.get("runner_mode")) else "unproven"
                open_trade_meta[symbol] = meta

            history_rows = open_position_pl_history.get(symbol)
            if not isinstance(history_rows, list):
                history_rows = []
            history_rows.append(
                {
                    "ts": now_et.isoformat(),
                    "plpc": round(float(plpc) * 100.0, 4),
                    "mark": round(float(live_mark_price), 6) if live_mark_price is not None else None,
                }
            )
            # Keep only the most recent ~3 hours at 15s loop cadence.
            open_position_pl_history[symbol] = history_rows[-800:]

            exit_reason = None
            close_qty = qty
            ticker_for_pos = str(meta.get("ticker", "") or "").upper()
            entry_time = _parse_trade_meta_entry_time(meta) if meta else None
            if not ticker_for_pos:
                parsed_ticker, _parsed_dir = _parse_option_symbol(symbol)
                ticker_for_pos = parsed_ticker.upper()
            if ticker_for_pos:
                total_qty_for_ticker = int(ticker_total_qty.get(ticker_for_pos, qty))
                keep_symbol = str(ticker_first_symbol.get(ticker_for_pos, symbol))
                if total_qty_for_ticker > 1:
                    if symbol != keep_symbol:
                        exit_reason = "exposure_normalize"
                    elif qty > 1:
                        exit_reason = "exposure_normalize"
                        close_qty = qty - 1
            if exit_reason is None and should_force_same_day_exit(entry_time, now_et):
                exit_reason = "overnight_forced_close"
            if exit_reason is None:
                expiry_date = _resolve_option_expiry(symbol, meta)
                if expiry_date is not None:
                    cutoff_date = _subtract_trading_days(
                        expiry_date,
                        int(config.OPTION_FORCE_EXIT_DAYS_BEFORE_EXPIRY),
                    )
                    if now_et.date() > cutoff_date:
                        exit_reason = "pre_expiry_exit_overdue"
                    elif now_et.date() == cutoff_date and is_at_or_after(now_et, config.OPTION_EXPIRY_EXIT_TIME):
                        exit_reason = "pre_expiry_exit"
            if exit_reason is None and is_at_or_after(now_et, config.HARD_CLOSE_TIME):
                exit_reason = "eod_close"
            if exit_reason is None and _is_in_anti_churn_window(entry_time, now_et):
                held_seconds = max(0, int((now_et - entry_time).total_seconds())) if entry_time else 0
                min_hold_minutes = float(getattr(config, "ANTI_CHURN_HOLD_MINUTES", 10) or 10)
                last_exit_debug = {
                    "loop_ts_et": ts(now_et),
                    "symbol": symbol,
                    "reason": "minimum_hold_active",
                    "held_seconds": held_seconds,
                    "min_hold_minutes": min_hold_minutes,
                    "result": "strategy_deferred_minimum_hold",
                }
                print(
                    f"[{ts(now_et)}] {symbol}: minimum hold active "
                    f"({held_seconds / 60.0:.1f}/{min_hold_minutes:.1f}m); "
                    "strategy exits deferred."
                )
                _save_runtime_state()
                continue
            # Rule 1: fixed-dollar stop loss
            stop_loss_usd_cap = float(meta.get("stop_loss_usd", _runtime_stop_loss_usd()) or _runtime_stop_loss_usd())
            if exit_reason is None and should_trigger_stop_loss(unrealized_usd, stop_loss_usd_cap):
                exit_reason = "stop_loss"
            stop_loss_pct_cap = abs(float(getattr(config, "STOP_LOSS_PCT", 0.0) or 0.0))
            if exit_reason is None and stop_loss_pct_cap > 0 and plpc <= -stop_loss_pct_cap:
                exit_reason = "stop_loss_pct"

            profit_targets = []
            immediate_take_profit_pct = _safe_signal_float(
                meta.get("immediate_take_profit_pct", getattr(config, "IMMEDIATE_TAKE_PROFIT_PCT", 0.0)),
                0.0,
            )
            if immediate_take_profit_pct > 0:
                profit_targets.append(immediate_take_profit_pct)
            if bool(getattr(config, "ENABLE_FIXED_PROFIT_TARGET", False)):
                fixed_profit_target_pct = _safe_signal_float(
                    getattr(config, "PROFIT_TARGET_PCT", 0.0),
                    0.0,
                )
                if fixed_profit_target_pct > 0:
                    profit_targets.append(fixed_profit_target_pct)
            profit_target_pct = min(profit_targets) if profit_targets else 0.0
            if exit_reason is None and profit_target_pct > 0 and plpc >= profit_target_pct:
                exit_reason = "profit_target"

            # --- Stateful profit management ---
            trade_state = _trade_state_from_meta(meta)

            protect_trigger = float(getattr(config, "TRADE_STATE_PROTECT_TRIGGER_PCT", 0.03) or 0.03)
            protect_floor = float(getattr(config, "TRADE_STATE_PROTECTED_STOP_FLOOR_PCT", 0.001) or 0.001)
            bank_trigger = float(getattr(config, "TRADE_STATE_BANK_OR_QUALIFY_TRIGGER_PCT", 0.08) or 0.08)

            if exit_reason is None and trade_state == "unproven" and plpc >= protect_trigger:
                current_floor = float(meta.get("stop_floor_plpc", -float(config.STOP_LOSS_PCT)) or -float(config.STOP_LOSS_PCT))
                meta["stop_floor_plpc"] = max(current_floor, protect_floor)
                meta["trade_state"] = "protected"
                trade_state = "protected"
                print(
                    f"[{ts(now_et)}] {symbol}: promoted to protected state at {plpc:+.2%}; "
                    f"floor={float(meta.get('stop_floor_plpc', 0.0)):+.2%}."
                )

            if exit_reason is None and trade_state in {"protected", "bank_or_qualify", "runner"}:
                floor_plpc = float(meta.get("stop_floor_plpc", -float(config.STOP_LOSS_PCT)) or -float(config.STOP_LOSS_PCT))
                if plpc <= floor_plpc:
                    exit_reason = "protected_floor_breach"

            # Stage 3: bank-or-qualify at meaningful green threshold.
            if (
                exit_reason is None
                and trade_state in {"protected", "bank_or_qualify"}
                and plpc >= bank_trigger
                and not _is_in_anti_churn_window(entry_time, now_et)
            ):
                meta["trade_state"] = "bank_or_qualify"
                trade_state = "bank_or_qualify"
                ticker_for_eligibility = ticker_for_pos or ""
                if (
                    ticker_for_eligibility
                    and not _runner_near_close_blocked(now_et)
                    and _is_runner_eligible(symbol, ticker_for_eligibility, meta, data_client, now_et)
                ):
                    meta["runner_mode"] = True
                    meta["trade_state"] = "runner"
                    runner_floor = float(getattr(config, "TRADE_STATE_RUNNER_PROMOTION_STOP_FLOOR_PCT", 0.03) or 0.03)
                    current_floor = float(meta.get("stop_floor_plpc", 0.0) or 0.0)
                    meta["stop_floor_plpc"] = max(current_floor, runner_floor)
                    trade_state = "runner"
                    print(f"[{ts(now_et)}] RUNNER ELIGIBLE: {symbol} at +{plpc:.2%}, promoted to runner mode")
                else:
                    exit_reason = "base_win_bank"
                    print(f"[{ts(now_et)}] BASE WIN BANK: {symbol} at +{plpc:.2%}, not runner eligible, taking profit")

            # --- Reversal detection exit ---
            # Reversal logic is a protected/runner-state manager (not an early scalp trigger).
            reversal_min_profit_pct = float(getattr(config, "REVERSAL_EXIT_MIN_PROFIT_PCT", 0.06) or 0.06)
            runner_reversal_min_profit_pct = float(
                getattr(config, "RUNNER_REVERSAL_EXIT_MIN_PROFIT_PCT", reversal_min_profit_pct)
                or reversal_min_profit_pct
            )
            active_reversal_threshold = runner_reversal_min_profit_pct if trade_state == "runner" else reversal_min_profit_pct
            if (
                exit_reason is None
                and bool(getattr(config, "ENABLE_REVERSAL_EXIT", True))
                and trade_state in {"protected", "bank_or_qualify", "runner"}
                and not _is_in_anti_churn_window(entry_time, now_et)
                and plpc >= active_reversal_threshold
                and ticker_for_pos
            ):
                try:
                    rev_bars = data_client.get_intraday_bars_since_open(
                        symbol=ticker_for_pos, now_et=now_et, limit=12
                    )
                    if rev_bars is not None and len(rev_bars) >= 5:
                        rev_closes = rev_bars["close"].astype(float)
                        trade_direction = str(meta.get("direction", "") or "").lower()

                        # Signal 1: EMA9 crosses against trade direction
                        rev_ema9 = rev_closes.ewm(span=9, adjust=False).mean()
                        rev_ema21 = rev_closes.ewm(span=21, adjust=False).mean()
                        ema_reversed = False
                        if len(rev_ema9) >= 3 and len(rev_ema21) >= 3:
                            if trade_direction == "call" and rev_ema9.iloc[-1] < rev_ema21.iloc[-1]:
                                ema_reversed = True
                            elif trade_direction == "put" and rev_ema9.iloc[-1] > rev_ema21.iloc[-1]:
                                ema_reversed = True

                        # Signal 2: Last 2 bars moving against trade direction
                        last2_roc = 0.0
                        if len(rev_closes) >= 3:
                            prev2 = float(rev_closes.iloc[-3])
                            curr = float(rev_closes.iloc[-1])
                            last2_roc = (curr - prev2) / prev2 * 100 if prev2 != 0 else 0.0
                        roc_reversed = False
                        reversal_roc_threshold = float(getattr(config, "REVERSAL_ROC_THRESHOLD_PCT", 0.3))
                        if trade_direction == "call" and last2_roc <= -reversal_roc_threshold:
                            roc_reversed = True
                        elif trade_direction == "put" and last2_roc >= reversal_roc_threshold:
                            roc_reversed = True

                        # Signal 3: Price crossed back through VWAP
                        from scanner import calculate_vwap
                        rev_vwap = calculate_vwap(rev_bars)
                        vwap_flipped = False
                        if rev_vwap and not (rev_vwap != rev_vwap):  # not nan
                            curr_price = float(rev_closes.iloc[-1])
                            if trade_direction == "call" and curr_price < rev_vwap:
                                vwap_flipped = True
                            elif trade_direction == "put" and curr_price > rev_vwap:
                                vwap_flipped = True

                        # Require at least 2 of 3 reversal signals to confirm
                        reversal_signals = sum([ema_reversed, roc_reversed, vwap_flipped])
                        reversal_confirm = int(getattr(config, "REVERSAL_CONFIRM_SIGNALS", 2))
                        if reversal_signals >= reversal_confirm:
                            exit_reason = f"reversal_detected(ema={int(ema_reversed)},roc={int(roc_reversed)},vwap={int(vwap_flipped)})"
                            print(
                                f"[{ts(now_et)}] REVERSAL EXIT {ticker_for_pos} {trade_direction} "
                                f"plpc={plpc:+.2%} signals={reversal_signals}/3 "
                                f"[ema={ema_reversed} roc={roc_reversed}({last2_roc:+.2f}%) vwap={vwap_flipped}]"
                            )
                except Exception as _rev_exc:  # noqa: BLE001
                    pass  # reversal check is best-effort; never block exit management
            if exit_reason is None:
                expiry_date = _resolve_option_expiry(symbol, meta)
                if expiry_date is not None:
                    cutoff_date = _subtract_trading_days(
                        expiry_date,
                        int(config.OPTION_FORCE_EXIT_DAYS_BEFORE_EXPIRY),
                    )
                    if now_et.date() > cutoff_date:
                        exit_reason = "pre_expiry_exit_overdue"
                    elif now_et.date() == cutoff_date and is_at_or_after(now_et, config.OPTION_EXPIRY_EXIT_TIME):
                        exit_reason = "pre_expiry_exit"
            if exit_reason is None and is_at_or_after(now_et, config.HARD_CLOSE_TIME):
                exit_reason = "eod_close"
            if exit_reason is None:
                if entry_time is not None:
                    held_minutes = int((now_et - entry_time).total_seconds() // 60)
                    max_hold_minutes = int(meta.get("max_hold_minutes", config.MAX_HOLD_MINUTES) or config.MAX_HOLD_MINUTES)
                    if held_minutes >= max_hold_minutes:
                        exit_reason = "time_stop"

            if exit_reason:
                if dry_run_enabled:
                    print(
                        f"[{ts(now_et)}] DRY-RUN exit candidate: {symbol} reason={exit_reason} "
                        f"qty={close_qty}/{qty} plpc={plpc:+.2%} unrealized_usd={unrealized_usd if unrealized_usd is not None else 'n/a'}"
                    )
                    last_exit_debug = {
                        "loop_ts_et": ts(now_et),
                        "symbol": symbol,
                        "reason": f"dry_run_{exit_reason}",
                        "requested_qty": close_qty,
                        "position_qty": qty,
                        "filled_qty": 0,
                        "result": "dry_run_skipped",
                    }
                    _save_runtime_state()
                    continue
                try:
                    last_exit_debug = {
                        "loop_ts_et": ts(now_et),
                        "symbol": symbol,
                        "reason": exit_reason,
                        "requested_qty": close_qty,
                        "position_qty": qty,
                        "filled_qty": 0,
                        "plpc_used": round(plpc, 6),
                        "unrealized_usd_used": round(float(unrealized_usd), 4) if unrealized_usd is not None else None,
                        "quote_mark_price": round(float(live_mark_price), 6) if live_mark_price else None,
                        "result": "submitted",
                    }
                    close_poll_override = None
                    close_wait_override = None
                    close_retry_override = None
                    if exit_reason in {"stop_loss", "stop_loss_pct"}:
                        close_poll_override = int(
                            getattr(config, "STOPLOSS_EXIT_ORDER_STATUS_POLL_SECONDS", 1) or 1
                        )
                        close_wait_override = int(
                            getattr(config, "STOPLOSS_EXIT_ORDER_MAX_WAIT_SECONDS", 3) or 3
                        )
                        close_retry_override = int(
                            getattr(config, "STOPLOSS_EXIT_CLOSE_RETRY_ATTEMPTS", 1) or 1
                        )
                    filled_close_qty, close_fill_price, close_execution = _close_position_with_confirmation(
                        symbol=symbol,
                        qty=close_qty,
                        now_et=now_et,
                        label=f"EXIT {exit_reason}",
                        exit_reason=exit_reason,
                        poll_seconds_override=close_poll_override,
                        max_wait_seconds_override=close_wait_override,
                        retry_attempts_override=close_retry_override,
                    )
                    if filled_close_qty <= 0 or close_fill_price is None or close_fill_price <= 0:
                        last_exit_debug["result"] = "pending_or_not_filled"
                        _save_runtime_state()
                        continue
                    last_exit_debug["filled_qty"] = filled_close_qty
                    last_exit_debug["result"] = "filled"
                    meta = open_trade_meta.get(symbol, {})
                    entry_price = float(meta.get("entry_price", getattr(pos, "avg_entry_price", 0) or 0))
                    exit_price = float(close_fill_price)
                    realized_plpc = 0.0
                    if entry_price > 0 and exit_price > 0:
                        realized_plpc = (exit_price - entry_price) / entry_price
                    trade_pnl_usd = (exit_price - entry_price) * filled_close_qty * 100
                    hold_seconds = 0
                    if entry_time is not None:
                        hold_seconds = max(0, int((now_et - entry_time).total_seconds()))
                    conservative_pnl_usd, conservative_pnl_pct = _conservative_executable_pnl(
                        entry_ask_price=meta.get("entry_ask_submit"),
                        exit_bid_price=close_execution.get("submit_bid"),
                        qty=filled_close_qty,
                    )
                    paper_reported_pnl_usd = round(trade_pnl_usd, 2)
                    paper_reported_pnl_pct = round(realized_plpc * 100.0, 4)
                    max_favorable_excursion_pct = round(float(meta.get("max_plpc", 0.0) or 0.0) * 100.0, 4)
                    max_adverse_excursion_pct = round(float(meta.get("min_plpc", 0.0) or 0.0) * 100.0, 4)
                    if trade_telemetry_day != now_et.date().isoformat():
                        trade_telemetry_day = now_et.date().isoformat()
                        trade_telemetry_closed_count = 0
                        trade_telemetry_total_pnl_usd = 0.0
                        trade_telemetry_last_close_iso = ""
                        trade_telemetry_last_log_error = ""

                    trade_row = {
                        "timestamp": ts(now_et),
                        "date": now_et.date().isoformat(),
                        "ticker": meta.get("ticker", ""),
                        "direction": meta.get("direction", ""),
                        "strategy_profile": meta.get("strategy_profile", ""),
                        "option_symbol": symbol,
                        "strike": meta.get("strike", ""),
                        "expiry": meta.get("expiry", ""),
                        "qty": filled_close_qty,
                        "signal_score": meta.get("signal_score", ""),
                        "direction_score": meta.get("direction_score", ""),
                        "rvol": meta.get("rvol", ""),
                        "rsi": meta.get("rsi", ""),
                        "roc": meta.get("roc", ""),
                        "iv_rank": meta.get("iv_rank", ""),
                        "price": meta.get("price", ""),
                        "vwap": meta.get("vwap", ""),
                        "flow_score": meta.get("flow_score", ""),
                        "regime_score": meta.get("regime_score", ""),
                        "contract_spread_pct": meta.get("contract_spread_pct", ""),
                        "atr_pct": meta.get("atr_pct", ""),
                        "volatility_score": meta.get("volatility_score", ""),
                        "signal_pattern_signature": meta.get("signal_pattern_signature", ""),
                        "signal_pattern_key": meta.get("signal_pattern_key", ""),
                        "signal_pattern_global_key": meta.get("signal_pattern_global_key", ""),
                        "pattern_direction_override": meta.get("pattern_direction_override", ""),
                        "pattern_direction_override_reason": meta.get("pattern_direction_override_reason", ""),
                        "pre_execution_history_check": meta.get("pre_execution_history_check", ""),
                        "entry_time": meta.get("entry_time_iso", ""),
                        "exit_time": now_et.isoformat(),
                        "hold_seconds": hold_seconds,
                        "entry_price": entry_price,
                        "exit_price": exit_price,
                        "realized_pnl_usd": round(trade_pnl_usd, 2),
                        "pnl_pct": round(realized_plpc, 4),
                        "paper_reported_pnl_usd": paper_reported_pnl_usd,
                        "paper_reported_pnl_pct": paper_reported_pnl_pct,
                        "conservative_executable_pnl_usd": conservative_pnl_usd,
                        "conservative_executable_pnl_pct": conservative_pnl_pct,
                        "max_favorable_excursion_pct": max_favorable_excursion_pct,
                        "max_adverse_excursion_pct": max_adverse_excursion_pct,
                        "entry_underlying_symbol": meta.get("ticker", ""),
                        "entry_bid_submit": meta.get("entry_bid_submit", ""),
                        "entry_ask_submit": meta.get("entry_ask_submit", ""),
                        "entry_midpoint_submit": meta.get("entry_midpoint_submit", ""),
                        "entry_intended_limit": meta.get("entry_intended_limit", ""),
                        "entry_filled_price": meta.get("entry_filled_price", entry_price),
                        "entry_spread_pct": meta.get("entry_spread_pct", ""),
                        "entry_fill_slippage_vs_ask_pct": meta.get("entry_fill_slippage_vs_ask_pct", ""),
                        "entry_fill_seconds": meta.get("entry_fill_seconds", ""),
                        "entry_attempts": meta.get("entry_attempts", ""),
                        "exit_underlying_symbol": meta.get("ticker", ""),
                        "exit_bid_submit": close_execution.get("submit_bid", ""),
                        "exit_ask_submit": close_execution.get("submit_ask", ""),
                        "exit_midpoint_submit": close_execution.get("submit_midpoint", ""),
                        "exit_intended_limit": close_execution.get("intended_limit", ""),
                        "exit_filled_price": exit_price,
                        "exit_spread_pct": close_execution.get("submit_spread_pct", ""),
                        "exit_fill_slippage_vs_bid_pct": close_execution.get("fill_slippage_vs_bid_pct", ""),
                        "exit_fill_seconds": close_execution.get("fill_seconds", ""),
                        "exit_attempts": close_execution.get("attempts", ""),
                        "exit_reason": exit_reason,
                    }
                    loss_diagnosis = {}
                    if trade_pnl_usd < 0:
                        loss_diagnosis = _record_local_loss_diagnosis(trade_row, now_et)
                        trade_row["loss_cause"] = loss_diagnosis.get("cause", "")
                        trade_row["loss_adaptation_action"] = loss_diagnosis.get("action", "")
                        trade_row["loss_underlying_move_pct"] = loss_diagnosis.get("underlying_move_pct", "")
                        last_exit_debug["loss_diagnosis"] = loss_diagnosis
                    _record_signal_pattern_outcome(trade_row, now_et)
                    try:
                        trade_logger.log_trade(trade_row)
                        trade_telemetry_last_log_error = ""
                    except Exception as log_exc:  # noqa: BLE001
                        trade_telemetry_last_log_error = str(log_exc)[:300]
                        print(f"[{ts(now_et)}] trade log write failed: {log_exc}")

                    trade_telemetry_closed_count += 1
                    trade_telemetry_total_pnl_usd += trade_pnl_usd
                    trade_telemetry_last_close_iso = now_et.isoformat()

                    if trade_pnl_usd < 0:
                        daily_realized_loss_usd += abs(trade_pnl_usd)
                        weekly_realized_loss_usd += abs(trade_pnl_usd)
                        consecutive_losses += 1
                    else:
                        consecutive_losses = 0

                    ticker = str(meta.get("ticker", "") or "").upper()
                    reversal_direction = ""
                    reentries_used = int(ticker_reentries_used.get(ticker, 0)) if ticker else 0
                    if ticker and remaining_qty <= 0:
                        _set_ticker_roundtrip_cooldown(
                            ticker,
                            now_et,
                            minutes=int(getattr(config, "TICKER_ROUNDTRIP_COOLDOWN_MINUTES", 0) or 0),
                            reason=str(exit_reason or "closed_roundtrip"),
                        )
                    if ticker and trade_pnl_usd < 0:
                        losing_direction = str(meta.get("direction", "") or "").lower()
                        reversal_direction = _arm_loss_direction_flip(
                            ticker,
                            losing_direction,
                            now_et,
                            reason=str(loss_diagnosis.get("cause") or exit_reason),
                        )
                        loss_cd_minutes = int(getattr(config, "REENTRY_COOLDOWN_LOSS_MINUTES", 20) or 20)
                        quick_loser_minutes = int(
                            getattr(config, "QUICK_LOSER_MAX_HOLD_MINUTES", 4) or 4
                        )
                        quick_loser_cooldown = int(
                            getattr(config, "QUICK_LOSER_REENTRY_COOLDOWN_MINUTES", 45) or 45
                        )
                        held_minutes_for_loss = max(0.0, hold_seconds / 60.0)
                        if held_minutes_for_loss <= float(quick_loser_minutes):
                            loss_cd_minutes = max(loss_cd_minutes, quick_loser_cooldown)
                        if str(exit_reason).lower() == "stop_loss":
                            loss_cd_minutes = int(
                                getattr(config, "STOP_LOSS_REENTRY_COOLDOWN_MINUTES", loss_cd_minutes)
                                or loss_cd_minutes
                            )
                        loss_cd_minutes = _cooldown_minutes_for_loss_cause(
                            str(loss_diagnosis.get("cause", "")),
                            loss_cd_minutes,
                        )
                        _set_ticker_loss_cooldown(
                            ticker,
                            now_et,
                            minutes=loss_cd_minutes,
                            reason=str(loss_diagnosis.get("cause") or exit_reason),
                        )
                    if (
                        ticker
                        and trade_pnl_usd < 0
                        and exit_reason == "stop_loss"
                        and bool(getattr(config, "ENABLE_STOPLOSS_REVERSAL_REENTRY", False))
                    ):
                        if not reversal_direction:
                            reversal_direction = _arm_loss_direction_flip(
                                ticker,
                                str(meta.get("direction", "") or "").lower(),
                                now_et,
                                reason=str(exit_reason),
                            )
                        reversal_direction = str(ticker_reentry_expected_direction.get(ticker, "") or "").lower()
                    elif ticker and trade_pnl_usd >= 0:
                        ticker_reentry_armed[ticker] = False
                        ticker_reentry_expected_direction[ticker] = ""

                    remaining_qty = max(0, qty - filled_close_qty)
                    if remaining_qty <= 0:
                        open_trade_meta.pop(symbol, None)
                    else:
                        if symbol in open_trade_meta:
                            open_trade_meta[symbol]["qty"] = remaining_qty
                    _save_runtime_state()
                    print(
                        f"[{ts(now_et)}] EXIT {symbol} qty={filled_close_qty}/{qty} "
                        f"reason={exit_reason} pnl_pct={realized_plpc:.2%}"
                    )
                    if (
                        exit_reason == "stop_loss"
                        and remaining_qty <= 0
                        and ticker
                        and reversal_direction in ("call", "put")
                        and bool(getattr(config, "ENABLE_STOPLOSS_REVERSAL_REENTRY", False))
                    ):
                        cd_until = _active_ticker_loss_cooldown_until(ticker, now_et)
                        if cd_until is None:
                            _attempt_reversal_entry(
                                ticker=ticker,
                                direction=reversal_direction,
                                now_et=now_et,
                                reentries_used=reentries_used,
                            )
                        else:
                            print(
                                f"[{ts(now_et)}] {ticker}: immediate reversal entry suppressed "
                                f"(loss cooldown until {ts(cd_until)})."
                            )
                except Exception as exc:  # noqa: BLE001
                    last_exit_debug = {
                        "loop_ts_et": ts(now_et),
                        "symbol": symbol,
                        "reason": exit_reason,
                        "requested_qty": close_qty,
                        "position_qty": qty,
                        "filled_qty": 0,
                        "result": "error",
                        "error": str(exc),
                    }
                    _save_runtime_state()
                    print(f"[{ts(now_et)}] {symbol}: error closing position: {exc}")
                time.sleep(config.RATE_LIMIT_SLEEP_SECONDS)

        live_symbols = {
            str(getattr(p, "symbol", "") or "")
            for p in option_positions
            if position_qty_as_int(getattr(p, "qty", 0)) > 0
        }
        stale_history_symbols = [sym for sym in open_position_pl_history.keys() if sym not in live_symbols]
        for stale_sym in stale_history_symbols:
            open_position_pl_history.pop(stale_sym, None)

        # Check hard close time again after exit loop — catches cases where the loop
        # was mid-iteration when the close window opened.
        now_et = datetime.now(tz)
        if is_at_or_after(now_et, config.HARD_CLOSE_TIME):
            print(f"[{ts(now_et)}] Hard close time reached. Flattening and shutting down.")
            option_positions = broker.get_open_option_positions()
            for pos in option_positions:
                symbol = str(getattr(pos, "symbol", ""))
                qty = position_qty_as_int(getattr(pos, "qty", 0))
                if qty > 0:
                    try:
                        filled_qty, _fill_price, _close_meta = _close_position_with_confirmation(
                            symbol=symbol,
                            qty=qty,
                            now_et=now_et,
                            label="EOD CLOSE",
                            exit_reason="eod_close",
                        )
                        if filled_qty > 0:
                            print(f"[{ts(now_et)}] EOD CLOSE {symbol} qty={filled_qty}/{qty}")
                        else:
                            print(f"[{ts(now_et)}] EOD CLOSE {symbol} qty={qty} pending/not filled.")
                    except Exception as exc:  # noqa: BLE001
                        print(f"[{ts(now_et)}] {symbol}: EOD close error: {exc}")
                    time.sleep(config.RATE_LIMIT_SLEEP_SECONDS)

            try:
                broker.cancel_all_open_orders()
            except Exception as exc:  # noqa: BLE001
                print(f"[{ts(now_et)}] Cancel orders error: {exc}")
            _save_runtime_state()
            alerts.send(
                "session_complete",
                "Hard close completed. Positions flattened and open orders canceled.",
                dedupe_key=f"session-close-{now_et.date().isoformat()}",
            )
            break

        _save_runtime_state()
        loop_sleep_seconds = max(1, int(config.LOOP_INTERVAL_SECONDS))
        if bool(getattr(config, "CONTINUOUS_ENTRY_SEARCH_ENABLED", False)):
            entries_filled_this_loop = int(entry_debug.get("entries_filled", 0) or 0)
            orders_submitted_this_loop = int(entry_debug.get("entry_orders_submitted", 0) or 0)
            active_entry_orders = sum(int(v) for v in alpaca_active_buy_orders_by_ticker.values())
            can_search_for_entry = (
                entries_filled_this_loop <= 0
                and orders_submitted_this_loop <= 0
                and active_entry_orders <= 0
                and open_count < int(getattr(config, "MAX_POSITIONS", 1) or 1)
                and is_at_or_after(now_et, config.NO_NEW_TRADES_BEFORE)
                and not is_at_or_after(now_et, config.NO_NEW_TRADES_AFTER)
            )
            if can_search_for_entry:
                loop_sleep_seconds = max(
                    1,
                    int(getattr(config, "CONTINUOUS_ENTRY_SEARCH_SLEEP_SECONDS", 1) or 1),
                )
                print(
                    f"[{ts(now_et)}] No executable entry found; continuing search in "
                    f"{loop_sleep_seconds}s across {len(watchlist)} tickers."
                )
        time.sleep(loop_sleep_seconds)

    print(f"[{ts()}] Trader stopped.")


if __name__ == "__main__":
    main()
