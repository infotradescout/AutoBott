"""Entry point for the intraday options autotrader."""

from __future__ import annotations

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
from trading_control import load_trading_control
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

    attempt_quotes = [quote_snapshot]
    retry_quote = _option_quote_snapshot(data_client, option_symbol)
    retry_ask = float(retry_quote.get("ask") or 0.0)
    if retry_ask > 0:
        attempt_quotes.append(retry_quote)
    else:
        attempt_quotes.append(quote_snapshot)

    for attempt_index, submit_quote in enumerate(attempt_quotes, start=1):
        submit_ask = float(submit_quote.get("ask") or 0.0)
        if submit_ask <= 0:
            continue
        if attempt_index == 1:
            limit_price = round(submit_ask, 4)
            wait_seconds = max(1, int(config.ENTRY_ORDER_STATUS_WAIT_SECONDS))
        else:
            retry_pct = max(0.0, float(getattr(config, "ENTRY_RETRY_LIMIT_PCT", 0.02) or 0.02))
            limit_price = round(max(submit_ask, submit_ask * (1.0 + retry_pct)), 4)
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
        if order_id:
            try:
                broker.cancel_order(order_id)
            except Exception as exc:  # noqa: BLE001
                print(f"[{ts(now_et)}] {label}: cancel entry order {order_id} failed: {exc}")
        if still_open:
            time.sleep(1)

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
        bars = data_client.get_intraday_bars_since_open(
            symbol=ticker,
            now_et=now_et,
            limit=max(3, int(config.ENTRY_CONFIRM_BARS)),
        )
        if bars is None or bars.empty or len(bars) < 2:
            return False
        closes = bars["close"].astype(float)
        last_close = float(closes.iloc[-1])
        prev_close = float(closes.iloc[-2])
        two_bar_ref = float(closes.iloc[-3]) if len(closes) >= 3 else prev_close
        if two_bar_ref <= 0 or prev_close <= 0:
            return False

        one_bar_move_pct = ((last_close - prev_close) / prev_close) * 100.0
        two_bar_move_pct = ((last_close - two_bar_ref) / two_bar_ref) * 100.0
        momentum_threshold_pct = 0.06

        if direction == "call":
            return (
                last_close > prev_close
                or last_close > two_bar_ref
                or one_bar_move_pct >= momentum_threshold_pct
                or two_bar_move_pct >= momentum_threshold_pct
            )
        return (
            last_close < prev_close
            or last_close < two_bar_ref
            or one_bar_move_pct <= -momentum_threshold_pct
            or two_bar_move_pct <= -momentum_threshold_pct
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
            "max_plpc": 0.0,
            "min_plpc": 0.0,
            "inferred": True,
        }
        hydrated += 1
    return hydrated


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
    if config.AUTO_EXPAND_UNIVERSE_WITH_MOVERS:
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
    next_heartbeat_at = 0.0
    last_heartbeat_persist_at = 0.0
    manual_stop_latched = False
    control_state = load_trading_control()
    strategy_profile = normalize_profile_name(str(control_state.get("strategy_profile", "balanced") or "balanced"))
    dry_run_enabled = bool(control_state.get("dry_run", False)) and is_enabled("FEATURE_DRY_RUN_MODE", False)
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
                "contract_spread_pct": round(float(entry_result.get("submit_spread_pct", 0.0) or 0.0), 4),
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
                limit_price = round(bid_price + (spread * max(0.0, reprice_pct)), 4)
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
        for pos in option_positions:
            symbol = str(getattr(pos, "symbol", "") or "")
            qty = position_qty_as_int(getattr(pos, "qty", 0))
            if not symbol or qty <= 0:
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

        total_filled = 0
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
        dry_run_enabled = bool(control_state.get("dry_run", False)) and is_enabled("FEATURE_DRY_RUN_MODE", False)
        manual_stop = bool(control_state.get("manual_stop", False))
        if manual_stop:
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
        dry_run_enabled = bool(control_state.get("dry_run", False)) and is_enabled("FEATURE_DRY_RUN_MODE", False)
        manual_stop = bool(control_state.get("manual_stop", False))
        if manual_stop:
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
            premarket_opening_signals = []
            premarket_signals_day = ""
            premarket_scan_runs = 0
            premarket_last_scan_at = None
            bad_fill_tracker = {}
            set_catalyst_mode(False, "")

        option_positions = broker.get_open_option_positions()
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
                if float(s.get("signal_score", 0) or 0) >= floor:
                    filtered_signals.append(s)
            signals = filtered_signals
            if before != len(signals):
                print(
                    f"[{ts(now_et)}] Entry signal-score gate (runtime floor {entry_min_signal_score:.2f}) "
                    f"filtered signals {before}->{len(signals)} (profile={strategy_profile})."
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
            "scan_pass_count": len(signals),
            "signals_considered": 0,
            "entry_orders_submitted": 0,
            "entries_filled": 0,
            "skips": {},
            "exceptions": [],
        }

        def _mark_skip(reason: str) -> None:
            skips = entry_debug.get("skips", {})
            if not isinstance(skips, dict):
                skips = {}
            skips[reason] = int(skips.get(reason, 0)) + 1
            entry_debug["skips"] = skips

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

            # --- Consecutive loss circuit breaker ---
            if consecutive_losses >= config.CONSECUTIVE_LOSS_LIMIT:
                _mark_skip("consecutive_loss_limit")
                print(
                    f"[{ts(now_et)}] {consecutive_losses} consecutive losses. "
                    f"Pausing new entries for the rest of the day."
                )
                break

            # --- PDT guard (only blocks if ENFORCE_PDT_GUARD=True) ---
            if local_trade_budget_hit:
                _mark_skip("pdt_local_budget_hit")
                break
            if config.ENFORCE_PDT_GUARD and not pdt_allowed:
                _mark_skip("pdt_broker_block")
                break

            ticker = signal["symbol"]
            direction = signal["direction"]
            if not _is_valid_long_direction(direction):
                _mark_skip("invalid_strategy_direction")
                print(f"[{ts(now_et)}] {ticker}: skip (invalid direction={direction!r}; only CALL/PUT allowed).")
                continue
            if _is_bad_fill_blocked(ticker, now_et):
                _mark_skip("bad_fill_cooldown")
                print(f"[{ts(now_et)}] {ticker}: skip (bad-fill cooldown active).")
                continue
            loss_cooldown_until = _active_ticker_loss_cooldown_until(ticker, now_et)
            if loss_cooldown_until is not None:
                _mark_skip("ticker_loss_cooldown")
                print(
                    f"[{ts(now_et)}] {ticker}: skip (loss cooldown until {ts(loss_cooldown_until)})."
                )
                continue
            if not is_at_or_after(now_et, config.NO_NEW_TRADES_BEFORE):
                _mark_skip("before_entry_window")
                print(f"[{ts(now_et)}] Entry window not open yet (before {config.NO_NEW_TRADES_BEFORE} ET).")
                break
            if is_at_or_after(now_et, config.NO_NEW_TRADES_AFTER):
                _mark_skip("after_entry_window")
                print(f"[{ts(now_et)}] Entry window closed (past {config.NO_NEW_TRADES_AFTER} ET).")
                break
            if _is_entry_hour_blocked(now_et, strategy_profile=strategy_profile):
                _mark_skip("blocked_entry_hour")
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
                print(f"[{ts(now_et)}] {ticker}: skip (existing option position).")
                continue
            if _has_ticker_open_meta(ticker):
                _mark_skip("existing_ticker_runtime_state")
                print(f"[{ts(now_et)}] {ticker}: skip (already open in runtime state).")
                continue
            if dry_run_enabled:
                _mark_skip("dry_run_mode")
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
            if prior_entries >= max_entries_per_ticker:
                if not reentry_armed:
                    _mark_skip("max_entries_per_ticker_reached")
                    print(
                        f"[{ts(now_et)}] {ticker}: skip (max entries reached "
                        f"{prior_entries}/{max_entries_per_ticker}; no stop-loss re-entry armed)."
                    )
                    continue
            if reentry_armed:
                if reentries_used >= int(config.MAX_REENTRIES_PER_TICKER):
                    _mark_skip("max_reentries_used")
                    print(
                        f"[{ts(now_et)}] {ticker}: skip (max re-entries used "
                        f"{reentries_used}/{int(config.MAX_REENTRIES_PER_TICKER)})."
                    )
                    continue
                if expected_direction in ("call", "put") and direction != expected_direction:
                    _mark_skip("waiting_for_reversal_signal")
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
                    print(f"[{ts(now_et)}] {ticker}: skip (entry confirmation candle not aligned).")
                    continue

            # Re-check live position count right before placing a new order.
            option_positions = broker.get_open_option_positions()
            open_count = len(option_positions)
            if not can_open_new_positions(open_count, config.MAX_POSITIONS):
                _mark_skip("max_positions_reached")
                print(f"[{ts(now_et)}] Max positions reached. Stopping new entries this loop.")
                break

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
                signal_stop_loss_usd = float(signal.get("stop_loss_usd", _runtime_stop_loss_usd()) or _runtime_stop_loss_usd())
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
                    print(f"[{ts(now_et)}] {ticker}: skip (no eligible option contract: {contract_reason}).")
                    time.sleep(config.RATE_LIMIT_SLEEP_SECONDS)
                    continue

                option_symbol = contract["symbol"]
                if not _option_symbol_matches_direction(option_symbol, direction):
                    _mark_skip("contract_direction_mismatch")
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
                    _mark_skip("quote_spread_too_wide" if "spread" in spread_reason else "no_option_ask")
                    print(f"[{ts(now_et)}] {ticker}: skip ({spread_reason}).")
                    time.sleep(config.RATE_LIMIT_SLEEP_SECONDS)
                    continue
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
                        spread_override_pct=signal_entry_max_spread,
                    )
                    retry_ask = float(retry_quote.get("ask") or 0.0)
                    if retry_ok and retry_ask > 0:
                        entry_quote = retry_quote
                        ask_price = retry_ask
                        pre_submit_slippage = _slippage_pct(initial_chain_ask, ask_price)
                    elif not retry_ok:
                        _mark_skip("quote_spread_too_wide" if "spread" in retry_reason else "no_option_ask")
                        print(f"[{ts(now_et)}] {ticker}: skip ({retry_reason}).")
                        time.sleep(config.RATE_LIMIT_SLEEP_SECONDS)
                        continue
                if pre_submit_slippage > (config.MAX_ENTRY_SLIPPAGE_PCT * 3):
                    _mark_skip("entry_slippage_too_high")
                    print(
                        f"[{ts(now_et)}] {ticker}: skip (entry slippage {pre_submit_slippage:.2f}% > "
                        f"hard cap {(config.MAX_ENTRY_SLIPPAGE_PCT * 3):.2f}%)."
                    )
                    time.sleep(config.RATE_LIMIT_SLEEP_SECONDS)
                    continue

                qty = 1
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
                if not bool(entry_result.get("filled", False)):
                    _mark_skip("entry_not_filled_after_retry")
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
                    "contract_spread_pct": round(float(entry_result.get("submit_spread_pct", 0.0) or 0.0), 4),
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
                entry_debug["entries_filled"] = int(entry_debug.get("entries_filled", 0)) + 1
                _save_runtime_state()
                time.sleep(config.RATE_LIMIT_SLEEP_SECONDS)
            except Exception as exc:  # noqa: BLE001
                _mark_skip("entry_flow_exception")
                _record_entry_exception(ticker, exc)
                print(
                    f"[{ts(now_et)}] {ticker}: error during entry flow "
                    f"({type(exc).__name__}): {exc!r}"
                )
                time.sleep(config.RATE_LIMIT_SLEEP_SECONDS)

        last_entry_debug = entry_debug

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
            # 2. IMMEDIATE TAKE PROFIT: exit instantly once gain reaches configured cap.
            # 3. REVERSAL EXIT: otherwise, if profitable, hold until momentum reverses.

            if meta:
                meta["max_plpc"] = max(float(meta.get("max_plpc", plpc) or plpc), plpc)
                meta["min_plpc"] = min(float(meta.get("min_plpc", plpc) or plpc), plpc)
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
            # Rule 1: fixed-dollar stop loss
            stop_loss_usd_cap = float(meta.get("stop_loss_usd", _runtime_stop_loss_usd()) or _runtime_stop_loss_usd())
            if exit_reason is None and should_trigger_stop_loss(unrealized_usd, stop_loss_usd_cap):
                exit_reason = "stop_loss"

            # Rule 2: immediate take-profit once the option has doubled (or configured threshold).
            immediate_take_profit_pct = float(
                meta.get("immediate_take_profit_pct", getattr(config, "IMMEDIATE_TAKE_PROFIT_PCT", 1.0))
                or getattr(config, "IMMEDIATE_TAKE_PROFIT_PCT", 1.0)
            )
            if exit_reason is None and plpc >= immediate_take_profit_pct:
                exit_reason = "immediate_take_profit"

            # --- Reversal detection exit ---
            # When the trade is in profit, check if the underlying is reversing.
            # Exit on confirmed reversal so we keep gains without a fixed cap.
            # Only triggers when ENABLE_REVERSAL_EXIT=true (default: true) and
            # the trade has reached a minimum profit threshold first.
            if (
                exit_reason is None
                and bool(getattr(config, "ENABLE_REVERSAL_EXIT", True))
                and plpc >= float(getattr(config, "REVERSAL_EXIT_MIN_PROFIT_PCT", 0.10))
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
                    if exit_reason == "stop_loss":
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
                    trade_logger.log_trade(
                        {
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
                            "contract_spread_pct": meta.get("contract_spread_pct", ""),
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
                    )

                    if trade_pnl_usd < 0:
                        daily_realized_loss_usd += abs(trade_pnl_usd)
                        weekly_realized_loss_usd += abs(trade_pnl_usd)
                        consecutive_losses += 1
                    else:
                        consecutive_losses = 0

                    ticker = str(meta.get("ticker", "") or "")
                    reversal_direction = ""
                    reentries_used = int(ticker_reentries_used.get(ticker, 0)) if ticker else 0
                    if ticker and trade_pnl_usd < 0:
                        loss_cd_minutes = int(getattr(config, "REENTRY_COOLDOWN_LOSS_MINUTES", 20) or 20)
                        if str(exit_reason).lower() == "stop_loss":
                            loss_cd_minutes = int(
                                getattr(config, "STOP_LOSS_REENTRY_COOLDOWN_MINUTES", loss_cd_minutes)
                                or loss_cd_minutes
                            )
                        _set_ticker_loss_cooldown(
                            ticker,
                            now_et,
                            minutes=loss_cd_minutes,
                            reason=str(exit_reason),
                        )
                    if ticker and exit_reason == "stop_loss":
                        ticker_reentry_armed[ticker] = True
                        prior_direction = str(meta.get("direction", "") or "").lower()
                        if prior_direction == "call":
                            ticker_reentry_expected_direction[ticker] = "put"
                        elif prior_direction == "put":
                            ticker_reentry_expected_direction[ticker] = "call"
                        else:
                            ticker_reentry_expected_direction[ticker] = ""
                        reversal_direction = str(ticker_reentry_expected_direction.get(ticker, "") or "").lower()
                    elif ticker:
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
        time.sleep(config.LOOP_INTERVAL_SECONDS)

    print(f"[{ts()}] Trader stopped.")


if __name__ == "__main__":
    main()
