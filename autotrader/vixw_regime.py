"""VIX-derived volatility proxy sidecar for AutoBott.

Paper-mode doctrine:
- Read the VIX level.
- If VIX is below the configured average level, prefer CALL exposure.
- If VIX is above the configured average level, prefer PUT exposure.
- Execute through an Alpaca-supported equity/ETF/ETN options proxy by default.
- Use contracts a few trading days out.

Default execution proxy: VIXY options. The signal remains ^VIX.
"""

from __future__ import annotations

import csv
import math
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytz
import yfinance as yf

import config
import runtime_telemetry
from broker import AlpacaBroker
from data import AlpacaDataClient
from trading_control import load_trading_control

EASTERN = pytz.timezone(str(getattr(config, "EASTERN_TZ", "US/Eastern") or "US/Eastern"))
VIX_PROXY_LOG_COLUMNS = [
    "timestamp",
    "vix_level",
    "average_level",
    "proxy_underlying",
    "proxy_underlying_price",
    "direction",
    "decision",
    "reason",
    "option_symbol",
    "strike",
    "expiration",
    "ask",
    "bid",
    "spread_pct",
    "qty",
    "profit_target_price",
    "profit_order_id",
    "profit_order_status",
    "ticker",
    "underlying_price",
    "vix_level_regime",
    "vix_1m_roc",
    "vix_5m_roc",
    "vix_15m_roc",
    "vxx_5m_roc",
    "vix_momentum_state",
    "upper_wick_ratio",
    "failed_breakout",
    "close_back_inside_range",
    "option_bid",
    "option_ask",
    "option_mark",
    "dte",
    "volume",
    "open_interest",
    "quote_age_seconds",
    "entry_allowed",
    "entry_reason",
    "skip_reason",
]
LEGACY_VIX_PROXY_LOG_COLUMNS = [
    "timestamp",
    "vix_level",
    "average_level",
    "direction",
    "decision",
    "reason",
    "option_symbol",
    "strike",
    "expiration",
    "ask",
    "bid",
    "spread_pct",
    "qty",
]


def _now_et() -> datetime:
    return datetime.now(EASTERN)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return float(default)
    if math.isnan(parsed) or math.isinf(parsed):
        return float(default)
    return parsed


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return int(default)


def _cfg(name: str, default: Any) -> Any:
    return getattr(config, name, default)


def _enum_text(value: Any) -> str:
    try:
        value = getattr(value, "value", value)
    except Exception:  # noqa: BLE001
        pass
    return str(value or "").strip().lower()


def _order_field_text(order: Any, field: str) -> str:
    return _enum_text(getattr(order, field, ""))


def _order_symbol(order: Any) -> str:
    return str(getattr(order, "symbol", "") or "").strip().upper()


def _order_submitted_at_et(order: Any) -> datetime | None:
    raw = getattr(order, "submitted_at", None) or getattr(order, "created_at", None)
    if raw is None:
        return None
    if isinstance(raw, datetime):
        dt = raw
    else:
        try:
            dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        except Exception:
            return None
    if dt.tzinfo is None:
        dt = EASTERN.localize(dt)
    return dt.astimezone(EASTERN)


def _as_et(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return EASTERN.localize(dt)
    return dt.astimezone(EASTERN)


def _time_from_config(value: Any, default: str) -> tuple[int, int]:
    raw = str(value or default).strip()
    try:
        hour_text, minute_text = raw.split(":", 1)
        return int(hour_text), int(minute_text)
    except Exception:
        hour_text, minute_text = str(default).split(":", 1)
        return int(hour_text), int(minute_text)


def _market_minutes(hour: int, minute: int) -> int:
    return int(hour) * 60 + int(minute)


def _vix_level_regime(vix_level: float) -> str:
    if vix_level < 14:
        return "low"
    if vix_level < 20:
        return "normal"
    if vix_level < 28:
        return "elevated"
    return "stressed"


def _roc_from_closes(closes: list[float], periods: int) -> float:
    if len(closes) <= periods:
        return 0.0
    current = _safe_float(closes[-1], 0.0)
    previous = _safe_float(closes[-1 - periods], 0.0)
    if current <= 0 or previous <= 0:
        return 0.0
    return ((current - previous) / previous) * 100.0


def _bar_value(bar: Any, field: str) -> float:
    if isinstance(bar, dict):
        return _safe_float(bar.get(field), 0.0)
    return _safe_float(getattr(bar, field, None), 0.0)


def _bar_closes(bars: list[Any]) -> list[float]:
    return [_bar_value(bar, "close") for bar in bars if _bar_value(bar, "close") > 0]


def _wick_metrics(bars: list[Any]) -> dict[str, Any]:
    if len(bars) < 2:
        return {
            "upper_wick_ratio": 0.0,
            "failed_breakout": False,
            "close_back_inside_range": False,
        }
    current = bars[-1]
    previous = bars[-2]
    open_price = _bar_value(current, "open")
    high = _bar_value(current, "high")
    low = _bar_value(current, "low")
    close = _bar_value(current, "close")
    prior_high = _bar_value(previous, "high")
    candle_range = max(0.0, high - low)
    body_top = max(open_price, close)
    upper_wick = max(0.0, high - body_top)
    upper_wick_ratio = (upper_wick / candle_range) if candle_range > 0 else 0.0
    failed_breakout = bool(high > prior_high and close < prior_high)
    return {
        "upper_wick_ratio": round(upper_wick_ratio, 4),
        "failed_breakout": failed_breakout,
        "close_back_inside_range": bool(close < prior_high),
    }


def _safe_option_mark(quote: dict[str, Any]) -> tuple[float, str]:
    bid = _safe_float(quote.get("bid"), 0.0)
    ask = _safe_float(quote.get("ask"), 0.0)
    mark = _safe_float(quote.get("mark"), 0.0)
    if mark > 0:
        return mark, "mark"
    if bid > 0 and ask > bid:
        return (bid + ask) / 2.0, "midpoint_fallback"
    return 0.0, "missing"


def _quote_age_seconds(quote: dict[str, Any], now_et: datetime) -> float:
    raw = quote.get("updated_at") or quote.get("timestamp") or quote.get("t")
    if raw is None:
        return 0.0
    if isinstance(raw, datetime):
        quote_dt = raw
    else:
        try:
            quote_dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        except Exception:
            return 999999.0
    quote_dt = _as_et(quote_dt)
    return max(0.0, (_as_et(now_et) - quote_dt).total_seconds())


def _contract_dte(contract: dict[str, Any], now_et: datetime) -> int:
    raw = _contract_expiration(contract)
    if not raw:
        return -1
    try:
        expiry = date.fromisoformat(raw[:10])
    except Exception:
        return -1
    return max(0, (expiry - _as_et(now_et).date()).days)


def _entry_time_skip_reason(now_et: datetime) -> str | None:
    now_et = _as_et(now_et)
    open_minutes = _market_minutes(9, 30)
    current_minutes = _market_minutes(now_et.hour, now_et.minute)
    block_after_open = int(_cfg("VIXW_ENTRY_BLOCK_AFTER_OPEN_MINUTES", 30) or 30)
    if open_minutes <= current_minutes < open_minutes + block_after_open:
        return "BLOCKED_OPENING_WINDOW"
    cutoff_hour, cutoff_minute = _time_from_config(_cfg("VIXW_NO_NEW_ENTRIES_AFTER", "15:45"), "15:45")
    if current_minutes >= _market_minutes(cutoff_hour, cutoff_minute):
        return "BLOCKED_LATE_SESSION"
    return None


def _build_vixw_entry_telemetry(
    *,
    ticker: str,
    underlying_price: float,
    vix_bars: list[Any],
    vxx_bars: list[Any],
    option_contract: dict[str, Any],
    option_quote: dict[str, Any],
    now_et: datetime,
    macro_blocked: bool = False,
) -> dict[str, Any]:
    now_et = _as_et(now_et)
    closes = _bar_closes(vix_bars)
    vxx_closes = _bar_closes(vxx_bars)
    vix_level = closes[-1] if closes else 0.0
    wick = _wick_metrics(vix_bars)
    mark, mark_source = _safe_option_mark(option_quote)
    bid = _safe_float(option_quote.get("bid"), 0.0)
    ask = _safe_float(option_quote.get("ask"), 0.0)
    midpoint = mark if mark > 0 else ((bid + ask) / 2.0 if bid > 0 and ask > bid else 0.0)
    spread_pct = ((ask - bid) / midpoint) * 100.0 if midpoint > 0 and ask >= bid else 999.0
    dte = _contract_dte(option_contract, now_et)
    quote_age = _quote_age_seconds(option_quote, now_et)
    vix_1m_roc = _roc_from_closes(closes, 1)
    vix_5m_roc = _roc_from_closes(closes, 5)
    vix_15m_roc = _roc_from_closes(closes, 15)
    vxx_5m_roc = _roc_from_closes(vxx_closes, 5)
    acceleration_min = float(_cfg("VIXW_ACCELERATING_ROC_MIN", 0.05) or 0.05)
    decel_tolerance = float(_cfg("VIXW_DECELERATION_TOLERANCE", 0.02) or 0.02)
    vix_5m_pace = vix_5m_roc / 5.0
    if vix_1m_roc > vix_5m_pace + acceleration_min and vix_5m_roc > 0:
        momentum_state = "accelerating"
    elif vix_1m_roc <= vix_5m_pace + decel_tolerance:
        momentum_state = "decelerating"
    else:
        momentum_state = "flat"

    telemetry = {
        "ticker": ticker,
        "underlying_price": round(float(underlying_price), 4) if underlying_price else 0.0,
        "vix_level": round(vix_level, 4),
        "vix_level_regime": _vix_level_regime(vix_level),
        "vix_1m_roc": round(vix_1m_roc, 4),
        "vix_5m_roc": round(vix_5m_roc, 4),
        "vix_15m_roc": round(vix_15m_roc, 4),
        "vxx_5m_roc": round(vxx_5m_roc, 4),
        "vix_momentum_state": momentum_state,
        "upper_wick_ratio": wick["upper_wick_ratio"],
        "failed_breakout": wick["failed_breakout"],
        "close_back_inside_range": wick["close_back_inside_range"],
        "option_symbol": _contract_symbol(option_contract),
        "option_bid": bid,
        "option_ask": ask,
        "option_mark": round(mark, 4),
        "option_mark_source": mark_source,
        "spread_pct": round(spread_pct, 4),
        "dte": dte,
        "volume": _safe_int(option_contract.get("volume", option_quote.get("volume", 0)), 0),
        "open_interest": _safe_int(option_contract.get("open_interest", option_quote.get("open_interest", 0)), 0),
        "quote_age_seconds": round(quote_age, 2),
        "entry_allowed": False,
        "entry_reason": "",
        "skip_reason": "",
    }

    time_skip = _entry_time_skip_reason(now_et)
    max_spread = float(_cfg("VIXW_MAX_OPTION_SPREAD_PCT", 3.0) or 3.0)
    min_dte = int(_cfg("VIXW_MIN_DTE_TRADING_DAYS", 3) or 3)
    max_dte = int(_cfg("VIXW_MAX_DTE_TRADING_DAYS", 7) or 7)
    max_quote_age = float(_cfg("VIXW_MAX_QUOTE_AGE_SECONDS", 60) or 60)
    min_volume = int(_cfg("VIXW_MIN_OPTION_VOLUME", 0) or 0)
    min_oi = int(_cfg("VIXW_MIN_OPTION_OPEN_INTEREST", 0) or 0)

    if macro_blocked:
        telemetry["skip_reason"] = "BLOCKED_MACRO_NEWS_EVENT"
    elif time_skip:
        telemetry["skip_reason"] = time_skip
    elif vix_level >= 28:
        telemetry["skip_reason"] = "BLOCKED_STRESSED_VIX"
    elif vix_5m_roc > 0 and momentum_state == "accelerating":
        telemetry["skip_reason"] = "BLOCKED_ACCELERATING_VIX_SPIKE"
    elif not (wick["failed_breakout"] and wick["close_back_inside_range"] and momentum_state == "decelerating"):
        telemetry["skip_reason"] = "BLOCKED_NO_WICK_FAILURE_DECELERATION"
    elif bid <= 0 or ask <= bid or mark <= 0:
        telemetry["skip_reason"] = "BLOCKED_OPTION_QUOTE_INVALID"
    elif spread_pct > max_spread:
        telemetry["skip_reason"] = "BLOCKED_OPTION_SPREAD_WIDE"
    elif dte < min_dte or dte > max_dte:
        telemetry["skip_reason"] = "BLOCKED_DTE_OUT_OF_RANGE"
    elif quote_age > max_quote_age:
        telemetry["skip_reason"] = "BLOCKED_STALE_OPTION_QUOTE"
    elif min_volume > 0 and telemetry["volume"] < min_volume:
        telemetry["skip_reason"] = "BLOCKED_OPTION_VOLUME_LOW"
    elif min_oi > 0 and telemetry["open_interest"] < min_oi:
        telemetry["skip_reason"] = "BLOCKED_OPTION_OPEN_INTEREST_LOW"
    else:
        telemetry["entry_allowed"] = True
        telemetry["entry_reason"] = "VIX_DECELERATION_WICK_FAILURE"
        telemetry["skip_reason"] = ""
    return telemetry


def _position_exit_decision(
    *,
    entry_price: float,
    mark: float,
    bid: float,
    held_minutes: float,
) -> dict[str, Any]:
    entry_price = _safe_float(entry_price, 0.0)
    mark = _safe_float(mark, 0.0)
    bid = _safe_float(bid, 0.0)
    if entry_price <= 0:
        return {"action": "hold", "reason": "HOLD_POSITION"}
    pnl_pct = ((mark - entry_price) / entry_price) if mark > 0 else -1.0
    if bid > 0 and bid <= entry_price * float(_cfg("VIXW_EMERGENCY_BID_COLLAPSE_PCT", 0.50) or 0.50):
        return {
            "action": "close",
            "reason": "EMERGENCY_LIQUIDITY_COLLAPSE_PROTECTION_TRIGGERED",
            "pnl_pct": pnl_pct,
        }
    if mark > 0 and pnl_pct >= (float(_cfg("VIXW_PROFIT_TARGET_MULTIPLIER", 1.25) or 1.25) - 1.0):
        return {"action": "profit", "reason": "PROFIT_TRAP_TARGET_REACHED", "pnl_pct": pnl_pct}
    if mark > 0 and pnl_pct <= -abs(float(_cfg("VIXW_STOP_LOSS_PCT", 0.15) or 0.15)):
        return {"action": "close", "reason": "PRIMARY_MARK_STOP_LOSS_TRIGGERED", "pnl_pct": pnl_pct}
    max_hold = float(_cfg("VIXW_MAX_HOLD_MINUTES", 15) or 15)
    min_progress = float(_cfg("VIXW_TIME_STOP_MIN_PROGRESS_PCT", 0.05) or 0.05)
    if held_minutes >= max_hold and pnl_pct < min_progress:
        return {"action": "close", "reason": "HARD_TIME_STOP_EXPIRED", "pnl_pct": pnl_pct}
    return {"action": "hold", "reason": "HOLD_POSITION", "pnl_pct": pnl_pct}


def _frame_to_bar_records(frame: Any) -> list[dict[str, Any]]:
    try:
        if frame is None or bool(getattr(frame, "empty", True)):
            return []
        columns = {str(col).lower(): col for col in getattr(frame, "columns", [])}
        required = ("open", "high", "low", "close")
        if not all(col in columns for col in required):
            return []
        records: list[dict[str, Any]] = []
        for _, row in frame.iterrows():
            records.append({
                "open": row[columns["open"]],
                "high": row[columns["high"]],
                "low": row[columns["low"]],
                "close": row[columns["close"]],
                "volume": row[columns["volume"]] if "volume" in columns else 0,
            })
        return records
    except Exception:
        return []


def _latest_yfinance_bars(symbol: str, *, limit: int = 30, interval: str = "1m") -> list[dict[str, Any]]:
    try:
        frame = yf.Ticker(symbol).history(period="5d", interval=interval, auto_adjust=False)
    except Exception as exc:  # noqa: BLE001
        print(f"[vix_proxy] yfinance bars failed for {symbol}: {exc}")
        return []
    return _frame_to_bar_records(frame.tail(limit) if frame is not None else frame)


def _runtime_entry_telemetry(
    data_client: AlpacaDataClient,
    *,
    contract: dict[str, Any],
    now_et: datetime,
) -> dict[str, Any]:
    proxy_underlying = str(contract.get("proxy_underlying") or _cfg("VIXW_OPTION_UNDERLYING_SYMBOL", "VIXY")).upper()
    vix_bars = _latest_yfinance_bars(str(_cfg("VIXW_SIGNAL_SOURCE_SYMBOL", "^VIX") or "^VIX"), limit=30, interval="1m")
    try:
        proxy_frame = data_client.get_stock_bars(proxy_underlying, limit=30, timeframe="1m")
    except Exception as exc:  # noqa: BLE001
        print(f"[vix_proxy] proxy bars failed for {proxy_underlying}: {exc}")
        proxy_frame = None
    proxy_bars = _frame_to_bar_records(proxy_frame)
    quote_age_seconds = _safe_float(contract.get("quote_age_seconds"), 0.0)
    quote = {
        "bid": contract.get("bid_price"),
        "ask": contract.get("ask_price"),
        "mark": contract.get("mark_price"),
        "updated_at": (_as_et(now_et) - timedelta(seconds=quote_age_seconds)).isoformat(),
        "volume": contract.get("volume", 0),
        "open_interest": contract.get("open_interest", 0),
    }
    if len(vix_bars) < 16 or len(proxy_bars) < 6:
        telemetry = _build_vixw_entry_telemetry(
            ticker=proxy_underlying,
            underlying_price=_safe_float(contract.get("underlying_price"), 0.0),
            vix_bars=vix_bars,
            vxx_bars=proxy_bars,
            option_contract=contract,
            option_quote=quote,
            now_et=now_et,
            macro_blocked=bool(_cfg("VIXW_MACRO_BLOCKED", False)),
        )
        telemetry["entry_allowed"] = False
        telemetry["entry_reason"] = ""
        telemetry["skip_reason"] = "BLOCKED_MARKET_BARS_UNAVAILABLE"
        return telemetry
    return _build_vixw_entry_telemetry(
        ticker=proxy_underlying,
        underlying_price=_safe_float(contract.get("underlying_price"), 0.0),
        vix_bars=vix_bars,
        vxx_bars=proxy_bars,
        option_contract=contract,
        option_quote=quote,
        now_et=now_et,
        macro_blocked=bool(_cfg("VIXW_MACRO_BLOCKED", False)),
    )


def _proxy_prefixes() -> tuple[str, ...]:
    default_underlying = str(_cfg("VIXW_OPTION_UNDERLYING_SYMBOL", "VIXY") or "VIXY").upper()
    raw = _cfg("VIXW_POSITION_SYMBOL_PREFIXES", (default_underlying, "VXX", "UVXY"))
    if isinstance(raw, str):
        items = [raw]
    else:
        try:
            items = list(raw)
        except TypeError:
            items = []
    prefixes: list[str] = []
    for item in [default_underlying, *items]:
        prefix = str(item or "").strip().upper()
        if prefix and prefix not in prefixes:
            prefixes.append(prefix)
    return tuple(prefixes or (default_underlying,))


def _is_proxy_symbol(symbol: str) -> bool:
    clean = str(symbol or "").strip().upper()
    return bool(clean) and clean.startswith(_proxy_prefixes())


_ACTIVE_ORDER_STATUSES = {
    "accepted",
    "accepted_for_bidding",
    "calculated",
    "held",
    "new",
    "partially_filled",
    "pending_cancel",
    "pending_new",
    "pending_replace",
    "stopped",
    "suspended",
}


def _is_active_order(order: Any) -> bool:
    return _order_field_text(order, "status") in _ACTIVE_ORDER_STATUSES


def _is_proxy_buy_order(order: Any) -> bool:
    return (
        _is_proxy_symbol(_order_symbol(order))
        and _order_field_text(order, "side") == "buy"
    )


def _recent_orders(broker: AlpacaBroker) -> tuple[list[Any], bool]:
    limit = max(50, int(_cfg("VIXW_RECENT_ORDER_LOOKBACK_LIMIT", 500) or 500))
    try:
        return list(broker.get_recent_orders(limit=limit) or []), True
    except Exception as exc:  # noqa: BLE001
        print(f"[vix_proxy] recent order lookup failed: {exc}")
        return [], False


def _describe_orders(orders: list[Any], *, max_items: int = 3) -> str:
    labels = []
    for order in orders[:max(1, int(max_items))]:
        symbol = _order_symbol(order) or "unknown"
        status = _order_field_text(order, "status") or "unknown"
        labels.append(f"{symbol}:{status}")
    more = len(orders) - len(labels)
    if more > 0:
        labels.append(f"+{more} more")
    return ", ".join(labels)


def _trading_control_block_reason() -> str | None:
    if not bool(_cfg("VIXW_REQUIRE_TRADING_CONTROL_CLEAR", True)):
        return None
    try:
        control = load_trading_control()
    except Exception as exc:  # noqa: BLE001
        return f"trading control lookup failed; conservative block ({exc})"
    if bool(control.get("manual_stop", False)):
        return "manual stop enabled"
    if bool(control.get("dry_run", False)):
        return "dry run enabled"
    return None


def _daily_cap_excluded_statuses() -> set[str]:
    excluded = {"expired", "rejected"}
    if not bool(_cfg("VIXW_COUNT_CANCELED_ORDERS_IN_DAILY_CAP", True)):
        excluded.add("canceled")
    return excluded


def _proxy_position_exposure_reason(broker: AlpacaBroker) -> str | None:
    try:
        positions = broker.get_open_option_positions()
    except Exception as exc:  # noqa: BLE001
        return f"position lookup failed; conservative block ({exc})"

    count = 0
    for pos in positions:
        symbol = str(getattr(pos, "symbol", "") or "").upper()
        qty = _safe_int(getattr(pos, "qty", 0), 0)
        if qty > 0 and _is_proxy_symbol(symbol):
            count += 1
    max_positions = int(_cfg("VIXW_MAX_OPEN_POSITIONS", 1) or 1)
    if count >= max_positions:
        return f"existing volatility proxy position exposure ({count}/{max_positions})"
    return None


def _position_avg_entry_price(position: Any) -> float:
    for field in ("avg_entry_price", "average_entry_price", "cost_basis"):
        value = _safe_float(getattr(position, field, None), 0.0)
        if value <= 0:
            continue
        if field == "cost_basis":
            qty = _safe_int(getattr(position, "qty", 0), 0)
            if qty > 0:
                return value / (qty * 100.0)
            continue
        return value
    return 0.0


def _position_opened_at_et(position: Any) -> datetime | None:
    raw = (
        getattr(position, "opened_at", None)
        or getattr(position, "created_at", None)
        or getattr(position, "asset_created_at", None)
    )
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return _as_et(raw)
    try:
        return _as_et(datetime.fromisoformat(str(raw).replace("Z", "+00:00")))
    except Exception:
        return None


def _cancel_open_proxy_sell_orders(broker: AlpacaBroker, symbol: str) -> int:
    canceled = 0
    try:
        orders = broker.get_open_orders_for_symbol(symbol=symbol, side="sell")
    except Exception as exc:  # noqa: BLE001
        print(f"[vix_proxy] open sell lookup failed for {symbol}: {exc}")
        return canceled
    for order in orders or []:
        order_id = str(getattr(order, "id", "") or "")
        if not order_id:
            continue
        try:
            broker.cancel_order(order_id)
            canceled += 1
        except Exception as exc:  # noqa: BLE001
            print(f"[vix_proxy] cancel profit trap failed for {symbol}/{order_id}: {exc}")
    return canceled


def _manage_proxy_stop_losses(
    broker: AlpacaBroker,
    data_client: AlpacaDataClient,
    now_et: datetime,
) -> list[dict[str, Any]]:
    if not bool(_cfg("VIXW_MANAGE_OPEN_POSITIONS", True)):
        return []
    stop_loss_pct = abs(float(_cfg("VIXW_STOP_LOSS_PCT", 0.15) or 0.0))
    if stop_loss_pct <= 0:
        return []

    try:
        positions = broker.get_open_option_positions()
    except Exception as exc:  # noqa: BLE001
        print(f"[vix_proxy] stop-loss position lookup failed: {exc}")
        return []

    actions: list[dict[str, Any]] = []
    for pos in positions or []:
        symbol = str(getattr(pos, "symbol", "") or "").upper()
        qty = _safe_int(getattr(pos, "qty", 0), 0)
        entry_price = _position_avg_entry_price(pos)
        if qty <= 0 or not _is_proxy_symbol(symbol) or entry_price <= 0:
            continue

        try:
            quote = data_client.get_latest_option_quote(symbol)
        except Exception as exc:  # noqa: BLE001
            print(f"[vix_proxy] stop-loss quote lookup failed for {symbol}: {exc}")
            continue
        bid = _safe_float(quote.get("bid"), 0.0)
        mark, mark_source = _safe_option_mark(quote)
        if bid <= 0 or mark <= 0:
            continue

        opened_at = _position_opened_at_et(pos)
        held_minutes = (
            max(0.0, (_as_et(now_et) - opened_at).total_seconds() / 60.0)
            if opened_at is not None else 0.0
        )
        exit_decision = _position_exit_decision(
            entry_price=entry_price,
            mark=mark,
            bid=bid,
            held_minutes=held_minutes,
        )
        action = str(exit_decision.get("action") or "")
        if action in {"hold", "profit"}:
            decision = "position_hold" if action == "hold" else "profit_target_observed"
            action_row = {
                "timestamp": now_et.isoformat(),
                "decision": decision,
                "reason": str(exit_decision.get("reason") or ""),
                "option_symbol": symbol,
                "bid": bid,
                "option_bid": bid,
                "option_mark": mark,
                "qty": qty,
                "profit_order_status": action,
            }
            _log_decision(action_row)
            actions.append(action_row)
            continue
        if action != "close":
            continue

        canceled = _cancel_open_proxy_sell_orders(broker, symbol)
        try:
            order = broker.close_option_market(symbol, qty)
            order_id = str(getattr(order, "id", "") or "")
            status = _order_field_text(order, "status") or "submitted"
            decision = "stop_loss_exit_submitted"
            reason = (
                f"{exit_decision.get('reason')}: mark {mark:.2f} ({mark_source}) "
                f"bid {bid:.2f} from entry {entry_price:.2f}; "
                f"held_minutes={held_minutes:.1f}; canceled_sell_orders={canceled}"
            )
        except Exception as exc:  # noqa: BLE001
            order_id = ""
            status = f"submit_failed:{type(exc).__name__}"
            decision = "stop_loss_exit_failed"
            reason = (
                f"{exit_decision.get('reason')}: mark {mark:.2f} ({mark_source}) "
                f"bid {bid:.2f} from entry {entry_price:.2f}; "
                f"close failed: {str(exc)[:180]}"
            )

        action = {
            "timestamp": now_et.isoformat(),
            "decision": decision,
            "reason": reason,
            "option_symbol": symbol,
            "bid": bid,
            "option_bid": bid,
            "option_mark": mark,
            "qty": qty,
            "profit_order_id": order_id,
            "profit_order_status": status,
        }
        _log_decision(action)
        actions.append(action)
    return actions


def _proxy_entry_block_reason(broker: AlpacaBroker, now_et: datetime) -> str | None:
    control_reason = _trading_control_block_reason()
    if control_reason:
        return control_reason

    position_reason = _proxy_position_exposure_reason(broker)
    if position_reason:
        return position_reason

    orders, orders_ok = _recent_orders(broker)
    if not orders_ok:
        return "proxy order lookup failed; conservative block"

    proxy_buys = [order for order in orders if _is_proxy_buy_order(order)]
    active_orders = [order for order in proxy_buys if _is_active_order(order)]
    if bool(_cfg("VIXW_INCLUDE_OPEN_ORDERS_IN_EXPOSURE", True)) and active_orders:
        return f"existing proxy buy order open ({_describe_orders(active_orders)})"

    max_daily = int(_cfg("VIXW_MAX_BUY_ORDERS_PER_DAY", 1) or 0)
    if max_daily > 0:
        excluded_statuses = _daily_cap_excluded_statuses()
        today_orders = []
        for order in proxy_buys:
            submitted_at = _order_submitted_at_et(order)
            if submitted_at is None or submitted_at.date() != now_et.date():
                continue
            if _order_field_text(order, "status") in excluded_statuses:
                continue
            today_orders.append(order)
        if len(today_orders) >= max_daily:
            return (
                f"daily proxy buy order cap reached "
                f"({len(today_orders)}/{max_daily}: {_describe_orders(today_orders)})"
            )
    return None


def _proxy_client_order_id(now_et: datetime, symbol: str) -> str:
    return f"abvix-{now_et.strftime('%Y%m%d')}-{str(symbol or '').upper()}"[:48]


def _add_trading_days(start: date, days: int) -> date:
    cursor = start
    count = 0
    while count < max(0, int(days)):
        cursor += timedelta(days=1)
        if cursor.weekday() < 5:
            count += 1
    return cursor


def _contract_symbol(contract: dict[str, Any]) -> str:
    return str(contract.get("symbol") or contract.get("option_symbol") or "").strip()


def _contract_expiration(contract: dict[str, Any]) -> str:
    return str(contract.get("expiration_date") or contract.get("expiration") or "").strip()


def _contract_strike(contract: dict[str, Any]) -> float:
    return _safe_float(contract.get("strike_price", contract.get("strike")), 0.0)


def _latest_vix_level() -> float | None:
    symbol = str(_cfg("VIXW_SIGNAL_SOURCE_SYMBOL", "^VIX") or "^VIX").strip()
    try:
        ticker = yf.Ticker(symbol)
        fast_info = getattr(ticker, "fast_info", None)
        if fast_info is not None:
            price = _safe_float(getattr(fast_info, "last_price", None), 0.0)
            if price > 0:
                return price
    except Exception as exc:  # noqa: BLE001
        print(f"[vix_proxy] fast_info failed for {symbol}: {exc}")

    try:
        history = yf.Ticker(symbol).history(period="5d", interval="1m", auto_adjust=False)
        if history is not None and not history.empty:
            close = float(history["Close"].dropna().iloc[-1])
            if close > 0:
                return close
    except Exception as exc:  # noqa: BLE001
        print(f"[vix_proxy] history failed for {symbol}: {exc}")
    return None


def _regime_direction(vix_level: float) -> tuple[str | None, str]:
    average = float(_cfg("VIXW_REGIME_AVERAGE_LEVEL", 19.0) or 19.0)
    neutral_band = float(_cfg("VIXW_NEUTRAL_BAND", 0.0) or 0.0)
    if vix_level < (average - neutral_band):
        return "call", f"VIX {vix_level:.2f} below average {average:.2f}; proxy CALL bias"
    if vix_level > (average + neutral_band):
        return "put", f"VIX {vix_level:.2f} above average {average:.2f}; proxy PUT bias"
    return None, f"VIX {vix_level:.2f} inside neutral band around {average:.2f}"


def _has_proxy_exposure(broker: AlpacaBroker) -> bool:
    return _proxy_position_exposure_reason(broker) is not None


def _select_proxy_contract(
    data_client: AlpacaDataClient,
    *,
    direction: str,
    now_et: datetime,
) -> tuple[dict[str, Any] | None, str]:
    underlying = str(_cfg("VIXW_OPTION_UNDERLYING_SYMBOL", "VIXY") or "VIXY").strip().upper()
    min_dte = int(_cfg("VIXW_MIN_DTE_TRADING_DAYS", 2) or 2)
    max_dte = int(_cfg("VIXW_MAX_DTE_TRADING_DAYS", 7) or 7)
    expiry_floor = _add_trading_days(now_et.date(), min_dte)
    expiry_ceiling = _add_trading_days(now_et.date(), max_dte)

    underlying_price = data_client.get_latest_stock_price(underlying)
    if underlying_price is None or underlying_price <= 0:
        return None, f"proxy underlying price unavailable for {underlying}"

    contracts = data_client.get_option_contracts(
        underlying_symbol=underlying,
        contract_type=direction,
        expiration_date_gte=expiry_floor,
        expiration_date_lte=expiry_ceiling,
    )
    if not contracts:
        return None, f"no {underlying}/{direction} contracts in {min_dte}-{max_dte} trading-day window"

    scored: list[dict[str, Any]] = []
    for raw in contracts:
        contract = dict(raw)
        symbol = _contract_symbol(contract)
        strike = _contract_strike(contract)
        expiration = _contract_expiration(contract)
        active = str(contract.get("status", "active")).lower() == "active"
        tradable = bool(contract.get("tradable", True))
        if not symbol or strike <= 0 or not expiration or not active or not tradable:
            continue
        contract["symbol"] = symbol
        contract["strike_price"] = strike
        contract["expiration_date"] = expiration
        contract["_select_score"] = abs(strike - float(underlying_price))
        scored.append(contract)

    if not scored:
        return None, f"{underlying} chain returned no active/tradable contracts with usable strikes"

    scored.sort(key=lambda item: (float(item.get("_select_score", 999.0)), str(item.get("expiration_date", ""))))
    max_spread = float(_cfg("VIXW_MAX_OPTION_SPREAD_PCT", getattr(config, "MAX_OPTION_SPREAD_PCT", 30.0)) or 30.0)
    for contract in scored[:50]:
        symbol = str(contract.get("symbol", "") or "")
        quote = data_client.get_latest_option_quote(symbol)
        bid = _safe_float(quote.get("bid"), 0.0)
        ask = _safe_float(quote.get("ask"), 0.0)
        mark, mark_source = _safe_option_mark(quote)
        quote_age = _quote_age_seconds(quote, now_et)
        max_quote_age = float(_cfg("VIXW_MAX_QUOTE_AGE_SECONDS", 60) or 60)
        volume = _safe_int(contract.get("volume", quote.get("volume", 0)), 0)
        open_interest = _safe_int(contract.get("open_interest", quote.get("open_interest", 0)), 0)
        min_volume = int(_cfg("VIXW_MIN_OPTION_VOLUME", 0) or 0)
        min_open_interest = int(_cfg("VIXW_MIN_OPTION_OPEN_INTEREST", 0) or 0)
        if bid <= 0 or ask <= 0 or ask <= bid or mark <= 0:
            continue
        if quote_age > max_quote_age:
            continue
        if min_volume > 0 and volume < min_volume:
            continue
        if min_open_interest > 0 and open_interest < min_open_interest:
            continue
        midpoint = mark
        spread_pct = ((ask - bid) / midpoint) * 100.0 if midpoint > 0 else 999.0
        if spread_pct > max_spread:
            continue
        contract["bid_price"] = bid
        contract["ask_price"] = ask
        contract["mark_price"] = round(mark, 4)
        contract["mark_source"] = mark_source
        contract["quote_age_seconds"] = round(quote_age, 2)
        contract["volume"] = volume
        contract["open_interest"] = open_interest
        contract["dte"] = _contract_dte(contract, now_et)
        contract["spread_pct"] = round(spread_pct, 2)
        contract["underlying_price"] = round(float(underlying_price), 4)
        contract["proxy_underlying"] = underlying
        return contract, "ok"

    return None, (
        f"no proxy contract passed quote/spread/liquidity gate "
        f"max_spread={max_spread:.2f}%"
    )


def _order_fill_snapshot(order: Any) -> tuple[int, float | None, str]:
    status = _order_field_text(order, "status") or "unknown"
    filled_qty = _safe_int(getattr(order, "filled_qty", 0), 0)
    fill_price = _safe_float(getattr(order, "filled_avg_price", None), 0.0)
    if fill_price <= 0:
        fill_price = _safe_float(getattr(order, "average_fill_price", None), 0.0)
    return filled_qty, fill_price if fill_price > 0 else None, status


def _await_proxy_entry_fill(
    broker: AlpacaBroker,
    *,
    order_id: str,
    requested_qty: int,
) -> dict[str, Any]:
    if not order_id:
        return {"filled": False, "status": "missing_order_id", "filled_qty": 0, "filled_price": None}

    wait_seconds = max(0, int(_cfg("VIXW_ENTRY_ORDER_STATUS_WAIT_SECONDS", 8) or 8))
    poll_seconds = max(1, int(_cfg("VIXW_ENTRY_ORDER_POLL_SECONDS", 1) or 1))
    terminal_no_fill = {"canceled", "expired", "rejected"}
    deadline = time.time() + wait_seconds
    last_status = "submitted"
    last_filled_qty = 0
    last_fill_price: float | None = None

    while True:
        try:
            order = broker.get_order_status(order_id)
            filled_qty, fill_price, status = _order_fill_snapshot(order)
        except Exception as exc:  # noqa: BLE001
            return {
                "filled": False,
                "status": f"status_lookup_error:{type(exc).__name__}",
                "filled_qty": last_filled_qty,
                "filled_price": last_fill_price,
            }

        last_status = status
        last_filled_qty = filled_qty
        last_fill_price = fill_price
        if filled_qty > 0:
            if filled_qty < requested_qty and _is_active_order(order):
                try:
                    broker.cancel_order(order_id)
                except Exception as exc:  # noqa: BLE001
                    print(f"[vix_proxy] partial-fill cancel failed for {order_id}: {exc}")
            return {
                "filled": True,
                "status": status,
                "filled_qty": min(filled_qty, requested_qty),
                "filled_price": fill_price,
            }
        if status in terminal_no_fill or time.time() >= deadline:
            break
        time.sleep(poll_seconds)

    cancel_note = ""
    if bool(_cfg("VIXW_CANCEL_UNFILLED_ENTRY_ORDERS", True)):
        try:
            broker.cancel_order(order_id)
            cancel_note = "cancel_requested"
        except Exception as exc:  # noqa: BLE001
            cancel_note = f"cancel_failed:{type(exc).__name__}"
            print(f"[vix_proxy] cancel unfilled entry order {order_id} failed: {exc}")

    return {
        "filled": False,
        "status": last_status,
        "filled_qty": last_filled_qty,
        "filled_price": last_fill_price,
        "cancel_note": cancel_note,
    }


def _submit_proxy_limit_buy(
    broker: AlpacaBroker,
    *,
    symbol: str,
    qty: int,
    limit_price: float,
    now_et: datetime,
) -> dict[str, Any]:
    order = broker.place_option_limit_buy(
        symbol,
        qty,
        limit_price,
        client_order_id=_proxy_client_order_id(now_et, symbol),
    )
    order_id = str(getattr(order, "id", "") or "")
    result = _await_proxy_entry_fill(broker, order_id=order_id, requested_qty=qty)
    result["order_id"] = order_id
    return result


def _profit_target_price(fill_price: float) -> float:
    target_multiplier = float(_cfg("VIXW_PROFIT_TARGET_MULTIPLIER", 1.25) or 1.25)
    min_increment = float(_cfg("VIXW_MIN_PROFIT_TARGET_INCREMENT", 0.01) or 0.01)
    raw_target = float(fill_price) * target_multiplier
    if min_increment > 0:
        raw_target = max(raw_target, float(fill_price) + min_increment)
    return round(raw_target, 2)


def _submit_proxy_profit_trap(
    broker: AlpacaBroker,
    *,
    symbol: str,
    qty: int,
    fill_price: float,
) -> dict[str, Any]:
    if not bool(_cfg("VIXW_PLACE_PROFIT_TRAP_AFTER_FILL", True)):
        return {"submitted": False, "status": "disabled", "target_price": ""}
    if qty <= 0 or fill_price <= 0:
        return {"submitted": False, "status": "invalid_fill", "target_price": ""}

    target_price = _profit_target_price(fill_price)
    try:
        order = broker.place_option_limit_sell(symbol, qty, target_price)
    except Exception as exc:  # noqa: BLE001
        return {
            "submitted": False,
            "status": f"submit_failed:{type(exc).__name__}",
            "target_price": target_price,
            "error": str(exc)[:250],
        }
    return {
        "submitted": True,
        "status": _order_field_text(order, "status") or "submitted",
        "target_price": target_price,
        "order_id": str(getattr(order, "id", "") or ""),
    }


def _coerce_legacy_row(row: dict[str, Any]) -> dict[str, Any]:
    payload = {key: row.get(key, "") for key in VIX_PROXY_LOG_COLUMNS}
    if not payload.get("proxy_underlying"):
        payload["proxy_underlying"] = str(_cfg("VIXW_OPTION_UNDERLYING_SYMBOL", "VIXY") or "VIXY").upper()
    return payload


def _ensure_log_header(path: Path) -> None:
    if not path.exists():
        return
    try:
        with path.open("r", newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            existing_columns = list(reader.fieldnames or [])
            rows = list(reader)
        if existing_columns == VIX_PROXY_LOG_COLUMNS:
            return

        migrated: list[dict[str, Any]] = []
        if existing_columns == LEGACY_VIX_PROXY_LOG_COLUMNS:
            for row in rows:
                payload = _coerce_legacy_row(row)
                payload["proxy_underlying_price"] = ""
                migrated.append(payload)
        else:
            for row in rows:
                # Handles malformed rows created after the schema changed while
                # the old CSV header still existed. Those rows often show
                # decision=unknown and reason=call/put in the journal.
                values = [row.get(key, "") for key in existing_columns]
                extra = row.get(None, [])
                if isinstance(extra, list):
                    values.extend(extra)
                payload = {key: (values[idx] if idx < len(values) else "") for idx, key in enumerate(VIX_PROXY_LOG_COLUMNS)}
                if not payload.get("proxy_underlying"):
                    payload["proxy_underlying"] = str(_cfg("VIXW_OPTION_UNDERLYING_SYMBOL", "VIXY") or "VIXY").upper()
                migrated.append(payload)

        backup = path.with_suffix(path.suffix + ".bak")
        try:
            if not backup.exists():
                backup.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
        except Exception:
            pass
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=VIX_PROXY_LOG_COLUMNS)
            writer.writeheader()
            for row in migrated:
                writer.writerow({key: row.get(key, "") for key in VIX_PROXY_LOG_COLUMNS})
        print(f"[vix_proxy] migrated decision log header at {path}")
    except Exception as exc:  # noqa: BLE001
        print(f"[vix_proxy] log header migration failed: {exc}")


def _log_decision(row: dict[str, Any]) -> None:
    path = Path(_cfg("VIXW_REGIME_LOG_CSV_PATH", Path(config.DATA_DIR) / "vixw_regime_log.csv"))
    path.parent.mkdir(parents=True, exist_ok=True)
    _ensure_log_header(path)
    write_header = not path.exists()
    payload = {key: row.get(key, "") for key in VIX_PROXY_LOG_COLUMNS}
    if not payload.get("proxy_underlying"):
        payload["proxy_underlying"] = str(_cfg("VIXW_OPTION_UNDERLYING_SYMBOL", "VIXY") or "VIXY").upper()
    try:
        with path.open("a", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=VIX_PROXY_LOG_COLUMNS)
            if write_header:
                writer.writeheader()
            writer.writerow(payload)
    except Exception as exc:  # noqa: BLE001
        print(f"[vix_proxy] decision log failed: {exc}")


def run_vixw_regime_forever(api_key: str, secret_key: str) -> None:
    runtime_telemetry.set_worker("vixw_regime_sidecar", True)
    if not bool(_cfg("VIXW_HEAVY_MODE", True)):
        print("[vix_proxy] VIXW_HEAVY_MODE disabled.")
        return
    if bool(_cfg("VIXW_ONLY_PAPER_MODE", True)) and not bool(getattr(config, "PAPER", True)):
        print("[vix_proxy] disabled because VIXW_ONLY_PAPER_MODE=True and PAPER=False.")
        return

    broker = AlpacaBroker(api_key, secret_key, paper=bool(getattr(config, "PAPER", True)))
    data_client = AlpacaDataClient(api_key, secret_key, paper=bool(getattr(config, "PAPER", True)))
    last_entry_at: datetime | None = None
    sleep_seconds = max(15, int(_cfg("VIXW_POLL_SECONDS", 60) or 60))
    cooldown_seconds = max(60, int(_cfg("VIXW_MIN_SECONDS_BETWEEN_ENTRIES", 1800) or 1800))

    print("[vix_proxy] VIX-derived proxy sidecar started.")
    while True:
        runtime_telemetry.set_last_loop("vixw_regime_loop")
        now_et = _now_et()
        try:
            clock = broker.get_clock()
            if not bool(getattr(clock, "is_open", False)):
                time.sleep(sleep_seconds)
                continue

            stop_actions = _manage_proxy_stop_losses(broker, data_client, now_et)
            if stop_actions:
                time.sleep(sleep_seconds)
                continue

            if last_entry_at is not None and (now_et - last_entry_at).total_seconds() < cooldown_seconds:
                time.sleep(sleep_seconds)
                continue

            entry_block_reason = _proxy_entry_block_reason(broker, now_et)
            if entry_block_reason:
                _log_decision({"timestamp": now_et.isoformat(), "decision": "skip", "reason": entry_block_reason})
                time.sleep(sleep_seconds)
                continue

            if bool(_cfg("VIXW_MACRO_BLOCKED", False)):
                _log_decision({
                    "timestamp": now_et.isoformat(),
                    "decision": "skip",
                    "reason": "BLOCKED_MACRO_NEWS_EVENT",
                    "entry_allowed": False,
                    "skip_reason": "BLOCKED_MACRO_NEWS_EVENT",
                })
                time.sleep(sleep_seconds)
                continue

            vix_level = _latest_vix_level()
            if vix_level is None or vix_level <= 0:
                _log_decision({"timestamp": now_et.isoformat(), "decision": "skip", "reason": "VIX level unavailable"})
                time.sleep(sleep_seconds)
                continue

            direction, regime_reason = _regime_direction(vix_level)
            if direction is None:
                _log_decision({"timestamp": now_et.isoformat(), "vix_level": round(vix_level, 4), "average_level": _cfg("VIXW_REGIME_AVERAGE_LEVEL", 19.0), "decision": "skip", "reason": regime_reason})
                time.sleep(sleep_seconds)
                continue

            contract, contract_reason = _select_proxy_contract(data_client, direction=direction, now_et=now_et)
            if not contract:
                print(f"[vix_proxy] skip: {contract_reason}")
                _log_decision({"timestamp": now_et.isoformat(), "vix_level": round(vix_level, 4), "average_level": _cfg("VIXW_REGIME_AVERAGE_LEVEL", 19.0), "direction": direction, "decision": "skip", "reason": contract_reason})
                time.sleep(sleep_seconds)
                continue

            telemetry = _runtime_entry_telemetry(data_client, contract=contract, now_et=now_et)
            runtime_telemetry.set_last_candidate(str(telemetry.get("option_symbol") or contract.get("symbol") or ""))
            if not bool(telemetry.get("entry_allowed")):
                skip_reason = str(telemetry.get("skip_reason") or "BLOCKED_VIXW_ENTRY_GATE")
                _log_decision({
                    "timestamp": now_et.isoformat(),
                    "average_level": _cfg("VIXW_REGIME_AVERAGE_LEVEL", 19.0),
                    "direction": direction,
                    "decision": "skip",
                    "reason": skip_reason,
                    **telemetry,
                })
                print(
                    f"[vix_proxy] skip {contract.get('symbol', '')}: {skip_reason} "
                    f"vix={telemetry.get('vix_level', '')} "
                    f"roc1={telemetry.get('vix_1m_roc', '')} "
                    f"roc5={telemetry.get('vix_5m_roc', '')} "
                    f"wick={telemetry.get('upper_wick_ratio', '')} "
                    f"spread={telemetry.get('spread_pct', '')} "
                    f"dte={telemetry.get('dte', '')}"
                )
                time.sleep(sleep_seconds)
                continue

            ask = _safe_float(contract.get("ask_price"), 0.0)
            if ask <= 0:
                time.sleep(sleep_seconds)
                continue
            budget = float(_cfg("VIXW_POSITION_SIZE_USD", 600.0) or 600.0)
            qty = max(1, int(budget // (ask * 100.0)))
            max_qty = int(_cfg("VIXW_MAX_CONTRACTS_PER_ENTRY", 3) or 3)
            qty = max(1, min(qty, max_qty))
            limit_multiplier = float(_cfg("VIXW_ENTRY_LIMIT_PRICE_MULTIPLIER", 1.0) or 1.0)
            limit_price = round(ask * limit_multiplier, 2)
            symbol = str(contract.get("symbol", "") or "")

            entry_result = _submit_proxy_limit_buy(
                broker,
                symbol=symbol,
                qty=qty,
                limit_price=limit_price,
                now_et=now_et,
            )
            last_entry_at = now_et
            order_id = str(entry_result.get("order_id", "") or "")
            filled_qty = _safe_int(entry_result.get("filled_qty"), 0)
            filled_price = _safe_float(entry_result.get("filled_price"), 0.0)
            status = str(entry_result.get("status", "unknown") or "unknown")
            if bool(entry_result.get("filled")):
                decision = "filled_buy"
                reason = regime_reason
                log_qty = filled_qty or qty
                profit_result = _submit_proxy_profit_trap(
                    broker,
                    symbol=symbol,
                    qty=log_qty,
                    fill_price=filled_price,
                )
                profit_status = str(profit_result.get("status", "") or "")
                profit_target = profit_result.get("target_price", "")
                profit_order_id = str(profit_result.get("order_id", "") or "")
                if bool(profit_result.get("submitted")):
                    decision = "filled_buy_profit_trap"
                    reason = f"{reason}; profit trap submitted at {float(profit_target):.2f}"
                else:
                    reason = f"{reason}; profit trap {profit_status}"
                    error = str(profit_result.get("error", "") or "").strip()
                    if error:
                        reason = f"{reason}: {error}"
                print(
                    f"[vix_proxy] filled {direction.upper()} buy {symbol} qty={log_qty} "
                    f"limit={limit_price:.2f} fill={filled_price:.2f} vix={vix_level:.2f} order={order_id}"
                )
            else:
                cancel_note = str(entry_result.get("cancel_note", "") or "").strip()
                decision = "entry_not_filled"
                reason = f"{regime_reason}; entry status={status}"
                if cancel_note:
                    reason = f"{reason}; {cancel_note}"
                log_qty = qty
                profit_status = ""
                profit_target = ""
                profit_order_id = ""
                print(
                    f"[vix_proxy] submitted but not filled {direction.upper()} buy {symbol} "
                    f"qty={qty} limit={limit_price:.2f} status={status} order={order_id}"
                )
            _log_decision({
                "timestamp": now_et.isoformat(),
                "vix_level": round(vix_level, 4),
                "average_level": _cfg("VIXW_REGIME_AVERAGE_LEVEL", 19.0),
                "proxy_underlying": contract.get("proxy_underlying", ""),
                "proxy_underlying_price": contract.get("underlying_price", ""),
                "direction": direction,
                "decision": decision,
                "reason": reason,
                "option_symbol": symbol,
                "strike": contract.get("strike_price", ""),
                "expiration": contract.get("expiration_date", ""),
                "ask": ask,
                "bid": contract.get("bid_price", ""),
                "option_bid": contract.get("bid_price", ""),
                "option_ask": ask,
                "option_mark": contract.get("mark_price", ""),
                "spread_pct": contract.get("spread_pct", ""),
                "dte": contract.get("dte", ""),
                "volume": contract.get("volume", ""),
                "open_interest": contract.get("open_interest", ""),
                "quote_age_seconds": contract.get("quote_age_seconds", ""),
                **telemetry,
                "qty": log_qty,
                "profit_target_price": profit_target,
                "profit_order_id": profit_order_id,
                "profit_order_status": profit_status,
            })
        except Exception as exc:  # noqa: BLE001
            print(f"[vix_proxy] sidecar error: {exc}")
            _log_decision({"timestamp": now_et.isoformat(), "decision": "error", "reason": str(exc)[:250]})
        time.sleep(sleep_seconds)
