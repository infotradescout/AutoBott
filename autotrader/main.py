"""Entry point for the intraday options autotrader."""

from __future__ import annotations

import time
from datetime import date, datetime, timedelta
import re

import pytz
import yfinance as yf

from env_config import get_required_env, load_runtime_env
load_runtime_env()

import config
from alerts import AlertManager
from broker import AlpacaBroker
from data import AlpacaDataClient
from logger import TradeLogger
from options import select_atm_option_contract_with_reason
from risk import (
    can_open_new_positions,
    is_at_or_after,
    position_matches_ticker,
)
from scanner import initialize_scanner, run_observation_phase, run_scan, set_catalyst_mode
from state_store import load_bot_state, save_bot_state
from trading_control import load_trading_control


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


def _live_option_mark_and_plpc(
    data_client: AlpacaDataClient,
    option_symbol: str,
    entry_price: float,
) -> tuple[float | None, float | None]:
    if entry_price <= 0:
        return None, None
    try:
        quote = data_client.get_latest_option_quote(option_symbol)
        bid_raw = quote.get("bid")
        ask_raw = quote.get("ask")
        bid = float(bid_raw) if bid_raw is not None else 0.0
        ask = float(ask_raw) if ask_raw is not None else 0.0
        mark: float | None = None
        if bid > 0 and ask > 0 and ask >= bid:
            mark = (bid + ask) / 2.0
        elif ask > 0:
            mark = ask
        elif bid > 0:
            mark = bid
        if mark is None or mark <= 0:
            return None, None
        return mark, ((mark - entry_price) / entry_price)
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


def _parse_option_symbol(option_symbol: str) -> tuple[str, str]:
    symbol = str(option_symbol or "").upper().strip()
    match = _OPTION_SYMBOL_RE.match(symbol)
    if not match:
        return "", ""
    ticker = match.group(1)
    cp = match.group(2)
    direction = "call" if cp == "C" else "put"
    return ticker, direction


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
        if direction == "call":
            return last_close > prev_close
        return last_close < prev_close
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
            "ticker": ticker,
            "direction": direction,
            "option_symbol": symbol,
            "strike": "",
            "expiry": "",
            "qty": qty,
            "entry_price": entry_price,
            "stop_floor_plpc": -float(config.STOP_LOSS_PCT),
            "max_plpc": 0.0,
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
    base = list(dict.fromkeys(base + core))
    combined = list(base)
    if config.AUTO_EXPAND_UNIVERSE_WITH_MOVERS:
        try:
            gainers, losers = data_client.get_top_movers(top=int(config.UNIVERSE_MOVER_TOP))
            combined.extend(str(sym).upper() for sym in gainers if str(sym).strip())
            combined.extend(str(sym).upper() for sym in losers if str(sym).strip())
        except Exception as exc:  # noqa: BLE001
            print(f"[{ts()}] Universe expansion skipped (movers unavailable): {exc}")
    deduped = list(dict.fromkeys(combined))
    max_tickers = max(1, int(config.UNIVERSE_MAX_TICKERS))
    return deduped[:max_tickers]


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
    last_entry_debug: dict = dict(state.get("last_entry_debug") or {})
    last_exit_debug: dict = dict(state.get("last_exit_debug") or {})
    last_trader_heartbeat_et = str(state.get("last_trader_heartbeat_et", "") or "")
    last_alpaca_auth_error_et = str(state.get("last_alpaca_auth_error_et", "") or "")
    last_alpaca_auth_error = str(state.get("last_alpaca_auth_error", "") or "")
    next_heartbeat_at = 0.0
    manual_stop_latched = False
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
                "last_entry_debug": last_entry_debug,
                "last_exit_debug": last_exit_debug,
                "last_trader_heartbeat_et": last_trader_heartbeat_et,
                "last_alpaca_auth_error_et": last_alpaca_auth_error_et,
                "last_alpaca_auth_error": last_alpaca_auth_error,
            }
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
        if direction not in ("call", "put"):
            return False
        if not is_at_or_after(now_et, config.NO_NEW_TRADES_BEFORE):
            print(f"[{ts(now_et)}] {ticker}: reversal skipped (before entry window).")
            return False
        if is_at_or_after(now_et, config.NO_NEW_TRADES_AFTER):
            print(f"[{ts(now_et)}] {ticker}: reversal skipped (after entry window).")
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

            ask_price = data_client.get_latest_option_ask(option_symbol)
            direct_market_entry = False
            if ask_price is None or ask_price <= 0:
                if config.EMERGENCY_EXECUTION_MODE and config.ALLOW_MARKET_ENTRY_WITHOUT_QUOTE:
                    direct_market_entry = True
                    ask_price = 0.0
                else:
                    print(f"[{ts(now_et)}] {ticker}: reversal skipped (no option ask for {option_symbol}).")
                    return False

            if not direct_market_entry:
                initial_chain_ask = float(contract.get("ask_price", ask_price) or ask_price)
                pre_submit_slippage = _slippage_pct(initial_chain_ask, ask_price)
                if pre_submit_slippage > config.MAX_ENTRY_SLIPPAGE_PCT:
                    retry_ask = data_client.get_latest_option_ask(option_symbol)
                    if retry_ask is not None and retry_ask > 0:
                        ask_price = retry_ask
                        pre_submit_slippage = _slippage_pct(initial_chain_ask, ask_price)
                if pre_submit_slippage > (config.MAX_ENTRY_SLIPPAGE_PCT * 3):
                    print(
                        f"[{ts(now_et)}] {ticker}: reversal skipped (entry slippage {pre_submit_slippage:.2f}% > "
                        f"hard cap {(config.MAX_ENTRY_SLIPPAGE_PCT * 3):.2f}%)."
                    )
                    return False

            qty = 1
            if direct_market_entry:
                order = broker.place_option_market_buy(option_symbol, qty)
                print(
                    f"[{ts(now_et)}] REVERSAL ENTRY {ticker} {direction.upper()} "
                    f"{option_symbol} qty={qty} market order_id={order.id}"
                )
            else:
                order = broker.place_option_limit_buy(option_symbol, qty, ask_price)
                print(
                    f"[{ts(now_et)}] REVERSAL ENTRY {ticker} {direction.upper()} "
                    f"{option_symbol} qty={qty} limit={ask_price:.2f} order_id={order.id}"
                )

            time.sleep(max(1, int(config.ENTRY_ORDER_STATUS_WAIT_SECONDS)))
            filled_order = broker.get_order_status(order.id)
            order_status = str(getattr(filled_order, "status", "")).lower()
            reject_detail = _order_reject_reason(filled_order)

            if order_status not in ("filled", "partially_filled"):
                try:
                    if not direct_market_entry:
                        broker.cancel_order(order.id)
                except Exception as exc:  # noqa: BLE001
                    print(f"[{ts(now_et)}] {ticker}: reversal cancel of {order.id} failed: {exc}")
                if not direct_market_entry:
                    aggressive_limit = round(float(ask_price) * 1.05, 4)
                    try:
                        retry_order = broker.place_option_limit_buy(option_symbol, qty, aggressive_limit)
                        time.sleep(max(1, int(config.ENTRY_RETRY_STATUS_WAIT_SECONDS)))
                        filled_order = broker.get_order_status(retry_order.id)
                        order_status = str(getattr(filled_order, "status", "")).lower()
                        reject_detail = _order_reject_reason(filled_order)
                        if order_status not in ("filled", "partially_filled"):
                            try:
                                broker.cancel_order(retry_order.id)
                            except Exception as exc:  # noqa: BLE001
                                print(f"[{ts(now_et)}] {ticker}: reversal retry cancel of {retry_order.id} failed: {exc}")
                            mkt_order = broker.place_option_market_buy(option_symbol, qty)
                            time.sleep(max(1, int(config.ENTRY_MARKET_FALLBACK_WAIT_SECONDS)))
                            filled_order = broker.get_order_status(mkt_order.id)
                            order_status = str(getattr(filled_order, "status", "")).lower()
                            reject_detail = _order_reject_reason(filled_order)
                    except Exception as exc:  # noqa: BLE001
                        print(f"[{ts(now_et)}] {ticker}: reversal fallback failed: {exc}")
                        return False
                if order_status not in ("filled", "partially_filled"):
                    extra = f" {reject_detail}" if reject_detail else ""
                    print(
                        f"[{ts(now_et)}] {ticker}: reversal not filled "
                        f"(status={order_status}).{extra}"
                    )
                    return False

            filled_avg_price = float(getattr(filled_order, "filled_avg_price", 0) or 0)
            filled_qty = position_qty_as_int(getattr(filled_order, "filled_qty", qty)) or qty
            if filled_qty > 1:
                extra_qty = filled_qty - 1
                try:
                    broker.close_option_market(option_symbol, extra_qty)
                    print(f"[{ts(now_et)}] {ticker}: trimmed reversal fill to 1 contract (closed extra {extra_qty}).")
                except Exception as exc:  # noqa: BLE001
                    print(f"[{ts(now_et)}] {ticker}: failed to trim reversal extra qty {extra_qty}: {exc}")
                filled_qty = 1
            fill_slippage = (
                _slippage_pct(ask_price, filled_avg_price) if (filled_avg_price > 0 and ask_price > 0) else 0.0
            )
            if (not direct_market_entry) and fill_slippage > config.MAX_FILL_SLIPPAGE_PCT:
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
                "ticker": ticker,
                "direction": direction,
                "option_symbol": option_symbol,
                "strike": contract.get("strike_price", ""),
                "expiry": contract.get("expiration_date", ""),
                "qty": filled_qty,
                "entry_price": filled_avg_price or ask_price,
                "stop_floor_plpc": -float(config.STOP_LOSS_PCT),
                "max_plpc": 0.0,
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
    ) -> tuple[int, float | None]:
        request_qty = max(0, int(qty))
        if request_qty <= 0:
            return 0, None

        poll_seconds = max(1, int(config.EXIT_ORDER_STATUS_POLL_SECONDS))
        max_wait_seconds = max(poll_seconds, int(config.EXIT_ORDER_MAX_WAIT_SECONDS))
        retry_attempts = max(1, int(config.EXIT_CLOSE_RETRY_ATTEMPTS))
        non_fill_terminal = {"canceled", "cancelled", "rejected", "expired", "done_for_day", "stopped", "suspended"}

        def _wait_for_fill(order_id: str, close_qty: int) -> tuple[int, float | None, str, bool]:
            deadline = time.time() + max_wait_seconds
            observed_filled = 0
            observed_avg_price: float | None = None
            last_status = ""
            while time.time() < deadline:
                try:
                    status_order = broker.get_order_status(order_id)
                except Exception as exc:  # noqa: BLE001
                    print(f"[{ts(now_et)}] {label} {symbol}: order status error for {order_id}: {exc}")
                    time.sleep(poll_seconds)
                    continue
                last_status = str(getattr(status_order, "status", "")).lower()
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
                    return min(close_qty, observed_filled), observed_avg_price, last_status, False
                if last_status in non_fill_terminal:
                    break
                time.sleep(poll_seconds)
            if observed_filled > 0:
                return min(close_qty, observed_filled), observed_avg_price, last_status, False
            is_still_open = last_status not in non_fill_terminal and last_status not in ("", "filled", "partially_filled")
            return 0, None, last_status, is_still_open

        try:
            existing_sells = broker.get_open_orders_for_symbol(symbol=symbol, side="sell")
            if existing_sells:
                existing_order_id = str(getattr(existing_sells[0], "id", "") or "")
                if existing_order_id:
                    filled_qty, filled_avg_price, status, still_open = _wait_for_fill(existing_order_id, request_qty)
                    if filled_qty > 0:
                        return filled_qty, filled_avg_price
                    if still_open:
                        print(
                            f"[{ts(now_et)}] {label} {symbol} qty={request_qty}: "
                            f"existing close order {existing_order_id} still pending ({status or 'unknown'})."
                        )
                        return 0, None

            for attempt in range(1, retry_attempts + 1):
                order = broker.close_option_market(symbol, request_qty)
                order_id = str(getattr(order, "id", "") or "")
                if not order_id:
                    print(f"[{ts(now_et)}] {label} {symbol} qty={request_qty}: close submitted without order id.")
                    return 0, None
                filled_qty, filled_avg_price, status, still_open = _wait_for_fill(order_id, request_qty)
                if filled_qty > 0:
                    return filled_qty, filled_avg_price
                if still_open:
                    print(
                        f"[{ts(now_et)}] {label} {symbol} qty={request_qty}: "
                        f"close order {order_id} still pending ({status or 'unknown'})."
                    )
                    return 0, None
                if attempt < retry_attempts:
                    print(
                        f"[{ts(now_et)}] {label} {symbol} qty={request_qty}: "
                        f"close attempt {attempt}/{retry_attempts} ended status={status or 'unknown'}, retrying."
                    )
            print(f"[{ts(now_et)}] {label} {symbol} qty={request_qty}: close not filled after {retry_attempts} attempt(s).")
            return 0, None
        except Exception as exc:  # noqa: BLE001
            print(f"[{ts(now_et)}] {label} {symbol} qty={request_qty}: close error: {exc}")
            return 0, None

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
    alerts.send(
        "startup",
        f"Autotrader online ({mode}). Pre-open readiness: {config.PREOPEN_READY_MINUTES}m.",
        dedupe_key="startup",
    )

    while True:
        now_et = datetime.now(tz)
        now_ct = datetime.now(pytz.timezone(config.CENTRAL_TZ))
        last_trader_heartbeat_et = datetime.now(tz).isoformat()
        control_state = load_trading_control()
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
        preopen_window_seconds = int(config.PREOPEN_READY_MINUTES) * 60
        if clock.is_open or (
            seconds_until_open is not None and 0 < seconds_until_open <= preopen_window_seconds
        ):
            break
        sleep_seconds = _closed_market_sleep_seconds(clock, preopen_ready_minutes=config.PREOPEN_READY_MINUTES)
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
        print(
            f"[{ts(now_et)} | {ts_ct(now_ct)}] Market closed. "
            f"Next open (CT): {next_open_ct or 'unknown'}. Sleeping {sleep_seconds}s."
        )
        time.sleep(sleep_seconds)

    print(
        f"[{ts()} | {ts_ct()}] Pre-open readiness window reached "
        f"({config.PREOPEN_READY_MINUTES}m before open) or market already open. Starting loop."
    )
    while True:
        now_et = datetime.now(tz)
        now_ct = datetime.now(pytz.timezone(config.CENTRAL_TZ))
        last_trader_heartbeat_et = now_et.isoformat()
        control_state = load_trading_control()
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
            sleep_seconds = _closed_market_sleep_seconds(clock, preopen_ready_minutes=config.PREOPEN_READY_MINUTES)
            next_open = getattr(clock, "next_open", None)
            next_open_ct = ""
            if next_open is not None:
                if next_open.tzinfo is None:
                    next_open = pytz.utc.localize(next_open)
                next_open_ct = next_open.astimezone(pytz.timezone(config.CENTRAL_TZ)).strftime("%Y-%m-%d %H:%M:%S %Z")
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

        watchlist = _build_scan_universe(data_client)
        if hot_tickers:
            watchlist = hot_tickers + [s for s in watchlist if s not in hot_tickers]
        print(f"[{ts(now_et)}] Running full-universe scan on {len(watchlist)} tickers.")

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

        index_bias = _index_regime_bias(data_client, now_et)
        if index_bias in ("call", "put") and signals:
            before = len(signals)
            signals = [s for s in signals if str(s.get("direction", "")).lower() == index_bias]
            print(f"[{ts(now_et)}] Index bias={index_bias.upper()} filtered signals {before}->{len(signals)}.")
        elif signals:
            print(f"[{ts(now_et)}] Index bias neutral; keeping both call/put signals.")

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
            if not is_at_or_after(now_et, config.NO_NEW_TRADES_BEFORE):
                _mark_skip("before_entry_window")
                print(f"[{ts(now_et)}] Entry window not open yet (before {config.NO_NEW_TRADES_BEFORE} ET).")
                break
            if is_at_or_after(now_et, config.NO_NEW_TRADES_AFTER):
                _mark_skip("after_entry_window")
                print(f"[{ts(now_et)}] Entry window closed (past {config.NO_NEW_TRADES_AFTER} ET).")
                break

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

            prior_entries = int(ticker_entry_counts.get(ticker, 0))
            reentries_used = int(ticker_reentries_used.get(ticker, 0))
            reentry_armed = bool(ticker_reentry_armed.get(ticker, False))
            expected_direction = str(ticker_reentry_expected_direction.get(ticker, "") or "").lower()
            if prior_entries >= 1:
                if not reentry_armed:
                    _mark_skip("already_traded_today")
                    print(f"[{ts(now_et)}] {ticker}: skip (already traded today; no stop-loss re-entry armed).")
                    continue
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
            if existing_qty_for_ticker > 0:
                _mark_skip("ticker_position_already_open")
                print(
                    f"[{ts(now_et)}] {ticker}: skip (existing open position qty={existing_qty_for_ticker}; "
                    "one position per ticker)."
                )
                continue

            try:
                print(f"[{ts(now_et)}] {ticker}: scanner signal={direction}. {signal.get('reason', '')}")

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
                ask_price = data_client.get_latest_option_ask(option_symbol)
                direct_market_entry = False
                if ask_price is None or ask_price <= 0:
                    if config.EMERGENCY_EXECUTION_MODE and config.ALLOW_MARKET_ENTRY_WITHOUT_QUOTE:
                        print(
                            f"[{ts(now_et)}] {ticker}: no option ask for {option_symbol}; "
                            "emergency mode using direct market entry."
                        )
                        direct_market_entry = True
                        ask_price = 0.0
                    else:
                        _mark_skip("no_option_ask")
                        print(f"[{ts(now_et)}] {ticker}: skip (no option ask for {option_symbol}).")
                        time.sleep(config.RATE_LIMIT_SLEEP_SECONDS)
                        continue

                if not direct_market_entry:
                    initial_chain_ask = float(contract.get("ask_price", ask_price) or ask_price)
                    pre_submit_slippage = _slippage_pct(initial_chain_ask, ask_price)
                    if pre_submit_slippage > config.MAX_ENTRY_SLIPPAGE_PCT:
                        # Retry quote once before skipping on stale chain snapshot drift.
                        retry_ask = data_client.get_latest_option_ask(option_symbol)
                        if retry_ask is not None and retry_ask > 0:
                            ask_price = retry_ask
                            pre_submit_slippage = _slippage_pct(initial_chain_ask, ask_price)
                    if pre_submit_slippage > (config.MAX_ENTRY_SLIPPAGE_PCT * 3):
                        _mark_skip("entry_slippage_too_high")
                        print(
                            f"[{ts(now_et)}] {ticker}: skip (entry slippage {pre_submit_slippage:.2f}% > "
                            f"hard cap {(config.MAX_ENTRY_SLIPPAGE_PCT * 3):.2f}%)."
                        )
                        time.sleep(config.RATE_LIMIT_SLEEP_SECONDS)
                        continue

                # Always trade exactly 1 contract
                qty = 1

                if direct_market_entry:
                    order = broker.place_option_market_buy(option_symbol, qty)
                    entry_debug["entry_orders_submitted"] = int(entry_debug.get("entry_orders_submitted", 0)) + 1
                    print(
                        f"[{ts(now_et)}] ENTRY {ticker} {direction.upper()} "
                        f"{option_symbol} qty={qty} market order_id={order.id}"
                    )
                else:
                    order = broker.place_option_limit_buy(option_symbol, qty, ask_price)
                    entry_debug["entry_orders_submitted"] = int(entry_debug.get("entry_orders_submitted", 0)) + 1
                    print(
                        f"[{ts(now_et)}] ENTRY {ticker} {direction.upper()} "
                        f"{option_symbol} qty={qty} limit={ask_price:.2f} order_id={order.id}"
                    )

                time.sleep(max(1, int(config.ENTRY_ORDER_STATUS_WAIT_SECONDS)))
                filled_order = broker.get_order_status(order.id)
                order_status = str(getattr(filled_order, "status", "")).lower()
                reject_detail = _order_reject_reason(filled_order)

                if order_status not in ("filled", "partially_filled"):
                    try:
                        if not direct_market_entry:
                            broker.cancel_order(order.id)
                    except Exception as exc:  # noqa: BLE001
                        print(f"[{ts(now_et)}] {ticker}: cancel of {order.id} failed: {exc}")
                    # Retry once with a slightly more aggressive limit before market fallback.
                    if not direct_market_entry:
                        aggressive_limit = round(float(ask_price) * 1.05, 4)
                        print(
                            f"[{ts(now_et)}] {ticker}: limit order {order.id} not filled ({order_status}). "
                            f"Retry limit={aggressive_limit:.4f}."
                        )
                        try:
                            retry_order = broker.place_option_limit_buy(option_symbol, qty, aggressive_limit)
                            time.sleep(max(1, int(config.ENTRY_RETRY_STATUS_WAIT_SECONDS)))
                            filled_order = broker.get_order_status(retry_order.id)
                            order_status = str(getattr(filled_order, "status", "")).lower()
                            reject_detail = _order_reject_reason(filled_order)
                            if order_status not in ("filled", "partially_filled"):
                                try:
                                    broker.cancel_order(retry_order.id)
                                except Exception as exc:  # noqa: BLE001
                                    print(f"[{ts(now_et)}] {ticker}: retry cancel of {retry_order.id} failed: {exc}")
                                print(f"[{ts(now_et)}] {ticker}: retry limit not filled ({order_status}). Trying market buy.")
                                mkt_order = broker.place_option_market_buy(option_symbol, qty)
                                time.sleep(max(1, int(config.ENTRY_MARKET_FALLBACK_WAIT_SECONDS)))
                                filled_order = broker.get_order_status(mkt_order.id)
                                order_status = str(getattr(filled_order, "status", "")).lower()
                                reject_detail = _order_reject_reason(filled_order)
                            if order_status not in ("filled", "partially_filled"):
                                extra = f" {reject_detail}" if reject_detail else ""
                                _mark_skip("entry_not_filled_after_fallback")
                                print(
                                    f"[{ts(now_et)}] {ticker}: market fallback not filled "
                                    f"(status={order_status}).{extra} Skipping."
                                )
                                time.sleep(config.RATE_LIMIT_SLEEP_SECONDS)
                                continue
                        except Exception as exc:  # noqa: BLE001
                            _mark_skip("market_fallback_exception")
                            print(f"[{ts(now_et)}] {ticker}: market fallback failed: {exc}")
                            time.sleep(config.RATE_LIMIT_SLEEP_SECONDS)
                            continue
                    else:
                        extra = f" {reject_detail}" if reject_detail else ""
                        _mark_skip("direct_market_not_filled")
                        print(
                            f"[{ts(now_et)}] {ticker}: direct market entry not filled "
                            f"(status={order_status}).{extra} Skipping."
                        )
                        time.sleep(config.RATE_LIMIT_SLEEP_SECONDS)
                        continue

                filled_avg_price = float(getattr(filled_order, "filled_avg_price", 0) or 0)
                filled_qty = position_qty_as_int(getattr(filled_order, "filled_qty", qty)) or qty
                if filled_qty > 1:
                    extra_qty = filled_qty - 1
                    try:
                        broker.close_option_market(option_symbol, extra_qty)
                        print(f"[{ts(now_et)}] {ticker}: trimmed fill to 1 contract (closed extra {extra_qty}).")
                    except Exception as exc:  # noqa: BLE001
                        print(f"[{ts(now_et)}] {ticker}: failed to trim extra qty {extra_qty}: {exc}")
                    filled_qty = 1
                fill_slippage = _slippage_pct(ask_price, filled_avg_price) if (filled_avg_price > 0 and ask_price > 0) else 0.0
                if (not direct_market_entry) and fill_slippage > config.MAX_FILL_SLIPPAGE_PCT:
                    _mark_skip("fill_slippage_too_high")
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
                    "ticker": ticker,
                    "direction": direction,
                    "option_symbol": option_symbol,
                    "strike": contract.get("strike_price", ""),
                    "expiry": contract.get("expiration_date", ""),
                    "qty": filled_qty,
                    "entry_price": filled_avg_price or ask_price,
                    "stop_floor_plpc": -float(config.STOP_LOSS_PCT),
                    "max_plpc": 0.0,
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

            try:
                plpc = float(getattr(pos, "unrealized_plpc", 0))
            except (TypeError, ValueError):
                plpc = 0.0
            # Prefer live quote-derived PLPC when available to avoid stale position snapshots.
            if live_plpc is not None:
                plpc = float(live_plpc)

            # --- Momentum-based trailing stop ---
            # Strategy: cut losers immediately with a tight stop; ride winners by
            # trailing a stop that locks in more profit as the trade grows.
            #
            # The trailing stop distance shrinks as max profit increases:
            #   - No profit yet          → tight stop at -STOP_LOSS_PCT (default -10%)
            #   - Profit > TRAIL_LOCK1   → stop locks to +TRAIL_LOCK1_STOP (default +5%)
            #   - Profit > TRAIL_LOCK2   → stop locks to +TRAIL_LOCK2_STOP (default +15%)
            #   - Profit > TRAIL_LOCK3   → stop locks to +TRAIL_LOCK3_STOP (default +25%)
            #   - Beyond that            → trail at TRAIL_PULLBACK_PCT below peak (default 8%)
            #     so the stop always rises with the trade and never falls.
            max_plpc = float(meta.get("max_plpc", plpc) or plpc)
            max_plpc = max(max_plpc, plpc)

            # Base stop: tight immediate cut if trade goes against us
            dynamic_stop_floor = -float(config.STOP_LOSS_PCT)

            trail_pullback = float(getattr(config, "TRAIL_PULLBACK_PCT", 0.08))
            lock3_trigger = float(getattr(config, "TRAIL_LOCK3_TRIGGER_PCT", 0.40))
            lock3_stop = float(getattr(config, "TRAIL_LOCK3_STOP_PCT", 0.25))

            if max_plpc >= lock3_trigger:
                # Deep in profit: trail dynamically — stop = peak minus pullback
                dynamic_stop_floor = max(lock3_stop, max_plpc - trail_pullback)
            elif max_plpc >= float(config.TRAIL_LOCK2_TRIGGER_PCT):
                dynamic_stop_floor = float(config.TRAIL_LOCK2_STOP_PCT)
            elif max_plpc >= float(config.TRAIL_LOCK1_TRIGGER_PCT):
                dynamic_stop_floor = float(config.TRAIL_LOCK1_STOP_PCT)

            # Stop floor can only move up, never down (ratchet)
            prev_floor = float(meta.get("stop_floor_plpc", dynamic_stop_floor) or dynamic_stop_floor)
            dynamic_stop_floor = max(dynamic_stop_floor, prev_floor)

            if meta:
                meta["stop_floor_plpc"] = dynamic_stop_floor
                meta["max_plpc"] = max_plpc
                open_trade_meta[symbol] = meta

            exit_reason = None
            close_qty = qty
            ticker_for_pos = str(meta.get("ticker", "") or "").upper()
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
            if exit_reason is None and config.ENABLE_FIXED_PROFIT_TARGET and plpc >= config.PROFIT_TARGET_PCT:
                exit_reason = "profit_target"
            elif exit_reason is None and plpc <= dynamic_stop_floor:
                exit_reason = "stop_loss"

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
                expiry_date = _option_expiry_date(meta, symbol)
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
                entry_time = _parse_trade_meta_entry_time(meta) if meta else None
                if entry_time is not None:
                    held_minutes = int((now_et - entry_time).total_seconds() // 60)
                    if held_minutes >= int(config.MAX_HOLD_MINUTES):
                        exit_reason = "time_stop"

            if exit_reason:
                try:
                    last_exit_debug = {
                        "loop_ts_et": ts(now_et),
                        "symbol": symbol,
                        "reason": exit_reason,
                        "requested_qty": close_qty,
                        "position_qty": qty,
                        "filled_qty": 0,
                        "plpc_used": round(plpc, 6),
                        "quote_mark_price": round(float(live_mark_price), 6) if live_mark_price else None,
                        "result": "submitted",
                    }
                    filled_close_qty, close_fill_price = _close_position_with_confirmation(
                        symbol=symbol,
                        qty=close_qty,
                        now_et=now_et,
                        label=f"EXIT {exit_reason}",
                    )
                    if filled_close_qty <= 0:
                        last_exit_debug["result"] = "pending_or_not_filled"
                        _save_runtime_state()
                        continue
                    last_exit_debug["filled_qty"] = filled_close_qty
                    last_exit_debug["result"] = "filled"
                    meta = open_trade_meta.get(symbol, {})
                    entry_price = float(meta.get("entry_price", getattr(pos, "avg_entry_price", 0) or 0))
                    if close_fill_price is not None and close_fill_price > 0:
                        exit_price = float(close_fill_price)
                    elif live_mark_price is not None and live_mark_price > 0:
                        exit_price = float(live_mark_price)
                    else:
                        exit_price = float(getattr(pos, "current_price", 0) or 0)
                    realized_plpc = plpc
                    if entry_price > 0 and exit_price > 0:
                        realized_plpc = (exit_price - entry_price) / entry_price
                    trade_logger.log_trade(
                        {
                            "timestamp": ts(now_et),
                            "ticker": meta.get("ticker", ""),
                            "direction": meta.get("direction", ""),
                            "option_symbol": symbol,
                            "strike": meta.get("strike", ""),
                            "expiry": meta.get("expiry", ""),
                            "qty": filled_close_qty,
                            "entry_price": entry_price,
                            "exit_price": exit_price,
                            "pnl_pct": round(realized_plpc, 4),
                            "exit_reason": exit_reason,
                        }
                    )

                    trade_pnl_usd = (exit_price - entry_price) * filled_close_qty * 100

                    if trade_pnl_usd < 0:
                        daily_realized_loss_usd += abs(trade_pnl_usd)
                        weekly_realized_loss_usd += abs(trade_pnl_usd)
                        consecutive_losses += 1
                    else:
                        consecutive_losses = 0

                    ticker = str(meta.get("ticker", "") or "")
                    reversal_direction = ""
                    reentries_used = int(ticker_reentries_used.get(ticker, 0)) if ticker else 0
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
                        _attempt_reversal_entry(
                            ticker=ticker,
                            direction=reversal_direction,
                            now_et=now_et,
                            reentries_used=reentries_used,
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

        if is_at_or_after(now_et, config.HARD_CLOSE_TIME):
            print(f"[{ts(now_et)}] Hard close time reached. Flattening and shutting down.")
            option_positions = broker.get_open_option_positions()
            for pos in option_positions:
                symbol = str(getattr(pos, "symbol", ""))
                qty = position_qty_as_int(getattr(pos, "qty", 0))
                if qty > 0:
                    try:
                        filled_qty, _fill_price = _close_position_with_confirmation(
                            symbol=symbol,
                            qty=qty,
                            now_et=now_et,
                            label="EOD CLOSE",
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
