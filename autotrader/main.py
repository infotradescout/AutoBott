"""Entry point for the intraday options autotrader."""

from __future__ import annotations

import time
from datetime import date, datetime, timedelta

import pytz
import yfinance as yf

from env_config import get_required_env, load_runtime_env
load_runtime_env()

import config
from alerts import AlertManager
from broker import AlpacaBroker
from data import AlpacaDataClient
from logger import TradeLogger
from options import select_atm_option_contract
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
    except Exception:
        pass
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
    option_positions = broker.get_open_option_positions()
    for pos in option_positions:
        symbol = str(getattr(pos, "symbol", ""))
        qty = position_qty_as_int(getattr(pos, "qty", 0))
        if qty <= 0:
            continue
        try:
            broker.close_option_market(symbol, qty)
            print(f"[{ts(now_et)}] {label} CLOSE {symbol} qty={qty}")
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
    loss_counters_day = date.fromisoformat(loss_counters_day_raw) if loss_counters_day_raw else None
    weekly_loss_key = str(state.get("weekly_loss_key") or _week_key(datetime.now(tz).date()))
    blocked_day_notice = state.get("blocked_day_notice")
    vix_block_notice = state.get("vix_block_notice")
    catalyst_mode_active = bool(state.get("catalyst_mode_active", False))
    catalyst_mode_reason = str(state.get("catalyst_mode_reason", "") or "")
    catalyst_mode_until = _parse_state_datetime(state.get("catalyst_mode_until_iso"))
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
            }
        )

    mode = "PAPER" if config.PAPER else "LIVE"
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
        clock = broker.get_clock()
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

        clock = broker.get_clock()
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
            set_catalyst_mode(False, "")

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
        pdt_allowed, pdt_info = broker.pdt_allows_new_day_trade()
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

        if not pdt_allowed:
            print(
                f"[{ts(now_et)}] PDT broker flag: no new entries reported. "
                f"equity={float(pdt_info.get('equity') or 0):.2f} "
                f"daytrades_5d={pdt_info.get('daytrade_count')}/{config.PDT_MAX_DAY_TRADES_5D} "
                f"(ENFORCE_PDT_GUARD={config.ENFORCE_PDT_GUARD})"
            )

        for signal in signals:
            now_et = datetime.now(tz)

            # --- Daily loss limit ---
            if daily_realized_loss_usd >= config.DAILY_LOSS_LIMIT_USD:
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

            # --- Consecutive loss circuit breaker (only if ENFORCE_PDT_GUARD is on) ---
            if config.ENFORCE_PDT_GUARD and consecutive_losses >= config.CONSECUTIVE_LOSS_LIMIT:
                print(
                    f"[{ts(now_et)}] {consecutive_losses} consecutive losses. "
                    f"Pausing new entries for the rest of the day."
                )
                break

            # --- PDT guard (only blocks if ENFORCE_PDT_GUARD=True) ---
            if local_trade_budget_hit:
                break
            if config.ENFORCE_PDT_GUARD and not pdt_allowed:
                break

            ticker = signal["symbol"]
            direction = signal["direction"]
            if not is_at_or_after(now_et, config.NO_NEW_TRADES_BEFORE):
                print(f"[{ts(now_et)}] Entry window not open yet (before {config.NO_NEW_TRADES_BEFORE} ET).")
                break
            if is_at_or_after(now_et, config.NO_NEW_TRADES_AFTER):
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
                print(f"[{ts(now_et)}] {ticker}: skip (existing option position).")
                continue

            if not can_open_new_positions(open_count, config.MAX_POSITIONS):
                print(f"[{ts(now_et)}] Max positions reached. Stopping new entries this loop.")
                break

            try:
                print(f"[{ts(now_et)}] {ticker}: scanner signal={direction}. {signal.get('reason', '')}")

                stock_price = data_client.get_latest_stock_price(ticker)
                if stock_price is None:
                    print(f"[{ts(now_et)}] {ticker}: skip (no stock quote).")
                    time.sleep(config.RATE_LIMIT_SLEEP_SECONDS)
                    continue

                contract = select_atm_option_contract(
                    data_client=data_client,
                    underlying_symbol=ticker,
                    direction=direction,
                    underlying_price=stock_price,
                    now_et=now_et,
                )
                if not contract:
                    print(f"[{ts(now_et)}] {ticker}: skip (no eligible option contract).")
                    time.sleep(config.RATE_LIMIT_SLEEP_SECONDS)
                    continue

                option_symbol = contract["symbol"]
                ask_price = data_client.get_latest_option_ask(option_symbol)
                if ask_price is None or ask_price <= 0:
                    print(f"[{ts(now_et)}] {ticker}: skip (no option ask for {option_symbol}).")
                    time.sleep(config.RATE_LIMIT_SLEEP_SECONDS)
                    continue

                initial_chain_ask = float(contract.get("ask_price", ask_price) or ask_price)
                pre_submit_slippage = _slippage_pct(initial_chain_ask, ask_price)
                if pre_submit_slippage > config.MAX_ENTRY_SLIPPAGE_PCT:
                    print(
                        f"[{ts(now_et)}] {ticker}: skip (entry slippage {pre_submit_slippage:.2f}% > "
                        f"{config.MAX_ENTRY_SLIPPAGE_PCT:.2f}%)."
                    )
                    time.sleep(config.RATE_LIMIT_SLEEP_SECONDS)
                    continue

                # Always trade exactly 1 contract
                qty = 1

                order = broker.place_option_limit_buy(option_symbol, qty, ask_price)
                print(
                    f"[{ts(now_et)}] ENTRY {ticker} {direction.upper()} "
                    f"{option_symbol} qty={qty} limit={ask_price:.2f} order_id={order.id}"
                )

                time.sleep(5)
                filled_order = broker.get_order_status(order.id)
                order_status = str(getattr(filled_order, "status", "")).lower()

                if order_status not in ("filled", "partially_filled"):
                    try:
                        broker.cancel_order(order.id)
                    except Exception:
                        pass
                    print(f"[{ts(now_et)}] {ticker}: limit order {order.id} not filled ({order_status}). Trying market buy.")
                    try:
                        mkt_order = broker.place_option_market_buy(option_symbol, qty)
                        time.sleep(3)
                        filled_order = broker.get_order_status(mkt_order.id)
                        order_status = str(getattr(filled_order, "status", "")).lower()
                        if order_status not in ("filled", "partially_filled"):
                            print(
                                f"[{ts(now_et)}] {ticker}: market fallback order {mkt_order.id} not filled "
                                f"(status={order_status}). Skipping."
                            )
                            time.sleep(config.RATE_LIMIT_SLEEP_SECONDS)
                            continue
                    except Exception as exc:  # noqa: BLE001
                        print(f"[{ts(now_et)}] {ticker}: market fallback failed: {exc}")
                        time.sleep(config.RATE_LIMIT_SLEEP_SECONDS)
                        continue

                filled_avg_price = float(getattr(filled_order, "filled_avg_price", 0) or 0)
                filled_qty = position_qty_as_int(getattr(filled_order, "filled_qty", qty)) or qty
                fill_slippage = _slippage_pct(ask_price, filled_avg_price) if filled_avg_price > 0 else 0.0
                if fill_slippage > config.MAX_FILL_SLIPPAGE_PCT:
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
                }
                open_count += 1
                entry_times_rolling.append(now_et)
                _save_runtime_state()
                time.sleep(config.RATE_LIMIT_SLEEP_SECONDS)
            except Exception as exc:  # noqa: BLE001
                print(f"[{ts(now_et)}] {ticker}: error during entry flow: {exc}")
                time.sleep(config.RATE_LIMIT_SLEEP_SECONDS)

        # --- Exit management ---
        option_positions = broker.get_open_option_positions()
        for pos in option_positions:
            now_et = datetime.now(tz)
            symbol = str(getattr(pos, "symbol", ""))
            qty = position_qty_as_int(getattr(pos, "qty", 0))
            if qty <= 0:
                continue

            try:
                plpc = float(getattr(pos, "unrealized_plpc", 0))
            except (TypeError, ValueError):
                plpc = 0.0

            exit_reason = None
            if plpc >= config.PROFIT_TARGET_PCT:
                exit_reason = "profit_target"
            elif plpc <= -config.STOP_LOSS_PCT:
                exit_reason = "stop_loss"
            elif is_at_or_after(now_et, config.HARD_CLOSE_TIME):
                exit_reason = "eod_close"
            else:
                meta = open_trade_meta.get(symbol, {})
                entry_time = _parse_trade_meta_entry_time(meta) if meta else None
                if entry_time is not None:
                    held_minutes = int((now_et - entry_time).total_seconds() // 60)
                    if held_minutes >= int(config.MAX_HOLD_MINUTES):
                        exit_reason = "time_stop"

            if exit_reason:
                try:
                    broker.close_option_market(symbol, qty)
                    meta = open_trade_meta.get(symbol, {})
                    entry_price = float(meta.get("entry_price", getattr(pos, "avg_entry_price", 0) or 0))
                    exit_price = float(getattr(pos, "current_price", 0) or 0)
                    trade_logger.log_trade(
                        {
                            "timestamp": ts(now_et),
                            "ticker": meta.get("ticker", ""),
                            "direction": meta.get("direction", ""),
                            "option_symbol": symbol,
                            "strike": meta.get("strike", ""),
                            "expiry": meta.get("expiry", ""),
                            "qty": qty,
                            "entry_price": entry_price,
                            "exit_price": exit_price,
                            "pnl_pct": round(plpc, 4),
                            "exit_reason": exit_reason,
                        }
                    )

                    premium_spent = entry_price * qty * 100
                    trade_pnl_usd = premium_spent * plpc

                    if trade_pnl_usd < 0:
                        daily_realized_loss_usd += abs(trade_pnl_usd)
                        weekly_realized_loss_usd += abs(trade_pnl_usd)
                        consecutive_losses += 1
                    else:
                        consecutive_losses = 0

                    open_trade_meta.pop(symbol, None)
                    _save_runtime_state()
                    print(f"[{ts(now_et)}] EXIT {symbol} qty={qty} reason={exit_reason} pnl_pct={plpc:.2%}")
                except Exception as exc:  # noqa: BLE001
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
                        broker.close_option_market(symbol, qty)
                        print(f"[{ts(now_et)}] EOD CLOSE {symbol} qty={qty}")
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
