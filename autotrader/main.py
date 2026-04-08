"""Entry point for the intraday options autotrader."""

from __future__ import annotations

import time
from datetime import datetime

import pytz

import config
from broker import AlpacaBroker
from data import AlpacaDataClient
from env_config import get_required_env, load_runtime_env
from logger import TradeLogger
from options import select_atm_option_contract
from risk import calculate_entry_qty, can_open_new_positions, is_at_or_after, position_matches_ticker
from scanner import build_watchlist, initialize_scanner, run_scan, should_build_watchlist


def ts(now_et: datetime | None = None) -> str:
    now_et = now_et or datetime.now(pytz.timezone(config.EASTERN_TZ))
    return now_et.strftime("%Y-%m-%d %H:%M:%S %Z")


def position_qty_as_int(qty_value) -> int:
    try:
        return int(float(qty_value))
    except (TypeError, ValueError):
        return 0


def _prune_recent_entries(entry_times: list[datetime], now_et: datetime, days: int = 5) -> list[datetime]:
    # Rolling calendar-day window used as a conservative throttle for sub-$25k accounts.
    threshold = now_et.timestamp() - (days * 24 * 60 * 60)
    return [dt for dt in entry_times if dt.timestamp() >= threshold]


def main():
    load_runtime_env()
    api_key = get_required_env("ALPACA_API_KEY")
    secret_key = get_required_env("ALPACA_SECRET_KEY")

    tz = pytz.timezone(config.EASTERN_TZ)
    broker = AlpacaBroker(api_key, secret_key, paper=config.PAPER)
    data_client = AlpacaDataClient(api_key, secret_key, paper=config.PAPER)
    initialize_scanner(data_client)
    trade_logger = TradeLogger()
    open_trade_meta: dict[str, dict] = {}
    watchlist: list[str] = []
    watchlist_built = False
    entry_times_rolling: list[datetime] = []
    daily_realized_loss_usd: float = 0.0
    consecutive_losses: int = 0
    loss_counters_day = None

    print(f"[{ts()}] Autotrader started. Waiting for market open.")
    while True:
        clock = broker.get_clock()
        now_et = datetime.now(tz)
        if clock.is_open:
            break
        print(f"[{ts(now_et)}] Market closed. Polling again in 60s.")
        time.sleep(60)

    print(f"[{ts()}] Market open. Starting loop.")
    while True:
        now_et = datetime.now(tz)
        clock = broker.get_clock()
        if not clock.is_open:
            print(f"[{ts(now_et)}] Market no longer open. Sleeping {config.LOOP_INTERVAL_SECONDS}s.")
            time.sleep(config.LOOP_INTERVAL_SECONDS)
            continue
        if loss_counters_day != now_et.date():
            # Reset daily counters when market opens (once per session)
            daily_realized_loss_usd = 0.0
            consecutive_losses = 0
            loss_counters_day = now_et.date()

        option_positions = broker.get_open_option_positions()
        open_count = len(option_positions)

        if not watchlist_built:
            if should_build_watchlist(now_et):
                watchlist = build_watchlist()
                watchlist_built = True
                print(f"[{ts(now_et)}] Morning watchlist built with {len(watchlist)} symbols: {watchlist}")
            else:
                print(f"[{ts(now_et)}] Waiting for {config.SCAN_MORNING_TIME} ET to build watchlist.")

        signals = run_scan(watchlist) if watchlist_built and watchlist else []
        pdt_allowed, pdt_info = broker.pdt_allows_new_day_trade()
        entry_times_rolling = _prune_recent_entries(entry_times_rolling, now_et, days=5)
        equity = pdt_info.get("equity")
        under_25k = equity is not None and float(equity) < config.PDT_MIN_EQUITY
        local_trade_budget_hit = under_25k and len(entry_times_rolling) >= config.PDT_MAX_DAY_TRADES_5D
        if not pdt_allowed:
            print(
                f"[{ts(now_et)}] PDT guard active: no new entries. "
                f"equity={float(pdt_info.get('equity') or 0):.2f} "
                f"daytrades_5d={pdt_info.get('daytrade_count')}/{config.PDT_MAX_DAY_TRADES_5D}"
            )
        elif local_trade_budget_hit:
            print(
                f"[{ts(now_et)}] PDT throttle active: {len(entry_times_rolling)}/{config.PDT_MAX_DAY_TRADES_5D} "
                f"entries used in rolling 5 days while equity < {config.PDT_MIN_EQUITY:.0f}. No new entries."
            )

        for signal in signals:
            now_et = datetime.now(tz)
            # Daily loss limit circuit breaker
            if daily_realized_loss_usd >= config.DAILY_LOSS_LIMIT_USD:
                print(
                    f"[{ts(now_et)}] DAILY LOSS LIMIT hit: "
                    f"${daily_realized_loss_usd:.2f} >= ${config.DAILY_LOSS_LIMIT_USD:.2f}. "
                    f"No new entries today."
                )
                break  # exit the signals loop entirely

            # Consecutive loss circuit breaker
            if consecutive_losses >= config.CONSECUTIVE_LOSS_LIMIT:
                print(
                    f"[{ts(now_et)}] {consecutive_losses} consecutive losses. "
                    f"Pausing new entries for the rest of the day."
                )
                break

            if under_25k and len(entry_times_rolling) >= config.PDT_MAX_DAY_TRADES_5D:
                break
            if not pdt_allowed or local_trade_budget_hit:
                break
            ticker = signal["symbol"]
            direction = signal["direction"]
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

                qty = calculate_entry_qty(config.POSITION_SIZE_USD, ask_price)
                if qty < 1:
                    print(f"[{ts(now_et)}] {ticker}: skip (qty calc invalid).")
                    time.sleep(config.RATE_LIMIT_SLEEP_SECONDS)
                    continue

                order = broker.place_option_limit_buy(option_symbol, qty, ask_price)
                print(
                    f"[{ts(now_et)}] ENTRY {ticker} {direction.upper()} "
                    f"{option_symbol} qty={qty} limit={ask_price:.2f} order_id={order.id}"
                )

                # Wait briefly then confirm the order filled
                time.sleep(5)
                filled_order = broker.get_order_status(order.id)
                order_status = str(getattr(filled_order, "status", "")).lower()

                if order_status not in ("filled", "partially_filled"):
                    # Did not fill - cancel it and skip
                    try:
                        broker.cancel_order(order.id)
                    except Exception:
                        pass
                    print(
                        f"[{ts(now_et)}] {ticker}: limit order {order.id} did not fill "
                        f"(status={order_status}). Cancelled."
                    )
                    time.sleep(config.RATE_LIMIT_SLEEP_SECONDS)
                    continue

                open_trade_meta[option_symbol] = {
                    "timestamp": ts(now_et),
                    "ticker": ticker,
                    "direction": direction,
                    "option_symbol": option_symbol,
                    "strike": contract.get("strike_price", ""),
                    "expiry": contract.get("expiration_date", ""),
                    "qty": qty,
                    "entry_price": ask_price,
                }
                open_count += 1
                entry_times_rolling.append(now_et)
                time.sleep(config.RATE_LIMIT_SLEEP_SECONDS)
            except Exception as exc:  # noqa: BLE001
                print(f"[{ts(now_et)}] {ticker}: error during entry flow: {exc}")
                time.sleep(config.RATE_LIMIT_SLEEP_SECONDS)

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

                    # Track realized loss for daily circuit breaker
                    entry_price = float(meta.get("entry_price", getattr(pos, "avg_entry_price", 0) or 0))
                    exit_price = float(getattr(pos, "current_price", 0) or 0)
                    contracts = qty * 100  # 1 contract = 100 shares
                    trade_pnl = (exit_price - entry_price) * contracts * (1 if meta.get("direction") == "call" else 1)
                    # Simpler: use plpc x original premium spent
                    premium_spent = entry_price * qty * 100
                    trade_pnl_usd = premium_spent * plpc  # plpc is already a signed fraction

                    if trade_pnl_usd < 0:
                        daily_realized_loss_usd += abs(trade_pnl_usd)
                        consecutive_losses += 1
                    else:
                        consecutive_losses = 0  # reset streak on a win

                    open_trade_meta.pop(symbol, None)
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
            break

        time.sleep(config.LOOP_INTERVAL_SECONDS)

    print(f"[{ts()}] Trader stopped.")


if __name__ == "__main__":
    main()
