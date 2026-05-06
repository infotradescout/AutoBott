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
        if bid <= 0 or ask <= 0 or ask < bid:
            continue
        midpoint = (bid + ask) / 2.0
        spread_pct = ((ask - bid) / midpoint) * 100.0 if midpoint > 0 else 999.0
        if spread_pct > max_spread:
            continue
        contract["bid_price"] = bid
        contract["ask_price"] = ask
        contract["spread_pct"] = round(spread_pct, 2)
        contract["underlying_price"] = round(float(underlying_price), 4)
        contract["proxy_underlying"] = underlying
        return contract, "ok"

    return None, f"no proxy contract passed quote/spread gate max_spread={max_spread:.2f}%"


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
        now_et = _now_et()
        try:
            clock = broker.get_clock()
            if not bool(getattr(clock, "is_open", False)):
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
                "spread_pct": contract.get("spread_pct", ""),
                "qty": log_qty,
            })
        except Exception as exc:  # noqa: BLE001
            print(f"[vix_proxy] sidecar error: {exc}")
            _log_decision({"timestamp": now_et.isoformat(), "decision": "error", "reason": str(exc)[:250]})
        time.sleep(sleep_seconds)
