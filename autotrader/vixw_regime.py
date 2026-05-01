"""VIXW-heavy volatility regime sidecar for AutoBott.

Paper-mode doctrine:
- Read the VIX level.
- If VIX is below the configured average level, prefer CALL exposure.
- If VIX is above the configured average level, prefer PUT exposure.
- Use contracts a few trading days out.
- Fail safely if Alpaca does not expose/trade the requested VIX/VIXW option chain.

This sidecar is intentionally isolated from the normal scanner so it can be
turned off without changing the core bot.
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

EASTERN = pytz.timezone(str(getattr(config, "EASTERN_TZ", "US/Eastern") or "US/Eastern"))


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
    symbol = str(getattr(config, "VIXW_SIGNAL_SOURCE_SYMBOL", "^VIX") or "^VIX").strip()
    try:
        ticker = yf.Ticker(symbol)
        fast_info = getattr(ticker, "fast_info", None)
        if fast_info is not None:
            price = _safe_float(getattr(fast_info, "last_price", None), 0.0)
            if price > 0:
                return price
    except Exception as exc:  # noqa: BLE001
        print(f"[vixw] fast_info failed for {symbol}: {exc}")

    try:
        history = yf.Ticker(symbol).history(period="5d", interval="1m", auto_adjust=False)
        if history is not None and not history.empty:
            close = float(history["Close"].dropna().iloc[-1])
            if close > 0:
                return close
    except Exception as exc:  # noqa: BLE001
        print(f"[vixw] history failed for {symbol}: {exc}")
    return None


def _regime_direction(vix_level: float) -> tuple[str | None, str]:
    average = float(getattr(config, "VIXW_REGIME_AVERAGE_LEVEL", 19.0) or 19.0)
    neutral_band = float(getattr(config, "VIXW_NEUTRAL_BAND", 0.0) or 0.0)
    if vix_level < (average - neutral_band):
        return "call", f"VIX {vix_level:.2f} below average {average:.2f}; mean-reversion CALL bias"
    if vix_level > (average + neutral_band):
        return "put", f"VIX {vix_level:.2f} above average {average:.2f}; mean-reversion PUT bias"
    return None, f"VIX {vix_level:.2f} inside neutral band around {average:.2f}"


def _has_vixw_exposure(broker: AlpacaBroker) -> bool:
    prefixes = tuple(str(x).upper() for x in getattr(config, "VIXW_POSITION_SYMBOL_PREFIXES", ("VIX", "VIXW")))
    try:
        positions = broker.get_open_option_positions()
    except Exception as exc:  # noqa: BLE001
        print(f"[vixw] position lookup failed: {exc}")
        return True
    count = 0
    for pos in positions:
        symbol = str(getattr(pos, "symbol", "") or "").upper()
        qty = _safe_int(getattr(pos, "qty", 0), 0)
        if qty > 0 and symbol.startswith(prefixes):
            count += 1
    return count >= int(getattr(config, "VIXW_MAX_OPEN_POSITIONS", 1) or 1)


def _select_vixw_contract(
    data_client: AlpacaDataClient,
    *,
    direction: str,
    vix_level: float,
    now_et: datetime,
) -> tuple[dict[str, Any] | None, str]:
    underlying = str(getattr(config, "VIXW_OPTION_UNDERLYING_SYMBOL", "VIX") or "VIX").strip().upper()
    min_dte = int(getattr(config, "VIXW_MIN_DTE_TRADING_DAYS", 2) or 2)
    max_dte = int(getattr(config, "VIXW_MAX_DTE_TRADING_DAYS", 7) or 7)
    expiry_floor = _add_trading_days(now_et.date(), min_dte)
    expiry_ceiling = _add_trading_days(now_et.date(), max_dte)

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
        contract["_select_score"] = abs(strike - vix_level)
        scored.append(contract)

    if not scored:
        return None, "VIXW chain returned no active/tradable contracts with usable strikes"

    scored.sort(key=lambda item: (float(item.get("_select_score", 999.0)), str(item.get("expiration_date", ""))))
    max_spread = float(getattr(config, "VIXW_MAX_OPTION_SPREAD_PCT", getattr(config, "MAX_OPTION_SPREAD_PCT", 30.0)) or 30.0)
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
        return contract, "ok"

    return None, f"no VIXW contract passed quote/spread gate max_spread={max_spread:.2f}%"


def _log_decision(row: dict[str, Any]) -> None:
    path = Path(getattr(config, "VIXW_REGIME_LOG_CSV_PATH", Path(config.DATA_DIR) / "vixw_regime_log.csv"))
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = [
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
    write_header = not path.exists()
    payload = {key: row.get(key, "") for key in columns}
    try:
        with path.open("a", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=columns)
            if write_header:
                writer.writeheader()
            writer.writerow(payload)
    except Exception as exc:  # noqa: BLE001
        print(f"[vixw] decision log failed: {exc}")


def run_vixw_regime_forever(api_key: str, secret_key: str) -> None:
    if not bool(getattr(config, "VIXW_HEAVY_MODE", True)):
        print("[vixw] VIXW_HEAVY_MODE disabled.")
        return
    if bool(getattr(config, "VIXW_ONLY_PAPER_MODE", True)) and not bool(getattr(config, "PAPER", True)):
        print("[vixw] disabled because VIXW_ONLY_PAPER_MODE=True and PAPER=False.")
        return

    broker = AlpacaBroker(api_key, secret_key, paper=bool(getattr(config, "PAPER", True)))
    data_client = AlpacaDataClient(api_key, secret_key, paper=bool(getattr(config, "PAPER", True)))
    last_entry_at: datetime | None = None
    sleep_seconds = max(15, int(getattr(config, "VIXW_POLL_SECONDS", 60) or 60))
    cooldown_seconds = max(60, int(getattr(config, "VIXW_MIN_SECONDS_BETWEEN_ENTRIES", 1800) or 1800))

    print("[vixw] VIXW regime sidecar started.")
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

            vix_level = _latest_vix_level()
            if vix_level is None or vix_level <= 0:
                _log_decision({"timestamp": now_et.isoformat(), "decision": "skip", "reason": "VIX level unavailable"})
                time.sleep(sleep_seconds)
                continue

            direction, regime_reason = _regime_direction(vix_level)
            if direction is None:
                _log_decision({"timestamp": now_et.isoformat(), "vix_level": round(vix_level, 4), "average_level": getattr(config, "VIXW_REGIME_AVERAGE_LEVEL", 19.0), "decision": "skip", "reason": regime_reason})
                time.sleep(sleep_seconds)
                continue

            if _has_vixw_exposure(broker):
                _log_decision({"timestamp": now_et.isoformat(), "vix_level": round(vix_level, 4), "average_level": getattr(config, "VIXW_REGIME_AVERAGE_LEVEL", 19.0), "direction": direction, "decision": "skip", "reason": "existing VIX/VIXW exposure or position lookup conservative block"})
                time.sleep(sleep_seconds)
                continue

            contract, contract_reason = _select_vixw_contract(data_client, direction=direction, vix_level=vix_level, now_et=now_et)
            if not contract:
                print(f"[vixw] skip: {contract_reason}")
                _log_decision({"timestamp": now_et.isoformat(), "vix_level": round(vix_level, 4), "average_level": getattr(config, "VIXW_REGIME_AVERAGE_LEVEL", 19.0), "direction": direction, "decision": "skip", "reason": contract_reason})
                time.sleep(sleep_seconds)
                continue

            ask = _safe_float(contract.get("ask_price"), 0.0)
            if ask <= 0:
                time.sleep(sleep_seconds)
                continue
            budget = float(getattr(config, "VIXW_POSITION_SIZE_USD", 600.0) or 600.0)
            qty = max(1, int(budget // (ask * 100.0)))
            limit_multiplier = float(getattr(config, "VIXW_ENTRY_LIMIT_PRICE_MULTIPLIER", 1.0) or 1.0)
            limit_price = round(ask * limit_multiplier, 2)
            symbol = str(contract.get("symbol", "") or "")

            order = broker.place_option_limit_buy(symbol, qty, limit_price)
            order_id = str(getattr(order, "id", "") or "")
            last_entry_at = now_et
            print(f"[vixw] submitted {direction.upper()} buy {symbol} qty={qty} limit={limit_price:.2f} vix={vix_level:.2f} order={order_id}")
            _log_decision({
                "timestamp": now_et.isoformat(),
                "vix_level": round(vix_level, 4),
                "average_level": getattr(config, "VIXW_REGIME_AVERAGE_LEVEL", 19.0),
                "direction": direction,
                "decision": "submitted_buy",
                "reason": regime_reason,
                "option_symbol": symbol,
                "strike": contract.get("strike_price", ""),
                "expiration": contract.get("expiration_date", ""),
                "ask": ask,
                "bid": contract.get("bid_price", ""),
                "spread_pct": contract.get("spread_pct", ""),
                "qty": qty,
            })
        except Exception as exc:  # noqa: BLE001
            print(f"[vixw] sidecar error: {exc}")
            _log_decision({"timestamp": now_et.isoformat(), "decision": "error", "reason": str(exc)[:250]})
        time.sleep(sleep_seconds)
