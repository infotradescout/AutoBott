"""Options contract lookup and ATM selection."""

from __future__ import annotations

import time
from datetime import date, datetime, timedelta
from typing import Any

import pytz

import config
from data import AlpacaDataClient


def _next_friday(d: date) -> date:
    days_ahead = (4 - d.weekday()) % 7
    return d + timedelta(days=days_ahead)


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


def select_atm_option_contract(
    data_client: AlpacaDataClient,
    underlying_symbol: str,
    direction: str,
    underlying_price: float,
    now_et: datetime | None = None,
) -> dict[str, Any] | None:
    now_et = now_et or datetime.now(pytz.timezone(config.EASTERN_TZ))
    today = now_et.date()
    expiry_floor = _add_trading_days(today, config.MIN_DTE_TRADING_DAYS)
    expiry_ceiling = _add_trading_days(today, config.MAX_DTE_TRADING_DAYS)

    contracts = data_client.get_option_contracts(
        underlying_symbol=underlying_symbol,
        contract_type=direction,
        expiration_date_gte=expiry_floor,
        expiration_date_lte=expiry_ceiling,
    )
    if not contracts:
        return None

    filtered: list[dict[str, Any]] = []
    for contract in contracts:
        active = contract.get("status", "active") == "active"
        tradable = contract.get("tradable", True)
        strike = _safe_float(contract.get("strike_price"))
        exp = contract.get("expiration_date")
        symbol = contract.get("symbol")
        details = contract
        open_interest = _safe_float(details.get("open_interest"))
        volume = _safe_float(details.get("volume") or details.get("daily_volume"))
        if (open_interest is None or volume is None) and symbol:
            try:
                details = data_client.get_option_contract(symbol)
            except Exception:
                details = contract
            open_interest = _safe_float(details.get("open_interest"))
            volume = _safe_float(details.get("volume") or details.get("daily_volume"))
        if not active or not tradable or strike is None or not exp or not symbol:
            continue
        if open_interest is None or open_interest <= config.MIN_OPTION_OPEN_INTEREST:
            continue
        if volume is None or volume <= config.MIN_OPTION_DAILY_VOLUME:
            continue
        if active and tradable and strike is not None and exp and symbol:
            details["open_interest"] = open_interest
            details["daily_volume"] = volume
            filtered.append(details)

    if not filtered:
        return None

    filtered.sort(
        key=lambda c: (
            c["expiration_date"],
            abs(float(c["strike_price"]) - underlying_price),
        )
    )

    for contract in filtered[:25]:
        symbol = contract["symbol"]
        quote = data_client.get_latest_option_quote(symbol)
        bid = _safe_float(quote.get("bid"))
        ask = _safe_float(quote.get("ask"))
        if bid is None or ask is None or bid <= 0 or ask <= 0 or ask < bid:
            time.sleep(config.RATE_LIMIT_SLEEP_SECONDS)
            continue
        mid = (bid + ask) / 2
        if mid <= 0:
            time.sleep(config.RATE_LIMIT_SLEEP_SECONDS)
            continue
        spread_pct = ((ask - bid) / mid) * 100
        if spread_pct >= config.MAX_OPTION_SPREAD_PCT:
            time.sleep(config.RATE_LIMIT_SLEEP_SECONDS)
            continue

        contract["bid_price"] = bid
        contract["ask_price"] = ask
        contract["spread_pct"] = round(spread_pct, 2)
        return contract

    return None
