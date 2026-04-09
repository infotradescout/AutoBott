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


def _safe_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except Exception:
        return None


def _extract_delta(contract: dict[str, Any]) -> float | None:
    raw = contract.get("delta")
    if raw is None:
        greeks = contract.get("greeks")
        if isinstance(greeks, dict):
            raw = greeks.get("delta")
    value = _safe_float(raw)
    if value is None:
        return None
    return abs(value)


def select_atm_option_contract(
    data_client: AlpacaDataClient,
    underlying_symbol: str,
    direction: str,
    underlying_price: float,
    now_et: datetime | None = None,
) -> dict[str, Any] | None:
    contract, _reason = select_atm_option_contract_with_reason(
        data_client=data_client,
        underlying_symbol=underlying_symbol,
        direction=direction,
        underlying_price=underlying_price,
        now_et=now_et,
    )
    return contract


def select_atm_option_contract_with_reason(
    data_client: AlpacaDataClient,
    underlying_symbol: str,
    direction: str,
    underlying_price: float,
    now_et: datetime | None = None,
) -> tuple[dict[str, Any] | None, str]:
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
        return None, (
            "no contracts in DTE window "
            f"{config.MIN_DTE_TRADING_DAYS}-{config.MAX_DTE_TRADING_DAYS} trading days"
        )

    filtered: list[dict[str, Any]] = []
    fail_counts = {
        "inactive_or_untradable": 0,
        "missing_fields": 0,
        "low_open_interest": 0,
        "low_volume": 0,
    }
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
        if not active or not tradable:
            fail_counts["inactive_or_untradable"] += 1
            continue
        if strike is None or not exp or not symbol:
            fail_counts["missing_fields"] += 1
            continue
        if open_interest is None or open_interest <= config.MIN_OPTION_OPEN_INTEREST:
            fail_counts["low_open_interest"] += 1
            continue
        if volume is None or volume <= config.MIN_OPTION_DAILY_VOLUME:
            fail_counts["low_volume"] += 1
            continue
        if active and tradable and strike is not None and exp and symbol:
            details["open_interest"] = open_interest
            details["daily_volume"] = volume
            filtered.append(details)

    if not filtered:
        reason = (
            f"no eligible contracts: inactive={fail_counts['inactive_or_untradable']}, "
            f"missing={fail_counts['missing_fields']}, "
            f"low_oi={fail_counts['low_open_interest']}<=min({config.MIN_OPTION_OPEN_INTEREST}), "
            f"low_vol={fail_counts['low_volume']}<=min({config.MIN_OPTION_DAILY_VOLUME})"
        )
        return None, reason

    scored: list[dict[str, Any]] = []
    for contract in filtered:
        exp_date = _safe_date(contract.get("expiration_date"))
        open_interest = _safe_float(contract.get("open_interest")) or 0.0
        if exp_date == today and open_interest < float(config.MIN_OPTION_OPEN_INTEREST_0DTE):
            fail_counts["low_open_interest"] += 1
            continue
        strike_gap = abs(float(contract["strike_price"]) - underlying_price)
        delta_abs = _extract_delta(contract)
        target_delta = float(config.TARGET_DELTA_FALLBACK)
        if direction == "call":
            target_delta = max(float(config.TARGET_DELTA_MIN), min(float(config.TARGET_DELTA_MAX), target_delta))
        if direction == "put":
            target_delta = max(float(config.TARGET_DELTA_MIN), min(float(config.TARGET_DELTA_MAX), target_delta))
        if config.ENABLE_DELTA_TARGETING and delta_abs is not None:
            contract["delta_abs"] = round(delta_abs, 4)
            # Prefer contracts inside target band; outside still allowed as fallback.
            in_band = float(config.TARGET_DELTA_MIN) <= delta_abs <= float(config.TARGET_DELTA_MAX)
            delta_penalty = abs(delta_abs - target_delta) * (1.0 if in_band else 3.0)
        elif delta_abs is not None:
            contract["delta_abs"] = round(delta_abs, 4)
            delta_penalty = abs(delta_abs - target_delta)
        else:
            contract["delta_abs"] = ""
            delta_penalty = 0.25
        score = (
            (0 if exp_date is None else (exp_date - today).days) * 0.20
            + strike_gap * 0.05
            + delta_penalty
        )
        contract["_select_score"] = score
        scored.append(contract)

    if not scored:
        # Fail-open fallback: if stricter 0DTE quality checks empty the pool,
        # fall back to the already-liquidity-filtered set so entries can proceed.
        scored = list(filtered)
        for contract in scored:
            strike_gap = abs(float(contract["strike_price"]) - underlying_price)
            contract["_select_score"] = strike_gap * 0.05
        if not scored:
            return None, "no eligible contracts after 0DTE/quality checks"

    scored.sort(key=lambda c: (float(c.get("_select_score", 99.0)), c.get("expiration_date", "")))

    quote_fail_counts = {
        "bad_quote": 0,
        "nonpositive_mid": 0,
        "spread_too_wide": 0,
    }
    for contract in scored[:40]:
        symbol = contract["symbol"]
        quote = data_client.get_latest_option_quote(symbol)
        bid = _safe_float(quote.get("bid"))
        ask = _safe_float(quote.get("ask"))
        if bid is None or ask is None or bid <= 0 or ask <= 0 or ask < bid:
            quote_fail_counts["bad_quote"] += 1
            time.sleep(config.RATE_LIMIT_SLEEP_SECONDS)
            continue
        mid = (bid + ask) / 2
        if mid <= 0:
            quote_fail_counts["nonpositive_mid"] += 1
            time.sleep(config.RATE_LIMIT_SLEEP_SECONDS)
            continue
        spread_pct = ((ask - bid) / mid) * 100
        if spread_pct >= config.MAX_OPTION_SPREAD_PCT:
            quote_fail_counts["spread_too_wide"] += 1
            time.sleep(config.RATE_LIMIT_SLEEP_SECONDS)
            continue

        contract["bid_price"] = bid
        contract["ask_price"] = ask
        contract["spread_pct"] = round(spread_pct, 2)
        return contract, "ok"

    reason = (
        f"quotes rejected: bad_quote={quote_fail_counts['bad_quote']}, "
        f"nonpositive_mid={quote_fail_counts['nonpositive_mid']}, "
        f"spread_too_wide={quote_fail_counts['spread_too_wide']}>=max({config.MAX_OPTION_SPREAD_PCT})"
    )
    return None, reason
