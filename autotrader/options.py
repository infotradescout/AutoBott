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


def _contract_symbol(contract: dict[str, Any]) -> str:
    return str(contract.get("symbol") or contract.get("option_symbol") or "").strip()


def _contract_expiration(contract: dict[str, Any]) -> Any:
    return contract.get("expiration_date") or contract.get("expiration")


def _contract_strike(contract: dict[str, Any]) -> float | None:
    strike = _safe_float(contract.get("strike_price"))
    if strike is None:
        strike = _safe_float(contract.get("strike"))
    return strike


def _filter_candidates_by_liquidity(
    candidates: list[dict[str, Any]],
    *,
    min_open_interest: float,
    min_daily_volume: float,
    fail_counts: dict[str, int],
) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    for details in candidates:
        active = str(details.get("status", "active")).lower() == "active"
        tradable = bool(details.get("tradable", True))
        strike = _contract_strike(details)
        exp = _contract_expiration(details)
        symbol = _contract_symbol(details)
        open_interest = _safe_float(details.get("open_interest"))
        volume = _safe_float(details.get("volume") or details.get("daily_volume"))

        if not active or not tradable:
            fail_counts["inactive_or_untradable"] += 1
            continue
        if strike is None or not exp or not symbol:
            fail_counts["missing_fields"] += 1
            continue
        if (not config.EMERGENCY_EXECUTION_MODE) and (open_interest is not None and open_interest < min_open_interest):
            fail_counts["low_open_interest"] += 1
            continue
        if (not config.EMERGENCY_EXECUTION_MODE) and (volume is not None and volume < min_daily_volume):
            fail_counts["low_volume"] += 1
            continue

        normalized = dict(details)
        normalized["symbol"] = symbol
        normalized["expiration_date"] = str(exp)
        normalized["strike_price"] = strike
        normalized["open_interest"] = open_interest
        normalized["daily_volume"] = volume
        selected.append(normalized)
    return selected


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
    direction = str(direction or "").lower().strip()
    if direction not in ("call", "put"):
        return None, f"invalid direction={direction!r}; only call/put supported"

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
    if (not contracts) and int(getattr(config, "MIN_DTE_TRADING_DAYS", 0) or 0) > 0:
        # Fail-open DTE fallback: on Fridays a 1-5 trading day window can exclude
        # both same-day weekly contracts and next-week weeklies.
        fallback_floor = _add_trading_days(today, 0)
        fallback_contracts = data_client.get_option_contracts(
            underlying_symbol=underlying_symbol,
            contract_type=direction,
            expiration_date_gte=fallback_floor,
            expiration_date_lte=expiry_ceiling,
        )
        if fallback_contracts:
            contracts = fallback_contracts
            print(
                f"[options] DTE fallback engaged for {underlying_symbol} {direction}: "
                f"window 0-{config.MAX_DTE_TRADING_DAYS} trading days."
            )
    if not contracts:
        return None, (
            "no contracts in DTE window "
            f"{config.MIN_DTE_TRADING_DAYS}-{config.MAX_DTE_TRADING_DAYS} trading days"
        )

    liquidity_candidates: list[dict[str, Any]] = []
    fail_counts = {
        "inactive_or_untradable": 0,
        "missing_fields": 0,
        "low_open_interest": 0,
        "low_volume": 0,
    }
    for contract in contracts:
        details = dict(contract)
        symbol = _contract_symbol(details)
        open_interest = _safe_float(details.get("open_interest"))
        volume = _safe_float(details.get("volume") or details.get("daily_volume"))
        # Only fetch individual contract detail if both OI and volume are missing
        # from the chain response. This avoids 429 rate-limit errors from making
        # one API call per strike across a full chain (30+ calls for NVDA, etc.).
        # Most chain responses already include open_interest; skip enrichment if so.
        needs_enrichment = (open_interest is None) and symbol
        if needs_enrichment:
            try:
                time.sleep(config.RATE_LIMIT_SLEEP_SECONDS)
                enriched = data_client.get_option_contract(symbol)
                if isinstance(enriched, dict):
                    details.update(enriched)
            except Exception as exc:  # noqa: BLE001
                print(f"[options] enrichment failed for {symbol}: {exc}")
            open_interest = _safe_float(details.get("open_interest"))
            volume = _safe_float(details.get("volume") or details.get("daily_volume"))
        details["open_interest"] = open_interest
        details["daily_volume"] = volume
        liquidity_candidates.append(details)

    liquidity_mode = "strict"
    filtered = _filter_candidates_by_liquidity(
        liquidity_candidates,
        min_open_interest=float(config.MIN_OPTION_OPEN_INTEREST),
        min_daily_volume=float(config.MIN_OPTION_DAILY_VOLUME),
        fail_counts=fail_counts,
    )

    if (
        (not filtered)
        and (not config.EMERGENCY_EXECUTION_MODE)
        and bool(getattr(config, "ENABLE_OPTION_LIQUIDITY_RELAX", True))
    ):
        base_oi = max(1.0, float(config.MIN_OPTION_OPEN_INTEREST))
        base_vol = max(1.0, float(config.MIN_OPTION_DAILY_VOLUME))
        for factor, label in ((0.5, "relaxed50"), (0.25, "relaxed25"), (0.1, "relaxed10")):
            relaxed_counts = {
                "inactive_or_untradable": 0,
                "missing_fields": 0,
                "low_open_interest": 0,
                "low_volume": 0,
            }
            relaxed = _filter_candidates_by_liquidity(
                liquidity_candidates,
                min_open_interest=max(1.0, base_oi * factor),
                min_daily_volume=max(1.0, base_vol * factor),
                fail_counts=relaxed_counts,
            )
            if relaxed:
                filtered = relaxed
                liquidity_mode = label
                print(
                    f"[options] liquidity relax engaged for {underlying_symbol} {direction}: "
                    f"mode={label} min_oi={max(1.0, base_oi * factor):.0f} min_vol={max(1.0, base_vol * factor):.0f}"
                )
                break

        # Final fail-open path: if liquidity thresholds still empty the pool,
        # keep active/tradable contracts and let quote/spread gates decide safety.
        if not filtered:
            failopen_counts = {
                "inactive_or_untradable": 0,
                "missing_fields": 0,
                "low_open_interest": 0,
                "low_volume": 0,
            }
            failopen = _filter_candidates_by_liquidity(
                liquidity_candidates,
                min_open_interest=0.0,
                min_daily_volume=0.0,
                fail_counts=failopen_counts,
            )
            if failopen:
                filtered = failopen
                liquidity_mode = "failopen_liquidity"
                print(
                    f"[options] fail-open liquidity engaged for {underlying_symbol} {direction}: "
                    "relying on quote/spread checks."
                )

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
        if (not config.EMERGENCY_EXECUTION_MODE) and exp_date == today and open_interest < float(config.MIN_OPTION_OPEN_INTEREST_0DTE):
            fail_counts["low_open_interest"] += 1
            continue
        strike_val = _contract_strike(contract)
        if strike_val is None:
            fail_counts["missing_fields"] += 1
            continue
        strike_gap = abs(float(strike_val) - underlying_price)
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
            strike_val = _contract_strike(contract)
            if strike_val is None:
                continue
            strike_gap = abs(float(strike_val) - underlying_price)
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
        symbol = _contract_symbol(contract)
        if not symbol:
            quote_fail_counts["bad_quote"] += 1
            continue
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
        if (not config.EMERGENCY_EXECUTION_MODE) and spread_pct >= config.MAX_OPTION_SPREAD_PCT:
            quote_fail_counts["spread_too_wide"] += 1
            time.sleep(config.RATE_LIMIT_SLEEP_SECONDS)
            continue

        contract["bid_price"] = bid
        contract["ask_price"] = ask
        contract["spread_pct"] = round(spread_pct, 2)
        return contract, f"ok({liquidity_mode})"

    if config.EMERGENCY_EXECUTION_MODE and scored:
        fallback = scored[0]
        fallback["bid_price"] = _safe_float(fallback.get("bid_price"))
        fallback["ask_price"] = _safe_float(fallback.get("ask_price"))
        fallback["spread_pct"] = fallback.get("spread_pct", "")
        return fallback, "emergency_fallback_without_quote"

    reason = (
        f"quotes rejected: bad_quote={quote_fail_counts['bad_quote']}, "
        f"nonpositive_mid={quote_fail_counts['nonpositive_mid']}, "
        f"spread_too_wide={quote_fail_counts['spread_too_wide']}>=max({config.MAX_OPTION_SPREAD_PCT})"
    )
    return None, reason
