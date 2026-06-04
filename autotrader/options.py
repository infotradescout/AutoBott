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


def _minutes_until_entry_expiry_cutoff(now_et: datetime) -> int | None:
    try:
        cutoff_times = []
        for value in (
            getattr(config, "OPTION_EXPIRY_EXIT_TIME", "15:55"),
            getattr(config, "HARD_CLOSE_TIME", "16:00"),
        ):
            hour_text, minute_text = str(value).split(":", 1)
            cutoff_times.append(
                now_et.replace(hour=int(hour_text), minute=int(minute_text), second=0, microsecond=0)
            )
        cutoff_dt = min(cutoff_times)
        return int((cutoff_dt - now_et).total_seconds() // 60)
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


def _contract_open_interest(contract: dict[str, Any]) -> float:
    value = _safe_float(contract.get("open_interest"))
    return float(value or 0.0)


def _contract_daily_volume(contract: dict[str, Any]) -> float:
    value = _safe_float(contract.get("volume") or contract.get("daily_volume"))
    return float(value or 0.0)


def _is_index_etf(symbol: str) -> bool:
    return str(symbol or "").upper() in {"SPY", "QQQ", "IWM"}


def _quote_is_stale(quote: dict[str, Any], now_et: datetime) -> bool:
    raw_timestamp = (
        quote.get("timestamp")
        or quote.get("t")
        or quote.get("time")
        or quote.get("quote_time")
        or quote.get("updated_at")
    )
    if not raw_timestamp:
        return False
    try:
        quote_time = datetime.fromisoformat(str(raw_timestamp).replace("Z", "+00:00"))
        if quote_time.tzinfo is None:
            quote_time = pytz.timezone(config.EASTERN_TZ).localize(quote_time)
        return (now_et - quote_time.astimezone(now_et.tzinfo)).total_seconds() > 120
    except Exception:
        return True


def _contract_quality_reject_reason(
    *,
    contract: dict[str, Any],
    underlying_symbol: str,
    underlying_price: float,
    bid: float | None,
    ask: float | None,
    quote: dict[str, Any] | None = None,
    now_et: datetime,
) -> tuple[str | None, dict[str, Any]]:
    strike = _contract_strike(contract)
    expiration = _safe_date(contract.get("expiration_date"))
    delta_abs = _extract_delta(contract)
    open_interest = _contract_open_interest(contract)
    daily_volume = _contract_daily_volume(contract)
    quality: dict[str, Any] = {
        "contract_bid": bid if bid is not None else "",
        "contract_ask": ask if ask is not None else "",
        "contract_delta_abs": round(float(delta_abs), 4) if delta_abs is not None else "",
        "contract_open_interest": int(open_interest),
        "contract_daily_volume": int(daily_volume),
        "contract_spread_pct": "",
        "contract_strike_distance_pct": "",
    }

    if bid is None or ask is None or bid <= 0 or ask <= 0 or ask < bid:
        return "contract_quality_bad_quote", quality
    if _is_index_etf(underlying_symbol) and _quote_is_stale(dict(quote or {}), now_et):
        return "contract_quality_bad_quote", quality

    midpoint = (bid + ask) / 2.0
    if midpoint <= 0:
        return "contract_quality_bad_quote", quality

    spread_pct = ((ask - bid) / midpoint) * 100.0
    quality["contract_spread_pct"] = round(spread_pct, 4)
    max_spread_pct = 1.5
    if spread_pct > max_spread_pct:
        return "contract_quality_spread_too_wide", quality

    if strike is None or underlying_price <= 0:
        return "contract_quality_strike_too_far", quality
    strike_distance_pct = abs(float(strike) - float(underlying_price)) / float(underlying_price) * 100.0
    quality["contract_strike_distance_pct"] = round(strike_distance_pct, 4)
    max_strike_distance = 1.5 if _is_index_etf(underlying_symbol) else 2.5
    if strike_distance_pct > max_strike_distance:
        return "contract_quality_strike_too_far", quality

    premium_pct = (ask / float(underlying_price)) * 100.0
    max_premium_pct = 2.5 if _is_index_etf(underlying_symbol) else 5.0
    if premium_pct > max_premium_pct:
        return "contract_quality_premium_too_large", quality

    if open_interest <= 0 and daily_volume <= 0 and spread_pct > 1.0:
        return "contract_quality_illiquid", quality

    if expiration == now_et.date() and (now_et.hour, now_et.minute) >= (13, 30):
        if not (_is_index_etf(underlying_symbol) and spread_pct <= 1.5):
            return "contract_quality_late_0dte_block", quality

    if delta_abs is not None and not (0.40 <= delta_abs <= 0.60):
        return "contract_quality_bad_delta", quality

    return None, quality


def _contract_quality_rank(contract: dict[str, Any], underlying_price: float) -> tuple[float, float, float, float, float, float]:
    spread_pct = float(contract.get("contract_spread_pct", contract.get("spread_pct", 999.0)) or 999.0)
    strike_distance = float(contract.get("contract_strike_distance_pct", 999.0) or 999.0)
    delta_abs = _safe_float(contract.get("contract_delta_abs"))
    delta_gap = abs(float(delta_abs) - 0.50) if delta_abs is not None else 0.25
    open_interest = _contract_open_interest(contract)
    daily_volume = _contract_daily_volume(contract)
    ask = _safe_float(contract.get("ask_price")) or 999.0
    premium_pct = (ask / underlying_price * 100.0) if underlying_price > 0 else 999.0
    liquidity_rank = -(open_interest + daily_volume)
    return (spread_pct, strike_distance, delta_gap, liquidity_rank, premium_pct, float(contract.get("_select_score", 999.0) or 999.0))


def _nearest_strike_lane_candidates(candidates: list[dict[str, Any]], underlying_price: float) -> list[dict[str, Any]]:
    strikes = sorted({float(strike) for item in candidates if (strike := _contract_strike(item)) is not None})
    if not strikes:
        return candidates
    below = [strike for strike in strikes if strike <= underlying_price]
    above = [strike for strike in strikes if strike >= underlying_price]
    allowed = {min(strikes, key=lambda strike: abs(strike - underlying_price))}
    if below:
        allowed.add(max(below))
    if above:
        allowed.add(min(above))
    return [item for item in candidates if float(_contract_strike(item) or -1.0) in allowed]


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
    enrich_attempts = 0
    enrich_attempt_cap = max(0, int(getattr(config, "OPTION_ENRICHMENT_MAX_ATTEMPTS_PER_CYCLE", 3) or 3))
    enrich_rate_limited = False
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
        needs_enrichment = (open_interest is None) and symbol and (not enrich_rate_limited)
        if needs_enrichment:
            if enrich_attempts >= enrich_attempt_cap:
                needs_enrichment = False
            else:
                enrich_attempts += 1
        if needs_enrichment:
            try:
                time.sleep(config.RATE_LIMIT_SLEEP_SECONDS)
                enriched = data_client.get_option_contract(symbol)
                if isinstance(enriched, dict):
                    details.update(enriched)
            except Exception as exc:  # noqa: BLE001
                print(f"[options] enrichment failed for {symbol}: {exc}")
                if "429" in str(exc):
                    enrich_rate_limited = True
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
    if _is_index_etf(underlying_symbol):
        filtered = _nearest_strike_lane_candidates(filtered, float(underlying_price))
        if not filtered:
            return None, "no ETF lane contracts at nearest strikes"

    scored: list[dict[str, Any]] = []
    for contract in filtered:
        exp_date = _safe_date(contract.get("expiration_date"))
        open_interest = _safe_float(contract.get("open_interest")) or 0.0
        if (
            (not config.EMERGENCY_EXECUTION_MODE)
            and exp_date == today
            and not _is_index_etf(underlying_symbol)
            and open_interest < float(config.MIN_OPTION_OPEN_INTEREST_0DTE)
        ):
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
        "premium_too_expensive": 0,
        "strike_too_far": 0,
        "contract_quality_bad_quote": 0,
        "contract_quality_spread_too_wide": 0,
        "contract_quality_strike_too_far": 0,
        "contract_quality_premium_too_large": 0,
        "contract_quality_bad_delta": 0,
        "contract_quality_illiquid": 0,
        "contract_quality_late_0dte_block": 0,
    }
    quality_candidates: list[dict[str, Any]] = []
    for contract in scored[:40]:
        symbol = _contract_symbol(contract)
        if not symbol:
            quote_fail_counts["bad_quote"] += 1
            quote_fail_counts["contract_quality_bad_quote"] += 1
            continue
        quote = data_client.get_latest_option_quote(symbol)
        bid = _safe_float(quote.get("bid"))
        ask = _safe_float(quote.get("ask"))
        quality_reason, quality = _contract_quality_reject_reason(
            contract=contract,
            underlying_symbol=underlying_symbol,
            underlying_price=float(underlying_price),
            bid=bid,
            ask=ask,
            quote=quote,
            now_et=now_et,
        )
        if quality_reason and not config.EMERGENCY_EXECUTION_MODE:
            quote_fail_counts[quality_reason] = int(quote_fail_counts.get(quality_reason, 0)) + 1
            if quality_reason == "contract_quality_bad_quote":
                quote_fail_counts["bad_quote"] += 1
            elif quality_reason == "contract_quality_spread_too_wide":
                quote_fail_counts["spread_too_wide"] += 1
            elif quality_reason == "contract_quality_premium_too_large":
                quote_fail_counts["premium_too_expensive"] += 1
            elif quality_reason == "contract_quality_strike_too_far":
                quote_fail_counts["strike_too_far"] += 1
            time.sleep(config.RATE_LIMIT_SLEEP_SECONDS)
            continue

        contract["bid_price"] = bid
        contract["ask_price"] = ask
        contract["spread_pct"] = round(float(quality.get("contract_spread_pct") or 0.0), 2)
        contract["daily_volume"] = _contract_daily_volume(contract)
        contract.update(quality)
        contract["contract_quality_reason"] = "contract_quality_selected"
        quality_candidates.append(contract)

    if quality_candidates:
        quality_candidates.sort(key=lambda item: _contract_quality_rank(item, float(underlying_price)))
        selected = quality_candidates[0]
        selected["selected_contract_rank"] = 1
        selected["selected_contract_score"] = round(
            float(_contract_quality_rank(selected, float(underlying_price))[0])
            + float(_contract_quality_rank(selected, float(underlying_price))[1])
            + float(_contract_quality_rank(selected, float(underlying_price))[2]),
            4,
        )
        return selected, f"ok({liquidity_mode})"

    if config.EMERGENCY_EXECUTION_MODE and scored:
        fallback = scored[0]
        fallback["bid_price"] = _safe_float(fallback.get("bid_price"))
        fallback["ask_price"] = _safe_float(fallback.get("ask_price"))
        fallback["spread_pct"] = fallback.get("spread_pct", "")
        return fallback, "emergency_fallback_without_quote"

    reason = (
        f"quotes rejected: bad_quote={quote_fail_counts['bad_quote']}, "
        f"nonpositive_mid={quote_fail_counts['nonpositive_mid']}, "
        f"spread_too_wide={quote_fail_counts['spread_too_wide']}>=max({config.MAX_OPTION_SPREAD_PCT}), "
        f"premium_too_expensive={quote_fail_counts['premium_too_expensive']}>max("
        f"{getattr(config, 'MAX_OPTION_PREMIUM_TO_UNDERLYING_PCT', 0.0)}% underlying), "
        f"strike_too_far={quote_fail_counts['strike_too_far']}>max("
        f"{getattr(config, 'MAX_OPTION_STRIKE_DISTANCE_PCT', 0.0)}% underlying), "
        f"contract_quality_bad_quote={quote_fail_counts['contract_quality_bad_quote']}, "
        f"contract_quality_spread_too_wide={quote_fail_counts['contract_quality_spread_too_wide']}, "
        f"contract_quality_strike_too_far={quote_fail_counts['contract_quality_strike_too_far']}, "
        f"contract_quality_premium_too_large={quote_fail_counts['contract_quality_premium_too_large']}, "
        f"contract_quality_bad_delta={quote_fail_counts['contract_quality_bad_delta']}, "
        f"contract_quality_illiquid={quote_fail_counts['contract_quality_illiquid']}, "
        f"contract_quality_late_0dte_block={quote_fail_counts['contract_quality_late_0dte_block']}"
    )
    return None, reason
