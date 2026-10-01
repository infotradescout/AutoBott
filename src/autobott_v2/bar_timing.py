"""Causal fixed-interval bars. Alpaca bar timestamps identify interval starts."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
import math
import re
from typing import Any


def aware_utc(value: str | datetime) -> datetime:
    result = datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else value
    if not isinstance(result, datetime) or result.tzinfo is None or result.utcoffset() is None:
        raise ValueError("market_timestamp_requires_timezone")
    return result.astimezone(UTC)


def bar_duration(timeframe: str) -> timedelta:
    match = re.fullmatch(r"([1-9][0-9]*)(Min|Hour)", timeframe) if isinstance(timeframe, str) else None
    if match is None:
        raise ValueError("unsupported_fixed_intraday_bar_timeframe")
    # Daily/week/month bars need an explicit session/calendar contract instead.
    return timedelta(seconds=int(match[1]) * (60 if match[2] == "Min" else 3600))


def completed_stock_bars(rows: Sequence[Mapping[str, Any]], *, cutoff: datetime,
                         timeframe: str, lookback: int, minimum: int = 30) -> list[dict[str, Any]]:
    cutoff = aware_utc(cutoff)
    duration = bar_duration(timeframe)
    if type(lookback) is not int or type(minimum) is not int or not 1 <= minimum <= lookback:
        raise ValueError("invalid_completed_bar_lookback")
    by_time: dict[datetime, dict[str, Any]] = {}
    for raw in rows:
        if not isinstance(raw, Mapping):
            raise ValueError("invalid_market_bar")
        timestamp = aware_utc(raw.get("t", raw.get("timestamp")))
        if timestamp + duration > cutoff:
            continue
        normalized: dict[str, Any] = {"timestamp": timestamp.isoformat()}
        for long, short in (("open", "o"), ("high", "h"), ("low", "l"), ("close", "c"), ("volume", "v")):
            value = raw.get(short, raw.get(long))
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
                raise ValueError("invalid_completed_bar_numeric_field")
            if (long == "volume" and (value < 0 or int(value) != value)) or (long != "volume" and value <= 0):
                raise ValueError("invalid_completed_bar_price_or_volume")
            normalized[long] = int(value) if long == "volume" else round(float(value), 4)
        if not normalized["low"] <= min(normalized["open"], normalized["close"]) <= max(normalized["open"], normalized["close"]) <= normalized["high"]:
            raise ValueError("invalid_completed_bar_range")
        if timestamp in by_time and by_time[timestamp] != normalized:
            raise ValueError("conflicting_completed_bar_timestamp")
        by_time[timestamp] = normalized
    result = [by_time[t] for t in sorted(by_time)][-lookback:]
    if len(result) < minimum:
        raise ValueError("insufficient_completed_market_bars")
    return result
