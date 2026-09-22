"""Causal price references for US-equity/ETF signal bars.

The weighted reference is a supplied-bar HLC3 proxy for the latest New York
calendar date, not tick VWAP, a full-day feed, or an exchange-session calendar.
Benchmark close returns use the same observed interval-start endpoints as the
instrument; missing endpoints are unknown rather than a zero market return.
"""
from __future__ import annotations

from datetime import UTC, datetime
import math
from zoneinfo import ZoneInfo

from .phase1_models import MarketBar

MARKET_TIMEZONE = ZoneInfo("America/New_York")


def _time(bar: MarketBar) -> datetime:
    value = bar.timestamp
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("signal_reference_timestamp_requires_timezone")
    return value.astimezone(UTC)


def current_market_date_reference(bars: list[MarketBar]) -> float:
    if not bars:
        return 0.0
    latest = bars[-1]
    last_time = _time(latest)
    last_date = last_time.astimezone(MARKET_TIMEZONE).date()
    rows = [bar for bar in bars if _time(bar) <= last_time
            and _time(bar).astimezone(MARKET_TIMEZONE).date() == last_date]
    volume = sum(max(bar.volume, 0) for bar in rows)
    if volume <= 0:
        # Missing volume does not borrow an older day's reference. Returning
        # the last close makes its price-distance contribution exactly neutral.
        return latest.close
    return sum(((bar.high + bar.low + bar.close) / 3.0) * max(bar.volume, 0)
               for bar in rows) / volume


def aligned_benchmark_return(
    spy_bars: list[MarketBar], qqq_bars: list[MarketBar], *,
    start: datetime, end: datetime,
) -> tuple[float | None, int]:
    if start.tzinfo is None or end.tzinfo is None or start.utcoffset() is None or end.utcoffset() is None:
        raise ValueError("benchmark_window_requires_timezone")
    start, end = start.astimezone(UTC), end.astimezone(UTC)
    if start >= end:
        raise ValueError("benchmark_window_must_increase")
    returns = []
    for rows in (spy_bars, qqq_bars):
        endpoints: dict[datetime, float] = {}
        invalid = False
        for bar in rows:
            stamp = _time(bar)
            if stamp not in {start, end}:
                continue
            close = bar.close
            if isinstance(close, bool) or not isinstance(close, (int, float)) or not math.isfinite(close) or close <= 0:
                invalid = True
                break
            if stamp in endpoints and endpoints[stamp] != close:
                invalid = True
                break
            endpoints[stamp] = close
        if not invalid and start in endpoints and end in endpoints:
            returns.append((endpoints[end] - endpoints[start]) / endpoints[start])
    return (sum(returns) / len(returns), len(returns)) if returns else (None, 0)
