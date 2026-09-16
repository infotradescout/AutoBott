"""Observed bid/ask provenance; never synthesize prices or a quote timestamp."""
from __future__ import annotations

import math
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any


class QuoteObservationError(ValueError):
    """A source quote is missing or internally invalid, not a fresh quote."""


def observed_quote_fields(quote: Any, *, allow_zero_bid: bool) -> tuple[float, float, str]:
    if not isinstance(quote, Mapping) or not quote:
        raise QuoteObservationError("missing_observed_quote")

    def first_present(*names: str) -> Any:
        for name in names:
            if name in quote:
                return quote[name]
        return None

    def price(*names: str) -> float:
        value = first_present(*names)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            raise QuoteObservationError("missing_or_nonfinite_observed_quote_price")
        return float(value)

    bid = price("bp", "bid_price", "bid")
    ask = price("ap", "ask_price", "ask")
    if bid < 0 or (bid == 0 and not allow_zero_bid) or ask <= 0 or ask < bid:
        raise QuoteObservationError("invalid_observed_bid_ask")
    raw_timestamp = first_present("t", "timestamp")
    if not isinstance(raw_timestamp, str) or not raw_timestamp.strip():
        raise QuoteObservationError("missing_observed_quote_timestamp")
    try:
        timestamp = datetime.fromisoformat(raw_timestamp.replace("Z", "+00:00"))
    except ValueError as exc:
        raise QuoteObservationError("invalid_observed_quote_timestamp") from exc
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise QuoteObservationError("observed_quote_timestamp_requires_timezone")
    # Preserve the source's age. A later trade/receipt time cannot refresh it.
    return bid, ask, timestamp.astimezone(UTC).isoformat()
