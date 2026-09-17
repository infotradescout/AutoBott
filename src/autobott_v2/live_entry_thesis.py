"""Pure entry-time invalidation against the last completed signal bar.

A price outside the whole signal bar against the proposed direction is rejected.
This is a candidate entry policy, not a proven predictive edge or an exit rule.
"""
from __future__ import annotations

from collections.abc import Mapping
import math
from typing import Any


def _price(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
        raise ValueError("invalid_live_thesis_price")
    return float(value)


def assess_live_entry_thesis(snapshot: Mapping[str, Any], *, direction: str,
                             signal_symbol: str, bid: float, ask: float) -> dict[str, Any]:
    """Check only observed prices; do not infer the unobserved intervening path.

    A bullish breach requires the entire quote below the bar low (ask < low).
    A bearish breach requires the entire quote above the bar high (bid > high).
    A touch or straddle alone is not a confirmed breach. Proxy/index price
    bases cannot be compared here and are explicitly NOT evaluated.
    """
    if direction not in {"bullish", "bearish"}:
        raise ValueError("directional_live_thesis_required")
    ticker = snapshot.get("ticker")
    if not isinstance(ticker, str) or not ticker.strip() or not isinstance(signal_symbol, str) or not signal_symbol.strip():
        raise ValueError("live_thesis_symbol_required")
    bid, ask = _price(bid), _price(ask)
    if bid > ask:
        raise ValueError("crossed_live_thesis_quote")
    evidence = {"rule": "completed_signal_bar_invalidation.v1", "direction": direction,
                "signal_symbol": signal_symbol.upper(), "bid": bid, "ask": ask}
    if signal_symbol.upper() != ticker.upper():
        return {**evidence, "status": "not_evaluated", "reason": "proxy_and_index_price_bases_differ"}
    declared = snapshot.get("bar_evidence", {}).get("signal_symbol", ticker)
    if not isinstance(declared, str) or declared.upper() != ticker.upper():
        raise ValueError("live_thesis_bar_symbol_mismatch")
    rows = snapshot.get("market_bars")
    if not isinstance(rows, list) or not rows or not isinstance(rows[-1], Mapping):
        raise ValueError("live_thesis_completed_bar_required")
    bar = rows[-1]
    low, high = _price(bar.get("low")), _price(bar.get("high"))
    if low > high:
        raise ValueError("invalid_live_thesis_bar_range")
    invalidated = ask < low if direction == "bullish" else bid > high
    return {**evidence, "status": "invalidated" if invalidated else "not_invalidated",
            "bar_timestamp": bar.get("timestamp"), "bar_low": low, "bar_high": high,
            "boundary": low if direction == "bullish" else high,
            "reason": "quote_breached_signal_bar_against_direction" if invalidated else "no_observed_adverse_bar_breach"}
