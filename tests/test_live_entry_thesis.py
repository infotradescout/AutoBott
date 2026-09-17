"""Synthetic policy contracts, not market-performance observations."""
from copy import deepcopy
import pytest
from autobott_v2.live_entry_thesis import assess_live_entry_thesis


def snapshot():
    return {"ticker": "AAPL", "bar_evidence": {"signal_symbol": "AAPL"},
            "market_bars": [{"timestamp": "2026-09-17T15:00:00+00:00", "low": 100, "high": 102}]}


@pytest.mark.parametrize("direction,bid,ask,expected", [
    ("bullish", 99, 99.99, "invalidated"), ("bullish", 99, 100, "not_invalidated"),
    ("bullish", 99, 101, "not_invalidated"), ("bullish", 101, 101.1, "not_invalidated"),
    ("bullish", 103, 104, "not_invalidated"),
    ("bearish", 102.01, 103, "invalidated"), ("bearish", 102, 103, "not_invalidated"),
    ("bearish", 101, 103, "not_invalidated"), ("bearish", 100.1, 100.2, "not_invalidated"),
    ("bearish", 98, 99, "not_invalidated"),
])
def test_directional_boundaries_and_spread_straddles(direction, bid, ask, expected):
    row = snapshot(); before = deepcopy(row)
    result = assess_live_entry_thesis(row, direction=direction, signal_symbol="AAPL", bid=bid, ask=ask)
    assert result["status"] == expected
    assert row == before


@pytest.mark.parametrize("field", ["bid", "ask", "low", "high"])
@pytest.mark.parametrize("bad", [0, -1, True, float("nan"), float("inf"), "100"])
def test_invalid_prices_never_support_admission(field, bad):
    row = snapshot(); args = {"bid": 101, "ask": 101.1}
    if field in args: args[field] = bad
    else: row["market_bars"][-1][field] = bad
    with pytest.raises(ValueError):
        assess_live_entry_thesis(row, direction="bullish", signal_symbol="AAPL", **args)


def test_proxy_units_are_not_misrepresented_as_a_valid_thesis():
    row = snapshot(); row["ticker"] = "VIX"; row["bar_evidence"]["signal_symbol"] = "VIXY"
    result = assess_live_entry_thesis(row, direction="bullish", signal_symbol="VIXY", bid=5, ask=6)
    assert result["status"] == "not_evaluated"
    assert result["reason"] == "proxy_and_index_price_bases_differ"


def test_direction_is_required():
    with pytest.raises(ValueError):
        assess_live_entry_thesis(snapshot(), direction="neutral", signal_symbol="AAPL", bid=101, ask=102)


def test_crossed_quote_is_rejected():
    with pytest.raises(ValueError):
        assess_live_entry_thesis(snapshot(), direction="bullish", signal_symbol="AAPL", bid=103, ask=102)


def test_bar_symbol_mismatch_is_not_comparable():
    row = snapshot(); row["bar_evidence"]["signal_symbol"] = "ANOTHER"
    with pytest.raises(ValueError):
        assess_live_entry_thesis(row, direction="bullish", signal_symbol="AAPL", bid=101, ask=102)


def test_missing_completed_bar_is_not_evidence():
    row = snapshot(); row["market_bars"] = []
    with pytest.raises(ValueError):
        assess_live_entry_thesis(row, direction="bullish", signal_symbol="AAPL", bid=101, ask=102)


def test_reversed_bar_range_is_rejected():
    row = snapshot(); row["market_bars"][-1].update(low=103, high=100)
    with pytest.raises(ValueError):
        assess_live_entry_thesis(row, direction="bullish", signal_symbol="AAPL", bid=101, ask=102)
