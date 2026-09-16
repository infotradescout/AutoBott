"""Completed-bar semantics and deterministic admission bounds."""
from copy import deepcopy
from datetime import UTC, datetime, timedelta
import pytest
from autobott_v2.bar_timing import aware_utc, bar_duration, completed_stock_bars
from autobott_v2.entry_admission import EntryMarketRules

AT = datetime(2026, 9, 16, 15, 0, tzinfo=UTC)


def rows(n=36):
    return [{"t": (AT - timedelta(minutes=n-1-i)).isoformat(),
             "o": 100, "h": 101, "l": 99, "c": 100.5, "v": 100}
            for i in range(n)]


def normalize(raw):
    return completed_stock_bars(raw, cutoff=AT, timeframe="1Min", lookback=35)


def test_exact_close_boundary_is_complete_but_current_bar_is_not():
    result = normalize(rows())
    assert len(result) == 35
    assert aware_utc(result[-1]["timestamp"]) == AT - timedelta(minutes=1)


def test_shuffled_and_identical_duplicate_rows_cannot_change_indicators():
    raw = rows()
    assert normalize(raw) == normalize(list(reversed(raw)) + deepcopy(raw))


def test_duplicate_start_with_conflicting_ohlc_fails_closed():
    raw = rows()
    conflict = {**raw[0], "c": 100.6}
    with pytest.raises(ValueError, match="conflicting_completed_bar_timestamp"):
        normalize(raw + [conflict])


def test_timestamp_format_equivalence_does_not_create_an_extra_bar():
    raw = rows()
    duplicate = {**raw[0], "t": raw[0]["t"].replace("+00:00", "Z")}
    assert normalize(raw) == normalize(raw + [duplicate])


def test_future_price_spike_cannot_change_completed_history():
    raw = rows()
    raw[-1].update(o=10000, h=12000, l=1, c=11000, v=1000000)
    later = {**raw[-1], "t": (AT + timedelta(days=1)).isoformat()}
    assert normalize(raw + [later]) == normalize(rows())


def test_minimum_applies_after_excluding_forming_bar_and_duplicates():
    raw = rows(30)
    with pytest.raises(ValueError, match="insufficient_completed_market_bars"):
        normalize(raw + deepcopy(raw))


@pytest.mark.parametrize("timeframe, seconds", [("1Min",60),("5Min",300),("1Hour",3600),("2Hour",7200)])
def test_duration_is_explicit(timeframe, seconds):
    assert bar_duration(timeframe).total_seconds() == seconds


@pytest.mark.parametrize("timeframe", ["1Day", "0Min", "unknown", None, "-1Hour"])
def test_calendar_or_unknown_timeframe_requires_separate_contract(timeframe):
    with pytest.raises(ValueError): bar_duration(timeframe)


@pytest.mark.parametrize("change", [{"t": "2026-09-16T14:30:00"}, {"t": "bad"}, {"o": float("nan")},
                                    {"h": float("inf")}, {"v": True}, {"v": -1}, {"v": 1.5},
                                    {"o": 0}, {"l": 102}, {"h": 99}])
def test_invalid_completed_bar_never_enters_signal(change):
    raw = rows()
    raw[0].update(change)
    with pytest.raises(ValueError): normalize(raw)


def test_normalization_does_not_mutate_provider_data():
    raw = rows()
    before = deepcopy(raw)
    normalize(raw)
    assert raw == before


@pytest.mark.parametrize("value", [0, -1, True, float("nan"), float("inf"), "30"])
def test_entry_age_bound_validation(value):
    with pytest.raises(ValueError): EntryMarketRules(max_quote_age_seconds=value)
    with pytest.raises(ValueError): EntryMarketRules(max_decision_age_seconds=value)
