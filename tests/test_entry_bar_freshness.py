"""Signal freshness regressions; synthetic data and fake broker only."""
from datetime import timedelta

import pytest

from autobott_v2.bar_timing import aware_utc
from test_entry_market_timing import EntryTape, run_cycle


@pytest.mark.parametrize("v2", [False, True])
@pytest.mark.parametrize("stale_symbol", ["all", "AAPL", "SPY", "QQQ", "VIXY"])
def test_old_completed_signal_bars_cannot_reach_broker(tmp_path, monkeypatch, v2, stale_symbol):
    original = EntryTape.get_stock_bars

    def stale_bars(self, symbols, **kwargs):
        result = original(self, symbols, **kwargs)
        for symbol, rows in result.items():
            if stale_symbol in {"all", symbol}:
                for row in rows:
                    row["t"] = (aware_utc(row["t"]) - timedelta(days=7)).isoformat()
        return result

    monkeypatch.setattr(EntryTape, "get_stock_bars", stale_bars)
    result, broker, client = run_cycle(tmp_path, monkeypatch, pair=True, v2=v2)
    assert broker.submitted == [], "Current quotes must not admit a week-old signal"
    assert result.trade_attempted_count == 0
    assert client.refreshes == []
    assert any(row["reason"] == "entry_stale_completed_bar" for row in result.skipped)


def _completed_snapshot(at, timeframe, age):
    from autobott_v2.bar_timing import bar_duration
    closed = at - timedelta(seconds=age)
    latest = {"timestamp": (closed - bar_duration(timeframe)).isoformat()}
    old = {"timestamp": (closed - timedelta(days=30)).isoformat()}
    rows = [old, latest]
    return {"bar_evidence": {"timestamp_semantics": "interval_start",
                            "completed_bars_only": True, "timeframe": timeframe,
                            "cutoff": at.isoformat()},
            "market_bars": rows,
            "context": {name: rows for name in ("spy_bars", "qqq_bars", "vix_bars")}}


@pytest.mark.parametrize("timeframe, limit", [("1Min", 90), ("1Hour", 3630)])
@pytest.mark.parametrize("offset", [-0.000001, 0, 0.000001])
def test_completed_bar_age_boundary_is_exact(timeframe, limit, offset):
    from types import SimpleNamespace
    from autobott_v2.entry_admission import _completed_evidence, EntryMarketRules, EntryMarketRejected
    from test_entry_market_timing import START
    row = _completed_snapshot(START, timeframe, limit + offset)
    kwargs = {"checked_at": START, "rules": EntryMarketRules()}
    if offset > 0:
        with pytest.raises(EntryMarketRejected, match="underlying") as exc:
            _completed_evidence(row, SimpleNamespace(timestamp=START), **kwargs)
        assert exc.value.reason == "entry_stale_completed_bar"
    else:
        result = _completed_evidence(row, SimpleNamespace(timestamp=START), **kwargs)
        assert result["max_completed_bar_age_seconds"] == limit


@pytest.mark.parametrize("v2", [False, True])
def test_refresh_latency_counts_against_signal_freshness(tmp_path, monkeypatch, v2):
    from autobott_v2.bar_timing import bar_duration
    from test_entry_market_timing import START
    original = EntryTape.get_stock_bars

    def near_deadline(self, symbols, **kwargs):
        result = original(self, symbols, **kwargs)
        duration = bar_duration(kwargs.get("timeframe", "1Min"))
        target_close = START - duration - timedelta(seconds=15)
        for rows in result.values():
            latest_start = max(aware_utc(row["t"]) for row in rows)
            shift = target_close - duration - latest_start
            for row in rows:
                row["t"] = (aware_utc(row["t"]) + shift).isoformat()
        return result

    monkeypatch.setattr(EntryTape, "get_stock_bars", near_deadline)
    clock = iter([START + timedelta(seconds=10), START + timedelta(seconds=20)])
    result, broker, client = run_cycle(tmp_path, monkeypatch, pair=True, v2=v2,
                                      clock=lambda: next(clock))
    assert client.refreshes, "The signal must pass the pre-refresh check"
    assert broker.submitted == []
    assert any(row["reason"] == "entry_stale_completed_bar" for row in result.skipped)


@pytest.mark.parametrize("bad", [0, -1, True, "30", float("nan"), float("inf")])
def test_invalid_bar_publication_delay_is_rejected(bad):
    from autobott_v2.entry_admission import EntryMarketRules
    with pytest.raises(ValueError, match="invalid_entry_market_age_limit"):
        EntryMarketRules(max_bar_publication_delay_seconds=bad)


def test_study_rejects_old_bars_using_the_runtime_freshness_rule(tmp_path):
    from test_primary_entry_study import assess, make_case
    case = make_case(tmp_path)
    row = case["snapshot"]
    for bars in [row["market_bars"], *[row["context"][k] for k in ("spy_bars", "qqq_bars", "vix_bars")]]:
        for bar in bars:
            bar["timestamp"] = (aware_utc(bar["timestamp"]) - timedelta(days=7)).isoformat()
    result = assess(case)
    assert result["status"] == "admission_rejected"
    assert result["reason"] == "entry_stale_completed_bar"
    assert result["quality"] is None


@pytest.mark.parametrize("stamp", ["2026-03-08T03:30:00-04:00", "2026-11-01T01:30:00-05:00"])
def test_bar_age_is_timezone_equivalent_and_does_not_rewrite_history(stamp):
    from copy import deepcopy
    from datetime import datetime
    from types import SimpleNamespace
    from autobott_v2.entry_admission import _completed_evidence, EntryMarketRules
    local = datetime.fromisoformat(stamp)
    row = _completed_snapshot(local, "1Min", 60)
    before = deepcopy(row)
    decision = SimpleNamespace(timestamp=local)
    expected = _completed_evidence(row, decision, checked_at=local, rules=EntryMarketRules())
    actual = _completed_evidence(row, decision, checked_at=aware_utc(local), rules=EntryMarketRules())
    assert actual == expected
    assert actual["completed_bar_ages_seconds"] == {k: 60 for k in ("underlying", "spy_bars", "qqq_bars", "vix_bars")}
    assert row == before
