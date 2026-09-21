from copy import deepcopy
from datetime import UTC, date, datetime
from types import SimpleNamespace

import pytest

from autobott_v2 import expiry_coverage as coverage
from autobott_v2 import trading_cycle_v2 as adapter


MONDAY = date(2026, 9, 21)


def chain(symbol="XOM"):
    return {f"{symbol}260925C00100000": {}, f"{symbol}261002C00100000": {},
            f"{symbol}261002P00100000": {}}


def test_observed_fridays_are_not_rounded_into_the_old_window():
    source = chain()
    original = deepcopy(source)
    report = coverage.summarize_expirations("XOM", source, as_of=MONDAY)
    assert report["status"] == "observed"
    assert report["contracts_counted"] == 3
    assert report["expirations"] == [
        {"expiration": "2026-09-25", "calendar_dte": 4, "calls": 1, "puts": 0},
        {"expiration": "2026-10-02", "calendar_dte": 11, "calls": 1, "puts": 1},
    ]
    assert not report["is_profitability_evidence"]
    assert source == original


@pytest.mark.parametrize("source", [None, [], {}])
def test_missing_chain_is_not_successful_empty_coverage(source):
    assert coverage.summarize_expirations("XOM", source, as_of=MONDAY)["status"] == "unavailable"


def test_invalid_date_symbol_root_and_conflicting_metadata_are_counted_not_invented():
    source = chain()
    source.update({"XOM260230C00100000": {}, "BAD": {}, "SPY261002C00100000": {},
                   "XOM261002P00101000": {"details": {"type": "call"}},
                   "XOM261002C00101000": {"details": {"expiration_date": "2026-10-03"}}})
    report = coverage.summarize_expirations("XOM", source, as_of=MONDAY)
    assert report["status"] == "partial"
    assert report["invalid_contracts"] == 5
    assert report["contracts_counted"] == 3
    assert report["contracts_counted"] + report["invalid_contracts"] == report["contracts_returned"]


def test_index_weekly_root_stays_under_the_index():
    report = coverage.summarize_expirations("VIX", {"VIXW261007C00020000": {}, "VIX261021P00018000": {}}, as_of=MONDAY)
    assert report["contracts_counted"] == 2
    assert report["invalid_contracts"] == 0


def test_size_limit_is_explicit(monkeypatch):
    monkeypatch.setattr(coverage, "_MAX_CONTRACTS", 1)
    report = coverage.summarize_expirations("XOM", chain(), as_of=MONDAY)
    assert report["status"] == "unavailable"
    assert report["reason"] == "coverage_size_limit"
    assert report["expirations"] == []


def test_proxy_uses_exact_same_request_and_returns_original_object():
    source = chain()
    calls = []
    def get(symbol, **kwargs):
        calls.append((symbol, kwargs))
        return source
    client = SimpleNamespace(get_option_chain_snapshots=get, stock_feed=object())
    records = []
    proxy = coverage.ExpiryCoverageClient(client, records)
    assert proxy.stock_feed is client.stock_feed
    assert proxy.get_option_chain_snapshots("XOM", limit=123) is source
    assert calls == [("XOM", {"limit": 123})]
    assert len(records) == 1
    assert "observed_at" in records[0]


def test_provider_error_propagates_unchanged_without_retry():
    calls = []
    error = ValueError("synthetic_provider_error")
    def get(symbol):
        calls.append(symbol)
        raise error
    records = []
    with pytest.raises(ValueError) as caught:
        coverage.ExpiryCoverageClient(SimpleNamespace(get_option_chain_snapshots=get), records).get_option_chain_snapshots("XOM")
    assert caught.value is error
    assert calls == ["XOM"]
    assert not records


def test_telemetry_failure_cannot_change_capture_or_print_private_error(monkeypatch):
    source = chain()
    def failed(*args, **kwargs):
        raise ValueError("SECRET-fixture")
    monkeypatch.setattr(coverage, "summarize_expirations", failed)
    records = []
    proxy = coverage.ExpiryCoverageClient(SimpleNamespace(get_option_chain_snapshots=lambda symbol: source), records)
    assert proxy.get_option_chain_snapshots("XOM") is source
    assert records == [{"symbol": "XOM", "status": "unavailable", "reason": "coverage_summary_failed"}]
    assert "SECRET" not in str(records)


def test_actual_v2_adapter_adds_only_coverage_to_the_completed_result(monkeypatch):
    calls = []
    raw = chain()
    result = adapter.TradingCycleResult(
        started_at=datetime(2026, 9, 21, 14, tzinfo=UTC), finished_at=datetime(2026, 9, 21, 14, tzinfo=UTC),
        symbols=["XOM"], snapshot_paths=[], decisions=[{"decision": "NO_TRADE"}], orders_submitted=[],
        skipped=[], runtime_state={}, execution_outcomes=[{"disposition": "existing"}],
    )
    def fake_shell(*, symbols, data_client):
        assert data_client.get_option_chain_snapshots(symbols[0]) is raw
        return result
    monkeypatch.setattr(adapter.legacy_cycle, "run_trading_cycle", fake_shell)
    def get(symbol):
        calls.append(symbol)
        return raw
    client = SimpleNamespace(get_option_chain_snapshots=get)
    first = adapter.run_trading_cycle(symbols=["XOM"], data_client=client)
    second = adapter.run_trading_cycle(symbols=["XOM"], data_client=client)
    assert calls == ["XOM", "XOM"]
    for observed in (first, second):
        assert observed.decisions is result.decisions
        assert observed.orders_submitted is result.orders_submitted
        assert observed.execution_outcomes[0] == result.execution_outcomes[0]
        assert observed.execution_outcomes[1]["extra_provider_requests"] == 0
        assert len(observed.execution_outcomes[1]["symbols"]) == 1
    assert len(result.execution_outcomes) == 1
    assert adapter.legacy_cycle.run_trading_cycle is fake_shell
