from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from autobott_v2 import trading_cycle as shell
from autobott_v2 import trading_cycle_v2 as adapter
from test_trading_cycle import FakeBroker


@pytest.mark.parametrize("ranked", [False, True])
@pytest.mark.parametrize("explicit", [False, True])
def test_autonomous_capture_does_not_reuse_a_slow_prior_symbols_cutoff(monkeypatch, tmp_path, ranked, explicit):
    start = datetime(2026, 9, 21, 15, 0, tzinfo=UTC)
    clock = [start]
    calls = []
    class ClockDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return clock[0].astimezone(tz) if tz else clock[0].replace(tzinfo=None)
    monkeypatch.setattr(shell, "datetime", ClockDateTime)
    monkeypatch.setattr(adapter, "datetime", ClockDateTime)
    from autobott_v2 import ranked_entry_scan
    monkeypatch.setattr(ranked_entry_scan, "datetime", ClockDateTime)
    monkeypatch.setattr(adapter, "portfolio_mode_enabled", lambda: ranked)
    monkeypatch.setenv("AUTOBOTT_DATA_ROOT", str(tmp_path / "state"))
    monkeypatch.setenv("AUTOBOTT_ARTIFACTS_ROOT", str(tmp_path / "artifacts"))
    monkeypatch.setenv("AUTOBOTT_GATE_PATH", str(tmp_path / "gate.json"))
    def capture(**kwargs):
        calls.append((clock[0], kwargs))
        clock[0] += timedelta(seconds=93)
        raise ValueError("synthetic_provider_unavailable")
    monkeypatch.setattr(shell, "capture_symbol_snapshot", capture)
    kwargs = {"scheduled_market_time": start, "captured_at_utc": start} if explicit else {}
    result = adapter.run_trading_cycle(symbols=["FIRST", "SECOND"], broker=FakeBroker(),
        data_client=SimpleNamespace(), corpus_root=tmp_path / "corpus",
        execution_log_path=str(tmp_path / "execution.jsonl"), **kwargs)
    assert len(calls) >= 2
    assert not result.orders_submitted
    for actual_start, supplied in calls:
        if explicit:
            # A ranked retry deliberately captures anew for an entry, rather
            # than changing the original recorded pre-ranking source cutoff.
            if actual_start < start + timedelta(seconds=186) or not ranked:
                assert supplied["scheduled_market_time"] == start
        else:
            assert supplied["scheduled_market_time"] == actual_start
            assert supplied["captured_at_utc"] == actual_start
    assert calls[1][0] - calls[0][0] == timedelta(seconds=93)
