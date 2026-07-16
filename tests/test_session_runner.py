from __future__ import annotations

from datetime import UTC, datetime, timedelta

from autobott_v2.session_runner import _cycle_symbols, run_trading_session
from autobott_v2.trading_cycle import TradingCycleResult


class FakeClock:
    def __init__(self) -> None:
        self.current = datetime(2026, 7, 1, 15, 30, tzinfo=UTC)

    def now(self) -> datetime:
        return self.current

    def sleep(self, seconds: float) -> None:
        self.current += timedelta(seconds=seconds)


def test_run_trading_session_runs_multiple_cycles() -> None:
    clock = FakeClock()
    calls = []

    def fake_cycle_runner(*, symbols, **kwargs):
        calls.append(list(symbols))
        return TradingCycleResult(
            started_at=clock.now(),
            finished_at=clock.now(),
            symbols=list(symbols),
            snapshot_paths=[],
            decisions=[],
            orders_submitted=[],
            skipped=[],
            runtime_state={},
        )

    result = run_trading_session(
        symbols=["AAPL", "MSFT"],
        interval_seconds=60,
        max_cycles=3,
        now_fn=clock.now,
        sleep_fn=clock.sleep,
        cycle_runner=fake_cycle_runner,
    )

    assert result.cycles_completed == 3
    assert len(result.cycle_results) == 3
    assert calls == [["AAPL", "MSFT"], ["AAPL", "MSFT"], ["AAPL", "MSFT"]]


def test_run_trading_session_rotates_symbol_batches() -> None:
    clock = FakeClock()
    calls = []

    def fake_cycle_runner(*, symbols, **kwargs):
        calls.append(list(symbols))
        return TradingCycleResult(
            started_at=clock.now(),
            finished_at=clock.now(),
            symbols=list(symbols),
            snapshot_paths=[],
            decisions=[],
            orders_submitted=[],
            skipped=[],
            runtime_state={},
        )

    run_trading_session(
        symbols=["AAPL", "MSFT", "NVDA", "TSLA", "AMD"],
        interval_seconds=60,
        max_cycles=4,
        symbol_batch_size=2,
        now_fn=clock.now,
        sleep_fn=clock.sleep,
        cycle_runner=fake_cycle_runner,
    )

    assert calls == [["AAPL", "MSFT"], ["NVDA", "TSLA"], ["AMD", "AAPL"], ["MSFT", "NVDA"]]


def test_priority_symbols_are_scanned_in_every_rotating_batch(monkeypatch) -> None:
    monkeypatch.setenv("AUTOBOTT_SESSION_PRIORITY_SYMBOLS", "VXX,UVXY")
    symbols = ["AAPL", "MSFT", "NVDA", "TSLA", "AMD", "VXX", "UVXY"]

    assert _cycle_symbols(symbols, cycle_index=0, batch_size=4) == ["VXX", "UVXY", "AAPL", "MSFT"]
    assert _cycle_symbols(symbols, cycle_index=1, batch_size=4) == ["VXX", "UVXY", "NVDA", "TSLA"]


def test_run_trading_session_respects_end_time() -> None:
    clock = FakeClock()

    def fake_cycle_runner(*, symbols, **kwargs):
        return TradingCycleResult(
            started_at=clock.now(),
            finished_at=clock.now(),
            symbols=list(symbols),
            snapshot_paths=[],
            decisions=[],
            orders_submitted=[],
            skipped=[],
            runtime_state={},
        )

    result = run_trading_session(
        symbols=["AAPL"],
        interval_seconds=60,
        start_time=datetime(2026, 7, 1, 15, 30, tzinfo=UTC).time(),
        end_time=datetime(2026, 7, 1, 15, 31, tzinfo=UTC).time(),
        now_fn=clock.now,
        sleep_fn=clock.sleep,
        cycle_runner=fake_cycle_runner,
    )

    assert result.cycles_completed == 2


def test_run_trading_session_respects_market_timezone_window() -> None:
    clock = FakeClock()
    clock.current = datetime(2026, 7, 1, 13, 29, tzinfo=UTC)
    calls = []

    def fake_cycle_runner(*, symbols, **kwargs):
        calls.append(clock.now())
        return TradingCycleResult(
            started_at=clock.now(),
            finished_at=clock.now(),
            symbols=list(symbols),
            snapshot_paths=[],
            decisions=[],
            orders_submitted=[],
            skipped=[],
            runtime_state={},
        )

    result = run_trading_session(
        symbols=["SPY"],
        interval_seconds=60,
        start_time=datetime(2026, 7, 1, 9, 30).time(),
        end_time=datetime(2026, 7, 1, 9, 31).time(),
        market_timezone="America/New_York",
        now_fn=clock.now,
        sleep_fn=clock.sleep,
        cycle_runner=fake_cycle_runner,
    )

    assert result.cycles_completed == 2
    assert [call.isoformat() for call in calls] == [
        "2026-07-01T13:30:00+00:00",
        "2026-07-01T13:31:00+00:00",
    ]


def test_run_trading_session_skips_non_trading_days() -> None:
    clock = FakeClock()
    clock.current = datetime(2026, 7, 5, 13, 30, tzinfo=UTC)
    calls = []

    def fake_cycle_runner(*, symbols, **kwargs):
        calls.append(list(symbols))
        return TradingCycleResult(
            started_at=clock.now(),
            finished_at=clock.now(),
            symbols=list(symbols),
            snapshot_paths=[],
            decisions=[],
            orders_submitted=[],
            skipped=[],
            runtime_state={},
        )

    result = run_trading_session(
        symbols=["SPY"],
        interval_seconds=60,
        start_time=datetime(2026, 7, 5, 9, 30).time(),
        end_time=datetime(2026, 7, 5, 9, 31).time(),
        market_timezone="America/New_York",
        now_fn=clock.now,
        sleep_fn=clock.sleep,
        cycle_runner=fake_cycle_runner,
    )

    assert result.cycles_completed == 0
    assert calls == []


def test_run_trading_session_continuous_window_waits_until_next_trading_day() -> None:
    clock = FakeClock()
    clock.current = datetime(2026, 7, 5, 18, 0, tzinfo=UTC)
    calls = []

    def fake_cycle_runner(*, symbols, **kwargs):
        calls.append(clock.now())
        return TradingCycleResult(
            started_at=clock.now(),
            finished_at=clock.now(),
            symbols=list(symbols),
            snapshot_paths=[],
            decisions=[],
            orders_submitted=[],
            skipped=[],
            runtime_state={},
        )

    result = run_trading_session(
        symbols=["SPY"],
        interval_seconds=1800,
        start_time=datetime(2026, 7, 6, 9, 30).time(),
        end_time=datetime(2026, 7, 6, 15, 31).time(),
        market_timezone="America/New_York",
        max_cycles=1,
        continuous_window=True,
        now_fn=clock.now,
        sleep_fn=clock.sleep,
        cycle_runner=fake_cycle_runner,
    )

    assert result.cycles_completed == 1
    assert [call.isoformat() for call in calls] == ["2026-07-06T13:30:00+00:00"]


def test_run_trading_session_continuous_window_waits_after_end_time() -> None:
    clock = FakeClock()
    clock.current = datetime(2026, 7, 1, 21, 0, tzinfo=UTC)
    calls = []

    def fake_cycle_runner(*, symbols, **kwargs):
        calls.append(clock.now())
        return TradingCycleResult(
            started_at=clock.now(),
            finished_at=clock.now(),
            symbols=list(symbols),
            snapshot_paths=[],
            decisions=[],
            orders_submitted=[],
            skipped=[],
            runtime_state={},
        )

    result = run_trading_session(
        symbols=["SPY"],
        interval_seconds=1800,
        start_time=datetime(2026, 7, 2, 9, 30).time(),
        end_time=datetime(2026, 7, 2, 15, 31).time(),
        market_timezone="America/New_York",
        max_cycles=1,
        continuous_window=True,
        now_fn=clock.now,
        sleep_fn=clock.sleep,
        cycle_runner=fake_cycle_runner,
    )

    assert result.cycles_completed == 1
    assert [call.isoformat() for call in calls] == ["2026-07-02T13:30:00+00:00"]
