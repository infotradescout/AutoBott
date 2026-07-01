from __future__ import annotations

from datetime import UTC, datetime, timedelta

from autobott_v2.session_runner import run_trading_session
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
