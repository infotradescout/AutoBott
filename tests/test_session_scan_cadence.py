"""Synthetic session timing; no broker, network, order, or control access."""
from datetime import UTC, datetime, time, timedelta
from types import SimpleNamespace

import pytest

from autobott_v2 import session_runner as session


class Clock:
    def __init__(self):
        self.wall = datetime(2026, 9, 21, 14, 0, tzinfo=UTC)
        self.tick = 0.0
        self.sleeps = []

    def now(self):
        return self.wall

    def monotonic(self):
        return self.tick

    def advance(self, seconds):
        self.tick += seconds
        self.wall += timedelta(seconds=seconds)

    def sleep(self, seconds):
        assert seconds >= 0
        self.sleeps.append(seconds)
        self.advance(seconds)


class Result:
    def __init__(self, started, finished, symbols):
        self.payload = {"started_at": started.isoformat(), "finished_at": finished.isoformat(),
                        "symbols": symbols, "orders_submitted": [], "decisions": []}

    def to_json_dict(self):
        return self.payload.copy()


def setup(monkeypatch, *, hosted=True):
    clock = Clock()
    monkeypatch.setattr(session, "time", SimpleNamespace(monotonic=clock.monotonic))
    monkeypatch.setattr(session, "is_hosted_paper_runtime", lambda: hosted)
    return clock


def run(clock, runner, **kwargs):
    return session.run_trading_session(
        symbols=kwargs.pop("symbols", ["SPY"]), interval_seconds=90,
        max_cycles=kwargs.pop("max_cycles", 3),
        now_fn=clock.now, sleep_fn=clock.sleep, cycle_runner=runner, **kwargs,
    )


@pytest.mark.parametrize("duration", [0, 30, 89, 90, 135, 270])
def test_hosted_scan_starts_do_not_add_another_interval_after_work(monkeypatch, duration):
    clock = setup(monkeypatch)
    starts = []
    busy = False
    def cycle(*, symbols, **kwargs):
        nonlocal busy
        assert not busy
        busy = True
        starts.append(clock.tick)
        before = clock.now()
        clock.advance(duration)
        busy = False
        return Result(before, clock.now(), symbols)
    result = run(clock, cycle)
    gap = max(90, duration)
    assert starts == [0, gap, gap * 2]
    assert result.cycles_completed == 3
    assert all(not row["orders_submitted"] for row in result.cycle_results)
    assert clock.sleeps == [max(0, 90 - duration)] * 2


def test_failed_scan_keeps_rotation_and_cadence_without_retry_bursts(monkeypatch):
    clock = setup(monkeypatch)
    calls = []
    published = []
    def cycle(*, symbols, **kwargs):
        calls.append((clock.tick, list(symbols)))
        before = clock.now()
        clock.advance(30)
        if len(calls) == 1:
            raise ValueError("synthetic_data_unavailable")
        return Result(before, clock.now(), symbols)
    result = run(clock, cycle, symbols=["AAA", "BBB", "CCC"],
                 symbol_batch_size=1, on_cycle_complete=published.append)
    assert calls == [(0, ["AAA"]), (90, ["BBB"]), (180, ["CCC"])]
    assert result.cycles_completed == 3
    assert "synthetic_data_unavailable" in published[0]["error"]
    assert len(published) == 3


def test_cycle_publication_time_is_included_in_interval(monkeypatch):
    clock = setup(monkeypatch)
    starts = []
    published = []
    def cycle(*, symbols, **kwargs):
        starts.append(clock.tick)
        before = clock.now()
        clock.advance(30)
        return Result(before, clock.now(), symbols)
    def publish(payload):
        published.append(payload)
        clock.advance(7)
    run(clock, cycle, on_cycle_complete=publish)
    assert starts == [0, 90, 180]
    assert clock.sleeps == [53, 53]
    assert len(published) == 3


def test_103_symbol_rotation_finishes_five_batches_without_sleep_drift(monkeypatch):
    clock = setup(monkeypatch)
    symbols = list(session.HOSTED_PRIORITY_SYMBOLS) + [f"TEST{i:03}" for i in range(98)]
    calls = []
    def cycle(*, symbols, **kwargs):
        calls.append((clock.tick, list(symbols)))
        before = clock.now()
        clock.advance(30)
        return Result(before, clock.now(), symbols)
    run(clock, cycle, symbols=symbols, max_cycles=5, symbol_batch_size=25)
    assert [at for at, _ in calls] == [0, 90, 180, 270, 360]
    assert set().union(*(set(batch) for _, batch in calls)) == set(symbols)
    assert all(len(batch) == len(set(batch)) == 25 for _, batch in calls)
    assert all(batch[:5] == list(session.HOSTED_PRIORITY_SYMBOLS) for _, batch in calls)
    assert clock.tick == 390


@pytest.mark.parametrize("jump", [-3600, 3600])
def test_elapsed_work_uses_monotonic_not_wall_clock(monkeypatch, jump):
    clock = setup(monkeypatch)
    starts = []
    def cycle(*, symbols, **kwargs):
        starts.append(clock.tick)
        before = clock.now()
        clock.advance(30)
        clock.wall += timedelta(seconds=jump)
        return Result(before, clock.now(), symbols)
    run(clock, cycle)
    assert starts == [0, 90, 180]
    assert clock.sleeps == [60, 60]


def test_market_window_is_rechecked_before_next_scan(monkeypatch):
    clock = setup(monkeypatch)
    clock.wall = datetime(2026, 9, 21, 19, 54, 30, tzinfo=UTC)
    calls = []
    def cycle(*, symbols, **kwargs):
        calls.append(clock.now())
        before = clock.now()
        clock.advance(45)
        return Result(before, clock.now(), symbols)
    result = run(clock, cycle, start_time=time(9, 35), end_time=time(15, 55),
                 market_timezone="America/New_York")
    assert result.cycles_completed == 1
    assert len(calls) == 1
    assert clock.sleeps == [45]


def test_local_delay_after_work_behavior_is_unchanged(monkeypatch):
    clock = setup(monkeypatch, hosted=False)
    starts = []
    def cycle(*, symbols, **kwargs):
        starts.append(clock.tick)
        before = clock.now()
        clock.advance(30)
        return Result(before, clock.now(), symbols)
    run(clock, cycle)
    assert starts == [0, 120, 240]
    assert clock.sleeps == [90, 90]
