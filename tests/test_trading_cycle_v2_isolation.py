from __future__ import annotations

from threading import Event, Thread

import pytest

import autobott_v2.trading_cycle as legacy
import autobott_v2.trading_cycle_v2 as adapter


def test_cycle_uses_private_bindings_and_preserves_keyword_defaults(monkeypatch):
    builder, monitor = legacy.build_decision_card, legacy.run_position_monitor
    shared_state = object()

    def shell(*, symbols, quantity=7, marker=None):
        assert legacy.build_decision_card is builder
        assert legacy.run_position_monitor is monitor
        assert globals()["build_decision_card"] is adapter.build_decision_card_v2
        assert globals()["run_position_monitor"] is adapter.run_position_monitor_v2
        return symbols, quantity, marker, shared_state

    monkeypatch.setattr(legacy, "run_trading_cycle", shell)
    marker = object()
    assert adapter.run_trading_cycle(symbols=["SYNTHETIC"], marker=marker) == (
        ["SYNTHETIC"], 7, marker, shared_state
    )
    assert legacy.build_decision_card is builder
    assert legacy.run_position_monitor is monitor


def test_overlapping_cycles_leave_shared_module_unchanged(monkeypatch):
    builder, monitor = legacy.build_decision_card, legacy.run_position_monitor
    first_started, second_started, release = Event(), Event(), Event()
    results, failures = [], []

    def shell(*, symbols):
        assert legacy.build_decision_card is builder
        assert legacy.run_position_monitor is monitor
        (first_started if symbols == ["FIRST"] else second_started).set()
        assert release.wait(5), "concurrent_cycle_was_not_released"
        return globals()["build_decision_card"], globals()["run_position_monitor"]

    def run(symbol):
        try:
            results.append(adapter.run_trading_cycle(symbols=[symbol]))
        except BaseException as exc:
            failures.append(exc)

    monkeypatch.setattr(legacy, "run_trading_cycle", shell)
    first, second = Thread(target=run, args=("FIRST",)), Thread(target=run, args=("SECOND",))
    first.start()
    try:
        assert first_started.wait(5), failures
        second.start()
        assert second_started.wait(5), failures
        assert legacy.build_decision_card is builder
        assert legacy.run_position_monitor is monitor
    finally:
        release.set()
        first.join(5)
        if second.ident is not None:
            second.join(5)
    assert not first.is_alive() and not second.is_alive()
    assert failures == []
    assert results == [(adapter.build_decision_card_v2, adapter.run_position_monitor_v2)] * 2
    assert legacy.build_decision_card is builder
    assert legacy.run_position_monitor is monitor


def test_nested_cycle_has_independent_bindings(monkeypatch):
    builder, monitor = legacy.build_decision_card, legacy.run_position_monitor
    seen = []

    def shell(*, symbols, depth=0):
        seen.append(globals()["build_decision_card"])
        assert legacy.build_decision_card is builder
        assert legacy.run_position_monitor is monitor
        if depth == 0:
            adapter.run_trading_cycle(symbols=symbols, depth=1)
        assert globals()["build_decision_card"] is adapter.build_decision_card_v2
        return depth

    monkeypatch.setattr(legacy, "run_trading_cycle", shell)
    assert adapter.run_trading_cycle(symbols=["SYNTHETIC"]) == 0
    assert seen == [adapter.build_decision_card_v2, adapter.build_decision_card_v2]


def test_exception_does_not_require_restoring_shared_globals(monkeypatch):
    builder, monitor = legacy.build_decision_card, legacy.run_position_monitor

    def shell(*, symbols):
        assert legacy.build_decision_card is builder
        assert legacy.run_position_monitor is monitor
        raise RuntimeError("synthetic_cycle_failure")

    monkeypatch.setattr(legacy, "run_trading_cycle", shell)
    with pytest.raises(RuntimeError, match="synthetic_cycle_failure"):
        adapter.run_trading_cycle(symbols=["SYNTHETIC"])
    assert legacy.build_decision_card is builder
    assert legacy.run_position_monitor is monitor


def test_unsupported_shell_fails_before_execution(monkeypatch):
    called = []

    class UnexpectedShell:
        def __call__(self, **kwargs):
            called.append(kwargs)

    monkeypatch.setattr(legacy, "run_trading_cycle", UnexpectedShell())
    with pytest.raises(TypeError, match="cycle_shell_must_be_python_function"):
        adapter.run_trading_cycle(symbols=["SYNTHETIC"])
    assert called == []
