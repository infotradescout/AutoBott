from __future__ import annotations

import threading
from dataclasses import replace

import pytest

import autobott_v2.session_supervisor as supervisor
from autobott_v2.runtime_control import default_runtime_state, load_runtime_state, save_runtime_state, set_kill_switch



@pytest.fixture(autouse=True)
def _join_test_workers_before_restoring_mocks(monkeypatch):
    # Keep each worker's dependencies mocked until that worker has finished.
    # The last test deliberately replaces global thread references, so capture
    # the thread/event pair at creation rather than trusting the final globals.
    from types import SimpleNamespace
    workers = []

    def tracked_thread(*args, **kwargs):
        worker = threading.Thread(*args, **kwargs)
        workers.append((worker, kwargs["args"][-1]))
        return worker

    monkeypatch.setattr(supervisor, "threading", SimpleNamespace(
        Thread=tracked_thread, Event=threading.Event,
    ))
    yield
    for _, stop_event in workers:
        stop_event.set()
    for worker, _ in workers:
        if worker.ident is not None:
            worker.join(timeout=2)
        assert not worker.is_alive(), "test supervisor worker escaped fixture teardown"


def _reset_supervisor_state() -> None:
    supervisor._SESSION_THREAD = None
    supervisor._POSITION_MONITOR_THREAD = None
    supervisor._SESSION_STOP_EVENT = None
    supervisor._POSITION_MONITOR_STOP_EVENT = None
    supervisor._SESSION_AUTOSTART_CONSUMED = False
    supervisor._SESSION_STATE.running = False
    supervisor._SESSION_STATE.started_at = None
    supervisor._SESSION_STATE.finished_at = None
    supervisor._SESSION_STATE.last_result = None
    supervisor._SESSION_STATE.last_error = None
    supervisor._SESSION_STATE.last_monitor_result = None
    supervisor._SESSION_STATE.last_monitor_error = None
    supervisor._SESSION_STATE.last_monitor_at = None
    supervisor._SESSION_STATE.last_evidence_result = None
    supervisor._SESSION_STATE.last_evidence_error = None
    supervisor._SESSION_STATE.last_evidence_at = None
    supervisor._SESSION_STATE.cycles_completed = 0
    supervisor._SESSION_STATE.last_cycle_at = None


def test_monitor_heartbeat_publishes_fault_exception_and_fresh_recovery(monkeypatch):
    from datetime import UTC, datetime, timedelta
    from types import SimpleNamespace
    _reset_supervisor_state()
    supervisor._SESSION_STATE.running = True
    observations = [
        {"ok": False, "enabled": True, "checked": 1, "actions": [
            {"symbol": "SYNTHETIC", "reason": "stop_loss", "exit_status": "rejected"}]},
        RuntimeError("private broker error and identity"),
        {"ok": True, "enabled": False, "checked": 0, "actions": []},
        None,
        {"ok": True, "enabled": True, "checked": 0, "actions": []},
    ]
    observed = []
    times = iter(datetime(2026, 10, 1, tzinfo=UTC) + timedelta(seconds=i) for i in range(5))
    monkeypatch.setattr(supervisor, "datetime", SimpleNamespace(now=lambda **kwargs: next(times)))

    def monitor():
        value = observations[len(observed)]
        if isinstance(value, Exception):
            raise value
        return value

    class Stop:
        def is_set(self):
            return len(observed) == len(observations)

        def wait(self, seconds):
            observed.append(supervisor._SESSION_STATE.to_json_dict())

    monkeypatch.setattr(supervisor, "run_position_monitor", monitor)
    supervisor._run_position_monitor_heartbeat(SimpleNamespace(position_monitor_heartbeat_seconds=15), Stop())
    assert observed[0]["last_monitor_result"]["exit_protection"]["status"] == "attention_required"
    assert observed[0]["last_monitor_error"]
    assert observed[1]["last_monitor_result"]["exit_protection"]["status"] == "unavailable"
    assert observed[1]["last_monitor_error"] and "private" not in str(observed[1])
    assert observed[2]["last_monitor_result"]["exit_protection"]["status"] == "disabled"
    assert observed[3]["last_monitor_error"]
    assert observed[4]["last_monitor_result"]["exit_protection"]["status"] == "monitoring"
    assert observed[4]["last_monitor_error"] is None
    assert all(row["running"] for row in observed)
    assert len({row["last_monitor_at"] for row in observed}) == 5


def test_load_session_supervisor_config_from_env(monkeypatch) -> None:
    _reset_supervisor_state()
    monkeypatch.setenv("AUTOBOTT_SESSION_AUTOSTART", "true")
    monkeypatch.setenv("AUTOBOTT_SESSION_SYMBOLS", "AAPL,MSFT")
    monkeypatch.setenv("AUTOBOTT_SESSION_INTERVAL_SECONDS", "120")
    monkeypatch.setenv("AUTOBOTT_SESSION_MAX_CYCLES", "4")
    monkeypatch.setenv("AUTOBOTT_SESSION_SYMBOL_BATCH_SIZE", "12")
    monkeypatch.setenv("AUTOBOTT_SESSION_START_TIME", "09:35")
    monkeypatch.setenv("AUTOBOTT_SESSION_END_TIME", "15:55")
    monkeypatch.setenv("AUTOBOTT_SESSION_MARKET_TIMEZONE", "America/New_York")
    monkeypatch.setenv("AUTOBOTT_SESSION_ARM_PAPER_EXECUTION", "true")
    monkeypatch.setenv("AUTOBOTT_POSITION_MONITOR_HEARTBEAT_ENABLED", "true")
    monkeypatch.setenv("AUTOBOTT_POSITION_MONITOR_HEARTBEAT_SECONDS", "15")
    config = supervisor.load_session_supervisor_config()
    assert config.enabled is True
    assert config.symbols == ["AAPL", "MSFT"]
    assert config.interval_seconds == 120
    assert config.max_cycles == 4
    assert config.symbol_batch_size == 12
    assert config.start_time == "09:35:00"
    assert config.end_time == "15:55:00"
    assert config.market_timezone == "America/New_York"
    assert config.arm_paper_execution_on_start is True
    assert config.position_monitor_heartbeat_enabled is True
    assert config.position_monitor_heartbeat_seconds == 15


def test_load_session_supervisor_config_run_forever_ignores_max_cycles(monkeypatch) -> None:
    _reset_supervisor_state()
    monkeypatch.setenv("AUTOBOTT_SESSION_MAX_CYCLES", "3")
    monkeypatch.setenv("AUTOBOTT_SESSION_RUN_FOREVER", "true")

    config = supervisor.load_session_supervisor_config()

    assert config.run_forever is True
    assert config.max_cycles is None


def test_hosted_autostart_off_never_starts_session_or_monitor(monkeypatch) -> None:
    monkeypatch.setenv("RENDER", "true")
    monkeypatch.setenv("AUTOBOTT_SESSION_AUTOSTART", "false")
    monkeypatch.setattr(
        supervisor,
        "_start_session_thread",
        lambda *_args, **_kwargs: pytest.fail("Disabled autostart must not create any worker"),
    )

    assert supervisor.maybe_start_session_supervisor() is False


def test_hosted_arm_off_does_not_rearm_runtime_when_session_runs(monkeypatch) -> None:
    monkeypatch.setenv("RENDER", "true")
    monkeypatch.setenv("AUTOBOTT_SESSION_ARM_PAPER_EXECUTION", "false")
    monkeypatch.setattr(supervisor, "_SESSION_STATE", supervisor.SessionSupervisorState())
    monkeypatch.setattr(
        "autobott_v2.runtime_control.arm_paper_execution",
        lambda **_kwargs: pytest.fail("Disabled startup arming must preserve the runtime lock"),
    )
    calls = []

    def fake_run_trading_session(**kwargs):
        calls.append(kwargs)

        class Result:
            def to_json_dict(self):
                return {"cycles_completed": 1}

        return Result()

    monkeypatch.setattr(supervisor, "run_trading_session", fake_run_trading_session)
    stop_event = threading.Event()
    supervisor._run_session(supervisor.load_session_supervisor_config(), stop_event)

    assert len(calls) == 1
    assert stop_event.is_set()
    assert supervisor._SESSION_STATE.last_error is None


def test_load_session_supervisor_config_expands_top_options_universe(monkeypatch) -> None:
    _reset_supervisor_state()
    monkeypatch.setenv("AUTOBOTT_SESSION_SYMBOLS", "TOP_OPTIONS_100")

    config = supervisor.load_session_supervisor_config()

    assert len(config.symbols) == 100
    assert config.symbols[:5] == ["SPY", "QQQ", "IWM", "DIA", "TLT"]


def test_maybe_start_session_supervisor_starts_once(monkeypatch, tmp_path) -> None:
    _reset_supervisor_state()
    _persist_authorized_test_state(monkeypatch, tmp_path)
    monkeypatch.setenv("AUTOBOTT_SESSION_AUTOSTART", "true")
    monkeypatch.setenv("AUTOBOTT_SESSION_MAX_CYCLES", "1")
    calls = []

    def fake_run_trading_session(**kwargs):
        calls.append(kwargs)
        class Result:
            def to_json_dict(self):
                return {"cycles_completed": 1}
        return Result()

    monkeypatch.setattr(supervisor, "run_trading_session", fake_run_trading_session)
    started = supervisor.maybe_start_session_supervisor()
    import time
    for _ in range(20):
        status = supervisor.session_supervisor_status()
        if status["state"]["last_result"] is not None:
            break
        time.sleep(0.01)
    assert started is True
    assert calls
    assert calls[0]["continuous_window"] is True
    assert calls[0]["on_cycle_complete"] is supervisor._record_cycle_result
    assert calls[0]["after_entry_window_runner"] is supervisor._poll_and_record_primary_evidence
    second = supervisor.maybe_start_session_supervisor()
    assert second is False


def test_supervisor_records_read_only_primary_evidence_summary(monkeypatch) -> None:
    _reset_supervisor_state()
    expected = {
        "trading_actions": 0,
        "fills": {"checked": 1, "filled": 1},
        "observations": {"checked": 1, "observed": 1},
        "development_metrics": {"materialized": 0},
    }
    monkeypatch.setattr(supervisor, "poll_primary_runtime_evidence_once", lambda: expected)
    result = supervisor._poll_and_record_primary_evidence()
    assert result == expected
    status = supervisor.session_supervisor_status()["state"]
    assert status["last_evidence_result"] == expected
    assert status["last_evidence_error"] is None
    assert status["last_evidence_at"] is not None


def test_supervisor_evidence_exception_is_status_only(monkeypatch) -> None:
    _reset_supervisor_state()

    def broken():
        raise RuntimeError("synthetic evidence failure")

    monkeypatch.setattr(supervisor, "poll_primary_runtime_evidence_once", broken)
    result = supervisor._poll_and_record_primary_evidence()
    assert result["trading_actions"] == 0
    status = supervisor.session_supervisor_status()["state"]
    assert "RuntimeError: synthetic evidence failure" == status["last_evidence_error"]
    assert status["last_evidence_result"] is None
    assert status["last_evidence_at"] is not None


def test_supervisor_publishes_each_cycle_before_session_finishes() -> None:
    _reset_supervisor_state()

    supervisor._record_cycle_result(
        {
            "scanner_candidates_count": 3,
            "trade_attempted_count": 2,
            "orders_submitted": [{"broker_order_id": "paper-1"}],
            "skipped": [{"symbol": "QQQ", "reason": "core_runner_pair_not_found"}],
        }
    )

    status = supervisor.session_supervisor_status()["state"]
    assert status["cycles_completed"] == 1
    assert status["last_cycle_at"] is not None
    assert status["last_result"]["cycles_completed"] == 1
    assert status["last_result"]["cycle_results"][0]["trade_attempted_count"] == 2


def test_maybe_start_session_supervisor_preserves_existing_paper_arm(monkeypatch, tmp_path) -> None:
    _reset_supervisor_state()
    _persist_authorized_test_state(monkeypatch, tmp_path)
    monkeypatch.setenv("AUTOBOTT_SESSION_AUTOSTART", "true")
    monkeypatch.setenv("AUTOBOTT_SESSION_MAX_CYCLES", "1")
    monkeypatch.setenv("AUTOBOTT_SESSION_ARM_PAPER_EXECUTION", "true")
    calls = []

    def fake_run_trading_session(**kwargs):
        calls.append(kwargs)

        class Result:
            def to_json_dict(self):
                return {"cycles_completed": 1}

        return Result()

    monkeypatch.setattr(supervisor, "run_trading_session", fake_run_trading_session)
    started = supervisor.maybe_start_session_supervisor()
    import time
    for _ in range(20):
        status = supervisor.session_supervisor_status()
        if status["state"]["last_result"] is not None:
            break
        time.sleep(0.01)
    assert started is True
    assert calls
    assert load_runtime_state().reason == "synthetic_operator_arm"
    assert calls[0]["continuous_window"] is True


def test_start_session_supervisor_can_start_manual_session(monkeypatch) -> None:
    _reset_supervisor_state()
    calls = []

    def fake_run_trading_session(**kwargs):
        calls.append(kwargs)
        class Result:
            def to_json_dict(self):
                return {"cycles_completed": 1}
        return Result()

    monkeypatch.setattr(supervisor, "run_trading_session", fake_run_trading_session)
    started = supervisor.start_session_supervisor(
        supervisor.SessionSupervisorConfig(
            enabled=True,
            symbols=["SPY"],
            interval_seconds=300,
            max_cycles=1,
            symbol_batch_size=10,
            quantity=1,
            position_count=0,
            daily_pnl=0.0,
            start_time=None,
            end_time=None,
            market_timezone="America/New_York",
            arm_paper_execution_on_start=False,
        )
    )
    import time
    for _ in range(20):
        status = supervisor.session_supervisor_status()
        if status["state"]["last_result"] is not None:
            break
        time.sleep(0.01)
    assert started is True
    assert calls
    assert calls[0]["continuous_window"] is True
    assert calls[0]["symbol_batch_size"] == 10


def test_position_monitor_heartbeat_survives_finished_session(monkeypatch) -> None:
    _reset_supervisor_state()
    calls = []
    monitor_calls = []

    def fake_run_trading_session(**kwargs):
        calls.append(kwargs)

        class Result:
            def to_json_dict(self):
                return {"cycles_completed": 1}

        return Result()

    def fake_run_position_monitor():
        monitor_calls.append("tick")
        return {"ok": True, "actions": []}

    monkeypatch.setattr(supervisor, "run_trading_session", fake_run_trading_session)
    monkeypatch.setattr(supervisor, "run_position_monitor", fake_run_position_monitor)
    started = supervisor.start_session_supervisor(
        supervisor.SessionSupervisorConfig(
            enabled=True,
            symbols=["SPY"],
            interval_seconds=300,
            max_cycles=1,
            symbol_batch_size=10,
            quantity=1,
            position_count=0,
            daily_pnl=0.0,
            start_time=None,
            end_time=None,
            market_timezone="America/New_York",
            arm_paper_execution_on_start=False,
            position_monitor_heartbeat_enabled=True,
            position_monitor_heartbeat_seconds=5,
        )
    )
    import time

    for _ in range(20):
        status = supervisor.session_supervisor_status()
        if status["state"]["finished_at"] is not None and monitor_calls:
            break
        time.sleep(0.01)

    assert started is True
    status = supervisor.session_supervisor_status()
    assert status["thread_alive"] is False
    assert status["position_monitor_thread_alive"] is True
    assert monitor_calls


def test_consumed_autostart_still_ensures_position_monitor(monkeypatch, tmp_path) -> None:
    _reset_supervisor_state()
    _persist_authorized_test_state(monkeypatch, tmp_path)
    monkeypatch.setenv("AUTOBOTT_SESSION_AUTOSTART", "true")
    monkeypatch.setenv("AUTOBOTT_SESSION_MAX_CYCLES", "1")
    monkeypatch.setenv("AUTOBOTT_POSITION_MONITOR_HEARTBEAT_ENABLED", "true")
    monitor_calls = []

    def fake_run_trading_session(**_kwargs):
        class Result:
            def to_json_dict(self):
                return {"cycles_completed": 1}

        return Result()

    def fake_run_position_monitor():
        monitor_calls.append("tick")
        return {"ok": True, "actions": []}

    monkeypatch.setattr(supervisor, "run_trading_session", fake_run_trading_session)
    monkeypatch.setattr(supervisor, "run_position_monitor", fake_run_position_monitor)
    assert supervisor.maybe_start_session_supervisor() is True
    import time

    for _ in range(20):
        if supervisor._SESSION_AUTOSTART_CONSUMED:
            break
        time.sleep(0.01)
    old_thread = supervisor._POSITION_MONITOR_THREAD
    supervisor._POSITION_MONITOR_THREAD = None

    assert supervisor.maybe_start_session_supervisor() is False
    assert supervisor._POSITION_MONITOR_THREAD is not None
    assert supervisor._POSITION_MONITOR_THREAD is not old_thread


def _persist_authorized_test_state(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("AUTOBOTT_DATA_ROOT", str(tmp_path))
    save_runtime_state(replace(default_runtime_state(), execution_enabled=True,
                               reason="synthetic_operator_arm"))


def test_supervisor_startup_defaults_disabled(monkeypatch) -> None:
    monkeypatch.delenv("AUTOBOTT_SESSION_AUTOSTART", raising=False)
    monkeypatch.delenv("AUTOBOTT_SESSION_ARM_PAPER_EXECUTION", raising=False)
    config = supervisor.load_session_supervisor_config()
    assert config.enabled is False
    assert config.arm_paper_execution_on_start is False


@pytest.mark.parametrize("state_kind", ["missing", "paused", "kill_switch"])
def test_autostart_requires_persisted_enabled_state(monkeypatch, tmp_path, state_kind) -> None:
    _reset_supervisor_state()
    monkeypatch.setenv("AUTOBOTT_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("AUTOBOTT_SESSION_AUTOSTART", "true")
    monkeypatch.setenv("AUTOBOTT_SESSION_ARM_PAPER_EXECUTION", "true")
    monkeypatch.setenv("AUTOBOTT_POSITION_MONITOR_HEARTBEAT_ENABLED", "true")
    if state_kind == "paused":
        save_runtime_state(default_runtime_state())
    elif state_kind == "kill_switch":
        set_kill_switch(True, reason="synthetic_operator_stop")
    before = load_runtime_state().to_json_dict() if state_kind != "missing" else None
    monkeypatch.setattr(supervisor, "_start_session_thread",
                        lambda *args, **kwargs: pytest.fail("paused startup created worker"))
    assert supervisor.maybe_start_session_supervisor() is False
    assert supervisor._SESSION_THREAD is None
    assert supervisor._POSITION_MONITOR_THREAD is None
    assert supervisor._SESSION_AUTOSTART_CONSUMED is False
    if before is not None:
        assert load_runtime_state().to_json_dict() == before


@pytest.mark.parametrize("killed", [False, True])
def test_manual_session_retained_arm_flag_preserves_safety_state(monkeypatch, tmp_path, killed) -> None:
    _reset_supervisor_state()
    monkeypatch.setenv("AUTOBOTT_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("AUTOBOTT_SESSION_ARM_PAPER_EXECUTION", "true")
    stopped = set_kill_switch(killed, reason="synthetic_operator_stop")
    class Result:
        def to_json_dict(self):
            return {"cycles_completed": 0}
    monkeypatch.setattr(supervisor, "run_trading_session", lambda **kwargs: Result())
    stop_event = threading.Event()
    supervisor._run_session(supervisor.load_session_supervisor_config(), stop_event)
    assert stop_event.is_set()
    assert load_runtime_state().to_json_dict() == stopped.to_json_dict()


@pytest.mark.parametrize("field,value", [("execution_enabled", "false"), ("kill_switch_enabled", "false"),
                                        ("kill_switch_enabled", None), ("live_mode_enabled", None)])
def test_retained_autostart_does_not_start_with_invalid_safety_fields(monkeypatch, tmp_path, field, value):
    import json
    from autobott_v2.runtime_control import runtime_state_path
    _reset_supervisor_state()
    monkeypatch.setenv("AUTOBOTT_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("AUTOBOTT_SESSION_AUTOSTART", "true")
    monkeypatch.setenv("AUTOBOTT_SESSION_ARM_PAPER_EXECUTION", "true")
    payload = default_runtime_state().to_json_dict()
    payload["execution_enabled"] = True
    if value is None:
        del payload[field]
    else:
        payload[field] = value
    path = runtime_state_path()
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setattr(supervisor, "_start_session_thread",
                        lambda *args, **kwargs: pytest.fail("invalid safety state created worker"))
    assert supervisor.maybe_start_session_supervisor() is False
    assert supervisor._POSITION_MONITOR_THREAD is None
