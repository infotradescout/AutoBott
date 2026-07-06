from __future__ import annotations

import autobott_v2.session_supervisor as supervisor


def _reset_supervisor_state() -> None:
    supervisor._SESSION_THREAD = None
    supervisor._SESSION_AUTOSTART_CONSUMED = False
    supervisor._SESSION_STATE.running = False
    supervisor._SESSION_STATE.started_at = None
    supervisor._SESSION_STATE.finished_at = None
    supervisor._SESSION_STATE.last_result = None
    supervisor._SESSION_STATE.last_error = None


def test_load_session_supervisor_config_from_env(monkeypatch) -> None:
    _reset_supervisor_state()
    monkeypatch.setenv("AUTOBOTT_SESSION_AUTOSTART", "true")
    monkeypatch.setenv("AUTOBOTT_SESSION_SYMBOLS", "AAPL,MSFT")
    monkeypatch.setenv("AUTOBOTT_SESSION_INTERVAL_SECONDS", "120")
    monkeypatch.setenv("AUTOBOTT_SESSION_MAX_CYCLES", "4")
    monkeypatch.setenv("AUTOBOTT_SESSION_START_TIME", "09:35")
    monkeypatch.setenv("AUTOBOTT_SESSION_END_TIME", "15:55")
    monkeypatch.setenv("AUTOBOTT_SESSION_MARKET_TIMEZONE", "America/New_York")
    monkeypatch.setenv("AUTOBOTT_SESSION_ARM_PAPER_EXECUTION", "true")
    config = supervisor.load_session_supervisor_config()
    assert config.enabled is True
    assert config.symbols == ["AAPL", "MSFT"]
    assert config.interval_seconds == 120
    assert config.max_cycles == 4
    assert config.start_time == "09:35:00"
    assert config.end_time == "15:55:00"
    assert config.market_timezone == "America/New_York"
    assert config.arm_paper_execution_on_start is True


def test_maybe_start_session_supervisor_starts_once(monkeypatch) -> None:
    _reset_supervisor_state()
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
    second = supervisor.maybe_start_session_supervisor()
    assert second is False


def test_maybe_start_session_supervisor_can_arm_paper_execution(monkeypatch) -> None:
    _reset_supervisor_state()
    monkeypatch.setenv("AUTOBOTT_SESSION_AUTOSTART", "true")
    monkeypatch.setenv("AUTOBOTT_SESSION_MAX_CYCLES", "1")
    monkeypatch.setenv("AUTOBOTT_SESSION_ARM_PAPER_EXECUTION", "true")
    calls = []
    armed = []

    def fake_run_trading_session(**kwargs):
        calls.append(kwargs)

        class Result:
            def to_json_dict(self):
                return {"cycles_completed": 1}

        return Result()

    monkeypatch.setattr(supervisor, "run_trading_session", fake_run_trading_session)
    monkeypatch.setattr(supervisor, "arm_paper_execution", lambda *, reason: armed.append(reason))
    started = supervisor.maybe_start_session_supervisor()
    import time
    for _ in range(20):
        status = supervisor.session_supervisor_status()
        if status["state"]["last_result"] is not None:
            break
        time.sleep(0.01)
    assert started is True
    assert calls
    assert armed == ["session_supervisor_autostart"]


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
