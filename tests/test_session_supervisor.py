from __future__ import annotations

import autobott_v2.session_supervisor as supervisor


def test_load_session_supervisor_config_from_env(monkeypatch) -> None:
    monkeypatch.setenv("AUTOBOTT_SESSION_AUTOSTART", "true")
    monkeypatch.setenv("AUTOBOTT_SESSION_SYMBOLS", "AAPL,MSFT")
    monkeypatch.setenv("AUTOBOTT_SESSION_INTERVAL_SECONDS", "120")
    monkeypatch.setenv("AUTOBOTT_SESSION_MAX_CYCLES", "4")
    config = supervisor.load_session_supervisor_config()
    assert config.enabled is True
    assert config.symbols == ["AAPL", "MSFT"]
    assert config.interval_seconds == 120
    assert config.max_cycles == 4


def test_maybe_start_session_supervisor_starts_once(monkeypatch) -> None:
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


def test_start_session_supervisor_can_start_manual_session(monkeypatch) -> None:
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
