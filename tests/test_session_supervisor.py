from __future__ import annotations

import autobott_v2.session_supervisor as supervisor


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
    supervisor._SESSION_STATE.cycles_completed = 0
    supervisor._SESSION_STATE.last_cycle_at = None


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


def test_load_session_supervisor_config_expands_top_options_universe(monkeypatch) -> None:
    _reset_supervisor_state()
    monkeypatch.setenv("AUTOBOTT_SESSION_SYMBOLS", "TOP_OPTIONS_100")

    config = supervisor.load_session_supervisor_config()

    assert len(config.symbols) == 100
    assert config.symbols[:5] == ["SPY", "QQQ", "IWM", "DIA", "TLT"]


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
    assert calls[0]["continuous_window"] is True
    assert calls[0]["on_cycle_complete"] is supervisor._record_cycle_result
    second = supervisor.maybe_start_session_supervisor()
    assert second is False


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


def test_consumed_autostart_still_ensures_position_monitor(monkeypatch) -> None:
    _reset_supervisor_state()
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
