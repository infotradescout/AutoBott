from __future__ import annotations

import os
import json
import threading
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, time as daytime
from typing import Any

from .hosted_policy import (
    HOSTED_POSITION_MONITOR_HEARTBEAT_ENABLED,
    HOSTED_POSITION_MONITOR_HEARTBEAT_SECONDS,
    HOSTED_SESSION_END_TIME,
    HOSTED_SESSION_INTERVAL_SECONDS,
    HOSTED_SESSION_MARKET_TIMEZONE,
    HOSTED_SESSION_START_TIME,
    HOSTED_SESSION_SYMBOL_BATCH_SIZE,
    HOSTED_SESSION_SYMBOL_TOKENS,
    is_hosted_paper_runtime,
)
from .options_universe import resolve_symbol_universe
from .position_monitor import run_position_monitor
from .primary_runtime_evidence import poll_primary_runtime_evidence_once
from .runtime_control import arm_paper_execution
from .session_runner import run_trading_session


def _normalize_bool(value: str | None, *, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class SessionSupervisorConfig:
    enabled: bool
    symbols: list[str]
    interval_seconds: int
    max_cycles: int | None
    symbol_batch_size: int | None
    quantity: int
    position_count: int
    daily_pnl: float
    start_time: str | None
    end_time: str | None
    market_timezone: str
    arm_paper_execution_on_start: bool
    position_monitor_heartbeat_enabled: bool = False
    position_monitor_heartbeat_seconds: int = 15
    run_forever: bool = False


@dataclass
class SessionSupervisorState:
    running: bool = False
    started_at: datetime | None = None
    finished_at: datetime | None = None
    last_result: dict[str, Any] | None = None
    last_error: str | None = None
    last_monitor_result: dict[str, Any] | None = None
    last_monitor_error: str | None = None
    last_evidence_result: dict[str, Any] | None = None
    last_evidence_error: str | None = None
    last_evidence_at: datetime | None = None
    cycles_completed: int = 0
    last_cycle_at: datetime | None = None

    def to_json_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["started_at"] = self.started_at.astimezone(UTC).isoformat() if self.started_at else None
        payload["finished_at"] = self.finished_at.astimezone(UTC).isoformat() if self.finished_at else None
        payload["last_cycle_at"] = self.last_cycle_at.astimezone(UTC).isoformat() if self.last_cycle_at else None
        payload["last_evidence_at"] = self.last_evidence_at.astimezone(UTC).isoformat() if self.last_evidence_at else None
        return payload


_SESSION_LOCK = threading.Lock()
_SESSION_THREAD: threading.Thread | None = None
_POSITION_MONITOR_THREAD: threading.Thread | None = None
_SESSION_STOP_EVENT: threading.Event | None = None
_POSITION_MONITOR_STOP_EVENT: threading.Event | None = None
_SESSION_STATE = SessionSupervisorState()
_SESSION_AUTOSTART_CONSUMED = False


def load_session_supervisor_config() -> SessionSupervisorConfig:
    hosted_paper = is_hosted_paper_runtime()
    symbol_source = (HOSTED_SESSION_SYMBOL_TOKENS if hosted_paper else tuple(
        item.strip() for item in (os.getenv("AUTOBOTT_SESSION_SYMBOLS") or "SPY").split(",") if item.strip()))
    symbols = resolve_symbol_universe(list(symbol_source))
    raw_max_cycles = os.getenv("AUTOBOTT_SESSION_MAX_CYCLES")
    raw_batch_size = os.getenv("AUTOBOTT_SESSION_SYMBOL_BATCH_SIZE")
    run_forever = True if hosted_paper else _normalize_bool(os.getenv("AUTOBOTT_SESSION_RUN_FOREVER"), default=False)
    return SessionSupervisorConfig(
        enabled=_normalize_bool(os.getenv("AUTOBOTT_SESSION_AUTOSTART"), default=True),
        symbols=symbols,
        interval_seconds=HOSTED_SESSION_INTERVAL_SECONDS if hosted_paper else int(os.getenv("AUTOBOTT_SESSION_INTERVAL_SECONDS", "300")),
        max_cycles=None if run_forever else (int(raw_max_cycles) if raw_max_cycles else None),
        symbol_batch_size=HOSTED_SESSION_SYMBOL_BATCH_SIZE if hosted_paper else (int(raw_batch_size) if raw_batch_size else None),
        quantity=1 if hosted_paper else int(os.getenv("AUTOBOTT_SESSION_QUANTITY", "1")),
        position_count=0 if hosted_paper else int(os.getenv("AUTOBOTT_SESSION_POSITION_COUNT", "0")),
        daily_pnl=0.0 if hosted_paper else float(os.getenv("AUTOBOTT_SESSION_DAILY_PNL", "0.0")),
        start_time=_normalize_time_text(HOSTED_SESSION_START_TIME if hosted_paper else (os.getenv("AUTOBOTT_SESSION_START_TIME") or "09:35")),
        end_time=_normalize_time_text(HOSTED_SESSION_END_TIME if hosted_paper else (os.getenv("AUTOBOTT_SESSION_END_TIME") or "15:55")),
        market_timezone=(HOSTED_SESSION_MARKET_TIMEZONE if hosted_paper else (os.getenv("AUTOBOTT_SESSION_MARKET_TIMEZONE") or "America/New_York").strip() or "America/New_York"),
        arm_paper_execution_on_start=_normalize_bool(os.getenv("AUTOBOTT_SESSION_ARM_PAPER_EXECUTION"), default=True),
        position_monitor_heartbeat_enabled=(HOSTED_POSITION_MONITOR_HEARTBEAT_ENABLED if hosted_paper
            else _normalize_bool(os.getenv("AUTOBOTT_POSITION_MONITOR_HEARTBEAT_ENABLED"), default=False)),
        position_monitor_heartbeat_seconds=(HOSTED_POSITION_MONITOR_HEARTBEAT_SECONDS if hosted_paper
            else max(5, int(os.getenv("AUTOBOTT_POSITION_MONITOR_HEARTBEAT_SECONDS", "15")))),
        run_forever=run_forever,
    )


def maybe_start_session_supervisor() -> bool:
    config = load_session_supervisor_config()
    if not config.enabled:
        return False
    return _start_session_thread(config, consume_autostart=True)


def start_session_supervisor(config: SessionSupervisorConfig) -> bool:
    return _start_session_thread(config, consume_autostart=False)


def _start_session_thread(config: SessionSupervisorConfig, *, consume_autostart: bool) -> bool:
    with _SESSION_LOCK:
        global _SESSION_THREAD, _SESSION_AUTOSTART_CONSUMED, _SESSION_STOP_EVENT
        _ensure_position_monitor_thread_locked(config)
        if _SESSION_THREAD is not None and _SESSION_THREAD.is_alive():
            return False
        if consume_autostart and _SESSION_AUTOSTART_CONSUMED:
            return False
        if consume_autostart:
            _SESSION_AUTOSTART_CONSUMED = True
        _SESSION_STATE.running = True
        _SESSION_STATE.started_at = datetime.now(tz=UTC)
        _SESSION_STATE.finished_at = None
        _SESSION_STATE.last_error = None
        _SESSION_STATE.last_result = None
        _SESSION_STATE.last_monitor_error = None
        _SESSION_STATE.last_monitor_result = None
        _SESSION_STATE.last_evidence_result = None
        _SESSION_STATE.last_evidence_error = None
        _SESSION_STATE.last_evidence_at = None
        _SESSION_STATE.cycles_completed = 0
        _SESSION_STATE.last_cycle_at = None
        _SESSION_STOP_EVENT = threading.Event()
        _SESSION_THREAD = threading.Thread(target=_run_session, args=(config, _SESSION_STOP_EVENT), daemon=True, name="autobott-session")
        _SESSION_THREAD.start()
        return True


def session_supervisor_status() -> dict[str, Any]:
    config = load_session_supervisor_config()
    with _SESSION_LOCK:
        status = {"config": asdict(config), "state": _SESSION_STATE.to_json_dict(),
            "thread_alive": bool(_SESSION_THREAD and _SESSION_THREAD.is_alive()),
            "position_monitor_thread_alive": bool(_POSITION_MONITOR_THREAD and _POSITION_MONITOR_THREAD.is_alive())}
    # Provider diagnostics must never hold the supervisor's publication lock.
    from .portfolio_status import optional_capacity_status
    capacity = optional_capacity_status()
    if capacity is not None:
        status["portfolio_capacity"] = capacity
    return status


def _poll_and_record_primary_evidence() -> dict[str, Any]:
    try:
        result = poll_primary_runtime_evidence_once()
    except Exception as exc:  # Defensive: observational evidence cannot stop the supervisor.
        with _SESSION_LOCK:
            _SESSION_STATE.last_evidence_result = None
            _SESSION_STATE.last_evidence_error = f"{type(exc).__name__}: {exc}"
            _SESSION_STATE.last_evidence_at = datetime.now(tz=UTC)
        return {"trading_actions": 0, "error": f"{type(exc).__name__}: {exc}"}
    with _SESSION_LOCK:
        _SESSION_STATE.last_evidence_result = result
        _SESSION_STATE.last_evidence_error = None
        _SESSION_STATE.last_evidence_at = datetime.now(tz=UTC)
    return result


def _run_session(config: SessionSupervisorConfig, stop_event: threading.Event) -> None:
    global _SESSION_STATE
    try:
        if config.arm_paper_execution_on_start:
            arm_paper_execution(reason="session_supervisor_autostart")
        result = run_trading_session(
            symbols=config.symbols, interval_seconds=config.interval_seconds,
            start_time=_parse_optional_time(config.start_time), end_time=_parse_optional_time(config.end_time),
            market_timezone=config.market_timezone, max_cycles=config.max_cycles,
            symbol_batch_size=config.symbol_batch_size, continuous_window=True,
            cycle_kwargs={"quantity": config.quantity, "position_count": config.position_count,
                          "current_daily_realized_pnl": config.daily_pnl},
            on_cycle_complete=_record_cycle_result,
            after_entry_window_runner=_poll_and_record_primary_evidence)
        with _SESSION_LOCK:
            _SESSION_STATE.last_result = result.to_json_dict()
            _SESSION_STATE.last_error = None
    except Exception as exc:  # pragma: no cover
        with _SESSION_LOCK:
            _SESSION_STATE.last_error = f"{type(exc).__name__}: {exc}"
    finally:
        stop_event.set()
        with _SESSION_LOCK:
            _SESSION_STATE.running = False
            _SESSION_STATE.finished_at = datetime.now(tz=UTC)


def _record_cycle_result(cycle_result: dict[str, Any]) -> None:
    """Publish every cycle while the continuous session is still running."""
    with _SESSION_LOCK:
        _SESSION_STATE.cycles_completed += 1
        _SESSION_STATE.last_cycle_at = datetime.now(tz=UTC)
        _SESSION_STATE.last_result = {"cycles_completed": _SESSION_STATE.cycles_completed, "cycle_results": [cycle_result]}
        _SESSION_STATE.last_error = cycle_result.get("error")
    if os.getenv("AUTOBOTT_ACCOUNTING_RECOVERY_MODE", "").strip().lower() in {"plan", "apply"}:
        try:
            from .accounting_recovery import maybe_recover_blocked_cycle
            recovery = maybe_recover_blocked_cycle(cycle_result)
            if recovery is not None:
                with _SESSION_LOCK:
                    _SESSION_STATE.last_result["accounting_recovery"] = recovery
                print("AUTOBOTT_ACCOUNTING_RECOVERY " + json.dumps(recovery, sort_keys=True, allow_nan=False), flush=True)
        except Exception as exc:
            print("AUTOBOTT_ACCOUNTING_RECOVERY " + json.dumps({"status": "blocked", "reason": "recovery_hook_failed", "error_type": type(exc).__name__}), flush=True)


def _ensure_position_monitor_thread_locked(config: SessionSupervisorConfig) -> None:
    global _POSITION_MONITOR_THREAD, _POSITION_MONITOR_STOP_EVENT
    if not config.position_monitor_heartbeat_enabled:
        return
    if _POSITION_MONITOR_THREAD is not None and _POSITION_MONITOR_THREAD.is_alive():
        return
    _POSITION_MONITOR_STOP_EVENT = threading.Event()
    _POSITION_MONITOR_THREAD = threading.Thread(target=_run_position_monitor_heartbeat,
        args=(config, _POSITION_MONITOR_STOP_EVENT), daemon=True, name="autobott-position-monitor")
    _POSITION_MONITOR_THREAD.start()


def _run_position_monitor_heartbeat(config: SessionSupervisorConfig, stop_event: threading.Event) -> None:
    while not stop_event.is_set():
        try:
            result = run_position_monitor()
            with _SESSION_LOCK:
                _SESSION_STATE.last_monitor_result = result
                _SESSION_STATE.last_monitor_error = None
        except Exception as exc:  # pragma: no cover
            with _SESSION_LOCK:
                _SESSION_STATE.last_monitor_error = f"{type(exc).__name__}: {exc}"
        stop_event.wait(config.position_monitor_heartbeat_seconds)


def _normalize_time_text(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    if not stripped:
        return None
    return daytime.fromisoformat(stripped).isoformat()


def _parse_optional_time(value: str | None) -> daytime | None:
    if value is None:
        return None
    return daytime.fromisoformat(value)
