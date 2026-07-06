from __future__ import annotations

import os
import threading
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, time as daytime
from typing import Any

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
    quantity: int
    position_count: int
    daily_pnl: float
    start_time: str | None
    end_time: str | None
    market_timezone: str
    arm_paper_execution_on_start: bool


@dataclass
class SessionSupervisorState:
    running: bool = False
    started_at: datetime | None = None
    finished_at: datetime | None = None
    last_result: dict[str, Any] | None = None
    last_error: str | None = None

    def to_json_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["started_at"] = self.started_at.astimezone(UTC).isoformat() if self.started_at else None
        payload["finished_at"] = self.finished_at.astimezone(UTC).isoformat() if self.finished_at else None
        return payload


_SESSION_LOCK = threading.Lock()
_SESSION_THREAD: threading.Thread | None = None
_SESSION_STATE = SessionSupervisorState()
_SESSION_AUTOSTART_CONSUMED = False


def load_session_supervisor_config() -> SessionSupervisorConfig:
    symbols = [item.strip().upper() for item in (os.getenv("AUTOBOTT_SESSION_SYMBOLS") or "SPY").split(",") if item.strip()]
    raw_max_cycles = os.getenv("AUTOBOTT_SESSION_MAX_CYCLES")
    return SessionSupervisorConfig(
        enabled=_normalize_bool(os.getenv("AUTOBOTT_SESSION_AUTOSTART"), default=False),
        symbols=symbols,
        interval_seconds=int(os.getenv("AUTOBOTT_SESSION_INTERVAL_SECONDS", "300")),
        max_cycles=int(raw_max_cycles) if raw_max_cycles else None,
        quantity=int(os.getenv("AUTOBOTT_SESSION_QUANTITY", "1")),
        position_count=int(os.getenv("AUTOBOTT_SESSION_POSITION_COUNT", "0")),
        daily_pnl=float(os.getenv("AUTOBOTT_SESSION_DAILY_PNL", "0.0")),
        start_time=_normalize_time_text(os.getenv("AUTOBOTT_SESSION_START_TIME")),
        end_time=_normalize_time_text(os.getenv("AUTOBOTT_SESSION_END_TIME")),
        market_timezone=(os.getenv("AUTOBOTT_SESSION_MARKET_TIMEZONE") or "America/New_York").strip() or "America/New_York",
        arm_paper_execution_on_start=_normalize_bool(os.getenv("AUTOBOTT_SESSION_ARM_PAPER_EXECUTION"), default=False),
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
        global _SESSION_THREAD, _SESSION_AUTOSTART_CONSUMED
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
        _SESSION_THREAD = threading.Thread(target=_run_session, args=(config,), daemon=True, name="autobott-session")
        _SESSION_THREAD.start()
        return True


def session_supervisor_status() -> dict[str, Any]:
    config = load_session_supervisor_config()
    with _SESSION_LOCK:
        return {
            "config": asdict(config),
            "state": _SESSION_STATE.to_json_dict(),
            "thread_alive": bool(_SESSION_THREAD and _SESSION_THREAD.is_alive()),
        }


def _run_session(config: SessionSupervisorConfig) -> None:
    global _SESSION_STATE
    try:
        if config.arm_paper_execution_on_start:
            arm_paper_execution(reason="session_supervisor_autostart")
        result = run_trading_session(
            symbols=config.symbols,
            interval_seconds=config.interval_seconds,
            start_time=_parse_optional_time(config.start_time),
            end_time=_parse_optional_time(config.end_time),
            market_timezone=config.market_timezone,
            max_cycles=config.max_cycles,
            continuous_window=True,
            cycle_kwargs={
                "quantity": config.quantity,
                "position_count": config.position_count,
                "current_daily_realized_pnl": config.daily_pnl,
            },
        )
        with _SESSION_LOCK:
            _SESSION_STATE.last_result = result.to_json_dict()
            _SESSION_STATE.last_error = None
    except Exception as exc:  # pragma: no cover
        with _SESSION_LOCK:
            _SESSION_STATE.last_error = f"{type(exc).__name__}: {exc}"
    finally:
        with _SESSION_LOCK:
            _SESSION_STATE.running = False
            _SESSION_STATE.finished_at = datetime.now(tz=UTC)


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
