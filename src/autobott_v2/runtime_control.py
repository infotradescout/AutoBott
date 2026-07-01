from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .runtime_paths import data_root


def runtime_state_path() -> Path:
    return data_root() / "execution" / "runtime_state.json"


@dataclass(frozen=True)
class RuntimeControlState:
    kill_switch_enabled: bool
    execution_enabled: bool
    live_mode_enabled: bool
    updated_at: datetime
    reason: str | None = None

    def to_json_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["updated_at"] = self.updated_at.astimezone(UTC).isoformat()
        return payload


def default_runtime_state() -> RuntimeControlState:
    return RuntimeControlState(
        kill_switch_enabled=False,
        execution_enabled=True,
        live_mode_enabled=False,
        updated_at=datetime.now(tz=UTC),
        reason="default_startup_state",
    )


def load_runtime_state(*, state_path: str | Path | None = None) -> RuntimeControlState:
    path = Path(state_path) if state_path is not None else runtime_state_path()
    if not path.exists():
        return default_runtime_state()
    payload = json.loads(path.read_text(encoding="utf-8"))
    return RuntimeControlState(
        kill_switch_enabled=bool(payload.get("kill_switch_enabled", False)),
        execution_enabled=bool(payload.get("execution_enabled", True)),
        live_mode_enabled=bool(payload.get("live_mode_enabled", False)),
        updated_at=datetime.fromisoformat(str(payload["updated_at"]).replace("Z", "+00:00")).astimezone(UTC),
        reason=payload.get("reason"),
    )


def save_runtime_state(state: RuntimeControlState, *, state_path: str | Path | None = None) -> Path:
    path = Path(state_path) if state_path is not None else runtime_state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state.to_json_dict(), indent=2, sort_keys=True), encoding="utf-8")
    return path


def set_kill_switch(enabled: bool, *, reason: str, state_path: str | Path | None = None) -> RuntimeControlState:
    current = load_runtime_state(state_path=state_path)
    updated = RuntimeControlState(
        kill_switch_enabled=enabled,
        execution_enabled=False if enabled else current.execution_enabled,
        live_mode_enabled=False if enabled else current.live_mode_enabled,
        updated_at=datetime.now(tz=UTC),
        reason=reason,
    )
    save_runtime_state(updated, state_path=state_path)
    return updated


def set_execution_mode(
    *,
    execution_enabled: bool,
    live_mode_enabled: bool,
    reason: str,
    state_path: str | Path | None = None,
) -> RuntimeControlState:
    current = load_runtime_state(state_path=state_path)
    updated = RuntimeControlState(
        kill_switch_enabled=current.kill_switch_enabled,
        execution_enabled=False if current.kill_switch_enabled else execution_enabled,
        live_mode_enabled=False if current.kill_switch_enabled else live_mode_enabled,
        updated_at=datetime.now(tz=UTC),
        reason=reason,
    )
    save_runtime_state(updated, state_path=state_path)
    return updated


def arm_paper_execution(*, reason: str, state_path: str | Path | None = None) -> RuntimeControlState:
    updated = RuntimeControlState(
        kill_switch_enabled=False,
        execution_enabled=True,
        live_mode_enabled=False,
        updated_at=datetime.now(tz=UTC),
        reason=reason,
    )
    save_runtime_state(updated, state_path=state_path)
    return updated


def disable_execution(*, reason: str, state_path: str | Path | None = None) -> RuntimeControlState:
    current = load_runtime_state(state_path=state_path)
    updated = RuntimeControlState(
        kill_switch_enabled=current.kill_switch_enabled,
        execution_enabled=False,
        live_mode_enabled=False,
        updated_at=datetime.now(tz=UTC),
        reason=reason,
    )
    save_runtime_state(updated, state_path=state_path)
    return updated
