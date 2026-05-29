"""JSON state persistence for restart-safe bot operation."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
import tempfile

import config
from kv_store import load_json, redis_key, save_json

_STATE_KEY = redis_key("runtime_state")
_LAST_STATE_HEALTH = "ok"
_LAST_STATE_ERROR = ""


def get_state_health() -> dict:
    return {
        "state_health": str(_LAST_STATE_HEALTH or "ok"),
        "state_error": str(_LAST_STATE_ERROR or ""),
    }


def _set_state_health(status: str, error: str = "") -> None:
    global _LAST_STATE_HEALTH, _LAST_STATE_ERROR
    _LAST_STATE_HEALTH = str(status or "ok")
    _LAST_STATE_ERROR = str(error or "")


def _load_file_state(path: Path) -> dict:
    if not path.exists():
        _set_state_health("ok", "")
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
        if isinstance(payload, dict):
            _set_state_health("ok", "")
            return payload
    except Exception as exc:  # noqa: BLE001
        print(f"[state] load failed: {exc}")
        try:
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
            corrupt_path = path.with_name(f"{path.stem}.corrupt.{stamp}.json")
            path.replace(corrupt_path)
            _set_state_health("corrupted_recovered", str(exc))
            print(f"[state] state_recovered_from_corruption=true moved_to={corrupt_path.name}")
        except Exception as move_exc:  # noqa: BLE001
            _set_state_health("load_failed", f"{exc}; quarantine_failed={move_exc}")
            print(f"[state] corruption quarantine failed: {move_exc}")
    else:
        _set_state_health("load_failed", "state payload not dict")
    return {}


def _state_updated_ts(payload: dict) -> datetime | None:
    raw = str(payload.get("_state_updated_at_iso", "") or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return None


def _select_fresher_state(cached: dict, file_state: dict) -> dict:
    if not cached and not file_state:
        return {}
    if cached and not file_state:
        return cached
    if file_state and not cached:
        return file_state

    cached_ts = _state_updated_ts(cached)
    file_ts = _state_updated_ts(file_state)
    if cached_ts and file_ts:
        return cached if cached_ts >= file_ts else file_state
    if cached_ts and not file_ts:
        return cached
    if file_ts and not cached_ts:
        return file_state

    # Fallback for legacy payloads without timestamp marker.
    return cached if len(cached) >= len(file_state) else file_state


def load_bot_state(path: Path | None = None) -> dict:
    state_path = path or config.STATE_JSON_PATH
    cached = load_json(_STATE_KEY)
    cached_state = cached if isinstance(cached, dict) else {}
    file_state = _load_file_state(state_path)
    return _select_fresher_state(cached_state, file_state)


def save_bot_state(state: dict, path: Path | None = None) -> None:
    payload = dict(state or {})
    payload["_state_updated_at_iso"] = datetime.now(timezone.utc).isoformat()

    redis_saved = save_json(_STATE_KEY, payload)
    if not redis_saved:
        print("[state] Redis save unavailable; writing file fallback only.")

    state_path = path or config.STATE_JSON_PATH
    state_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd, tmp_name = tempfile.mkstemp(prefix=f"{state_path.stem}.", suffix=".tmp", dir=str(state_path.parent))
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2, sort_keys=True)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_name, state_path)
            _set_state_health("ok", "")
        finally:
            if os.path.exists(tmp_name):
                try:
                    os.remove(tmp_name)
                except Exception:
                    pass
    except Exception as exc:  # noqa: BLE001
        _set_state_health("load_failed", str(exc))
        print(f"[state] save failed: {exc}")
