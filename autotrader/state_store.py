"""JSON state persistence for restart-safe bot operation."""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

try:
    from autotrader import config
except ImportError:
    import config
from kv_store import load_json, redis_key, save_json

_STATE_KEY = redis_key("runtime_state")
_REDIS_FALLBACK_NOTICE_EMITTED = False


def _atomic_write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = None
    tmp_path = None
    try:
        fd, tmp_name = tempfile.mkstemp(prefix=".runtime_state_", suffix=".tmp", dir=str(path.parent))
        tmp_path = Path(tmp_name)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            fd = None
            json.dump(payload, f, indent=2, sort_keys=True)
            f.flush()
            os.fsync(f.fileno())
        os.replace(str(tmp_path), str(path))
    finally:
        if fd is not None:
            try:
                os.close(fd)
            except Exception:
                pass
        if tmp_path is not None and tmp_path.exists():
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass


def _load_file_state(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
        if isinstance(payload, dict):
            return payload
    except Exception as exc:  # noqa: BLE001
        print(f"[state] load failed: {exc}")
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
    global _REDIS_FALLBACK_NOTICE_EMITTED
    payload = dict(state or {})
    payload["_state_updated_at_iso"] = datetime.now(timezone.utc).isoformat()

    redis_saved = save_json(_STATE_KEY, payload)
    if not redis_saved and not _REDIS_FALLBACK_NOTICE_EMITTED:
        print("[state] Redis unavailable. Using persistent file-backed state only.")
        _REDIS_FALLBACK_NOTICE_EMITTED = True

    state_path = path or config.STATE_JSON_PATH
    try:
        _atomic_write_json(state_path, payload)
    except Exception as exc:  # noqa: BLE001
        print(f"[state] save failed: {exc}")
