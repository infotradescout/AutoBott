"""Shared desk-state bus for market context and ranked candidates."""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import config
from kv_store import load_json, redis_key, save_json

_MARKET_CONTEXT_KEY = redis_key("market_context")
_CANDIDATE_QUEUE_KEY = redis_key("candidate_queue")


def _with_updated_ts(payload: dict[str, Any]) -> dict[str, Any]:
    out = dict(payload or {})
    out["_updated_at_iso"] = datetime.now(timezone.utc).isoformat()
    return out


def _load_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
        return payload if isinstance(payload, dict) else {}
    except Exception as exc:  # noqa: BLE001
        print(f"[desk_state] load failed for {path}: {exc}")
        return {}


def _save_file(path: Path, payload: dict[str, Any]) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_suffix(f".tmp.{int(time.time_ns())}")
        with tmp_path.open("w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, sort_keys=True)
            f.flush()
            os.fsync(f.fileno())
        tmp_path.replace(path)
    except Exception as exc:  # noqa: BLE001
        print(f"[desk_state] save failed for {path}: {exc}")


def _parse_updated_at(payload: dict[str, Any]) -> datetime | None:
    raw = str(payload.get("_updated_at_iso", "") or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw)
    except Exception:
        return None


def _newer_payload(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(left, dict):
        left = {}
    if not isinstance(right, dict):
        right = {}

    if not left and right:
        return right
    if not right and left:
        return left
    if not left and not right:
        return {}

    left_ts = _parse_updated_at(left)
    right_ts = _parse_updated_at(right)
    if left_ts is None and right_ts is None:
        return right if right else left
    if left_ts is None:
        return right
    if right_ts is None:
        return left
    return right if right_ts >= left_ts else left


def _load(key: str, path: Path) -> dict[str, Any]:
    redis_payload = load_json(key)
    file_payload = _load_file(path)
    if not isinstance(redis_payload, dict):
        redis_payload = {}
    return _newer_payload(redis_payload, file_payload)


def _save(key: str, path: Path, payload: dict[str, Any]) -> None:
    updated = _with_updated_ts(payload)
    save_json(key, updated)
    _save_file(path, updated)


def load_market_context() -> dict[str, Any]:
    return _load(_MARKET_CONTEXT_KEY, config.MARKET_CONTEXT_JSON_PATH)


def save_market_context(payload: dict[str, Any]) -> None:
    _save(_MARKET_CONTEXT_KEY, config.MARKET_CONTEXT_JSON_PATH, payload)


def load_candidate_queue() -> dict[str, Any]:
    return _load(_CANDIDATE_QUEUE_KEY, config.CANDIDATE_QUEUE_JSON_PATH)


def save_candidate_queue(payload: dict[str, Any]) -> None:
    _save(_CANDIDATE_QUEUE_KEY, config.CANDIDATE_QUEUE_JSON_PATH, payload)
