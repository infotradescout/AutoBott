"""Shared desk-state bus for market context and ranked candidates."""

from __future__ import annotations

import json
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
        with path.open("w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, sort_keys=True)
    except Exception as exc:  # noqa: BLE001
        print(f"[desk_state] save failed for {path}: {exc}")


def _load(key: str, path: Path) -> dict[str, Any]:
    cached = load_json(key)
    if isinstance(cached, dict):
        return cached
    return _load_file(path)


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
