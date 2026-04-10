"""JSON state persistence for restart-safe bot operation."""

from __future__ import annotations

import json
from pathlib import Path

import config
from kv_store import load_json, redis_key, save_json

_STATE_KEY = redis_key("runtime_state")


def load_bot_state(path: Path | None = None) -> dict:
    cached = load_json(_STATE_KEY)
    if isinstance(cached, dict):
        return cached

    state_path = path or config.STATE_JSON_PATH
    if not state_path.exists():
        return {}
    try:
        with state_path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
        if isinstance(payload, dict):
            return payload
    except Exception as exc:  # noqa: BLE001
        print(f"[state] load failed: {exc}")
    return {}


def save_bot_state(state: dict, path: Path | None = None) -> None:
    if save_json(_STATE_KEY, state):
        return

    state_path = path or config.STATE_JSON_PATH
    state_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with state_path.open("w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, sort_keys=True)
    except Exception as exc:  # noqa: BLE001
        print(f"[state] save failed: {exc}")
