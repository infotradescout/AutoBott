"""Persistent manual trading control (kill-switch)."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytz

import config
from kv_store import load_json, redis_key, save_json

_CONTROL_KEY = redis_key("trading_control")


def _default_state() -> dict:
    return {
        "manual_stop": False,
        "updated_at_et": "",
        "reason": "",
    }


def load_trading_control(path: Path | None = None) -> dict:
    cached = load_json(_CONTROL_KEY)
    if isinstance(cached, dict):
        merged = _default_state()
        merged.update(cached)
        merged["manual_stop"] = bool(merged.get("manual_stop", False))
        return merged

    control_path = path or config.TRADING_CONTROL_PATH
    if not control_path.exists():
        return _default_state()
    try:
        with control_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            merged = _default_state()
            merged.update(data)
            merged["manual_stop"] = bool(merged.get("manual_stop", False))
            return merged
    except Exception as exc:  # noqa: BLE001
        print(f"[control] load failed: {exc}")
    return _default_state()


def save_trading_control(state: dict, path: Path | None = None) -> dict:
    payload = _default_state()
    payload.update(state or {})
    payload["manual_stop"] = bool(payload.get("manual_stop", False))
    if not payload.get("updated_at_et"):
        now_et = datetime.now(pytz.timezone(config.EASTERN_TZ))
        payload["updated_at_et"] = now_et.strftime("%Y-%m-%d %H:%M:%S %Z")

    if save_json(_CONTROL_KEY, payload):
        return payload

    control_path = path or config.TRADING_CONTROL_PATH
    control_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with control_path.open("w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, sort_keys=True)
    except Exception as exc:  # noqa: BLE001
        print(f"[control] save failed: {exc}")
    return payload


def set_manual_stop(enabled: bool, reason: str = "") -> dict:
    current = load_trading_control()
    now_et = datetime.now(pytz.timezone(config.EASTERN_TZ))
    current["manual_stop"] = bool(enabled)
    current["reason"] = reason or ("manual_stop" if enabled else "manual_start")
    current["updated_at_et"] = now_et.strftime("%Y-%m-%d %H:%M:%S %Z")
    return save_trading_control(current)
