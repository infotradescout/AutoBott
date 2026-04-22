"""Persistent watchlist policy for scan/trade filtering."""

from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Iterable

import pytz

try:
    from autotrader import config
except ImportError:
    import config
from kv_store import load_json, redis_key, save_json

_WATCHLIST_KEY = redis_key("watchlist_control")
_VALID_MODES = {"off", "only_listed", "exclude_listed"}
_TICKER_RE = re.compile(r"^[A-Z][A-Z0-9.\-]{0,9}$")


def _sanitize_tickers(values: Iterable[str]) -> list[str]:
    cleaned: list[str] = []
    for raw in values:
        ticker = str(raw or "").strip().upper()
        if not ticker:
            continue
        if not _TICKER_RE.match(ticker):
            continue
        cleaned.append(ticker)
    return list(dict.fromkeys(cleaned))


def _default_state() -> dict:
    return {
        "mode": "off",
        "tickers": [],
        "updated_at_et": "",
        "reason": "",
    }


def load_watchlist_control(path: Path | None = None) -> dict:
    cached = load_json(_WATCHLIST_KEY)
    if isinstance(cached, dict):
        merged = _default_state()
        merged.update(cached)
        mode = str(merged.get("mode", "off") or "off").strip().lower()
        merged["mode"] = mode if mode in _VALID_MODES else "off"
        merged["tickers"] = _sanitize_tickers(merged.get("tickers") or [])
        return merged

    control_path = path or config.WATCHLIST_CONTROL_PATH
    if not control_path.exists():
        return _default_state()
    try:
        with control_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            merged = _default_state()
            merged.update(data)
            mode = str(merged.get("mode", "off") or "off").strip().lower()
            merged["mode"] = mode if mode in _VALID_MODES else "off"
            merged["tickers"] = _sanitize_tickers(merged.get("tickers") or [])
            return merged
    except Exception as exc:  # noqa: BLE001
        print(f"[watchlist] load failed: {exc}")
    return _default_state()


def save_watchlist_control(state: dict, path: Path | None = None) -> dict:
    payload = _default_state()
    payload.update(state or {})
    mode = str(payload.get("mode", "off") or "off").strip().lower()
    payload["mode"] = mode if mode in _VALID_MODES else "off"
    payload["tickers"] = _sanitize_tickers(payload.get("tickers") or [])
    if not payload.get("updated_at_et"):
        now_et = datetime.now(pytz.timezone(config.EASTERN_TZ))
        payload["updated_at_et"] = now_et.strftime("%Y-%m-%d %H:%M:%S %Z")

    if save_json(_WATCHLIST_KEY, payload):
        return payload

    control_path = path or config.WATCHLIST_CONTROL_PATH
    control_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with control_path.open("w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, sort_keys=True)
    except Exception as exc:  # noqa: BLE001
        print(f"[watchlist] save failed: {exc}")
    return payload


def update_watchlist_control(
    *,
    mode: str | None = None,
    tickers: Iterable[str] | None = None,
    reason: str = "",
) -> dict:
    current = load_watchlist_control()
    if mode is not None:
        normalized_mode = str(mode or "").strip().lower()
        if normalized_mode in _VALID_MODES:
            current["mode"] = normalized_mode
    if tickers is not None:
        current["tickers"] = _sanitize_tickers(tickers)
    now_et = datetime.now(pytz.timezone(config.EASTERN_TZ))
    current["updated_at_et"] = now_et.strftime("%Y-%m-%d %H:%M:%S %Z")
    current["reason"] = reason or "watchlist_update"
    return save_watchlist_control(current)

