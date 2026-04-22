"""Centralized feature-flag helpers.

All flags default to False in config so current behavior is unchanged
until each feature is explicitly enabled.
"""

from __future__ import annotations

import json
from pathlib import Path

try:
    from autotrader import config
except ImportError:
    import config

_KNOWN_FLAGS = (
    "FEATURE_SESSION_GUARDRAIL_PANEL",
    "FEATURE_TRADE_REPLAY",
    "FEATURE_PREMARKET_OPENING_PLAN_CARD",
    "FEATURE_EXIT_RELIABILITY_METRICS",
    "FEATURE_DRY_RUN_MODE",
    "FEATURE_SMART_ALERTS",
    "FEATURE_TICKER_SCORECARDS",
    "FEATURE_STRATEGY_PROFILES",
    "FEATURE_BAD_FILL_DETECTOR",
    "FEATURE_WEEKLY_REVIEW_GENERATOR",
)

_RUNTIME_FLAGS_PATH = Path(config.TRADING_CONTROL_PATH).with_name("feature_flags.json")


def _load_runtime_overrides() -> dict[str, bool]:
    if not _RUNTIME_FLAGS_PATH.exists():
        return {}
    try:
        payload = json.loads(_RUNTIME_FLAGS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    overrides: dict[str, bool] = {}
    for name in _KNOWN_FLAGS:
        if name in payload:
            overrides[name] = bool(payload.get(name))
    return overrides


def save_runtime_overrides(overrides: dict[str, bool]) -> dict[str, bool]:
    payload: dict[str, bool] = {}
    for name in _KNOWN_FLAGS:
        if name in overrides:
            payload[name] = bool(overrides.get(name))
    _RUNTIME_FLAGS_PATH.parent.mkdir(parents=True, exist_ok=True)
    _RUNTIME_FLAGS_PATH.write_text(
        json.dumps(payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return payload


def clear_runtime_overrides() -> None:
    try:
        _RUNTIME_FLAGS_PATH.unlink(missing_ok=True)
    except Exception:
        pass


def is_enabled(flag_name: str, default: bool = False) -> bool:
    normalized = str(flag_name)
    runtime_overrides = _load_runtime_overrides()
    if normalized in runtime_overrides:
        return bool(runtime_overrides.get(normalized))
    return bool(getattr(config, normalized, default))


def get_feature_flags_snapshot() -> dict[str, bool]:
    return {name: is_enabled(name) for name in _KNOWN_FLAGS}


def get_runtime_overrides_snapshot() -> dict[str, bool]:
    return _load_runtime_overrides()
