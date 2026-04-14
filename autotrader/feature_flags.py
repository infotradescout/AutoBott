"""Centralized feature-flag helpers.

All flags default to False in config so current behavior is unchanged
until each feature is explicitly enabled.
"""

from __future__ import annotations

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


def is_enabled(flag_name: str, default: bool = False) -> bool:
    return bool(getattr(config, str(flag_name), default))


def get_feature_flags_snapshot() -> dict[str, bool]:
    return {name: is_enabled(name) for name in _KNOWN_FLAGS}

