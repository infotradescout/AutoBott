"""Strategy profile presets for runtime overrides."""

from __future__ import annotations

PROFILE_PRESETS: dict[str, dict[str, float]] = {
    # Highest selectivity + tightest stop.
    "conservative": {
        "entry_min_signal_score": 7.2,
        "stop_loss_usd": 8.0,
        "entry_max_quote_spread_pct": 10.0,
    },
    # Baseline behavior.
    "balanced": {
        "entry_min_signal_score": 6.0,
        "stop_loss_usd": 9.0,
        "entry_max_quote_spread_pct": 12.0,
    },
    # Looser selectivity + wider stop.
    "aggressive": {
        "entry_min_signal_score": 5.0,
        "stop_loss_usd": 12.0,
        "entry_max_quote_spread_pct": 15.0,
    },
}


def normalize_profile_name(name: str | None) -> str:
    value = str(name or "").strip().lower()
    if value in PROFILE_PRESETS:
        return value
    return "balanced"


def get_profile_overrides(name: str | None) -> dict[str, float]:
    normalized = normalize_profile_name(name)
    return dict(PROFILE_PRESETS.get(normalized, PROFILE_PRESETS["balanced"]))
