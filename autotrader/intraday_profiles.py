"""Named intraday strategy profiles and profile-level execution controls."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class IntradayProfile:
    name: str
    window_start: str
    window_end: str
    symbols: tuple[str, ...]
    entry_max_quote_spread_pct: float
    stop_loss_usd: float
    immediate_take_profit_pct: float
    max_hold_minutes: int
    min_signal_score: float
    priority: int


PROFILES: dict[str, IntradayProfile] = {
    "open_drive_momentum": IntradayProfile(
        name="open_drive_momentum",
        window_start="09:30",
        window_end="10:35",
        symbols=("SPY", "QQQ", "IWM", "NVDA", "TSLA"),
        entry_max_quote_spread_pct=14.0,
        stop_loss_usd=10.0,
        immediate_take_profit_pct=0.70,
        max_hold_minutes=45,
        min_signal_score=4.8,
        priority=1,
    ),
    "vwap_continuation": IntradayProfile(
        name="vwap_continuation",
        window_start="09:45",
        window_end="13:00",
        symbols=("SPY", "QQQ", "AAPL", "MSFT", "NVDA", "AMD"),
        entry_max_quote_spread_pct=16.0,
        stop_loss_usd=12.0,
        immediate_take_profit_pct=0.90,
        max_hold_minutes=75,
        min_signal_score=4.5,
        priority=2,
    ),
    "reversal_snapback": IntradayProfile(
        name="reversal_snapback",
        window_start="10:45",
        window_end="15:00",
        symbols=("SPY", "QQQ", "TSLA", "NVDA", "META", "AMZN"),
        entry_max_quote_spread_pct=18.0,
        stop_loss_usd=13.0,
        immediate_take_profit_pct=0.55,
        max_hold_minutes=40,
        min_signal_score=4.2,
        priority=3,
    ),
    "catalyst_impulse": IntradayProfile(
        name="catalyst_impulse",
        window_start="09:30",
        window_end="15:00",
        symbols=("SPY", "QQQ", "NVDA", "TSLA", "META", "AMZN", "AAPL", "MSFT"),
        entry_max_quote_spread_pct=20.0,
        stop_loss_usd=14.0,
        immediate_take_profit_pct=1.00,
        max_hold_minutes=90,
        min_signal_score=4.0,
        priority=4,
    ),
    # ── Fallback profile ────────────────────────────────────────────────────
    # Catches core liquid names that don't match a named profile.
    # symbols=() means universal — any symbol in permissive_core is eligible.
    # Logic gate is evaluated inline in _profile_signals_for_candidate().
    "generic_intraday_continuation": IntradayProfile(
        name="generic_intraday_continuation",
        window_start="09:30",
        window_end="16:00",
        symbols=(),          # universal — handled via permissive_core in scanner
        entry_max_quote_spread_pct=22.0,
        stop_loss_usd=12.0,
        immediate_take_profit_pct=0.80,
        max_hold_minutes=60,
        min_signal_score=3.0,   # intentionally lower — this is the safety net
        priority=5,             # lowest priority — only fires when named profiles miss
    ),
}


def _to_minutes(hhmm: str) -> int:
    parts = str(hhmm or "").split(":", 1)
    if len(parts) != 2:
        return -1
    try:
        hour = int(parts[0])
        minute = int(parts[1])
    except (TypeError, ValueError):
        return -1
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        return -1
    return hour * 60 + minute


def is_profile_window_open(now_et: datetime, profile: IntradayProfile) -> bool:
    now_minutes = (now_et.hour * 60) + now_et.minute
    start = _to_minutes(profile.window_start)
    end = _to_minutes(profile.window_end)
    if start < 0 or end < 0:
        return True
    return start <= now_minutes < end


def enrich_signal_for_profile(signal: dict[str, Any], profile: IntradayProfile) -> dict[str, Any]:
    out = dict(signal)
    out["strategy_profile"] = profile.name
    out["entry_max_quote_spread_pct"] = float(profile.entry_max_quote_spread_pct)
    out["stop_loss_usd"] = float(profile.stop_loss_usd)
    out["immediate_take_profit_pct"] = float(profile.immediate_take_profit_pct)
    out["max_hold_minutes"] = int(profile.max_hold_minutes)
    out["profile_min_signal_score"] = float(profile.min_signal_score)
    out["profile_priority"] = int(profile.priority)
    return out
