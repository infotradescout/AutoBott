"""Rank scanner hits into a shared candidate queue."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import config


def _safe_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _alignment_bonus(signal: dict, market_context: dict) -> float:
    preferred = str(market_context.get("preferred_direction", "both") or "both").lower()
    direction = str(signal.get("direction", "") or "").lower()
    if preferred in {"call", "put"}:
        return 2.0 if direction == preferred else -3.0
    return 0.0


def _profile_bonus(signal: dict, market_context: dict) -> float:
    profile = str(signal.get("strategy_profile", "") or "").lower()
    blocked = {str(item).lower() for item in market_context.get("blocked_profiles", []) or []}
    allowed = {str(item).lower() for item in market_context.get("allowed_profiles", []) or []}
    if profile and profile in blocked:
        return -4.0
    if profile and profile in allowed:
        return 1.5
    return 0.0


def candidate_edge_score(signal: dict, market_context: dict) -> float:
    signal_score = _safe_float(signal.get("signal_score"))
    direction_score = abs(_safe_float(signal.get("direction_score")))
    rvol = min(5.0, max(0.0, _safe_float(signal.get("rvol"))))
    roc = min(3.0, abs(_safe_float(signal.get("roc"))))
    return round(
        signal_score
        + (direction_score * 2.0)
        + (rvol * 0.35)
        + (roc * 0.25)
        + _alignment_bonus(signal, market_context)
        + _profile_bonus(signal, market_context),
        4,
    )


def build_candidate_queue(
    signals: list[dict],
    *,
    market_context: dict,
    now_et: datetime,
    source: str = "main_loop",
) -> dict[str, Any]:
    max_items = max(1, int(getattr(config, "CANDIDATE_QUEUE_MAX_ITEMS", 80) or 80))
    candidates: list[dict[str, Any]] = []
    for idx, signal in enumerate(signals or []):
        payload = dict(signal)
        payload["candidate_id"] = f"{now_et.strftime('%Y%m%d%H%M%S')}-{idx}-{payload.get('symbol', '')}"
        payload["edge_score"] = candidate_edge_score(payload, market_context)
        payload["regime"] = market_context.get("regime", "")
        payload["regime_preferred_direction"] = market_context.get("preferred_direction", "both")
        candidates.append(payload)
    candidates.sort(key=lambda item: _safe_float(item.get("edge_score")), reverse=True)
    return {
        "timestamp_et": now_et.isoformat(),
        "source": source,
        "market_context_ts": str(market_context.get("timestamp_et", "") or ""),
        "regime": str(market_context.get("regime", "") or ""),
        "preferred_direction": str(market_context.get("preferred_direction", "both") or "both"),
        "count": min(len(candidates), max_items),
        "candidates": candidates[:max_items],
    }
