"""Read-only entry decision trace for AutoBott.

This module turns scan log rows into an operator-friendly view of why a ticker
is likely approved or blocked before order submission. It does not place orders,
change state, or alter strategy behavior.
"""

from __future__ import annotations

import csv
import re
from collections import Counter, deque
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    import pytz
except Exception:  # noqa: BLE001
    pytz = None

try:
    from autotrader import config
except ImportError:
    import config  # type: ignore

EASTERN = pytz.timezone(str(getattr(config, "EASTERN_TZ", "US/Eastern") or "US/Eastern")) if pytz is not None else None


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _parse_timestamp(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None and EASTERN is not None:
            dt = EASTERN.localize(dt)
        return dt.astimezone(EASTERN) if EASTERN is not None and dt.tzinfo is not None else dt
    except ValueError:
        pass
    for suffix in (" EDT", " EST"):
        if raw.upper().endswith(suffix.strip()):
            base = raw[: -len(suffix)].strip()
            try:
                dt = datetime.strptime(base, "%Y-%m-%d %H:%M:%S")
                return EASTERN.localize(dt) if EASTERN is not None else dt
            except ValueError:
                return None
    try:
        dt = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")
        return EASTERN.localize(dt) if EASTERN is not None else dt
    except ValueError:
        return None


def _read_recent_rows(path: Path, limit: int = 5000) -> list[dict[str, str]]:
    if not path.exists():
        return []
    try:
        with path.open("r", newline="", encoding="utf-8") as handle:
            return list(deque(csv.DictReader(handle), maxlen=max(1, int(limit))))
    except Exception:
        return []


def _reason_contains(reason: str, needle: str) -> bool:
    return needle.lower() in str(reason or "").lower()


def _extract_direction_score(reason: str) -> float | None:
    text = str(reason or "")
    # Scanner reason commonly includes "Dir +1.00" or "Dir -0.78".
    match = re.search(r"\bDir\s+([+-]?\d+(?:\.\d+)?)", text, re.IGNORECASE)
    if match:
        return _safe_float(match.group(1), 0.0)
    # Reject reason commonly includes "direction conviction weak (+0.05 < 0.25)".
    match = re.search(r"direction conviction weak \(([+-]?\d+(?:\.\d+)?)\s*<", text, re.IGNORECASE)
    if match:
        return _safe_float(match.group(1), 0.0)
    return None


def _direction_alignment(row: dict[str, str]) -> dict[str, Any]:
    direction = str(row.get("direction", "") or "").strip().lower()
    reason = str(row.get("reason", "") or "")
    direction_score = _extract_direction_score(reason)
    if direction_score is None:
        direction_score = _safe_float(row.get("direction_score"), 0.0)

    roc = _safe_float(row.get("roc"), 0.0)
    rvol = _safe_float(row.get("rvol"), 0.0)
    signal_score = _safe_float(row.get("signal_score"), 0.0)

    above_vwap = _reason_contains(reason, "above vwap")
    below_vwap = _reason_contains(reason, "below vwap")
    ema_bullish = _reason_contains(reason, "ema bullish")
    ema_bearish = _reason_contains(reason, "ema bearish")

    if direction == "call":
        checks = {
            "direction_score_ok": direction_score >= float(getattr(config, "FAST_START_MIN_DIRECTION_SCORE", 0.6)),
            "vwap_ok": above_vwap,
            "roc_ok": roc > 0 or _reason_contains(reason, "roc +"),
            "ema_ok": ema_bullish,
        }
    elif direction == "put":
        checks = {
            "direction_score_ok": abs(direction_score) >= float(getattr(config, "FAST_START_MIN_DIRECTION_SCORE", 0.6)),
            "vwap_ok": below_vwap,
            "roc_ok": roc < 0 or _reason_contains(reason, "roc -"),
            "ema_ok": ema_bearish,
        }
    else:
        checks = {
            "direction_score_ok": False,
            "vwap_ok": False,
            "roc_ok": False,
            "ema_ok": False,
        }

    aligned_count = sum(1 for value in checks.values() if value)
    required = 4
    accuracy_status = "approved" if aligned_count >= required else "watch_only"
    if str(row.get("result", "") or "").lower() != "pass":
        accuracy_status = "scanner_rejected"

    final_blocker = ""
    if accuracy_status == "scanner_rejected":
        final_blocker = str(row.get("reason", "") or "scanner rejected")
    elif aligned_count < required:
        missing = [key.replace("_ok", "") for key, value in checks.items() if not value]
        final_blocker = "direction alignment missing: " + ", ".join(missing)

    return {
        "symbol": str(row.get("symbol", "") or row.get("ticker", "") or "").upper(),
        "timestamp": str(row.get("timestamp", "") or ""),
        "result": str(row.get("result", "") or ""),
        "direction": direction.upper() if direction else "--",
        "signal_score": round(signal_score, 2),
        "direction_score": round(direction_score, 2),
        "rvol": round(rvol, 2),
        "roc": round(roc, 4),
        "vwap_state": "above" if above_vwap else "below" if below_vwap else "unknown",
        "ema_state": "bullish" if ema_bullish else "bearish" if ema_bearish else "unknown",
        "aligned_count": aligned_count,
        "required_count": required,
        "accuracy_status": accuracy_status,
        "final_blocker": final_blocker,
        "checks": checks,
        "reason": reason,
    }


def build_entry_decision_trace(limit: int = 25) -> dict[str, Any]:
    rows = _read_recent_rows(Path(config.SCAN_LOG_CSV_PATH), limit=5000)
    if not rows:
        return {
            "summary": {"rows": 0, "approved": 0, "watch_only": 0, "scanner_rejected": 0},
            "items": [],
            "top_blockers": [],
        }

    today = None
    if EASTERN is not None:
        today = datetime.now(EASTERN).date()
    recent_today = []
    for row in rows:
        dt = _parse_timestamp(row.get("timestamp", ""))
        if today is None or (dt is not None and dt.date() == today):
            recent_today.append(row)

    # Latest rows first, but preserve only one newest row per symbol so the panel is readable.
    recent_today.reverse()
    seen: set[str] = set()
    items: list[dict[str, Any]] = []
    blockers: Counter[str] = Counter()
    for row in recent_today:
        symbol = str(row.get("symbol", "") or row.get("ticker", "") or "").upper()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        item = _direction_alignment(row)
        items.append(item)
        if item["final_blocker"]:
            blockers[str(item["final_blocker"])] += 1
        if len(items) >= int(limit):
            break

    counts = Counter(str(item["accuracy_status"]) for item in items)
    return {
        "summary": {
            "rows": len(items),
            "approved": counts.get("approved", 0),
            "watch_only": counts.get("watch_only", 0),
            "scanner_rejected": counts.get("scanner_rejected", 0),
            "direction_threshold": float(getattr(config, "FAST_START_MIN_DIRECTION_SCORE", 0.6)),
            "required_alignment": 4,
        },
        "items": items,
        "top_blockers": [{"reason": key, "count": value} for key, value in blockers.most_common(8)],
    }


if __name__ == "__main__":
    import json

    print(json.dumps(build_entry_decision_trace(limit=25), indent=2))
