"""Helpers for promoting replay-farm candidates into runtime scanner overrides."""

from __future__ import annotations

import csv
import json
import math
from pathlib import Path
from typing import Any


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return int(default)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return float(default)
    if math.isnan(parsed) or math.isinf(parsed):
        return float(default)
    return parsed


def _read_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    try:
        with path.open("r", newline="", encoding="utf-8") as handle:
            return list(csv.DictReader(handle))
    except Exception:
        return []


def _parse_overrides(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return dict(raw)
    text = str(raw or "").strip()
    if not text:
        return {}
    try:
        payload = json.loads(text)
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    return payload


def _sanitize_overrides(raw: dict[str, Any], *, allowed_keys: set[str]) -> dict[str, float]:
    sanitized: dict[str, float] = {}
    for key, value in raw.items():
        normalized = str(key or "").strip().upper()
        if not normalized or normalized not in allowed_keys:
            continue
        numeric = _safe_float(value, math.nan)
        if math.isnan(numeric) or math.isinf(numeric):
            continue
        sanitized[normalized] = float(numeric)
    return sanitized


def _row_sort_key(row: dict[str, Any]) -> tuple[str, int, str]:
    return (
        str(row.get("run_timestamp", "") or ""),
        _safe_int(row.get("iteration"), 0),
        str(row.get("window_end", "") or ""),
    )


def select_candidate_overrides(
    *,
    farm_runs_csv: Path,
    candidate: str,
    worker_names: set[str] | None = None,
    allowed_keys: tuple[str, ...] = (),
) -> tuple[dict[str, float], dict[str, Any] | None]:
    rows = _read_rows(farm_runs_csv)
    target = str(candidate or "").strip()
    if not target:
        return {}, None

    worker_filter = {str(name or "").strip() for name in (worker_names or set())}
    normalized_allowed = {str(key or "").strip().upper() for key in allowed_keys if str(key or "").strip()}
    if not normalized_allowed:
        return {}, None

    matches = [
        row
        for row in rows
        if str(row.get("candidate", "") or "").strip() == target
        and (not worker_filter or str(row.get("worker", "") or "").strip() in worker_filter)
    ]
    if not matches:
        return {}, None

    for row in sorted(matches, key=_row_sort_key, reverse=True):
        parsed = _parse_overrides(row.get("overrides_json", ""))
        overrides = _sanitize_overrides(parsed, allowed_keys=normalized_allowed)
        if overrides:
            return overrides, row
    return {}, None


def build_promotion_snapshot(
    *,
    aggregate_payload: dict[str, Any],
    worker_names: set[str] | None = None,
    allowed_override_keys: tuple[str, ...] = (),
) -> dict[str, Any]:
    best = aggregate_payload.get("best") if isinstance(aggregate_payload, dict) else None
    farm_runs_csv = Path(str(aggregate_payload.get("farm_runs_csv", "") or ""))
    candidate = str((best or {}).get("candidate", "") or "").strip()
    promotable = bool((best or {}).get("promotable", False))

    snapshot: dict[str, Any] = {
        "ok": True,
        "generated_at": str(aggregate_payload.get("generated_at", "") or ""),
        "farm_runs_csv": str(farm_runs_csv),
        "candidate": candidate,
        "promotable": promotable,
        "best": best if isinstance(best, dict) else {},
        "worker_filter": sorted(worker_names) if worker_names else [],
        "overrides": {},
        "override_source": {},
    }
    if not promotable or not candidate or not farm_runs_csv:
        return snapshot

    overrides, source_row = select_candidate_overrides(
        farm_runs_csv=farm_runs_csv,
        candidate=candidate,
        worker_names=worker_names,
        allowed_keys=allowed_override_keys,
    )
    snapshot["overrides"] = overrides
    snapshot["override_source"] = {
        "worker": str((source_row or {}).get("worker", "") or ""),
        "iteration": _safe_int((source_row or {}).get("iteration"), 0),
        "run_timestamp": str((source_row or {}).get("run_timestamp", "") or ""),
        "window_start": str((source_row or {}).get("window_start", "") or ""),
        "window_end": str((source_row or {}).get("window_end", "") or ""),
    }
    return snapshot
