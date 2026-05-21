"""Persistent decision learning memory for AutoBott.

Read-only learning layer with durable storage.

What this module does:
- Evaluates recent decisions using decision_outcomes.py.
- Persists evaluated rows into DATA_DIR/decision_memory.csv.
- Deduplicates across restarts so learning does not reset.
- Builds stable summaries by source, stage, symbol, direction, score ranges,
  RVOL ranges, ROC ranges, and direction-score ranges.
- Produces recommendations, but does not auto-change trading config.

Persistence doctrine:
- Runtime learning artifacts live under config.DATA_DIR.
- On Render, DATA_DIR should be /data with a persistent disk attached.
- If DATA_DIR falls back to /tmp, learning will not survive service rebuilds.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import time
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

import pytz

try:
    from autotrader import config
except ImportError:
    import config  # type: ignore

try:
    from decision_outcomes import build_decision_outcomes
except ImportError:
    from autotrader.decision_outcomes import build_decision_outcomes  # type: ignore

EASTERN = pytz.timezone(str(getattr(config, "EASTERN_TZ", "US/Eastern") or "US/Eastern"))

MEMORY_COLUMNS = [
    "decision_id",
    "first_seen_et",
    "last_seen_et",
    "seen_count",
    "timestamp",
    "source",
    "stage",
    "symbol",
    "direction",
    "decision",
    "verdict",
    "lesson",
    "score_delta",
    "horizon_minutes",
    "entry_price",
    "end_price",
    "directional_move_pct",
    "max_favorable_pct",
    "max_adverse_pct",
    "bars_used",
    "signal_score",
    "direction_score",
    "rvol",
    "roc",
    "vwap_state",
    "ema_state",
    "reason",
    "summary",
]


def _now_et() -> datetime:
    return datetime.now(EASTERN)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return float(default)
    if math.isnan(parsed) or math.isinf(parsed):
        return float(default)
    return parsed


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return int(default)


def _memory_dir() -> Path:
    base = Path(getattr(config, "DATA_DIR", os.getenv("DATA_DIR", "/tmp/autotrader-data")))
    base.mkdir(parents=True, exist_ok=True)
    return base


def memory_paths() -> dict[str, str]:
    base = _memory_dir()
    return {
        "data_dir": str(base),
        "memory_csv": str(base / "decision_memory.csv"),
        "summary_json": str(base / "decision_learning_summary.json"),
    }


def _decision_id(row: dict[str, Any]) -> str:
    stable = "|".join(
        str(row.get(key, ""))
        for key in ("timestamp", "source", "stage", "symbol", "direction", "decision", "reason")
    )
    return hashlib.sha256(stable.encode("utf-8", errors="ignore")).hexdigest()[:24]


def _flatten_outcome(row: dict[str, Any]) -> dict[str, Any]:
    metrics = row.get("metrics", {}) if isinstance(row.get("metrics"), dict) else {}
    now = _now_et().isoformat()
    payload = {
        "decision_id": _decision_id(row),
        "first_seen_et": now,
        "last_seen_et": now,
        "seen_count": 1,
        "timestamp": row.get("timestamp", ""),
        "source": row.get("source", ""),
        "stage": row.get("stage", ""),
        "symbol": row.get("symbol", ""),
        "direction": row.get("direction", ""),
        "decision": row.get("decision", ""),
        "verdict": row.get("verdict", ""),
        "lesson": row.get("lesson", ""),
        "score_delta": row.get("score_delta", 0),
        "horizon_minutes": row.get("horizon_minutes", 0),
        "entry_price": row.get("entry_price", 0.0),
        "end_price": row.get("end_price", 0.0),
        "directional_move_pct": row.get("directional_move_pct", 0.0),
        "max_favorable_pct": row.get("max_favorable_pct", 0.0),
        "max_adverse_pct": row.get("max_adverse_pct", 0.0),
        "bars_used": row.get("bars_used", 0),
        "signal_score": metrics.get("signal_score", ""),
        "direction_score": metrics.get("direction_score", ""),
        "rvol": metrics.get("rvol", ""),
        "roc": metrics.get("roc", ""),
        "vwap_state": metrics.get("vwap_state", ""),
        "ema_state": metrics.get("ema_state", ""),
        "reason": row.get("reason", ""),
        "summary": row.get("summary", ""),
    }
    return {key: payload.get(key, "") for key in MEMORY_COLUMNS}


def _read_memory(path: Path) -> dict[str, dict[str, str]]:
    if not path.exists():
        return {}
    try:
        with path.open("r", newline="", encoding="utf-8") as handle:
            rows = csv.DictReader(handle)
            return {str(row.get("decision_id", "")): {key: row.get(key, "") for key in MEMORY_COLUMNS} for row in rows if row.get("decision_id")}
    except Exception as exc:  # noqa: BLE001
        print(f"[decision_memory] read failed: {exc}")
        return {}


def _write_memory(path: Path, rows_by_id: dict[str, dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    ordered = sorted(rows_by_id.values(), key=lambda row: str(row.get("timestamp", "")))
    with temp.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=MEMORY_COLUMNS)
        writer.writeheader()
        for row in ordered:
            writer.writerow({key: row.get(key, "") for key in MEMORY_COLUMNS})
    temp.replace(path)


def _bucket(value: Any, buckets: list[tuple[float, str]], default: str = "unknown") -> str:
    val = _safe_float(value, math.nan)
    if math.isnan(val):
        return default
    for limit, label in buckets:
        if val < limit:
            return label
    return buckets[-1][1] if buckets else default


def _rvol_bucket(value: Any) -> str:
    return _bucket(value, [(0.10, "rvol<0.10"), (0.25, "0.10-0.24"), (0.50, "0.25-0.49"), (1.00, "0.50-0.99"), (999.0, "1.00+")])


def _score_bucket(value: Any) -> str:
    return _bucket(value, [(8.0, "score<8"), (12.0, "8-11.99"), (16.0, "12-15.99"), (999.0, "16+")])


def _direction_score_bucket(value: Any) -> str:
    val = abs(_safe_float(value, math.nan))
    if math.isnan(val):
        return "unknown"
    if val < 0.60:
        return "abs_dir<0.60"
    if val < 0.80:
        return "0.60-0.79"
    if val < 1.00:
        return "0.80-0.99"
    return "1.00"


def _roc_bucket(value: Any) -> str:
    val = abs(_safe_float(value, math.nan))
    if math.isnan(val):
        return "unknown"
    if val < 0.05:
        return "abs_roc<0.05"
    if val < 0.15:
        return "0.05-0.14"
    if val < 0.30:
        return "0.15-0.29"
    return "0.30+"


def _aggregate(rows: list[dict[str, Any]], key_fn) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = defaultdict(lambda: {"count": 0, "score": 0, "moves": [], "good": 0, "bad": 0, "neutral": 0})
    for row in rows:
        key = str(key_fn(row))
        verdict = str(row.get("verdict", ""))
        grouped[key]["count"] += 1
        grouped[key]["score"] += _safe_int(row.get("score_delta"), 0)
        grouped[key]["moves"].append(_safe_float(row.get("directional_move_pct"), 0.0))
        if verdict in {"good_go", "acceptable_go", "good_block"}:
            grouped[key]["good"] += 1
        elif verdict in {"bad_go", "bad_block", "questionable_block"}:
            grouped[key]["bad"] += 1
        else:
            grouped[key]["neutral"] += 1

    results = []
    for key, payload in grouped.items():
        moves = payload.pop("moves", [])
        count = max(1, int(payload["count"]))
        results.append(
            {
                "key": key,
                "count": payload["count"],
                "score": payload["score"],
                "avg_directional_move_pct": round(sum(moves) / len(moves), 4) if moves else 0.0,
                "good_rate": round(payload["good"] / count, 4),
                "bad_rate": round(payload["bad"] / count, 4),
                "neutral_rate": round(payload["neutral"] / count, 4),
            }
        )
    results.sort(key=lambda item: (int(item["score"]), -int(item["count"])))
    return results


def _recommendations(rows: list[dict[str, Any]], aggregates: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    recs: list[dict[str, Any]] = []
    verdict_counts = Counter(str(row.get("verdict", "")) for row in rows)
    total = max(1, len(rows))
    neutral_rate = (verdict_counts.get("neutral_go", 0) + verdict_counts.get("neutral_block", 0) + verdict_counts.get("neutral", 0)) / total

    if total < 100:
        recs.append(
            {
                "priority": "high",
                "type": "collect_more_data",
                "message": f"Only {total} persisted evaluated decisions exist. Do not auto-tune yet; collect at least 100-300 rows.",
                "action": "keep learning memory enabled and review after more market sessions",
            }
        )

    if neutral_rate > 0.60:
        recs.append(
            {
                "priority": "high",
                "type": "reduce_neutral_approvals",
                "message": "Most decisions are neutral. The bot is often choosing the right direction but without enough follow-through.",
                "action": "prefer changes that increase follow-through quality, not changes that increase trade count",
            }
        )

    for item in aggregates.get("by_rvol_bucket", [])[:3]:
        if int(item.get("count", 0)) >= 10 and int(item.get("score", 0)) < 0:
            recs.append(
                {
                    "priority": "medium",
                    "type": "rvol_bucket_underperforming",
                    "message": f"RVOL bucket {item['key']} has negative score {item['score']} across {item['count']} rows.",
                    "action": "consider raising RVOL requirement only for patterns in this bucket after enough samples",
                }
            )

    for item in aggregates.get("by_symbol", [])[:5]:
        if int(item.get("count", 0)) >= 5 and int(item.get("score", 0)) < 0:
            recs.append(
                {
                    "priority": "medium",
                    "type": "symbol_underperforming",
                    "message": f"{item['key']} has negative learned score {item['score']} across {item['count']} evaluated rows.",
                    "action": "consider symbol-specific caution or watchlist downgrade after more samples",
                }
            )

    vix_no_contracts = [row for row in rows if row.get("source") == "vix_proxy" and "no VIXY" in str(row.get("reason", ""))]
    if len(vix_no_contracts) >= 3:
        recs.append(
            {
                "priority": "high",
                "type": "vix_proxy_contract_window_problem",
                "message": f"VIX proxy repeatedly could not find VIXY contracts in the current DTE window ({len(vix_no_contracts)} persisted cases).",
                "action": "expand the VIXY DTE window or add VXX/UVXY fallback before expecting volatility-proxy trades",
            }
        )

    if not recs:
        recs.append(
            {
                "priority": "normal",
                "type": "no_action_yet",
                "message": "No strong learning signal yet.",
                "action": "keep collecting outcomes before tuning",
            }
        )
    return recs


def _learning_quality(
    rows: list[dict[str, Any]],
    verdict_counts: Counter[str],
    score_total: int,
    aggregates: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    total = max(1, len(rows))
    good = (
        int(verdict_counts.get("good_go", 0))
        + int(verdict_counts.get("acceptable_go", 0))
        + int(verdict_counts.get("good_block", 0))
    )
    bad = (
        int(verdict_counts.get("bad_go", 0))
        + int(verdict_counts.get("bad_block", 0))
        + int(verdict_counts.get("questionable_block", 0))
    )
    neutral = max(0, total - good - bad)
    good_rate = good / total
    bad_rate = bad / total
    neutral_rate = neutral / total

    worst_rvol = None
    for item in aggregates.get("by_rvol_bucket", []):
        if int(item.get("count", 0)) < 100:
            continue
        if worst_rvol is None or int(item.get("score", 0)) < int(worst_rvol.get("score", 0)):
            worst_rvol = item

    flags: list[str] = []
    if len(rows) < 300:
        flags.append("sample_size_low")
    if score_total < 0:
        flags.append("net_score_negative")
    if bad_rate > good_rate:
        flags.append("bad_rate_above_good_rate")
    if neutral_rate > 0.55:
        flags.append("too_many_neutral_outcomes")
    if isinstance(worst_rvol, dict) and int(worst_rvol.get("score", 0)) < -50:
        flags.append(f"weak_rvol_bucket:{worst_rvol.get('key')}")

    if len(flags) >= 3:
        verdict = "bad"
    elif len(flags) >= 1:
        verdict = "warning"
    else:
        verdict = "good"

    return {
        "verdict": verdict,
        "flags": flags,
        "sample_size": len(rows),
        "score_total": int(score_total),
        "good_rate": round(good_rate, 4),
        "bad_rate": round(bad_rate, 4),
        "neutral_rate": round(neutral_rate, 4),
        "good_count": int(good),
        "bad_count": int(bad),
        "neutral_count": int(neutral),
        "worst_rvol_bucket": worst_rvol or {},
    }


def build_learning_summary() -> dict[str, Any]:
    path = Path(memory_paths()["memory_csv"])
    rows_by_id = _read_memory(path)
    rows = list(rows_by_id.values())

    verdict_counts = Counter(str(row.get("verdict", "")) for row in rows)
    score_total = sum(_safe_int(row.get("score_delta"), 0) for row in rows)
    aggregates = {
        "by_source_stage_decision": _aggregate(rows, lambda row: f"{row.get('source')}::{row.get('stage')}::{row.get('decision')}"),
        "by_symbol": _aggregate(rows, lambda row: str(row.get("symbol", ""))),
        "by_direction": _aggregate(rows, lambda row: str(row.get("direction", ""))),
        "by_rvol_bucket": _aggregate(rows, lambda row: _rvol_bucket(row.get("rvol"))),
        "by_signal_score_bucket": _aggregate(rows, lambda row: _score_bucket(row.get("signal_score"))),
        "by_direction_score_bucket": _aggregate(rows, lambda row: _direction_score_bucket(row.get("direction_score"))),
        "by_roc_bucket": _aggregate(rows, lambda row: _roc_bucket(row.get("roc"))),
    }

    summary = {
        "generated_at_et": _now_et().isoformat(),
        "ok": True,
        "persistence": {
            **memory_paths(),
            "data_dir_is_persistent_candidate": str(memory_paths()["data_dir"]).startswith("/data"),
            "note": "Render must mount a persistent disk at /data and DATA_DIR must be /data for memory to survive rebuilds.",
        },
        "totals": {
            "persisted_decisions": len(rows),
            "score_total": score_total,
            "verdict_counts": dict(verdict_counts),
        },
        "learning_quality": _learning_quality(rows, verdict_counts, score_total, aggregates),
        "aggregates": aggregates,
        "recommendations": _recommendations(rows, aggregates),
    }
    return summary


def update_decision_memory(*, journal_limit: int = 300, horizons: tuple[int, ...] = (15, 30, 60)) -> dict[str, Any]:
    path = Path(memory_paths()["memory_csv"])
    rows_by_id = _read_memory(path)
    before = len(rows_by_id)
    now = _now_et().isoformat()
    evaluated_count = 0

    for horizon in horizons:
        try:
            payload = build_decision_outcomes(journal_limit=journal_limit, horizon_minutes=int(horizon))
        except Exception as exc:  # noqa: BLE001
            print(f"[decision_memory] outcome build failed horizon={horizon}: {exc}")
            continue
        for outcome in payload.get("outcomes", []):
            if not isinstance(outcome, dict) or not bool(outcome.get("evaluated", False)):
                continue
            evaluated_count += 1
            flat = _flatten_outcome(outcome)
            did = str(flat.get("decision_id", ""))
            if not did:
                continue
            existing = rows_by_id.get(did)
            if existing:
                existing["last_seen_et"] = now
                existing["seen_count"] = str(_safe_int(existing.get("seen_count"), 1) + 1)
                # Preserve the latest verdict for the longest horizon seen.
                if _safe_int(flat.get("horizon_minutes"), 0) >= _safe_int(existing.get("horizon_minutes"), 0):
                    for key in MEMORY_COLUMNS:
                        if key not in {"first_seen_et", "seen_count"}:
                            existing[key] = flat.get(key, existing.get(key, ""))
                    existing["last_seen_et"] = now
                rows_by_id[did] = existing
            else:
                rows_by_id[did] = flat

    _write_memory(path, rows_by_id)
    summary = build_learning_summary()
    summary_path = Path(memory_paths()["summary_json"])
    try:
        summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    except Exception as exc:  # noqa: BLE001
        print(f"[decision_memory] summary write failed: {exc}")

    after = len(rows_by_id)
    return {
        "generated_at_et": now,
        "ok": True,
        "evaluated_rows_seen_this_run": evaluated_count,
        "new_rows_added": after - before,
        "persisted_rows_total": after,
        "paths": memory_paths(),
        "summary": summary,
    }


def run_learning_memory_forever() -> None:
    interval = max(60, int(getattr(config, "DECISION_MEMORY_UPDATE_SECONDS", 300) or 300))
    print(f"[decision_memory] worker started interval={interval}s")
    while True:
        try:
            result = update_decision_memory()
            print(
                "[decision_memory] update ok "
                f"new={result.get('new_rows_added')} total={result.get('persisted_rows_total')}"
            )
        except Exception as exc:  # noqa: BLE001
            print(f"[decision_memory] worker update failed: {exc}")
        time.sleep(interval)


if __name__ == "__main__":
    print(json.dumps(update_decision_memory(), indent=2))
