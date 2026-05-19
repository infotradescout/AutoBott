"""Replay-trained directional edge model for live entry gating."""

from __future__ import annotations

import csv
import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    import config
except ImportError:  # pragma: no cover
    from autotrader import config  # type: ignore


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return float(default)
    if math.isnan(parsed) or math.isinf(parsed):
        return float(default)
    return parsed


def _bucket_direction(value: Any) -> str:
    score = abs(_safe_float(value))
    if score < 0.40:
        return "weak"
    if score < 0.65:
        return "mixed"
    if score < 0.85:
        return "strong"
    return "elite"


def _bucket_rvol(value: Any) -> str:
    rvol = _safe_float(value)
    if rvol < 0.75:
        return "dead"
    if rvol < 1.25:
        return "normal"
    if rvol < 2.50:
        return "active"
    return "surge"


def _bucket_roc(value: Any) -> str:
    roc = abs(_safe_float(value))
    if roc < 0.08:
        return "flat"
    if roc < 0.20:
        return "moving"
    if roc < 0.50:
        return "trend"
    return "impulse"


def _entry_hour(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return str(int(parsed.hour))
    except ValueError:
        pass
    if len(raw) >= 13 and raw[11:13].isdigit():
        return str(int(raw[11:13]))
    return ""


def _feature_payload(signal: dict, *, ticker: str, direction: str, now_et: datetime | None = None) -> dict[str, str]:
    hour = str(int(now_et.hour)) if now_et is not None else _entry_hour(signal.get("timestamp"))
    return {
        "symbol": str(ticker or signal.get("symbol", "") or "").upper(),
        "direction": str(direction or signal.get("direction", "") or "").lower(),
        "hour": hour,
        "direction_bucket": _bucket_direction(signal.get("direction_score")),
        "rvol_bucket": _bucket_rvol(signal.get("rvol")),
        "roc_bucket": _bucket_roc(signal.get("roc")),
        "profile": str(signal.get("strategy_profile", "") or "generic").lower(),
    }


def _keys(features: dict[str, str]) -> list[str]:
    return [
        "|".join(
            [
                "symbol_hour",
                features["symbol"],
                features["direction"],
                features["hour"],
                features["direction_bucket"],
                features["rvol_bucket"],
                features["roc_bucket"],
            ]
        ),
        "|".join(
            [
                "profile_hour",
                features["profile"],
                features["direction"],
                features["hour"],
                features["direction_bucket"],
                features["rvol_bucket"],
                features["roc_bucket"],
            ]
        ),
        "|".join(
            [
                "directional",
                features["direction"],
                features["direction_bucket"],
                features["rvol_bucket"],
                features["roc_bucket"],
            ]
        ),
    ]


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    try:
        with path.open("r", newline="", encoding="utf-8") as handle:
            return list(csv.DictReader(handle))
    except Exception:
        return []


def build_model_from_replay_outputs(*, replay_csv_paths: list[Path], output_path: Path | None = None) -> dict[str, Any]:
    groups: dict[str, dict[str, Any]] = {}
    rows_seen = 0
    rows_used = 0
    for path in replay_csv_paths:
        for row in _read_csv(path):
            rows_seen += 1
            verdict = str(row.get("verdict", "") or "").lower()
            if verdict not in {"win", "loss"}:
                continue
            features = _feature_payload(
                row,
                ticker=str(row.get("symbol", "") or ""),
                direction=str(row.get("direction", "") or ""),
            )
            move = _safe_float(row.get("directional_move_pct"))
            is_win = verdict == "win"
            rows_used += 1
            for key in _keys(features):
                item = groups.setdefault(key, {"n": 0, "wins": 0, "losses": 0, "move_sum": 0.0})
                item["n"] += 1
                item["wins"] += 1 if is_win else 0
                item["losses"] += 0 if is_win else 1
                item["move_sum"] += move
    finalized: dict[str, dict[str, Any]] = {}
    for key, item in groups.items():
        n = int(item["n"])
        if n <= 0:
            continue
        finalized[key] = {
            "n": n,
            "wins": int(item["wins"]),
            "losses": int(item["losses"]),
            "win_rate": round(float(item["wins"]) / n, 4),
            "avg_directional_move_pct": round(float(item["move_sum"]) / n, 4),
        }
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "source": "historical_replay",
        "rows_seen": rows_seen,
        "rows_used": rows_used,
        "groups": finalized,
    }
    target = output_path or getattr(config, "ENTRY_EDGE_MODEL_PATH", None)
    if target is not None:
        target = Path(target)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return payload


def build_model_from_optimizer_rows(*, optimizer_rows: list[dict[str, Any]], output_path: Path | None = None) -> dict[str, Any]:
    paths: list[Path] = []
    for row in optimizer_rows:
        raw = str(row.get("output", "") or "").strip()
        if raw:
            paths.append(Path(raw))
    unique_paths = list(dict.fromkeys(paths))
    return build_model_from_replay_outputs(replay_csv_paths=unique_paths, output_path=output_path)


def _load_model(path: Path | None = None) -> dict[str, Any]:
    target = path or getattr(config, "ENTRY_EDGE_MODEL_PATH", None)
    if target is None:
        return {}
    try:
        return json.loads(Path(target).read_text(encoding="utf-8"))
    except Exception:
        return {}


def evaluate_signal(
    *,
    signal: dict,
    ticker: str,
    direction: str,
    now_et: datetime,
    model: dict[str, Any] | None = None,
) -> tuple[bool, str]:
    if not bool(getattr(config, "ENABLE_REPLAY_EDGE_MODEL_GATE", False)):
        return True, ""
    payload = model if isinstance(model, dict) else _load_model()
    groups = payload.get("groups", {}) if isinstance(payload, dict) else {}
    if not isinstance(groups, dict) or not groups:
        return True, ""
    min_samples = max(1, int(getattr(config, "REPLAY_EDGE_MODEL_MIN_SAMPLES", 20) or 20))
    min_win_rate = float(getattr(config, "REPLAY_EDGE_MODEL_MIN_WIN_RATE", 0.56) or 0.56)
    min_avg_move = float(getattr(config, "REPLAY_EDGE_MODEL_MIN_AVG_MOVE_PCT", 0.03) or 0.03)
    features = _feature_payload(signal, ticker=ticker, direction=direction, now_et=now_et)
    for key in _keys(features):
        item = groups.get(key)
        if not isinstance(item, dict):
            continue
        n = int(item.get("n", 0) or 0)
        if n < min_samples:
            continue
        win_rate = _safe_float(item.get("win_rate"))
        avg_move = _safe_float(item.get("avg_directional_move_pct"))
        if win_rate >= min_win_rate and avg_move >= min_avg_move:
            return True, f"replay edge accepted {key}: n={n} winrate={win_rate:.0%} avg_move={avg_move:.3f}%"
        return (
            False,
            f"replay edge rejected {key}: n={n} winrate={win_rate:.0%} avg_move={avg_move:.3f}% "
            f"required winrate>={min_win_rate:.0%} avg_move>={min_avg_move:.3f}%",
        )
    return True, "replay edge model has no mature bucket; fail-open"
