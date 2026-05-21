"""Auto-tune direction/TP/SL from synthetic trainer outcomes."""

from __future__ import annotations

import argparse
import csv
import json
import math
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pytz

import config

EASTERN = pytz.timezone(config.EASTERN_TZ)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return float(default)
    if math.isnan(parsed) or math.isinf(parsed):
        return float(default)
    return parsed


def _read_rows(path: Path, limit: int) -> list[dict[str, str]]:
    if not path.exists():
        return []
    try:
        with path.open("r", newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
    except Exception:
        return []
    if limit > 0 and len(rows) > limit:
        return rows[-limit:]
    return rows


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _build_overrides(rows: list[dict[str, str]]) -> tuple[dict[str, float], dict[str, Any]]:
    evaluated = [r for r in rows if str(r.get("evaluated", "")).lower() in {"true", "1"}]
    wins = [r for r in evaluated if str(r.get("verdict", "")).lower() == "win"]
    losses = [r for r in evaluated if str(r.get("verdict", "")).lower() == "loss"]
    total_eval = len(evaluated)
    win_rate = (len(wins) / total_eval) if total_eval > 0 else 0.0
    avg_move = sum(_safe_float(r.get("directional_move_pct")) for r in evaluated) / max(1, total_eval)
    avg_score = sum(_safe_float(r.get("signal_score")) for r in evaluated) / max(1, total_eval)
    avg_dir = sum(abs(_safe_float(r.get("direction_score"))) for r in evaluated) / max(1, total_eval)
    avg_rvol = sum(_safe_float(r.get("rvol")) for r in evaluated) / max(1, total_eval)

    min_signal = float(getattr(config, "MIN_SIGNAL_SCORE", 2.5) or 2.5)
    min_dir_conv = float(getattr(config, "DIRECTION_CONVICTION_MIN", 0.15) or 0.15)
    min_rvol = float(getattr(config, "RVOL_MIN", 0.05) or 0.05)
    stop_loss_pct = float(getattr(config, "STOP_LOSS_PCT", 0.12) or 0.12)
    take_profit_pct = float(getattr(config, "IMMEDIATE_TAKE_PROFIT_PCT", 0.35) or 0.35)
    trail_pullback_pct = float(getattr(config, "TRAIL_PULLBACK_PCT", 0.08) or 0.08)

    if total_eval < 200:
        # Warmup: keep permissive so we keep collecting signal/outcome data.
        tuned = {
            "MIN_SIGNAL_SCORE": _clamp(min_signal, 2.0, 4.0),
            "DIRECTION_CONVICTION_MIN": _clamp(min_dir_conv, 0.10, 0.35),
            "RVOL_MIN": _clamp(min_rvol, 0.05, 0.35),
            "STOP_LOSS_PCT": _clamp(stop_loss_pct, 0.10, 0.18),
            "IMMEDIATE_TAKE_PROFIT_PCT": _clamp(take_profit_pct, 0.25, 0.45),
            "TRAIL_PULLBACK_PCT": _clamp(trail_pullback_pct, 0.06, 0.12),
        }
    elif win_rate < 0.47:
        # Losing too often: raise directional quality, tighten stop, quicker profit.
        tuned = {
            "MIN_SIGNAL_SCORE": _clamp(max(min_signal, avg_score + 0.4), 2.5, 9.5),
            "DIRECTION_CONVICTION_MIN": _clamp(max(min_dir_conv, avg_dir + 0.08), 0.15, 0.90),
            "RVOL_MIN": _clamp(max(min_rvol, avg_rvol + 0.10), 0.05, 2.0),
            "STOP_LOSS_PCT": _clamp(stop_loss_pct - 0.01, 0.06, 0.20),
            "IMMEDIATE_TAKE_PROFIT_PCT": _clamp(take_profit_pct - 0.02, 0.20, 0.60),
            "TRAIL_PULLBACK_PCT": _clamp(trail_pullback_pct - 0.005, 0.03, 0.20),
        }
    elif win_rate > 0.56 and avg_move > 0.04:
        # Winning with follow-through: allow runner room.
        tuned = {
            "MIN_SIGNAL_SCORE": _clamp(min_signal - 0.10, 2.0, 9.5),
            "DIRECTION_CONVICTION_MIN": _clamp(min_dir_conv - 0.02, 0.10, 0.90),
            "RVOL_MIN": _clamp(min_rvol - 0.03, 0.05, 2.0),
            "STOP_LOSS_PCT": _clamp(stop_loss_pct + 0.005, 0.06, 0.20),
            "IMMEDIATE_TAKE_PROFIT_PCT": _clamp(take_profit_pct + 0.02, 0.20, 0.60),
            "TRAIL_PULLBACK_PCT": _clamp(trail_pullback_pct + 0.005, 0.03, 0.20),
        }
    else:
        # Mid-zone: hold mostly steady, tiny bias toward observed directional quality.
        tuned = {
            "MIN_SIGNAL_SCORE": _clamp((min_signal * 0.85) + (avg_score * 0.15), 2.0, 9.5),
            "DIRECTION_CONVICTION_MIN": _clamp((min_dir_conv * 0.85) + (avg_dir * 0.15), 0.10, 0.90),
            "RVOL_MIN": _clamp((min_rvol * 0.85) + (avg_rvol * 0.15), 0.05, 2.0),
            "STOP_LOSS_PCT": _clamp(stop_loss_pct, 0.06, 0.20),
            "IMMEDIATE_TAKE_PROFIT_PCT": _clamp(take_profit_pct, 0.20, 0.60),
            "TRAIL_PULLBACK_PCT": _clamp(trail_pullback_pct, 0.03, 0.20),
        }

    metrics = {
        "rows_considered": len(rows),
        "evaluated": total_eval,
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": round(win_rate * 100.0, 2),
        "avg_directional_move_pct": round(avg_move, 4),
        "avg_signal_score": round(avg_score, 4),
        "avg_abs_direction_score": round(avg_dir, 4),
        "avg_rvol": round(avg_rvol, 4),
    }
    return tuned, metrics


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _tick(trades_path: Path, overrides_path: Path, status_path: Path, window: int) -> None:
    rows = _read_rows(trades_path, window)
    overrides, metrics = _build_overrides(rows)
    now = datetime.now(EASTERN).isoformat()
    _write_json(
        overrides_path,
        {
            "updated_at_et": now,
            "source": "synthetic_auto_tuner",
            "window_rows": max(0, int(window)),
            "overrides": overrides,
            "metrics": metrics,
        },
    )
    _write_json(
        status_path,
        {
            "running": True,
            "updated_at_et": now,
            "trades_path": str(trades_path),
            "overrides_path": str(overrides_path),
            "metrics": metrics,
            "overrides": overrides,
        },
    )
    print(
        f"[synthetic_tuner] applied overrides "
        f"(wr={metrics['win_rate_pct']}% eval={metrics['evaluated']}): {overrides}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Auto-tune runtime entry params from synthetic outcomes.")
    parser.add_argument("--trades", default=str(getattr(config, "SYNTHETIC_TRADES_CSV_PATH", Path(config.DATA_DIR) / "synthetic_trades.csv")))
    parser.add_argument("--overrides", default=str(getattr(config, "SYNTHETIC_TUNER_OVERRIDES_PATH", Path(config.DATA_DIR) / "synthetic_tuner_overrides.json")))
    parser.add_argument("--status", default=str(getattr(config, "SYNTHETIC_TUNER_STATUS_PATH", Path(config.DATA_DIR) / "synthetic_tuner_status.json")))
    parser.add_argument("--window", type=int, default=5000)
    parser.add_argument("--interval-seconds", type=float, default=30.0)
    parser.add_argument("--loop-forever", action=argparse.BooleanOptionalAction, default=True)
    args = parser.parse_args()

    trades_path = Path(args.trades)
    overrides_path = Path(args.overrides)
    status_path = Path(args.status)
    window = max(200, int(args.window))

    while True:
        _tick(trades_path, overrides_path, status_path, window)
        if not bool(args.loop_forever):
            return
        time.sleep(max(5.0, float(args.interval_seconds)))


if __name__ == "__main__":
    main()

