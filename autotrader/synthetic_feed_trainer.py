"""24/7 synthetic paper-trading trainer.

Runs the scanner against synthetic intraday bars so the strategy can keep
learning direction/TP/SL behavior without Alpaca execution dependency.
"""

from __future__ import annotations

import argparse
import csv
import math
import random
import time
from dataclasses import dataclass
from datetime import datetime, time as dtime, timedelta
from pathlib import Path
from typing import Any
import json

import pandas as pd
import pytz

import config
from historical_replay import HistoricalReplayDataClient
from scanner import IntradayScanner

EASTERN = pytz.timezone(config.EASTERN_TZ)


@dataclass(frozen=True)
class SyntheticConfig:
    symbols: list[str]
    sessions: int
    bars_per_session: int
    bar_minutes: int
    scan_every_bars: int
    horizon_bars: int
    take_profit_pct: float
    stop_loss_pct: float
    base_price: float
    drift_pct_per_bar: float
    vol_pct_per_bar: float
    output: Path
    summary: Path
    status: Path
    sleep_seconds: float
    loop_forever: bool


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return float(default)
    if math.isnan(parsed) or math.isinf(parsed):
        return float(default)
    return parsed


def _session_start(day_offset: int) -> datetime:
    now = datetime.now(EASTERN).replace(hour=9, minute=30, second=0, microsecond=0)
    return now - timedelta(days=day_offset)


def _generate_symbol_bars(symbol: str, cfg: SyntheticConfig) -> pd.DataFrame:
    rng = random.Random(f"{symbol}:{datetime.now().date().isoformat()}")
    rows: list[dict[str, Any]] = []
    last_close = max(5.0, cfg.base_price * (0.8 + rng.random() * 0.4))
    for s in range(cfg.sessions):
        start = _session_start(cfg.sessions - s)
        regime = rng.choice([-1.0, -0.5, 0.0, 0.5, 1.0])
        for b in range(cfg.bars_per_session):
            ts = start + timedelta(minutes=b * cfg.bar_minutes)
            noise = rng.gauss(0.0, cfg.vol_pct_per_bar)
            step_pct = (cfg.drift_pct_per_bar * regime) + noise
            close = max(1.0, last_close * (1.0 + (step_pct / 100.0)))
            high = max(last_close, close) * (1.0 + abs(rng.gauss(0.0, cfg.vol_pct_per_bar / 5.0)) / 100.0)
            low = min(last_close, close) * (1.0 - abs(rng.gauss(0.0, cfg.vol_pct_per_bar / 5.0)) / 100.0)
            rows.append(
                {
                    "timestamp": ts,
                    "open": round(last_close, 4),
                    "high": round(max(high, close, last_close), 4),
                    "low": round(min(low, close, last_close), 4),
                    "close": round(close, 4),
                    "volume": float(max(1000, int(25000 + rng.gauss(0, 9000)))),
                }
            )
            last_close = close
    return pd.DataFrame(rows)


def _simulate_outcome(
    *,
    bars: pd.DataFrame,
    entry_idx: int,
    direction: str,
    horizon_bars: int,
    take_profit_pct: float,
    stop_loss_pct: float,
) -> dict[str, Any]:
    future = bars.iloc[entry_idx + 1 : entry_idx + 1 + horizon_bars].copy()
    if future.empty:
        return {"evaluated": False, "verdict": "no_future_bars"}
    entry = float(bars.iloc[entry_idx]["close"])
    if entry <= 0:
        return {"evaluated": False, "verdict": "invalid_entry"}
    max_fav = 0.0
    max_adv = 0.0
    verdict = "timeout"
    exit_price = float(future.iloc[-1]["close"])
    for _, row in future.iterrows():
        high = float(row["high"])
        low = float(row["low"])
        if direction == "call":
            fav = ((high - entry) / entry) * 100.0
            adv = ((low - entry) / entry) * 100.0
            tp_hit = fav >= take_profit_pct
            sl_hit = adv <= -abs(stop_loss_pct)
        else:
            fav = ((entry - low) / entry) * 100.0
            adv = ((entry - high) / entry) * 100.0
            tp_hit = fav >= take_profit_pct
            sl_hit = adv <= -abs(stop_loss_pct)
        max_fav = max(max_fav, fav)
        max_adv = min(max_adv, adv)
        if tp_hit:
            verdict = "win"
            exit_price = entry * (1.0 + take_profit_pct / 100.0) if direction == "call" else entry * (1.0 - take_profit_pct / 100.0)
            break
        if sl_hit:
            verdict = "loss"
            exit_price = entry * (1.0 - stop_loss_pct / 100.0) if direction == "call" else entry * (1.0 + stop_loss_pct / 100.0)
            break
    directional_move = ((exit_price - entry) / entry) * 100.0
    if direction == "put":
        directional_move = -directional_move
    if verdict == "timeout":
        verdict = "win" if directional_move > 0 else "loss" if directional_move < 0 else "flat"
    return {
        "evaluated": True,
        "verdict": verdict,
        "directional_move_pct": round(directional_move, 4),
        "max_favorable_pct": round(max_fav, 4),
        "max_adverse_pct": round(max_adv, 4),
    }


def _fallback_signal_from_bars(symbol: str, bars: pd.DataFrame, now_et: datetime) -> dict[str, Any] | None:
    row_idx = bars.index[bars["timestamp"] == now_et]
    if len(row_idx) == 0:
        return None
    idx = int(row_idx[0])
    if idx < 3:
        return None
    recent = bars.iloc[max(0, idx - 3) : idx + 1]["close"].astype(float)
    if len(recent) < 2:
        return None
    start = float(recent.iloc[0])
    end = float(recent.iloc[-1])
    if start <= 0:
        return None
    roc = ((end - start) / start) * 100.0
    direction = "call" if roc >= 0 else "put"
    direction_score = min(1.0, max(0.0, abs(roc) / 0.25))
    return {
        "symbol": symbol,
        "direction": direction,
        "signal_score": 5.0 + (direction_score * 5.0),
        "direction_score": direction_score if direction == "call" else -direction_score,
        "rvol": 1.0,
        "roc": roc,
        "reason": "synthetic fallback direction signal",
    }


def _write_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists()
    fieldnames = [
        "timestamp",
        "symbol",
        "direction",
        "signal_score",
        "direction_score",
        "rvol",
        "roc",
        "reason",
        "evaluated",
        "verdict",
        "directional_move_pct",
        "max_favorable_pct",
        "max_adverse_pct",
        "source",
    ]
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerows(rows)


def _summarize(path: Path, rows: list[dict[str, Any]]) -> None:
    evaluated = [r for r in rows if bool(r.get("evaluated"))]
    wins = [r for r in evaluated if r.get("verdict") == "win"]
    losses = [r for r in evaluated if r.get("verdict") == "loss"]
    payload = {
        "generated_at": datetime.now(EASTERN).isoformat(),
        "rows": len(rows),
        "evaluated": len(evaluated),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": round((len(wins) / max(1, len(evaluated))) * 100.0, 2),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(pd.Series(payload).to_json(indent=2), encoding="utf-8")


def _load_previous_totals(path: Path) -> tuple[int, int, int, int]:
    if not path.exists():
        return 0, 0, 0, 0
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return (
            int(payload.get("total_rows", 0) or 0),
            int(payload.get("total_evaluated", 0) or 0),
            int(payload.get("total_wins", 0) or 0),
            int(payload.get("total_losses", 0) or 0),
        )
    except Exception:
        return 0, 0, 0, 0


def _write_status(
    *,
    path: Path,
    output_path: Path,
    summary_path: Path,
    pass_rows: list[dict[str, Any]],
    total_rows: int,
    total_evaluated: int,
    total_wins: int,
    total_losses: int,
) -> None:
    pass_evaluated = [r for r in pass_rows if bool(r.get("evaluated"))]
    pass_wins = [r for r in pass_evaluated if str(r.get("verdict", "")).lower() == "win"]
    pass_losses = [r for r in pass_evaluated if str(r.get("verdict", "")).lower() == "loss"]
    recent = pass_rows[-20:]
    payload = {
        "running": True,
        "updated_at_et": datetime.now(EASTERN).isoformat(),
        "output_csv": str(output_path),
        "summary_json": str(summary_path),
        "pass_rows": len(pass_rows),
        "pass_evaluated": len(pass_evaluated),
        "pass_wins": len(pass_wins),
        "pass_losses": len(pass_losses),
        "pass_win_rate_pct": round((len(pass_wins) / max(1, len(pass_evaluated))) * 100.0, 2),
        "total_rows": total_rows,
        "total_evaluated": total_evaluated,
        "total_wins": total_wins,
        "total_losses": total_losses,
        "total_win_rate_pct": round((total_wins / max(1, total_evaluated)) * 100.0, 2),
        "recent_trades": [
            {
                "timestamp": str(item.get("timestamp", "") or ""),
                "symbol": str(item.get("symbol", "") or ""),
                "direction": str(item.get("direction", "") or ""),
                "verdict": str(item.get("verdict", "") or ""),
                "directional_move_pct": _safe_float(item.get("directional_move_pct")),
                "signal_score": _safe_float(item.get("signal_score")),
                "direction_score": _safe_float(item.get("direction_score")),
                "rvol": _safe_float(item.get("rvol")),
                "roc": _safe_float(item.get("roc")),
            }
            for item in recent
        ],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _one_pass(cfg: SyntheticConfig) -> tuple[list[dict[str, Any]], dict[str, pd.DataFrame]]:
    bars_by_symbol = {symbol: _generate_symbol_bars(symbol, cfg) for symbol in cfg.symbols}
    daily_bars = {}
    for symbol, bars in bars_by_symbol.items():
        daily = (
            bars.set_index("timestamp")
            .resample("1D")
            .agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
            .dropna()
            .reset_index()
        )
        daily_bars[symbol] = daily
    data_client = HistoricalReplayDataClient(bars_by_symbol, daily_bars)
    scanner = IntradayScanner(data_client, emit_summary=False, write_scan_log=False)  # type: ignore[arg-type]
    timestamps = sorted({ts for df in bars_by_symbol.values() for ts in df["timestamp"].tolist()})
    out_rows: list[dict[str, Any]] = []
    for idx, now_et in enumerate(timestamps):
        if now_et.time() < dtime(9, 35):
            continue
        if idx % max(1, cfg.scan_every_bars) != 0:
            continue
        signals = scanner.run_scan(cfg.symbols, now_et=now_et)
        selected_signals = list(signals)
        if not selected_signals:
            for symbol in cfg.symbols:
                bars = bars_by_symbol.get(symbol)
                if bars is None or bars.empty:
                    continue
                fallback = _fallback_signal_from_bars(symbol, bars, now_et)
                if fallback:
                    selected_signals.append(fallback)
        for signal in selected_signals:
            symbol = str(signal.get("symbol", "") or "").upper()
            direction = str(signal.get("direction", "") or "").lower()
            if direction not in {"call", "put"}:
                continue
            bars = bars_by_symbol.get(symbol)
            if bars is None or bars.empty:
                continue
            bar_index = bars.index[bars["timestamp"] == now_et]
            if len(bar_index) == 0:
                continue
            outcome = _simulate_outcome(
                bars=bars,
                entry_idx=int(bar_index[0]),
                direction=direction,
                horizon_bars=cfg.horizon_bars,
                take_profit_pct=cfg.take_profit_pct,
                stop_loss_pct=cfg.stop_loss_pct,
            )
            out_rows.append(
                {
                    "timestamp": now_et.isoformat(),
                    "symbol": symbol,
                    "direction": direction,
                    "signal_score": _safe_float(signal.get("signal_score")),
                    "direction_score": _safe_float(signal.get("direction_score")),
                    "rvol": _safe_float(signal.get("rvol")),
                    "roc": _safe_float(signal.get("roc")),
                    "reason": str(signal.get("reason", "") or ""),
                    "evaluated": bool(outcome.get("evaluated", False)),
                    "verdict": str(outcome.get("verdict", "") or ""),
                    "directional_move_pct": _safe_float(outcome.get("directional_move_pct")),
                    "max_favorable_pct": _safe_float(outcome.get("max_favorable_pct")),
                    "max_adverse_pct": _safe_float(outcome.get("max_adverse_pct")),
                    "source": "synthetic_feed",
                }
            )
    return out_rows, bars_by_symbol


def run(cfg: SyntheticConfig) -> None:
    total_rows, total_evaluated, total_wins, total_losses = _load_previous_totals(cfg.status)
    while True:
        rows, _ = _one_pass(cfg)
        if rows:
            _write_rows(cfg.output, rows)
            _summarize(cfg.summary, rows)
            evaluated = [r for r in rows if bool(r.get("evaluated"))]
            wins = [r for r in evaluated if str(r.get("verdict", "")).lower() == "win"]
            losses = [r for r in evaluated if str(r.get("verdict", "")).lower() == "loss"]
            total_rows += len(rows)
            total_evaluated += len(evaluated)
            total_wins += len(wins)
            total_losses += len(losses)
            _write_status(
                path=cfg.status,
                output_path=cfg.output,
                summary_path=cfg.summary,
                pass_rows=rows,
                total_rows=total_rows,
                total_evaluated=total_evaluated,
                total_wins=total_wins,
                total_losses=total_losses,
            )
            print(
                f"[synthetic_feed] wrote {len(rows)} row(s) to {cfg.output} "
                f"(summary: {cfg.summary})"
            )
        else:
            print("[synthetic_feed] no rows generated in this pass.")
        if not cfg.loop_forever:
            return
        time.sleep(max(1.0, cfg.sleep_seconds))


def _parse_args() -> SyntheticConfig:
    parser = argparse.ArgumentParser(description="24/7 synthetic feed trainer for direction learning.")
    parser.add_argument("--symbols", default=",".join(config.CORE_TICKERS))
    parser.add_argument("--sessions", type=int, default=12)
    parser.add_argument("--bars-per-session", type=int, default=78)
    parser.add_argument("--bar-minutes", type=int, default=5)
    parser.add_argument("--scan-every-bars", type=int, default=1)
    parser.add_argument("--horizon-bars", type=int, default=6)
    parser.add_argument("--take-profit-pct", type=float, default=0.35)
    parser.add_argument("--stop-loss-pct", type=float, default=0.20)
    parser.add_argument("--base-price", type=float, default=120.0)
    parser.add_argument("--drift-pct-per-bar", type=float, default=0.02)
    parser.add_argument("--vol-pct-per-bar", type=float, default=0.22)
    parser.add_argument("--output", default=str(Path(config.DATA_DIR) / "synthetic_trades.csv"))
    parser.add_argument("--summary", default=str(Path(config.DATA_DIR) / "synthetic_trades_summary.json"))
    parser.add_argument("--status", default=str(Path(config.DATA_DIR) / "synthetic_trainer_status.json"))
    parser.add_argument("--sleep-seconds", type=float, default=15.0)
    parser.add_argument("--loop-forever", action=argparse.BooleanOptionalAction, default=True)
    args = parser.parse_args()
    symbols = [s.strip().upper() for s in str(args.symbols).split(",") if s.strip()]
    return SyntheticConfig(
        symbols=symbols or list(config.CORE_TICKERS),
        sessions=max(2, int(args.sessions)),
        bars_per_session=max(30, int(args.bars_per_session)),
        bar_minutes=max(1, int(args.bar_minutes)),
        scan_every_bars=max(1, int(args.scan_every_bars)),
        horizon_bars=max(1, int(args.horizon_bars)),
        take_profit_pct=max(0.01, float(args.take_profit_pct)),
        stop_loss_pct=max(0.01, float(args.stop_loss_pct)),
        base_price=max(1.0, float(args.base_price)),
        drift_pct_per_bar=float(args.drift_pct_per_bar),
        vol_pct_per_bar=max(0.01, float(args.vol_pct_per_bar)),
        output=Path(args.output),
        summary=Path(args.summary),
        status=Path(args.status),
        sleep_seconds=max(1.0, float(args.sleep_seconds)),
        loop_forever=bool(args.loop_forever),
    )


if __name__ == "__main__":
    run(_parse_args())
