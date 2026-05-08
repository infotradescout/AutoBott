"""Continuous offline optimizer for the historical replay trainer.

This module repeatedly replays historical bars through the same scanner path
used live, sweeps conservative scanner/execution settings, and persists a
leaderboard. It never places orders.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import time
import os
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterator

import config
from data import AlpacaDataClient
from env_config import load_runtime_env
from historical_replay import ReplayConfig, run_replay


@dataclass(frozen=True)
class OptimizerCandidate:
    name: str
    overrides: dict[str, Any]
    horizon_minutes: int
    take_profit_pct: float
    stop_loss_pct: float
    scan_every_minutes: int
    max_signals_per_scan: int
    min_daily_bars: int


RESULT_COLUMNS = [
    "run_timestamp",
    "iteration",
    "candidate",
    "window_start",
    "window_end",
    "symbols",
    "interval",
    "horizon_minutes",
    "take_profit_pct",
    "stop_loss_pct",
    "scan_every_minutes",
    "max_signals_per_scan",
    "min_daily_bars",
    "scan_iterations",
    "scan_failures",
    "opportunities",
    "evaluated",
    "wins",
    "losses",
    "win_rate_pct",
    "expectancy_pct",
    "pass_target",
    "top_failures_json",
    "top_failure_details_json",
    "overrides_json",
    "output",
    "summary_path",
]


def _ratio_value(wins: int, losses: int) -> float:
    if losses <= 0:
        return float("inf")
    return round(wins / max(1, losses), 4)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return float(default)
    return parsed


def _parse_date(value: str) -> date:
    return datetime.fromisoformat(str(value)).date()


def _parse_cache_range_from_filename(path: Path) -> tuple[date, date] | None:
    match = re.match(
        r"^(?P<symbol>.+)_(?P<interval>[^_]+)_(?P<start>\d{4}-\d{2}-\d{2})_(?P<end>\d{4}-\d{2}-\d{2})\.csv$",
        path.name,
    )
    if not match:
        return None
    try:
        return datetime.fromisoformat(match.group("start")).date(), datetime.fromisoformat(match.group("end")).date()
    except ValueError:
        return None


def _latest_cache_end_for_symbol(cache_dir: Path, symbol: str, interval: str) -> date | None:
    symbol_prefix = f"{symbol.upper()}_{interval}_"
    best: date | None = None
    for path in cache_dir.glob(f"{symbol_prefix}*.csv"):
        parsed = _parse_cache_range_from_filename(path)
        if not parsed:
            continue
        _, file_end = parsed
        if best is None or file_end > best:
            best = file_end
    return best


def _has_cached_window(
    cache_dir: Path,
    symbol: str,
    interval: str,
    start: date,
    end: date,
) -> bool:
    symbol_prefix = f"{symbol.upper()}_{interval}_"
    for path in cache_dir.glob(f"{symbol_prefix}*.csv"):
        parsed = _parse_cache_range_from_filename(path)
        if not parsed:
            continue
        file_start, file_end = parsed
        if file_start <= start and file_end >= end:
            return True
    return False


def _latest_cached_end_date(symbols: list[str], interval: str, cache_dir: Path) -> date | None:
    if not symbols:
        return None
    ends: list[date] = []
    for symbol in symbols:
        end = _latest_cache_end_for_symbol(cache_dir, symbol, interval)
        if end is None:
            return None
        ends.append(end)
    return min(ends)


def _next_window_dates(
    current_start: date,
    current_end: date,
    args: argparse.Namespace,
    symbols: list[str],
    cache_dir: Path,
    *,
    offline: bool = True,
) -> tuple[date, date] | None:
    policy = str(getattr(args, "rolling_end_policy", "fixed")).strip().lower()
    if policy not in {"fixed", "today", "cache"}:
        raise ValueError(f"Unsupported rolling_end_policy: {policy}")

    span = current_end - current_start
    if policy == "fixed":
        step = max(1, int(getattr(args, "rolling_step_days", 1)))
        return current_start + timedelta(days=step), current_end + timedelta(days=step)

    if policy == "today":
        candidate_end = datetime.now().date()
        if candidate_end <= current_end:
            return None
        candidate_start = candidate_end - span
        if offline:
            for symbol in symbols:
                if not _has_cached_window(
                    cache_dir,
                    symbol,
                    str(args.interval),
                    candidate_start,
                    candidate_end,
                ):
                    return None
        return candidate_start, candidate_end

    candidate_end = _latest_cached_end_date(symbols, str(args.interval), cache_dir)
    if candidate_end is None or candidate_end <= current_end:
        if offline:
            return None
        candidate_end = datetime.now().date()
        if candidate_end <= current_end:
            return None
    candidate_start = candidate_end - span
    if offline:
        for symbol in symbols:
            if not _has_cached_window(
                cache_dir,
                symbol,
                str(args.interval),
                candidate_start,
                candidate_end,
            ):
                return None
    return candidate_start, candidate_end


def _default_start_end() -> tuple[date, date]:
    end = datetime.now().date()
    return end - timedelta(days=21), end


def _window_ranges(start: date, end: date, *, window_days: int, step_days: int, max_windows: int) -> list[tuple[date, date]]:
    if start >= end:
        raise ValueError("--start must be before --end")
    windows: list[tuple[date, date]] = []
    cursor = start
    while cursor < end:
        window_end = min(end, cursor + timedelta(days=max(1, window_days)))
        if window_end > cursor:
            windows.append((cursor, window_end))
        cursor += timedelta(days=max(1, step_days))
        if max_windows > 0 and len(windows) >= max_windows:
            break
    return windows


def _candidate_grid() -> list[OptimizerCandidate]:
    base_score = float(getattr(config, "MIN_SIGNAL_SCORE", 7.6) or 7.6)
    base_direction = float(getattr(config, "DIRECTION_CONVICTION_MIN", 0.65) or 0.65)
    base_rvol = float(getattr(config, "RVOL_MIN", 0.9) or 0.9)
    base_atr = float(getattr(config, "ATR_PCT_MIN", 1.0) or 1.0)
    return [
        OptimizerCandidate(
            name="baseline",
            overrides={},
            horizon_minutes=45,
            take_profit_pct=0.35,
            stop_loss_pct=0.20,
            scan_every_minutes=5,
            max_signals_per_scan=2,
            min_daily_bars=8,
        ),
        OptimizerCandidate(
            name="quality_score",
            overrides={"MIN_SIGNAL_SCORE": round(base_score + 0.35, 2)},
            horizon_minutes=45,
            take_profit_pct=0.35,
            stop_loss_pct=0.20,
            scan_every_minutes=5,
            max_signals_per_scan=2,
            min_daily_bars=8,
        ),
        OptimizerCandidate(
            name="direction_strict",
            overrides={"DIRECTION_CONVICTION_MIN": round(min(0.95, base_direction + 0.10), 2)},
            horizon_minutes=45,
            take_profit_pct=0.35,
            stop_loss_pct=0.20,
            scan_every_minutes=5,
            max_signals_per_scan=2,
            min_daily_bars=8,
        ),
        OptimizerCandidate(
            name="fewer_trades",
            overrides={
                "MIN_SIGNAL_SCORE": round(base_score + 0.55, 2),
                "DIRECTION_CONVICTION_MIN": round(min(0.95, base_direction + 0.10), 2),
            },
            horizon_minutes=60,
            take_profit_pct=0.45,
            stop_loss_pct=0.25,
            scan_every_minutes=10,
            max_signals_per_scan=1,
            min_daily_bars=8,
        ),
        OptimizerCandidate(
            name="data_light",
            overrides={
                "ATR_PCT_MIN": round(max(0.45, base_atr - 0.35), 2),
                "RVOL_MIN": round(max(0.35, base_rvol - 0.20), 2),
            },
            horizon_minutes=45,
            take_profit_pct=0.35,
            stop_loss_pct=0.20,
            scan_every_minutes=5,
            max_signals_per_scan=2,
            min_daily_bars=5,
        ),
        OptimizerCandidate(
            name="liquid_index",
            overrides={
                "ATR_PCT_MIN": round(max(0.40, base_atr - 0.45), 2),
                "RVOL_MIN": round(max(0.35, base_rvol - 0.30), 2),
                "MIN_SIGNAL_SCORE": round(max(6.8, base_score - 0.30), 2),
            },
            horizon_minutes=45,
            take_profit_pct=0.30,
            stop_loss_pct=0.18,
            scan_every_minutes=5,
            max_signals_per_scan=2,
            min_daily_bars=5,
        ),
    ]


@contextmanager
def _temporary_config(overrides: dict[str, Any]) -> Iterator[None]:
    old_values = {key: getattr(config, key, None) for key in overrides.keys()}
    try:
        for key, value in overrides.items():
            setattr(config, key, value)
        yield
    finally:
        for key, value in old_values.items():
            setattr(config, key, value)


def _append_csv(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists()
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=RESULT_COLUMNS, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerow({field: row.get(field, "") for field in RESULT_COLUMNS})


def _read_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _expectancy_pct(summary: dict[str, Any], candidate: OptimizerCandidate) -> float:
    evaluated = max(1, int(summary.get("evaluated", 0) or 0))
    wins = int(summary.get("wins", 0) or 0)
    losses = int(summary.get("losses", 0) or 0)
    return round(((wins / evaluated) * candidate.take_profit_pct) - ((losses / evaluated) * abs(candidate.stop_loss_pct)), 4)


def _row_passes(
    row: dict[str, Any],
    *,
    min_trades: int,
    target_win_rate_pct: float,
    target_expectancy_pct: float,
    min_win_loss_ratio: float,
) -> bool:
    wins = int(row.get("wins", 0) or 0)
    losses = int(row.get("losses", 0) or 0)
    ratio = _ratio_value(wins, losses)
    return (
        int(row.get("evaluated", 0) or 0) >= min_trades
        and _safe_float(row.get("win_rate_pct")) >= target_win_rate_pct
        and _safe_float(row.get("expectancy_pct")) >= target_expectancy_pct
        and ratio >= min_win_loss_ratio
    )


def _leaderboard(
    rows: list[dict[str, Any]],
    *,
    min_trades: int,
    target_win_rate_pct: float,
    target_expectancy_pct: float,
    min_consistency_pct: float,
    min_win_loss_ratio: float,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row.get("candidate", "") or ""), []).append(row)

    board: list[dict[str, Any]] = []
    for candidate, items in grouped.items():
        evaluated = sum(int(item.get("evaluated", 0) or 0) for item in items)
        wins = sum(int(item.get("wins", 0) or 0) for item in items)
        losses = sum(int(item.get("losses", 0) or 0) for item in items)
        win_loss_ratio = _ratio_value(wins, losses)
        pass_count = sum(
            1
            for item in items
            if _row_passes(
                item,
                min_trades=min_trades,
                target_win_rate_pct=target_win_rate_pct,
                target_expectancy_pct=target_expectancy_pct,
                min_win_loss_ratio=min_win_loss_ratio,
            )
        )
        windows = len(items)
        consistency_pct = round((pass_count / max(1, windows)) * 100.0, 2)
        win_rate_pct = round((wins / max(1, evaluated)) * 100.0, 2)
        avg_expectancy = round(
            sum(_safe_float(item.get("expectancy_pct")) for item in items) / max(1, windows),
            4,
        )
        board.append(
            {
                "candidate": candidate,
                "windows": windows,
                "passing_windows": pass_count,
                "consistency_pct": consistency_pct,
                "evaluated": evaluated,
                "wins": wins,
                "losses": losses,
                "win_rate_pct": win_rate_pct,
                "win_loss_ratio": win_loss_ratio,
                "avg_expectancy_pct": avg_expectancy,
                "promotable": (
                    evaluated >= min_trades
                    and consistency_pct >= min_consistency_pct
                    and win_rate_pct >= target_win_rate_pct
                    and avg_expectancy >= target_expectancy_pct
                    and win_loss_ratio >= min_win_loss_ratio
                ),
            }
        )
    board.sort(
        key=lambda item: (
            bool(item["promotable"]),
            float(item["consistency_pct"]),
            float(item["avg_expectancy_pct"]),
            int(item["evaluated"]),
        ),
        reverse=True,
    )
    return board


def _write_best(path: Path, board: list[dict[str, Any]], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    best = next((item for item in board if item.get("promotable")), board[0] if board else None)
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "best": best,
        "leaderboard": board,
        "note": "Offline replay optimizer only. Review before changing live trading settings.",
    }
    if best:
        latest_for_best = [row for row in rows if str(row.get("candidate", "")) == best["candidate"]]
        if latest_for_best:
            payload["latest_candidate_run"] = latest_for_best[-1]
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _append_ratio_history(path: Path, board: list[dict[str, Any]], iteration: int, iteration_timestamp: str) -> None:
    if not board:
        return
    board_path = path
    path.parent.mkdir(parents=True, exist_ok=True)
    new_file = not board_path.exists()
    with board_path.open("a", newline="", encoding="utf-8") as handle:
        fieldnames = [
            "iteration",
            "run_timestamp",
            "candidate",
            "windows",
            "evaluated",
            "wins",
            "losses",
            "win_rate_pct",
            "win_loss_ratio",
        ]
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        if new_file:
            writer.writeheader()
        for item in board:
            evaluated = int(item.get("evaluated", 0) or 0)
            wins = int(item.get("wins", 0) or 0)
            losses = int(item.get("losses", 0) or 0)
            writer.writerow(
                {
                    "iteration": iteration,
                    "run_timestamp": iteration_timestamp,
                    "candidate": item.get("candidate", ""),
                    "windows": item.get("windows", 0),
                    "evaluated": evaluated,
                    "wins": wins,
                    "losses": losses,
                    "win_rate_pct": item.get("win_rate_pct", 0.0),
                    "win_loss_ratio": _ratio_value(wins, losses),
                }
            )


def _read_symbols_file(path: Path) -> list[str]:
    raw = path.read_text(encoding="utf-8")
    if not raw.strip():
        return []
    return [token.strip().upper() for token in raw.replace(",", "\n").splitlines() if token.strip()]


def run_optimizer(args: argparse.Namespace) -> dict[str, Any]:
    symbols = [s.strip().upper() for s in str(args.symbols).split(",") if s.strip()]
    symbols_source = str(getattr(args, "symbols_source", "manual") or "manual").strip().lower()
    offline = bool(getattr(args, "offline", False))
    if symbols_source in {"all_optionable", "all-optionable", "all"}:
        if offline:
            symbols_file_raw = str(getattr(args, "symbols_file", "") or "").strip()
            if not symbols_file_raw:
                raise ValueError(
                    "When --offline is used with --symbols-source all_optionable, provide --symbols-file."
                )
            symbols_file = Path(symbols_file_raw).expanduser()
            if not symbols_file.exists():
                raise FileNotFoundError(f"symbols-file not found: {symbols_file}")
            symbols = _read_symbols_file(symbols_file)
            if not symbols:
                raise ValueError(f"No symbols were found in symbols-file: {symbols_file}")
        else:
            load_runtime_env()
            api_key = os.getenv("ALPACA_API_KEY", "")
            secret_key = os.getenv("ALPACA_SECRET_KEY", "")
            if not api_key or not secret_key:
                raise ValueError("ALPACA_API_KEY and ALPACA_SECRET_KEY are required when using --symbols-source all_optionable.")
            max_count = int(getattr(args, "symbols_limit", 0) or 0)
            client = AlpacaDataClient(api_key=api_key, secret_key=secret_key, paper=True)
            symbols = client.get_all_optionable_tickers(max_count=max_count if max_count > 0 else None)
            symbols = [s.upper() for s in symbols]
    else:
        symbols_file = str(getattr(args, "symbols_file", "") or "").strip()
        if symbols_file:
            symbols.extend(_read_symbols_file(Path(symbols_file).expanduser()))
    if symbols:
        deduped: list[str] = []
        seen: set[str] = set()
        for symbol in symbols:
            if not symbol:
                continue
            usym = symbol.upper()
            if usym in seen:
                continue
            seen.add(usym)
            deduped.append(usym)
        symbols = deduped

    if not symbols:
        raise ValueError("At least one symbol is required.")
    default_start, default_end = _default_start_end()
    start = _parse_date(args.start) if args.start else default_start
    end = _parse_date(args.end) if args.end else default_end
    windows = _window_ranges(
        start,
        end,
        window_days=max(1, int(args.window_days)),
        step_days=max(1, int(args.step_days)),
        max_windows=max(0, int(args.max_windows)),
    )
    output_dir = Path(args.output_dir)
    results_csv = output_dir / "optimizer_runs.csv"
    best_json = output_dir / "best_candidate.json"
    candidates = _candidate_grid()
    iterations = max(0, int(args.iterations))
    runs_completed = 0
    rolling = bool(getattr(args, "rolling", False))
    current_start = start
    current_end = end
    ratio_history_csv = output_dir / "optimizer_win_loss_ratio.csv"

    while iterations == 0 or runs_completed < iterations:
        if rolling and runs_completed > 0:
            next_window = _next_window_dates(
                current_start=current_start,
                current_end=current_end,
                args=args,
                symbols=symbols,
                cache_dir=Path(args.cache_dir),
                offline=offline,
            )
            if next_window is None:
                print(
                    json.dumps(
                        {
                            "status": "waiting_for_new_data",
                            "offline": offline,
                            "rolling_end_policy": str(getattr(args, "rolling_end_policy", "fixed")),
                            "window_start": str(current_start),
                            "window_end": str(current_end),
                        },
                        indent=2,
                    )
                )
                time.sleep(max(1, int(args.sleep_seconds)))
                continue
            current_start, current_end = next_window
            windows = _window_ranges(
                current_start,
                current_end,
                window_days=max(1, int(args.window_days)),
                step_days=max(1, int(args.step_days)),
                max_windows=max(0, int(args.max_windows)),
            )
        runs_completed += 1
        run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        for window_start, window_end in windows:
            for candidate in candidates:
                replay_output = output_dir / "replays" / (
                    f"{run_timestamp}_iter{runs_completed}_{candidate.name}_{window_start}_{window_end}.csv"
                )
                replay_cfg = ReplayConfig(
                    symbols=symbols,
                    start=window_start.isoformat(),
                    end=window_end.isoformat(),
                    interval=str(args.interval),
                    scan_bars=max(1, int(args.scan_bars)),
                    scan_every_minutes=candidate.scan_every_minutes,
                    horizon_minutes=candidate.horizon_minutes,
                    take_profit_pct=candidate.take_profit_pct,
                    stop_loss_pct=candidate.stop_loss_pct,
                    max_signals_per_scan=candidate.max_signals_per_scan,
                    output=replay_output,
                    cache_dir=Path(args.cache_dir),
                    daily_lookback_days=max(30, int(args.daily_lookback_days)),
                    min_daily_bars=candidate.min_daily_bars,
                    offline=offline,
                )
                with _temporary_config(candidate.overrides):
                    result = run_replay(replay_cfg)
                summary = result.get("summary", {})
                row = {
                    "run_timestamp": run_timestamp,
                    "iteration": runs_completed,
                    "candidate": candidate.name,
                    "window_start": window_start.isoformat(),
                    "window_end": window_end.isoformat(),
                    "symbols": ",".join(symbols),
                    "interval": str(args.interval),
                    "horizon_minutes": candidate.horizon_minutes,
                    "take_profit_pct": candidate.take_profit_pct,
                    "stop_loss_pct": candidate.stop_loss_pct,
                    "scan_every_minutes": candidate.scan_every_minutes,
                    "max_signals_per_scan": candidate.max_signals_per_scan,
                    "min_daily_bars": candidate.min_daily_bars,
                    "scan_iterations": int(summary.get("scan_iterations", 0) or 0),
                    "scan_failures": int(summary.get("scan_failures", 0) or 0),
                    "opportunities": int(summary.get("opportunities", 0) or 0),
                    "evaluated": int(summary.get("evaluated", 0) or 0),
                    "wins": int(summary.get("wins", 0) or 0),
                    "losses": int(summary.get("losses", 0) or 0),
                    "win_rate_pct": _safe_float(summary.get("win_rate_pct")),
                    "expectancy_pct": _expectancy_pct(summary, candidate),
                    "top_failures_json": json.dumps(summary.get("top_failures", [])),
                    "top_failure_details_json": json.dumps(summary.get("top_failure_details", [])),
                    "overrides_json": json.dumps(candidate.overrides, sort_keys=True),
                    "output": result.get("output", ""),
                    "summary_path": result.get("summary_path", ""),
                }
                row["pass_target"] = _row_passes(
                    row,
                    min_trades=int(args.min_trades),
                    target_win_rate_pct=float(args.target_win_rate_pct),
                    target_expectancy_pct=float(args.target_expectancy_pct),
                    min_win_loss_ratio=float(args.min_win_loss_ratio),
                )
                _append_csv(results_csv, row)

        all_rows = _read_rows(results_csv)
        board = _leaderboard(
            all_rows,
            min_trades=int(args.min_trades),
            target_win_rate_pct=float(args.target_win_rate_pct),
            target_expectancy_pct=float(args.target_expectancy_pct),
            min_consistency_pct=float(args.min_consistency_pct),
            min_win_loss_ratio=float(args.min_win_loss_ratio),
        )
        _append_ratio_history(ratio_history_csv, board, runs_completed, run_timestamp)
        _write_best(best_json, board, all_rows)
        print(
            json.dumps(
                {
                    "iteration": runs_completed,
                    "windows": len(windows),
                    "candidates": len(candidates),
                    "results_csv": str(results_csv),
                    "ratio_history_csv": str(ratio_history_csv),
                    "best_json": str(best_json),
                    "best": board[0] if board else None,
                },
                indent=2,
            )
        )
        if iterations == 0 or runs_completed < iterations:
            time.sleep(max(1, int(args.sleep_seconds)))

    all_rows = _read_rows(results_csv)
    board = _leaderboard(
        all_rows,
        min_trades=int(args.min_trades),
        target_win_rate_pct=float(args.target_win_rate_pct),
        target_expectancy_pct=float(args.target_expectancy_pct),
        min_consistency_pct=float(args.min_consistency_pct),
        min_win_loss_ratio=float(args.min_win_loss_ratio),
    )
    return {"results_csv": str(results_csv), "best_json": str(best_json), "leaderboard": board[:5]}


def _parse_args() -> argparse.Namespace:
    default_start, default_end = _default_start_end()
    parser = argparse.ArgumentParser(description="Continuously replay historical data and rank trading candidates.")
    parser.add_argument("--symbols", default=",".join(config.CORE_TICKERS), help="Comma-separated symbols.")
    parser.add_argument(
        "--symbols-file",
        default="",
        help="Optional file with symbols (one per line or comma-separated), used with manual symbols or offline all_optionable mode.",
    )
    parser.add_argument(
        "--offline",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Use only cached historical bars (or fetch from network when disabled). "
            "Defaults to offline mode. Use --no-offline to allow live fetch."
        ),
    )
    parser.add_argument(
        "--symbols-source",
        default="manual",
        choices=("manual", "all_optionable"),
        help="symbol input mode: manual list or all optionable US equities from Alpaca assets."
    )
    parser.add_argument(
        "--symbols-limit",
        type=int,
        default=0,
        help="Optional cap when --symbols-source all_optionable (0 = no cap)."
    )
    parser.add_argument("--start", default=default_start.isoformat(), help="YYYY-MM-DD start date.")
    parser.add_argument("--end", default=default_end.isoformat(), help="YYYY-MM-DD end date.")
    parser.add_argument("--interval", default="5m", help="yfinance interval, e.g. 1m, 5m, 15m.")
    parser.add_argument(
        "--scan-bars",
        type=int,
        default=int(getattr(config, "SCAN_INTRADAY_BARS", 60)),
        help="Scanner lookback bars per symbol at each replay timestamp.",
    )
    parser.add_argument("--window-days", type=int, default=5)
    parser.add_argument("--step-days", type=int, default=5)
    parser.add_argument("--max-windows", type=int, default=4, help="0 means use all windows in the range.")
    parser.add_argument("--iterations", type=int, default=1, help="0 means run forever.")
    parser.add_argument(
        "--rolling",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Advance the window automatically for each run instead of repeating the same date range.",
    )
    parser.add_argument(
        "--rolling-step-days",
        type=int,
        default=1,
        help="Days to shift start/end per run when --rolling and --rolling-end-policy fixed.",
    )
    parser.add_argument(
        "--rolling-end-policy",
        default="fixed",
        choices=("fixed", "today", "cache"),
        help="In rolling mode, where to anchor the next end date.",
    )
    parser.add_argument("--sleep-seconds", type=int, default=300)
    parser.add_argument("--daily-lookback-days", type=int, default=90)
    parser.add_argument("--min-trades", type=int, default=5)
    parser.add_argument("--target-win-rate-pct", type=float, default=55.0)
    parser.add_argument("--target-expectancy-pct", type=float, default=0.05)
    parser.add_argument("--min-consistency-pct", type=float, default=60.0)
    parser.add_argument(
        "--min-win-loss-ratio",
        type=float,
        default=1.25,
        help="Minimum wins/losses ratio required for pass_target and promotable status.",
    )
    parser.add_argument("--output-dir", default=str(Path(config.DATA_DIR) / "replay_optimizer"))
    parser.add_argument("--cache-dir", default=str(Path(config.DATA_DIR) / "historical_cache"))
    return parser.parse_args()


if __name__ == "__main__":
    print(json.dumps(run_optimizer(_parse_args()), indent=2))
