"""Replay optimizer farm launcher and cross-dataset aggregator.

The farm runs multiple offline optimizer workers against different symbol
universes, intervals, and historical windows. Workers never place orders.
Each worker writes to its own output directory; the farm only starts, stops,
checks, and aggregates them.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import signal
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import config


@dataclass(frozen=True)
class FarmWorkerSpec:
    name: str
    symbols: tuple[str, ...]
    interval: str = "5m"
    start: str = ""
    end: str = ""
    window_days: int = 5
    step_days: int = 5
    max_windows: int = 4
    sleep_seconds: int = 900
    daily_lookback_days: int = 90
    min_trades: int = 5
    target_win_rate_pct: float = 55.0
    target_expectancy_pct: float = 0.05
    min_consistency_pct: float = 60.0
    min_win_loss_ratio: float = 1.25
    scan_bars: int = int(getattr(config, "SCAN_INTRADAY_BARS", 60))
    rolling: bool = False
    rolling_step_days: int = 1
    rolling_end_policy: str = "fixed"
    symbols_file: str | None = None


def _today() -> date:
    return datetime.now().date()


def _date_text(days_ago: int) -> str:
    return (_today() - timedelta(days=days_ago)).isoformat()


def default_worker_specs() -> dict[str, FarmWorkerSpec]:
    """Return independent datasets that multiply replay evidence."""
    return {
        "indexes_recent": FarmWorkerSpec(
            name="indexes_recent",
            symbols=("SPY", "QQQ", "IWM", "DIA"),
            start=_date_text(24),
            end=_date_text(1),
            window_days=5,
            step_days=5,
            max_windows=4,
        ),
        "mega_cap_recent": FarmWorkerSpec(
            name="mega_cap_recent",
            symbols=("AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA"),
            start=_date_text(24),
            end=_date_text(1),
            window_days=5,
            step_days=5,
            max_windows=4,
        ),
        "semis_recent": FarmWorkerSpec(
            name="semis_recent",
            symbols=("AMD", "NVDA", "INTC", "AVGO", "SMH"),
            start=_date_text(24),
            end=_date_text(1),
            window_days=5,
            step_days=5,
            max_windows=4,
        ),
        "high_beta_recent": FarmWorkerSpec(
            name="high_beta_recent",
            symbols=("TSLA", "AMD", "NVDA", "COIN", "PLTR"),
            start=_date_text(24),
            end=_date_text(1),
            window_days=5,
            step_days=5,
            max_windows=4,
        ),
        "indexes_1m_recent": FarmWorkerSpec(
            name="indexes_1m_recent",
            symbols=("SPY", "QQQ"),
            interval="1m",
            start=_date_text(7),
            end=_date_text(1),
            window_days=2,
            step_days=2,
            max_windows=3,
            sleep_seconds=1800,
            min_trades=3,
        ),
        "broad_holdout": FarmWorkerSpec(
            name="broad_holdout",
            symbols=("SPY", "QQQ", "AAPL", "MSFT", "NVDA", "AMD", "TSLA"),
            start=_date_text(45),
            end=_date_text(21),
            window_days=6,
            step_days=6,
            max_windows=4,
            sleep_seconds=1800,
            min_trades=5,
        ),
    }


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return int(default)


def _safe_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(value) if value is not None else default


def _ratio_value(wins: int, losses: int) -> float:
    if losses <= 0:
        return float("inf")
    return round(wins / max(1, losses), 4)


def _coerce_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _coerce_symbol_tokens(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        raw = value
    elif isinstance(value, (list, tuple, set)):
        raw = ",".join(str(item) for item in value)
    else:
        raw = str(value)
    seen: set[str] = set()
    out: list[str] = []
    for token in raw.replace("\n", ",").replace("\r", ",").split(","):
        symbol = token.strip().upper()
        if not symbol:
            continue
        if symbol in seen:
            continue
        seen.add(symbol)
        out.append(symbol)
    return tuple(out)


def _read_worker_symbols_file(path: Path) -> tuple[str, ...]:
    if not path.exists():
        raise FileNotFoundError(f"Worker symbols file not found: {path}")
    raw = path.read_text(encoding="utf-8", errors="ignore")
    return _coerce_symbol_tokens(raw)


def _build_worker_spec_from_dict(source_path: Path, payload: dict[str, Any]) -> FarmWorkerSpec:
    name = _coerce_str(payload.get("name"))
    if not name:
        raise ValueError("Worker spec missing required name.")
    start = _coerce_str(payload.get("start"))
    end = _coerce_str(payload.get("end"))
    if not start or not end:
        raise ValueError(f"Worker spec '{name}' requires start and end.")
    symbols = _coerce_symbol_tokens(payload.get("symbols"))
    symbols_file = _coerce_str(payload.get("symbols_file"))
    resolved_symbols_file: str | None = None
    if symbols_file:
        candidate = (source_path.parent / symbols_file).resolve()
        symbols_from_file = _read_worker_symbols_file(candidate)
        symbols = tuple(dict.fromkeys((*symbols, *symbols_from_file)))
        resolved_symbols_file = str(candidate)
    if not symbols:
        raise ValueError(f"Worker spec '{name}' must define symbols or symbols_file.")
    interval = _coerce_str(payload.get("interval") or "5m")
    scan_bars = max(1, _safe_int(payload.get("scan_bars"), int(getattr(config, "SCAN_INTRADAY_BARS", 60))))
    rolling_end_policy = _coerce_str(payload.get("rolling_end_policy") or "fixed").lower() or "fixed"
    if rolling_end_policy not in {"fixed", "today", "cache"}:
        raise ValueError(f"Worker spec '{name}' has invalid rolling_end_policy '{rolling_end_policy}'.")
    return FarmWorkerSpec(
        name=name,
        symbols=symbols,
        interval=interval if interval else "5m",
        start=start,
        end=end,
        window_days=max(1, _safe_int(payload.get("window_days"), 5)),
        step_days=max(1, _safe_int(payload.get("step_days"), 5)),
        max_windows=max(0, _safe_int(payload.get("max_windows"), 4)),
        sleep_seconds=max(1, _safe_int(payload.get("sleep_seconds"), 900)),
        daily_lookback_days=max(30, _safe_int(payload.get("daily_lookback_days"), 90)),
        min_trades=max(1, _safe_int(payload.get("min_trades"), 5)),
        target_win_rate_pct=max(0.0, _safe_float(payload.get("target_win_rate_pct"), 55.0)),
        target_expectancy_pct=_safe_float(payload.get("target_expectancy_pct"), 0.05),
        min_consistency_pct=_safe_float(payload.get("min_consistency_pct"), 60.0),
        min_win_loss_ratio=max(0.0, _safe_float(payload.get("min_win_loss_ratio"), 1.25)),
        scan_bars=scan_bars,
        rolling=_safe_bool(payload.get("rolling"), False),
        rolling_step_days=max(1, _safe_int(payload.get("rolling_step_days"), 1)),
        rolling_end_policy=rolling_end_policy,
        symbols_file=resolved_symbols_file,
    )


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _registry_path(output_root: Path) -> Path:
    return output_root / "farm_registry.json"


def _load_registry(output_root: Path) -> dict[str, Any]:
    path = _registry_path(output_root)
    if not path.exists():
        return {"workers": {}}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"workers": {}}


def _write_registry(output_root: Path, registry: dict[str, Any]) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    registry["updated_at"] = datetime.now().isoformat(timespec="seconds")
    _registry_path(output_root).write_text(json.dumps(registry, indent=2), encoding="utf-8")


def _pid_running(pid: int | None) -> bool:
    if not pid or pid <= 0:
        return False
    if os.name == "nt":
        try:
            result = subprocess.run(  # noqa: S603
                [
                    "powershell",
                    "-NoProfile",
                    "-Command",
                    f"if (Get-Process -Id {int(pid)} -ErrorAction SilentlyContinue) {{ exit 0 }} else {{ exit 1 }}",
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=5,
                check=False,
            )
            return result.returncode == 0
        except Exception:
            return False
    try:
        os.kill(pid, 0)
        return True
    except (OSError, SystemError):
        return False


def _selected_specs(worker_names: str, specs: dict[str, FarmWorkerSpec]) -> list[FarmWorkerSpec]:
    if worker_names.strip().lower() in {"", "all"}:
        return list(specs.values())
    selected: list[FarmWorkerSpec] = []
    missing: list[str] = []
    for name in [part.strip() for part in worker_names.split(",") if part.strip()]:
        spec = specs.get(name)
        if spec is None:
            missing.append(name)
        else:
            selected.append(spec)
    if missing:
        raise ValueError(f"Unknown worker(s): {', '.join(missing)}")
    return selected


def _load_worker_specs(path: Path) -> dict[str, FarmWorkerSpec]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    payload = raw
    if isinstance(payload, dict):
        payload = payload.get("workers", payload.get("specs", []))
    if not isinstance(payload, list):
        raise ValueError("Worker spec file must be a JSON list or object with a 'workers'/'specs' list.")
    result: dict[str, FarmWorkerSpec] = {}
    for item in payload:
        if not isinstance(item, dict):
            raise ValueError("Each worker spec must be an object.")
        spec = _build_worker_spec_from_dict(path, item)
        if spec.name in result:
            raise ValueError(f"Duplicate worker name in spec file: {spec.name}")
        result[spec.name] = spec
    return result


def _optimizer_command(
    *,
    python_exe: Path,
    spec: FarmWorkerSpec,
    output_dir: Path,
    cache_dir: Path,
    offline: bool,
) -> list[str]:
    optimizer = Path(__file__).resolve().with_name("replay_optimizer.py")
    args = [
        str(python_exe),
        "-u",
        str(optimizer),
        "--symbols",
        ",".join(spec.symbols),
        "--start",
        spec.start,
        "--end",
        spec.end,
        "--interval",
        spec.interval,
        "--window-days",
        str(spec.window_days),
        "--step-days",
        str(spec.step_days),
        "--max-windows",
        str(spec.max_windows),
        "--iterations",
        "0",
        "--sleep-seconds",
        str(spec.sleep_seconds),
        "--daily-lookback-days",
        str(spec.daily_lookback_days),
        "--min-trades",
        str(spec.min_trades),
        "--target-win-rate-pct",
        str(spec.target_win_rate_pct),
        "--target-expectancy-pct",
        str(spec.target_expectancy_pct),
        "--min-consistency-pct",
        str(spec.min_consistency_pct),
        "--min-win-loss-ratio",
        str(spec.min_win_loss_ratio),
        "--scan-bars",
        str(spec.scan_bars),
        "--output-dir",
        str(output_dir),
        "--cache-dir",
        str(cache_dir),
    ]
    if spec.symbols_file:
        args.extend(["--symbols-file", str(spec.symbols_file)])
    if spec.rolling:
        args.extend(
            [
                "--rolling",
                "--rolling-step-days",
                str(spec.rolling_step_days),
                "--rolling-end-policy",
                spec.rolling_end_policy,
            ]
        )
    if offline:
        args.append("--offline")
    else:
        args.append("--no-offline")
    return args


def start_workers(
    *,
    worker_names: str,
    worker_specs: dict[str, FarmWorkerSpec] | None = None,
    output_root: Path,
    cache_dir: Path,
    python_exe: Path,
    stagger_seconds: int,
    offline: bool = False,
    restart: bool = False,
) -> dict[str, Any]:
    specs = worker_specs or default_worker_specs()
    selected = _selected_specs(worker_names, specs)
    python_exe = python_exe.resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    (output_root / "logs").mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)
    registry = _load_registry(output_root)
    registry.setdefault("workers", {})
    started: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []

    for index, spec in enumerate(selected):
        existing = registry["workers"].get(spec.name, {})
        existing_pid = _safe_int(existing.get("pid"), 0)
        if existing_pid and _pid_running(existing_pid) and not restart:
            skipped.append({"worker": spec.name, "pid": existing_pid, "reason": "already running"})
            continue
        if existing_pid and _pid_running(existing_pid) and restart:
            try:
                os.kill(existing_pid, signal.SIGTERM)
            except OSError:
                pass

        worker_output = output_root / spec.name
        stdout_log = output_root / "logs" / f"{spec.name}.out.log"
        stderr_log = output_root / "logs" / f"{spec.name}.err.log"
        command = _optimizer_command(
            python_exe=python_exe,
            spec=spec,
            output_dir=worker_output,
            cache_dir=cache_dir,
            offline=offline,
        )
        stdout_handle = stdout_log.open("a", encoding="utf-8")
        stderr_handle = stderr_log.open("a", encoding="utf-8")
        creationflags = 0
        popen_kwargs: dict[str, Any] = {"cwd": str(_repo_root())}
        if os.name == "nt":
            creationflags = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS
        else:
            popen_kwargs["start_new_session"] = True
        process = subprocess.Popen(  # noqa: S603
            command,
            stdout=stdout_handle,
            stderr=stderr_handle,
            creationflags=creationflags,
            **popen_kwargs,
        )
        stdout_handle.close()
        stderr_handle.close()
        record = {
            "pid": process.pid,
            "started_at": datetime.now().isoformat(timespec="seconds"),
            "command": command,
            "output_dir": str(worker_output),
            "stdout_log": str(stdout_log),
            "stderr_log": str(stderr_log),
            "spec": asdict(spec),
        }
        registry["workers"][spec.name] = record
        started.append({"worker": spec.name, "pid": process.pid, "output_dir": str(worker_output)})
        _write_registry(output_root, registry)
        if stagger_seconds > 0 and index < len(selected) - 1:
            time.sleep(stagger_seconds)

    return {"started": started, "skipped": skipped, "registry": str(_registry_path(output_root))}


def stop_workers(*, worker_names: str, output_root: Path, worker_specs: dict[str, FarmWorkerSpec] | None = None) -> dict[str, Any]:
    specs = worker_specs or default_worker_specs()
    selected = _selected_specs(worker_names, specs)
    selected_names = {spec.name for spec in selected}
    registry = _load_registry(output_root)
    stopped: list[dict[str, Any]] = []
    not_running: list[dict[str, Any]] = []
    for name, item in registry.get("workers", {}).items():
        if name not in selected_names:
            continue
        pid = _safe_int(item.get("pid"), 0)
        if pid and _pid_running(pid):
            try:
                os.kill(pid, signal.SIGTERM)
                stopped.append({"worker": name, "pid": pid})
            except OSError as exc:
                not_running.append({"worker": name, "pid": pid, "error": str(exc)})
        else:
            not_running.append({"worker": name, "pid": pid, "reason": "not running"})
    return {"stopped": stopped, "not_running": not_running}


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _latest_error(path: Path, max_lines: int = 5) -> list[str]:
    if not str(path) or not path.exists() or path.is_dir():
        return []
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    return lines[-max_lines:]


def status_workers(*, output_root: Path) -> dict[str, Any]:
    registry = _load_registry(output_root)
    statuses: list[dict[str, Any]] = []
    for name, item in sorted(registry.get("workers", {}).items()):
        pid = _safe_int(item.get("pid"), 0)
        output_dir = Path(str(item.get("output_dir", "")))
        rows = _read_csv(output_dir / "optimizer_runs.csv")
        best_path = output_dir / "best_candidate.json"
        best: dict[str, Any] | None = None
        if best_path.exists():
            try:
                payload = json.loads(best_path.read_text(encoding="utf-8"))
                best = payload.get("best")
            except Exception:
                best = None
        statuses.append(
            {
                "worker": name,
                "pid": pid,
                "running": _pid_running(pid),
                "rows": len(rows),
                "output_dir": str(output_dir),
                "best": best,
                "latest_errors": _latest_error(Path(str(item.get("stderr_log", "")))),
            }
        )
    return {"workers": statuses, "registry": str(_registry_path(output_root))}


def _row_passes(
    row: dict[str, Any],
    *,
    min_trades: int,
    target_win_rate_pct: float,
    target_expectancy_pct: float,
    min_win_loss_ratio: float,
) -> bool:
    wins = _safe_int(row.get("wins"))
    losses = _safe_int(row.get("losses"))
    ratio = _ratio_value(wins, losses)
    return (
        _safe_int(row.get("evaluated")) >= min_trades
        and _safe_float(row.get("win_rate_pct")) >= target_win_rate_pct
        and _safe_float(row.get("expectancy_pct")) >= target_expectancy_pct
        and ratio >= min_win_loss_ratio
    )


def aggregate_farm(
    *,
    output_root: Path,
    min_total_trades: int,
    min_workers: int,
    min_passing_workers: int,
    min_passing_window_pct: float,
    target_win_rate_pct: float,
    target_expectancy_pct: float,
    min_win_loss_ratio: float,
    min_worker_win_loss_ratio: float,
    worker_names: set[str] | None = None,
) -> dict[str, Any]:
    registry = _load_registry(output_root)
    all_rows: list[dict[str, Any]] = []
    fieldnames: list[str] = ["worker"]
    for name, item in sorted(registry.get("workers", {}).items()):
        if worker_names is not None and name not in worker_names:
            continue
        output_dir = Path(str(item.get("output_dir", "")))
        rows = _read_csv(output_dir / "optimizer_runs.csv")
        for row in rows:
            payload = {"worker": name, **row}
            all_rows.append(payload)
            for key in payload.keys():
                if key not in fieldnames:
                    fieldnames.append(key)

    output_root.mkdir(parents=True, exist_ok=True)
    farm_runs = output_root / "farm_runs.csv"
    with farm_runs.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in all_rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})

    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in all_rows:
        grouped.setdefault(str(row.get("candidate", "") or "unknown"), []).append(row)

    leaderboard: list[dict[str, Any]] = []
    for candidate, rows in grouped.items():
        workers = sorted({str(row.get("worker", "") or "") for row in rows})
        evaluated = sum(_safe_int(row.get("evaluated")) for row in rows)
        wins = sum(_safe_int(row.get("wins")) for row in rows)
        losses = sum(_safe_int(row.get("losses")) for row in rows)
        passing_rows = sum(
            1
            for row in rows
            if _row_passes(
                row,
                min_trades=5,
                target_win_rate_pct=target_win_rate_pct,
                target_expectancy_pct=target_expectancy_pct,
                min_win_loss_ratio=min_worker_win_loss_ratio,
            )
        )
        worker_pass: dict[str, bool] = {}
        for worker in workers:
            worker_rows = [row for row in rows if row.get("worker") == worker]
            worker_evaluated = sum(_safe_int(row.get("evaluated")) for row in worker_rows)
            worker_wins = sum(_safe_int(row.get("wins")) for row in worker_rows)
            worker_losses = sum(_safe_int(row.get("losses")) for row in worker_rows)
            worker_ratio = _ratio_value(worker_wins, worker_losses)
            worker_expectancy = sum(_safe_float(row.get("expectancy_pct")) for row in worker_rows) / max(1, len(worker_rows))
            worker_win_rate = round(worker_wins / max(1, worker_evaluated) * 100.0, 2)
            worker_pass[worker] = (
                worker_evaluated >= 5
                and worker_win_rate >= target_win_rate_pct
                and worker_expectancy >= target_expectancy_pct
                and worker_ratio >= min_worker_win_loss_ratio
            )
        passing_workers = sum(1 for ok in worker_pass.values() if ok)
        passing_window_pct = round(passing_rows / max(1, len(rows)) * 100.0, 2)
        win_rate_pct = round(wins / max(1, evaluated) * 100.0, 2)
        win_loss_ratio = _ratio_value(wins, losses)
        avg_expectancy_pct = round(sum(_safe_float(row.get("expectancy_pct")) for row in rows) / max(1, len(rows)), 4)
        promotable = (
            evaluated >= min_total_trades
            and len(workers) >= min_workers
            and passing_workers >= min_passing_workers
            and passing_window_pct >= min_passing_window_pct
            and win_rate_pct >= target_win_rate_pct
            and avg_expectancy_pct >= target_expectancy_pct
            and win_loss_ratio >= min_win_loss_ratio
        )
        leaderboard.append(
            {
                "candidate": candidate,
                "workers": workers,
                "worker_count": len(workers),
                "passing_workers": passing_workers,
                "windows": len(rows),
                "passing_windows": passing_rows,
                "passing_window_pct": passing_window_pct,
                "evaluated": evaluated,
                "wins": wins,
                "losses": losses,
                "win_rate_pct": win_rate_pct,
                "win_loss_ratio": win_loss_ratio,
                "avg_expectancy_pct": avg_expectancy_pct,
                "promotable": promotable,
            }
        )

    leaderboard.sort(
        key=lambda item: (
            bool(item["promotable"]),
            int(item["passing_workers"]),
            float(item["passing_window_pct"]),
            float(item["win_loss_ratio"]),
            float(item["avg_expectancy_pct"]),
            int(item["evaluated"]),
        ),
        reverse=True,
    )
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "farm_runs_csv": str(farm_runs),
        "worker_count": len({str(row.get("worker", "") or "") for row in all_rows}),
        "row_count": len(all_rows),
        "worker_filter": sorted(worker_names) if worker_names is not None else [],
        "requirements": {
            "min_total_trades": min_total_trades,
            "min_workers": min_workers,
            "min_passing_workers": min_passing_workers,
            "min_passing_window_pct": min_passing_window_pct,
            "target_win_rate_pct": target_win_rate_pct,
            "target_expectancy_pct": target_expectancy_pct,
            "min_win_loss_ratio": min_win_loss_ratio,
            "min_worker_win_loss_ratio": min_worker_win_loss_ratio,
        },
        "best": next((row for row in leaderboard if row.get("promotable")), leaderboard[0] if leaderboard else None),
        "top_3": leaderboard[:3],
        "leaderboard": leaderboard,
        "note": "Farm aggregation is offline evidence only. Review before changing live settings.",
    }
    leaderboard_path = output_root / "farm_leaderboard.json"
    leaderboard_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return payload


def _parse_args() -> argparse.Namespace:
    default_output_root = Path(config.DATA_DIR) / "replay_farm"
    default_cache_dir = Path(config.DATA_DIR) / "historical_cache"
    parser = argparse.ArgumentParser(description="Launch and aggregate multiple replay optimizer workers.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    common_parent = argparse.ArgumentParser(add_help=False)
    common_parent.add_argument("--output-root", default=str(default_output_root))

    start_parser = subparsers.add_parser("start", parents=[common_parent], help="Start farm workers.")
    start_parser.add_argument("--workers", default="all", help="Comma-separated worker names, or all.")
    start_parser.add_argument(
        "--workers-file",
        default="",
        help="Optional JSON file containing worker dataset specs.",
    )
    start_parser.add_argument("--cache-dir", default=str(default_cache_dir))
    start_parser.add_argument("--python", default=sys.executable)
    start_parser.add_argument("--stagger-seconds", type=int, default=20)
    start_parser.add_argument(
        "--offline",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Run replay optimizer workers in offline mode (cache-only). "
            "Use --no-offline if workers should fetch missing bars."
        ),
    )
    start_parser.add_argument("--restart", action="store_true")

    stop_parser = subparsers.add_parser("stop", parents=[common_parent], help="Stop farm workers from the registry.")
    stop_parser.add_argument("--workers", default="all", help="Comma-separated worker names, or all.")
    stop_parser.add_argument(
        "--workers-file",
        default="",
        help="Optional JSON file containing worker dataset specs matching active output_root workers.",
    )

    subparsers.add_parser("status", parents=[common_parent], help="Show worker status.")

    aggregate_parser = subparsers.add_parser("aggregate", parents=[common_parent], help="Build farm leaderboard.")
    aggregate_parser.add_argument("--min-total-trades", type=int, default=100)
    aggregate_parser.add_argument("--min-workers", type=int, default=2)
    aggregate_parser.add_argument("--min-passing-workers", type=int, default=2)
    aggregate_parser.add_argument("--min-passing-window-pct", type=float, default=40.0)
    aggregate_parser.add_argument("--target-win-rate-pct", type=float, default=55.0)
    aggregate_parser.add_argument("--target-expectancy-pct", type=float, default=0.05)
    aggregate_parser.add_argument("--min-win-loss-ratio", type=float, default=1.25)
    aggregate_parser.add_argument("--min-worker-win-loss-ratio", type=float, default=1.15)

    subparsers.add_parser("list", help="List available farm workers.")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    output_root = Path(getattr(args, "output_root", Path(config.DATA_DIR) / "replay_farm"))
    if args.command == "list":
        print(json.dumps({"workers": [asdict(spec) for spec in default_worker_specs().values()]}, indent=2))
        return 0
    if args.command == "start":
        workers_file = _coerce_str(args.workers_file)
        worker_specs = None
        if workers_file:
            worker_specs = _load_worker_specs(Path(workers_file).expanduser())
        result = start_workers(
            worker_names=str(args.workers),
            worker_specs=worker_specs,
            output_root=output_root,
            cache_dir=Path(args.cache_dir),
            python_exe=Path(args.python),
            stagger_seconds=max(0, int(args.stagger_seconds)),
            offline=bool(args.offline),
            restart=bool(args.restart),
        )
        print(json.dumps(result, indent=2))
        return 0
    if args.command == "stop":
        workers_file = _coerce_str(args.workers_file)
        worker_specs = None
        if workers_file:
            worker_specs = _load_worker_specs(Path(workers_file).expanduser())
        print(
            json.dumps(
                stop_workers(
                    worker_names=str(args.workers),
                    output_root=output_root,
                    worker_specs=worker_specs,
                ),
                indent=2,
            )
        )
        return 0
    if args.command == "status":
        print(json.dumps(status_workers(output_root=output_root), indent=2))
        return 0
    if args.command == "aggregate":
        result = aggregate_farm(
            output_root=output_root,
            min_total_trades=max(1, int(args.min_total_trades)),
            min_workers=max(1, int(args.min_workers)),
            min_passing_workers=max(1, int(args.min_passing_workers)),
            min_passing_window_pct=float(args.min_passing_window_pct),
            target_win_rate_pct=float(args.target_win_rate_pct),
            target_expectancy_pct=float(args.target_expectancy_pct),
            min_win_loss_ratio=float(args.min_win_loss_ratio),
            min_worker_win_loss_ratio=float(args.min_worker_win_loss_ratio),
        )
        promotable_count = sum(1 for item in result.get("leaderboard", []) if bool(item.get("promotable")))
        print(
            json.dumps(
                {
                    "generated_at": result.get("generated_at"),
                    "best": result["best"],
                    "row_count": result["row_count"],
                    "worker_count": result["worker_count"],
                    "promotable_count": promotable_count,
                    "requirements": result.get("requirements", {}),
                },
                indent=2,
            )
        )
        return 0
    raise ValueError(f"Unhandled command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
