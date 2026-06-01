"""Render single-service runner: starts trader loop + dashboard in one process."""

from __future__ import annotations

import csv
import json
import os
import socket
import shutil
import sys
import threading
import time
import traceback
import yfinance as yf
from datetime import datetime
from pathlib import Path
import math

from env_config import get_required_env, load_runtime_env

load_runtime_env()


def _force_writable_data_dir() -> None:
    current = (os.getenv("DATA_DIR") or "").strip()
    persistent_default = Path("/data")

    def _first_writable(paths: list[Path]) -> Path | None:
        for path in paths:
            try:
                path.mkdir(parents=True, exist_ok=True)
                probe = path / ".write_test"
                with probe.open("w", encoding="utf-8") as f:
                    f.write("ok")
                probe.unlink(missing_ok=True)
                return path
            except Exception:
                continue
        return None

    if not current:
        chosen = _first_writable([persistent_default, Path("/tmp/autotrader-data")])
        if chosen is None:
            chosen = Path("/tmp/autotrader-data")
            chosen.mkdir(parents=True, exist_ok=True)
        os.environ["DATA_DIR"] = str(chosen)
        return

    target = Path(current)
    try:
        target.mkdir(parents=True, exist_ok=True)
        probe = target / ".write_test"
        with probe.open("w", encoding="utf-8") as f:
            f.write("ok")
        probe.unlink(missing_ok=True)
    except Exception:
        fallback = _first_writable([persistent_default, Path("/tmp/autotrader-data")]) or Path("/tmp/autotrader-data")
        fallback.mkdir(parents=True, exist_ok=True)
        os.environ["DATA_DIR"] = str(fallback)
        print(
            f"[render_service] DATA_DIR '{current}' not writable. "
            f"Using '{fallback}'."
        )


def _migrate_runtime_files_to_active_data_dir() -> None:
    target_dir = Path((os.getenv("DATA_DIR") or "").strip() or "/tmp/autotrader-data")
    try:
        target_dir.mkdir(parents=True, exist_ok=True)
    except Exception:
        return

    runtime_files = (
        "trades.csv",
        "scan_log.csv",
        "runtime_state.json",
        "trading_control.json",
        "watchlist_control.json",
        "observation_log.csv",
        "feature_flags.json",
        "market_context.json",
        "candidate_queue.json",
    )

    legacy_candidates = [
        Path(__file__).resolve().parent,  # older default behavior: autotrader/ dir
        Path("/tmp/autotrader-data"),
    ]
    copied = 0
    for filename in runtime_files:
        target_file = target_dir / filename
        if target_file.exists():
            continue
        for legacy_dir in legacy_candidates:
            if legacy_dir.resolve() == target_dir.resolve():
                continue
            legacy_file = legacy_dir / filename
            if not legacy_file.exists():
                continue
            try:
                shutil.copy2(legacy_file, target_file)
                copied += 1
                break
            except Exception:
                continue
    if copied > 0:
        print(f"[render_service] Migrated {copied} runtime file(s) into DATA_DIR '{target_dir}'.")


_force_writable_data_dir()
_migrate_runtime_files_to_active_data_dir()
import config

from alerts import AlertManager
from broker import AlpacaBroker
from data import AlpacaDataClient
from dashboard import app
import desk_state
from main import main as trader_main
import market_context
import replay_farm
from replay_promotion import build_promotion_snapshot
from state_store import load_bot_state, save_bot_state
from trading_control import load_trading_control, set_manual_stop

try:
    import pytz
except Exception:  # noqa: BLE001
    pytz = None

ALERTS = AlertManager()
BROKER: AlpacaBroker | None = None
_BROKER_INIT_ERROR: str | None = None


def _position_qty_as_int(qty_value) -> int:
    try:
        return int(float(qty_value))
    except (TypeError, ValueError):
        return 0


def _can_bind_port(host: str, port: int) -> bool:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            probe.bind((host, int(port)))
        return True
    except OSError:
        return False


def _resolve_dashboard_port() -> int:
    raw_port = str(os.getenv("PORT", "5000") or "5000").strip()
    try:
        requested_port = int(raw_port)
    except ValueError:
        requested_port = 5000
    requested_port = max(1, min(65535, requested_port))

    candidates: list[int] = []
    for candidate in (requested_port, 5000, 5051, 5052, 8080):
        if candidate not in candidates:
            candidates.append(candidate)

    for candidate in candidates:
        if _can_bind_port("0.0.0.0", candidate):
            if candidate != requested_port:
                print(
                    f"[render_service] Requested PORT={requested_port} unavailable; "
                    f"falling back to {candidate}."
                )
            os.environ["PORT"] = str(candidate)
            return candidate

    os.environ["PORT"] = str(requested_port)
    return requested_port


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value))
    except Exception:  # noqa: BLE001
        return None


def _is_trader_loop_stale(runtime_state: dict) -> bool:
    heartbeat_raw = str(runtime_state.get("last_trader_heartbeat_et", "") or "")
    heartbeat_dt = _parse_iso_datetime(heartbeat_raw)
    if heartbeat_dt is None:
        return True
    now_dt = datetime.now(heartbeat_dt.tzinfo) if heartbeat_dt.tzinfo is not None else datetime.now()
    heartbeat_age_seconds = int((now_dt - heartbeat_dt).total_seconds())
    stale_after = max(60, int(config.LOOP_INTERVAL_SECONDS) * 4)
    return heartbeat_age_seconds > stale_after


def _heartbeat_age_seconds(runtime_state: dict) -> int | None:
    heartbeat_raw = str(runtime_state.get("last_trader_heartbeat_et", "") or "")
    heartbeat_dt = _parse_iso_datetime(heartbeat_raw)
    if heartbeat_dt is None:
        return None
    now_dt = datetime.now(heartbeat_dt.tzinfo) if heartbeat_dt.tzinfo is not None else datetime.now()
    return max(0, int((now_dt - heartbeat_dt).total_seconds()))


def _enum_text(value) -> str:
    try:
        value = getattr(value, "value", value)
    except Exception:
        pass
    text = str(value or "").strip().lower()
    if "." in text:
        text = text.split(".")[-1]
    return text


def _now_et_dt() -> datetime:
    if pytz is not None:
        try:
            return datetime.now(pytz.timezone(str(getattr(config, "EASTERN_TZ", "US/Eastern"))))
        except Exception:
            pass
    return datetime.utcnow()


def _as_et_datetime(value) -> datetime | None:
    if value is None:
        return None
    tz = pytz.timezone(str(getattr(config, "EASTERN_TZ", "US/Eastern"))) if pytz is not None else None
    if isinstance(value, datetime):
        if tz is not None:
            if value.tzinfo is None:
                return tz.localize(value)
            return value.astimezone(tz)
        return value
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if tz is not None:
            if parsed.tzinfo is None:
                return tz.localize(parsed)
            return parsed.astimezone(tz)
        return parsed
    except Exception:
        return None


def _fetch_vix_level() -> float | None:
    try:
        ticker = yf.Ticker("^VIX")
        fast = getattr(ticker, "fast_info", None)
        if fast is not None:
            price = getattr(fast, "last_price", None)
            if price is None and isinstance(fast, dict):
                price = fast.get("last_price")
            if price is not None:
                value = float(price)
                if value > 0:
                    return value
    except Exception as exc:  # noqa: BLE001
        print(f"[render_service] VIX lookup failed: {exc}")
    return None


def _runtime_position_entry_time(runtime_state: dict, symbol: str) -> datetime | None:
    meta = dict((runtime_state.get("open_trade_meta") or {}).get(symbol) or {})
    for field in ("entry_time_iso", "timestamp"):
        parsed = _as_et_datetime(meta.get(field))
        if parsed is not None:
            return parsed
    return None


def _recent_filled_buy_entry_time(broker: AlpacaBroker, symbol: str, now_et: datetime) -> datetime | None:
    latest: datetime | None = None
    try:
        orders = broker.get_recent_orders(limit=100)
    except Exception:
        return None
    want = str(symbol or "").upper()
    for order in orders:
        if str(getattr(order, "symbol", "") or "").upper() != want:
            continue
        if _enum_text(getattr(order, "side", "")) != "buy":
            continue
        if _enum_text(getattr(order, "status", "")) != "filled":
            continue
        filled_at = _as_et_datetime(getattr(order, "filled_at", None) or getattr(order, "submitted_at", None))
        if filled_at is None or filled_at.date() != now_et.date():
            continue
        if latest is None or filled_at > latest:
            latest = filled_at
    return latest


def _minimum_hold_remaining_seconds(
    *,
    runtime_state: dict,
    broker: AlpacaBroker,
    symbol: str,
    now_et: datetime,
) -> int:
    min_hold_seconds = max(0, int(float(getattr(config, "ANTI_CHURN_HOLD_MINUTES", 10) or 10) * 60))
    if min_hold_seconds <= 0:
        return 0
    entry_time = _runtime_position_entry_time(runtime_state, symbol)
    if entry_time is None:
        entry_time = _recent_filled_buy_entry_time(broker, symbol, now_et)
    if entry_time is None:
        return 0
    elapsed = max(0, int((now_et - entry_time).total_seconds()))
    return max(0, min_hold_seconds - elapsed)


def _watchdog_hard_stale_seconds() -> int:
    default = max(1200, int(getattr(config, "LOOP_INTERVAL_SECONDS", 30) or 30) * 30)
    raw = str(os.getenv("TRADER_HEARTBEAT_STALE_SECONDS", "") or "").strip()
    if not raw:
        return default
    try:
        value = int(float(raw))
    except ValueError:
        value = default
    return max(300, min(86400, value))


def _watchdog_check_seconds() -> int:
    raw = str(os.getenv("TRADER_WATCHDOG_CHECK_SECONDS", "") or "").strip()
    default = 30
    if not raw:
        return default
    try:
        value = int(float(raw))
    except ValueError:
        value = default
    return max(10, min(600, value))


def _env_bool(name: str, default: bool) -> bool:
    raw = str(os.getenv(name, "") or "").strip().lower()
    if not raw:
        return bool(default)
    return raw in {"1", "true", "yes", "y", "on"}


def _env_float(name: str, default: float, *, minimum: float, maximum: float) -> float:
    raw = str(os.getenv(name, "") or "").strip()
    if not raw:
        value = float(default)
    else:
        try:
            value = float(raw)
        except ValueError:
            value = float(default)
    return max(float(minimum), min(float(maximum), float(value)))


def _historical_learning_enabled() -> bool:
    # Keep live trader memory headroom by default; replay workers can be enabled explicitly.
    return _env_bool("ENABLE_HISTORICAL_REPLAY_LEARNING", False)


def _historical_learning_allowed_during_market_hours() -> bool:
    return _env_bool("ENABLE_HISTORICAL_REPLAY_DURING_MARKET_HOURS", False)


def _is_regular_market_hours(now_et: datetime | None = None) -> bool:
    now = now_et or _now_et_dt()
    if now.weekday() >= 5:
        return False
    h = now.hour
    m = now.minute
    minutes = h * 60 + m
    return (9 * 60 + 30) <= minutes < (16 * 60)


def _historical_learning_offline() -> bool:
    return _env_bool("HISTORICAL_REPLAY_OFFLINE", False)


def _historical_learning_check_seconds() -> int:
    raw = str(os.getenv("HISTORICAL_REPLAY_HEALTH_CHECK_SECONDS", "") or "").strip()
    default = 120
    if not raw:
        return default
    try:
        value = int(float(raw))
    except ValueError:
        value = default
    return max(20, min(3600, value))


def _historical_learning_restart_cooldown_seconds() -> int:
    raw = str(os.getenv("HISTORICAL_REPLAY_RESTART_COOLDOWN_SECONDS", "") or "").strip()
    default = 180
    if not raw:
        return default
    try:
        value = int(float(raw))
    except ValueError:
        value = default
    return max(30, min(7200, value))


def _historical_learning_stagger_seconds() -> int:
    raw = str(os.getenv("HISTORICAL_REPLAY_STAGGER_SECONDS", "") or "").strip()
    default = 30
    if not raw:
        return default
    try:
        value = int(float(raw))
    except ValueError:
        value = default
    return max(0, min(300, value))


def _historical_learning_output_root() -> Path:
    raw = str(os.getenv("REPLAY_FARM_OUTPUT_ROOT", "") or "").strip()
    if raw:
        return Path(raw).expanduser()
    return Path(config.DATA_DIR) / "replay_farm"


def _historical_learning_cache_dir() -> Path:
    raw = str(os.getenv("REPLAY_FARM_CACHE_DIR", "") or "").strip()
    if raw:
        return Path(raw).expanduser()
    return Path(config.DATA_DIR) / "historical_cache"


def _historical_learning_python_exe() -> Path:
    raw = str(os.getenv("REPLAY_FARM_PYTHON_EXE", "") or "").strip()
    if raw:
        return Path(raw).expanduser()
    return Path(sys.executable)


def _historical_learning_workers_file() -> Path | None:
    raw = str(os.getenv("REPLAY_FARM_WORKERS_FILE", "") or "").strip()
    if raw:
        candidate = Path(raw).expanduser()
        return candidate if candidate.exists() else None
    default_file = Path(__file__).resolve().with_name("replay_workers.json")
    return default_file if default_file.exists() else None


_REPLAY_AUTO_PROMOTE_OVERRIDE_KEYS = (
    "MIN_SIGNAL_SCORE",
    "DIRECTION_CONVICTION_MIN",
    "DIRECTION_MIN_ALIGNED_VOTES",
    "RVOL_MIN",
    "OPENING_RVOL_MIN",
    "RVOL_RELAXED_MIN",
    "EXECUTION_MIN_RVOL_AFTER_IGNORE",
    "ATR_PCT_MIN",
    "MOVEMENT_FORCE_MIN_PCT",
    "FAST_START_MIN_DIRECTION_SCORE",
    "FAST_START_MIN_RVOL",
    "FAST_START_MIN_ABS_ROC_PCT",
    "FAST_START_MIN_VWAP_DISTANCE_PCT",
    "OPENING_FAST_START_MIN_DIRECTION_SCORE",
    "OPENING_FAST_START_MIN_RVOL",
    "OPENING_FAST_START_MIN_ABS_ROC_PCT",
    "OPENING_FAST_START_MIN_VWAP_DISTANCE_PCT",
)
_REPLAY_AUTO_PROMOTE_BASELINE: dict[str, float] = {
    key: float(getattr(config, key, 0.0) or 0.0)
    for key in _REPLAY_AUTO_PROMOTE_OVERRIDE_KEYS
}


def _replay_auto_promote_enabled() -> bool:
    return _env_bool("ENABLE_REPLAY_AUTO_PROMOTE", True)


def _replay_auto_promote_paper_only() -> bool:
    return _env_bool("REPLAY_AUTO_PROMOTE_PAPER_ONLY", True)


def _replay_auto_promote_min_total_trades() -> int:
    return int(_env_float("REPLAY_AUTO_PROMOTE_MIN_TOTAL_TRADES", 100, minimum=1, maximum=1000000))


def _replay_auto_promote_min_workers(expected_worker_count: int) -> int:
    default = 2 if expected_worker_count >= 2 else 1
    return int(_env_float("REPLAY_AUTO_PROMOTE_MIN_WORKERS", default, minimum=1, maximum=500))


def _replay_auto_promote_min_passing_workers(expected_worker_count: int) -> int:
    default = 2 if expected_worker_count >= 2 else 1
    return int(_env_float("REPLAY_AUTO_PROMOTE_MIN_PASSING_WORKERS", default, minimum=1, maximum=500))


def _replay_auto_promote_min_passing_window_pct() -> float:
    return _env_float("REPLAY_AUTO_PROMOTE_MIN_PASSING_WINDOW_PCT", 40.0, minimum=0.0, maximum=100.0)


def _replay_auto_promote_target_win_rate_pct() -> float:
    return _env_float("REPLAY_AUTO_PROMOTE_TARGET_WIN_RATE_PCT", 55.0, minimum=0.0, maximum=100.0)


def _replay_auto_promote_target_expectancy_pct() -> float:
    return _env_float("REPLAY_AUTO_PROMOTE_TARGET_EXPECTANCY_PCT", 0.05, minimum=-100.0, maximum=100.0)


def _replay_auto_promote_min_win_loss_ratio() -> float:
    return _env_float("REPLAY_AUTO_PROMOTE_MIN_WIN_LOSS_RATIO", 1.25, minimum=0.0, maximum=1000.0)


def _replay_auto_promote_min_worker_win_loss_ratio() -> float:
    return _env_float("REPLAY_AUTO_PROMOTE_MIN_WORKER_WIN_LOSS_RATIO", 1.15, minimum=0.0, maximum=1000.0)


def _replay_auto_promote_check_seconds() -> int:
    return int(_env_float("REPLAY_AUTO_PROMOTE_CHECK_SECONDS", 300, minimum=30, maximum=3600))


def _replay_auto_promote_revert_when_not_promotable() -> bool:
    return _env_bool("REPLAY_AUTO_PROMOTE_REVERT_WHEN_NOT_PROMOTABLE", False)


def _apply_replay_auto_promote_overrides(overrides: dict[str, Any]) -> dict[str, float]:
    applied: dict[str, float] = {}
    for key in _REPLAY_AUTO_PROMOTE_OVERRIDE_KEYS:
        if key not in overrides:
            continue
        try:
            value = float(overrides.get(key))
        except (TypeError, ValueError):
            continue
        if not math.isfinite(value):
            continue
        setattr(config, key, value)
        applied[key] = float(value)
    if applied:
        _write_replay_promoted_overrides(applied)
    return applied


def _apply_replay_auto_promote_baseline() -> dict[str, float]:
    for key, value in _REPLAY_AUTO_PROMOTE_BASELINE.items():
        setattr(config, key, float(value))
    _write_replay_promoted_overrides({})
    return dict(_REPLAY_AUTO_PROMOTE_BASELINE)


def _write_replay_promoted_overrides(overrides: dict[str, float]) -> None:
    path = Path(getattr(config, "REPLAY_PROMOTED_OVERRIDES_PATH", Path(config.DATA_DIR) / "replay_promoted_overrides.json"))
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "updated_at_et": _now_et_iso(),
        "source": "replay_auto_promote",
        "allowed_keys": list(_REPLAY_AUTO_PROMOTE_OVERRIDE_KEYS),
        "overrides": dict(overrides),
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _replay_auto_promote_signature(candidate: str, overrides: dict[str, Any]) -> str:
    parts = [str(candidate or "").strip()]
    for key in sorted(_REPLAY_AUTO_PROMOTE_OVERRIDE_KEYS):
        if key not in overrides:
            continue
        try:
            value = float(overrides.get(key))
        except (TypeError, ValueError):
            continue
        if not math.isfinite(value):
            continue
        parts.append(f"{key}={value:.8f}")
    return "|".join(parts)


def _replay_auto_promote_events_path() -> Path:
    return Path(config.DATA_DIR) / "replay_auto_promote_events.csv"


def _append_replay_auto_promote_event(status: dict[str, Any]) -> None:
    path = _replay_auto_promote_events_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = [
        "timestamp_et",
        "enabled",
        "paper_mode",
        "candidate",
        "promotable",
        "applied",
        "reason",
        "signature",
        "worker_filter_json",
        "overrides_json",
        "best_json",
        "aggregate_requirements_json",
    ]
    row = {
        "timestamp_et": str(status.get("updated_at_et", "") or _now_et_iso()),
        "enabled": bool(status.get("enabled", False)),
        "paper_mode": bool(status.get("paper_mode", bool(getattr(config, "PAPER", True)))),
        "candidate": str(status.get("candidate", "") or ""),
        "promotable": bool(status.get("promotable", False)),
        "applied": bool(status.get("applied", False)),
        "reason": str(status.get("reason", "") or ""),
        "signature": str(status.get("signature", "") or ""),
        "worker_filter_json": json.dumps(list(status.get("worker_filter") or []), sort_keys=True),
        "overrides_json": json.dumps(dict(status.get("overrides") or {}), sort_keys=True),
        "best_json": json.dumps(dict(status.get("best") or {}), sort_keys=True),
        "aggregate_requirements_json": json.dumps(dict(status.get("aggregate_requirements") or {}), sort_keys=True),
    }
    write_header = not path.exists()
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        if write_header:
            writer.writeheader()
        writer.writerow(row)


def _evaluate_replay_auto_promotion(*, output_root: Path, worker_names: set[str], current_signature: str) -> tuple[dict[str, Any], str]:
    status: dict[str, Any] = {
        "enabled": _replay_auto_promote_enabled(),
        "paper_mode": bool(getattr(config, "PAPER", True)),
        "worker_filter": sorted(worker_names),
        "candidate": "",
        "promotable": False,
        "applied": False,
        "overrides": {},
        "signature": current_signature,
        "reason": "",
    }
    if not status["enabled"]:
        status["reason"] = "disabled"
        return status, current_signature
    if _replay_auto_promote_paper_only() and not bool(getattr(config, "PAPER", True)):
        status["reason"] = "paper_only_guard"
        return status, current_signature

    min_workers = _replay_auto_promote_min_workers(len(worker_names))
    min_passing_workers = _replay_auto_promote_min_passing_workers(len(worker_names))
    min_workers = min(min_workers, max(1, len(worker_names)))
    min_passing_workers = min(min_passing_workers, max(1, len(worker_names)))

    aggregate = replay_farm.aggregate_farm(
        output_root=output_root,
        min_total_trades=_replay_auto_promote_min_total_trades(),
        min_workers=min_workers,
        min_passing_workers=min_passing_workers,
        min_passing_window_pct=_replay_auto_promote_min_passing_window_pct(),
        target_win_rate_pct=_replay_auto_promote_target_win_rate_pct(),
        target_expectancy_pct=_replay_auto_promote_target_expectancy_pct(),
        min_win_loss_ratio=_replay_auto_promote_min_win_loss_ratio(),
        min_worker_win_loss_ratio=_replay_auto_promote_min_worker_win_loss_ratio(),
        worker_names=set(worker_names),
    )
    snapshot = build_promotion_snapshot(
        aggregate_payload=aggregate,
        worker_names=set(worker_names),
        allowed_override_keys=_REPLAY_AUTO_PROMOTE_OVERRIDE_KEYS,
    )
    status["candidate"] = str(snapshot.get("candidate", "") or "")
    status["promotable"] = bool(snapshot.get("promotable", False))
    status["aggregate_requirements"] = dict(aggregate.get("requirements", {}))
    status["best"] = snapshot.get("best", {}) if isinstance(snapshot.get("best"), dict) else {}
    status["override_source"] = snapshot.get("override_source", {}) if isinstance(snapshot.get("override_source"), dict) else {}

    overrides = snapshot.get("overrides", {}) if isinstance(snapshot.get("overrides"), dict) else {}
    if not status["promotable"] or not overrides:
        status["reason"] = "not_promotable_or_no_overrides"
        if _replay_auto_promote_revert_when_not_promotable() and current_signature:
            baseline = _apply_replay_auto_promote_baseline()
            status["applied"] = True
            status["overrides"] = baseline
            status["signature"] = ""
            status["reason"] = "reverted_to_baseline"
            return status, ""
        return status, current_signature

    signature = _replay_auto_promote_signature(str(status["candidate"]), overrides)
    status["signature"] = signature
    if signature and signature != current_signature:
        applied = _apply_replay_auto_promote_overrides(overrides)
        status["overrides"] = applied
        status["applied"] = bool(applied)
        status["reason"] = "applied_new_candidate" if applied else "candidate_missing_valid_overrides"
        return status, signature if applied else current_signature

    status["overrides"] = dict(overrides)
    status["reason"] = "already_applied"
    return status, current_signature


def _load_historical_learning_specs() -> tuple[dict[str, replay_farm.FarmWorkerSpec], str]:
    workers_file = _historical_learning_workers_file()
    if workers_file is not None:
        try:
            specs = replay_farm._load_worker_specs(workers_file.resolve())
            if specs:
                return specs, f"workers_file={workers_file}"
        except Exception as exc:  # noqa: BLE001
            print(f"[render_service] historical replay worker-file load failed: {exc}")
    return replay_farm.default_worker_specs(), "built_in_defaults"


def _run_historical_learning_supervisor() -> None:
    output_root = _historical_learning_output_root()
    cache_dir = _historical_learning_cache_dir()
    python_exe = _historical_learning_python_exe()
    check_seconds = _historical_learning_check_seconds()
    restart_cooldown_seconds = _historical_learning_restart_cooldown_seconds()
    stagger_seconds = _historical_learning_stagger_seconds()
    offline = _historical_learning_offline()

    specs, spec_source = _load_historical_learning_specs()
    if not specs:
        print("[render_service] historical replay supervisor disabled: no worker specs available.")
        return
    expected_names = list(specs.keys())
    expected_name_set = set(expected_names)
    auto_promote_enabled = _replay_auto_promote_enabled()
    auto_promote_check_seconds = _replay_auto_promote_check_seconds()
    print(
        "[render_service] Historical replay supervisor enabled "
        f"(workers={len(expected_names)}, offline={offline}, check={check_seconds}s, "
        f"source={spec_source}, output_root={output_root}, cache_dir={cache_dir})."
    )
    if auto_promote_enabled:
        print(
            "[render_service] Replay auto-promote enabled "
            f"(paper_only={_replay_auto_promote_paper_only()}, check={auto_promote_check_seconds}s, "
            f"override_keys={','.join(_REPLAY_AUTO_PROMOTE_OVERRIDE_KEYS)})."
        )
    else:
        print("[render_service] Replay auto-promote disabled by ENABLE_REPLAY_AUTO_PROMOTE=false.")
        _patch_runtime_state(
            {
                "replay_auto_promote_status": {
                    "enabled": False,
                    "reason": "disabled",
                    "updated_at_et": _now_et_iso(),
                    "worker_filter": sorted(expected_name_set),
                }
            }
        )

    last_restart_attempt = 0.0
    last_auto_promote_signature = ""
    last_auto_promote_digest = ""
    next_auto_promote_eval_at = 0.0
    while True:
        try:
            if _is_regular_market_hours() and not _historical_learning_allowed_during_market_hours():
                running_status = replay_farm.status_workers(output_root=output_root)
                running_names = [
                    str(item.get("worker", "") or "")
                    for item in running_status.get("workers", [])
                    if isinstance(item, dict) and bool(item.get("running", False))
                ]
                if running_names:
                    worker_names = ",".join(running_names)
                    replay_farm.stop_workers(worker_names=worker_names, output_root=output_root, worker_specs=specs)
                    print(
                        "[render_service] Historical replay paused during regular market hours "
                        f"(workers_stopped={worker_names})."
                    )
                time.sleep(check_seconds)
                continue
            status = replay_farm.status_workers(output_root=output_root)
            running_by_name = {
                str(item.get("worker", "") or ""): bool(item.get("running", False))
                for item in status.get("workers", [])
                if isinstance(item, dict)
            }
            down_or_missing = [name for name in expected_names if not running_by_name.get(name, False)]
            if down_or_missing:
                now = time.time()
                if now - last_restart_attempt >= restart_cooldown_seconds:
                    worker_names = ",".join(down_or_missing)
                    print(
                        "[render_service] Historical replay: starting/restarting workers "
                        f"({worker_names})."
                    )
                    result = replay_farm.start_workers(
                        worker_names=worker_names,
                        worker_specs=specs,
                        output_root=output_root,
                        cache_dir=cache_dir,
                        python_exe=python_exe,
                        stagger_seconds=stagger_seconds,
                        offline=offline,
                        restart=True,
                    )
                    started_count = len(result.get("started", []))
                    skipped_count = len(result.get("skipped", []))
                    print(
                        "[render_service] Historical replay supervisor action complete "
                        f"(started={started_count}, skipped={skipped_count})."
                    )
                    last_restart_attempt = now
                else:
                    remaining = int(restart_cooldown_seconds - (now - last_restart_attempt))
                    print(
                        "[render_service] Historical replay: workers down but restart cooldown active "
                        f"({remaining}s remaining)."
                    )
        except Exception as exc:  # noqa: BLE001
            print(f"[render_service] historical replay supervisor error: {exc}")

        if auto_promote_enabled:
            now = time.time()
            if now >= next_auto_promote_eval_at:
                try:
                    promote_status, last_auto_promote_signature = _evaluate_replay_auto_promotion(
                        output_root=output_root,
                        worker_names=expected_name_set,
                        current_signature=last_auto_promote_signature,
                    )
                    promote_status["updated_at_et"] = _now_et_iso()
                    _patch_runtime_state({"replay_auto_promote_status": promote_status})
                    digest = (
                        f"{promote_status.get('candidate', '')}|"
                        f"{promote_status.get('promotable', False)}|"
                        f"{promote_status.get('reason', '')}|"
                        f"{promote_status.get('signature', '')}"
                    )
                    if digest != last_auto_promote_digest:
                        print(
                            "[render_service] Replay auto-promote status "
                            f"(candidate={promote_status.get('candidate', '')}, "
                            f"promotable={promote_status.get('promotable', False)}, "
                            f"reason={promote_status.get('reason', '')}, "
                            f"applied={promote_status.get('applied', False)})."
                        )
                        _append_replay_auto_promote_event(promote_status)
                        last_auto_promote_digest = digest
                except Exception as exc:  # noqa: BLE001
                    print(f"[render_service] replay auto-promote evaluation error: {exc}")
                next_auto_promote_eval_at = now + auto_promote_check_seconds
        time.sleep(check_seconds)


def _position_unrealized_usd(pos) -> float | None:
    try:
        pl_raw = float(getattr(pos, "unrealized_pl", 0) or 0)
        if math.isfinite(pl_raw):
            return pl_raw
    except (TypeError, ValueError):
        pass
    try:
        qty = _position_qty_as_int(getattr(pos, "qty", 0))
        entry = float(getattr(pos, "avg_entry_price", 0) or 0)
        current = float(getattr(pos, "current_price", 0) or 0)
        if qty > 0 and entry > 0 and current > 0:
            return (current - entry) * qty * 100.0
    except (TypeError, ValueError):
        pass
    return None


def _broker() -> AlpacaBroker:
    global BROKER, _BROKER_INIT_ERROR
    if BROKER is None:
        try:
            api_key = get_required_env("ALPACA_API_KEY")
            secret_key = get_required_env("ALPACA_SECRET_KEY")
            BROKER = AlpacaBroker(api_key, secret_key, paper=config.PAPER)
            _BROKER_INIT_ERROR = None
        except Exception as exc:  # noqa: BLE001
            _BROKER_INIT_ERROR = str(exc)
            raise
    return BROKER


def _now_et_iso() -> str:
    if pytz is not None:
        try:
            return datetime.now(pytz.timezone("US/Eastern")).isoformat()
        except Exception:  # noqa: BLE001
            pass
    return datetime.utcnow().isoformat()


def _patch_runtime_state(updates: dict) -> None:
    try:
        state = load_bot_state()
        if not isinstance(state, dict):
            state = {}
        state.update(updates)
        save_bot_state(state)
    except Exception as exc:  # noqa: BLE001
        print(f"[render_service] runtime state patch failed: {exc}")


def _print_startup_readiness() -> None:
    data_dir = Path(str(getattr(config, "DATA_DIR", "") or os.getenv("DATA_DIR", "")).strip() or "/tmp/autotrader-data")
    token_enabled = bool(str(getattr(config, "DASHBOARD_CONTROL_TOKEN", "") or "").strip())
    live_options_keys = bool(
        str(getattr(config, "ALPACA_LIVE_API_KEY", "") or "").strip()
        and str(getattr(config, "ALPACA_LIVE_SECRET_KEY", "") or "").strip()
    )
    control = load_trading_control()

    print("[render_service] STARTUP READINESS")
    print(f"[render_service] paper_mode={bool(getattr(config, 'PAPER', True))}")
    print(f"[render_service] alpaca_key_present={bool(str(os.getenv('ALPACA_API_KEY', '')).strip())}")
    print(f"[render_service] alpaca_secret_present={bool(str(os.getenv('ALPACA_SECRET_KEY', '')).strip())}")
    print(f"[render_service] live_options_keys_present={live_options_keys}")
    print(f"[render_service] data_dir={data_dir} writable={data_dir.exists() and os.access(data_dir, os.W_OK)}")
    print(f"[render_service] dashboard_control_auth_enabled={token_enabled}")
    print(f"[render_service] manual_stop={bool(control.get('manual_stop', False))}")
    print(f"[render_service] dry_run={bool(control.get('dry_run', False))}")
    print(f"[render_service] historical_replay_learning_enabled={_historical_learning_enabled()}")
    print(f"[render_service] historical_replay_offline={_historical_learning_offline()}")
    print(f"[render_service] replay_auto_promote_enabled={_replay_auto_promote_enabled()}")
    print(f"[render_service] replay_auto_promote_paper_only={_replay_auto_promote_paper_only()}")


def _apply_boot_auto_resume() -> None:
    env_value = str(os.getenv("AUTO_RESUME_TRADING_ON_BOOT", "") or "").strip().lower()
    if env_value:
        auto_resume_enabled = env_value in {"1", "true", "yes", "y", "on"}
    else:
        auto_resume_enabled = bool(getattr(config, "AUTO_RESUME_TRADING_ON_BOOT", True))
    if not auto_resume_enabled:
        return
    try:
        control = load_trading_control()
        if bool(control.get("manual_stop", False)):
            updated = set_manual_stop(False, reason="boot_auto_resume")
            print(
                "[render_service] AUTO_RESUME_TRADING_ON_BOOT cleared manual_stop "
                f"(previous reason={str(control.get('reason', '') or '')!r}, "
                f"updated_at={str(updated.get('updated_at_et', '') or '')!r})."
            )
    except Exception as exc:  # noqa: BLE001
        print(f"[render_service] boot auto-resume failed: {exc}")


def _run_trader_forever() -> None:
    restart_count = 0
    while True:
        restart_count += 1
        _patch_runtime_state(
            {
                "trader_thread_last_start_et": _now_et_iso(),
                "trader_thread_restart_count": restart_count,
            }
        )
        try:
            trader_main()
        except Exception as exc:  # noqa: BLE001
            print(f"[render_service] Trader crashed: {exc}")
            traceback.print_exc()
            _patch_runtime_state(
                {
                    "trader_thread_last_crash_et": _now_et_iso(),
                    "trader_thread_last_crash": str(exc)[:500],
                }
            )
            ALERTS.send(
                "trader_crash",
                f"Trader crashed and will restart in 30 seconds: {exc}",
                level="error",
                dedupe_key=f"trader-crash-{int(time.time() // 60)}",
            )
        finally:
            _patch_runtime_state({"trader_thread_last_stop_et": _now_et_iso()})
        # Always restart trader loop so service can stay 24/7.
        time.sleep(30)


def _run_independent_stoploss_guard() -> None:
    guard_sleep_seconds = max(1, int(getattr(config, "INDEPENDENT_STOPLOSS_INTERVAL_SECONDS", 2) or 2))
    require_stale_loop = bool(getattr(config, "INDEPENDENT_STOPLOSS_REQUIRE_STALE_LOOP", False))
    broker_missing_warned = False
    while True:
        try:
            runtime_state = load_bot_state()
            if not isinstance(runtime_state, dict):
                runtime_state = {}
            if require_stale_loop and (not _is_trader_loop_stale(runtime_state)):
                time.sleep(guard_sleep_seconds)
                continue

            try:
                broker = _broker()
            except Exception as exc:  # noqa: BLE001
                if not broker_missing_warned:
                    print(f"[render_service] independent stop-loss guard unavailable: {exc}")
                    broker_missing_warned = True
                time.sleep(max(guard_sleep_seconds, 30))
                continue
            broker_missing_warned = False
            positions = broker.get_open_option_positions()
            stop_cap = abs(float(getattr(config, "STOP_LOSS_USD", 10.0) or 10.0))
            if stop_cap <= 0:
                time.sleep(guard_sleep_seconds)
                continue

            for pos in positions:
                symbol = str(getattr(pos, "symbol", "") or "")
                qty = _position_qty_as_int(getattr(pos, "qty", 0))
                if not symbol or qty == 0:
                    continue
                if qty < 0:
                    cover_qty = abs(qty)
                    if broker.has_open_order_for_symbol(symbol=symbol, side="buy"):
                        continue
                    try:
                        broker.cover_option_market(symbol, cover_qty)
                        _patch_runtime_state(
                            {
                                "independent_short_guard_last_trigger_et": _now_et_iso(),
                                "independent_short_guard_last_symbol": symbol,
                                "independent_short_guard_last_qty": cover_qty,
                            }
                        )
                        print(f"[render_service] INDEPENDENT_SHORT_GUARD covered {symbol} qty={cover_qty}")
                        ALERTS.send(
                            "independent_short_guard",
                            f"Independent short guard bought to cover {symbol} qty={cover_qty}.",
                            level="error",
                            dedupe_key=f"independent-short-{symbol}-{int(time.time() // 30)}",
                        )
                    except Exception as exc:  # noqa: BLE001
                        print(f"[render_service] independent short cover failed for {symbol}: {exc}")
                    continue
                unrealized_usd = _position_unrealized_usd(pos)
                if unrealized_usd is None or unrealized_usd > -stop_cap:
                    continue
                now_et = _now_et_dt()
                min_hold_remaining = _minimum_hold_remaining_seconds(
                    runtime_state=runtime_state,
                    broker=broker,
                    symbol=symbol,
                    now_et=now_et,
                )
                if min_hold_remaining > 0:
                    _patch_runtime_state(
                        {
                            "independent_stoploss_last_deferred_et": now_et.isoformat(),
                            "independent_stoploss_last_deferred_symbol": symbol,
                            "independent_stoploss_last_deferred_unrealized_usd": round(float(unrealized_usd), 4),
                            "independent_stoploss_last_deferred_seconds_remaining": min_hold_remaining,
                        }
                    )
                    print(
                        f"[render_service] INDEPENDENT_STOPLOSS deferred {symbol} "
                        f"for minimum hold ({min_hold_remaining}s remaining); "
                        f"unrealized_usd={unrealized_usd:.2f} cap=-{stop_cap:.2f}"
                    )
                    continue
                if broker.has_open_order_for_symbol(symbol=symbol, side="sell"):
                    continue

                try:
                    broker.close_option_market(symbol, qty)
                    _patch_runtime_state(
                        {
                            "independent_stoploss_last_trigger_et": _now_et_iso(),
                            "independent_stoploss_last_symbol": symbol,
                            "independent_stoploss_last_unrealized_usd": round(float(unrealized_usd), 4),
                            "independent_stoploss_last_qty": qty,
                        }
                    )
                    print(
                        f"[render_service] INDEPENDENT_STOPLOSS closed {symbol} qty={qty} "
                        f"unrealized_usd={unrealized_usd:.2f} cap=-{stop_cap:.2f}"
                    )
                    ALERTS.send(
                        "independent_stoploss",
                        (
                            f"Independent stop-loss closed {symbol} qty={qty} "
                            f"unrealized=${unrealized_usd:.2f} (cap -${stop_cap:.2f})."
                        ),
                        level="warning",
                        dedupe_key=f"independent-stoploss-{symbol}-{int(time.time() // 30)}",
                    )
                except Exception as exc:  # noqa: BLE001
                    print(f"[render_service] independent stop-loss close failed for {symbol}: {exc}")
        except Exception as exc:  # noqa: BLE001
            print(f"[render_service] independent stop-loss guard error: {exc}")
        time.sleep(guard_sleep_seconds)


def _run_trader_watchdog(trader_thread: threading.Thread) -> None:
    hard_stale_seconds = _watchdog_hard_stale_seconds()
    check_seconds = _watchdog_check_seconds()
    print(
        f"[render_service] Trader watchdog enabled "
        f"(check_every={check_seconds}s, hard_stale={hard_stale_seconds}s)."
    )
    while True:
        try:
            if not trader_thread.is_alive():
                _patch_runtime_state(
                    {
                        "trader_watchdog_last_restart_request_et": _now_et_iso(),
                        "trader_watchdog_reason": "trader_thread_not_alive",
                    }
                )
                print("[render_service] WATCHDOG: trader thread not alive; forcing process restart.")
                ALERTS.send(
                    "trader_watchdog_restart",
                    "Trader watchdog requested restart: trader thread not alive.",
                    level="error",
                    dedupe_key=f"watchdog-dead-{int(time.time() // 300)}",
                )
                time.sleep(2)
                os._exit(1)

            runtime_state = load_bot_state()
            if not isinstance(runtime_state, dict):
                runtime_state = {}
            heartbeat_age = _heartbeat_age_seconds(runtime_state)
            if heartbeat_age is not None and heartbeat_age >= hard_stale_seconds:
                _patch_runtime_state(
                    {
                        "trader_watchdog_last_restart_request_et": _now_et_iso(),
                        "trader_watchdog_reason": f"heartbeat_stale_{heartbeat_age}s",
                        "trader_watchdog_last_heartbeat_age_seconds": heartbeat_age,
                    }
                )
                print(
                    "[render_service] WATCHDOG: heartbeat stale "
                    f"({heartbeat_age}s >= {hard_stale_seconds}s); forcing process restart."
                )
                ALERTS.send(
                    "trader_watchdog_restart",
                    (
                        "Trader watchdog requested restart: "
                        f"heartbeat stale for {heartbeat_age}s (threshold {hard_stale_seconds}s)."
                    ),
                    level="error",
                    dedupe_key=f"watchdog-stale-{int(time.time() // 300)}",
                )
                time.sleep(2)
                os._exit(1)
        except Exception as exc:  # noqa: BLE001
            print(f"[render_service] WATCHDOG error: {exc}")
        time.sleep(check_seconds)


def _run_market_context_worker() -> None:
    print("[render_service] Market context worker enabled.")
    try:
        api_key = get_required_env("ALPACA_API_KEY")
        secret_key = get_required_env("ALPACA_SECRET_KEY")
        data_client = AlpacaDataClient(api_key, secret_key, paper=config.PAPER)
    except Exception as exc:  # noqa: BLE001
        print(f"[render_service] Market context worker unavailable: {exc}")
        return

    sleep_seconds = max(5, int(getattr(config, "MARKET_CONTEXT_REFRESH_SECONDS", 10) or 10))
    while True:
        try:
            now_et = _now_et_dt()
            vix_value = _fetch_vix_level()
            context = market_context.build_market_context(data_client, now_et, vix_value=vix_value)
            desk_state.save_market_context(context)
        except Exception as exc:  # noqa: BLE001
            print(f"[render_service] market context worker error: {exc}")
        time.sleep(sleep_seconds)


if __name__ == "__main__":
    # Render's starter instance is memory-constrained; keep single trader loop ownership in render_service.
    os.environ.setdefault("ENABLE_EMBEDDED_TRADER_FALLBACK", "false")
    _apply_boot_auto_resume()
    _print_startup_readiness()
    trader_thread = threading.Thread(target=_run_trader_forever, daemon=True, name="autobott-trader")
    trader_thread.start()
    stoploss_guard_thread = threading.Thread(target=_run_independent_stoploss_guard, daemon=True, name="autobott-stoploss")
    stoploss_guard_thread.start()
    watchdog_thread = threading.Thread(
        target=_run_trader_watchdog,
        args=(trader_thread,),
        daemon=True,
        name="autobott-trader-watchdog",
    )
    watchdog_thread.start()
    if bool(getattr(config, "ENABLE_MARKET_CONTEXT_WORKER", True)):
        market_context_thread = threading.Thread(
            target=_run_market_context_worker,
            daemon=True,
            name="autobott-market-context",
        )
        market_context_thread.start()
    else:
        print("[render_service] Market context worker disabled by ENABLE_MARKET_CONTEXT_WORKER=false.")
    if _historical_learning_enabled():
        historical_learning_thread = threading.Thread(
            target=_run_historical_learning_supervisor,
            daemon=True,
            name="autobott-historical-replay-supervisor",
        )
        historical_learning_thread.start()
    else:
        print("[render_service] Historical replay supervisor disabled by ENABLE_HISTORICAL_REPLAY_LEARNING=false.")

    port = _resolve_dashboard_port()
    print(f"[render_service] Starting dashboard on 0.0.0.0:{port}")
    app.run(host="0.0.0.0", port=port, debug=False)
