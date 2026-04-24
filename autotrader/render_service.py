"""Render single-service runner: starts trader loop + dashboard in one process."""

from __future__ import annotations

import os
import shutil
import threading
import time
import traceback
from datetime import datetime
from pathlib import Path
import math

from env_config import get_required_env, load_runtime_env

load_runtime_env()


def _force_writable_data_dir() -> None:
    current = (os.getenv("DATA_DIR") or "").strip()
    windows_default = Path(__file__).resolve().parent

    if os.name == "nt":
        # Keep Windows runs deterministic in the repo data dir unless explicitly overridden.
        if not current:
            os.environ["DATA_DIR"] = str(windows_default)
            return

        target = Path(current)
        try:
            target.mkdir(parents=True, exist_ok=True)
            probe = target / ".write_test"
            with probe.open("w", encoding="utf-8") as f:
                f.write("ok")
            probe.unlink(missing_ok=True)
            return
        except Exception:
            os.environ["DATA_DIR"] = str(windows_default)
            print(
                f"[render_service] DATA_DIR '{current}' not writable. "
                f"Using '{windows_default}'."
            )
            return

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
    target_fallback = Path(__file__).resolve().parent if os.name == "nt" else Path("/tmp/autotrader-data")
    target_dir = Path((os.getenv("DATA_DIR") or "").strip() or str(target_fallback))
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
try:
    from autotrader import config
except ImportError:
    import config

from alerts import AlertManager
from broker import AlpacaBroker
from dashboard import app
from main import main as trader_main
from state_store import load_bot_state, save_bot_state
from trading_control import load_trading_control, set_manual_stop

try:
    import pytz
except Exception:  # noqa: BLE001
    pytz = None

ALERTS = AlertManager()
BROKER: AlpacaBroker | None = None


def _position_qty_as_int(qty_value) -> int:
    try:
        return int(float(qty_value))
    except (TypeError, ValueError):
        return 0


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
    global BROKER
    if BROKER is None:
        api_key = get_required_env("ALPACA_API_KEY")
        secret_key = get_required_env("ALPACA_SECRET_KEY")
        BROKER = AlpacaBroker(api_key, secret_key, paper=config.PAPER)
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


def _apply_boot_auto_resume() -> None:
    if not bool(getattr(config, "AUTO_RESUME_TRADING_ON_BOOT", True)):
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
    while True:
        try:
            runtime_state = load_bot_state()
            if not isinstance(runtime_state, dict):
                runtime_state = {}
            if require_stale_loop and (not _is_trader_loop_stale(runtime_state)):
                time.sleep(guard_sleep_seconds)
                continue

            broker = _broker()
            positions = broker.get_open_option_positions()
            stop_cap = abs(float(getattr(config, "STOP_LOSS_USD", 10.0) or 10.0))
            if stop_cap <= 0:
                time.sleep(guard_sleep_seconds)
                continue

            for pos in positions:
                symbol = str(getattr(pos, "symbol", "") or "")
                qty = _position_qty_as_int(getattr(pos, "qty", 0))
                if not symbol or qty <= 0:
                    continue
                unrealized_usd = _position_unrealized_usd(pos)
                if unrealized_usd is None or unrealized_usd > -stop_cap:
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


if __name__ == "__main__":
    _apply_boot_auto_resume()
    _print_startup_readiness()
    trader_thread = threading.Thread(target=_run_trader_forever, daemon=True)
    trader_thread.start()
    stoploss_guard_thread = threading.Thread(target=_run_independent_stoploss_guard, daemon=True)
    stoploss_guard_thread.start()

    port = int(os.getenv("PORT", "5000"))
    print(f"[render_service] Starting dashboard on 0.0.0.0:{port}")
    app.run(host="0.0.0.0", port=port, debug=False)
