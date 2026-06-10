"""Minimal Render Starter dashboard for AutoBott.

This module intentionally avoids the full dashboard_v2 import graph. It keeps
only health, runtime debug, and basic pause/resume controls available while the
trader loop owns the process memory budget.
"""

from __future__ import annotations

import os
import time
from datetime import datetime
from typing import Any

import pytz
from flask import Flask, jsonify, request

import config
from state_store import get_state_health, load_bot_state
from trading_control import load_trading_control, set_dry_run, set_manual_stop

app = Flask(__name__)
_BOOT_TS = time.time()
_EASTERN = pytz.timezone(str(getattr(config, "EASTERN_TZ", "US/Eastern") or "US/Eastern"))
_CONTROL_TOKEN = str(getattr(config, "DASHBOARD_CONTROL_TOKEN", "") or "").strip()


def _now_et() -> str:
    return datetime.now(_EASTERN).isoformat()


def _parse_iso(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return None


def _heartbeat_age_seconds(state: dict[str, Any]) -> int | None:
    heartbeat = _parse_iso(state.get("last_trader_heartbeat_et"))
    if heartbeat is None:
        return None
    now = datetime.now(heartbeat.tzinfo) if heartbeat.tzinfo is not None else datetime.now()
    return max(0, int((now - heartbeat).total_seconds()))


def _runtime_payload() -> dict[str, Any]:
    state = load_bot_state()
    if not isinstance(state, dict):
        state = {}
    control = load_trading_control()
    if not isinstance(control, dict):
        control = {}
    heartbeat_age = _heartbeat_age_seconds(state)
    stale_after = max(120, int(getattr(config, "LOOP_INTERVAL_SECONDS", 60) or 60) * 4)
    return {
        "generated_at_et": _now_et(),
        "uptime_seconds": int(time.time() - _BOOT_TS),
        "deployment": {
            "git_commit": str(os.getenv("RENDER_GIT_COMMIT", "") or ""),
            "service_name": str(os.getenv("RENDER_SERVICE_NAME", "") or ""),
            "service_id": str(os.getenv("RENDER_SERVICE_ID", "") or ""),
            "instance_id": str(os.getenv("RENDER_INSTANCE_ID", "") or ""),
            "external_url": str(os.getenv("RENDER_EXTERNAL_URL", "") or ""),
        },
        "state_health": get_state_health(),
        "control": control,
        "runtime": {
            "heartbeat_age_seconds": heartbeat_age,
            "trader_loop_stale": bool(heartbeat_age is None or heartbeat_age >= stale_after),
            "trader_loop_stale_after_seconds": stale_after,
            "state_updated_at": str(state.get("_state_updated_at_iso", "") or ""),
            "trader_thread_last_start_et": str(state.get("trader_thread_last_start_et", "") or ""),
            "trader_thread_last_stop_et": str(state.get("trader_thread_last_stop_et", "") or ""),
            "trader_thread_restart_count": int(state.get("trader_thread_restart_count", 0) or 0),
            "trader_thread_last_crash_et": str(state.get("trader_thread_last_crash_et", "") or ""),
            "trader_thread_last_crash": str(state.get("trader_thread_last_crash", "") or ""),
            "open_trade_meta_count": len(dict(state.get("open_trade_meta") or {})),
            "today_closed_trade_count": int(state.get("today_closed_trade_count", 0) or 0),
            "today_closed_pnl_from_trades_csv": float(state.get("today_closed_pnl_from_trades_csv", 0.0) or 0.0),
            "last_entry_debug": dict(state.get("last_entry_debug") or {}),
            "last_exit_debug": dict(state.get("last_exit_debug") or {}),
            "independent_stoploss_last_trigger_et": str(
                state.get("independent_stoploss_last_trigger_et", "") or ""
            ),
        },
        "starter_safe": {
            "render_starter_safe_mode": bool(getattr(config, "RENDER_STARTER_SAFE_MODE", False)),
            "universe_mode": str(getattr(config, "UNIVERSE_MODE", "") or ""),
            "auto_expand_universe_with_movers": bool(getattr(config, "AUTO_EXPAND_UNIVERSE_WITH_MOVERS", False)),
            "enable_yfinance_fallback": bool(getattr(config, "ENABLE_YFINANCE_FALLBACK", True)),
            "scan_intraday_bars": int(getattr(config, "SCAN_INTRADAY_BARS", 0) or 0),
            "option_enrichment_max_attempts_per_cycle": int(
                getattr(config, "OPTION_ENRICHMENT_MAX_ATTEMPTS_PER_CYCLE", 0) or 0
            ),
            "max_contracts_per_ticker_per_hour": int(
                getattr(config, "MAX_CONTRACTS_PER_TICKER_PER_HOUR", 0) or 0
            ),
        },
    }


def _verify_control_token() -> tuple[bool, str, int]:
    if not _CONTROL_TOKEN:
        return True, "", 200
    token = str(request.headers.get("X-Control-Token", "") or request.args.get("token", "") or "").strip()
    if token == _CONTROL_TOKEN:
        return True, "", 200
    return False, "invalid_control_token", 403


@app.get("/")
def index():
    payload = _runtime_payload()
    runtime = payload["runtime"]
    return (
        "AutoBott Starter Runtime\n"
        f"build={payload['deployment'].get('git_commit') or 'unknown'}\n"
        f"uptime_seconds={payload['uptime_seconds']}\n"
        f"heartbeat_age_seconds={runtime.get('heartbeat_age_seconds')}\n"
        f"trader_loop_stale={runtime.get('trader_loop_stale')}\n"
        f"open_trade_meta_count={runtime.get('open_trade_meta_count')}\n"
    )


@app.get("/healthz")
def healthz():
    return jsonify({"ok": True, "service": "autobott-starter", **_runtime_payload()})


@app.get("/api/runtime-debug")
def api_runtime_debug():
    return jsonify(_runtime_payload())


@app.get("/api/trading-control")
def api_trading_control():
    return jsonify({"ok": True, "control": load_trading_control()})


@app.post("/api/runtime/trading/pause")
def api_pause():
    ok, err, status = _verify_control_token()
    if not ok:
        return jsonify({"ok": False, "error": err}), status
    return jsonify({"ok": True, "control": set_manual_stop(True, reason="starter_dashboard_pause")})


@app.post("/api/runtime/trading/resume")
def api_resume():
    ok, err, status = _verify_control_token()
    if not ok:
        return jsonify({"ok": False, "error": err}), status
    set_dry_run(False, reason="starter_dashboard_resume")
    return jsonify({"ok": True, "control": set_manual_stop(False, reason="starter_dashboard_resume")})
