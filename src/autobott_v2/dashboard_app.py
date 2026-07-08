from __future__ import annotations

import json
import os
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any, Callable
from wsgiref.simple_server import make_server

from .execution_config import load_alpaca_execution_config
from .env_bootstrap import bootstrap_env_file
from .decision_lab import build_decision_lab_report
from .phase1_alpaca_capture_now import capture_now
from .phase1_alpaca_client import AlpacaPaperClient
from .phase1_alpaca_config import load_alpaca_paper_config
from .phase1_campaign_runner import run_phase1_campaign
from .phase1_historical_backfill import run_historical_backfill
from .paper_readiness import run_paper_readiness_probe
from .session_supervisor import (
    SessionSupervisorConfig,
    maybe_start_session_supervisor,
    session_supervisor_status,
    start_session_supervisor,
)
from .session_runner import run_trading_session
from .trading_cycle import load_decision_cards, run_trading_cycle
from .execution_broker import AlpacaExecutionBroker
from .execution_reconciler import reconcile_open_positions
from .exit_orchestrator import cancel_open_order, replace_open_order, submit_exit_for_position
from .position_monitor import load_position_monitor_rules
from .position_store import load_open_positions, position_store_path
from .runtime_control import (
    arm_paper_execution,
    disable_execution,
    load_runtime_state,
    runtime_state_path,
    set_kill_switch,
)
from .runtime_paths import gate_path as default_gate_path
from .runtime_paths import phase1_replay_campaign_root, phase1_snapshots_root


JsonDict = dict[str, Any]

DECISION_LAB_MAX_SYNC_SYMBOLS = 2
DECISION_LAB_MAX_SYNC_DAYS = 3
DECISION_LAB_MIN_SYNC_INTERVAL_MINUTES = 30


def app(environ: dict[str, Any], start_response: Callable[..., Any]) -> list[bytes]:
    method = environ.get("REQUEST_METHOD", "GET").upper()
    path = environ.get("PATH_INFO", "/")
    headers = _extract_headers(environ)
    body = _read_body(environ)

    try:
        status_code, content_type, payload = handle_request(method, path, headers, body)
    except PermissionError as exc:
        status_code = 401
        content_type = "application/json; charset=utf-8"
        payload = {"ok": False, "error": "unauthorized", "detail": str(exc)}
    except Exception as exc:  # pragma: no cover
        status_code = 500
        content_type = "application/json; charset=utf-8"
        payload = {"ok": False, "error": type(exc).__name__, "detail": str(exc)}

    start_response(f"{status_code} {_reason(status_code)}", [("Content-Type", content_type)])
    if isinstance(payload, str):
        return [payload.encode("utf-8")]
    return [json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")]


def handle_request(method: str, path: str, headers: dict[str, str], body: bytes) -> tuple[int, str, JsonDict | str]:
    if path == "/":
        return 200, "text/html; charset=utf-8", _dashboard_html()
    if path == "/api/health":
        payload = _health_payload()
        return (200 if payload["ok"] else 503), "application/json; charset=utf-8", payload
    if path.startswith("/api/"):
        _require_auth(headers)
        if path == "/api/safety" and method == "GET":
            return 200, "application/json; charset=utf-8", _safety_payload()
        if path == "/api/alpaca/status" and method == "GET":
            return 200, "application/json; charset=utf-8", _alpaca_status_payload()
        if path == "/api/paper/readiness" and method == "GET":
            return 200, "application/json; charset=utf-8", _paper_readiness_payload()
        if path == "/api/positions/open" and method == "GET":
            return 200, "application/json; charset=utf-8", _open_positions_payload()
        if path == "/api/account/positions" and method == "GET":
            return 200, "application/json; charset=utf-8", _account_positions_payload()
        if path == "/api/account/orders" and method == "GET":
            return 200, "application/json; charset=utf-8", _account_orders_payload()
        if path == "/api/options/scout" and method == "GET":
            return 200, "application/json; charset=utf-8", _options_scout_payload()
        if path == "/api/options/timeline" and method == "GET":
            return 200, "application/json; charset=utf-8", _options_timeline_payload()
        if path == "/api/corpus/latest" and method == "GET":
            return 200, "application/json; charset=utf-8", _latest_corpus_payload()
        if path == "/api/campaign/latest" and method == "GET":
            return 200, "application/json; charset=utf-8", _latest_campaign_payload()
        if path == "/api/decisions/latest" and method == "GET":
            return 200, "application/json; charset=utf-8", _latest_decisions_payload()
        if path == "/api/decisions/feed" and method == "GET":
            return 200, "application/json; charset=utf-8", _decision_feed_payload()
        if path == "/api/execution/state" and method == "GET":
            return 200, "application/json; charset=utf-8", _execution_state_payload()
        if path == "/api/session/status" and method == "GET":
            return 200, "application/json; charset=utf-8", _session_status_payload()
        if path == "/api/runtime/arm-paper" and method == "POST":
            return 200, "application/json; charset=utf-8", _runtime_arm_paper_payload(_json_body(body))
        if path == "/api/runtime/disable-execution" and method == "POST":
            return 200, "application/json; charset=utf-8", _runtime_disable_execution_payload(_json_body(body))
        if path == "/api/runtime/kill-switch" and method == "POST":
            return 200, "application/json; charset=utf-8", _runtime_kill_switch_payload(_json_body(body))
        if path == "/api/execution/reconcile" and method == "POST":
            return 200, "application/json; charset=utf-8", _execution_reconcile_payload()
        if path == "/api/session/start" and method == "POST":
            return 200, "application/json; charset=utf-8", _session_start_payload(_json_body(body))
        if path == "/api/reports/bucket-edge/latest" and method == "GET":
            return 200, "application/json; charset=utf-8", _latest_bucket_edge_payload()
        if path == "/api/reports/thesis-failures/latest" and method == "GET":
            return 200, "application/json; charset=utf-8", _latest_thesis_failures_payload()
        if path == "/api/reports/gate-candidate/latest" and method == "GET":
            return 200, "application/json; charset=utf-8", _latest_gate_candidate_payload()
        if path == "/api/reports/decision-lab/latest" and method == "GET":
            return 200, "application/json; charset=utf-8", _latest_decision_lab_payload()
        if path == "/api/reports/decision-lab/backfill-run" and method == "POST":
            return 200, "application/json; charset=utf-8", _decision_lab_backfill_run_payload(_json_body(body))
        if path == "/api/capture/start" and method == "POST":
            return 200, "application/json; charset=utf-8", _capture_start_payload(_json_body(body))
        if path == "/api/campaign/run" and method == "POST":
            return 200, "application/json; charset=utf-8", _campaign_run_payload(_json_body(body))
        if path == "/api/trading-cycle/run" and method == "POST":
            return 200, "application/json; charset=utf-8", _trading_cycle_run_payload(_json_body(body))
        if path == "/api/trading-session/run" and method == "POST":
            return 200, "application/json; charset=utf-8", _trading_session_run_payload(_json_body(body))
        if path == "/api/execution/exit" and method == "POST":
            return 200, "application/json; charset=utf-8", _execution_exit_payload(_json_body(body))
        if path == "/api/execution/cancel" and method == "POST":
            return 200, "application/json; charset=utf-8", _execution_cancel_payload(_json_body(body))
        if path == "/api/execution/replace" and method == "POST":
            return 200, "application/json; charset=utf-8", _execution_replace_payload(_json_body(body))
    return 404, "application/json; charset=utf-8", {"ok": False, "error": "not_found"}


def _health_payload() -> JsonDict:
    healthy, detail = _session_watchdog_status()
    payload: JsonDict = {
        "ok": healthy,
        "app": "autobott-phase1-dashboard",
        "timestamp": datetime.now(UTC).isoformat(),
        "version": os.getenv("RENDER_GIT_COMMIT") or "dev",
    }
    if detail:
        payload["session_supervisor"] = detail
    return payload


def _session_watchdog_status() -> tuple[bool, JsonDict | None]:
    # The session loop runs forever once started (see session_supervisor.py);
    # the only way it has a started_at but isn't alive is a crash that
    # escaped the loop's own error handling. Autostart only fires once, so a
    # dead loop otherwise leaves every open position unmonitored -- stop-loss
    # and trailing-stop exits included -- for the rest of the trading day.
    # Failing the health check here lets Render's own restart-on-unhealthy
    # behavior re-arm the supervisor via main()'s autostart on the next boot.
    try:
        status = session_supervisor_status()
    except Exception:
        return True, None
    state = status.get("state") or {}
    config = status.get("config") or {}
    if not config.get("enabled"):
        return True, None
    if state.get("started_at") and not status.get("thread_alive"):
        return False, {"stalled": True, "last_error": state.get("last_error")}
    return True, None


def _safety_payload() -> JsonDict:
    config = load_alpaca_paper_config()
    execution_config = load_alpaca_execution_config()
    gate_path = _gate_path()
    runtime_state = load_runtime_state()
    open_positions = load_open_positions()
    status = _paper_execution_status_payload(config=config, execution_config=execution_config, runtime_state=runtime_state)
    return {
        "alpaca_env": config.env,
        "paper_only": config.paper_only,
        "live_trading_enabled": status["live_trading_enabled"],
        "order_placement_enabled": status["order_placement_enabled"],
        "order_placement_configured": status["order_placement_configured"],
        "paper_trade_through_enabled": status["paper_trade_through_enabled"],
        "effective_max_open_positions": status["effective_max_open_positions"],
        "effective_max_new_entry_attempts_per_loop": status["effective_max_new_entry_attempts_per_loop"],
        "active_gate_mutation_allowed": False,
        "active_gate_hash": _file_hash(gate_path),
        "active_gate_path": str(gate_path),
        "order_methods_present": _order_methods_present(),
        "kill_switch_enabled": runtime_state.kill_switch_enabled,
        "execution_enabled": runtime_state.execution_enabled,
        "runtime_state_path": str(runtime_state_path()),
        "position_store_path": str(position_store_path()),
        "open_position_count": len(open_positions),
        "mode_banner": status["mode_banner"],
    }


def _execution_state_payload() -> JsonDict:
    runtime_state = load_runtime_state()
    positions = load_open_positions()
    return {
        "ok": True,
        "kill_switch_enabled": runtime_state.kill_switch_enabled,
        "execution_enabled": runtime_state.execution_enabled,
        "live_mode_enabled": runtime_state.live_mode_enabled,
        "runtime_state_path": str(runtime_state_path()),
        "position_store_path": str(position_store_path()),
        "open_position_count": len(positions),
        "updated_at": runtime_state.updated_at.isoformat(),
        "reason": runtime_state.reason,
    }


def _session_status_payload() -> JsonDict:
    return {"ok": True, **session_supervisor_status()}


def _runtime_arm_paper_payload(payload: JsonDict) -> JsonDict:
    reason = str(payload.get("reason", "dashboard_arm_paper"))
    state = arm_paper_execution(reason=reason)
    return {"ok": True, "runtime_state": state.to_json_dict()}


def _runtime_disable_execution_payload(payload: JsonDict) -> JsonDict:
    reason = str(payload.get("reason", "dashboard_disable_execution"))
    state = disable_execution(reason=reason)
    return {"ok": True, "runtime_state": state.to_json_dict()}


def _runtime_kill_switch_payload(payload: JsonDict) -> JsonDict:
    enabled = bool(payload.get("enabled", True))
    reason = str(payload.get("reason", "dashboard_kill_switch"))
    state = set_kill_switch(enabled, reason=reason)
    return {"ok": True, "runtime_state": state.to_json_dict()}


def _execution_reconcile_payload() -> JsonDict:
    summary = reconcile_open_positions(
        AlpacaExecutionBroker(),
        journal_path=str(_artifacts_root() / "dashboard_execution_reconcile.jsonl"),
    )
    return {
        "ok": True,
        "checked": summary.checked,
        "updated": summary.updated,
        "unchanged": summary.unchanged,
        "missing": summary.missing,
        "open_position_count": len(load_open_positions()),
    }


def _session_start_payload(payload: JsonDict) -> JsonDict:
    symbols = [str(symbol).upper() for symbol in payload.get("symbols", ["SPY"]) if str(symbol).strip()]
    config = SessionSupervisorConfig(
        enabled=True,
        symbols=symbols,
        interval_seconds=int(payload.get("interval_seconds", 300)),
        max_cycles=int(payload["max_cycles"]) if payload.get("max_cycles") is not None else None,
        symbol_batch_size=int(payload["symbol_batch_size"]) if payload.get("symbol_batch_size") is not None else None,
        quantity=int(payload.get("quantity", 1)),
        position_count=int(payload.get("position_count", 0)),
        daily_pnl=float(payload.get("daily_pnl", 0.0)),
        start_time=str(payload["start_time"]).strip() if payload.get("start_time") else None,
        end_time=str(payload["end_time"]).strip() if payload.get("end_time") else None,
        market_timezone=str(payload.get("market_timezone", "America/New_York")).strip() or "America/New_York",
        arm_paper_execution_on_start=bool(payload.get("arm_paper_execution_on_start", False)),
    )
    started = start_session_supervisor(config)
    return {
        "ok": started,
        "started": started,
        "status": "started" if started else "already_running",
        **session_supervisor_status(),
    }


def _alpaca_status_payload() -> JsonDict:
    config = load_alpaca_paper_config()
    execution_config = load_alpaca_execution_config()
    runtime_state = load_runtime_state()
    status = _paper_execution_status_payload(config=config, execution_config=execution_config, runtime_state=runtime_state)
    response: JsonDict = {
        "config": config.redacted_dict(),
        "paper_only": config.paper_only,
        "live_trading_enabled": status["live_trading_enabled"],
        "order_placement_enabled": status["order_placement_enabled"],
        "order_placement_configured": status["order_placement_configured"],
        "paper_trade_through_enabled": status["paper_trade_through_enabled"],
        "effective_max_open_positions": status["effective_max_open_positions"],
        "effective_max_new_entry_attempts_per_loop": status["effective_max_new_entry_attempts_per_loop"],
        "mode_banner": status["mode_banner"],
        "credentials_present": bool(config.api_key and config.secret_key),
    }
    try:
        config.validate()
    except Exception as exc:
        response.update({"ok": False, "status": "config_invalid", "detail": str(exc)})
        return response

    client = AlpacaPaperClient(config)
    try:
        account = client.get_account()
        quotes = client.get_latest_stock_quotes(["SPY", "QQQ"])
        response.update(
            {
                "ok": True,
                "status": "paper_connected",
                "account_status": str(account.get("status", "unknown")),
                "quote_symbols": sorted(quotes.keys()),
                "quote_checks": {"SPY": "PASS" if "SPY" in quotes else "FAIL", "QQQ": "PASS" if "QQQ" in quotes else "FAIL"},
            }
        )
    except Exception as exc:
        response.update({"ok": False, "status": "paper_connection_failed", "detail": str(exc)})
    return response


def _paper_readiness_payload() -> JsonDict:
    return run_paper_readiness_probe()


def _paper_execution_status_payload(*, config: Any, execution_config: Any, runtime_state: Any) -> JsonDict:
    order_placement_configured = bool(getattr(execution_config, "allow_order_placement", False))
    order_placement_enabled = bool(
        order_placement_configured
        and runtime_state.execution_enabled
        and not runtime_state.kill_switch_enabled
        and not runtime_state.live_mode_enabled
    )
    effective_max_open_positions = (
        execution_config.effective_max_open_positions()
        if hasattr(execution_config, "effective_max_open_positions")
        else getattr(execution_config, "max_open_positions", None)
    )
    effective_max_new_entry_attempts_per_loop = (
        execution_config.effective_max_new_entry_attempts_per_loop()
        if hasattr(execution_config, "effective_max_new_entry_attempts_per_loop")
        else None
    )
    return {
        "paper_only": bool(getattr(config, "paper_only", True)),
        "live_trading_enabled": bool(runtime_state.live_mode_enabled),
        "order_placement_configured": order_placement_configured,
        "order_placement_enabled": order_placement_enabled,
        "paper_trade_through_enabled": bool(getattr(execution_config, "paper_trade_all_passed_signals", False)),
        "effective_max_open_positions": effective_max_open_positions,
        "effective_max_new_entry_attempts_per_loop": effective_max_new_entry_attempts_per_loop,
        "mode_banner": _mode_banner(
            paper_only=bool(getattr(config, "paper_only", True)),
            runtime_state=runtime_state,
            order_placement_configured=order_placement_configured,
            order_placement_enabled=order_placement_enabled,
        ),
    }


def _mode_banner(*, paper_only: bool, runtime_state: Any, order_placement_configured: bool, order_placement_enabled: bool) -> str:
    mode = "PAPER ONLY" if paper_only else "PAPER MODE UNKNOWN"
    if runtime_state.live_mode_enabled:
        suffix = "LIVE MODE FLAGGED"
    elif runtime_state.kill_switch_enabled:
        suffix = "KILL SWITCH ACTIVE"
    elif not order_placement_configured:
        suffix = "ORDER PLACEMENT CONFIG DISABLED"
    elif not runtime_state.execution_enabled:
        suffix = "RUNTIME EXECUTION PAUSED"
    elif order_placement_enabled:
        suffix = "PAPER EXECUTION ARMED"
    else:
        suffix = "EXECUTION BLOCKED"
    return f"{mode} | LIVE TRADING LOCKED | {suffix}"


def _open_positions_payload() -> JsonDict:
    positions = [position.to_json_dict() for position in load_open_positions()]
    return {
        "ok": True,
        "count": len(positions),
        "positions": positions,
        "position_store_path": str(position_store_path()),
    }


def _account_positions_payload() -> JsonDict:
    config = load_alpaca_paper_config()
    try:
        config.validate()
    except Exception as exc:
        return {"ok": False, "status": "config_invalid", "detail": str(exc)}
    client = AlpacaPaperClient(config)
    try:
        account = client.get_account()
        positions = client.get_positions()
    except Exception as exc:
        return {"ok": False, "status": "alpaca_request_failed", "detail": str(exc)}
    equity = float(account.get("equity") or 0.0)
    last_equity = float(account.get("last_equity") or 0.0)
    day_pl = equity - last_equity
    return {
        "ok": True,
        "account": {
            "equity": equity,
            "last_equity": last_equity,
            "day_pl": day_pl,
            "day_pl_pct": (day_pl / last_equity * 100.0) if last_equity else 0.0,
            "cash": float(account.get("cash") or 0.0),
            "buying_power": float(account.get("buying_power") or 0.0),
            "portfolio_value": float(account.get("portfolio_value") or 0.0),
        },
        "positions": [
            {
                "symbol": position.get("symbol"),
                **_option_symbol_parts(str(position.get("symbol") or "")),
                "side": position.get("side"),
                "qty": position.get("qty"),
                "avg_entry_price": position.get("avg_entry_price"),
                "current_price": position.get("current_price"),
                "market_value": position.get("market_value"),
                "unrealized_pl": position.get("unrealized_pl"),
                "unrealized_plpc": position.get("unrealized_plpc"),
            }
            for position in positions
        ],
    }


def _account_orders_payload() -> JsonDict:
    config = load_alpaca_paper_config()
    try:
        config.validate()
    except Exception as exc:
        return {"ok": False, "status": "config_invalid", "detail": str(exc)}
    client = AlpacaPaperClient(config)
    try:
        orders = client.get_orders(status="all", limit=50)
    except Exception as exc:
        return {"ok": False, "status": "alpaca_request_failed", "detail": str(exc)}
    return {
        "ok": True,
        "orders": [
            {
                "symbol": order.get("symbol"),
                **_option_symbol_parts(str(order.get("symbol") or "")),
                "side": order.get("side"),
                "qty": order.get("qty"),
                "filled_qty": order.get("filled_qty"),
                "filled_avg_price": order.get("filled_avg_price"),
                "status": order.get("status"),
                "submitted_at": order.get("submitted_at"),
                "filled_at": order.get("filled_at"),
            }
            for order in orders
        ],
    }


def _options_scout_payload() -> JsonDict:
    config = load_alpaca_paper_config()
    try:
        config.validate()
    except Exception as exc:
        return {"ok": False, "status": "config_invalid", "detail": str(exc)}
    client = AlpacaPaperClient(config)
    try:
        positions = client.get_positions()
        orders = client.get_orders(status="open", limit=100)
    except Exception as exc:
        return {"ok": False, "status": "alpaca_request_failed", "detail": str(exc)}

    rules = load_position_monitor_rules()
    pending_exits = _pending_scout_exits_by_symbol(orders)
    rows: list[JsonDict] = []
    for position in positions:
        symbol = str(position.get("symbol") or "").upper()
        if not symbol:
            continue
        plpc = _float_or_zero(position.get("unrealized_plpc"))
        current_price = _float_or_zero(position.get("current_price"))
        pending_exit = pending_exits.get(symbol)
        tier = _scout_profit_tier(plpc, rules)
        target_price = _scout_target_price(plpc, current_price, rules)
        attention = _scout_position_attention(plpc, pending_exit=pending_exit, tier=tier, rules=rules)
        rows.append(
            {
                "source": "open_position",
                "symbol": symbol,
                **_option_symbol_parts(symbol),
                "side": position.get("side"),
                "qty": position.get("qty"),
                "current_price": current_price,
                "avg_entry_price": _float_or_zero(position.get("avg_entry_price")),
                "unrealized_pl": _float_or_zero(position.get("unrealized_pl")),
                "unrealized_plpc": plpc,
                "profit_tier": tier,
                "target_exit_price": target_price,
                "pending_exit_order_id": pending_exit.get("id") if pending_exit else None,
                "pending_exit_limit_price": _float_or_none(pending_exit.get("limit_price")) if pending_exit else None,
                "attention": attention,
                "score": _scout_attention_score(attention, plpc),
            }
        )

    for row in _latest_decision_scout_rows():
        rows.append(row)

    ranked = sorted(rows, key=lambda row: (-float(row.get("score") or 0.0), str(row.get("symbol") or "")))
    return {
        "ok": True,
        "status": "options_scout_ready",
        "generated_at": datetime.now(UTC).isoformat(),
        "mode": "paper_account_live_data_scout",
        "scout_rows": ranked[:20],
        "counts": {
            "open_positions": len(positions),
            "pending_exit_orders": len(pending_exits),
            "rows": len(ranked),
        },
        "profit_ladder": {
            "initial_pct": rules.take_profit_pct,
            "tighten_pct": rules.take_profit_tighten_pct,
            "harvest_pct": rules.take_profit_harvest_pct,
            "force_exit_pct": rules.take_profit_force_exit_pct,
        },
    }


def _options_timeline_payload() -> JsonDict:
    config = load_alpaca_paper_config()
    try:
        config.validate()
    except Exception as exc:
        return {"ok": False, "status": "config_invalid", "detail": str(exc)}
    client = AlpacaPaperClient(config)
    try:
        orders = client.get_orders(status="all", limit=200)
    except Exception as exc:
        return {"ok": False, "status": "alpaca_request_failed", "detail": str(exc)}

    normalized = sorted((_normalize_order_for_timeline(order) for order in orders), key=lambda row: row["event_time"] or datetime.min.replace(tzinfo=UTC))
    round_trips, pending = _timeline_round_trips(normalized)
    clusters = _timeline_clusters(normalized, round_trips)
    warnings = _timeline_warnings(clusters, round_trips, pending)
    return {
        "ok": True,
        "status": "options_timeline_ready",
        "generated_at": datetime.now(UTC).isoformat(),
        "mode": "paper_order_timeline",
        "round_trips": sorted(round_trips, key=lambda row: row.get("exit_time") or "", reverse=True)[:30],
        "pending_orders": sorted(pending, key=lambda row: row.get("submitted_at") or "", reverse=True)[:30],
        "clusters": sorted(clusters, key=lambda row: row.get("bucket_start") or "", reverse=True)[:20],
        "warnings": warnings,
        "summary": {
            "orders_seen": len(normalized),
            "round_trips": len(round_trips),
            "pending_orders": len(pending),
            "realized_pnl": round(sum(float(row.get("pnl") or 0.0) for row in round_trips), 2),
            "winners": sum(1 for row in round_trips if float(row.get("pnl") or 0.0) > 0),
            "losers": sum(1 for row in round_trips if float(row.get("pnl") or 0.0) < 0),
        },
    }


def _normalize_order_for_timeline(order: dict[str, Any]) -> JsonDict:
    symbol = str(order.get("symbol") or "").upper()
    submitted_at = _parse_datetime(order.get("submitted_at"))
    filled_at = _parse_datetime(order.get("filled_at"))
    event_time = filled_at or submitted_at
    return {
        "symbol": symbol,
        **_option_symbol_parts(symbol),
        "side": str(order.get("side") or "").lower(),
        "status": str(order.get("status") or "").lower(),
        "qty": _float_or_zero(order.get("qty")),
        "filled_qty": _float_or_zero(order.get("filled_qty")),
        "filled_avg_price": _float_or_none(order.get("filled_avg_price")),
        "limit_price": _float_or_none(order.get("limit_price")),
        "submitted_at": submitted_at.isoformat() if submitted_at else None,
        "filled_at": filled_at.isoformat() if filled_at else None,
        "event_time": event_time,
    }


def _timeline_round_trips(orders: list[JsonDict]) -> tuple[list[JsonDict], list[JsonDict]]:
    open_buys: dict[str, list[JsonDict]] = {}
    round_trips: list[JsonDict] = []
    pending: list[JsonDict] = []
    for order in orders:
        symbol = str(order.get("symbol") or "")
        side = str(order.get("side") or "")
        status = str(order.get("status") or "")
        filled_qty = float(order.get("filled_qty") or 0.0)
        filled_price = order.get("filled_avg_price")
        if status not in {"filled", "partially_filled"}:
            if status in {"new", "accepted", "pending_new", "pending_replace"}:
                pending.append(_public_timeline_order(order))
            continue
        if filled_qty <= 0 or filled_price is None:
            continue
        if side == "buy":
            open_buys.setdefault(symbol, []).append(order)
            continue
        if side != "sell":
            continue
        buy = open_buys.get(symbol, []).pop(0) if open_buys.get(symbol) else None
        if buy is None:
            round_trips.append(_unmatched_sell_round_trip(order))
            continue
        entry_price = float(buy.get("filled_avg_price") or 0.0)
        exit_price = float(filled_price)
        qty = min(float(buy.get("filled_qty") or 0.0), filled_qty)
        pnl = round((exit_price - entry_price) * qty * 100.0, 2)
        return_pct = ((exit_price - entry_price) / entry_price) if entry_price else 0.0
        round_trips.append(
            {
                "symbol": symbol,
                **_option_symbol_parts(symbol),
                "entry_time": buy.get("filled_at") or buy.get("submitted_at"),
                "exit_time": order.get("filled_at") or order.get("submitted_at"),
                "entry_price": entry_price,
                "exit_price": exit_price,
                "qty": qty,
                "pnl": pnl,
                "return_pct": round(return_pct, 4),
                "classification": _round_trip_classification(return_pct),
            }
        )
    for buys in open_buys.values():
        for buy in buys:
            pending.append(_public_timeline_order(buy) | {"pending_kind": "open_filled_buy"})
    return round_trips, pending


def _unmatched_sell_round_trip(order: JsonDict) -> JsonDict:
    return {
        "symbol": order.get("symbol"),
        **_option_symbol_parts(str(order.get("symbol") or "")),
        "entry_time": None,
        "exit_time": order.get("filled_at") or order.get("submitted_at"),
        "entry_price": None,
        "exit_price": order.get("filled_avg_price"),
        "qty": order.get("filled_qty"),
        "pnl": 0.0,
        "return_pct": None,
        "classification": "unmatched_sell",
    }


def _public_timeline_order(order: JsonDict) -> JsonDict:
    return {
        "symbol": order.get("symbol"),
        "side": order.get("side"),
        "status": order.get("status"),
        "qty": order.get("qty"),
        "filled_qty": order.get("filled_qty"),
        "limit_price": order.get("limit_price"),
        "filled_avg_price": order.get("filled_avg_price"),
        "submitted_at": order.get("submitted_at"),
        "filled_at": order.get("filled_at"),
        **_option_symbol_parts(str(order.get("symbol") or "")),
    }


def _round_trip_classification(return_pct: float) -> str:
    if return_pct >= 1.2:
        return "huge_winner"
    if return_pct >= 0.3:
        return "winner"
    if return_pct <= -0.22:
        return "loss_cut"
    if return_pct < 0:
        return "small_loss"
    return "flat"


def _timeline_clusters(orders: list[JsonDict], round_trips: list[JsonDict]) -> list[JsonDict]:
    clusters: dict[str, JsonDict] = {}
    for order in orders:
        bucket = _time_bucket(order.get("event_time"))
        if bucket is None:
            continue
        cluster = clusters.setdefault(bucket.isoformat(), {"bucket_start": bucket.isoformat(), "buy_orders": 0, "sell_orders": 0, "pending_orders": 0, "filled_orders": 0, "symbols": set(), "realized_pnl": 0.0})
        side = str(order.get("side") or "")
        status = str(order.get("status") or "")
        if side == "buy":
            cluster["buy_orders"] += 1
        elif side == "sell":
            cluster["sell_orders"] += 1
        if status == "filled":
            cluster["filled_orders"] += 1
        elif status in {"new", "accepted", "pending_new", "pending_replace"}:
            cluster["pending_orders"] += 1
        if order.get("symbol"):
            cluster["symbols"].add(order["symbol"])
    for trip in round_trips:
        exit_time = _parse_datetime(trip.get("exit_time"))
        bucket = _time_bucket(exit_time)
        if bucket is None:
            continue
        cluster = clusters.setdefault(bucket.isoformat(), {"bucket_start": bucket.isoformat(), "buy_orders": 0, "sell_orders": 0, "pending_orders": 0, "filled_orders": 0, "symbols": set(), "realized_pnl": 0.0})
        cluster["realized_pnl"] = round(float(cluster.get("realized_pnl") or 0.0) + float(trip.get("pnl") or 0.0), 2)
    return [{**cluster, "symbols": sorted(cluster["symbols"])[:8]} for cluster in clusters.values()]


def _timeline_warnings(clusters: list[JsonDict], round_trips: list[JsonDict], pending: list[JsonDict]) -> list[JsonDict]:
    warnings: list[JsonDict] = []
    if any(trip.get("classification") == "huge_winner" for trip in round_trips):
        warnings.append({"type": "profit_harvest_seen", "detail": "Huge winners were harvested; new-entry throttle should be stricter after this cluster."})
    if any(float(trip.get("pnl") or 0.0) < 0 for trip in round_trips[-10:]) and pending:
        warnings.append({"type": "reversal_with_pending_entries", "detail": "Recent losses coexist with pending new entries; review whether shock-day reversal throttle should pause fresh entries."})
    for cluster in clusters:
        if int(cluster.get("buy_orders") or 0) >= 5:
            warnings.append({"type": "entry_cluster", "bucket_start": cluster.get("bucket_start"), "detail": "Large entry cluster detected."})
        if int(cluster.get("sell_orders") or 0) >= 5:
            warnings.append({"type": "exit_cluster", "bucket_start": cluster.get("bucket_start"), "detail": "Large exit cluster detected."})
    return warnings[:12]


def _time_bucket(value: Any, *, minutes: int = 15) -> datetime | None:
    timestamp = value if isinstance(value, datetime) else _parse_datetime(value)
    if timestamp is None:
        return None
    return timestamp.replace(minute=(timestamp.minute // minutes) * minutes, second=0, microsecond=0)


def _pending_scout_exits_by_symbol(orders: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    pending: dict[str, dict[str, Any]] = {}
    for order in orders:
        symbol = str(order.get("symbol") or "").upper()
        side = str(order.get("side") or "").lower()
        status = str(order.get("status") or "").lower()
        if symbol and side == "sell" and status in {"new", "accepted", "partially_filled", "pending_new", "pending_replace"}:
            pending.setdefault(symbol, order)
    return pending


def _scout_profit_tier(plpc: float, rules: Any) -> str:
    if plpc >= rules.take_profit_force_exit_pct:
        return "force_exit"
    if plpc >= rules.take_profit_harvest_pct:
        return "harvest"
    if plpc >= rules.take_profit_tighten_pct:
        return "tighten"
    if plpc >= rules.take_profit_pct:
        return "initial"
    if plpc >= rules.trailing_activation_pct:
        return "trail_watch"
    return "monitor"


def _scout_target_price(plpc: float, current_price: float, rules: Any) -> float | None:
    if current_price <= 0 or plpc < rules.take_profit_pct:
        return None
    if plpc >= rules.take_profit_force_exit_pct:
        return current_price
    if plpc >= rules.take_profit_harvest_pct:
        factor = rules.take_profit_harvest_limit_price_factor
    elif plpc >= rules.take_profit_tighten_pct:
        factor = rules.take_profit_tight_limit_price_factor
    else:
        factor = rules.take_profit_limit_price_factor
    return round(current_price * factor, 2)


def _scout_position_attention(plpc: float, *, pending_exit: dict[str, Any] | None, tier: str, rules: Any) -> str:
    if tier == "force_exit":
        return "force_exit_due"
    if plpc >= rules.take_profit_pct and pending_exit is None:
        return "profit_exit_missing"
    if plpc >= rules.take_profit_pct and pending_exit is not None:
        return "profit_exit_working"
    if plpc >= rules.trailing_activation_pct:
        return "winner_trailing"
    if plpc <= -abs(rules.stop_loss_pct):
        return "stop_loss_due"
    return "watch"


def _scout_attention_score(attention: str, plpc: float) -> float:
    base = {
        "force_exit_due": 100.0,
        "stop_loss_due": 95.0,
        "profit_exit_missing": 85.0,
        "profit_exit_working": 70.0,
        "winner_trailing": 45.0,
        "decision_candidate": 35.0,
        "wide_spread_candidate": 30.0,
        "watch": 10.0,
    }.get(attention, 5.0)
    return round(base + min(20.0, abs(plpc) * 10.0), 4)


def _latest_decision_scout_rows() -> list[JsonDict]:
    rows: list[JsonDict] = []
    for record in load_decision_cards(limit=10):
        decision = record.get("decision_card", record)
        contract = decision.get("selected_contract") or {}
        option_symbol = str(contract.get("option_symbol") or "")
        if not option_symbol:
            continue
        spread_pct = _float_or_zero(contract.get("spread_pct"))
        iv = _float_or_zero(contract.get("implied_volatility"))
        attention = "wide_spread_candidate" if spread_pct >= 0.12 else "decision_candidate"
        rows.append(
            {
                "source": "latest_decision",
                "symbol": option_symbol,
                **_option_symbol_parts(option_symbol),
                "ticker": decision.get("ticker"),
                "decision": decision.get("decision"),
                "confidence_score": decision.get("confidence_score"),
                "spread_pct": spread_pct,
                "implied_volatility": iv,
                "mid": contract.get("mid"),
                "attention": attention,
                "score": _scout_attention_score(attention, 0.0),
            }
        )
    return rows


def _float_or_zero(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _option_symbol_parts(symbol: str) -> JsonDict:
    stripped = symbol.strip().upper()
    if len(stripped) < 15:
        return {"underlying": stripped or None, "option_type": None, "expiration": None, "strike": None}
    option_type_index = -1
    for index, char in enumerate(stripped):
        if char in {"C", "P"} and index >= 1 and index + 9 <= len(stripped):
            prefix = stripped[index - 6 : index]
            suffix = stripped[index + 1 :]
            if len(prefix) == 6 and prefix.isdigit() and suffix.isdigit():
                option_type_index = index
                break
    if option_type_index < 0:
        return {"underlying": stripped or None, "option_type": None, "expiration": None, "strike": None}
    root = stripped[: option_type_index - 6]
    expiry = stripped[option_type_index - 6 : option_type_index]
    strike_text = stripped[option_type_index + 1 :]
    option_type = "CALL" if stripped[option_type_index] == "C" else "PUT"
    expiration = f"20{expiry[:2]}-{expiry[2:4]}-{expiry[4:6]}"
    strike = round(int(strike_text) / 1000, 3) if strike_text.isdigit() else None
    return {
        "underlying": root,
        "option_type": option_type,
        "expiration": expiration,
        "strike": strike,
    }


def _latest_corpus_payload() -> JsonDict:
    manifest_path = _latest_manifest(_corpus_root())
    if manifest_path is None:
        return {"ok": False, "status": "no_corpus_found"}
    manifest = _read_json(manifest_path)
    symbol_dir = manifest_path.parent
    return {
        "ok": True,
        "manifest_path": str(manifest_path),
        "source": manifest.get("source"),
        "corpus_type": manifest.get("corpus_type"),
        "symbol": manifest.get("symbol"),
        "trading_date": manifest.get("trading_date"),
        "snapshots_captured": manifest.get("snapshots_captured"),
        "option_quotes_captured": manifest.get("option_quotes_captured"),
        "skipped_option_quote_reason": manifest.get("skipped_option_quote_reason"),
        "data_quality_flags": manifest.get("data_quality_flags", []),
        "raw_payload_count": len(list((symbol_dir / "raw").glob("*.json"))),
    }


def _latest_campaign_payload() -> JsonDict:
    campaign_dir = _latest_campaign_dir()
    if campaign_dir is None:
        return {"ok": False, "status": "no_campaign_found"}
    manifest = _read_json(campaign_dir / "manifest.json")
    thesis_by_fill_model = manifest.get("thesis_validation_by_fill_model", {})
    primary_thesis = thesis_by_fill_model.get("realistic_mid_penalty", {})
    return {
        "ok": True,
        "artifact_dir": str(campaign_dir),
        "campaign_run_id": manifest.get("campaign_run_id", campaign_dir.name),
        "corpus_type": manifest.get("corpus_type"),
        "symbols": manifest.get("symbols", []),
        "campaign_quality": manifest.get("campaign_quality", {}),
        "corpus_quality": manifest.get("corpus_quality", {}),
        "thesis_validation_by_fill_model": thesis_by_fill_model,
        "primary_thesis_validation": primary_thesis,
    }


def _latest_decisions_payload() -> JsonDict:
    rows = load_decision_cards(limit=10)
    return {
        "ok": True,
        "count": len(rows),
        "decisions": rows,
    }


def _decision_feed_payload() -> JsonDict:
    rows = [_manual_decision_row(record) for record in load_decision_cards(limit=25)]
    ranked = sorted(rows, key=lambda row: (-float(row.get("score") or 0.0), str(row.get("timestamp") or "")))
    return {
        "ok": True,
        "status": "decision_feed_ready",
        "generated_at": datetime.now(UTC).isoformat(),
        "mode": "manual_replication_feed",
        "count": len(ranked),
        "decisions": ranked[:20],
    }


def _manual_decision_row(record: JsonDict) -> JsonDict:
    decision = record.get("decision_card", record)
    contract = decision.get("selected_contract") or {}
    status = str(decision.get("decision") or "UNKNOWN")
    action = "BUY_TO_OPEN" if status == "TRADE_CANDIDATE" and contract else "NO_TRADE"
    option_symbol = str(contract.get("option_symbol") or "")
    confidence = _float_or_zero(decision.get("confidence_score"))
    spread_pct = _float_or_zero(contract.get("spread_pct"))
    iv = _float_or_zero(contract.get("implied_volatility"))
    blocked_reason = decision.get("blocked_reason")
    row = {
        "decision_id": decision.get("decision_id"),
        "timestamp": decision.get("timestamp") or record.get("recorded_at"),
        "ticker": decision.get("ticker"),
        "decision": status,
        "action": action,
        "manual_status": "candidate" if action == "BUY_TO_OPEN" else "blocked_or_watch",
        "blocked_reason": blocked_reason,
        "trade_setup": decision.get("trade_setup"),
        "execution_layer": decision.get("execution_layer"),
        "confidence_score": confidence,
        "direction_bias": (decision.get("direction") or {}).get("bias"),
        "direction_score": (decision.get("direction") or {}).get("score"),
        "regime": (decision.get("regime") or {}).get("primary"),
        "option_symbol": option_symbol or None,
        **_option_symbol_parts(option_symbol),
        "entry_reference": contract.get("mid"),
        "bid": contract.get("bid"),
        "ask": contract.get("ask"),
        "spread_pct": spread_pct,
        "implied_volatility": iv,
        "delta": contract.get("delta"),
        "target_exit_mid": contract.get("target_exit_mid"),
        "stop_exit_mid": contract.get("stop_exit_mid"),
        "exit_rule": contract.get("exit_rule"),
        "reason_codes": decision.get("reason_codes", []),
        "score_reasons": contract.get("score_reasons", []),
        "explanation": decision.get("explanation"),
        "score": _decision_feed_score(status=status, confidence=confidence, spread_pct=spread_pct, blocked_reason=blocked_reason),
    }
    if not option_symbol:
        row["underlying"] = decision.get("ticker")
    return row


def _decision_feed_score(*, status: str, confidence: float, spread_pct: float, blocked_reason: Any) -> float:
    if status == "TRADE_CANDIDATE":
        return round(80.0 + confidence * 20.0 - min(20.0, spread_pct * 50.0), 4)
    if blocked_reason:
        return 20.0
    return 10.0


def _latest_bucket_edge_payload() -> JsonDict:
    campaign_dir = _latest_campaign_dir()
    if campaign_dir is None:
        return {"ok": False, "status": "no_campaign_found"}
    report = _read_json(campaign_dir / "bucket_edge_report.json")
    summary = []
    for bucket_key, bucket in report.get("buckets", {}).items():
        metrics = bucket.get("metrics_by_fill_model", {})
        summary.append(
            {
                "bucket": bucket_key,
                "fill_models": {
                    fill_model: {
                        "closed_trades": values.get("closed_trades"),
                        "profit_factor": values.get("profit_factor"),
                        "expectancy": values.get("expectancy"),
                        "unresolved_position_rate": values.get("unresolved_position_rate"),
                        "thesis_pass_rate": values.get("thesis_pass_rate"),
                        "tactical_2dte_pass_rate": values.get("tactical_2dte_pass_rate"),
                    }
                    for fill_model, values in metrics.items()
                },
            }
        )
    return {"ok": True, "artifact_dir": str(campaign_dir), "bucket_count": len(summary), "buckets": summary}


def _latest_thesis_failures_payload() -> JsonDict:
    campaign_dir = _latest_campaign_dir()
    if campaign_dir is None:
        return {"ok": False, "status": "no_campaign_found"}
    primary_dir = campaign_dir / "fill_model_results" / "realistic_mid_penalty"
    thesis_rows = _read_jsonl(primary_dir / "thesis_validation.jsonl")
    decision_rows = {row.get("decision_id"): row for row in _read_jsonl(primary_dir / "decisions.jsonl")}
    failures = []
    for row in thesis_rows:
        if row.get("passed"):
            continue
        decision = decision_rows.get(row.get("decision_id"), {})
        failures.append(
            {
                "decision_id": row.get("decision_id"),
                "ticker": row.get("ticker"),
                "trade_setup": row.get("trade_setup"),
                "option_type": row.get("option_type"),
                "reason": row.get("reason"),
                "contract_dte_days": row.get("contract_dte_days"),
                "net_move_pct": row.get("net_move_pct"),
                "first_move_pct": row.get("first_move_pct"),
                "adverse_move_pct": row.get("adverse_move_pct"),
                "followthrough_rate": row.get("followthrough_rate"),
                "first_move_match": row.get("first_move_match"),
                "reversal_confirmed": row.get("reversal_confirmed"),
                "confidence_score": decision.get("confidence_score"),
                "decision": decision.get("decision"),
                "reason_codes": decision.get("reason_codes", []),
            }
        )
    ranked = sorted(failures, key=_thesis_failure_sort_key)
    return {
        "ok": True,
        "artifact_dir": str(campaign_dir),
        "fill_model": "realistic_mid_penalty",
        "count": len(ranked),
        "failures": ranked[:8],
    }


def _latest_gate_candidate_payload() -> JsonDict:
    campaign_dir = _latest_campaign_dir()
    if campaign_dir is None:
        return {"ok": False, "status": "no_campaign_found"}
    report = _read_json(campaign_dir / "gate_candidate_report.json")
    for candidate in report.get("bucket_candidates", {}).values():
        candidate["live_enabled"] = False
        candidate["manual_approval_required"] = True
    return {
        "ok": True,
        "artifact_dir": str(campaign_dir),
        "live_enabled": False,
        "manual_approval_required": True,
        "bucket_candidates": report.get("bucket_candidates", {}),
    }


def _latest_decision_lab_payload() -> JsonDict:
    campaign_dir = _latest_campaign_dir()
    if campaign_dir is None:
        return {"ok": False, "status": "no_campaign_found"}
    return build_decision_lab_report(campaign_dir)


def _decision_lab_backfill_run_payload(payload: JsonDict) -> JsonDict:
    requested_symbols = [str(symbol).upper() for symbol in payload.get("symbols", ["SPY", "QQQ"])]
    symbols = list(dict.fromkeys(requested_symbols))[:DECISION_LAB_MAX_SYNC_SYMBOLS]
    days = min(max(int(payload.get("days", 2)), 1), DECISION_LAB_MAX_SYNC_DAYS)
    interval_minutes = min(max(int(payload.get("interval_minutes", DECISION_LAB_MIN_SYNC_INTERVAL_MINUTES)), DECISION_LAB_MIN_SYNC_INTERVAL_MINUTES), 60)
    end_date = _parse_date(payload.get("end_date")) or datetime.now(UTC).date()
    start_date = _parse_date(payload.get("start_date")) or (end_date - timedelta(days=days))
    earliest_sync_start = end_date - timedelta(days=DECISION_LAB_MAX_SYNC_DAYS - 1)
    if start_date < earliest_sync_start:
        start_date = earliest_sync_start
    run_id = str(payload.get("campaign_run_id", datetime.now(UTC).strftime("decision-lab-%Y%m%d-%H%M%S")))
    corpus_root = _artifacts_root() / "decision_lab_historical_corpus" / run_id
    backfill = run_historical_backfill(
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        corpus_root=corpus_root,
        interval_minutes=interval_minutes,
    )
    gate_before = _file_hash(_gate_path())
    campaign = run_phase1_campaign(
        corpus_root,
        artifacts_root=_artifacts_root(),
        campaign_run_id=run_id,
        active_gate_path=_gate_path(),
    )
    gate_after = _file_hash(_gate_path())
    report = build_decision_lab_report(_artifacts_root() / run_id)
    return {
        "ok": True,
        "operational_limits": {
            "requested_symbols": requested_symbols,
            "used_symbols": symbols,
            "max_sync_symbols": DECISION_LAB_MAX_SYNC_SYMBOLS,
            "max_sync_days": DECISION_LAB_MAX_SYNC_DAYS,
            "min_sync_interval_minutes": DECISION_LAB_MIN_SYNC_INTERVAL_MINUTES,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "interval_minutes": interval_minutes,
        },
        "backfill": backfill,
        "campaign": campaign | {"active_gate_changed": gate_before != gate_after},
        "decision_lab": report,
    }


def _capture_start_payload(payload: JsonDict) -> JsonDict:
    symbols = [str(symbol).upper() for symbol in payload.get("symbols", ["SPY", "QQQ"])]
    minutes = int(payload.get("minutes", 5))
    interval_seconds = int(payload.get("interval_seconds", 60))
    if minutes > 180:
        raise ValueError("capture_minutes_exceed_v1_limit")
    gate_before = _file_hash(_gate_path())
    result = capture_now(
        symbols=symbols,
        minutes=minutes,
        interval_seconds=interval_seconds,
        corpus_root=_corpus_root(),
        active_gate_path=_gate_path(),
    )
    gate_after = _file_hash(_gate_path())
    result["active_gate_changed"] = gate_before != gate_after
    return result


def _campaign_run_payload(payload: JsonDict) -> JsonDict:
    corpus_root = Path(str(payload.get("corpus_root", _corpus_root())))
    run_id = str(payload.get("campaign_run_id", datetime.now(UTC).strftime("dashboard-%Y%m%d-%H%M%S")))
    gate_before = _file_hash(_gate_path())
    result = run_phase1_campaign(
        corpus_root,
        artifacts_root=_artifacts_root(),
        campaign_run_id=run_id,
        active_gate_path=_gate_path(),
    )
    gate_after = _file_hash(_gate_path())
    result["active_gate_changed"] = gate_before != gate_after
    return result


def _trading_cycle_run_payload(payload: JsonDict) -> JsonDict:
    symbols = [str(symbol).upper() for symbol in payload.get("symbols", ["SPY"])]
    quantity = int(payload.get("quantity", 1))
    position_count = int(payload.get("position_count", 0))
    daily_pnl = float(payload.get("daily_pnl", 0.0))
    result = run_trading_cycle(
        symbols=symbols,
        quantity=quantity,
        position_count=position_count,
        current_daily_realized_pnl=daily_pnl,
    )
    return {"ok": True, **result.to_json_dict()}


def _trading_session_run_payload(payload: JsonDict) -> JsonDict:
    symbols = [str(symbol).upper() for symbol in payload.get("symbols", ["SPY"])]
    quantity = int(payload.get("quantity", 1))
    interval_seconds = int(payload.get("interval_seconds", 300))
    max_cycles = int(payload.get("max_cycles", 1))
    position_count = int(payload.get("position_count", 0))
    daily_pnl = float(payload.get("daily_pnl", 0.0))
    result = run_trading_session(
        symbols=symbols,
        interval_seconds=interval_seconds,
        max_cycles=max_cycles,
        cycle_kwargs={
            "quantity": quantity,
            "position_count": position_count,
            "current_daily_realized_pnl": daily_pnl,
        },
    )
    return {"ok": True, **result.to_json_dict()}


def _execution_exit_payload(payload: JsonDict) -> JsonDict:
    broker_order_id = str(payload.get("broker_order_id", ""))
    limit_price = float(payload.get("limit_price", 0))
    positions = load_open_positions()
    position = next((item for item in positions if item.broker_order_id == broker_order_id), None)
    if position is None:
        raise ValueError("open_position_not_found")
    order = submit_exit_for_position(
        position,
        broker=AlpacaExecutionBroker(),
        limit_price=limit_price,
    )
    return {
        "ok": True,
        "broker_order_id": order.broker_order_id,
        "state": order.state.value,
        "source_position": broker_order_id,
    }


def _execution_cancel_payload(payload: JsonDict) -> JsonDict:
    broker_order_id = str(payload.get("broker_order_id", ""))
    result = cancel_open_order(
        broker_order_id=broker_order_id,
        broker=AlpacaExecutionBroker(),
    )
    return {"ok": True, "result": result}


def _execution_replace_payload(payload: JsonDict) -> JsonDict:
    broker_order_id = str(payload.get("broker_order_id", ""))
    limit_price = float(payload.get("limit_price", 0))
    result = replace_open_order(
        broker_order_id=broker_order_id,
        broker=AlpacaExecutionBroker(),
        limit_price=limit_price,
    )
    return {"ok": True, "result": result}


def _dashboard_html() -> str:
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>AutoBott Phase 1 Operator Console</title>
  <style>
    :root {
      --bg:#0b0f14;
      --bg-alt:#121821;
      --panel:#151d28;
      --panel-2:#192230;
      --panel-3:#0f151d;
      --text:#e7edf5;
      --muted:#97a6ba;
      --line:#243244;
      --accent:#3ddc97;
      --accent-dim:#204636;
      --warn:#f4b860;
      --danger:#ff6b6b;
      --info:#6ec1ff;
      --shadow:0 16px 40px rgba(0,0,0,0.28);
      --radius:18px;
    }
    * { box-sizing:border-box; }
    body {
      margin:0;
      font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background:
        radial-gradient(circle at top right, rgba(61,220,151,0.10), transparent 24rem),
        radial-gradient(circle at top left, rgba(110,193,255,0.08), transparent 22rem),
        linear-gradient(180deg, #0b0f14, #111824 55%, #0b0f14);
      color:var(--text);
      min-height:100vh;
    }
    .shell { max-width:1440px; margin:0 auto; padding:20px; }
    .topbar {
      display:grid;
      gap:18px;
      grid-template-columns: minmax(0, 1.4fr) minmax(320px, 1fr);
      align-items:stretch;
    }
    .hero, .meta-card, .panel {
      background:linear-gradient(180deg, rgba(21,29,40,0.98), rgba(15,21,29,0.98));
      border:1px solid var(--line);
      border-radius:var(--radius);
      box-shadow:var(--shadow);
    }
    .hero { padding:22px; }
    .hero-top, .meta-top, .section-head, .group-head, .panel-head {
      display:flex;
      align-items:center;
      justify-content:space-between;
      gap:12px;
      flex-wrap:wrap;
    }
    .eyebrow {
      color:var(--accent);
      letter-spacing:0.16em;
      text-transform:uppercase;
      font-size:12px;
      font-weight:700;
    }
    h1 {
      margin:10px 0 8px;
      font-size:clamp(28px, 4vw, 40px);
      line-height:1.05;
      letter-spacing:-0.03em;
    }
    .hero p, .meta-note, .section-note, .muted { color:var(--muted); }
    .chip-row, .status-row, .action-row { display:flex; flex-wrap:wrap; gap:10px; }
    .chip, .badge {
      display:inline-flex;
      align-items:center;
      gap:8px;
      border-radius:999px;
      padding:8px 12px;
      font-size:12px;
      font-weight:700;
      letter-spacing:0.06em;
      text-transform:uppercase;
      border:1px solid var(--line);
      background:rgba(255,255,255,0.03);
    }
    .badge.safe, .chip.safe { color:var(--accent); border-color:#275640; background:rgba(61,220,151,0.10); }
    .badge.warn, .chip.warn { color:var(--warn); border-color:#5b4423; background:rgba(244,184,96,0.10); }
    .badge.danger, .chip.danger { color:var(--danger); border-color:#5a2b2b; background:rgba(255,107,107,0.12); }
    .badge.info, .chip.info { color:var(--info); border-color:#244767; background:rgba(110,193,255,0.10); }
    .meta-card { padding:18px; display:grid; gap:16px; }
    .meta-grid {
      display:grid;
      grid-template-columns:repeat(2, minmax(0, 1fr));
      gap:12px;
    }
    .mini {
      background:rgba(255,255,255,0.02);
      border:1px solid var(--line);
      border-radius:14px;
      padding:12px;
    }
    .mini-label {
      font-size:11px;
      color:var(--muted);
      text-transform:uppercase;
      letter-spacing:0.10em;
      margin-bottom:8px;
    }
    .mono {
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      word-break:break-word;
    }
    main { display:grid; gap:18px; margin-top:18px; }
    .section {
      display:grid;
      gap:14px;
    }
    .grid {
      display:grid;
      grid-template-columns:repeat(auto-fit, minmax(250px, 1fr));
      gap:14px;
    }
    .panel { padding:16px; min-height:190px; }
    .panel-head h2, .group-head h2, .section-head h2 {
      margin:0;
      font-size:15px;
      text-transform:uppercase;
      letter-spacing:0.08em;
    }
    .panel-head h3 { margin:0; font-size:16px; }
    .panel-body { margin-top:14px; display:grid; gap:12px; }
    .metrics { display:grid; gap:10px; }
    .metric {
      display:flex;
      justify-content:space-between;
      gap:16px;
      border-bottom:1px solid rgba(255,255,255,0.05);
      padding-bottom:8px;
    }
    .metric:last-child { border-bottom:0; padding-bottom:0; }
    .metric-label { color:var(--muted); font-size:13px; }
    .metric-value { text-align:right; font-weight:600; }
    .metric-value.compact { font-size:13px; }
    .operator-layout {
      display:grid;
      grid-template-columns:minmax(0, 1.2fr) minmax(320px, 0.8fr);
      gap:18px;
    }
    .group-grid {
      display:grid;
      grid-template-columns:repeat(auto-fit, minmax(210px, 1fr));
      gap:12px;
      margin-top:14px;
    }
    .group {
      border:1px solid var(--line);
      background:rgba(255,255,255,0.02);
      border-radius:14px;
      padding:14px;
      display:grid;
      gap:12px;
    }
    .group-title {
      font-size:13px;
      font-weight:700;
      letter-spacing:0.08em;
      text-transform:uppercase;
      color:var(--muted);
    }
    button {
      border:1px solid transparent;
      border-radius:12px;
      padding:11px 13px;
      font-weight:700;
      font-size:13px;
      cursor:pointer;
      background:#203146;
      color:var(--text);
      transition:transform 120ms ease, border-color 120ms ease, background 120ms ease, opacity 120ms ease;
      text-align:left;
    }
    button:hover:not(:disabled) { transform:translateY(-1px); border-color:#32506f; }
    button:disabled { cursor:not-allowed; opacity:0.45; }
    button.primary { background:linear-gradient(180deg, #1c6e4a, #124b32); border-color:#2d7f59; }
    button.secondary { background:#1d2a39; border-color:#2f4157; }
    button.ghost { background:transparent; border-color:#2a394c; }
    .button-note { color:var(--muted); font-size:12px; }
    .log {
      min-height:260px;
      background:#091019;
      border:1px solid #1e2a38;
      border-radius:14px;
      padding:14px;
      display:grid;
      gap:10px;
      align-content:start;
    }
    .log-entry {
      border-left:3px solid #2d7f59;
      background:rgba(255,255,255,0.02);
      border-radius:10px;
      padding:10px 12px;
    }
    .log-entry.warn { border-left-color:var(--warn); }
    .log-entry.danger { border-left-color:var(--danger); }
    .log-label {
      font-size:11px;
      text-transform:uppercase;
      letter-spacing:0.08em;
      color:var(--muted);
      margin-bottom:4px;
    }
    .locked, .empty, .error-state {
      min-height:110px;
      display:grid;
      align-content:center;
      gap:6px;
      border:1px dashed var(--line);
      border-radius:14px;
      padding:16px;
      background:rgba(255,255,255,0.02);
    }
    .locked strong, .error-state strong, .empty strong { font-size:15px; }
    .locked strong { color:var(--warn); }
    .error-state strong { color:var(--danger); }
    details {
      border-top:1px solid rgba(255,255,255,0.06);
      padding-top:10px;
    }
    summary {
      cursor:pointer;
      color:var(--muted);
      font-size:12px;
      text-transform:uppercase;
      letter-spacing:0.08em;
      font-weight:700;
    }
    pre {
      margin:10px 0 0;
      white-space:pre-wrap;
      word-break:break-word;
      background:#0a1118;
      border:1px solid #1d2b3a;
      color:#c8d5e5;
      border-radius:12px;
      padding:12px;
      font-size:12px;
      max-height:260px;
      overflow:auto;
    }
    .table {
      width:100%;
      border-collapse:collapse;
      font-size:13px;
    }
    .table th, .table td {
      text-align:left;
      padding:8px 0;
      border-bottom:1px solid rgba(255,255,255,0.05);
      vertical-align:top;
    }
    .table th { color:var(--muted); font-weight:600; }
    .foot-note { color:var(--muted); font-size:12px; }
    @media (max-width: 1080px) {
      .topbar, .operator-layout { grid-template-columns:1fr; }
    }
    @media (max-width: 720px) {
      .shell { padding:14px; }
      .meta-grid, .group-grid { grid-template-columns:1fr; }
      .grid { grid-template-columns:1fr; }
    }
  </style>
</head>
<body>
  <div class="shell">
    <header class="topbar">
      <section class="hero">
        <div class="hero-top">
          <span class="eyebrow">AutoBott / Trader's Corner</span>
          <div class="status-row">
            <span class="badge safe" id="mode-badge">PAPER ONLY</span>
            <span class="badge warn" id="live-lock-badge">LIVE TRADING LOCKED</span>
            <span class="badge info" id="execution-badge">EXECUTION CHECKING</span>
          </div>
        </div>
        <h1>AutoBott Phase 1 Operator Console</h1>
        <p>Production operator command center for paper capture, advisory replay, report review, and gate safety verification.</p>
        <div class="muted mono" id="mode-banner-text">PAPER ONLY | LIVE TRADING LOCKED | EXECUTION CHECKING</div>
        <div class="chip-row">
          <span class="chip info">Current Service <span id="service-name">autobott-phase1-dashboard</span></span>
          <span class="chip warn" id="auth-badge">LOCKED</span>
          <span class="chip safe" id="service-badge">BOOT CHECK RUNNING</span>
        </div>
      </section>

      <aside class="meta-card">
        <div class="meta-top">
          <h2 style="margin:0;">System Status</h2>
          <span class="badge info mono" id="version-badge">Version loading</span>
        </div>
        <div class="meta-grid">
          <div class="mini">
            <div class="mini-label">Environment</div>
            <div class="mono" id="env-value">PAPER ONLY</div>
          </div>
          <div class="mini">
            <div class="mini-label">Auth State</div>
            <div class="mono" id="auth-state-text">LOCKED</div>
          </div>
          <div class="mini">
            <div class="mini-label">Health</div>
            <div class="mono" id="health-state-text">Checking</div>
          </div>
          <div class="mini">
            <div class="mini-label">Persistence Root</div>
            <div class="mono" id="persistence-root-text">Waiting for data</div>
          </div>
        </div>
        <div class="meta-note">Execution is operator-controlled, paper-first, and live-locked until explicitly enabled elsewhere.</div>
      </aside>
    </header>

    <main>
      <section class="section">
        <div class="section-head">
          <div>
            <h2>Operator Snapshot</h2>
            <div class="section-note">High-signal status cards for safety, connectivity, capture state, and latest campaign output.</div>
          </div>
          <span class="badge info" id="last-refresh">Awaiting refresh</span>
        </div>
        <div class="grid">
          <section class="panel">
            <div class="panel-head"><h3>Alpaca Paper Config</h3><span class="badge safe">STATUS</span></div>
            <div class="panel-body" id="alpaca-status"></div>
          </section>
          <section class="panel">
            <div class="panel-head"><h3>Account Summary</h3><span class="badge info">P/L</span></div>
            <div class="panel-body" id="account-summary"></div>
          </section>
          <section class="panel">
            <div class="panel-head"><h3>Volatility Scout</h3><span class="badge warn">OPTIONS FEED</span></div>
            <div class="panel-body" id="options-scout"></div>
          </section>
          <section class="panel">
            <div class="panel-head"><h3>Decision Feed</h3><span class="badge safe">MANUAL MIRROR</span></div>
            <div class="panel-body" id="decision-feed"></div>
          </section>
          <section class="panel">
            <div class="panel-head"><h3>Order Timeline</h3><span class="badge info">REGIME TRACE</span></div>
            <div class="panel-body" id="options-timeline"></div>
          </section>
          <section class="panel">
            <div class="panel-head"><h3>Open Positions</h3><span class="badge info">P/L</span></div>
            <div class="panel-body" id="account-positions"></div>
          </section>
          <section class="panel">
            <div class="panel-head"><h3>Recent Trades</h3><span class="badge info">HISTORY</span></div>
            <div class="panel-body" id="account-orders"></div>
          </section>
          <section class="panel">
            <div class="panel-head"><h3>Paper Readiness</h3><span class="badge info">EXECUTION</span></div>
            <div class="panel-body" id="paper-readiness"></div>
          </section>
          <section class="panel">
            <div class="panel-head"><h3>Latest Capture</h3><span class="badge info">CAPTURE</span></div>
            <div class="panel-body" id="corpus-status"></div>
          </section>
          <section class="panel">
            <div class="panel-head"><h3>Latest Campaign</h3><span class="badge info">REPLAY</span></div>
            <div class="panel-body" id="campaign-status"></div>
          </section>
          <section class="panel">
            <div class="panel-head"><h3>Active Gate Safety</h3><span class="badge warn">SAFETY / GATE</span></div>
            <div class="panel-body" id="safety-status"></div>
          </section>
          <section class="panel">
            <div class="panel-head"><h3>Session Supervisor</h3><span class="badge info">AUTOMATION</span></div>
            <div class="panel-body" id="session-status"></div>
          </section>
          <section class="panel">
            <div class="panel-head"><h3>Bucket Edge Summary</h3><span class="badge info">REPORTS</span></div>
            <div class="panel-body" id="bucket-report"></div>
          </section>
          <section class="panel">
            <div class="panel-head"><h3>Decision Lab</h3><span class="badge info">BASELINES</span></div>
            <div class="panel-body" id="decision-lab"></div>
          </section>
          <section class="panel">
            <div class="panel-head"><h3>Worst Thesis Failures</h3><span class="badge danger">REPORTS</span></div>
            <div class="panel-body" id="thesis-failures"></div>
          </section>
          <section class="panel">
            <div class="panel-head"><h3>Gate Candidate Summary</h3><span class="badge warn">REPORTS</span></div>
            <div class="panel-body" id="gate-report"></div>
          </section>
          <section class="panel">
            <div class="panel-head"><h3>Persistence Status</h3><span class="badge info">STORAGE</span></div>
            <div class="panel-body" id="persistence-status"></div>
          </section>
        </div>
      </section>

      <section class="operator-layout">
        <section class="panel">
          <div class="group-head">
            <div>
              <h2>Operator Actions</h2>
              <div class="section-note">Controlled flows only. Token-gated actions stay disabled until authentication succeeds.</div>
            </div>
            <span class="badge warn" id="action-state">TOKEN REQUIRED</span>
          </div>
          <div class="group-grid">
            <div class="group">
              <div class="group-title">Auth</div>
              <button class="primary" onclick="setToken()">Set Dashboard Token</button>
              <button class="ghost" onclick="clearToken()">Clear Token</button>
              <div class="button-note">Locked panels will show “Dashboard token required” until a valid token is accepted.</div>
            </div>
            <div class="group">
              <div class="group-title">Runtime</div>
              <button class="primary protected-action" onclick="armPaperMode()">Arm paper execution</button>
              <button class="secondary protected-action" onclick="disableExecution()">Disable execution</button>
              <button class="ghost protected-action" onclick="engageKillSwitch()">Engage kill switch</button>
              <div class="button-note">These controls affect paper execution only. Live mode remains locked.</div>
            </div>
            <div class="group">
              <div class="group-title">Capture</div>
              <button class="primary protected-action" onclick="startCapture(5)">Run 5-minute capture</button>
              <button class="secondary protected-action" onclick="startCapture(30)">Run 30-minute capture</button>
              <div class="button-note">Paper-only snapshot capture for evidence and diagnostics.</div>
            </div>
            <div class="group">
              <div class="group-title">Campaign</div>
              <button class="primary protected-action" onclick="runCampaign()">Run campaign from latest corpus</button>
              <button class="secondary protected-action" onclick="runDecisionLabBackfill()">Run historical decision lab</button>
              <div class="button-note">Advisory replay only. Live trading remains disabled.</div>
            </div>
            <div class="group">
              <div class="group-title">Trading Cycle</div>
              <button class="primary protected-action" onclick="runTradingCycle()">Run protected trading cycle</button>
              <button class="secondary protected-action" onclick="startPaperSession()">Start paper session</button>
              <button class="ghost protected-action" onclick="reconcileExecution()">Reconcile open orders</button>
              <div class="button-note">Capture, decision, and broker submit when runtime controls permit.</div>
            </div>
            <div class="group">
              <div class="group-title">Refresh</div>
              <button class="secondary" onclick="refreshAll()">Refresh all panels</button>
              <button class="ghost protected-action" onclick="refreshProtected()">Refresh protected only</button>
              <div class="button-note">Health is public-by-design. Protected panels still fail closed.</div>
            </div>
          </div>
        </section>

        <aside class="panel">
          <div class="group-head">
            <div>
              <h2>Operator Log</h2>
              <div class="section-note">Compact console log with plain-English action results.</div>
            </div>
            <span class="badge info">LATEST</span>
          </div>
          <div class="panel-body">
            <div class="log" id="action-log">
              <div class="log-entry">
                <div class="log-label">Status</div>
                <div>Console ready. Protected panels are locked until authentication succeeds.</div>
              </div>
            </div>
          </div>
        </aside>
      </section>
    </main>
  </div>
  <script>
    const dashboardState = {
      authState: 'LOCKED',
      version: 'loading',
      safety: null,
      corpus: null,
      campaign: null,
      session: null
    };

    const apiHeaders = () => {
      const token = sessionStorage.getItem('dashboardToken') || '';
      return token ? { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' } : { 'Content-Type': 'application/json' };
    };

    async function callApi(path, options = {}) {
      const response = await fetch(path, { ...options, headers: { ...apiHeaders(), ...(options.headers || {}) } });
      let payload = {};
      try {
        payload = await response.json();
      } catch {
        payload = {};
      }
      return { ok: response.ok, status: response.status, payload };
    }

    function setToken() {
      const value = window.prompt('Enter dashboard auth token');
      if (value) {
        sessionStorage.setItem('dashboardToken', value);
        logEntry('Token updated', 'Dashboard token stored in browser session. Refreshing protected panels.', 'warn');
        refreshAll();
      }
    }

    function clearToken() {
      sessionStorage.removeItem('dashboardToken');
      setAuthState('LOCKED');
      syncActionState();
      logEntry('Token cleared', 'Protected panels are locked again until a valid token is set.', 'warn');
      refreshAll();
    }

    function setAuthState(state) {
      dashboardState.authState = state;
      document.getElementById('auth-badge').textContent = state;
      document.getElementById('auth-badge').className = `chip ${state === 'AUTHENTICATED' ? 'safe' : 'warn'}`;
      document.getElementById('auth-state-text').textContent = state;
      document.getElementById('action-state').textContent = state === 'AUTHENTICATED' ? 'CONTROLLED ACCESS' : 'TOKEN REQUIRED';
      document.getElementById('action-state').className = `badge ${state === 'AUTHENTICATED' ? 'safe' : 'warn'}`;
    }

    function syncActionState() {
      const tokenPresent = !!sessionStorage.getItem('dashboardToken');
      const enabled = tokenPresent && dashboardState.authState === 'AUTHENTICATED';
      document.querySelectorAll('.protected-action').forEach((button) => {
        button.disabled = !enabled;
      });
    }

    function updateRefreshStamp() {
      document.getElementById('last-refresh').textContent = `Refreshed ${new Date().toLocaleTimeString()}`;
    }

    function statusBadge(text, tone = 'info') {
      return `<span class="badge ${tone}">${text}</span>`;
    }

    function detailsBlock(payload) {
      return `<details><summary>Raw JSON</summary><pre>${escapeHtml(JSON.stringify(payload, null, 2))}</pre></details>`;
    }

    function escapeHtml(value) {
      return String(value)
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;');
    }

    function metricList(items) {
      return `<div class="metrics">${items.map(([label, value]) => `
        <div class="metric">
          <div class="metric-label">${escapeHtml(label)}</div>
          <div class="metric-value compact">${value}</div>
        </div>`).join('')}
      </div>`;
    }

    function lockedState() {
      return `
        <div class="locked">
          <strong>Locked</strong>
          <div>Dashboard token required</div>
          <div class="muted">Set token to view this panel.</div>
        </div>`;
    }

    function emptyState(title, detail) {
      return `<div class="empty"><strong>${escapeHtml(title)}</strong><div>${escapeHtml(detail)}</div></div>`;
    }

    function errorState(title, detail, payload = null) {
      return `
        <div class="error-state">
          <strong>${escapeHtml(title)}</strong>
          <div>${escapeHtml(detail)}</div>
        </div>
        ${payload ? detailsBlock(payload) : ''}`;
    }

    function logEntry(title, detail, tone = 'safe') {
      const container = document.getElementById('action-log');
      const entry = document.createElement('div');
      entry.className = `log-entry ${tone === 'danger' ? 'danger' : tone === 'warn' ? 'warn' : ''}`;
      entry.innerHTML = `<div class="log-label">${escapeHtml(new Date().toLocaleTimeString())} · ${escapeHtml(title)}</div><div>${escapeHtml(detail)}</div>`;
      container.prepend(entry);
      while (container.children.length > 6) {
        container.removeChild(container.lastChild);
      }
    }

    function renderHealth(payload) {
      dashboardState.version = payload.version || 'dev';
      document.getElementById('service-name').textContent = payload.app || 'autobott-phase1-dashboard';
      document.getElementById('version-badge').textContent = payload.version || 'dev';
      document.getElementById('health-state-text').textContent = payload.ok ? 'OK' : 'CHECK FAILED';
      document.getElementById('service-badge').textContent = payload.ok ? 'SERVICE HEALTHY' : 'HEALTH CHECK FAILED';
      document.getElementById('service-badge').className = `chip ${payload.ok ? 'safe' : 'danger'}`;
    }

    function renderProtectedPanel(targetId, result, formatter) {
      const target = document.getElementById(targetId);
      if (result.status === 401) {
        target.innerHTML = lockedState();
        return false;
      }
      if (!result.ok) {
        target.innerHTML = errorState('Request failed', result.payload.detail || result.payload.error || 'Unknown error', result.payload);
        return false;
      }
      setAuthState('AUTHENTICATED');
      target.innerHTML = formatter(result.payload);
      return true;
    }

    function renderSafety(payload) {
      dashboardState.safety = payload;
      const gateHash = payload.active_gate_hash ? `${payload.active_gate_hash.slice(0, 12)}...` : 'missing';
      document.getElementById('env-value').textContent = payload.paper_only ? 'PAPER ONLY' : 'UNKNOWN';
      document.getElementById('mode-banner-text').textContent = payload.mode_banner || 'PAPER ONLY | LIVE TRADING LOCKED | EXECUTION CHECKING';
      document.getElementById('mode-badge').textContent = payload.paper_only ? 'PAPER ONLY' : 'MODE CHECK';
      document.getElementById('mode-badge').className = `badge ${payload.paper_only ? 'safe' : 'warn'}`;
      document.getElementById('live-lock-badge').textContent = payload.live_trading_enabled ? 'LIVE MODE FLAGGED' : 'LIVE TRADING LOCKED';
      document.getElementById('live-lock-badge').className = `badge ${payload.live_trading_enabled ? 'danger' : 'warn'}`;
      document.getElementById('execution-badge').textContent = payload.order_placement_enabled ? 'PAPER EXECUTION ARMED' : (payload.order_placement_configured ? 'EXECUTION PAUSED' : 'ORDERS CONFIG DISABLED');
      document.getElementById('execution-badge').className = `badge ${payload.order_placement_enabled ? 'danger' : payload.order_placement_configured ? 'warn' : 'info'}`;
      return `
        ${metricList([
          ['Mode', payload.paper_only ? statusBadge('PAPER ONLY', 'safe') : statusBadge('UNKNOWN', 'warn')],
          ['Live trading', payload.live_trading_enabled ? statusBadge('ENABLED', 'danger') : statusBadge('LOCKED', 'safe')],
          ['Order placement', payload.order_placement_enabled ? statusBadge('ARMED', 'danger') : statusBadge(payload.order_placement_configured ? 'PAUSED' : 'CONFIG DISABLED', payload.order_placement_configured ? 'warn' : 'safe')],
          ['Trade-through', payload.paper_trade_through_enabled ? statusBadge('ENABLED', 'warn') : statusBadge('DISABLED', 'info')],
          ['Gate mutations', payload.active_gate_mutation_allowed ? statusBadge('ALLOWED', 'danger') : statusBadge('BLOCKED', 'safe')],
          ['Order methods', payload.order_methods_present ? statusBadge('PRESENT', 'danger') : statusBadge('ABSENT', 'safe')],
          ['Gate hash', `<span class="mono">${escapeHtml(gateHash)}</span>`]
        ])}
        ${detailsBlock(payload)}`;
    }

    function renderAlpaca(payload) {
      const tone = payload.ok ? 'safe' : 'warn';
      return `
        ${metricList([
          ['Environment', statusBadge(payload.paper_only ? 'PAPER' : 'UNKNOWN', tone)],
          ['Connection', statusBadge(payload.status || 'unknown', payload.ok ? 'safe' : 'warn')],
          ['Credentials', statusBadge(payload.credentials_present ? 'PRESENT' : 'MISSING', payload.credentials_present ? 'safe' : 'warn')],
          ['Order placement', payload.order_placement_enabled ? statusBadge('ARMED', 'danger') : statusBadge(payload.order_placement_configured ? 'PAUSED' : 'CONFIG DISABLED', payload.order_placement_configured ? 'warn' : 'safe')],
          ['Account status', escapeHtml(payload.account_status || 'not available')],
          ['Quote checks', `<span class="mono">${escapeHtml(JSON.stringify(payload.quote_checks || {}, null, 0))}</span>`]
        ])}
        ${detailsBlock(payload)}`;
    }

    function formatMoney(value) {
      const num = Number(value);
      if (Number.isNaN(num)) return 'n/a';
      return `$${num.toFixed(2)}`;
    }

    function renderAccountSummary(payload) {
      if (!payload.ok) {
        return emptyState('Account unavailable', payload.detail || 'Could not load Alpaca account summary.');
      }
      const account = payload.account || {};
      const plTone = account.day_pl > 0 ? 'safe' : account.day_pl < 0 ? 'danger' : 'info';
      return `
        ${metricList([
          ['Equity', formatMoney(account.equity)],
          ['Day P/L', `<span class="badge ${plTone}">${formatMoney(account.day_pl)} (${Number(account.day_pl_pct || 0).toFixed(2)}%)</span>`],
          ['Cash', formatMoney(account.cash)],
          ['Buying power', formatMoney(account.buying_power)],
          ['Portfolio value', formatMoney(account.portfolio_value)]
        ])}
        ${detailsBlock(payload)}`;
    }

    function renderAccountPositions(payload) {
      if (!payload.ok) {
        return emptyState('Positions unavailable', payload.detail || 'Could not load open positions.');
      }
      const positions = payload.positions || [];
      const rows = positions.map((position) => {
        const pl = Number(position.unrealized_pl);
        const tone = pl > 0 ? 'safe' : pl < 0 ? 'danger' : 'info';
        return `<tr>
          <td>${escapeHtml(position.symbol || 'n/a')}</td>
          <td>${escapeHtml(position.option_type || 'n/a')}</td>
          <td>${escapeHtml(position.strike ?? 'n/a')}</td>
          <td>${escapeHtml(position.side || 'n/a')}</td>
          <td>${escapeHtml(position.qty ?? 'n/a')}</td>
          <td>${formatMoney(position.avg_entry_price)}</td>
          <td>${formatMoney(position.current_price)}</td>
          <td><span class="badge ${tone}">${formatMoney(position.unrealized_pl)}</span></td>
        </tr>`;
      }).join('');
      return `
        <table class="table">
          <thead><tr><th>Symbol</th><th>Type</th><th>Strike</th><th>Side</th><th>Qty</th><th>Entry</th><th>Current</th><th>Unrealized P/L</th></tr></thead>
          <tbody>${rows || '<tr><td colspan="8">No open positions</td></tr>'}</tbody>
        </table>
        ${detailsBlock(payload)}`;
    }

    function renderAccountOrders(payload) {
      if (!payload.ok) {
        return emptyState('Trade history unavailable', payload.detail || 'Could not load recent trades.');
      }
      const orders = payload.orders || [];
      const rows = orders.map((order) => `
        <tr>
          <td>${escapeHtml(order.symbol || 'n/a')}</td>
          <td>${escapeHtml(order.option_type || 'n/a')}</td>
          <td>${escapeHtml(order.strike ?? 'n/a')}</td>
          <td>${escapeHtml(order.side || 'n/a')}</td>
          <td>${escapeHtml(order.filled_qty || order.qty || 'n/a')}</td>
          <td>${formatMoney(order.filled_avg_price)}</td>
          <td>${escapeHtml(order.status || 'n/a')}</td>
          <td>${escapeHtml(order.filled_at || order.submitted_at || 'n/a')}</td>
        </tr>`).join('');
      return `
        <table class="table">
          <thead><tr><th>Symbol</th><th>Type</th><th>Strike</th><th>Side</th><th>Qty</th><th>Fill price</th><th>Status</th><th>When</th></tr></thead>
          <tbody>${rows || '<tr><td colspan="8">No trades yet</td></tr>'}</tbody>
        </table>
        ${detailsBlock(payload)}`;
    }

    function renderOptionsScout(payload) {
      if (!payload.ok) {
        return emptyState('Scout unavailable', payload.detail || 'Could not load options scout feed.');
      }
      const rows = payload.scout_rows || [];
      const feedRows = rows.slice(0, 8).map((row) => {
        const attention = String(row.attention || 'watch');
        const tone = attention.includes('force') || attention.includes('stop') ? 'danger' : attention.includes('missing') || attention.includes('wide') ? 'warn' : attention.includes('working') ? 'safe' : 'info';
        const plpc = row.unrealized_plpc == null ? 'n/a' : `${(Number(row.unrealized_plpc) * 100).toFixed(1)}%`;
        return `<tr>
          <td><span class="mono">${escapeHtml(row.symbol || 'n/a')}</span></td>
          <td>${statusBadge(escapeHtml(attention.replaceAll('_', ' ')).toUpperCase(), tone)}</td>
          <td>${escapeHtml(row.profit_tier || row.decision || 'n/a')}</td>
          <td>${plpc}</td>
          <td>${formatMoney(row.current_price ?? row.mid)}</td>
          <td>${formatMoney(row.target_exit_price)}</td>
          <td>${row.pending_exit_order_id ? `<span class="mono">${escapeHtml(row.pending_exit_order_id)}</span>` : 'none'}</td>
        </tr>`;
      }).join('');
      const ladder = payload.profit_ladder || {};
      return `
        ${metricList([
          ['Mode', escapeHtml(payload.mode || 'options scout')],
          ['Rows', escapeHtml(payload.counts?.rows ?? 0)],
          ['Open positions', escapeHtml(payload.counts?.open_positions ?? 0)],
          ['Ladder', `${Number(ladder.initial_pct || 0) * 100}% / ${Number(ladder.tighten_pct || 0) * 100}% / ${Number(ladder.harvest_pct || 0) * 100}% / ${Number(ladder.force_exit_pct || 0) * 100}%`]
        ])}
        <table class="table">
          <thead><tr><th>Contract</th><th>Signal</th><th>Tier</th><th>P/L</th><th>Now</th><th>Target</th><th>Pending</th></tr></thead>
          <tbody>${feedRows || '<tr><td colspan="7">No scout rows</td></tr>'}</tbody>
        </table>
        ${detailsBlock(payload)}`;
    }

    function renderDecisionFeed(payload) {
      if (!payload.ok) {
        return emptyState('Decision feed unavailable', payload.detail || 'Could not load bot decisions.');
      }
      const rows = payload.decisions || [];
      const feedRows = rows.slice(0, 8).map((row) => {
        const candidate = row.action === 'BUY_TO_OPEN';
        const tone = candidate ? 'safe' : row.blocked_reason ? 'warn' : 'info';
        const reasons = (row.reason_codes || row.score_reasons || []).slice(0, 2).join(', ') || row.blocked_reason || 'watch';
        return `<tr>
          <td>${statusBadge(escapeHtml(row.action || 'NO TRADE'), tone)}</td>
          <td>${escapeHtml(row.ticker || row.underlying || 'n/a')}</td>
          <td>${escapeHtml(row.direction_bias || row.trade_setup || 'n/a')}</td>
          <td><span class="mono">${escapeHtml(row.option_symbol || 'none')}</span></td>
          <td>${formatMoney(row.entry_reference)}</td>
          <td>${formatMoney(row.target_exit_mid)}</td>
          <td>${formatMoney(row.stop_exit_mid)}</td>
          <td>${Number(row.confidence_score || 0).toFixed(2)}</td>
          <td>${escapeHtml(reasons)}</td>
        </tr>`;
      }).join('');
      return `
        ${metricList([
          ['Mode', escapeHtml(payload.mode || 'manual decision feed')],
          ['Rows', escapeHtml(payload.count ?? 0)],
          ['Top action', escapeHtml(rows[0]?.action || 'NONE')],
          ['Top contract', `<span class="mono">${escapeHtml(rows[0]?.option_symbol || 'none')}</span>`]
        ])}
        <table class="table">
          <thead><tr><th>Action</th><th>Ticker</th><th>Bias</th><th>Contract</th><th>Entry ref</th><th>Target</th><th>Stop</th><th>Conf</th><th>Why</th></tr></thead>
          <tbody>${feedRows || '<tr><td colspan="9">No decisions yet</td></tr>'}</tbody>
        </table>
        ${detailsBlock(payload)}`;
    }

    function renderOptionsTimeline(payload) {
      if (!payload.ok) {
        return emptyState('Timeline unavailable', payload.detail || 'Could not load option order timeline.');
      }
      const summary = payload.summary || {};
      const warningRows = (payload.warnings || []).slice(0, 3).map((warning) => `
        <div class="log-entry warn">
          <div class="log-label">${escapeHtml(warning.type || 'warning')}</div>
          <div>${escapeHtml(warning.detail || '')}</div>
        </div>`).join('');
      const clusterRows = (payload.clusters || []).slice(0, 5).map((cluster) => {
        const pnl = Number(cluster.realized_pnl || 0);
        const tone = pnl > 0 ? 'safe' : pnl < 0 ? 'danger' : 'info';
        return `<tr>
          <td>${escapeHtml(cluster.bucket_start || 'n/a')}</td>
          <td>${escapeHtml(cluster.buy_orders ?? 0)}</td>
          <td>${escapeHtml(cluster.sell_orders ?? 0)}</td>
          <td>${statusBadge(formatMoney(pnl), tone)}</td>
        </tr>`;
      }).join('');
      const tripRows = (payload.round_trips || []).slice(0, 5).map((trip) => {
        const pnl = Number(trip.pnl || 0);
        const tone = pnl > 0 ? 'safe' : pnl < 0 ? 'danger' : 'info';
        return `<tr>
          <td><span class="mono">${escapeHtml(trip.symbol || 'n/a')}</span></td>
          <td>${escapeHtml(trip.classification || 'n/a')}</td>
          <td>${formatMoney(trip.entry_price)}</td>
          <td>${formatMoney(trip.exit_price)}</td>
          <td>${statusBadge(formatMoney(pnl), tone)}</td>
        </tr>`;
      }).join('');
      return `
        ${metricList([
          ['Mode', escapeHtml(payload.mode || 'timeline')],
          ['Orders seen', escapeHtml(summary.orders_seen ?? 0)],
          ['Round trips', escapeHtml(summary.round_trips ?? 0)],
          ['Realized P/L', formatMoney(summary.realized_pnl)],
          ['Pending', escapeHtml(summary.pending_orders ?? 0)]
        ])}
        ${warningRows || '<div class="foot-note">No timeline warnings.</div>'}
        <table class="table">
          <thead><tr><th>Bucket</th><th>Buys</th><th>Sells</th><th>P/L</th></tr></thead>
          <tbody>${clusterRows || '<tr><td colspan="4">No clusters yet</td></tr>'}</tbody>
        </table>
        <table class="table">
          <thead><tr><th>Contract</th><th>Class</th><th>Entry</th><th>Exit</th><th>P/L</th></tr></thead>
          <tbody>${tripRows || '<tr><td colspan="5">No paired round trips yet</td></tr>'}</tbody>
        </table>
        ${detailsBlock(payload)}`;
    }

    function renderPaperReadiness(payload) {
      const tone = payload.paper_execution_ready ? 'safe' : payload.ok ? 'warn' : 'warn';
      return `
        ${metricList([
          ['Status', statusBadge(payload.status || 'unknown', tone)],
          ['Config valid', statusBadge(payload.paper_config_valid ? 'YES' : 'NO', payload.paper_config_valid ? 'safe' : 'warn')],
          ['Execution config', statusBadge(payload.paper_execution_config_valid ? 'YES' : 'NO', payload.paper_execution_config_valid ? 'safe' : 'warn')],
          ['Credentials', statusBadge(payload.credentials_present ? 'PRESENT' : 'MISSING', payload.credentials_present ? 'safe' : 'warn')],
          ['Execution ready', statusBadge(payload.paper_execution_ready ? 'YES' : 'NO', payload.paper_execution_ready ? 'danger' : 'warn')],
          ['Option snapshots', escapeHtml(payload.option_snapshot_count ?? 'n/a')],
          ['Option chain', escapeHtml(payload.option_chain_count ?? 'n/a')],
          ['Decision', escapeHtml(payload.decision_status || 'n/a')],
          ['Contract', `<span class="mono">${escapeHtml(payload.selected_contract || 'none')}</span>`]
        ])}
        ${detailsBlock(payload)}`;
    }

    function renderCorpus(payload) {
      dashboardState.corpus = payload;
      if (!payload.ok) {
        return emptyState('No paper capture found', 'Run a safe capture after authentication to populate this panel.');
      }
      return `
        ${metricList([
          ['Symbol', escapeHtml(payload.symbol || 'unknown')],
          ['Trading date', escapeHtml(payload.trading_date || 'unknown')],
          ['Snapshots', escapeHtml(payload.snapshots_captured ?? '0')],
          ['Option quotes', escapeHtml(payload.option_quotes_captured ?? '0')],
          ['Quality flags', escapeHtml((payload.data_quality_flags || []).join(', ') || 'None')]
        ])}
        ${detailsBlock(payload)}`;
    }

    function renderCampaign(payload) {
      dashboardState.campaign = payload;
      if (!payload.ok) {
        return emptyState('No campaign artifacts found', 'Run a campaign from the latest corpus after authentication.');
      }
      const thesis = payload.primary_thesis_validation || {};
      return `
        ${metricList([
          ['Campaign', escapeHtml(payload.campaign_run_id || 'unknown')],
          ['Corpus type', escapeHtml(payload.corpus_type || 'unknown')],
          ['Symbols', escapeHtml((payload.symbols || []).join(', ') || 'unknown')],
          ['Campaign valid', statusBadge(payload.campaign_quality?.campaign_valid ? 'VALID' : 'PENDING', payload.campaign_quality?.campaign_valid ? 'safe' : 'warn')],
          ['Trading days', escapeHtml(payload.corpus_quality?.trading_days ?? 'unknown')],
          ['Theory pass', escapeHtml(thesis.pass_rate ?? 'n/a')],
          ['2DTE pass', escapeHtml(thesis.tactical_2dte_pass_rate ?? 'n/a')],
          ['Reversal pass', escapeHtml(thesis.reversal_pass_rate ?? 'n/a')]
        ])}
        ${detailsBlock(payload)}`;
    }

    function renderSession(payload) {
      dashboardState.session = payload;
      const state = payload.state || {};
      const config = payload.config || {};
      const tone = state.last_error ? 'danger' : (state.running ? 'safe' : 'warn');
      return `
        ${metricList([
          ['Autostart', statusBadge(config.enabled ? 'ENABLED' : 'DISABLED', config.enabled ? 'safe' : 'warn')],
          ['Thread', statusBadge(payload.thread_alive ? 'RUNNING' : 'IDLE', tone)],
          ['Symbols', `<span class="mono">${escapeHtml((config.symbols || []).join(', ') || 'n/a')}</span>`],
          ['Interval', escapeHtml(String(config.interval_seconds ?? 'n/a'))],
          ['Max cycles', escapeHtml(String(config.max_cycles ?? 'continuous'))],
          ['Last error', state.last_error ? `<span class="mono">${escapeHtml(state.last_error)}</span>` : statusBadge('NONE', 'safe')],
          ['Last result', state.last_result ? statusBadge(`CYCLES ${state.last_result.cycles_completed ?? 0}`, 'safe') : statusBadge('NONE', 'warn')]
        ])}
        ${detailsBlock(payload)}`;
    }

    function renderBucketReport(payload) {
      if (!payload.ok) {
        return emptyState('No bucket edge report', 'Run a campaign to generate advisory bucket metrics.');
      }
      const rows = (payload.buckets || []).slice(0, 4).map((bucket) => {
        const primary = bucket.fill_models?.realistic_mid_penalty || {};
        return `<tr><td>${escapeHtml(bucket.bucket)}</td><td>${escapeHtml(primary.closed_trades ?? '0')}</td><td>${escapeHtml(primary.profit_factor ?? 'n/a')}</td><td>${escapeHtml(primary.tactical_2dte_pass_rate ?? 'n/a')}</td></tr>`;
      }).join('');
      return `
        <table class="table">
          <thead><tr><th>Bucket</th><th>Closed</th><th>PF</th><th>2DTE</th></tr></thead>
          <tbody>${rows || '<tr><td colspan="4">No bucket data</td></tr>'}</tbody>
        </table>
        ${detailsBlock(payload)}`;
    }

    function renderDecisionLab(payload) {
      if (!payload.ok) {
        return emptyState('No decision lab report', 'Run a campaign to score buckets against baselines.');
      }
      const summary = payload.summary || {};
      const actual = payload.baselines?.actual_strategy || {};
      const recRows = (payload.recommendations || []).slice(0, 4).map((row) => `
        <tr>
          <td>${statusBadge(escapeHtml(row.severity || 'info').toUpperCase(), row.severity || 'info')}</td>
          <td>${escapeHtml(row.action || 'unknown')}</td>
          <td>${escapeHtml(row.bucket || row.reason || 'n/a')}</td>
        </tr>`).join('');
      const bucketRows = (payload.buckets || []).slice(0, 4).map((bucket) => `
        <tr>
          <td>${escapeHtml(bucket.bucket)}</td>
          <td>${escapeHtml(bucket.closed_trades ?? 0)}</td>
          <td>${escapeHtml(bucket.expectancy ?? 'n/a')}</td>
          <td>${statusBadge(escapeHtml(bucket.status || 'unknown').toUpperCase(), bucket.status === 'approved' ? 'safe' : bucket.status === 'underperforming' ? 'danger' : 'warn')}</td>
        </tr>`).join('');
      return `
        ${metricList([
          ['Closed trades', escapeHtml(summary.closed_trades ?? actual.closed_trades ?? 0)],
          ['Actual vs no trade', escapeHtml(payload.baselines?.actual_vs_no_trade ?? 'n/a')],
          ['Expectancy', escapeHtml(actual.expectancy ?? summary.expectancy_per_trade ?? 'n/a')],
          ['Profit factor', escapeHtml(actual.profit_factor ?? summary.profit_factor ?? 'n/a')]
        ])}
        <table class="table">
          <thead><tr><th>Action</th><th>Type</th><th>Reason</th></tr></thead>
          <tbody>${recRows || '<tr><td colspan="3">No recommendations</td></tr>'}</tbody>
        </table>
        <table class="table">
          <thead><tr><th>Bucket</th><th>Closed</th><th>Expectancy</th><th>Status</th></tr></thead>
          <tbody>${bucketRows || '<tr><td colspan="4">No bucket rows</td></tr>'}</tbody>
        </table>
        ${detailsBlock(payload)}`;
    }

    function renderGateReport(payload) {
      if (!payload.ok) {
        return emptyState('No gate candidate report', 'Run a campaign to generate candidate review output.');
      }
      const rows = Object.entries(payload.bucket_candidates || {}).slice(0, 4).map(([bucket, candidate]) => `
        <tr>
          <td>${escapeHtml(bucket)}</td>
          <td>${candidate.eligible_for_paper_forward ? statusBadge('PAPER REVIEW', 'safe') : statusBadge('BLOCKED', 'warn')}</td>
          <td>${candidate.live_enabled ? statusBadge('LIVE', 'danger') : statusBadge('OFF', 'safe')}</td>
        </tr>`).join('');
      return `
        <table class="table">
          <thead><tr><th>Bucket</th><th>Paper</th><th>Live</th></tr></thead>
          <tbody>${rows || '<tr><td colspan="3">No gate candidates</td></tr>'}</tbody>
        </table>
        ${detailsBlock(payload)}`;
    }

    function renderThesisFailures(payload) {
      if (!payload.ok) {
        return emptyState('No thesis failures report', 'Run a campaign to inspect wrong-way or non-reversing picks.');
      }
      const rows = (payload.failures || []).slice(0, 5).map((row) => `
        <tr>
          <td>${escapeHtml(row.ticker || 'unknown')}</td>
          <td>${escapeHtml(row.trade_setup || 'unknown')}</td>
          <td>${escapeHtml(row.option_type || 'unknown')}</td>
          <td>${escapeHtml(row.reason || 'unknown')}</td>
          <td>${escapeHtml(row.contract_dte_days ?? 'n/a')}</td>
        </tr>`).join('');
      return `
        <table class="table">
          <thead><tr><th>Ticker</th><th>Setup</th><th>Type</th><th>Failure</th><th>DTE</th></tr></thead>
          <tbody>${rows || '<tr><td colspan="5">No thesis failures</td></tr>'}</tbody>
        </table>
        ${detailsBlock(payload)}`;
    }

    function renderPersistenceStatus() {
      const safety = dashboardState.safety;
      const corpus = dashboardState.corpus;
      const campaign = dashboardState.campaign;
      if (!safety) {
        document.getElementById('persistence-status').innerHTML = emptyState('Persistence unknown', 'Authenticate to inspect active runtime paths.');
        return;
      }
      const roots = [safety.active_gate_path, corpus?.manifest_path, campaign?.artifact_dir].filter(Boolean);
      const durable = roots.some((path) => String(path).startsWith('/var/data/autobott'));
      document.getElementById('persistence-root-text').textContent = durable ? '/var/data/autobott' : (roots[0] || 'not visible');
      document.getElementById('persistence-status').innerHTML = `
        ${metricList([
          ['Disk-backed path detected', durable ? statusBadge('YES', 'safe') : statusBadge('UNKNOWN', 'warn')],
          ['Gate path', `<span class="mono">${escapeHtml(safety.active_gate_path || 'unknown')}</span>`],
          ['Capture path', `<span class="mono">${escapeHtml(corpus?.manifest_path || 'not yet visible')}</span>`],
          ['Campaign path', `<span class="mono">${escapeHtml(campaign?.artifact_dir || 'not yet visible')}</span>`]
        ])}
        <div class="foot-note">Persistent disk proof is strongest after capture/campaign output survives a restart or redeploy.</div>`;
    }

    async function refreshHealth() {
      const result = await callApi('/api/health');
      renderHealth(result.payload);
      return result;
    }

    async function refreshProtected() {
      setAuthState(sessionStorage.getItem('dashboardToken') ? 'LOCKED' : 'LOCKED');
      const protectedResults = await Promise.all([
        callApi('/api/safety'),
        callApi('/api/alpaca/status'),
        callApi('/api/paper/readiness'),
        callApi('/api/corpus/latest'),
        callApi('/api/campaign/latest'),
        callApi('/api/session/status'),
        callApi('/api/reports/bucket-edge/latest'),
        callApi('/api/reports/thesis-failures/latest'),
        callApi('/api/reports/gate-candidate/latest'),
        callApi('/api/reports/decision-lab/latest'),
        callApi('/api/account/positions'),
        callApi('/api/account/orders'),
        callApi('/api/options/scout'),
        callApi('/api/decisions/feed'),
        callApi('/api/options/timeline')
      ]);
      renderProtectedPanel('safety-status', protectedResults[0], renderSafety);
      renderProtectedPanel('alpaca-status', protectedResults[1], renderAlpaca);
      renderProtectedPanel('paper-readiness', protectedResults[2], renderPaperReadiness);
      renderProtectedPanel('corpus-status', protectedResults[3], renderCorpus);
      renderProtectedPanel('campaign-status', protectedResults[4], renderCampaign);
      renderProtectedPanel('session-status', protectedResults[5], renderSession);
      renderProtectedPanel('bucket-report', protectedResults[6], renderBucketReport);
      renderProtectedPanel('thesis-failures', protectedResults[7], renderThesisFailures);
      renderProtectedPanel('gate-report', protectedResults[8], renderGateReport);
      renderProtectedPanel('decision-lab', protectedResults[9], renderDecisionLab);
      renderProtectedPanel('account-summary', protectedResults[10], renderAccountSummary);
      renderProtectedPanel('account-positions', protectedResults[10], renderAccountPositions);
      renderProtectedPanel('account-orders', protectedResults[11], renderAccountOrders);
      renderProtectedPanel('options-scout', protectedResults[12], renderOptionsScout);
      renderProtectedPanel('decision-feed', protectedResults[13], renderDecisionFeed);
      renderProtectedPanel('options-timeline', protectedResults[14], renderOptionsTimeline);
      renderPersistenceStatus();
      syncActionState();
    }

    async function refreshAll() {
      await refreshHealth();
      await refreshProtected();
      updateRefreshStamp();
    }

    async function startCapture(minutes) {
      const result = await callApi('/api/capture/start', { method:'POST', body: JSON.stringify({ symbols:['SPY','QQQ'], minutes, interval_seconds:60 }) });
      if (result.ok) {
        logEntry('Capture completed', `${minutes}-minute paper capture finished without enabling trading or mutating the active gate.`);
      } else if (result.status === 401) {
        logEntry('Capture blocked', 'Dashboard token required before protected actions can run.', 'warn');
      } else {
        logEntry('Capture failed', result.payload.detail || result.payload.error || 'Unknown capture failure.', 'danger');
      }
      await refreshAll();
    }

    async function runCampaign() {
      const result = await callApi('/api/campaign/run', { method:'POST', body: JSON.stringify({}) });
      if (result.ok) {
        logEntry('Campaign completed', 'Advisory replay campaign finished. Review report panels for candidate and bucket summaries.');
      } else if (result.status === 401) {
        logEntry('Campaign blocked', 'Dashboard token required before protected actions can run.', 'warn');
      } else {
        logEntry('Campaign failed', result.payload.detail || result.payload.error || 'Unknown campaign failure.', 'danger');
      }
      await refreshAll();
    }

    async function runDecisionLabBackfill() {
      const result = await callApi('/api/reports/decision-lab/backfill-run', {
        method:'POST',
        body: JSON.stringify({
          symbols:['SPY','QQQ'],
          days:2,
          interval_minutes:30,
          campaign_run_id:`decision-lab-${Date.now()}`
        })
      });
      if (result.ok) {
        const lab = result.payload.decision_lab || {};
        logEntry('Decision Lab completed', `Closed ${lab.summary?.closed_trades ?? 0} replay trades. Actual vs no-trade: ${lab.baselines?.actual_vs_no_trade ?? 'n/a'}.`);
      } else if (result.status === 401) {
        logEntry('Decision Lab blocked', 'Dashboard token required before protected actions can run.', 'warn');
      } else {
        logEntry('Decision Lab failed', result.payload.detail || result.payload.error || 'Unknown decision lab failure.', 'danger');
      }
      await refreshAll();
    }

    async function runTradingCycle() {
      const result = await callApi('/api/trading-cycle/run', { method:'POST', body: JSON.stringify({ symbols:['SPY'], quantity:1 }) });
      if (result.ok) {
        const candidates = result.payload.scanner_candidates_count ?? 0;
        const attempts = result.payload.trade_attempted_count ?? result.payload.orders_submitted?.length ?? 0;
        const rejected = Object.values(result.payload.execution_rejected_count_by_reason || {}).reduce((sum, value) => sum + Number(value || 0), 0);
        logEntry('Trading cycle completed', `Candidates: ${candidates}. Trade attempts: ${attempts}. Execution rejections: ${rejected}.`);
      } else if (result.status === 401) {
        logEntry('Trading cycle blocked', 'Dashboard token required before protected actions can run.', 'warn');
      } else {
        logEntry('Trading cycle failed', result.payload.detail || result.payload.error || 'Unknown trading cycle failure.', 'danger');
      }
      await refreshAll();
    }

    async function armPaperMode() {
      const result = await callApi('/api/runtime/arm-paper', { method:'POST', body: JSON.stringify({ reason:'dashboard_arm_paper' }) });
      if (result.ok) {
        logEntry('Paper execution armed', 'Paper execution is enabled and live mode stays locked.', 'safe');
      } else if (result.status === 401) {
        logEntry('Arm blocked', 'Dashboard token required before protected actions can run.', 'warn');
      } else {
        logEntry('Arm failed', result.payload.detail || result.payload.error || 'Unknown runtime control failure.', 'danger');
      }
      await refreshAll();
    }

    async function disableExecution() {
      const result = await callApi('/api/runtime/disable-execution', { method:'POST', body: JSON.stringify({ reason:'dashboard_disable_execution' }) });
      if (result.ok) {
        logEntry('Execution disabled', 'New paper entries are disabled until paper mode is armed again.', 'warn');
      } else if (result.status === 401) {
        logEntry('Disable blocked', 'Dashboard token required before protected actions can run.', 'warn');
      } else {
        logEntry('Disable failed', result.payload.detail || result.payload.error || 'Unknown runtime control failure.', 'danger');
      }
      await refreshAll();
    }

    async function engageKillSwitch() {
      const result = await callApi('/api/runtime/kill-switch', { method:'POST', body: JSON.stringify({ enabled:true, reason:'dashboard_kill_switch' }) });
      if (result.ok) {
        logEntry('Kill switch engaged', 'Execution and live mode were forced off immediately.', 'danger');
      } else if (result.status === 401) {
        logEntry('Kill switch blocked', 'Dashboard token required before protected actions can run.', 'warn');
      } else {
        logEntry('Kill switch failed', result.payload.detail || result.payload.error || 'Unknown runtime control failure.', 'danger');
      }
      await refreshAll();
    }

    async function reconcileExecution() {
      const result = await callApi('/api/execution/reconcile', { method:'POST', body: JSON.stringify({}) });
      if (result.ok) {
        logEntry('Reconcile completed', `Checked ${result.payload.checked} orders and updated ${result.payload.updated}.`, 'safe');
      } else if (result.status === 401) {
        logEntry('Reconcile blocked', 'Dashboard token required before protected actions can run.', 'warn');
      } else {
        logEntry('Reconcile failed', result.payload.detail || result.payload.error || 'Unknown reconcile failure.', 'danger');
      }
      await refreshAll();
    }

    async function startPaperSession() {
      const result = await callApi('/api/session/start', { method:'POST', body: JSON.stringify({ symbols:['SPY'], interval_seconds:300, quantity:1 }) });
      if (result.ok && result.payload.started) {
        logEntry('Session started', 'Protected paper session launched successfully.', 'safe');
      } else if (result.status === 401) {
        logEntry('Session blocked', 'Dashboard token required before protected actions can run.', 'warn');
      } else if (result.ok) {
        logEntry('Session already running', 'Supervisor ignored the request because a session is already active.', 'warn');
      } else {
        logEntry('Session start failed', result.payload.detail || result.payload.error || 'Unknown session start failure.', 'danger');
      }
      await refreshAll();
    }

    syncActionState();
    refreshAll();
  </script>
</body>
</html>
"""


def _require_auth(headers: dict[str, str]) -> None:
    expected = os.getenv("AUTOBOTT_DASHBOARD_AUTH_TOKEN", "")
    if not expected:
        raise PermissionError("dashboard_auth_token_not_configured")
    provided = headers.get("authorization", "")
    if provided != f"Bearer {expected}":
        raise PermissionError("dashboard_auth_required")


def _extract_headers(environ: dict[str, Any]) -> dict[str, str]:
    headers: dict[str, str] = {}
    for key, value in environ.items():
        if key.startswith("HTTP_"):
            header = key[5:].replace("_", "-").lower()
            headers[header] = str(value)
    if "CONTENT_TYPE" in environ:
        headers["content-type"] = str(environ["CONTENT_TYPE"])
    return headers


def _read_body(environ: dict[str, Any]) -> bytes:
    try:
        length = int(environ.get("CONTENT_LENGTH") or "0")
    except ValueError:
        length = 0
    stream = environ.get("wsgi.input")
    return stream.read(length) if stream is not None else b""


def _json_body(body: bytes) -> JsonDict:
    return json.loads(body.decode("utf-8")) if body else {}


def _parse_date(value: Any) -> date | None:
    if not value:
        return None
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


def _reason(status_code: int) -> str:
    return {200: "OK", 401: "Unauthorized", 404: "Not Found", 500: "Internal Server Error"}.get(status_code, "OK")


def _corpus_root() -> Path:
    return phase1_snapshots_root()


def _artifacts_root() -> Path:
    return phase1_replay_campaign_root()


def _gate_path() -> Path:
    return default_gate_path()


def _latest_manifest(root: Path) -> Path | None:
    manifests = sorted(root.rglob("manifest.json"), key=lambda path: path.stat().st_mtime)
    return manifests[-1] if manifests else None


def _latest_campaign_dir() -> Path | None:
    manifests = sorted(_artifacts_root().rglob("manifest.json"), key=lambda path: path.stat().st_mtime)
    return manifests[-1].parent if manifests else None


def _read_json(path: Path) -> JsonDict:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def _read_jsonl(path: Path) -> list[JsonDict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _file_hash(path: Path) -> str | None:
    if not path.exists():
        return None
    return sha256(path.read_bytes()).hexdigest()


def _order_methods_present() -> bool:
    forbidden = ("submit_order", "replace_order", "cancel_order", "buy", "sell", "close", "liquidate")
    return any(hasattr(AlpacaPaperClient, method_name) for method_name in forbidden)


def _thesis_failure_sort_key(row: JsonDict) -> tuple[float, float, float, str]:
    dte_penalty = 0 if (row.get("contract_dte_days") or 99) <= 2 else 1
    followthrough = float(row.get("followthrough_rate") or 0.0)
    adverse = float(row.get("adverse_move_pct") or 0.0)
    return (dte_penalty, followthrough, adverse, str(row.get("decision_id") or ""))


def main() -> int:
    bootstrap_env_file()
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))
    maybe_start_session_supervisor()
    with make_server(host, port, app) as httpd:
        print(f"AutoBott dashboard serving on http://{host}:{port}")
        httpd.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
