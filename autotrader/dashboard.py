"""Live read-only dashboard for the Alpaca options autotrader."""

from __future__ import annotations

import csv
import hmac
import os
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pytz
import requests
from flask import Flask, jsonify, render_template_string, request

import config
from env_config import get_required_env, load_runtime_env
from feature_flags import get_feature_flags_snapshot
from feature_flags import is_enabled as feature_enabled
from state_store import load_bot_state
from strategy_profiles import PROFILE_PRESETS, normalize_profile_name
from trading_control import (
    load_trading_control,
    set_dry_run,
    set_manual_stop,
    set_strategy_profile,
)
from watchlist_control import load_watchlist_control, update_watchlist_control

load_runtime_env()

API_KEY = get_required_env("ALPACA_API_KEY")
SECRET_KEY = get_required_env("ALPACA_SECRET_KEY")
PAPER = bool(config.PAPER)
BASE_URL = "https://paper-api.alpaca.markets" if PAPER else "https://api.alpaca.markets"
DATA_BASE_URL = config.ALPACA_DATA_BASE_URL
HEADERS = {"APCA-API-KEY-ID": API_KEY or "", "APCA-API-SECRET-KEY": SECRET_KEY or ""}

TRADES_CSV = Path(config.TRADES_CSV_PATH)
SCAN_LOG_CSV = Path(config.SCAN_LOG_CSV_PATH)
EASTERN = pytz.timezone(config.EASTERN_TZ)
CENTRAL = pytz.timezone(config.CENTRAL_TZ)
_REVIEW_CACHE: dict[str, Any] = {"ts": None, "payload": None}
CONTROL_TOKEN = str(config.DASHBOARD_CONTROL_TOKEN or "").strip()

app = Flask(__name__)

@app.get("/healthz")
def healthz():
    return jsonify({"ok": True, "service": "autobott"})


def _now_et() -> datetime:
    return datetime.now(EASTERN)


def _to_ct_label(dt: datetime | None) -> str:
    if dt is None:
        return ""
    return dt.astimezone(CENTRAL).strftime("%Y-%m-%d %H:%M:%S %Z")


def _extract_underlying(symbol: str) -> str:
    match = re.match(r"^([A-Z.]+)\d{6}[CP]\d{8}$", symbol or "")
    return match.group(1) if match else ""


def _extract_direction(symbol: str) -> str:
    match = re.match(r"^[A-Z.]+\d{6}([CP])\d{8}$", symbol or "")
    if not match:
        return ""
    return "CALL" if match.group(1) == "C" else "PUT"


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _verify_control_token() -> tuple[bool, str, int]:
    if not CONTROL_TOKEN:
        return False, "dashboard control token not configured", 503
    provided = str(request.headers.get("X-Trade-Control-Token", "") or "").strip()
    if not provided or not hmac.compare_digest(provided, CONTROL_TOKEN):
        return False, "unauthorized", 401
    return True, "", 200


def _read_csv_rows(path: Path, limit: int, reverse: bool = True) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    if reverse:
        rows = list(reversed(rows))
    return rows[:limit]


def _parse_ts(value: str) -> datetime | None:
    if not value:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S %Z", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(value, fmt)
            if dt.tzinfo is None:
                dt = EASTERN.localize(dt)
            return dt.astimezone(EASTERN)
        except ValueError:
            continue
    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = EASTERN.localize(dt)
        return dt.astimezone(EASTERN)
    except ValueError:
        return None


def _today_trade_rows() -> list[dict[str, str]]:
    today = _now_et().date()
    rows = _read_csv_rows(TRADES_CSV, limit=5000, reverse=False)
    out: list[dict[str, str]] = []
    for row in rows:
        dt = _parse_ts(row.get("timestamp", ""))
        if dt and dt.date() == today:
            out.append(row)
    return out


def _daily_loss_and_streak(today_rows: list[dict[str, str]]) -> tuple[float, int]:
    daily_loss = 0.0
    for row in today_rows:
        pnl_pct = _safe_float(row.get("pnl_pct"), 0.0)
        if pnl_pct < 0:
            entry = _safe_float(row.get("entry_price"), 0.0)
            qty = int(_safe_float(row.get("qty"), 0.0))
            premium = entry * qty * 100
            daily_loss += abs(premium * pnl_pct)

    streak = 0
    for row in reversed(today_rows):
        pnl_pct = _safe_float(row.get("pnl_pct"), 0.0)
        if pnl_pct < 0:
            streak += 1
        else:
            break
    return daily_loss, streak


def _progress_color(pct: float) -> str:
    if pct < 50:
        return "#00c853"
    if pct <= 80:
        return "#ffb300"
    return "#ff1744"


def _today_scan_rows() -> list[dict[str, str]]:
    today = _now_et().date()
    rows = _read_csv_rows(SCAN_LOG_CSV, limit=10000, reverse=False)
    out: list[dict[str, str]] = []
    for row in rows:
        dt = _parse_ts(row.get("timestamp", ""))
        if dt and dt.date() == today:
            out.append(row)
    return out


def _clock_hhmm_to_minutes(hhmm: str) -> int:
    parts = hhmm.split(":")
    if len(parts) != 2:
        return 0
    try:
        hour = int(parts[0])
        minute = int(parts[1])
    except ValueError:
        return 0
    return (hour * 60) + minute


def _fetch_snapshots(symbols: list[str]) -> dict[str, dict[str, Any]]:
    if not symbols:
        return {}
    try:
        resp = requests.get(
            f"{DATA_BASE_URL}/v2/stocks/snapshots",
            headers=HEADERS,
            params={"symbols": ",".join(symbols)},
            timeout=12,
        )
        resp.raise_for_status()
        body = resp.json()
        return body if isinstance(body, dict) else {}
    except Exception:
        return {}


def _fetch_intraday_stock_series(symbols: list[str], limit: int = 78) -> dict[str, list[dict[str, Any]]]:
    clean_symbols = [str(s).upper() for s in symbols if str(s).strip()]
    if not clean_symbols:
        return {}
    now_et = _now_et()
    start_et = now_et - timedelta(hours=8)
    try:
        resp = requests.get(
            f"{DATA_BASE_URL}/v2/stocks/bars",
            headers=HEADERS,
            params={
                "symbols": ",".join(clean_symbols),
                "timeframe": "5Min",
                "start": start_et.astimezone(pytz.UTC).isoformat().replace("+00:00", "Z"),
                "end": now_et.astimezone(pytz.UTC).isoformat().replace("+00:00", "Z"),
                "limit": max(50, int(limit)),
                "adjustment": "raw",
                "feed": "iex",
            },
            timeout=12,
        )
        resp.raise_for_status()
        body = resp.json()
        bars_map = body.get("bars", {}) if isinstance(body, dict) else {}
        out: dict[str, list[dict[str, Any]]] = {}
        if not isinstance(bars_map, dict):
            return out
        for symbol in clean_symbols:
            rows = bars_map.get(symbol, [])
            if not isinstance(rows, list):
                out[symbol] = []
                continue
            points: list[dict[str, Any]] = []
            for item in rows[-limit:]:
                ts_raw = item.get("t")
                close = _safe_float(item.get("c"), 0.0)
                if not ts_raw or close <= 0:
                    continue
                points.append({"ts": str(ts_raw), "close": round(close, 4)})
            out[symbol] = points
        return out
    except Exception:
        return {}


def _build_skipped_review(scan_rows: list[dict[str, str]]) -> dict[str, Any]:
    failed = [r for r in scan_rows if str(r.get("result", "")).lower() == "fail"]
    if not failed:
        return {"items": [], "reason_summary": []}

    per_symbol: dict[str, dict[str, Any]] = {}
    reason_counts: dict[str, int] = {}
    for row in failed:
        symbol = str(row.get("symbol", "")).upper()
        if not symbol:
            continue
        reason = str(row.get("reason", "")).strip() or "unknown"
        dt = _parse_ts(row.get("timestamp", ""))
        reason_counts[reason] = reason_counts.get(reason, 0) + 1
        if symbol not in per_symbol:
            per_symbol[symbol] = {
                "symbol": symbol,
                "fail_count": 0,
                "first_seen": row.get("timestamp", ""),
                "last_seen": row.get("timestamp", ""),
                "last_reason": reason,
                "_first_dt": dt,
                "_last_dt": dt,
            }
        item = per_symbol[symbol]
        item["fail_count"] = int(item["fail_count"]) + 1
        item["last_reason"] = reason
        if dt is not None:
            first_dt = item.get("_first_dt")
            last_dt = item.get("_last_dt")
            if first_dt is None or dt < first_dt:
                item["_first_dt"] = dt
                item["first_seen"] = row.get("timestamp", "")
            if last_dt is None or dt > last_dt:
                item["_last_dt"] = dt
                item["last_seen"] = row.get("timestamp", "")

    ranked = sorted(
        per_symbol.values(),
        key=lambda x: (int(x["fail_count"]), str(x["last_seen"])),
        reverse=True,
    )[:12]
    symbols = [str(i["symbol"]) for i in ranked]
    snapshots = _fetch_snapshots(symbols)

    items: list[dict[str, Any]] = []
    for item in ranked:
        symbol = str(item["symbol"])
        snap = snapshots.get(symbol, {}) if isinstance(snapshots, dict) else {}
        daily = snap.get("dailyBar", {}) if isinstance(snap, dict) else {}
        latest_trade = snap.get("latestTrade", {}) if isinstance(snap, dict) else {}
        minute_bar = snap.get("minuteBar", {}) if isinstance(snap, dict) else {}

        day_open = _safe_float(daily.get("o"), 0.0)
        latest = _safe_float(latest_trade.get("p"), 0.0)
        if latest <= 0:
            latest = _safe_float(minute_bar.get("c"), 0.0)
        if latest <= 0:
            latest = _safe_float(daily.get("c"), 0.0)
        day_high = _safe_float(daily.get("h"), 0.0)
        day_low = _safe_float(daily.get("l"), 0.0)

        move_pct = ((latest - day_open) / day_open * 100.0) if day_open > 0 and latest > 0 else None
        range_pct = ((day_high - day_low) / day_open * 100.0) if day_open > 0 and day_high > 0 else None

        items.append(
            {
                "symbol": symbol,
                "fail_count": int(item["fail_count"]),
                "first_seen": item["first_seen"],
                "last_seen": item["last_seen"],
                "last_reason": item["last_reason"],
                "day_open": round(day_open, 4) if day_open > 0 else None,
                "latest_price": round(latest, 4) if latest > 0 else None,
                "day_move_pct": round(move_pct, 2) if move_pct is not None else None,
                "day_range_pct": round(range_pct, 2) if range_pct is not None else None,
            }
        )

    reason_summary = sorted(
        [{"reason": reason, "count": count} for reason, count in reason_counts.items()],
        key=lambda x: int(x["count"]),
        reverse=True,
    )[:8]
    return {"items": items, "reason_summary": reason_summary}


def _build_logic_checks(scan_rows: list[dict[str, str]], trade_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    checks: list[dict[str, str]] = []
    now_et = _now_et()
    now_minutes = (now_et.hour * 60) + now_et.minute
    scan_start = _clock_hhmm_to_minutes(config.SCAN_MORNING_TIME)
    hard_close = _clock_hhmm_to_minutes(config.HARD_CLOSE_TIME)

    if scan_rows:
        last_scan_dt = _parse_ts(scan_rows[-1].get("timestamp", ""))
        if last_scan_dt is None:
            checks.append({"status": "warn", "name": "Scan Timestamps", "detail": "Could not parse last scan timestamp."})
        else:
            age_min = int((now_et - last_scan_dt).total_seconds() // 60)
            market_window = scan_start <= now_minutes <= hard_close
            if market_window and age_min > 45:
                checks.append(
                    {
                        "status": "warn",
                        "name": "Scan Freshness",
                        "detail": f"No scan rows for {age_min} minutes during market hours.",
                    }
                )
            else:
                checks.append({"status": "ok", "name": "Scan Freshness", "detail": f"Last scan {max(age_min, 0)} minutes ago."})
    else:
        checks.append({"status": "warn", "name": "Scan Activity", "detail": "No scan rows for today yet."})

    pass_count = sum(1 for r in scan_rows if str(r.get("result", "")).lower() == "pass")
    fail_count = sum(1 for r in scan_rows if str(r.get("result", "")).lower() == "fail")
    total_scans = pass_count + fail_count
    if total_scans > 0:
        pass_rate = (pass_count / total_scans) * 100.0
        checks.append(
            {
                "status": "ok" if pass_rate >= 5 else "warn",
                "name": "Pass Rate",
                "detail": f"{pass_count}/{total_scans} passed ({pass_rate:.1f}%).",
            }
        )

    scan_errors = 0
    for r in scan_rows:
        reason = str(r.get("reason", "")).lower()
        if "scan error" in reason or "unavailable" in reason:
            scan_errors += 1
    if scan_errors > 0:
        checks.append({"status": "warn", "name": "Data Quality", "detail": f"{scan_errors} rows had scanner/data errors."})
    else:
        checks.append({"status": "ok", "name": "Data Quality", "detail": "No scanner/data errors detected."})

    today_loss, streak = _daily_loss_and_streak(trade_rows)
    if today_loss >= float(config.DAILY_LOSS_LIMIT_USD):
        checks.append(
            {
                "status": "warn",
                "name": "Daily Loss Guard",
                "detail": f"Loss ${today_loss:.2f} reached/exceeded limit ${float(config.DAILY_LOSS_LIMIT_USD):.2f}.",
            }
        )
    else:
        checks.append(
            {
                "status": "ok",
                "name": "Daily Loss Guard",
                "detail": f"Loss ${today_loss:.2f} below limit ${float(config.DAILY_LOSS_LIMIT_USD):.2f}.",
            }
        )

    if streak >= int(config.CONSECUTIVE_LOSS_LIMIT):
        checks.append(
            {
                "status": "warn",
                "name": "Consecutive Loss Guard",
                "detail": f"Loss streak {streak} reached limit {int(config.CONSECUTIVE_LOSS_LIMIT)}.",
            }
        )
    else:
        checks.append(
            {
                "status": "ok",
                "name": "Consecutive Loss Guard",
                "detail": f"Current loss streak {streak}/{int(config.CONSECUTIVE_LOSS_LIMIT)}.",
            }
        )
    return checks


def _build_daily_review_payload() -> dict[str, Any]:
    scan_rows = _today_scan_rows()
    trade_rows = _today_trade_rows()
    return {
        "date": _now_et().strftime("%Y-%m-%d"),
        "generated_at": _now_et().strftime("%Y-%m-%d %H:%M:%S ET"),
        "skipped": _build_skipped_review(scan_rows),
        "checks": _build_logic_checks(scan_rows, trade_rows),
    }


@app.get("/api/account")
def api_account():
    try:
        resp = requests.get(f"{BASE_URL}/v2/account", headers=HEADERS, timeout=10)
        resp.raise_for_status()
        body = resp.json()
        return jsonify(
            {
                "equity": body.get("equity", "0"),
                "buying_power": body.get("buying_power", "0"),
                "cash": body.get("cash", "0"),
                "portfolio_value": body.get("portfolio_value", body.get("equity", "0")),
                "status": body.get("status", "UNKNOWN"),
                "trading_blocked": body.get("trading_blocked", False),
                "account_blocked": body.get("account_blocked", False),
                "transfers_blocked": body.get("transfers_blocked", False),
                "trade_suspended_by_user": body.get("trade_suspended_by_user", False),
                "options_trading_level": body.get("options_trading_level", ""),
                "options_approved_level": body.get("options_approved_level", ""),
                "daytrade_count": body.get("daytrade_count", 0),
                "broker_ok": True,
                "broker_error": "",
            }
        )
    except Exception as exc:  # noqa: BLE001
        return jsonify(
            {
                "equity": "0",
                "buying_power": "0",
                "cash": "0",
                "portfolio_value": "0",
                "status": "UNAVAILABLE",
                "trading_blocked": True,
                "account_blocked": False,
                "transfers_blocked": False,
                "trade_suspended_by_user": False,
                "options_trading_level": "",
                "options_approved_level": "",
                "daytrade_count": 0,
                "broker_ok": False,
                "broker_error": str(exc),
            }
        )


@app.get("/api/positions")
def api_positions():
    try:
        resp = requests.get(f"{BASE_URL}/v2/positions", headers=HEADERS, timeout=10)
        resp.raise_for_status()
        rows = []
        for pos in resp.json():
            asset_class = str(pos.get("asset_class", "")).lower()
            if asset_class not in ("us_option", "option"):
                continue
            symbol = pos.get("symbol", "")
            rows.append(
                {
                    "symbol": symbol,
                    "underlying": _extract_underlying(symbol),
                    "direction": _extract_direction(symbol),
                    "qty": int(_safe_float(pos.get("qty"), 0)),
                    "entry_price": _safe_float(pos.get("avg_entry_price"), 0.0),
                    "current_price": _safe_float(pos.get("current_price"), 0.0),
                    "market_value": _safe_float(pos.get("market_value"), 0.0),
                    "unrealized_pl": _safe_float(pos.get("unrealized_pl"), 0.0),
                    "unrealized_plpc": round(_safe_float(pos.get("unrealized_plpc"), 0.0) * 100, 2),
                }
            )
        return jsonify(
          {
            "rows": rows,
            "broker_ok": True,
            "broker_error": "",
          }
        )
    except Exception as exc:  # noqa: BLE001
        return jsonify(
            {
                "rows": [],
                "broker_ok": False,
                "broker_error": str(exc),
            }
        )


@app.get("/api/trades")
def api_trades():
    try:
        rows = _read_csv_rows(TRADES_CSV, limit=50, reverse=True)
        out: list[dict[str, Any]] = []
        for row in rows:
            ts_raw = str(row.get("timestamp", "") or "")
            ts_dt = _parse_ts(ts_raw)
            patched = dict(row)
            patched["timestamp"] = _to_ct_label(ts_dt) or ts_raw
            out.append(patched)
        return jsonify(out)
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@app.get("/api/scanlog")
def api_scanlog():
    try:
        today = _now_et().date()
        rows = _read_csv_rows(SCAN_LOG_CSV, limit=5000, reverse=True)
        passed: list[dict[str, Any]] = []
        for row in rows:
            if str(row.get("result", "")).lower() != "pass":
                continue
            dt = _parse_ts(str(row.get("timestamp", "")))
            if dt is not None and dt.date() != today:
                continue
            passed.append(row)
            if len(passed) >= 30:
                break
        return jsonify(passed)
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@app.route("/api/scanfails")
def api_scanfails():
    """Return the last 20 scan failures from scan_log.csv."""
    try:
        rows = _read_csv_rows(SCAN_LOG_CSV, limit=200, reverse=True)
        fails = [r for r in rows if str(r.get("result", "")).lower() == "fail"]
        out: list[dict[str, Any]] = []
        for row in fails[:20]:
            ts_raw = str(row.get("timestamp", "") or "")
            ts_dt = _parse_ts(ts_raw)
            patched = dict(row)
            patched["timestamp"] = _to_ct_label(ts_dt) or ts_raw
            out.append(patched)
        return jsonify(out)
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@app.route("/api/scansummary")
def api_scansummary():
    """Return counts and most common failure reason from the last scan loop."""
    try:
        rows = _read_csv_rows(SCAN_LOG_CSV, limit=200, reverse=True)
        if not rows:
            return jsonify({"pass_count": 0, "fail_count": 0, "top_reason": "No scan data yet", "last_scan": ""})

        last_ts = rows[0].get("timestamp", "") if rows else ""
        same_loop = [r for r in rows if r.get("timestamp") == last_ts]
        pass_count = sum(1 for r in same_loop if r.get("result") == "pass")
        fail_count = sum(1 for r in same_loop if r.get("result") == "fail")
        reasons = [r.get("reason", "") for r in same_loop if r.get("result") == "fail"]
        top_reason = max(set(reasons), key=reasons.count) if reasons else ""

        return jsonify(
            {
                "pass_count": pass_count,
                "fail_count": fail_count,
                "top_reason": top_reason,
                "last_scan": _to_ct_label(_parse_ts(str(last_ts))) or str(last_ts),
            }
        )
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@app.get("/api/status")
def api_status():
    now_et = _now_et()
    try:
        clock = requests.get(f"{BASE_URL}/v2/clock", headers=HEADERS, timeout=10)
        clock.raise_for_status()
        clock_body = clock.json()

        today_rows = _today_trade_rows()
        now_minutes = (now_et.hour * 60) + now_et.minute
        entry_open = _clock_hhmm_to_minutes(config.NO_NEW_TRADES_BEFORE)
        entry_close = _clock_hhmm_to_minutes(config.NO_NEW_TRADES_AFTER)
        entry_window_open = bool(clock_body.get("is_open", False)) and (entry_open <= now_minutes < entry_close)
        runtime_state = load_bot_state()
        catalyst_mode_active = bool(runtime_state.get("catalyst_mode_active", False))
        catalyst_mode_reason = str(runtime_state.get("catalyst_mode_reason", "") or "")
        catalyst_mode_until = str(runtime_state.get("catalyst_mode_until_iso", "") or "")
        last_entry_debug = runtime_state.get("last_entry_debug", {})
        last_exit_debug = runtime_state.get("last_exit_debug", {})
        heartbeat_et_raw = str(runtime_state.get("last_trader_heartbeat_et", "") or "")
        heartbeat_dt = _parse_ts(heartbeat_et_raw)
        heartbeat_age_seconds = int((now_et - heartbeat_dt).total_seconds()) if heartbeat_dt else None
        market_is_open = bool(clock_body.get("is_open", False))
        loop_stale_after = max(60, int(config.LOOP_INTERVAL_SECONDS) * 4) if market_is_open else 1800
        trader_loop_alive = heartbeat_age_seconds is not None and heartbeat_age_seconds <= loop_stale_after
        last_auth_error_et = str(runtime_state.get("last_alpaca_auth_error_et", "") or "")
        last_auth_error_msg = str(runtime_state.get("last_alpaca_auth_error", "") or "")
        last_auth_error_dt = _parse_ts(last_auth_error_et)
        auth_error_recent = False
        if last_auth_error_dt is not None:
            auth_error_recent = (now_et - last_auth_error_dt).total_seconds() <= 600

        wins = 0
        losses = 0
        total_plpc = 0.0
        for row in today_rows:
            plpc = _safe_float(row.get("pnl_pct"), 0.0)
            total_plpc += plpc
            if plpc > 0:
                wins += 1
            elif plpc < 0:
                losses += 1

        control = load_trading_control()
        trading_paused = bool(control.get("manual_stop", False))
        dry_run = bool(control.get("dry_run", False))
        strategy_profile = normalize_profile_name(str(control.get("strategy_profile", "balanced") or "balanced"))
        blockers: list[str] = []
        if trading_paused:
            blockers.append("manual_stop")
        if not bool(clock_body.get("is_open", False)):
            blockers.append("market_closed")
        if not entry_window_open:
            blockers.append("outside_entry_window")
        blocked_day_notice = str(runtime_state.get("blocked_day_notice", "") or "")
        if blocked_day_notice:
            blockers.append(f"event_day_block:{blocked_day_notice}")
        vix_block_notice = str(runtime_state.get("vix_block_notice", "") or "")
        if vix_block_notice:
            blockers.append(f"vix_guard_block:{vix_block_notice}")
        if auth_error_recent:
            blockers.append("alpaca_auth_error_recent")
        if not trader_loop_alive:
            blockers.append("trader_loop_stale")

        return jsonify(
            {
                "market_open": bool(clock_body.get("is_open", False)),
                "trading_paused": trading_paused,
                "dry_run": dry_run,
                "strategy_profile": strategy_profile,
                "entry_window_open": entry_window_open,
                "entry_window_label": f"{config.NO_NEW_TRADES_BEFORE}-{config.NO_NEW_TRADES_AFTER} ET",
                "catalyst_mode_active": catalyst_mode_active,
                "catalyst_mode_reason": catalyst_mode_reason,
                "catalyst_mode_until": catalyst_mode_until,
                "can_enter_now": len(blockers) == 0,
                "blockers": blockers,
                "trader_loop_alive": bool(trader_loop_alive),
                "trader_loop_stale_after_seconds": loop_stale_after,
                "trader_heartbeat_et": _to_ct_label(heartbeat_dt) if heartbeat_dt else "",
                "trader_heartbeat_age_seconds": heartbeat_age_seconds,
                "last_alpaca_auth_error_et": _to_ct_label(last_auth_error_dt) if last_auth_error_dt else "",
                "last_alpaca_auth_error": last_auth_error_msg,
                "alpaca_auth_error_recent": auth_error_recent,
                "last_entry_debug": last_entry_debug if isinstance(last_entry_debug, dict) else {},
                "last_exit_debug": last_exit_debug if isinstance(last_exit_debug, dict) else {},
                "feature_flags": get_feature_flags_snapshot(),
                "last_updated": _now_et().strftime("%Y-%m-%d %H:%M:%S ET"),
                "trades_today": len(today_rows),
                "wins_today": wins,
                "losses_today": losses,
                "daily_pnl_pct": round(total_plpc, 4),
            }
        )
    except Exception as exc:  # noqa: BLE001
        return jsonify(
            {
                "error": str(exc),
                "market_open": False,
                "trading_paused": bool(load_trading_control().get("manual_stop", False)),
                "dry_run": bool(load_trading_control().get("dry_run", False)),
                "strategy_profile": normalize_profile_name(
                    str(load_trading_control().get("strategy_profile", "balanced") or "balanced")
                ),
                "entry_window_open": False,
                "entry_window_label": f"{config.NO_NEW_TRADES_BEFORE}-{config.NO_NEW_TRADES_AFTER} ET",
                "catalyst_mode_active": False,
                "catalyst_mode_reason": "",
                "catalyst_mode_until": "",
                "can_enter_now": False,
                "blockers": ["status_unavailable"],
                "trader_loop_alive": False,
                "trader_loop_stale_after_seconds": max(60, int(config.LOOP_INTERVAL_SECONDS) * 4),
                "trader_heartbeat_et": "",
                "trader_heartbeat_age_seconds": None,
                "last_alpaca_auth_error_et": "",
                "last_alpaca_auth_error": "",
                "alpaca_auth_error_recent": False,
                "last_entry_debug": {},
                "last_exit_debug": {},
                "feature_flags": get_feature_flags_snapshot(),
                "last_updated": now_et.strftime("%Y-%m-%d %H:%M:%S ET"),
                "trades_today": 0,
                "wins_today": 0,
                "losses_today": 0,
                "daily_pnl_pct": 0.0,
            }
        )


@app.get("/api/trading-control")
def api_trading_control():
    try:
        state = load_trading_control()
        return jsonify(
            {
                "manual_stop": bool(state.get("manual_stop", False)),
                "dry_run": bool(state.get("dry_run", False)),
                "strategy_profile": normalize_profile_name(str(state.get("strategy_profile", "balanced") or "balanced")),
                "available_profiles": sorted(PROFILE_PRESETS.keys()),
                "updated_at_et": str(state.get("updated_at_et", "")),
                "reason": str(state.get("reason", "")),
            }
        )
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@app.post("/api/trading-control/stop")
def api_trading_stop():
    try:
        ok, err, status = _verify_control_token()
        if not ok:
            return jsonify({"error": err}), status
        payload = request.get_json(silent=True) or {}
        reason = str(payload.get("reason", "") or "manual_stop_dashboard")
        state = set_manual_stop(True, reason=reason)
        return jsonify(
            {
                "ok": True,
                "manual_stop": bool(state.get("manual_stop", False)),
                "updated_at_et": str(state.get("updated_at_et", "")),
                "reason": str(state.get("reason", "")),
            }
        )
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@app.post("/api/trading-control/start")
def api_trading_start():
    try:
        ok, err, status = _verify_control_token()
        if not ok:
            return jsonify({"error": err}), status
        payload = request.get_json(silent=True) or {}
        reason = str(payload.get("reason", "") or "manual_start_dashboard")
        state = set_manual_stop(False, reason=reason)
        return jsonify(
            {
                "ok": True,
                "manual_stop": bool(state.get("manual_stop", False)),
                "updated_at_et": str(state.get("updated_at_et", "")),
                "reason": str(state.get("reason", "")),
            }
        )
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@app.post("/api/runtime-control")
def api_runtime_control_update():
    try:
        ok, err, status = _verify_control_token()
        if not ok:
            return jsonify({"error": err}), status
        payload = request.get_json(silent=True) or {}
        dry_run = payload.get("dry_run")
        profile = payload.get("strategy_profile")
        state = load_trading_control()
        if dry_run is not None:
            state = set_dry_run(bool(dry_run), reason="dashboard_runtime_control")
        if profile is not None:
            normalized = normalize_profile_name(str(profile))
            state = set_strategy_profile(normalized, reason="dashboard_runtime_control")
        return jsonify(
            {
                "ok": True,
                "manual_stop": bool(state.get("manual_stop", False)),
                "dry_run": bool(state.get("dry_run", False)),
                "strategy_profile": normalize_profile_name(str(state.get("strategy_profile", "balanced") or "balanced")),
                "available_profiles": sorted(PROFILE_PRESETS.keys()),
                "updated_at_et": str(state.get("updated_at_et", "")),
                "reason": str(state.get("reason", "")),
            }
        )
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@app.get("/api/watchlist-control")
def api_watchlist_control():
    try:
        state = load_watchlist_control()
        return jsonify(
            {
                "mode": str(state.get("mode", "off")),
                "tickers": list(state.get("tickers") or []),
                "updated_at_et": str(state.get("updated_at_et", "")),
                "reason": str(state.get("reason", "")),
            }
        )
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@app.post("/api/watchlist-control")
def api_watchlist_control_update():
    try:
        ok, err, status = _verify_control_token()
        if not ok:
            return jsonify({"error": err}), status
        payload = request.get_json(silent=True) or {}
        mode_raw = payload.get("mode")
        mode = str(mode_raw or "").strip().lower() if mode_raw is not None else None
        tickers_raw = payload.get("tickers")
        tickers: list[str] | None = None
        if isinstance(tickers_raw, list):
            tickers = [str(t).upper() for t in tickers_raw]
        elif isinstance(tickers_raw, str):
            tickers = [chunk.strip().upper() for chunk in re.split(r"[\s,]+", tickers_raw) if chunk.strip()]
        state = update_watchlist_control(mode=mode, tickers=tickers, reason="dashboard_watchlist_update")
        return jsonify(
            {
                "ok": True,
                "mode": str(state.get("mode", "off")),
                "tickers": list(state.get("tickers") or []),
                "updated_at_et": str(state.get("updated_at_et", "")),
                "reason": str(state.get("reason", "")),
            }
        )
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@app.get("/api/watch/open")
def api_watch_open():
    try:
        resp = requests.get(f"{BASE_URL}/v2/positions", headers=HEADERS, timeout=10)
        resp.raise_for_status()
        state = load_bot_state()
        open_meta = dict(state.get("open_trade_meta") or {})
        pl_history = dict(state.get("open_position_pl_history") or {})
        option_rows = []
        underlyings: list[str] = []
        for pos in resp.json():
            asset_class = str(pos.get("asset_class", "")).lower()
            if asset_class not in ("us_option", "option"):
                continue
            symbol = str(pos.get("symbol", "") or "")
            if not symbol:
                continue
            underlying = _extract_underlying(symbol)
            if underlying:
                underlyings.append(underlying)
            meta = open_meta.get(symbol, {})
            option_rows.append(
                {
                    "symbol": symbol,
                    "underlying": underlying,
                    "direction": _extract_direction(symbol),
                    "qty": int(_safe_float(pos.get("qty"), 0)),
                    "entry_price": _safe_float(pos.get("avg_entry_price"), 0.0),
                    "current_price": _safe_float(pos.get("current_price"), 0.0),
                    "unrealized_plpc": round(_safe_float(pos.get("unrealized_plpc"), 0.0) * 100.0, 4),
                "entry_time": str(meta.get("entry_time_iso", "") or ""),
                "entry_time_label": _to_ct_label(_parse_ts(str(meta.get("entry_time_iso", "") or ""))),
                }
            )

        stock_series_map = _fetch_intraday_stock_series(list(dict.fromkeys(underlyings)), limit=96)
        payload_rows = []
        for row in option_rows:
            symbol = str(row.get("symbol", ""))
            underlying = str(row.get("underlying", ""))
            raw_series = pl_history.get(symbol, [])
            pnl_series: list[dict[str, Any]] = []
            if isinstance(raw_series, list):
                for point in raw_series[-240:]:
                    if not isinstance(point, dict):
                        continue
                    ts_raw = point.get("ts")
                    plpc_raw = point.get("plpc")
                    if ts_raw is None or plpc_raw is None:
                        continue
                    pnl_series.append({"ts": str(ts_raw), "plpc": _safe_float(plpc_raw)})
            payload_rows.append(
                {
                    **row,
                    "pnl_series": pnl_series,
                    "stock_series": stock_series_map.get(underlying, []),
                }
            )
        return jsonify({"rows": payload_rows, "generated_at": _now_et().strftime("%Y-%m-%d %H:%M:%S ET")})
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@app.get("/api/watch/history")
def api_watch_history():
    try:
        rows = _read_csv_rows(TRADES_CSV, limit=5000, reverse=False)
        cumulative_pnl_usd = 0.0
        trade_points: list[dict[str, Any]] = []
        daily_map: dict[str, dict[str, Any]] = {}
        wins = 0
        losses = 0
        total_pnl_usd = 0.0
        total_trades = 0

        for row in rows:
            ts_raw = str(row.get("timestamp", "") or "")
            dt = _parse_ts(ts_raw)
            if dt is None:
                continue
            entry_price = _safe_float(row.get("entry_price"), 0.0)
            exit_price = _safe_float(row.get("exit_price"), 0.0)
            qty = int(_safe_float(row.get("qty"), 0))
            pnl_pct = _safe_float(row.get("pnl_pct"), 0.0) * 100.0
            pnl_usd = (exit_price - entry_price) * qty * 100.0
            cumulative_pnl_usd += pnl_usd
            total_pnl_usd += pnl_usd
            total_trades += 1
            if pnl_usd > 0:
                wins += 1
            elif pnl_usd < 0:
                losses += 1

            day_key = dt.strftime("%Y-%m-%d")
            day_bucket = daily_map.get(day_key)
            if not isinstance(day_bucket, dict):
                day_bucket = {"date": day_key, "pnl_usd": 0.0, "trades": 0, "wins": 0, "losses": 0}
                daily_map[day_key] = day_bucket
            day_bucket["pnl_usd"] = _safe_float(day_bucket.get("pnl_usd"), 0.0) + pnl_usd
            day_bucket["trades"] = int(day_bucket.get("trades", 0)) + 1
            if pnl_usd > 0:
                day_bucket["wins"] = int(day_bucket.get("wins", 0)) + 1
            elif pnl_usd < 0:
                day_bucket["losses"] = int(day_bucket.get("losses", 0)) + 1

            trade_points.append(
                {
                    "timestamp": _to_ct_label(dt) or ts_raw,
                    "ticker": str(row.get("ticker", "") or ""),
                    "direction": str(row.get("direction", "") or "").upper(),
                    "entry_price": round(entry_price, 4),
                    "exit_price": round(exit_price, 4),
                    "qty": qty,
                    "pnl_pct": round(pnl_pct, 4),
                    "pnl_usd": round(pnl_usd, 2),
                    "cum_pnl_usd": round(cumulative_pnl_usd, 2),
                    "exit_reason": str(row.get("exit_reason", "") or ""),
                }
            )

        daily_series = sorted(daily_map.values(), key=lambda x: str(x.get("date", "")))
        for day_row in daily_series:
            day_row["pnl_usd"] = round(_safe_float(day_row.get("pnl_usd"), 0.0), 2)

        avg_trade_pnl = (total_pnl_usd / total_trades) if total_trades > 0 else 0.0
        win_rate_pct = ((wins / (wins + losses)) * 100.0) if (wins + losses) > 0 else 0.0

        return jsonify(
            {
                "summary": {
                    "total_trades": total_trades,
                    "wins": wins,
                    "losses": losses,
                    "win_rate_pct": round(win_rate_pct, 2),
                    "total_pnl_usd": round(total_pnl_usd, 2),
                    "avg_trade_pnl_usd": round(avg_trade_pnl, 2),
                },
                "cumulative_series": [{"ts": p["timestamp"], "value": p["cum_pnl_usd"]} for p in trade_points[-800:]],
                "daily_series": daily_series[-120:],
                "recent_trades": list(reversed(trade_points[-80:])),
                "generated_at": _now_et().strftime("%Y-%m-%d %H:%M:%S ET"),
            }
        )
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@app.get("/api/daily-review")
def api_daily_review():
    try:
        now = _now_et()
        cache_ts = _REVIEW_CACHE.get("ts")
        cache_payload = _REVIEW_CACHE.get("payload")
        if cache_ts and cache_payload is not None:
            age_seconds = (now - cache_ts).total_seconds()
            if age_seconds < 300:
                return jsonify(cache_payload)

        payload = _build_daily_review_payload()
        _REVIEW_CACHE["ts"] = now
        _REVIEW_CACHE["payload"] = payload
        return jsonify(payload)
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


def _pnl_usd_from_trade_row(row: dict[str, str]) -> float:
    entry_price = _safe_float(row.get("entry_price"), 0.0)
    exit_price = _safe_float(row.get("exit_price"), 0.0)
    qty = int(_safe_float(row.get("qty"), 0))
    if qty <= 0 or entry_price <= 0 or exit_price <= 0:
        pnl_pct = _safe_float(row.get("pnl_pct"), 0.0)
        return entry_price * qty * 100.0 * pnl_pct
    return (exit_price - entry_price) * qty * 100.0


def _recent_trade_rows(days: int = 7) -> list[dict[str, str]]:
    now = _now_et()
    rows = _read_csv_rows(TRADES_CSV, limit=8000, reverse=False)
    out: list[dict[str, str]] = []
    for row in rows:
        dt = _parse_ts(str(row.get("timestamp", "")))
        if dt is None:
            continue
        if (now - dt).days > max(1, int(days)):
            continue
        out.append(row)
    return out


@app.get("/api/trade-replay")
def api_trade_replay():
    try:
        if not feature_enabled("FEATURE_TRADE_REPLAY", False):
            return jsonify({"enabled": False, "rows": [], "generated_at": _now_et().strftime("%Y-%m-%d %H:%M:%S ET")})
        trades = _read_csv_rows(TRADES_CSV, limit=300, reverse=True)
        scans = _read_csv_rows(SCAN_LOG_CSV, limit=2000, reverse=True)
        scan_by_symbol: dict[str, list[dict[str, str]]] = {}
        for row in scans:
            symbol = str(row.get("symbol", "")).upper()
            if not symbol:
                continue
            scan_by_symbol.setdefault(symbol, []).append(row)

        replay_rows: list[dict[str, Any]] = []
        for trade in trades[:120]:
            ticker = str(trade.get("ticker", "") or "").upper()
            t_dt = _parse_ts(str(trade.get("timestamp", "")))
            scan_reason = ""
            scan_direction = ""
            if ticker and t_dt is not None:
                for scan in scan_by_symbol.get(ticker, []):
                    s_dt = _parse_ts(str(scan.get("timestamp", "")))
                    if s_dt is None or s_dt > t_dt:
                        continue
                    scan_reason = str(scan.get("reason", "") or "")
                    scan_direction = str(scan.get("direction", "") or "")
                    break
            replay_rows.append(
                {
                    "timestamp": str(trade.get("timestamp", "") or ""),
                    "ticker": ticker,
                    "direction": str(trade.get("direction", "") or "").upper(),
                    "entry_price": _safe_float(trade.get("entry_price"), 0.0),
                    "exit_price": _safe_float(trade.get("exit_price"), 0.0),
                    "qty": int(_safe_float(trade.get("qty"), 0)),
                    "exit_reason": str(trade.get("exit_reason", "") or ""),
                    "pnl_pct": round(_safe_float(trade.get("pnl_pct"), 0.0) * 100.0, 2),
                    "pnl_usd": round(_pnl_usd_from_trade_row(trade), 2),
                    "scan_direction": scan_direction.upper(),
                    "scan_reason": scan_reason,
                }
            )
        return jsonify({"enabled": True, "rows": replay_rows, "generated_at": _now_et().strftime("%Y-%m-%d %H:%M:%S ET")})
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@app.get("/api/premarket-plan")
def api_premarket_plan():
    try:
        if not feature_enabled("FEATURE_PREMARKET_OPENING_PLAN_CARD", False):
            return jsonify({"enabled": False, "rows": [], "generated_at": _now_et().strftime("%Y-%m-%d %H:%M:%S ET")})
        runtime_state = load_bot_state()
        rows = list(runtime_state.get("premarket_opening_signals") or [])
        day = str(runtime_state.get("premarket_signals_day", "") or "")
        plan_rows: list[dict[str, Any]] = []
        for item in rows[:12]:
            if not isinstance(item, dict):
                continue
            plan_rows.append(
                {
                    "symbol": str(item.get("symbol", "") or "").upper(),
                    "direction": str(item.get("direction", "") or "").upper(),
                    "signal_score": round(_safe_float(item.get("signal_score"), 0.0), 2),
                    "rvol": round(_safe_float(item.get("rvol"), 0.0), 2),
                    "reason": str(item.get("reason", "") or ""),
                }
            )
        return jsonify(
            {
                "enabled": True,
                "day": day,
                "rows": plan_rows,
                "generated_at": _now_et().strftime("%Y-%m-%d %H:%M:%S ET"),
            }
        )
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@app.get("/api/exit-reliability")
def api_exit_reliability():
    try:
        if not feature_enabled("FEATURE_EXIT_RELIABILITY_METRICS", False):
            return jsonify({"enabled": False, "generated_at": _now_et().strftime("%Y-%m-%d %H:%M:%S ET")})
        trades = _recent_trade_rows(days=7)
        total = len(trades)
        reason_counts: dict[str, int] = {}
        for row in trades:
            reason = str(row.get("exit_reason", "") or "unknown")
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
        stop_loss = int(reason_counts.get("stop_loss", 0))
        pre_expiry = int(reason_counts.get("pre_expiry_exit", 0) + reason_counts.get("pre_expiry_exit_overdue", 0))
        eod = int(reason_counts.get("eod_close", 0))
        overnight = int(reason_counts.get("overnight_forced_close", 0))
        reliability = 100.0
        if total > 0:
            reliability = max(0.0, 100.0 - ((overnight / total) * 100.0))
        return jsonify(
            {
                "enabled": True,
                "window_days": 7,
                "total_exits": total,
                "stop_loss_exits": stop_loss,
                "pre_expiry_exits": pre_expiry,
                "eod_exits": eod,
                "overnight_forced_closes": overnight,
                "reliability_score_pct": round(reliability, 2),
                "reason_counts": reason_counts,
                "generated_at": _now_et().strftime("%Y-%m-%d %H:%M:%S ET"),
            }
        )
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@app.get("/api/ticker-scorecards")
def api_ticker_scorecards():
    try:
        if not feature_enabled("FEATURE_TICKER_SCORECARDS", False):
            return jsonify({"enabled": False, "rows": [], "generated_at": _now_et().strftime("%Y-%m-%d %H:%M:%S ET")})
        trades = _recent_trade_rows(days=21)
        per_ticker: dict[str, dict[str, Any]] = {}
        for row in trades:
            ticker = str(row.get("ticker", "") or "").upper()
            if not ticker:
                continue
            item = per_ticker.get(ticker)
            if item is None:
                item = {"ticker": ticker, "trades": 0, "wins": 0, "losses": 0, "total_pnl_usd": 0.0, "avg_pnl_pct": 0.0}
                per_ticker[ticker] = item
            pnl_pct = _safe_float(row.get("pnl_pct"), 0.0) * 100.0
            pnl_usd = _pnl_usd_from_trade_row(row)
            item["trades"] = int(item["trades"]) + 1
            if pnl_usd > 0:
                item["wins"] = int(item["wins"]) + 1
            elif pnl_usd < 0:
                item["losses"] = int(item["losses"]) + 1
            item["total_pnl_usd"] = float(item["total_pnl_usd"]) + pnl_usd
            item["avg_pnl_pct"] = float(item["avg_pnl_pct"]) + pnl_pct

        rows: list[dict[str, Any]] = []
        for item in per_ticker.values():
            trades_count = max(1, int(item["trades"]))
            wins = int(item["wins"])
            losses = int(item["losses"])
            rows.append(
                {
                    "ticker": str(item["ticker"]),
                    "trades": int(item["trades"]),
                    "wins": wins,
                    "losses": losses,
                    "win_rate_pct": round((wins / max(1, wins + losses)) * 100.0, 2) if (wins + losses) > 0 else 0.0,
                    "total_pnl_usd": round(float(item["total_pnl_usd"]), 2),
                    "avg_pnl_pct": round(float(item["avg_pnl_pct"]) / trades_count, 2),
                }
            )
        rows.sort(key=lambda r: (float(r.get("total_pnl_usd", 0.0)), float(r.get("win_rate_pct", 0.0))), reverse=True)
        return jsonify({"enabled": True, "rows": rows[:30], "generated_at": _now_et().strftime("%Y-%m-%d %H:%M:%S ET")})
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@app.get("/api/weekly-review")
def api_weekly_review():
    try:
        if not feature_enabled("FEATURE_WEEKLY_REVIEW_GENERATOR", False):
            return jsonify({"enabled": False, "generated_at": _now_et().strftime("%Y-%m-%d %H:%M:%S ET")})
        trades = _recent_trade_rows(days=7)
        total = len(trades)
        wins = 0
        total_pnl = 0.0
        reason_counts: dict[str, int] = {}
        for row in trades:
            pnl = _pnl_usd_from_trade_row(row)
            total_pnl += pnl
            if pnl > 0:
                wins += 1
            reason = str(row.get("exit_reason", "") or "unknown")
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
        win_rate = (wins / total * 100.0) if total > 0 else 0.0
        top_reason = ""
        if reason_counts:
            top_reason = sorted(reason_counts.items(), key=lambda x: x[1], reverse=True)[0][0]
        recommendations: list[str] = []
        if total == 0:
            recommendations.append("No closed trades this week. Verify scanner cadence and entry window settings.")
        if win_rate < 45 and total > 5:
            recommendations.append("Win rate below 45%. Consider raising entry_min_signal_score or conservative profile.")
        if reason_counts.get("stop_loss", 0) >= max(3, total // 3):
            recommendations.append("High stop-loss frequency. Review opening volatility filters and bad-fill detector settings.")
        if reason_counts.get("overnight_forced_close", 0) > 0:
            recommendations.append("Overnight forced closes occurred. Confirm EOD hard-close timing and loop heartbeat reliability.")
        if not recommendations:
            recommendations.append("Performance stable. Keep current profile and review top ticker scorecards for sizing opportunities.")
        return jsonify(
            {
                "enabled": True,
                "window_days": 7,
                "total_trades": total,
                "win_rate_pct": round(win_rate, 2),
                "total_pnl_usd": round(total_pnl, 2),
                "top_exit_reason": top_reason,
                "reason_counts": reason_counts,
                "recommendations": recommendations,
                "generated_at": _now_et().strftime("%Y-%m-%d %H:%M:%S ET"),
            }
        )
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@app.get("/watch")
def watch_page():
    return render_template_string(
        """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Trade Watch</title>
  <style>
    :root {
      --bg:#09111a;
      --card:rgba(16, 25, 38, .88);
      --text:#eaf3fc;
      --muted:#97aec7;
      --border:rgba(126,159,194,.3);
      --green:#25d366;
      --red:#ff5d66;
      --cyan:#31cbff;
      --yellow:#ffbf4a;
      --radius:14px;
    }
    * { box-sizing:border-box; }
    body {
      margin:0; color:var(--text);
      font-family:"Avenir Next","Nunito Sans","Segoe UI",Tahoma,sans-serif;
      background:
        radial-gradient(1200px 500px at 100% -20%, rgba(49,203,255,.14), transparent 45%),
        radial-gradient(900px 420px at 0% -10%, rgba(37,211,102,.1), transparent 40%),
        linear-gradient(150deg, #060b12 0%, #0d1726 50%, #0a111b 100%);
      min-height:100vh;
    }
    .wrap { max-width:1300px; margin:0 auto; padding:16px; }
    .top { display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap; margin-bottom:12px; }
    .title { font-size:clamp(20px, 2.2vw, 30px); font-weight:800; }
    .muted { color:var(--muted); }
    .btn {
      border:1px solid var(--border); border-radius:12px; padding:10px 12px;
      background:rgba(127,156,191,.15); color:var(--text); font-weight:700; cursor:pointer;
      text-decoration:none;
    }
    .card {
      background:var(--card); border:1px solid var(--border);
      border-radius:var(--radius); padding:12px; margin-bottom:10px;
    }
    .row { display:flex; flex-wrap:wrap; gap:10px; align-items:center; }
    .input, .select {
      background:#0a1321; color:var(--text); border:1px solid var(--border);
      border-radius:10px; padding:10px;
    }
    .select { min-width:190px; }
    .watch-input { min-width:320px; }
    .chips { display:flex; flex-wrap:wrap; gap:7px; margin-top:8px; }
    .chip {
      border:1px solid var(--border); border-radius:999px; padding:4px 8px;
      font-size:12px; color:var(--text);
    }
    .grid { display:grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap:10px; }
    .kpi { display:grid; grid-template-columns:repeat(4, minmax(0,1fr)); gap:8px; margin-bottom:8px; font-size:13px; color:var(--muted); }
    .kpi strong { color:var(--text); font-family:ui-monospace,SFMono-Regular,Menlo,monospace; font-size:15px; }
    svg { width:100%; height:130px; border:1px solid var(--border); border-radius:10px; background:rgba(5,10,17,.6); }
    .tabs { display:flex; gap:8px; margin-bottom:10px; }
    .tab { border:1px solid var(--border); border-radius:10px; padding:8px 10px; font-weight:700; cursor:pointer; background:rgba(127,156,191,.15); }
    .tab.active { background:rgba(42,199,255,.22); border-color:rgba(42,199,255,.45); }
    .hidden { display:none; }
    .hist-grid { display:grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap:10px; }
    .hist-kpi { display:grid; grid-template-columns: repeat(5, minmax(0,1fr)); gap:8px; margin-bottom:10px; }
    .hist-kpi .card { margin:0; padding:10px; }
    .label2 { color:var(--muted); font-size:12px; text-transform:uppercase; letter-spacing:.35px; margin-bottom:4px; }
    .value2 { font-family:ui-monospace,SFMono-Regular,Menlo,monospace; font-size:20px; }
    table { width:100%; border-collapse: collapse; font-size:13px; }
    th, td { border-bottom: 1px solid var(--border); padding: 8px 6px; text-align: left; white-space: nowrap; }
    th { color: var(--muted); font-weight: 600; }
    .pos { color:var(--green); }
    .neg { color:var(--red); }
    .zero { color:var(--muted); }
    #hist-trades-wrap { overflow-x:auto; -webkit-overflow-scrolling: touch; }
    #hist-trades-wrap table { min-width: 720px; }
    @media (max-width: 980px) {
      .grid { grid-template-columns: 1fr; }
      .kpi { grid-template-columns:repeat(2, minmax(0,1fr)); }
      .hist-grid { grid-template-columns: 1fr; }
      .hist-kpi { grid-template-columns:repeat(2, minmax(0,1fr)); }
    }
    @media (max-width: 640px) {
      .wrap { padding:12px; }
      .tabs { width:100%; }
      .tab { flex:1 1 140px; text-align:center; }
      .watch-input, .select, .btn { width:100%; min-width:0; }
      .btn { min-height:40px; }
      .kpi { grid-template-columns:1fr; gap:6px; }
      svg { height:120px; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="top">
      <div>
        <div class="title">Open Trade Watch</div>
        <div class="muted">Track live open trade P&L and intraday stock movement.</div>
      </div>
      <div class="row">
        <a class="btn" href="/">Dashboard</a>
      </div>
    </div>

    <div class="tabs">
      <button id="tab-open" class="tab active" onclick="setTab('open')">Open Trades</button>
      <button id="tab-history" class="tab" onclick="setTab('history')">Trade History</button>
    </div>

    <div id="panel-open">
      <div class="card">
        <div style="font-weight:700; margin-bottom:8px;">Watchlist Trading Policy</div>
        <div class="row">
          <select id="watch-mode" class="select">
            <option value="off">Off (No watchlist filter)</option>
            <option value="only_listed">Trade only listed tickers</option>
            <option value="exclude_listed">Trade all except listed tickers</option>
          </select>
          <input id="watch-input" class="input watch-input" placeholder="Add tickers: AAPL, MSFT, NVDA" />
          <button class="btn" onclick="saveWatchlist()">Save Watchlist</button>
        </div>
        <div class="muted" style="margin-top:8px;">Current list:</div>
        <div id="watchlist-chips" class="chips"></div>
        <div id="watch-updated" class="muted" style="margin-top:8px;"></div>
      </div>
      <div id="open-rows" class="grid"></div>
    </div>

    <div id="panel-history" class="hidden">
      <div class="hist-kpi">
        <div class="card"><div class="label2">Total Trades</div><div id="hist-total-trades" class="value2">--</div></div>
        <div class="card"><div class="label2">Win Rate</div><div id="hist-win-rate" class="value2">--</div></div>
        <div class="card"><div class="label2">Total P&L</div><div id="hist-total-pnl" class="value2">--</div></div>
        <div class="card"><div class="label2">Avg / Trade</div><div id="hist-avg-trade" class="value2">--</div></div>
        <div class="card"><div class="label2">W/L</div><div id="hist-wl" class="value2">--</div></div>
      </div>
      <div class="hist-grid">
        <div class="card">
          <div class="label2">Cumulative P&L (USD)</div>
          <svg id="hist-cum-chart" viewBox="0 0 420 120" preserveAspectRatio="none"></svg>
        </div>
        <div class="card">
          <div class="label2">Daily P&L (Last 120 Days)</div>
          <svg id="hist-daily-chart" viewBox="0 0 420 120" preserveAspectRatio="none"></svg>
        </div>
      </div>
      <div class="card">
        <div class="label2">Recent Closed Trades</div>
        <div id="hist-trades-wrap" class="muted">Loading...</div>
      </div>
    </div>
  </div>

  <script>
    function setTab(tab) {
      const open = tab === "open";
      const openPanel = document.getElementById("panel-open");
      const historyPanel = document.getElementById("panel-history");
      const openTab = document.getElementById("tab-open");
      const historyTab = document.getElementById("tab-history");
      if (openPanel) openPanel.className = open ? "" : "hidden";
      if (historyPanel) historyPanel.className = open ? "hidden" : "";
      if (openTab) openTab.className = open ? "tab active" : "tab";
      if (historyTab) historyTab.className = open ? "tab" : "tab active";
    }
    function cls(v) {
      if (v > 0) return "pos";
      if (v < 0) return "neg";
      return "zero";
    }
    function fmtPct(v) {
      const n = Number(v);
      if (Number.isNaN(n)) return "--";
      return `${n >= 0 ? "+" : ""}${n.toFixed(2)}%`;
    }
    function fmtMoney(v) {
      const n = Number(v);
      if (Number.isNaN(n)) return "--";
      return n.toLocaleString(undefined, {style:"currency", currency:"USD", maximumFractionDigits:2});
    }
    function drawSeries(svg, values, color) {
      if (!svg) return;
      if (!Array.isArray(values) || !values.length) {
        svg.innerHTML = `<text x="10" y="22" fill="#8ca2bd" font-size="12">No series yet</text>`;
        return;
      }
      const min = Math.min(...values, 0);
      const max = Math.max(...values, 0);
      const span = Math.max(1e-6, max - min);
      const toX = i => values.length === 1 ? 210 : (i / (values.length - 1)) * 420;
      const toY = v => 112 - ((v - min) / span) * 98;
      const points = values.map((v, i) => `${toX(i)},${toY(v)}`).join(" ");
      const zeroY = toY(0);
      const fill = `M 0 112 L ${points} L 420 112 Z`;
      svg.innerHTML = `
        <line x1="0" y1="${zeroY}" x2="420" y2="${zeroY}" stroke="rgba(140,162,189,.35)" stroke-dasharray="3 3"></line>
        <path d="${fill}" fill="${color}22"></path>
        <path d="M ${points}" fill="none" stroke="${color}" stroke-width="2.3"></path>
      `;
    }
    async function fetchJson(url, options) {
      try {
        const res = await fetch(url, options);
        const body = await res.json();
        if (!res.ok) return { error: body.error || "request failed" };
        return body;
      } catch {
        return { error: "request failed" };
      }
    }
    function parseTickerInput(raw) {
      return String(raw || "")
        .toUpperCase()
        .split(/[\s,]+/)
        .map(x => x.trim())
        .filter(Boolean);
    }
    function drawDailyBars(svg, values) {
      if (!svg) return;
      if (!Array.isArray(values) || !values.length) {
        svg.innerHTML = `<text x="10" y="22" fill="#8ca2bd" font-size="12">No daily history yet</text>`;
        return;
      }
      const min = Math.min(...values, 0);
      const max = Math.max(...values, 0);
      const span = Math.max(1e-6, max - min);
      const width = 420;
      const height = 112;
      const barW = Math.max(1.5, Math.floor(width / Math.max(1, values.length)));
      const y0 = height - ((0 - min) / span) * 98;
      let bars = `<line x1="0" y1="${y0}" x2="${width}" y2="${y0}" stroke="rgba(140,162,189,.35)" stroke-dasharray="3 3"></line>`;
      values.forEach((v, i) => {
        const x = i * (width / Math.max(1, values.length));
        const y = height - ((v - min) / span) * 98;
        const h = Math.abs(y0 - y);
        const top = Math.min(y0, y);
        const color = v >= 0 ? "#1dd75f" : "#ff4e57";
        bars += `<rect x="${x}" y="${top}" width="${barW}" height="${Math.max(1, h)}" fill="${color}" opacity="0.85"></rect>`;
      });
      svg.innerHTML = bars;
    }
    async function loadWatchlist() {
      const data = await fetchJson("/api/watchlist-control");
      if (data.error) return;
      const modeEl = document.getElementById("watch-mode");
      if (modeEl) modeEl.value = String(data.mode || "off");
      const chips = document.getElementById("watchlist-chips");
      if (chips) {
        const tickers = Array.isArray(data.tickers) ? data.tickers : [];
        chips.innerHTML = tickers.length ? tickers.map(t => `<span class="chip">${t}</span>`).join("") : `<span class="muted">No tickers saved</span>`;
      }
      const upd = document.getElementById("watch-updated");
      if (upd) upd.textContent = data.updated_at_et ? `Updated: ${data.updated_at_et}` : "";
    }
    async function saveWatchlist() {
      const mode = document.getElementById("watch-mode").value;
      const tickers = parseTickerInput(document.getElementById("watch-input").value);
      let controlToken = localStorage.getItem("tradeControlToken") || "";
      if (!controlToken) {
        controlToken = window.prompt("Enter dashboard control token");
        if (controlToken) localStorage.setItem("tradeControlToken", controlToken);
      }
      if (!controlToken) {
        alert("Control token required");
        return;
      }
      const body = await fetchJson("/api/watchlist-control", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Trade-Control-Token": controlToken,
        },
        body: JSON.stringify({ mode, tickers }),
      });
      if (body.error) {
        alert(`Watchlist update failed: ${body.error}`);
        return;
      }
      document.getElementById("watch-input").value = "";
      await loadWatchlist();
    }
    function renderOpen(rows) {
      const wrap = document.getElementById("open-rows");
      if (!wrap) return;
      const data = Array.isArray(rows) ? rows : [];
      if (!data.length) {
        wrap.innerHTML = `<div class="card muted">No open option trades right now.</div>`;
        return;
      }
      wrap.innerHTML = data.map((r, i) => `
        <div class="card">
          <div style="font-weight:700; margin-bottom:8px;">${r.underlying || "-"} <span class="${cls(Number(r.unrealized_plpc || 0))}">(${fmtPct(Number(r.unrealized_plpc || 0))})</span></div>
          <div class="kpi">
            <div>Option<br><strong>${r.symbol || "-"}</strong></div>
            <div>Direction<br><strong>${r.direction || "-"}</strong></div>
            <div>Entry<br><strong>${fmtMoney(r.entry_price)}</strong></div>
            <div>Now<br><strong>${fmtMoney(r.current_price)}</strong></div>
            <div>Opened<br><strong>${r.entry_time_label || r.entry_time || "-"}</strong></div>
          </div>
          <div class="muted" style="font-size:12px; margin-bottom:6px;">Open P&L % (loop samples)</div>
          <svg id="pnl-${i}" viewBox="0 0 420 120" preserveAspectRatio="none"></svg>
          <div class="muted" style="font-size:12px; margin:8px 0 6px;">Underlying Stock Price (5m)</div>
          <svg id="stk-${i}" viewBox="0 0 420 120" preserveAspectRatio="none"></svg>
        </div>
      `).join("");

      data.forEach((r, i) => {
        const pnl = Array.isArray(r.pnl_series) ? r.pnl_series.map(p => Number(p.plpc || 0)).filter(v => !Number.isNaN(v)) : [];
        const stk = Array.isArray(r.stock_series) ? r.stock_series.map(p => Number(p.close || 0)).filter(v => !Number.isNaN(v) && v > 0) : [];
        drawSeries(document.getElementById(`pnl-${i}`), pnl, "#2ac7ff");
        drawSeries(document.getElementById(`stk-${i}`), stk, "#f8b739");
      });
    }
    function renderHistory(data) {
      if (!data || data.error) return;
      const summary = data.summary || {};
      const totalTrades = Number(summary.total_trades || 0);
      const wins = Number(summary.wins || 0);
      const losses = Number(summary.losses || 0);
      const winRate = Number(summary.win_rate_pct || 0);
      const totalPnl = Number(summary.total_pnl_usd || 0);
      const avgTrade = Number(summary.avg_trade_pnl_usd || 0);

      const tt = document.getElementById("hist-total-trades");
      const wr = document.getElementById("hist-win-rate");
      const tp = document.getElementById("hist-total-pnl");
      const at = document.getElementById("hist-avg-trade");
      const wl = document.getElementById("hist-wl");
      if (tt) tt.textContent = String(totalTrades);
      if (wr) { wr.textContent = `${winRate.toFixed(1)}%`; wr.className = `value2 ${cls(winRate - 50)}`; }
      if (tp) { tp.textContent = fmtMoney(totalPnl); tp.className = `value2 ${cls(totalPnl)}`; }
      if (at) { at.textContent = fmtMoney(avgTrade); at.className = `value2 ${cls(avgTrade)}`; }
      if (wl) wl.textContent = `${wins}/${losses}`;

      const cumulative = Array.isArray(data.cumulative_series) ? data.cumulative_series.map(x => Number(x.value || 0)) : [];
      drawSeries(document.getElementById("hist-cum-chart"), cumulative, "#2ac7ff");

      const dailyVals = Array.isArray(data.daily_series) ? data.daily_series.map(x => Number(x.pnl_usd || 0)) : [];
      drawDailyBars(document.getElementById("hist-daily-chart"), dailyVals);

      const tradesWrap = document.getElementById("hist-trades-wrap");
      const recent = Array.isArray(data.recent_trades) ? data.recent_trades : [];
      if (!tradesWrap) return;
      if (!recent.length) {
        tradesWrap.textContent = "No closed trades yet.";
        return;
      }
      const rows = recent.slice(0, 60).map(t => `
        <tr>
          <td>${t.timestamp || "-"}</td>
          <td>${t.ticker || "-"}</td>
          <td>${t.direction || "-"}</td>
          <td>${fmtMoney(t.entry_price)}</td>
          <td>${fmtMoney(t.exit_price)}</td>
          <td class="${cls(Number(t.pnl_usd || 0))}">${fmtMoney(t.pnl_usd)}</td>
          <td class="${cls(Number(t.pnl_pct || 0))}">${fmtPct(Number(t.pnl_pct || 0))}</td>
          <td>${t.exit_reason || "-"}</td>
        </tr>
      `).join("");
      tradesWrap.innerHTML = `
        <table>
          <thead><tr><th>Time</th><th>Ticker</th><th>Dir</th><th>Entry</th><th>Exit</th><th>P&L $</th><th>P&L %</th><th>Reason</th></tr></thead>
          <tbody>${rows}</tbody>
        </table>
      `;
    }
    async function refreshOpen() {
      const body = await fetchJson("/api/watch/open");
      if (body.error) return;
      renderOpen(body.rows || []);
    }
    async function refreshHistory() {
      const body = await fetchJson("/api/watch/history");
      if (body.error) return;
      renderHistory(body);
    }
    async function refreshAll() {
      await Promise.all([loadWatchlist(), refreshOpen(), refreshHistory()]);
    }
    refreshAll();
    setInterval(async () => {
      await Promise.all([refreshOpen(), refreshHistory()]);
    }, 15000);
  </script>
</body>
</html>
        """
    )


@app.get("/")
def home():
    return render_template_string(
        """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Alpaca Options Dashboard</title>
  <style>
    :root {
      --bg: #071018;
      --card: rgba(15, 23, 35, 0.84);
      --card-strong: rgba(24, 38, 58, 0.96);
      --text: #ebf3fb;
      --muted: #9ab0c8;
      --green: #25d366;
      --red: #ff5d66;
      --yellow: #ffbf4a;
      --cyan: #31cbff;
      --border: rgba(141, 172, 206, 0.26);
      --shadow: 0 14px 34px rgba(2, 8, 20, 0.45);
      --radius: 16px;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      color: var(--text);
      font-family: "Avenir Next", "Nunito Sans", "Segoe UI", Tahoma, sans-serif;
      background:
        radial-gradient(1200px 500px at -10% -20%, rgba(49, 203, 255, 0.18), transparent 45%),
        radial-gradient(1000px 450px at 110% -10%, rgba(37, 211, 102, 0.13), transparent 45%),
        linear-gradient(140deg, #050a11 0%, #0b1524 45%, #0a111d 100%);
      min-height: 100vh;
    }
    .wrap { max-width: 1260px; margin: 0 auto; padding: 16px; }
    .header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      margin-bottom: 14px;
      padding: 10px 4px;
    }
    .title {
      font-size: clamp(19px, 2.4vw, 30px);
      font-weight: 800;
      line-height: 1.1;
      letter-spacing: 0.2px;
    }
    .paper {
      color: #101214;
      background: linear-gradient(180deg, #ffd773 0%, #ffbf4a 100%);
      padding: 6px 10px;
      border-radius: 999px;
      font-size: 12px;
      font-weight: 800;
      white-space: nowrap;
    }
    .ctrl { display:flex; gap:8px; align-items:center; flex-wrap:wrap; }
    .ctrl-btn {
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 10px 12px;
      min-height: 40px;
      font-weight: 700;
      cursor: pointer;
      color: var(--text);
      background: rgba(127, 156, 191, 0.15);
      transition: transform .15s ease, filter .15s ease;
      -webkit-tap-highlight-color: transparent;
    }
    .ctrl-btn:hover { filter: brightness(1.07); transform: translateY(-1px); }
    .ctrl-btn:active { transform: translateY(0); }
    .ctrl-btn.stop { background: rgba(255, 78, 87, 0.2); border-color: rgba(255, 78, 87, 0.45); }
    .ctrl-btn.start { background: rgba(29, 215, 95, 0.2); border-color: rgba(29, 215, 95, 0.45); }
    .ctrl-state { font-size: 13px; color: var(--muted); }
    .muted { color: var(--muted); }
    .grid4 { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 11px; }
    .grid3 { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 11px; }
    .card {
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      padding: 13px;
      box-shadow: var(--shadow);
      backdrop-filter: blur(4px);
    }
    .card.strong { background: var(--card-strong); }
    .label {
      font-size: 11px;
      color: var(--muted);
      margin-bottom: 6px;
      text-transform: uppercase;
      letter-spacing: 0.8px;
      font-weight: 700;
    }
    .num {
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      font-size: clamp(20px, 3vw, 25px);
      line-height: 1.1;
      font-weight: 700;
    }
    .section { margin-top: 12px; }
    .section h3 { margin: 0 0 8px 0; font-size: 13px; color: var(--muted); letter-spacing: 0.65px; }
    .bar-wrap { margin-bottom: 10px; }
    .bar-line { display:flex; justify-content:space-between; gap:8px; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 13px; }
    .bar { height: 10px; background: #0b1016; border: 1px solid var(--border); border-radius: 999px; overflow: hidden; margin-top: 4px; }
    .fill { height: 100%; width: 0%; background: var(--green); transition: width .2s; }
    #positions-wrap, #trades-wrap, #scan-wrap, #review-checks-wrap, #review-skipped-wrap, #scan-fails-table {
      overflow-x: auto;
      -webkit-overflow-scrolling: touch;
    }
    table { width: 100%; border-collapse: collapse; font-size: 13px; }
    th, td {
      border-bottom: 1px solid var(--border);
      padding: 9px 6px;
      text-align: left;
      vertical-align: top;
      white-space: nowrap;
    }
    th { color: var(--muted); font-weight: 700; font-size: 12px; letter-spacing: 0.35px; }
    #positions-wrap table,
    #trades-wrap table,
    #scan-wrap table,
    #review-checks-wrap table,
    #review-skipped-wrap table,
    #scan-fails-table table {
      min-width: 720px;
    }
    .badge { font-size: 11px; padding: 2px 7px; border-radius: 999px; border: 1px solid var(--border); }
    .b-green { color: var(--green); border-color: rgba(0,200,83,0.4); }
    .b-red { color: var(--red); border-color: rgba(255,23,68,0.4); }
    .b-gray { color: #aaa; border-color: #555; }
    .b-cyan-soft { color: var(--cyan); border-color: rgba(42, 199, 255, 0.45); }
    .pnl-pos { color: var(--green); }
    .pnl-neg { color: var(--red); }
    .pnl-zero { color: #888; }
    .kpi-sub { font-size: 12px; color: var(--muted); margin-top: 4px; }
    .viz-card-title { display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px; font-size: 13px; color: var(--muted); letter-spacing: 0.35px; }
    .viz-box { border: 1px solid var(--border); border-radius: 12px; background: rgba(6, 12, 21, 0.58); padding: 8px; min-height: 130px; }
    .sparkline { width: 100%; height: 110px; }
    .sparkline path.line { fill: none; stroke: var(--cyan); stroke-width: 2.4; }
    .sparkline path.fill { fill: rgba(42, 199, 255, 0.14); }
    .row { display: grid; grid-template-columns: 130px 1fr 44px; align-items: center; gap: 10px; margin-bottom: 8px; font-size: 12px; }
    .track { width: 100%; height: 10px; border-radius: 999px; background: rgba(127, 156, 191, 0.2); overflow: hidden; }
    .bar2 { height: 100%; border-radius: inherit; }
    .b-cyan { background: linear-gradient(90deg, #1da8ff, #2ac7ff); }
    .b-green2 { background: linear-gradient(90deg, #19bf58, #1dd75f); }
    .b-red2 { background: linear-gradient(90deg, #ff6d66, #ff4e57); }
    .head-actions { display:flex; align-items:center; gap:8px; }
    .spacer-sm { height: 10px; }
    .mobile-list { display: grid; gap: 8px; }
    .mobile-item {
      border: 1px solid var(--border);
      border-radius: 12px;
      background: rgba(7, 14, 24, 0.62);
      padding: 10px;
    }
    .mobile-title {
      font-size: 13px;
      font-weight: 700;
      margin-bottom: 6px;
      display: flex;
      justify-content: space-between;
      gap: 8px;
      align-items: center;
    }
    .mobile-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 6px 10px;
      font-size: 12px;
    }
    .mobile-k { color: var(--muted); }
    .mobile-v { color: var(--text); font-weight: 600; }
    @media (max-width: 1120px) {
      .grid4 { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    }
    @media (max-width: 900px) {
      .grid3 { grid-template-columns: 1fr; }
      .wrap { padding: 12px; }
      .header { flex-direction: column; align-items: flex-start; }
      .head-actions { width: 100%; justify-content: space-between; }
      .ctrl { width: 100%; }
      .ctrl-btn { flex: 1 1 160px; justify-content: center; }
      .card { padding: 12px; border-radius: 14px; }
      .row { grid-template-columns: 92px 1fr 36px; gap: 8px; font-size: 11px; }
    }
    @media (max-width: 640px) {
      .grid4 { grid-template-columns: 1fr; }
      .title { font-size: 21px; }
      .paper { font-size: 11px; padding: 5px 8px; }
      .num { font-size: 22px; }
      .kpi-sub { font-size: 11px; line-height: 1.3; }
      .section { margin-top: 10px; }
      .viz-box { min-height: 116px; }
      .bar-line { font-size: 12px; }
      .mobile-grid { grid-template-columns: 1fr; gap: 5px; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="header">
      <div>
        <div class="title">Alpaca Options Autotrader Dashboard</div>
        <div class="muted">Last updated: <span id="last-updated">--</span> | Auto-refresh: 30s</div>
      </div>
      <div class="head-actions">
        <a href="/watch" class="ctrl-btn" style="text-decoration:none;">WATCH PAGE</a>
        <div class="paper">{{ "PAPER MODE" if paper else "LIVE MODE" }}</div>
      </div>
    </div>

    <div class="card section">
      <h3>TRADING CONTROL</h3>
      <div class="ctrl">
        <button class="ctrl-btn stop" onclick="setTradingControl('stop')">STOP TRADING</button>
        <button class="ctrl-btn start" onclick="setTradingControl('start')">START TRADING</button>
        <span id="trading-control-status" class="ctrl-state">Control: --</span>
      </div>
    </div>

    <div id="runtime-control-card" class="card section" style="display:none;">
      <h3>RUNTIME CONTROLS</h3>
      <div class="ctrl">
        <button id="dry-run-btn" class="ctrl-btn" onclick="toggleDryRun()">Toggle Dry Run</button>
        <select id="strategy-profile-select" class="ctrl-btn" onchange="setStrategyProfile()">
          <option value="balanced">balanced</option>
          <option value="conservative">conservative</option>
          <option value="aggressive">aggressive</option>
        </select>
        <span id="runtime-control-status" class="ctrl-state">Runtime: --</span>
      </div>
    </div>

    <div id="guardrail-panel-card" class="card section" style="display:none;">
      <h3>SESSION GUARDRAILS</h3>
      <div id="guardrail-wrap" class="muted">Loading...</div>
    </div>

    <div class="grid4">
      <div class="card strong"><div class="label">Equity</div><div id="equity" class="num">--</div><div class="kpi-sub">Portfolio net liquidation</div></div>
      <div class="card strong"><div class="label">Buying Power</div><div id="buying-power" class="num">--</div><div class="kpi-sub">Available for entries</div></div>
      <div class="card strong"><div class="label">Today P&L</div><div id="daily-pnl" class="num">--</div><div class="kpi-sub">Sum of closed trade %</div></div>
      <div class="card strong"><div class="label">Market Status</div><div id="market-status" class="num">--</div><div id="entry-window-status" class="kpi-sub">Entry Window: --</div><div id="catalyst-mode-status" class="kpi-sub">Catalyst Mode: --</div><div id="trader-loop-status" class="kpi-sub">Trader Loop: --</div><div id="blockers-status" class="kpi-sub">Blockers: --</div></div>
    </div>

    <div class="grid3 section">
      <div class="card"><div class="label">Trades Today</div><div id="trades-today" class="num">--</div></div>
      <div class="card"><div class="label">Win Rate</div><div id="win-rate" class="num">--</div></div>
      <div class="card"><div class="label">Open Positions</div><div id="open-positions-count" class="num">--</div></div>
    </div>

    <div class="card section">
      <h3>CIRCUIT BREAKERS</h3>
      <div class="bar-wrap">
        <div class="bar-line"><span id="daily-loss-text">Daily Loss: --</span><span id="daily-loss-pct">--</span></div>
        <div class="bar"><div id="daily-loss-bar" class="fill"></div></div>
      </div>
      <div class="bar-wrap">
        <div class="bar-line"><span id="streak-text">Consec. Losses: --</span><span id="streak-pct">--</span></div>
        <div class="bar"><div id="streak-bar" class="fill"></div></div>
      </div>
    </div>

    <div class="card section" id="scan-status-card">
      <div class="label">SCANNER STATUS</div>
      <div id="scan-summary">Loading...</div>
      <div style="margin-top:10px; font-size:12px; color:#888">
        Last failures (up to 20):
      </div>
      <table id="scan-fails-table">
        <thead><tr><th>Time</th><th>Symbol</th><th>Reason</th></tr></thead>
        <tbody id="scan-fails-body"></tbody>
      </table>
    </div>

    <div id="premarket-plan-card" class="card section" style="display:none;">
      <h3>PREMARKET OPENING PLAN</h3>
      <div id="premarket-plan-wrap" class="muted">Loading...</div>
    </div>

    <div id="exit-reliability-card" class="card section" style="display:none;">
      <h3>EXIT RELIABILITY</h3>
      <div id="exit-reliability-wrap" class="muted">Loading...</div>
    </div>

    <div class="grid3 section">
      <div class="card">
        <div class="viz-card-title">
          <span>P&L TREND (LAST 10 CLOSED)</span>
          <span id="trend-last">--</span>
        </div>
        <div class="viz-box">
          <svg id="pnl-sparkline" class="sparkline" viewBox="0 0 320 110" preserveAspectRatio="none"></svg>
        </div>
      </div>
      <div class="card">
        <div class="viz-card-title">
          <span>SIGNAL MIX</span>
          <span id="signal-total">--</span>
        </div>
        <div id="signal-mix" class="viz-box">
          <div class="muted">Loading...</div>
        </div>
      </div>
      <div class="card">
        <div class="viz-card-title">
          <span>RISK LOAD</span>
          <span id="risk-load-pct">--</span>
        </div>
        <div id="risk-load" class="viz-box">
          <div class="muted">Loading...</div>
        </div>
      </div>
    </div>

    <div class="card section">
      <h3>OPEN POSITIONS</h3>
      <div id="positions-wrap" class="muted">Loading...</div>
    </div>

    <div class="card section">
      <h3>RECENT TRADES (last 10)</h3>
      <div id="trades-wrap" class="muted">Loading...</div>
    </div>

    <div class="card section">
      <h3>SCANNER - Last Passing Signals</h3>
      <div id="scan-wrap" class="muted">Loading...</div>
    </div>

    <div class="card section">
      <h3>DAILY REVIEW + SELF CHECK</h3>
      <div id="review-checks-wrap" class="muted">Loading review checks...</div>
      <div class="spacer-sm"></div>
      <div id="review-skipped-wrap" class="muted">Loading skipped analysis...</div>
    </div>

    <div id="trade-replay-card" class="card section" style="display:none;">
      <h3>TRADE REPLAY</h3>
      <div id="trade-replay-wrap" class="muted">Loading...</div>
    </div>

    <div id="ticker-scorecards-card" class="card section" style="display:none;">
      <h3>TICKER SCORECARDS</h3>
      <div id="ticker-scorecards-wrap" class="muted">Loading...</div>
    </div>

    <div id="weekly-review-card" class="card section" style="display:none;">
      <h3>WEEKLY REVIEW</h3>
      <div id="weekly-review-wrap" class="muted">Loading...</div>
    </div>
  </div>

  <script>
    const DAILY_LOSS_LIMIT = {{ daily_loss_limit }};
    const CONSEC_LOSS_LIMIT = {{ consec_limit }};
    const MAX_POSITIONS = {{ max_positions }};

    function fmtMoney(v) {
      const n = Number(v);
      if (Number.isNaN(n)) return "--";
      return n.toLocaleString(undefined, {style:"currency", currency:"USD", maximumFractionDigits:2});
    }
    function pctClass(v) {
      if (v > 0) return "pnl-pos";
      if (v < 0) return "pnl-neg";
      return "pnl-zero";
    }
    function asPct(v, digits = 2) {
      const n = Number(v);
      if (Number.isNaN(n)) return "--";
      return `${n >= 0 ? "+" : ""}${n.toFixed(digits)}%`;
    }
    function reasonBadge(reason) {
      if (reason === "profit_target") return `<span class="badge b-green">${reason}</span>`;
      if (reason === "stop_loss") return `<span class="badge b-red">${reason}</span>`;
      return `<span class="badge b-gray">${reason || "manual"}</span>`;
    }
    function barColor(p) {
      if (p < 50) return "#00c853";
      if (p <= 80) return "#ffb300";
      return "#ff1744";
    }
    function isMobileView() {
      return window.matchMedia("(max-width: 640px)").matches;
    }
    function pct(n, total) {
      if (!total) return 0;
      return Math.round((n / total) * 100);
    }
    function renderSparkline(trades) {
      const svg = document.getElementById("pnl-sparkline");
      const lastEl = document.getElementById("trend-last");
      if (!svg) return;
      const rows = Array.isArray(trades) ? trades.slice(0, 10).reverse() : [];
      const values = rows.map(t => Number(t.pnl_pct || 0) * 100).filter(v => !Number.isNaN(v));
      if (!values.length) {
        svg.innerHTML = `<text x="10" y="24" fill="#8fa1b8" font-size="12">No closed trades yet</text>`;
        lastEl.textContent = "--";
        return;
      }
      const min = Math.min(...values, 0);
      const max = Math.max(...values, 0);
      const span = Math.max(max - min, 1);
      const toX = i => (values.length === 1 ? 160 : (i / (values.length - 1)) * 320);
      const toY = v => 100 - ((v - min) / span) * 88;
      const points = values.map((v, i) => `${toX(i)},${toY(v)}`).join(" ");
      const fill = `M 0 100 L ${points} L 320 100 Z`;
      const zeroY = toY(0);
      svg.innerHTML = `
        <line x1="0" y1="${zeroY}" x2="320" y2="${zeroY}" stroke="rgba(143,161,184,0.35)" stroke-dasharray="3 3"></line>
        <path class="fill" d="${fill}"></path>
        <path class="line" d="M ${points}"></path>
      `;
      const last = values[values.length - 1];
      lastEl.textContent = `${last >= 0 ? "+" : ""}${last.toFixed(2)}%`;
      lastEl.className = pctClass(last);
    }
    function renderSignalMix(scanRows) {
      const el = document.getElementById("signal-mix");
      const totalEl = document.getElementById("signal-total");
      if (!el) return;
      const rows = Array.isArray(scanRows) ? scanRows : [];
      const calls = rows.filter(r => String(r.direction || "").toLowerCase() === "call").length;
      const puts = rows.filter(r => String(r.direction || "").toLowerCase() === "put").length;
      const total = rows.length;
      totalEl.textContent = total ? `${total} signals` : "No signals";
      if (!total) {
        el.innerHTML = `<div class="muted">No passing signals yet</div>`;
        return;
      }
      el.innerHTML = `
        <div class="row">
          <span>CALL signals</span>
          <div class="track"><div class="bar2 b-green2" style="width:${pct(calls, total)}%"></div></div>
          <span>${calls}</span>
        </div>
        <div class="row">
          <span>PUT signals</span>
          <div class="track"><div class="bar2 b-red2" style="width:${pct(puts, total)}%"></div></div>
          <span>${puts}</span>
        </div>
      `;
    }
    function renderRiskLoad(positions) {
      const el = document.getElementById("risk-load");
      const pctEl = document.getElementById("risk-load-pct");
      if (!el) return;
      const rows = Array.isArray(positions) ? positions : [];
      const total = rows.length;
      const max = Number(MAX_POSITIONS || 0);
      const p = Math.min(100, Math.round((total / max) * 100));
      pctEl.textContent = `${p}%`;
      el.innerHTML = `
        <div class="row">
          <span>Slots used</span>
          <div class="track"><div class="bar2 b-cyan" style="width:${p}%"></div></div>
          <span>${total}/${max}</span>
        </div>
        <div class="muted" style="font-size:12px; margin-top:10px;">
          Based on MAX_POSITIONS cap in config.
        </div>
      `;
    }

    function renderPositions(data) {
      const el = document.getElementById("positions-wrap");
      if (!Array.isArray(data)) { el.textContent = "—"; return; }
      if (data.length === 0) { el.textContent = "No open positions"; return; }
      if (isMobileView()) {
        const cards = data.map(p => `
          <div class="mobile-item">
            <div class="mobile-title">
              <span>${p.underlying || "-"}</span>
              <span class="${pctClass(Number(p.unrealized_plpc || 0))}">${asPct(Number(p.unrealized_plpc || 0), 1)}</span>
            </div>
            <div class="mobile-grid">
              <div><span class="mobile-k">Dir</span> <span class="mobile-v">${p.direction || "-"}</span></div>
              <div><span class="mobile-k">Qty</span> <span class="mobile-v">${p.qty ?? "-"}</span></div>
              <div><span class="mobile-k">Entry</span> <span class="mobile-v">${fmtMoney(p.entry_price)}</span></div>
              <div><span class="mobile-k">Now</span> <span class="mobile-v">${fmtMoney(p.current_price)}</span></div>
            </div>
          </div>
        `).join("");
        el.innerHTML = `<div class="mobile-list">${cards}</div>`;
        return;
      }
      let rows = data.map(p => `
        <tr>
          <td>${p.underlying || "-"}</td>
          <td><span class="badge ${p.direction === "CALL" ? "b-green" : "b-red"}">${p.direction || "-"}</span></td>
          <td>${p.qty ?? "-"}</td>
          <td>${fmtMoney(p.entry_price)}</td>
          <td>${fmtMoney(p.current_price)}</td>
          <td class="${pctClass(Number(p.unrealized_plpc || 0))}">${asPct(Number(p.unrealized_plpc || 0), 1)}</td>
        </tr>`).join("");
      el.innerHTML = `
        <table>
          <thead><tr><th>Symbol</th><th>Dir</th><th>Qty</th><th>Entry</th><th>Now</th><th>P&L %</th></tr></thead>
          <tbody>${rows}</tbody>
        </table>`;
    }

    function renderTrades(data) {
      const el = document.getElementById("trades-wrap");
      if (!Array.isArray(data)) { el.textContent = "—"; return; }
      const slice = data.slice(0, 10);
      if (slice.length === 0) { el.textContent = "No trades yet"; return; }
      if (isMobileView()) {
        const cards = slice.map(t => {
          const pl = Number(t.pnl_pct || 0) * 100;
          return `
            <div class="mobile-item">
              <div class="mobile-title">
                <span>${t.ticker || "-"}</span>
                <span class="${pctClass(pl)}">${asPct(pl, 2)}</span>
              </div>
              <div class="mobile-grid">
                <div><span class="mobile-k">Time</span> <span class="mobile-v">${t.timestamp || "-"}</span></div>
                <div><span class="mobile-k">Dir</span> <span class="mobile-v">${(t.direction || "-").toUpperCase()}</span></div>
                <div><span class="mobile-k">Entry</span> <span class="mobile-v">${fmtMoney(t.entry_price)}</span></div>
                <div><span class="mobile-k">Exit</span> <span class="mobile-v">${fmtMoney(t.exit_price)}</span></div>
                <div><span class="mobile-k">Reason</span> <span class="mobile-v">${t.exit_reason || "manual"}</span></div>
              </div>
            </div>`;
        }).join("");
        el.innerHTML = `<div class="mobile-list">${cards}</div>`;
        return;
      }
      const rows = slice.map(t => {
        const pl = Number(t.pnl_pct || 0) * 100;
        return `<tr>
          <td>${t.timestamp || "-"}</td>
          <td>${t.ticker || "-"}</td>
          <td><span class="badge ${(t.direction || "").toLowerCase() === "call" ? "b-green" : "b-red"}">${(t.direction || "-").toUpperCase()}</span></td>
          <td>${fmtMoney(t.entry_price)}</td>
          <td>${fmtMoney(t.exit_price)}</td>
          <td class="${pctClass(pl)}">${asPct(pl, 2)}</td>
          <td>${reasonBadge(t.exit_reason || "")}</td>
        </tr>`;
      }).join("");
      el.innerHTML = `<table><thead><tr><th>Time</th><th>Ticker</th><th>Dir</th><th>Entry</th><th>Exit</th><th>P&L %</th><th>Reason</th></tr></thead><tbody>${rows}</tbody></table>`;
    }

    function renderScan(data) {
      const el = document.getElementById("scan-wrap");
      if (!Array.isArray(data)) { el.textContent = "—"; return; }
      const slice = data.slice(0, 10);
      if (slice.length === 0) { el.textContent = "No passing scan signals yet"; return; }
      if (isMobileView()) {
        const cards = slice.map(s => `
          <div class="mobile-item">
            <div class="mobile-title">
              <span>${s.symbol || "-"}</span>
              <span class="badge ${String(s.direction || "").toLowerCase() === "call" ? "b-green" : "b-red"}">${String(s.direction || "-").toUpperCase()}</span>
            </div>
            <div class="mobile-grid">
              <div><span class="mobile-k">Time</span> <span class="mobile-v">${s.timestamp || "-"}</span></div>
              <div><span class="mobile-k">RVOL</span> <span class="mobile-v">${s.rvol || "-"}</span></div>
              <div><span class="mobile-k">RSI</span> <span class="mobile-v">${s.rsi || "-"}</span></div>
              <div><span class="mobile-k">IVR</span> <span class="mobile-v">${s.iv_rank || "-"}</span></div>
              <div><span class="mobile-k">Reason</span> <span class="mobile-v">${s.reason || "-"}</span></div>
            </div>
          </div>
        `).join("");
        el.innerHTML = `<div class="mobile-list">${cards}</div>`;
        return;
      }
      const rows = slice.map(s => `
        <tr>
          <td>${s.timestamp || "-"}</td>
          <td>${s.symbol || "-"}</td>
          <td><span class="badge ${String(s.direction || "").toLowerCase() === "call" ? "b-green" : "b-red"}">${String(s.direction || "-").toUpperCase()}</span></td>
          <td>${s.rvol || "-"}</td>
          <td>${s.rsi || "-"}</td>
          <td>${s.iv_rank || "-"}</td>
          <td>${s.reason || "-"}</td>
        </tr>`).join("");
      el.innerHTML = `<table><thead><tr><th>Time</th><th>Symbol</th><th>Dir</th><th>RVOL</th><th>RSI</th><th>IVR %</th><th>Reason</th></tr></thead><tbody>${rows}</tbody></table>`;
    }

    function reviewBadge(status) {
      if (status === "ok") return `<span class="badge b-green">OK</span>`;
      if (status === "warn") return `<span class="badge b-red">WARN</span>`;
      return `<span class="badge b-gray">INFO</span>`;
    }

    function renderDailyReview(review) {
      const checksEl = document.getElementById("review-checks-wrap");
      const skippedEl = document.getElementById("review-skipped-wrap");
      if (!checksEl || !skippedEl) return;
      if (!review || review.error) {
        checksEl.innerHTML = `<div class="muted">Daily review unavailable</div>`;
        skippedEl.innerHTML = "";
        return;
      }

      const checks = Array.isArray(review.checks) ? review.checks : [];
      if (!checks.length) {
        checksEl.innerHTML = `<div class="muted">No checks available yet.</div>`;
      } else {
        if (isMobileView()) {
          const cards = checks.map(c => `
            <div class="mobile-item">
              <div class="mobile-title">
                <span>${c.name || "-"}</span>
                <span>${reviewBadge(String(c.status || ""))}</span>
              </div>
              <div class="mobile-grid">
                <div><span class="mobile-k">Detail</span> <span class="mobile-v">${c.detail || "-"}</span></div>
              </div>
            </div>
          `).join("");
          checksEl.innerHTML = `<div class="mobile-list">${cards}</div>`;
        } else {
        const rows = checks.map(c => `
          <tr>
            <td>${reviewBadge(String(c.status || ""))}</td>
            <td>${c.name || "-"}</td>
            <td>${c.detail || "-"}</td>
          </tr>`).join("");
        checksEl.innerHTML = `
          <table>
            <thead><tr><th>Status</th><th>Check</th><th>Detail</th></tr></thead>
            <tbody>${rows}</tbody>
          </table>`;
        }
      }

      const skipped = review.skipped || {};
      const items = Array.isArray(skipped.items) ? skipped.items : [];
      const reasons = Array.isArray(skipped.reason_summary) ? skipped.reason_summary : [];
      if (!items.length) {
        skippedEl.innerHTML = `<div class="muted">No skipped symbols recorded today yet.</div>`;
        return;
      }
      const topReasons = reasons.slice(0, 3).map(r => `<span class="badge b-cyan-soft">${r.reason} (${r.count})</span>`).join(" ");
      if (isMobileView()) {
        const cards = items.map(i => `
          <div class="mobile-item">
            <div class="mobile-title">
              <span>${i.symbol || "-"}</span>
              <span>${i.fail_count ?? "-" } fails</span>
            </div>
            <div class="mobile-grid">
              <div><span class="mobile-k">Last Reason</span> <span class="mobile-v">${i.last_reason || "-"}</span></div>
              <div><span class="mobile-k">Day Move</span> <span class="mobile-v ${pctClass(Number(i.day_move_pct || 0))}">${i.day_move_pct == null ? "--" : asPct(Number(i.day_move_pct), 2)}</span></div>
              <div><span class="mobile-k">Day Range</span> <span class="mobile-v">${i.day_range_pct == null ? "--" : asPct(Number(i.day_range_pct), 2)}</span></div>
              <div><span class="mobile-k">Last Seen</span> <span class="mobile-v">${i.last_seen || "-"}</span></div>
            </div>
          </div>
        `).join("");
        skippedEl.innerHTML = `
          <div style="margin-bottom:8px;" class="muted">Top fail reasons: ${topReasons || "—"}</div>
          <div class="mobile-list">${cards}</div>
        `;
        return;
      }
      const rows = items.map(i => `
        <tr>
          <td>${i.symbol || "-"}</td>
          <td>${i.fail_count ?? "-"}</td>
          <td>${i.last_reason || "-"}</td>
          <td class="${pctClass(Number(i.day_move_pct || 0))}">${i.day_move_pct == null ? "--" : asPct(Number(i.day_move_pct), 2)}</td>
          <td>${i.day_range_pct == null ? "--" : asPct(Number(i.day_range_pct), 2)}</td>
          <td>${i.last_seen || "-"}</td>
        </tr>`).join("");
      skippedEl.innerHTML = `
        <div style="margin-bottom:8px;" class="muted">Top fail reasons: ${topReasons || "—"}</div>
        <table>
          <thead><tr><th>Symbol</th><th>Fails</th><th>Last Skip Reason</th><th>Day Move</th><th>Day Range</th><th>Last Seen</th></tr></thead>
          <tbody>${rows}</tbody>
        </table>`;
    }

    function computeCircuitBreakers(trades) {
      const today = new Date().toLocaleDateString("en-CA");
      const todayRows = (Array.isArray(trades) ? trades : []).filter(t => String(t.timestamp || "").startsWith(today));
      let dailyLoss = 0;
      for (const t of todayRows) {
        const pnlPct = Number(t.pnl_pct || 0);
        if (pnlPct < 0) {
          const entry = Number(t.entry_price || 0);
          const qty = Number(t.qty || 0);
          const premium = entry * qty * 100;
          dailyLoss += Math.abs(premium * pnlPct);
        }
      }
      let streak = 0;
      for (let i = 0; i < todayRows.length; i++) {
        const pnlPct = Number(todayRows[i].pnl_pct || 0);
        if (pnlPct < 0) streak += 1; else break;
      }
      return { dailyLoss, streak };
    }

    async function fetchJson(url) {
      try {
        const res = await fetch(url);
        const data = await res.json();
        if (!res.ok) return { error: data.error || "request failed" };
        return data;
      } catch {
        return { error: "request failed" };
      }
    }

    async function setTradingControl(action) {
      const endpoint = action === "stop" ? "/api/trading-control/stop" : "/api/trading-control/start";
      let controlToken = localStorage.getItem("tradeControlToken") || "";
      if (!controlToken) {
        controlToken = window.prompt("Enter dashboard control token");
        if (controlToken) {
          localStorage.setItem("tradeControlToken", controlToken);
        }
      }
      if (!controlToken) {
        alert("Control token is required");
        return;
      }
      try {
        const res = await fetch(endpoint, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "X-Trade-Control-Token": controlToken,
          },
          body: JSON.stringify({reason: `dashboard_${action}`}),
        });
        const body = await res.json();
        if (!res.ok || body.error) {
          alert(`Trading control failed: ${body.error || "request failed"}`);
          return;
        }
      } catch {
        alert("Trading control request failed");
        return;
      }
      await refresh();
    }

    function applyFeatureVisibility(flags) {
      const f = flags || {};
      const runtimeCard = document.getElementById("runtime-control-card");
      const guardrailCard = document.getElementById("guardrail-panel-card");
      const premarketCard = document.getElementById("premarket-plan-card");
      const exitReliabilityCard = document.getElementById("exit-reliability-card");
      const replayCard = document.getElementById("trade-replay-card");
      const tickerCard = document.getElementById("ticker-scorecards-card");
      const weeklyCard = document.getElementById("weekly-review-card");
      if (runtimeCard) runtimeCard.style.display = (f.FEATURE_DRY_RUN_MODE || f.FEATURE_STRATEGY_PROFILES) ? "" : "none";
      if (guardrailCard) guardrailCard.style.display = f.FEATURE_SESSION_GUARDRAIL_PANEL ? "" : "none";
      if (premarketCard) premarketCard.style.display = f.FEATURE_PREMARKET_OPENING_PLAN_CARD ? "" : "none";
      if (exitReliabilityCard) exitReliabilityCard.style.display = f.FEATURE_EXIT_RELIABILITY_METRICS ? "" : "none";
      if (replayCard) replayCard.style.display = f.FEATURE_TRADE_REPLAY ? "" : "none";
      if (tickerCard) tickerCard.style.display = f.FEATURE_TICKER_SCORECARDS ? "" : "none";
      if (weeklyCard) weeklyCard.style.display = f.FEATURE_WEEKLY_REVIEW_GENERATOR ? "" : "none";
    }

    function renderGuardrails(status) {
      const el = document.getElementById("guardrail-wrap");
      if (!el) return;
      if (!status || status.error) {
        el.innerHTML = `<div class="muted">Guardrail data unavailable</div>`;
        return;
      }
      const blockers = Array.isArray(status.blockers) ? status.blockers : [];
      const rows = blockers.length ? blockers.map(b => `<span class="badge b-red">${b}</span>`).join(" ") : `<span class="badge b-green">no blockers</span>`;
      el.innerHTML = `
        <div class="mobile-grid">
          <div><span class="mobile-k">Can Enter</span> <span class="mobile-v ${status.can_enter_now ? "pnl-pos" : "pnl-neg"}">${status.can_enter_now ? "YES" : "NO"}</span></div>
          <div><span class="mobile-k">Profile</span> <span class="mobile-v">${status.strategy_profile || "balanced"}</span></div>
          <div><span class="mobile-k">Dry Run</span> <span class="mobile-v">${status.dry_run ? "ON" : "OFF"}</span></div>
          <div><span class="mobile-k">Entry Window</span> <span class="mobile-v">${status.entry_window_open ? "OPEN" : "CLOSED"}</span></div>
        </div>
        <div style="margin-top:8px;">${rows}</div>
      `;
    }

    function renderPremarketPlan(data) {
      const el = document.getElementById("premarket-plan-wrap");
      if (!el) return;
      if (!data || data.error || !data.enabled) {
        el.innerHTML = `<div class="muted">Premarket plan disabled</div>`;
        return;
      }
      const rows = Array.isArray(data.rows) ? data.rows : [];
      if (!rows.length) {
        el.innerHTML = `<div class="muted">No staged premarket signals for ${data.day || "today"}.</div>`;
        return;
      }
      const items = rows.slice(0, 8).map(r => `
        <tr>
          <td>${r.symbol || "-"}</td>
          <td><span class="badge ${String(r.direction || "").toLowerCase() === "call" ? "b-green" : "b-red"}">${r.direction || "-"}</span></td>
          <td>${Number(r.signal_score || 0).toFixed(2)}</td>
          <td>${Number(r.rvol || 0).toFixed(2)}x</td>
          <td>${r.reason || "-"}</td>
        </tr>
      `).join("");
      el.innerHTML = `<table><thead><tr><th>Symbol</th><th>Dir</th><th>Score</th><th>RVOL</th><th>Reason</th></tr></thead><tbody>${items}</tbody></table>`;
    }

    function renderExitReliability(data) {
      const el = document.getElementById("exit-reliability-wrap");
      if (!el) return;
      if (!data || data.error || !data.enabled) {
        el.innerHTML = `<div class="muted">Exit reliability metrics disabled</div>`;
        return;
      }
      el.innerHTML = `
        <div class="mobile-grid">
          <div><span class="mobile-k">Reliability</span> <span class="mobile-v">${Number(data.reliability_score_pct || 0).toFixed(2)}%</span></div>
          <div><span class="mobile-k">Total Exits</span> <span class="mobile-v">${data.total_exits || 0}</span></div>
          <div><span class="mobile-k">Stop Loss</span> <span class="mobile-v">${data.stop_loss_exits || 0}</span></div>
          <div><span class="mobile-k">Pre-expiry</span> <span class="mobile-v">${data.pre_expiry_exits || 0}</span></div>
          <div><span class="mobile-k">EOD</span> <span class="mobile-v">${data.eod_exits || 0}</span></div>
          <div><span class="mobile-k">Overnight Forced</span> <span class="mobile-v">${data.overnight_forced_closes || 0}</span></div>
        </div>
      `;
    }

    function renderTradeReplay(data) {
      const el = document.getElementById("trade-replay-wrap");
      if (!el) return;
      if (!data || data.error || !data.enabled) {
        el.innerHTML = `<div class="muted">Trade replay disabled</div>`;
        return;
      }
      const rows = Array.isArray(data.rows) ? data.rows : [];
      if (!rows.length) {
        el.innerHTML = `<div class="muted">No replay rows yet</div>`;
        return;
      }
      const body = rows.slice(0, 30).map(r => `
        <tr>
          <td>${r.timestamp || "-"}</td>
          <td>${r.ticker || "-"}</td>
          <td>${r.direction || "-"}</td>
          <td class="${pctClass(Number(r.pnl_pct || 0))}">${asPct(Number(r.pnl_pct || 0), 2)}</td>
          <td>${r.exit_reason || "-"}</td>
          <td>${r.scan_direction || "-"}</td>
          <td>${r.scan_reason || "-"}</td>
        </tr>
      `).join("");
      el.innerHTML = `<table><thead><tr><th>Time</th><th>Ticker</th><th>Dir</th><th>P&L %</th><th>Exit</th><th>Scan Dir</th><th>Scan Reason</th></tr></thead><tbody>${body}</tbody></table>`;
    }

    function renderTickerScorecards(data) {
      const el = document.getElementById("ticker-scorecards-wrap");
      if (!el) return;
      if (!data || data.error || !data.enabled) {
        el.innerHTML = `<div class="muted">Ticker scorecards disabled</div>`;
        return;
      }
      const rows = Array.isArray(data.rows) ? data.rows : [];
      if (!rows.length) {
        el.innerHTML = `<div class="muted">No scorecards yet</div>`;
        return;
      }
      const body = rows.slice(0, 20).map(r => `
        <tr>
          <td>${r.ticker || "-"}</td>
          <td>${r.trades || 0}</td>
          <td>${Number(r.win_rate_pct || 0).toFixed(1)}%</td>
          <td class="${pctClass(Number(r.total_pnl_usd || 0))}">${fmtMoney(r.total_pnl_usd || 0)}</td>
          <td class="${pctClass(Number(r.avg_pnl_pct || 0))}">${asPct(Number(r.avg_pnl_pct || 0), 2)}</td>
        </tr>
      `).join("");
      el.innerHTML = `<table><thead><tr><th>Ticker</th><th>Trades</th><th>Win Rate</th><th>Total P&L</th><th>Avg P&L %</th></tr></thead><tbody>${body}</tbody></table>`;
    }

    function renderWeeklyReview(data) {
      const el = document.getElementById("weekly-review-wrap");
      if (!el) return;
      if (!data || data.error || !data.enabled) {
        el.innerHTML = `<div class="muted">Weekly review disabled</div>`;
        return;
      }
      const recs = Array.isArray(data.recommendations) ? data.recommendations : [];
      const recHtml = recs.map(r => `<li>${r}</li>`).join("");
      el.innerHTML = `
        <div class="mobile-grid">
          <div><span class="mobile-k">Trades</span> <span class="mobile-v">${data.total_trades || 0}</span></div>
          <div><span class="mobile-k">Win Rate</span> <span class="mobile-v">${Number(data.win_rate_pct || 0).toFixed(2)}%</span></div>
          <div><span class="mobile-k">Total P&L</span> <span class="mobile-v ${pctClass(Number(data.total_pnl_usd || 0))}">${fmtMoney(data.total_pnl_usd || 0)}</span></div>
          <div><span class="mobile-k">Top Exit Reason</span> <span class="mobile-v">${data.top_exit_reason || "-"}</span></div>
        </div>
        <ol style="margin-top:8px;">${recHtml}</ol>
      `;
    }

    async function updateRuntimeControl(payload) {
      let controlToken = localStorage.getItem("tradeControlToken") || "";
      if (!controlToken) {
        controlToken = window.prompt("Enter dashboard control token");
        if (controlToken) localStorage.setItem("tradeControlToken", controlToken);
      }
      if (!controlToken) {
        alert("Control token is required");
        return;
      }
      const res = await fetch("/api/runtime-control", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Trade-Control-Token": controlToken,
        },
        body: JSON.stringify(payload || {}),
      });
      const body = await res.json();
      if (!res.ok || body.error) {
        alert(`Runtime control failed: ${body.error || "request failed"}`);
        return;
      }
      await refresh();
    }

    async function toggleDryRun() {
      const current = document.getElementById("runtime-control-status");
      const currentlyOn = current && String(current.textContent || "").toUpperCase().includes("DRY RUN ON");
      await updateRuntimeControl({ dry_run: !currentlyOn });
    }

    async function setStrategyProfile() {
      const el = document.getElementById("strategy-profile-select");
      if (!el) return;
      await updateRuntimeControl({ strategy_profile: String(el.value || "balanced") });
    }

    async function refresh() {
      const [
        account, positions, trades, scanlog, status, scansummary, scanfails, review, control,
        replay, premarketPlan, exitReliability, tickerScorecards, weeklyReview,
      ] = await Promise.all([
        fetchJson("/api/account"),
        fetchJson("/api/positions"),
        fetchJson("/api/trades"),
        fetchJson("/api/scanlog"),
        fetchJson("/api/status"),
        fetchJson("/api/scansummary"),
        fetchJson("/api/scanfails"),
        fetchJson("/api/daily-review"),
        fetchJson("/api/trading-control"),
        fetchJson("/api/trade-replay"),
        fetchJson("/api/premarket-plan"),
        fetchJson("/api/exit-reliability"),
        fetchJson("/api/ticker-scorecards"),
        fetchJson("/api/weekly-review"),
      ]);

      document.getElementById("last-updated").textContent = new Date().toLocaleTimeString();
      const accountOk = !account.error && account.broker_ok !== false;
      const positionsOk = !positions.error && positions.broker_ok !== false;
      const positionsRows = Array.isArray(positions.rows) ? positions.rows : [];
      document.getElementById("equity").textContent = accountOk ? fmtMoney(account.equity) : "—";
      document.getElementById("buying-power").textContent = accountOk ? fmtMoney(account.buying_power) : "—";

      const dailyPct = status.error ? 0 : Number(status.daily_pnl_pct || 0) * 100;
      const pnlEl = document.getElementById("daily-pnl");
      pnlEl.className = `num ${pctClass(dailyPct)}`;
      pnlEl.textContent = status.error ? "—" : asPct(dailyPct, 2);

      const market = status.error ? "—" : (status.market_open ? "OPEN ●" : "CLOSED ○");
      document.getElementById("market-status").textContent = market;
      document.getElementById("market-status").className = `num ${status.error ? "" : (status.market_open ? "pnl-pos" : "pnl-neg")}`;
      const entryEl = document.getElementById("entry-window-status");
      if (entryEl) {
        const windowLabel = status.error ? "--" : String(status.entry_window_label || "--");
        const entryState = status.error ? "--" : (status.entry_window_open ? "OPEN" : "CLOSED");
        entryEl.textContent = `Entry Window: ${entryState} (${windowLabel})`;
        entryEl.style.color = status.error ? "var(--muted)" : (status.entry_window_open ? "var(--green)" : "var(--yellow)");
      }
      const catalystEl = document.getElementById("catalyst-mode-status");
      if (catalystEl) {
        if (status.error) {
          catalystEl.textContent = "Catalyst Mode: --";
          catalystEl.style.color = "var(--muted)";
        } else if (status.catalyst_mode_active) {
          const reason = String(status.catalyst_mode_reason || "shock detected");
          catalystEl.textContent = `Catalyst Mode: ON (${reason})`;
          catalystEl.style.color = "var(--green)";
        } else {
          catalystEl.textContent = "Catalyst Mode: OFF";
          catalystEl.style.color = "var(--muted)";
        }
      }
      const loopEl = document.getElementById("trader-loop-status");
      if (loopEl) {
        if (status.error) {
          loopEl.textContent = "Trader Loop: --";
          loopEl.style.color = "var(--muted)";
        } else if (status.trader_loop_alive) {
          const age = Number(status.trader_heartbeat_age_seconds || 0);
          loopEl.textContent = `Trader Loop: RUNNING (${age}s ago)`;
          loopEl.style.color = "var(--green)";
        } else {
          const age = status.trader_heartbeat_age_seconds == null ? "no heartbeat" : `${status.trader_heartbeat_age_seconds}s`;
          loopEl.textContent = `Trader Loop: NOT RUNNING (${age})`;
          loopEl.style.color = "var(--red)";
        }
      }
      const blockersEl = document.getElementById("blockers-status");
      if (blockersEl) {
        if (status.error) {
          blockersEl.textContent = "Blockers: status unavailable";
          blockersEl.style.color = "var(--muted)";
        } else {
          const blockers = Array.isArray(status.blockers) ? status.blockers : [];
          if (!blockers.length) {
            blockersEl.textContent = "Blockers: none";
            blockersEl.style.color = "var(--green)";
          } else {
            blockersEl.textContent = `Blockers: ${blockers.join(", ")}`;
            blockersEl.style.color = "var(--yellow)";
          }
        }
      }
      const paused = !control.error && Boolean(control.manual_stop);
      const controlEl = document.getElementById("trading-control-status");
      if (controlEl) {
        const when = !control.error ? String(control.updated_at_et || "") : "";
        if (status.error) {
          controlEl.textContent = paused ? `Control: PAUSED (${when || "manual"})` : "Control: AUTO (status unknown)";
          controlEl.style.color = "var(--muted)";
        } else if (paused) {
          controlEl.textContent = `Control: PAUSED (${when || "manual"})`;
          controlEl.style.color = "var(--red)";
        } else if (!status.trader_loop_alive) {
          controlEl.textContent = "Control: AUTO (trader loop not running)";
          controlEl.style.color = "var(--yellow)";
        } else if (!status.can_enter_now) {
          controlEl.textContent = "Control: AUTO (entries currently blocked)";
          controlEl.style.color = "var(--yellow)";
        } else {
          controlEl.textContent = "Control: AUTO (entries allowed)";
          controlEl.style.color = "var(--green)";
        }
      }
      const runtimeStatusEl = document.getElementById("runtime-control-status");
      if (runtimeStatusEl) {
        const dryRunLabel = control && !control.error && control.dry_run ? "DRY RUN ON" : "DRY RUN OFF";
        const profile = control && !control.error ? String(control.strategy_profile || "balanced") : "balanced";
        runtimeStatusEl.textContent = `${dryRunLabel} | profile=${profile}`;
      }
      const profileSelect = document.getElementById("strategy-profile-select");
      if (profileSelect && control && !control.error) {
        profileSelect.value = String(control.strategy_profile || "balanced");
      }
      applyFeatureVisibility((status && status.feature_flags) ? status.feature_flags : {});
      renderGuardrails(status);
      renderPremarketPlan(premarketPlan);
      renderExitReliability(exitReliability);
      renderTradeReplay(replay);
      renderTickerScorecards(tickerScorecards);
      renderWeeklyReview(weeklyReview);

      renderPositions(positionsOk ? positionsRows : []);
      renderTrades(trades.error ? [] : trades);
      renderScan(scanlog.error ? [] : scanlog);
      renderSparkline(trades.error ? [] : trades);
      renderSignalMix(scanlog.error ? [] : scanlog);
      renderRiskLoad(positionsOk ? positionsRows : []);
      renderDailyReview(review);

      const sumEl = document.getElementById("scan-summary");
      if (sumEl) {
        if (scansummary && !scansummary.error) {
          const color = scansummary.pass_count > 0 ? "#00c853" : "#ff9800";
          sumEl.innerHTML = `
              <span style="color:${color}">✓ ${scansummary.pass_count} passed</span>
              &nbsp;|&nbsp;
              <span style="color:#888">${scansummary.fail_count} failed</span>
              &nbsp;|&nbsp;
              Last: ${scansummary.last_scan || "—"}
              <br><small style="color:#888">Top reason: ${scansummary.top_reason || "—"}</small>
          `;
        } else {
          sumEl.textContent = "No scan data yet";
        }
      }

      const failsBody = document.getElementById("scan-fails-body");
      if (failsBody) {
        failsBody.innerHTML = "";
        const failRows = Array.isArray(scanfails) ? scanfails : [];
        failRows.slice(0, 20).forEach(f => {
          const row = document.createElement("tr");
          row.innerHTML = `
              <td>${(f.timestamp || "").slice(11,16)}</td>
              <td>${f.symbol || ""}</td>
              <td style="color:#888; font-size:11px">${f.reason || ""}</td>
          `;
          failsBody.appendChild(row);
        });
      }

      const tradesToday = status.error ? 0 : Number(status.trades_today || 0);
      const wins = status.error ? 0 : Number(status.wins_today || 0);
      const losses = status.error ? 0 : Number(status.losses_today || 0);
      const closed = wins + losses;
      const winRate = closed > 0 ? (wins / closed) * 100 : 0;
      document.getElementById("trades-today").textContent = String(tradesToday);
      const wr = document.getElementById("win-rate");
      wr.textContent = closed > 0 ? `${winRate.toFixed(0)}%` : "--";
      wr.className = `num ${closed > 0 ? pctClass(winRate - 50) : ""}`;
      document.getElementById("open-positions-count").textContent = positionsOk ? String(positionsRows.length) : "--";

      const cb = computeCircuitBreakers(trades.error ? [] : trades);
      const lossPct = DAILY_LOSS_LIMIT > 0 ? Math.min(100, (cb.dailyLoss / DAILY_LOSS_LIMIT) * 100) : 0;
      const streakPct = CONSEC_LOSS_LIMIT > 0 ? Math.min(100, (cb.streak / CONSEC_LOSS_LIMIT) * 100) : 0;

      document.getElementById("daily-loss-text").textContent = `Daily Loss: ${fmtMoney(cb.dailyLoss)} / ${fmtMoney(DAILY_LOSS_LIMIT)}`;
      document.getElementById("daily-loss-pct").textContent = `${lossPct.toFixed(0)}%`;
      const dbar = document.getElementById("daily-loss-bar");
      dbar.style.width = `${lossPct}%`;
      dbar.style.background = barColor(lossPct);

      document.getElementById("streak-text").textContent = `Consec. Losses: ${cb.streak} / ${CONSEC_LOSS_LIMIT}`;
      document.getElementById("streak-pct").textContent = `${streakPct.toFixed(0)}%`;
      const sbar = document.getElementById("streak-bar");
      sbar.style.width = `${streakPct}%`;
      sbar.style.background = barColor(streakPct);
    }

    refresh();
    setInterval(refresh, 30000);
    let resizeTimer = null;
    window.addEventListener("resize", () => {
      if (resizeTimer) clearTimeout(resizeTimer);
      resizeTimer = setTimeout(() => { refresh(); }, 250);
    });
  </script>
</body>
</html>
        """,
        paper=PAPER,
        daily_loss_limit=float(config.DAILY_LOSS_LIMIT_USD),
        consec_limit=int(config.CONSECUTIVE_LOSS_LIMIT),
        max_positions=int(config.MAX_POSITIONS),
    )


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    print(f"Dashboard running at http://localhost:{port}")
    app.run(host="0.0.0.0", port=port, debug=False)
