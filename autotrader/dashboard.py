"""Live read-only dashboard for the Alpaca options autotrader."""

from __future__ import annotations

import csv
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import pytz
import requests
from flask import Flask, jsonify, render_template_string, request

import config
from env_config import get_required_env, load_runtime_env
from state_store import load_bot_state
from trading_control import load_trading_control, set_manual_stop

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
            }
        )
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


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
        return jsonify(rows)
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@app.get("/api/trades")
def api_trades():
    try:
        return jsonify(_read_csv_rows(TRADES_CSV, limit=50, reverse=True))
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
    try:
        clock = requests.get(f"{BASE_URL}/v2/clock", headers=HEADERS, timeout=10)
        clock.raise_for_status()
        clock_body = clock.json()

        today_rows = _today_trade_rows()
        now_et = _now_et()
        now_minutes = (now_et.hour * 60) + now_et.minute
        entry_open = _clock_hhmm_to_minutes(config.NO_NEW_TRADES_BEFORE)
        entry_close = _clock_hhmm_to_minutes(config.NO_NEW_TRADES_AFTER)
        entry_window_open = bool(clock_body.get("is_open", False)) and (entry_open <= now_minutes < entry_close)
        runtime_state = load_bot_state()
        catalyst_mode_active = bool(runtime_state.get("catalyst_mode_active", False))
        catalyst_mode_reason = str(runtime_state.get("catalyst_mode_reason", "") or "")
        catalyst_mode_until = str(runtime_state.get("catalyst_mode_until_iso", "") or "")
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

        trading_paused = bool(load_trading_control().get("manual_stop", False))
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

        return jsonify(
            {
                "market_open": bool(clock_body.get("is_open", False)),
                "trading_paused": trading_paused,
                "entry_window_open": entry_window_open,
                "entry_window_label": f"{config.NO_NEW_TRADES_BEFORE}-{config.NO_NEW_TRADES_AFTER} ET",
                "catalyst_mode_active": catalyst_mode_active,
                "catalyst_mode_reason": catalyst_mode_reason,
                "catalyst_mode_until": catalyst_mode_until,
                "can_enter_now": len(blockers) == 0,
                "blockers": blockers,
                "last_updated": _now_et().strftime("%Y-%m-%d %H:%M:%S ET"),
                "trades_today": len(today_rows),
                "wins_today": wins,
                "losses_today": losses,
                "daily_pnl_pct": round(total_plpc, 4),
            }
        )
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@app.get("/api/trading-control")
def api_trading_control():
    try:
        state = load_trading_control()
        return jsonify(
            {
                "manual_stop": bool(state.get("manual_stop", False)),
                "updated_at_et": str(state.get("updated_at_et", "")),
                "reason": str(state.get("reason", "")),
            }
        )
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@app.post("/api/trading-control/stop")
def api_trading_stop():
    try:
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
      --bg: #070b11;
      --card: rgba(15, 23, 36, 0.82);
      --card-strong: rgba(20, 31, 49, 0.95);
      --text: #e9f0f7;
      --muted: #8fa1b8;
      --green: #1dd75f;
      --red: #ff4e57;
      --yellow: #f8b739;
      --cyan: #2ac7ff;
      --border: rgba(127, 156, 191, 0.25);
      --shadow: 0 12px 30px rgba(2, 8, 20, 0.45);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      color: var(--text);
      font-family: "Segoe UI", Tahoma, Geneva, Verdana, sans-serif;
      background:
        radial-gradient(1200px 500px at -10% -20%, rgba(42, 199, 255, 0.16), transparent 45%),
        radial-gradient(1000px 450px at 110% -10%, rgba(29, 215, 95, 0.12), transparent 45%),
        linear-gradient(140deg, #05080f 0%, #0a1220 45%, #090f19 100%);
      min-height: 100vh;
    }
    .wrap { max-width: 1200px; margin: 0 auto; padding: 16px; }
    .header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 14px; padding: 8px 2px; }
    .title { font-size: 24px; font-weight: 750; letter-spacing: 0.2px; }
    .paper { color: #111; background: linear-gradient(180deg, #ffd773 0%, #f8b739 100%); padding: 5px 10px; border-radius: 999px; font-size: 12px; font-weight: 800; }
    .ctrl { display:flex; gap:8px; align-items:center; flex-wrap:wrap; }
    .ctrl-btn { border: 1px solid var(--border); border-radius: 10px; padding: 8px 10px; font-weight: 700; cursor: pointer; color: var(--text); background: rgba(127, 156, 191, 0.15); }
    .ctrl-btn.stop { background: rgba(255, 78, 87, 0.2); border-color: rgba(255, 78, 87, 0.45); }
    .ctrl-btn.start { background: rgba(29, 215, 95, 0.2); border-color: rgba(29, 215, 95, 0.45); }
    .ctrl-state { font-size: 13px; color: var(--muted); }
    .muted { color: var(--muted); }
    .grid4 { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 10px; }
    .grid3 { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 10px; }
    .card {
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 12px;
      box-shadow: var(--shadow);
      backdrop-filter: blur(3px);
    }
    .card.strong { background: var(--card-strong); }
    .label { font-size: 12px; color: var(--muted); margin-bottom: 6px; text-transform: uppercase; letter-spacing: 0.4px; }
    .num { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 22px; }
    .section { margin-top: 12px; }
    .section h3 { margin: 0 0 8px 0; font-size: 14px; color: var(--muted); letter-spacing: 0.4px; }
    .bar-wrap { margin-bottom: 10px; }
    .bar-line { display:flex; justify-content:space-between; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 13px; }
    .bar { height: 10px; background: #0b1016; border: 1px solid var(--border); border-radius: 999px; overflow: hidden; margin-top: 4px; }
    .fill { height: 100%; width: 0%; background: var(--green); transition: width .2s; }
    table { width: 100%; border-collapse: collapse; font-size: 13px; }
    th, td { border-bottom: 1px solid var(--border); padding: 8px 6px; text-align: left; }
    th { color: var(--muted); font-weight: 600; }
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
    @media (max-width: 900px) {
      .grid4 { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .grid3 { grid-template-columns: 1fr; }
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
      <div class="paper">{{ "PAPER MODE" if paper else "LIVE MODE" }}</div>
    </div>

    <div class="card section">
      <h3>TRADING CONTROL</h3>
      <div class="ctrl">
        <button class="ctrl-btn stop" onclick="setTradingControl('stop')">STOP TRADING</button>
        <button class="ctrl-btn start" onclick="setTradingControl('start')">START TRADING</button>
        <span id="trading-control-status" class="ctrl-state">Control: --</span>
      </div>
    </div>

    <div class="grid4">
      <div class="card strong"><div class="label">Equity</div><div id="equity" class="num">--</div><div class="kpi-sub">Portfolio net liquidation</div></div>
      <div class="card strong"><div class="label">Buying Power</div><div id="buying-power" class="num">--</div><div class="kpi-sub">Available for entries</div></div>
      <div class="card strong"><div class="label">Today P&L</div><div id="daily-pnl" class="num">--</div><div class="kpi-sub">Sum of closed trade %</div></div>
      <div class="card strong"><div class="label">Market Status</div><div id="market-status" class="num">--</div><div id="entry-window-status" class="kpi-sub">Entry Window: --</div><div id="catalyst-mode-status" class="kpi-sub">Catalyst Mode: --</div></div>
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
      <div style="height:10px;"></div>
      <div id="review-skipped-wrap" class="muted">Loading skipped analysis...</div>
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

      const skipped = review.skipped || {};
      const items = Array.isArray(skipped.items) ? skipped.items : [];
      const reasons = Array.isArray(skipped.reason_summary) ? skipped.reason_summary : [];
      if (!items.length) {
        skippedEl.innerHTML = `<div class="muted">No skipped symbols recorded today yet.</div>`;
        return;
      }
      const topReasons = reasons.slice(0, 3).map(r => `<span class="badge b-cyan-soft">${r.reason} (${r.count})</span>`).join(" ");
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
      try {
        const res = await fetch(endpoint, {
          method: "POST",
          headers: {"Content-Type": "application/json"},
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

    async function refresh() {
      const [account, positions, trades, scanlog, status, scansummary, scanfails, review, control] = await Promise.all([
        fetchJson("/api/account"),
        fetchJson("/api/positions"),
        fetchJson("/api/trades"),
        fetchJson("/api/scanlog"),
        fetchJson("/api/status"),
        fetchJson("/api/scansummary"),
        fetchJson("/api/scanfails"),
        fetchJson("/api/daily-review"),
        fetchJson("/api/trading-control"),
      ]);

      document.getElementById("last-updated").textContent = new Date().toLocaleTimeString();
      document.getElementById("equity").textContent = account.error ? "—" : fmtMoney(account.equity);
      document.getElementById("buying-power").textContent = account.error ? "—" : fmtMoney(account.buying_power);

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
      const paused = !control.error && Boolean(control.manual_stop);
      const controlEl = document.getElementById("trading-control-status");
      if (controlEl) {
        const when = !control.error ? String(control.updated_at_et || "") : "";
        controlEl.textContent = paused ? `Control: PAUSED (${when || "manual"})` : "Control: AUTO";
        controlEl.style.color = paused ? "var(--red)" : "var(--green)";
      }

      renderPositions(positions.error ? [] : positions);
      renderTrades(trades.error ? [] : trades);
      renderScan(scanlog.error ? [] : scanlog);
      renderSparkline(trades.error ? [] : trades);
      renderSignalMix(scanlog.error ? [] : scanlog);
      renderRiskLoad(positions.error ? [] : positions);
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
      document.getElementById("open-positions-count").textContent = Array.isArray(positions) ? String(positions.length) : "--";

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
