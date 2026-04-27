"""Live read-only dashboard for the Alpaca options autotrader."""

from __future__ import annotations

import csv
import hashlib
import hmac
import json
import os
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pytz
import requests
from flask import Flask, Response, jsonify, render_template_string, request

from env_config import get_required_env, load_runtime_env

load_runtime_env()
try:
    from autotrader import config
except ImportError:
    import config
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

API_KEY = get_required_env("ALPACA_API_KEY")
SECRET_KEY = get_required_env("ALPACA_SECRET_KEY")
PAPER = bool(config.PAPER)
BASE_URL = "https://paper-api.alpaca.markets" if PAPER else "https://api.alpaca.markets"
DATA_BASE_URL = config.ALPACA_DATA_BASE_URL
HEADERS = {"APCA-API-KEY-ID": API_KEY or "", "APCA-API-SECRET-KEY": SECRET_KEY or ""}

TRADES_CSV = Path(config.TRADES_CSV_PATH)
SCAN_LOG_CSV = Path(config.SCAN_LOG_CSV_PATH)
DASHBOARD_DIR = Path(__file__).resolve().parent
LISA_FEED_JSON_PATH = DASHBOARD_DIR / "autobott_lisa_feed.json"
LISA_FEED_NDJSON_PATH = DASHBOARD_DIR / "autobott_lisa_feed.ndjson"
LISA_FEED_PUBLISHED_PATH = DASHBOARD_DIR / "autobott_lisa_feed_published.json"
EASTERN = pytz.timezone(config.EASTERN_TZ)
CENTRAL = pytz.timezone(config.CENTRAL_TZ)
def _resolve_display_tz() -> Any:
    raw = str(os.getenv("DASHBOARD_DISPLAY_TZ", "America/Chicago") or "America/Chicago").strip()
    normalized = raw.upper().replace("_", "").replace("/", "")
    central_aliases = {"CST", "CDT", "CT", "CENTRAL", "AMERICACHICAGO"}
    if normalized in central_aliases:
        return pytz.timezone("America/Chicago")
    try:
        return pytz.timezone(raw)
    except Exception:
        return pytz.timezone("America/Chicago")


DISPLAY_TZ = _resolve_display_tz()
DISPLAY_TZ_LABEL = str(os.getenv("DASHBOARD_DISPLAY_TZ_LABEL", "CST") or "CST").strip() or "CST"
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
  value = dt.astimezone(DISPLAY_TZ).strftime("%Y-%m-%d %H:%M:%S")
  return f"{value} {DISPLAY_TZ_LABEL}"


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
  expected = str(CONTROL_TOKEN or "").strip()
  if not expected:
    return False, "Dashboard control token is not configured.", 503

  provided = ""
  auth_header = str(request.headers.get("Authorization", "") or "").strip()
  if auth_header.lower().startswith("bearer "):
    provided = auth_header.split(" ", 1)[1].strip()
  if not provided:
    provided = str(request.headers.get("X-Control-Token", "") or "").strip()
  if not provided:
    provided = str(request.args.get("token", "") or "").strip()

  if not provided:
    return False, "Missing control token.", 401
  if not hmac.compare_digest(provided, expected):
    return False, "Invalid control token.", 403
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

    raw = str(value).strip()

    # Fast path: ISO strings, including UTC Z suffix.
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = EASTERN.localize(dt)
        return dt.astimezone(EASTERN)
    except ValueError:
        pass

    # Handle common log timestamps like "2026-04-14 12:14:12 EDT".
    tz_match = re.match(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})(?:\s+([A-Za-z]{2,5}))?$", raw)
    if tz_match:
        base_text = str(tz_match.group(1) or "")
        tz_abbrev = str(tz_match.group(2) or "").upper()
        try:
            base_dt = datetime.strptime(base_text, "%Y-%m-%d %H:%M:%S")
            tz_map = {
                "EDT": EASTERN,
                "EST": EASTERN,
                "CDT": CENTRAL,
                "CST": CENTRAL,
                "UTC": pytz.UTC,
                "GMT": pytz.UTC,
            }
            tzinfo = tz_map.get(tz_abbrev, EASTERN)
            return tzinfo.localize(base_dt).astimezone(EASTERN)
        except ValueError:
            pass

    for fmt in ("%Y-%m-%d %H:%M:%S %Z", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(raw, fmt)
            if dt.tzinfo is None:
                dt = EASTERN.localize(dt)
            return dt.astimezone(EASTERN)
        except ValueError:
            continue
    return None


def _parse_state_iso(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            return EASTERN.localize(dt)
        return dt.astimezone(EASTERN)
    except Exception:
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


def _entry_window_label_for_display() -> str:
    try:
        start_raw = str(config.NO_NEW_TRADES_BEFORE)
        end_raw = str(config.NO_NEW_TRADES_AFTER)
        start_hour, start_min = [int(p) for p in start_raw.split(":", 1)]
        end_hour, end_min = [int(p) for p in end_raw.split(":", 1)]
        now_et = _now_et()
        start_et = EASTERN.localize(datetime(now_et.year, now_et.month, now_et.day, start_hour, start_min, 0))
        end_et = EASTERN.localize(datetime(now_et.year, now_et.month, now_et.day, end_hour, end_min, 0))
        start_local = start_et.astimezone(DISPLAY_TZ).strftime("%H:%M")
        end_local = end_et.astimezone(DISPLAY_TZ).strftime("%H:%M")
        return f"{start_local}-{end_local} {DISPLAY_TZ_LABEL}"
    except Exception:
        return f"{config.NO_NEW_TRADES_BEFORE}-{config.NO_NEW_TRADES_AFTER} ET"


def _scan_fail_stage(reason: Any) -> str:
    text = str(reason or "").strip().lower()
    if text.startswith("cooldown_skip:"):
        return "cooldown_skip"
    if text.startswith("hard_block:"):
        return "hard_block"
    if text.startswith("universe_reject:") or text.startswith("universe rejected"):
        return "universe_reject"
    if text.startswith("setup_reject:") or text.startswith("setup rejected"):
        return "setup_reject"
    if text.startswith("execution_reject:"):
        return "execution_reject"
    if text.startswith("profile_miss:"):
        return "profile_miss"
    return "other_fail"


def _latest_scan_loop_rows(limit: int = 500) -> tuple[str, list[dict[str, Any]]]:
    rows = _read_csv_rows(SCAN_LOG_CSV, limit=max(50, int(limit)), reverse=True)
    if not rows:
        return "", []
    last_ts = str(rows[0].get("timestamp", "") or "")
    loop_rows = [r for r in rows if str(r.get("timestamp", "") or "") == last_ts]
    return last_ts, loop_rows


def _scan_row_stage(row: dict[str, Any]) -> str:
  result = str(row.get("result", "") or "").strip().lower()
  if result == "pass":
    return "setup_pass"
  return _scan_fail_stage(row.get("reason", ""))


def _timeline_for_symbol(scan_rows: list[dict[str, Any]], symbol: str, *, max_items: int = 5) -> list[str]:
  want = str(symbol or "").upper().strip()
  if not want:
    return []

  rows = [r for r in scan_rows if _scan_symbol(r) == want]
  rows.sort(key=lambda item: str(item.get("timestamp", "") or ""), reverse=True)
  out: list[str] = []
  for row in rows[: max(1, int(max_items))]:
    ts_dt = _parse_ts(str(row.get("timestamp", "") or ""))
    ts_label = ts_dt.strftime("%H:%M") if ts_dt is not None else str(row.get("timestamp", "") or "")[:5]
    stage = _scan_row_stage(row)
    reason = str(row.get("reason", "") or "")
    if len(reason) > 70:
      reason = reason[:67] + "..."
    if str(row.get("result", "") or "").lower() == "pass":
      out.append(f"{ts_label} pass ({stage})")
    else:
      out.append(f"{ts_label} fail ({stage}) {reason}")
  return out


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


def _watch_stock_window_config(window_key: str) -> dict[str, Any]:
    key = str(window_key or "1D").upper()
    presets = {
        "1H": {"key": "1H", "timeframe": "1Min", "lookback_minutes": 60, "limit": 120, "label": "1m, last 1h"},
        "1D": {"key": "1D", "timeframe": "5Min", "lookback_minutes": 390, "limit": 120, "label": "5m, 1 day"},
        "1W": {"key": "1W", "timeframe": "15Min", "lookback_minutes": 1950, "limit": 160, "label": "15m, 1 week"},
        "1M": {"key": "1M", "timeframe": "1Hour", "lookback_minutes": 11700, "limit": 260, "label": "1h, 1 month"},
    }
    return dict(presets.get(key, presets["1D"]))


def _watch_history_window_config(window_key: str) -> dict[str, Any]:
    key = str(window_key or "1M").upper()
    presets = {
        "1H": {"key": "1H", "lookback_minutes": 60, "label": "Last 1 hour"},
        "1D": {"key": "1D", "lookback_minutes": 1440, "label": "Last 1 day"},
        "1W": {"key": "1W", "lookback_minutes": 10080, "label": "Last 1 week"},
        "1M": {"key": "1M", "lookback_minutes": 43200, "label": "Last 1 month"},
        "ALL": {"key": "ALL", "lookback_minutes": None, "label": "All history"},
    }
    return dict(presets.get(key, presets["1M"]))


def _fetch_intraday_stock_series(
    symbols: list[str],
    limit: int = 360,
    timeframe: str = "1Min",
    lookback_minutes: int = 360,
) -> dict[str, list[dict[str, Any]]]:
    clean_symbols = [str(s).upper() for s in symbols if str(s).strip()]
    if not clean_symbols:
        return {}
    now_et = _now_et()
    start_et = now_et - timedelta(minutes=max(15, int(lookback_minutes)))
    try:
        resp = requests.get(
            f"{DATA_BASE_URL}/v2/stocks/bars",
            headers=HEADERS,
            params={
                "symbols": ",".join(clean_symbols),
              "timeframe": str(timeframe or "1Min"),
                "start": start_et.astimezone(pytz.UTC).isoformat().replace("+00:00", "Z"),
                "end": now_et.astimezone(pytz.UTC).isoformat().replace("+00:00", "Z"),
              "limit": max(60, int(limit)),
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

    setup_valid_count = sum(1 for r in scan_rows if str(r.get("result", "")).lower() == "pass")
    fail_count = sum(1 for r in scan_rows if str(r.get("result", "")).lower() == "fail")
    total_scans = setup_valid_count + fail_count
    if total_scans > 0:
        setup_yield_rate = (setup_valid_count / total_scans) * 100.0
        checks.append(
            {
                "status": "ok" if setup_yield_rate >= 5 else "warn",
                "name": "Setup Yield",
                "detail": f"{setup_valid_count}/{total_scans} scanner-valid ({setup_yield_rate:.1f}%).",
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


def _scan_symbol(row: dict[str, Any]) -> str:
    return str(row.get("symbol", "") or row.get("ticker", "") or "").upper()


def _trade_ticker(row: dict[str, Any]) -> str:
    return str(row.get("ticker", "") or row.get("symbol", "") or "").upper()


def _build_trade_report_summary(trade_rows: list[dict[str, str]]) -> dict[str, Any]:
    total = len(trade_rows)
    wins = 0
    losses = 0
    total_pnl_usd = 0.0
    total_pnl_pct = 0.0
    exit_reasons: dict[str, int] = {}
    option_no_progress_exit_count = 0
    option_momentum_stall_exit_count = 0
    weak_index_bias_trade_count = 0
    first_green_seconds: list[float] = []
    best_trade: dict[str, Any] | None = None
    worst_trade: dict[str, Any] | None = None

    for row in trade_rows:
        ticker = _trade_ticker(row)
        pnl_usd = _pnl_usd_from_trade_row(row)
        pnl_pct = _safe_float(row.get("pnl_pct"), 0.0) * 100.0
        exit_reason = str(row.get("exit_reason", "") or "unknown")
        item = {
            "timestamp": str(row.get("timestamp", "") or ""),
            "ticker": ticker,
            "direction": str(row.get("direction", "") or "").upper(),
            "entry_price": round(_safe_float(row.get("entry_price"), 0.0), 4),
            "exit_price": round(_safe_float(row.get("exit_price"), 0.0), 4),
            "qty": int(_safe_float(row.get("qty"), 0.0)),
            "pnl_usd": round(pnl_usd, 2),
            "pnl_pct": round(pnl_pct, 2),
            "exit_reason": exit_reason,
        }
        total_pnl_usd += pnl_usd
        total_pnl_pct += pnl_pct
        exit_reasons[exit_reason] = exit_reasons.get(exit_reason, 0) + 1
        if exit_reason == "option_no_progress":
            option_no_progress_exit_count += 1
        if exit_reason == "option_momentum_stall":
            option_momentum_stall_exit_count += 1
        weak_bias_raw = str(row.get("weak_index_bias_trade", "") or "").strip().lower()
        if weak_bias_raw in {"1", "true", "yes", "y"}:
            weak_index_bias_trade_count += 1
        first_green_raw = _safe_float(row.get("time_to_first_green_seconds"), -1.0)
        if first_green_raw >= 0:
            first_green_seconds.append(float(first_green_raw))
        if pnl_usd > 0:
            wins += 1
        elif pnl_usd < 0:
            losses += 1
        if best_trade is None or pnl_usd > float(best_trade.get("pnl_usd", 0.0)):
            best_trade = item
        if worst_trade is None or pnl_usd < float(worst_trade.get("pnl_usd", 0.0)):
            worst_trade = item

    ordered_exit_reasons = [
        {"reason": reason, "count": count}
        for reason, count in sorted(exit_reasons.items(), key=lambda item: (-item[1], item[0]))
    ]
    return {
        "total_trades": total,
        "wins": wins,
        "losses": losses,
        "win_rate_pct": round((wins / total) * 100.0, 2) if total > 0 else 0.0,
        "total_pnl_usd": round(total_pnl_usd, 2),
        "avg_trade_pnl_usd": round(total_pnl_usd / total, 2) if total > 0 else 0.0,
        "avg_trade_pnl_pct": round(total_pnl_pct / total, 2) if total > 0 else 0.0,
        "avg_time_to_first_green_seconds": (
            round(sum(first_green_seconds) / len(first_green_seconds), 1)
            if first_green_seconds
            else None
        ),
        "option_no_progress_exit_count": option_no_progress_exit_count,
        "option_momentum_stall_exit_count": option_momentum_stall_exit_count,
        "weak_index_bias_trade_count": weak_index_bias_trade_count,
        "weak_index_bias_trade_share_pct": (
            round((weak_index_bias_trade_count / total) * 100.0, 2) if total > 0 else 0.0
        ),
        "best_trade": best_trade,
        "worst_trade": worst_trade,
        "exit_reasons": ordered_exit_reasons,
    }


def _build_scan_report_summary(scan_rows: list[dict[str, str]]) -> dict[str, Any]:
    pass_rows: list[dict[str, Any]] = []
    fail_reasons: dict[str, int] = {}
    direction_counts: dict[str, int] = {}
    symbols: set[str] = set()

    for row in scan_rows:
        symbol = _scan_symbol(row)
        if symbol:
            symbols.add(symbol)
        result = str(row.get("result", "") or "").lower()
        if result == "pass":
            direction = str(row.get("direction", "") or "").upper()
            if direction:
                direction_counts[direction] = direction_counts.get(direction, 0) + 1
            pass_rows.append(
                {
                    "timestamp": str(row.get("timestamp", "") or ""),
                    "symbol": symbol,
                    "direction": direction,
                    "signal_score": round(_safe_float(row.get("signal_score"), 0.0), 2),
                    "rvol": round(_safe_float(row.get("rvol"), 0.0), 2),
                    "reason": str(row.get("reason", "") or ""),
                }
            )
        elif result == "fail":
            reason = str(row.get("reason", "") or "unknown")
            fail_reasons[reason] = fail_reasons.get(reason, 0) + 1

    pass_rows.sort(key=lambda item: (float(item.get("signal_score", 0.0)), float(item.get("rvol", 0.0))), reverse=True)
    top_fail_reasons = [
        {"reason": reason, "count": count}
        for reason, count in sorted(fail_reasons.items(), key=lambda item: (-item[1], item[0]))[:8]
    ]
    return {
        "total_rows": len(scan_rows),
      "setup_valid_count": len(pass_rows),
        "pass_count": len(pass_rows),
        "fail_count": sum(fail_reasons.values()),
        "unique_symbols": len(symbols),
        "direction_counts": direction_counts,
      "top_setup_valid": pass_rows[:8],
        "top_passes": pass_rows[:8],
        "top_fail_reasons": top_fail_reasons,
    }


def _build_ticker_scorecard_rows(trade_rows: list[dict[str, str]], limit: int = 5) -> list[dict[str, Any]]:
    per_ticker: dict[str, dict[str, Any]] = {}
    for row in trade_rows:
        ticker = _trade_ticker(row)
        if not ticker:
            continue
        item = per_ticker.get(ticker)
        if item is None:
            item = {
                "ticker": ticker,
                "trades": 0,
                "wins": 0,
                "losses": 0,
                "total_pnl_usd": 0.0,
                "avg_pnl_pct": 0.0,
            }
            per_ticker[ticker] = item
        pnl_usd = _pnl_usd_from_trade_row(row)
        pnl_pct = _safe_float(row.get("pnl_pct"), 0.0) * 100.0
        item["trades"] = int(item["trades"]) + 1
        item["total_pnl_usd"] = float(item["total_pnl_usd"]) + pnl_usd
        item["avg_pnl_pct"] = float(item["avg_pnl_pct"]) + pnl_pct
        if pnl_usd > 0:
            item["wins"] = int(item["wins"]) + 1
        elif pnl_usd < 0:
            item["losses"] = int(item["losses"]) + 1

    rows: list[dict[str, Any]] = []
    for item in per_ticker.values():
        trades_count = max(1, int(item["trades"]))
        wins = int(item["wins"])
        losses = int(item["losses"])
        rows.append(
            {
                "ticker": str(item["ticker"]),
                "trades": trades_count,
                "wins": wins,
                "losses": losses,
                "win_rate_pct": round((wins / max(1, wins + losses)) * 100.0, 2) if (wins + losses) > 0 else 0.0,
                "total_pnl_usd": round(float(item["total_pnl_usd"]), 2),
                "avg_pnl_pct": round(float(item["avg_pnl_pct"]) / trades_count, 2),
            }
        )
    rows.sort(key=lambda item: (float(item.get("total_pnl_usd", 0.0)), float(item.get("win_rate_pct", 0.0))), reverse=True)
    return rows[: max(1, int(limit))]


def _morning_recommendations(
    staged_rows: list[dict[str, Any]],
    recent_summary: dict[str, Any],
    scan_summary: dict[str, Any],
) -> list[str]:
    notes: list[str] = []
    if not staged_rows:
        notes.append("No staged premarket candidates yet. Stay selective and wait for clean live confirmation after the open.")
    else:
        top = staged_rows[0]
        notes.append(
            f"Lead setup is {top.get('symbol', '-')} {top.get('direction', '-')} with score {float(top.get('signal_score', 0.0)):.2f}. Prioritize best-quality names first."
        )
    if float(recent_summary.get("total_pnl_usd", 0.0)) < 0:
        notes.append("Recent closed-trade P&L is negative. Keep size tight and let the first winner prove the session before pressing harder.")
    if float(recent_summary.get("win_rate_pct", 0.0)) >= 55.0 and int(recent_summary.get("total_trades", 0)) >= 4:
        notes.append("Recent hit rate is constructive. If the open confirms the staged names, the current profile can stay engaged.")
    top_fail_reasons = list(scan_summary.get("top_fail_reasons") or [])
    if top_fail_reasons:
        top_fail = top_fail_reasons[0]
        notes.append(
            f"Main rejection trend this morning is '{top_fail.get('reason', 'unknown')}' ({int(top_fail.get('count', 0))} hits). Avoid forcing setups through that filter."
        )
    if not notes:
        notes.append("Premarket conditions are balanced. Trade the first confirmed A-setups and avoid chasing weak opens.")
    return notes[:4]


def _evening_recommendations(
    trade_summary: dict[str, Any],
    scan_summary: dict[str, Any],
) -> list[str]:
    notes: list[str] = []
    total_trades = int(trade_summary.get("total_trades", 0))
    total_pnl_usd = float(trade_summary.get("total_pnl_usd", 0.0))
    win_rate = float(trade_summary.get("win_rate_pct", 0.0))
    exit_reasons = list(trade_summary.get("exit_reasons") or [])
    if total_trades == 0:
        notes.append("No closed trades today. Review whether the scanner stayed too selective or market structure never confirmed entries.")
    elif total_pnl_usd < 0:
        notes.append("The day closed red. Review the worst trade first and confirm whether entries were late or stops were too exposed.")
    else:
        notes.append("The day closed green. Preserve the same setups and session discipline that produced the best trade.")
    if win_rate < 45.0 and total_trades >= 4:
        notes.append("Win rate was soft. Consider a more conservative profile or a higher minimum signal score tomorrow.")
    if exit_reasons and str(exit_reasons[0].get("reason", "")) == "stop_loss":
        notes.append("Stop-loss exits led the book. Focus tomorrow on cleaner opening alignment and avoid marginal momentum entries.")
    if int(scan_summary.get("setup_valid_count", scan_summary.get("pass_count", 0))) == 0:
      notes.append("No setup-valid scanner signals today. Check whether filters were appropriately strict for the tape.")
    if not notes:
        notes.append("Session stats were stable. Keep the same process and review ticker scorecards for sizing opportunities.")
    return notes[:4]


def _build_morning_report_payload() -> dict[str, Any]:
    now = _now_et()
    runtime_state = load_bot_state()
    staged_day = str(runtime_state.get("premarket_signals_day", "") or now.date().isoformat())
    entry_open_minutes = _clock_hhmm_to_minutes(str(config.NO_NEW_TRADES_BEFORE))
    premarket_scans = []
    for row in _today_scan_rows():
        dt = _parse_ts(str(row.get("timestamp", "") or ""))
        if dt is None:
            continue
        if (dt.hour * 60 + dt.minute) < entry_open_minutes:
            premarket_scans.append(row)

    staged_rows: list[dict[str, Any]] = []
    staged_direction_counts: dict[str, int] = {}
    for item in list(runtime_state.get("premarket_opening_signals") or [])[:8]:
        if not isinstance(item, dict):
            continue
        direction = str(item.get("direction", "") or "").upper()
        if direction:
            staged_direction_counts[direction] = staged_direction_counts.get(direction, 0) + 1
        staged_rows.append(
            {
                "symbol": str(item.get("symbol", "") or "").upper(),
                "direction": direction,
                "signal_score": round(_safe_float(item.get("signal_score"), 0.0), 2),
                "rvol": round(_safe_float(item.get("rvol"), 0.0), 2),
                "reason": str(item.get("reason", "") or ""),
            }
        )

    recent_trades = _recent_trade_rows(days=5)
    recent_summary = _build_trade_report_summary(recent_trades)
    scan_summary = _build_scan_report_summary(premarket_scans)
    ready_by = str(getattr(config, "PREMARKET_REPORT_READY_TIME", "08:20") or "08:20")
    ready_by_minutes = _clock_hhmm_to_minutes(ready_by)
    now_minutes = now.hour * 60 + now.minute
    report_status = "ready" if staged_rows else ("building" if now_minutes < ready_by_minutes else "waiting_for_signals")

    last_scan_at = _parse_ts(str(runtime_state.get("premarket_last_scan_at_iso", "") or ""))
    return {
        "report_type": "morning",
        "date": staged_day,
        "generated_at": now.strftime("%Y-%m-%d %H:%M:%S ET"),
        "report_ready_time": ready_by,
        "status": report_status,
        "scan_runs": int(runtime_state.get("premarket_scan_runs", 0) or 0),
        "last_scan_at": last_scan_at.strftime("%Y-%m-%d %H:%M:%S ET") if last_scan_at else "",
        "staged_signal_count": len(staged_rows),
        "staged_direction_counts": staged_direction_counts,
        "staged_signals": staged_rows,
        "premarket_scan_summary": scan_summary,
        "recent_performance": recent_summary,
        "ticker_leaders": _build_ticker_scorecard_rows(recent_trades, limit=5),
        "recommendations": _morning_recommendations(staged_rows, recent_summary, scan_summary),
    }


def _file_health(path: Path) -> dict[str, Any]:
    try:
        exists = path.exists()
        if not exists:
            return {
                "path": str(path),
                "exists": False,
                "size_bytes": 0,
                "modified_at": "",
            }
        stat = path.stat()
        modified = datetime.fromtimestamp(stat.st_mtime, tz=EASTERN)
        return {
            "path": str(path),
            "exists": True,
            "size_bytes": int(stat.st_size),
            "modified_at": modified.strftime("%Y-%m-%d %H:%M:%S ET"),
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "path": str(path),
            "exists": False,
            "size_bytes": 0,
            "modified_at": "",
            "error": str(exc),
        }


def _fetch_broker_order_telemetry() -> dict[str, Any]:
    now = _now_et()
    day_start_et = now.replace(hour=0, minute=0, second=0, microsecond=0)
    day_start_utc = day_start_et.astimezone(pytz.UTC).isoformat().replace("+00:00", "Z")
    try:
        resp = requests.get(
            f"{BASE_URL}/v2/orders",
            headers=HEADERS,
            params={
                "status": "all",
                "after": day_start_utc,
                "direction": "desc",
                "limit": 500,
                "nested": "false",
            },
            timeout=12,
        )
        resp.raise_for_status()
        payload = resp.json()
        if not isinstance(payload, list):
            return {
                "ok": False,
                "error": "unexpected response format",
                "generated_at": now.strftime("%Y-%m-%d %H:%M:%S ET"),
            }

        option_orders_today = 0
        option_filled_orders_today = 0
        option_buy_fills_today = 0
        option_sell_fills_today = 0
        option_rejected_or_canceled_today = 0
        filled_qty_contracts = 0.0
        symbols: set[str] = set()
        status_counts: dict[str, int] = {}

        for order in payload:
            if not isinstance(order, dict):
                continue
            symbol = str(order.get("symbol", "") or "").upper()
            if not _extract_underlying(symbol):
                continue
            submitted_dt = _parse_ts(str(order.get("submitted_at", "") or ""))
            if submitted_dt is None or submitted_dt.date() != now.date():
                continue
            option_orders_today += 1
            symbols.add(symbol)

            status = str(order.get("status", "") or "").lower()
            status_counts[status] = status_counts.get(status, 0) + 1
            filled_qty = _safe_float(order.get("filled_qty"), 0.0)
            side = str(order.get("side", "") or "").lower()
            if status in {"rejected", "canceled", "expired"}:
                option_rejected_or_canceled_today += 1
            if status in {"filled", "partially_filled"} and filled_qty > 0:
                option_filled_orders_today += 1
                filled_qty_contracts += filled_qty
                if side == "buy":
                    option_buy_fills_today += 1
                elif side == "sell":
                    option_sell_fills_today += 1

        return {
            "ok": True,
            "generated_at": now.strftime("%Y-%m-%d %H:%M:%S ET"),
            "option_orders_today": option_orders_today,
            "option_filled_orders_today": option_filled_orders_today,
            "option_buy_fills_today": option_buy_fills_today,
            "option_sell_fills_today": option_sell_fills_today,
            "option_rejected_or_canceled_today": option_rejected_or_canceled_today,
            "filled_qty_contracts": round(filled_qty_contracts, 2),
            "unique_option_symbols": len(symbols),
            "status_counts": status_counts,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "error": str(exc),
            "generated_at": now.strftime("%Y-%m-%d %H:%M:%S ET"),
        }


def _build_evening_report_payload() -> dict[str, Any]:
    now = _now_et()
    runtime_state = load_bot_state()
    today_trades = _today_trade_rows()
    today_scans = _today_scan_rows()
    trade_summary = _build_trade_report_summary(today_trades)
    scan_summary = _build_scan_report_summary(today_scans)
    daily_review = _build_daily_review_payload()
    broker_telemetry = _fetch_broker_order_telemetry()

    telemetry_trade_day = str(runtime_state.get("trade_telemetry_day", "") or "")
    telemetry_closed_count = int(runtime_state.get("trade_telemetry_closed_count", 0) or 0)
    telemetry_total_pnl = round(float(runtime_state.get("trade_telemetry_total_pnl_usd", 0.0) or 0.0), 2)
    telemetry_last_close_iso = str(runtime_state.get("trade_telemetry_last_close_iso", "") or "")
    telemetry_last_close_dt = _parse_state_iso(telemetry_last_close_iso)
    telemetry_log_error = str(runtime_state.get("trade_telemetry_last_log_error", "") or "")

    data_health = {
        "trades_csv": _file_health(TRADES_CSV),
        "scan_log_csv": _file_health(SCAN_LOG_CSV),
      "runtime_state_json": _file_health(config.STATE_JSON_PATH),
        "last_trader_heartbeat_et": str(runtime_state.get("last_trader_heartbeat_et", "") or ""),
      "state_updated_at_iso": str(runtime_state.get("_state_updated_at_iso", "") or ""),
    }

    telemetry_alerts: list[str] = []
    if telemetry_trade_day == now.date().isoformat() and telemetry_closed_count > int(trade_summary.get("total_trades", 0)):
        telemetry_alerts.append(
            (
                f"Runtime recorded {telemetry_closed_count} closed trade(s) today but trades.csv only has "
                f"{int(trade_summary.get('total_trades', 0))}. Local trade log is lagging or failed."
            )
        )
    if broker_telemetry.get("ok") and int(broker_telemetry.get("option_sell_fills_today", 0)) > 0 and int(trade_summary.get("total_trades", 0)) == 0:
        telemetry_alerts.append(
            "Broker reports option sell fills today, but closed-trade rows are still zero. Check trade CSV writer health and runtime log errors."
        )
    if telemetry_log_error:
        telemetry_alerts.append(f"Recent trade log write error: {telemetry_log_error}")

    open_trade_meta = runtime_state.get("open_trade_meta") or {}
    report_finalized = (not bool(open_trade_meta)) and ((now.hour * 60 + now.minute) >= _clock_hhmm_to_minutes(str(config.NO_NEW_TRADES_AFTER)))

    return {
        "report_type": "evening",
        "date": now.strftime("%Y-%m-%d"),
        "generated_at": now.strftime("%Y-%m-%d %H:%M:%S ET"),
        "status": "finalized" if report_finalized else "live",
        "trade_summary": trade_summary,
        "scan_summary": scan_summary,
        "ticker_leaders": _build_ticker_scorecard_rows(today_trades, limit=5),
        "daily_review": daily_review,
        "recommendations": _evening_recommendations(trade_summary, scan_summary),
        "telemetry": {
          "runtime_trades": {
            "day": telemetry_trade_day,
            "closed_count": telemetry_closed_count,
            "total_pnl_usd": telemetry_total_pnl,
            "last_close_at": telemetry_last_close_dt.strftime("%Y-%m-%d %H:%M:%S ET") if telemetry_last_close_dt else "",
            "last_log_error": telemetry_log_error,
          },
          "broker_activity": broker_telemetry,
          "data_health": data_health,
          "alerts": telemetry_alerts,
        },
    }


def _json_read(path: Path) -> dict[str, Any] | None:
    try:
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _resolve_existing_path(candidates: list[Path]) -> Path | None:
    for candidate in candidates:
        try:
            if candidate.exists():
                return candidate
        except Exception:
            continue
    return None


def _load_latest_trade_report() -> dict[str, Any]:
    candidates = [
        DASHBOARD_DIR / "trade_report.json",
        DASHBOARD_DIR / "autotrader" / "trade_report.json",
        Path("autotrader") / "trade_report.json",
        Path("trade_report.json"),
    ]
    found = _resolve_existing_path(candidates)
    if found is None:
        return {"metadata": {}, "overall": {}, "joint_tradeability": {}, "stop_loss_geometry": {}}
    payload = _json_read(found)
    if isinstance(payload, dict):
        return payload
    return {"metadata": {}, "overall": {}, "joint_tradeability": {}, "stop_loss_geometry": {}}


def _load_report_csv(filename: str) -> list[dict[str, str]]:
    candidates = [
        DASHBOARD_DIR / "reports" / filename,
        DASHBOARD_DIR / "autotrader" / "reports" / filename,
        Path("autotrader") / "reports" / filename,
    ]
    found = _resolve_existing_path(candidates)
    if found is None:
        return []
    try:
        with found.open("r", newline="", encoding="utf-8") as handle:
            return list(csv.DictReader(handle))
    except Exception:
        return []


def _signal_key(signal_type: str, symbol: str, title: str) -> str:
    base = f"{signal_type}|{symbol}|{title}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()[:16]


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _confidence_from_n(n: int, *, base: float = 0.35) -> float:
    return round(_clamp(base + (min(20, max(0, n)) * 0.025), 0.20, 0.95), 3)


def _severity_label(score: float) -> str:
    if score >= 0.8:
        return "high"
    if score >= 0.5:
        return "medium"
    return "low"


def _published_signal_keys() -> set[str]:
    payload = _json_read(LISA_FEED_PUBLISHED_PATH)
    if not isinstance(payload, dict):
        return set()
    keys = payload.get("signal_keys")
    if not isinstance(keys, list):
        return set()
    return {str(k) for k in keys if str(k).strip()}


def _build_knowledge_signal(
    *,
    signal_type: str,
    source_type: str,
    symbol: str,
    lane: str,
    category: str,
    title: str,
    summary: str,
    evidence: dict[str, Any],
    metrics: dict[str, Any],
    recommended_action: str,
    tags: list[str],
    severity_score: float,
    time_scope: dict[str, Any],
    published_keys: set[str],
) -> dict[str, Any]:
    key = _signal_key(signal_type, symbol, title)
    novelty = 0.25 if key in published_keys else 0.85
    n = int(_safe_float(evidence.get("trades", 0), 0))
    confidence = _confidence_from_n(n, base=0.40 if novelty >= 0.8 else 0.30)
    return {
        "signal_id": key,
        "signal_type": signal_type,
        "source_system": "autobott",
        "source_type": source_type,
        "generated_at": _now_et().strftime("%Y-%m-%d %H:%M:%S ET"),
        "symbol": symbol,
        "lane": lane,
        "category": category,
        "title": title,
        "summary": summary,
        "evidence": evidence,
        "metrics": metrics,
        "confidence": confidence,
        "novelty": novelty,
        "severity": _severity_label(severity_score),
        "severity_score": round(_clamp(severity_score, 0.0, 1.0), 3),
        "time_scope": time_scope,
        "recommended_action": recommended_action,
        "tags": tags,
    }


def _price_move_label(day_move_pct: float | None) -> str:
    if day_move_pct is None:
        return "flat"
    if day_move_pct >= 1.0:
        return "strong_up"
    if day_move_pct >= 0.25:
        return "up"
    if day_move_pct <= -1.0:
        return "strong_down"
    if day_move_pct <= -0.25:
        return "down"
    return "flat"


def _range_position_pct(latest: float, day_low: float, day_high: float) -> float | None:
    if day_high <= day_low or latest <= 0:
        return None
    return _clamp(((latest - day_low) / (day_high - day_low)) * 100.0, 0.0, 100.0)


def _build_price_intelligence_signals(
    *,
    scan_rows: list[dict[str, Any]],
    published_keys: set[str],
    now_label: str,
) -> tuple[list[dict[str, Any]], list[str]]:
    symbol_stats: dict[str, dict[str, Any]] = {}
    for row in scan_rows:
        symbol = _scan_symbol(row)
        if not symbol:
            continue
        stats = symbol_stats.setdefault(
            symbol,
            {
                "pass_count": 0,
                "fail_count": 0,
                "last_seen": "",
                "last_direction": "",
                "last_score": 0.0,
                "last_rvol": 0.0,
            },
        )
        is_pass = str(row.get("result", "") or "").lower() == "pass"
        if is_pass:
            stats["pass_count"] = int(stats["pass_count"]) + 1
        else:
            stats["fail_count"] = int(stats["fail_count"]) + 1
        stats["last_seen"] = str(row.get("timestamp", "") or stats["last_seen"])
        stats["last_direction"] = str(row.get("direction", "") or stats["last_direction"])
        stats["last_score"] = _safe_float(row.get("signal_score"), float(stats.get("last_score", 0.0) or 0.0))
        stats["last_rvol"] = _safe_float(row.get("rvol"), float(stats.get("last_rvol", 0.0) or 0.0))

    ranked_symbols = sorted(
        symbol_stats.keys(),
        key=lambda s: (
            int(symbol_stats[s].get("pass_count", 0)),
            _safe_float(symbol_stats[s].get("last_score"), 0.0),
            _safe_float(symbol_stats[s].get("last_rvol"), 0.0),
        ),
        reverse=True,
    )[:8]
    if not ranked_symbols:
        return [], []

    snapshots = _fetch_snapshots(ranked_symbols)
    signals: list[dict[str, Any]] = []
    briefing: list[str] = []

    for symbol in ranked_symbols:
        snap = snapshots.get(symbol, {}) if isinstance(snapshots, dict) else {}
        daily = snap.get("dailyBar", {}) if isinstance(snap, dict) else {}
        minute_bar = snap.get("minuteBar", {}) if isinstance(snap, dict) else {}
        latest_trade = snap.get("latestTrade", {}) if isinstance(snap, dict) else {}

        latest = _safe_float(latest_trade.get("p"), 0.0)
        if latest <= 0:
            latest = _safe_float(minute_bar.get("c"), 0.0)
        if latest <= 0:
            latest = _safe_float(daily.get("c"), 0.0)
        if latest <= 0:
            continue

        day_open = _safe_float(daily.get("o"), 0.0)
        day_high = _safe_float(daily.get("h"), 0.0)
        day_low = _safe_float(daily.get("l"), 0.0)
        day_volume = _safe_float(daily.get("v"), 0.0)
        day_move_pct = ((latest - day_open) / day_open * 100.0) if day_open > 0 else None
        range_pos = _range_position_pct(latest, day_low, day_high)

        stats = symbol_stats.get(symbol, {})
        pass_count = int(stats.get("pass_count", 0) or 0)
        fail_count = int(stats.get("fail_count", 0) or 0)
        score = _safe_float(stats.get("last_score"), 0.0)
        rvol = _safe_float(stats.get("last_rvol"), 0.0)
        direction = str(stats.get("last_direction", "") or "").upper()

        move_label = _price_move_label(day_move_pct)
        bias = "watch"
        if move_label in {"strong_up", "up"} and pass_count >= fail_count:
            bias = "bullish_continuation_bias"
        elif move_label in {"strong_down", "down"} and pass_count <= fail_count:
            bias = "bearish_continuation_bias"
        elif move_label == "flat":
            bias = "range_wait_bias"

        sev = _clamp(
            (abs(day_move_pct or 0.0) / 3.0)
            + (max(0, pass_count - fail_count) / 10.0)
            + min(0.2, max(0.0, rvol - 1.0) / 5.0),
            0.2,
            0.92,
        )

        move_text = "n/a" if day_move_pct is None else f"{day_move_pct:+.2f}%"
        range_text = "n/a" if range_pos is None else f"{range_pos:.0f}%"
        summary = (
            f"{symbol} is {move_text} today at ${latest:.2f}, trading near {range_text} of intraday range. "
            f"Scanner pressure: {pass_count} setup-valid / {fail_count} fail, last score {score:.2f}, RVOL {rvol:.2f}x."
        )

        signals.append(
            _build_knowledge_signal(
                signal_type="price_action_signal",
                source_type="market_api_snapshot",
                symbol=symbol,
                lane="market",
                category="price_intelligence",
                title=f"{symbol} price lens",
                summary=summary,
                evidence={
                    "trades": max(1, pass_count + fail_count),
                    "last_seen": str(stats.get("last_seen", "") or ""),
                    "source": "alpaca_snapshot_api",
                },
                metrics={
                    "latest_price": round(latest, 4),
                    "day_move_pct": round(day_move_pct, 2) if day_move_pct is not None else None,
                    "range_position_pct": round(range_pos, 2) if range_pos is not None else None,
                    "day_high": round(day_high, 4) if day_high > 0 else None,
                    "day_low": round(day_low, 4) if day_low > 0 else None,
                    "day_volume": int(day_volume) if day_volume > 0 else 0,
                    "scanner_pass_count": pass_count,
                    "scanner_fail_count": fail_count,
                    "last_signal_score": round(score, 2),
                    "last_rvol": round(rvol, 2),
                    "direction": direction,
                },
                recommended_action=bias,
                tags=["price_tool", "snapshot", "scanner_bridge", "human_readable"],
                severity_score=sev,
                time_scope={"window": "intraday_live", "as_of": now_label},
                published_keys=published_keys,
            )
        )

        briefing.append(
            f"{symbol}: ${latest:.2f} ({move_text}) | range {range_text} | scanner {pass_count}v/{fail_count}f | bias={bias}."
        )

    return signals, briefing[:6]


def _scan_fail_family(reason: str) -> str:
    text = str(reason or "").strip().lower()
    if not text:
        return "unknown"
    if "weak movement" in text or "roc" in text or "vwap" in text:
        return "weak_movement"
    if "spread" in text or "quote" in text:
        return "quote_spread"
    if "confirmation" in text:
        return "confirmation_mismatch"
    if text.startswith("cooldown_skip:") or "cooldown" in text:
        return "cooldown"
    if text.startswith("hard_block:") or "hard block" in text:
        return "hard_block"
    if text.startswith("universe_reject:") or "universe rejected" in text:
        return "universe_reject"
    if "loss limit" in text or "max positions" in text or "risk" in text:
        return "risk_guard"
    if "before_entry_window" in text or "after_entry_window" in text:
        return "timing_window"
    return "other"


def _synthesize_lisa_signals() -> dict[str, Any]:
    report = _load_latest_trade_report()
    runtime_state = load_bot_state()
    published_keys = _published_signal_keys()

    signals: list[dict[str, Any]] = []
    now_label = _now_et().strftime("%Y-%m-%d %H:%M:%S ET")
    now_et = _now_et()

    # Live scanner and runtime signal bridge for operational LISA packets.
    scan_rows = _today_scan_rows()
    scan_summary = _build_scan_report_summary(scan_rows)

    price_signals, human_briefing = _build_price_intelligence_signals(
        scan_rows=scan_rows,
        published_keys=published_keys,
        now_label=now_label,
    )
    signals.extend(price_signals)

    pass_rows: list[dict[str, Any]] = []
    fail_rows: list[dict[str, Any]] = []
    for row in scan_rows:
        result = str(row.get("result", "") or "").lower()
        if result == "pass":
            pass_rows.append(row)
        elif result == "fail":
            fail_rows.append(row)

    symbol_state: dict[str, dict[str, Any]] = {}
    for row in pass_rows:
        symbol = _scan_symbol(row)
        if not symbol:
            continue

        score = _safe_float(row.get("signal_score"), 0.0)
        rvol = _safe_float(row.get("rvol"), 0.0)
        direction = str(row.get("direction", "") or "").upper()
        timestamp = str(row.get("timestamp", "") or "")
        ts_dt = _parse_ts(timestamp)

        state = symbol_state.get(symbol)
        if state is None:
            state = {
                "symbol": symbol,
                "pass_count": 0,
                "direction_counts": {},
                "latest_row": None,
                "latest_dt": None,
                "latest_score": 0.0,
                "latest_rvol": 0.0,
                "peak_score": 0.0,
                "peak_rvol": 0.0,
                "peak_timestamp": "",
            }
            symbol_state[symbol] = state

        state["pass_count"] = int(state["pass_count"]) + 1
        if direction:
            dir_counts = state["direction_counts"]
            dir_counts[direction] = int(dir_counts.get(direction, 0) or 0) + 1

        current_latest_dt = state.get("latest_dt")
        if current_latest_dt is None or (ts_dt is not None and ts_dt >= current_latest_dt):
            state["latest_row"] = row
            state["latest_dt"] = ts_dt
            state["latest_score"] = score
            state["latest_rvol"] = rvol

        if score >= float(state.get("peak_score", 0.0) or 0.0):
            state["peak_score"] = score
            state["peak_rvol"] = rvol
            state["peak_timestamp"] = timestamp

    for symbol, state in sorted(
        symbol_state.items(),
        key=lambda item: (
            int(item[1].get("pass_count", 0) or 0),
            float(item[1].get("latest_score", 0.0) or 0.0),
            float(item[1].get("peak_score", 0.0) or 0.0),
        ),
        reverse=True,
    )[:10]:
        latest_row = state.get("latest_row") or {}
        latest_direction = str(latest_row.get("direction", "") or "").upper()
        direction_counts = dict(state.get("direction_counts") or {})
        active_dirs = [d for d, c in direction_counts.items() if int(c or 0) > 0]
        direction_conflict = len(active_dirs) > 1
        pass_n = max(1, int(state.get("pass_count", 1) or 1))
        latest_score = _safe_float(state.get("latest_score"), 0.0)
        latest_rvol = _safe_float(state.get("latest_rvol"), 0.0)
        peak_score = _safe_float(state.get("peak_score"), latest_score)
        peak_rvol = _safe_float(state.get("peak_rvol"), latest_rvol)
        sev = _clamp((max(latest_score, peak_score) / 12.0) + min(0.35, max(latest_rvol, peak_rvol) / 8.0), 0.2, 0.9)

        if direction_conflict:
            summary = (
                f"{symbol} has mixed scanner direction in the live window (current {latest_direction or 'n/a'}; "
                f"direction counts {direction_counts})."
            )
            action = "scanner_direction_conflict_review"
        else:
            summary = (
                f"{symbol} current scanner state is {latest_direction or 'n/a'} with latest score {latest_score:.2f} "
                f"(peak {peak_score:.2f}) and RVOL {latest_rvol:.2f}x."
            )
            action = "scanner_symbol_state"

        signals.append(
            _build_knowledge_signal(
                signal_type="scanner_symbol_state",
                source_type="scanner_live",
                symbol=symbol,
                lane="market",
                category="live_scanner_knowledge",
                title=f"{symbol} consolidated scanner state",
                summary=summary,
                evidence={
                    "trades": pass_n,
                    "last_seen": str(latest_row.get("timestamp", "") or ""),
                    "peak_seen": str(state.get("peak_timestamp", "") or ""),
                },
                metrics={
                    "current_direction": latest_direction,
                    "direction_counts": direction_counts,
                    "direction_conflict": direction_conflict,
                    "latest_score": round(latest_score, 2),
                    "peak_score": round(peak_score, 2),
                    "latest_rvol": round(latest_rvol, 2),
                    "peak_rvol": round(peak_rvol, 2),
                    "pass_count": pass_n,
                },
                recommended_action=action,
                tags=["scanner", "state", "live", "deduped", "symbol_consolidated"],
                severity_score=sev,
                time_scope={"window": "today_live", "as_of": now_label},
                published_keys=published_keys,
            )
        )

    fail_family_counts: dict[str, int] = {}
    fail_family_examples: dict[str, str] = {}
    for row in fail_rows:
        reason = str(row.get("reason", "") or "unknown")
        family = _scan_fail_family(reason)
        fail_family_counts[family] = int(fail_family_counts.get(family, 0) or 0) + 1
        if family not in fail_family_examples and reason:
            fail_family_examples[family] = reason

    if fail_family_counts:
        ranked_families = sorted(fail_family_counts.items(), key=lambda item: item[1], reverse=True)
        top_family, top_count = ranked_families[0]
        total_fails = sum(int(v or 0) for v in fail_family_counts.values())
        sev = _clamp((top_count / max(1, total_fails)) + (total_fails / 40.0), 0.2, 0.95)
        signals.append(
            _build_knowledge_signal(
                signal_type="scanner_fail_summary",
                source_type="scanner_live",
                symbol="ALL",
                lane="market",
                category="live_scanner_knowledge",
                title="Scanner fail-family summary (grouped)",
                summary=(
                    f"Scanner fails are led by '{top_family}' ({top_count}/{total_fails}). "
                    "Fine-grained reasons are grouped into families for LISA clarity."
                ),
                evidence={"trades": max(1, total_fails)},
                metrics={
                    "top_fail_family": top_family,
                    "top_fail_family_count": int(top_count),
                    "total_fail_count": int(total_fails),
                    "fail_family_counts": {k: int(v) for k, v in ranked_families[:6]},
                    "fail_family_examples": {k: fail_family_examples.get(k, "") for k, _v in ranked_families[:4]},
                },
                recommended_action="scanner_fail_summary",
                tags=["scanner", "fail", "live", "grouped"],
                severity_score=sev,
                time_scope={"window": "today_live", "as_of": now_label},
                published_keys=published_keys,
            )
        )

    hard_block_symbol_counts: dict[str, int] = {}
    hard_block_reason_counts: dict[str, int] = {}
    cooldown_symbol_counts: dict[str, int] = {}

    for row in fail_rows:
        symbol = _scan_symbol(row)
        reason = str(row.get("reason", "") or "")
        reason_l = reason.lower()

        if reason_l.startswith("cooldown_skip:"):
            if symbol:
                cooldown_symbol_counts[symbol] = cooldown_symbol_counts.get(symbol, 0) + 1
            continue

        if reason_l.startswith("hard_block:") or reason_l.startswith("universe rejected"):
            if symbol:
                hard_block_symbol_counts[symbol] = hard_block_symbol_counts.get(symbol, 0) + 1
            hard_block_reason_counts[reason] = hard_block_reason_counts.get(reason, 0) + 1

    for symbol, count in sorted(cooldown_symbol_counts.items(), key=lambda item: item[1], reverse=True)[:8]:
        sev = _clamp((count / 8.0), 0.2, 0.8)
        signals.append(
            _build_knowledge_signal(
                signal_type="cooldown_signal",
                source_type="scanner_live",
                symbol=symbol,
                lane="execution",
                category="runtime_system_knowledge",
                title=f"{symbol} scanner cooldown active",
                summary=f"{symbol} is currently being skipped by scanner cooldown gates.",
                evidence={"trades": count},
                metrics={"cooldown_skip_count": count, "channel": "scanner_reject_cooldown"},
                recommended_action="cooldown_signal",
                tags=["cooldown", "scanner", "live"],
                severity_score=sev,
                time_scope={"window": "today_live", "as_of": now_label},
                published_keys=published_keys,
            )
        )

    for symbol, count in sorted(hard_block_symbol_counts.items(), key=lambda item: item[1], reverse=True)[:8]:
        sev = _clamp((count / 8.0) + 0.25, 0.3, 1.0)
        signals.append(
            _build_knowledge_signal(
                signal_type="hard_block_signal",
                source_type="scanner_live",
                symbol=symbol,
                lane="risk",
                category="live_scanner_knowledge",
                title=f"{symbol} hard-blocked by scanner gates",
                summary=f"{symbol} hit hard-block style scanner rejections {count} times in recent scans.",
                evidence={"trades": count},
                metrics={"hard_block_count": count},
                recommended_action="hard_block_signal",
                tags=["hard_block", "scanner", "live"],
                severity_score=sev,
                time_scope={"window": "today_live", "as_of": now_label},
                published_keys=published_keys,
            )
        )

    if hard_block_reason_counts:
        top_reason, top_reason_count = sorted(
            hard_block_reason_counts.items(), key=lambda item: item[1], reverse=True
        )[0]
        sev = _clamp((top_reason_count / 10.0) + 0.2, 0.3, 0.95)
        signals.append(
            _build_knowledge_signal(
                signal_type="hard_block_signal",
                source_type="scanner_live",
                symbol="ALL",
                lane="risk",
                category="live_scanner_knowledge",
                title="Dominant hard-block reason in current scanner cycle",
                summary=f"Hard-block pressure is led by '{top_reason}'.",
                evidence={"trades": max(1, int(top_reason_count))},
                metrics={"reason": top_reason, "count": int(top_reason_count)},
                recommended_action="hard_block_signal",
                tags=["hard_block", "reason", "scanner"],
                severity_score=sev,
                time_scope={"window": "today_live", "as_of": now_label},
                published_keys=published_keys,
            )
        )

    ticker_loss_cooldown = runtime_state.get("ticker_loss_cooldown_until") if isinstance(runtime_state, dict) else {}
    if isinstance(ticker_loss_cooldown, dict):
        for symbol_raw, until_raw in list(ticker_loss_cooldown.items())[:30]:
            until_dt = _parse_state_iso(until_raw)
            if until_dt is None or until_dt <= now_et:
                continue
            mins_left = max(1, int((until_dt - now_et).total_seconds() // 60))
            sev = _clamp(0.3 + (mins_left / 180.0), 0.3, 0.9)
            symbol = str(symbol_raw or "").upper()
            if not symbol:
                continue
            signals.append(
                _build_knowledge_signal(
                    signal_type="cooldown_signal",
                    source_type="runtime_state",
                    symbol=symbol,
                    lane="execution",
                    category="runtime_system_knowledge",
                    title=f"{symbol} ticker loss cooldown active",
                    summary=f"Ticker re-entry is blocked for approximately {mins_left} more minutes.",
                    evidence={"trades": 1, "blocked_until": until_dt.strftime("%Y-%m-%d %H:%M:%S ET")},
                    metrics={"minutes_remaining": mins_left, "channel": "ticker_loss_cooldown"},
                    recommended_action="cooldown_signal",
                    tags=["cooldown", "loss_control", "runtime"],
                    severity_score=sev,
                    time_scope={"window": "runtime_recent", "as_of": now_label},
                    published_keys=published_keys,
                )
            )

    metadata = report.get("metadata") if isinstance(report, dict) else {}
    overall = report.get("overall") if isinstance(report, dict) else {}
    closed_trades = int(_safe_float((metadata or {}).get("closed_trade_count"), 0))
    conservative_exp = _safe_float((overall or {}).get("conservative_expectancy_usd"), 0.0)
    if closed_trades > 0:
        sev = _clamp((abs(conservative_exp) / 20.0) + (0.2 if conservative_exp < 0 else 0.0), 0.15, 0.95)
        signals.append(
            _build_knowledge_signal(
                signal_type="review_signal",
                source_type="trade_review",
                symbol="ALL",
                lane="market",
                category="execution_knowledge",
                title="Latest review expectancy snapshot",
                summary=(
                    f"Closed-trade review currently reports conservative expectancy {conservative_exp:.2f} USD over {closed_trades} trades."
                ),
                evidence={"trades": closed_trades},
                metrics={"conservative_expectancy_usd": conservative_exp, "closed_trade_count": closed_trades},
                recommended_action="review_signal",
                tags=["review", "expectancy", "knowledge"],
                severity_score=sev,
                time_scope={"window": "rolling", "as_of": now_label},
                published_keys=published_keys,
            )
        )

    joint = report.get("joint_tradeability") if isinstance(report, dict) else {}
    if not isinstance(joint, dict):
        joint = {}

    for row in list(joint.get("ticker_x_hour") or []):
        n = int(_safe_float(row.get("n"), 0))
        exp = _safe_float(row.get("conservative_expectancy_usd"), 0.0)
        symbol = str(row.get("ticker", "") or "")
        hour = str(row.get("entry_hour", "") or "")
        if n < 4 or not symbol:
            continue
        stop_rate = _safe_float(row.get("stop_loss_rate"), 0.0)
        if exp < 0:
            sev = _clamp((abs(exp) / 20.0) + stop_rate, 0.2, 1.0)
            signals.append(
                _build_knowledge_signal(
                    signal_type="symbol_hour_tradeability_warning",
                    source_type="trade_review",
                    symbol=symbol,
                    lane="market",
                    category="execution_knowledge",
                    title=f"{symbol} hour {hour}: negative conservative expectancy",
                    summary=(
                        f"{symbol} in hour {hour} shows negative conservative expectancy with stop-loss concentration."
                    ),
                    evidence={"trades": n, "hour_et": hour},
                    metrics={
                        "conservative_expectancy_usd": exp,
                        "stop_loss_rate": stop_rate,
                        "conservative_win_rate": _safe_float(row.get("conservative_win_rate"), None),
                    },
                    recommended_action="block_hour_for_symbol",
                    tags=["tradeability", "symbol_hour", "negative_expectancy"],
                    severity_score=sev,
                    time_scope={"window": "rolling", "as_of": now_label},
                    published_keys=published_keys,
                )
            )
        elif exp > 0 and n >= 6:
            sev = _clamp((exp / 25.0), 0.15, 0.8)
            signals.append(
                _build_knowledge_signal(
                    signal_type="positive_tradeability_pocket",
                    source_type="trade_review",
                    symbol=symbol,
                    lane="market",
                    category="execution_knowledge",
                    title=f"{symbol} hour {hour}: positive conservative pocket",
                    summary=f"{symbol} in hour {hour} remains positive after conservative accounting.",
                    evidence={"trades": n, "hour_et": hour},
                    metrics={
                        "conservative_expectancy_usd": exp,
                        "stop_loss_rate": stop_rate,
                    },
                    recommended_action="prefer_symbol_hour_bucket",
                    tags=["tradeability", "symbol_hour", "positive_pocket"],
                    severity_score=sev,
                    time_scope={"window": "rolling", "as_of": now_label},
                    published_keys=published_keys,
                )
            )

    for row in list(joint.get("spread_quartile_x_score_bucket") or []):
        n = int(_safe_float(row.get("n"), 0))
        exp = _safe_float(row.get("conservative_expectancy_usd"), 0.0)
        spread = str(row.get("spread_quartile", "") or "")
        bucket = str(row.get("score_bucket", "") or "")
        if n < 4:
            continue
        stop_rate = _safe_float(row.get("stop_loss_rate"), 0.0)
        if exp < 0:
            sev = _clamp((abs(exp) / 18.0) + stop_rate, 0.2, 1.0)
            signals.append(
                _build_knowledge_signal(
                    signal_type="spread_regime_failure_pattern",
                    source_type="signal_diagnostic",
                    symbol="ALL",
                    lane="market",
                    category="signal_knowledge",
                    title=f"Spread {spread} x score {bucket}: underperformance",
                    summary=(
                        f"Score bucket {bucket} under spread regime {spread} is negative after conservative accounting."
                    ),
                    evidence={"trades": n, "spread_quartile": spread, "score_bucket": bucket},
                    metrics={
                        "conservative_expectancy_usd": exp,
                        "stop_loss_rate": stop_rate,
                    },
                    recommended_action="raise_score_floor_or_block_spread_regime",
                    tags=["spread", "score_bucket", "negative_expectancy"],
                    severity_score=sev,
                    time_scope={"window": "rolling", "as_of": now_label},
                    published_keys=published_keys,
                )
            )

    stop_ticker_rows = list(joint.get("stop_loss_x_ticker") or [])
    for row in stop_ticker_rows:
        symbol = str(row.get("ticker", "") or "")
        n = int(_safe_float(row.get("n"), 0))
        if n < 3 or not symbol:
            continue
        stop_rate = _safe_float(row.get("stop_loss_rate"), 0.0)
        exp = _safe_float(row.get("conservative_expectancy_usd"), 0.0)
        if stop_rate >= 0.55:
            sev = _clamp(stop_rate + (abs(min(exp, 0.0)) / 20.0), 0.25, 1.0)
            signals.append(
                _build_knowledge_signal(
                    signal_type="stop_loss_cluster_by_symbol",
                    source_type="risk_diagnostic",
                    symbol=symbol,
                    lane="risk",
                    category="risk_knowledge",
                    title=f"Stop-loss cluster detected on {symbol}",
                    summary=f"Stop-loss exits are concentrated on {symbol}; cut symbol first before widening global stops.",
                    evidence={"trades": n},
                    metrics={"stop_loss_rate": stop_rate, "conservative_expectancy_usd": exp},
                    recommended_action="deprioritize_or_block_symbol",
                    tags=["stop_loss", "symbol_cluster", "risk"],
                    severity_score=sev,
                    time_scope={"window": "rolling", "as_of": now_label},
                    published_keys=published_keys,
                )
            )

    stop_hour_rows = list(joint.get("stop_loss_x_hour") or [])
    for row in stop_hour_rows:
        hour = str(row.get("entry_hour", "") or "")
        n = int(_safe_float(row.get("n"), 0))
        if n < 3:
            continue
        stop_rate = _safe_float(row.get("stop_loss_rate"), 0.0)
        exp = _safe_float(row.get("conservative_expectancy_usd"), 0.0)
        if stop_rate >= 0.55:
            sev = _clamp(stop_rate + (abs(min(exp, 0.0)) / 22.0), 0.25, 1.0)
            signals.append(
                _build_knowledge_signal(
                    signal_type="stop_loss_cluster_by_hour",
                    source_type="risk_diagnostic",
                    symbol="ALL",
                    lane="risk",
                    category="risk_knowledge",
                    title=f"Stop-loss cluster in hour {hour}",
                    summary="Stop-loss exits cluster in this hour; block hour first before changing stop geometry globally.",
                    evidence={"trades": n, "hour_et": hour},
                    metrics={"stop_loss_rate": stop_rate, "conservative_expectancy_usd": exp},
                    recommended_action="block_hour_globally_or_per_symbol",
                    tags=["stop_loss", "hour_cluster", "risk"],
                    severity_score=sev,
                    time_scope={"window": "rolling", "as_of": now_label},
                    published_keys=published_keys,
                )
            )

    stop_geometry = report.get("stop_loss_geometry") if isinstance(report, dict) else {}
    if isinstance(stop_geometry, dict):
        stop_count = int(_safe_float(stop_geometry.get("stop_loss_trade_count"), 0))
        cfg_stop = _safe_float(stop_geometry.get("configured_stop_loss_usd"), _safe_float(getattr(config, "STOP_LOSS_USD", 10.0), 10.0))
        est_mae = stop_geometry.get("estimated_mae_usd_1c") if isinstance(stop_geometry.get("estimated_mae_usd_1c"), dict) else {}
        med_mae = _safe_float(est_mae.get("median"), 0.0)
        if stop_count >= 5 and cfg_stop > 0 and med_mae > (cfg_stop * 1.25):
            sev = _clamp((med_mae / cfg_stop) - 1.0, 0.3, 1.0)
            signals.append(
                _build_knowledge_signal(
                    signal_type="stop_loss_geometry_misalignment",
                    source_type="risk_diagnostic",
                    symbol="ALL",
                    lane="risk",
                    category="risk_knowledge",
                    title="Empirical MAE suggests stop geometry is too tight",
                    summary=(
                        "Median estimated MAE at stop-loss exits exceeds configured stop by material margin after cluster-aware filtering."
                    ),
                    evidence={"trades": stop_count},
                    metrics={"configured_stop_loss_usd": cfg_stop, "median_estimated_mae_usd": med_mae},
                    recommended_action="consider_stop_adjustment_after_selection_cuts",
                    tags=["stop_geometry", "mae", "risk"],
                    severity_score=sev,
                    time_scope={"window": "rolling", "as_of": now_label},
                    published_keys=published_keys,
                )
            )

    bad_fill_tracker = runtime_state.get("bad_fill_tracker") if isinstance(runtime_state, dict) else {}
    if isinstance(bad_fill_tracker, dict):
        for symbol, info in bad_fill_tracker.items():
            if not isinstance(info, dict):
                continue
            count = int(_safe_float(info.get("count"), 0))
            blocked_until = str(info.get("blocked_until_iso", "") or "")
            last_slip = _safe_float(info.get("last_slippage_pct"), 0.0)
            if count <= 0 and not blocked_until:
                continue
            sev = _clamp((count * 0.2) + (last_slip / 12.0), 0.2, 0.95)
            signals.append(
                _build_knowledge_signal(
                    signal_type="execution_slippage_cluster",
                    source_type="execution_diagnostic",
                    symbol=str(symbol).upper(),
                    lane="market",
                    category="runtime_system_knowledge",
                    title=f"Bad-fill pattern observed for {str(symbol).upper()}",
                    summary="Bad-fill tracker indicates repeated or recent slippage stress for this symbol.",
                    evidence={"trades": max(1, count), "blocked_until": blocked_until},
                    metrics={"last_slippage_pct": last_slip, "bad_fill_count": count},
                    recommended_action="tighten_symbol_spread_filter_or_cooldown",
                    tags=["bad_fill", "slippage", "runtime"],
                    severity_score=sev,
                    time_scope={"window": "runtime_recent", "as_of": now_label},
                    published_keys=published_keys,
                )
            )

    signals.sort(
        key=lambda s: (
            float(s.get("severity_score", 0.0)),
            float(s.get("confidence", 0.0)),
            float(s.get("novelty", 0.0)),
        ),
        reverse=True,
    )
    return {
        "feed_name": "autobott_lisa_feed",
      "schema_version": "1.1.0",
        "generated_at": now_label,
        "source_system": "autobott",
      "data_sources": ["scanner_live_csv", "runtime_state", "trade_review", "alpaca_snapshot_api"],
      "human_briefing": human_briefing,
        "signal_count": len(signals),
        "signals": signals,
    }


def _persist_lisa_feed(payload: dict[str, Any]) -> dict[str, Any]:
    LISA_FEED_JSON_PATH.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    ndjson_lines = [json.dumps(signal, ensure_ascii=False) for signal in list(payload.get("signals") or [])]
    LISA_FEED_NDJSON_PATH.write_text("\n".join(ndjson_lines) + ("\n" if ndjson_lines else ""), encoding="utf-8")
    return {
        "json_path": str(LISA_FEED_JSON_PATH),
        "ndjson_path": str(LISA_FEED_NDJSON_PATH),
        "signal_count": int(_safe_float(payload.get("signal_count"), 0)),
    }


def _lens_confidence(score: float, rvol: float) -> float:
    return round(_clamp(0.3 + (max(0.0, score) / 12.0) + min(0.25, max(0.0, rvol - 1.0) / 4.0), 0.2, 0.95), 2)


def _tradeability_label(score: float, rvol: float) -> str:
    if score >= 8.0 and rvol >= 1.5:
      return "high"
    if score >= 6.0 and rvol >= 1.1:
      return "medium"
    return "low"


def _safe_pct_text(value: float | None) -> str:
    if value is None:
      return "n/a"
    return f"{value:+.2f}%"


def _build_internal_trader_layer() -> dict[str, Any]:
    now = _now_et()
    runtime_state = load_bot_state()
    scan_rows = _today_scan_rows()
    trade_rows = _today_trade_rows()
    scan_summary = _build_scan_report_summary(scan_rows)
    trade_summary = _build_trade_report_summary(trade_rows)
    last_entry_debug = runtime_state.get("last_entry_debug") if isinstance(runtime_state, dict) else {}
    open_trade_meta = runtime_state.get("open_trade_meta") if isinstance(runtime_state, dict) else {}

    return {
      "layer": "internal_trader",
      "generated_at": now.strftime("%Y-%m-%d %H:%M:%S ET"),
      "setup_validity": {
        "setup_valid_count": int(scan_summary.get("setup_valid_count", scan_summary.get("pass_count", 0)) or 0),
        "fail_count": int(scan_summary.get("fail_count", 0) or 0),
        "top_fail_reasons": list(scan_summary.get("top_fail_reasons") or []),
      },
      "direction_score": {
        "direction_counts": dict(scan_summary.get("direction_counts") or {}),
      },
      "entry_eligibility": {
        "signals_considered": int((last_entry_debug or {}).get("signals_considered", 0) or 0),
        "eligible_count": int((last_entry_debug or {}).get("entry_stage4_eligible_count", 0) or 0),
        "rejected_count": int((last_entry_debug or {}).get("entry_stage4_reject_count", 0) or 0),
        "rejected_reasons": dict((last_entry_debug or {}).get("entry_stage4_reject_reasons") or {}),
      },
      "execution_state": {
        "orders_submitted": int((last_entry_debug or {}).get("entry_orders_submitted", 0) or 0),
        "entries_filled": int((last_entry_debug or {}).get("entries_filled", 0) or 0),
        "skips": dict((last_entry_debug or {}).get("skips") or {}),
      },
      "order_fill_lifecycle": {
        "closed_trades_today": int(trade_summary.get("total_trades", 0) or 0),
        "wins": int(trade_summary.get("wins", 0) or 0),
        "losses": int(trade_summary.get("losses", 0) or 0),
        "realized_pnl_usd": round(float(trade_summary.get("total_pnl_usd", 0.0) or 0.0), 2),
      },
      "position_state": {
        "open_positions": len(open_trade_meta) if isinstance(open_trade_meta, dict) else 0,
        "symbols": sorted(list(open_trade_meta.keys()))[:20] if isinstance(open_trade_meta, dict) else [],
      },
      "portfolio_risk": {
        "daily_loss_limit_usd": float(getattr(config, "DAILY_LOSS_LIMIT_USD", 0.0) or 0.0),
        "max_positions": int(getattr(config, "MAX_POSITIONS", 0) or 0),
        "entry_max_spread_pct": float(getattr(config, "ENTRY_MAX_QUOTE_SPREAD_PCT", 0.0) or 0.0),
        "premium_per_trade_cap_usd": float(getattr(config, "MAX_PREMIUM_PER_TRADE_USD", 0.0) or 0.0),
      },
    }


def _build_public_livestream_layer() -> dict[str, Any]:
    now = _now_et()
    runtime_state = load_bot_state()
    scan_rows = _today_scan_rows()
    trade_rows = _today_trade_rows()
    scan_summary = _build_scan_report_summary(scan_rows)
    trade_summary = _build_trade_report_summary(trade_rows)

    symbol_candidates = [
      str(row.get("symbol", "") or "").upper()
      for row in list(scan_summary.get("top_setup_valid") or scan_summary.get("top_passes") or [])
      if str(row.get("symbol", "") or "").strip()
    ]
    symbol_candidates = [s for s in symbol_candidates if s][:6]
    if not symbol_candidates:
      symbol_candidates = ["SPY", "QQQ", "IWM"]

    snapshots = _fetch_snapshots(symbol_candidates)
    price_lens: list[dict[str, Any]] = []
    news_lens: list[dict[str, Any]] = []

    for symbol in symbol_candidates:
      matching = [r for r in scan_rows if _scan_symbol(r) == symbol]
      pass_n = sum(1 for r in matching if str(r.get("result", "") or "").lower() == "pass")
      fail_n = sum(1 for r in matching if str(r.get("result", "") or "").lower() == "fail")
      latest_row = matching[-1] if matching else {}
      score = _safe_float((latest_row or {}).get("signal_score"), 0.0)
      rvol = _safe_float((latest_row or {}).get("rvol"), 0.0)
      direction = str((latest_row or {}).get("direction", "") or "").upper()

      snap = snapshots.get(symbol, {}) if isinstance(snapshots, dict) else {}
      daily = snap.get("dailyBar", {}) if isinstance(snap, dict) else {}
      latest_trade = snap.get("latestTrade", {}) if isinstance(snap, dict) else {}
      latest_price = _safe_float(latest_trade.get("p"), _safe_float(daily.get("c"), 0.0))
      day_open = _safe_float(daily.get("o"), 0.0)
      day_high = _safe_float(daily.get("h"), 0.0)
      day_low = _safe_float(daily.get("l"), 0.0)
      move_pct = ((latest_price - day_open) / day_open * 100.0) if latest_price > 0 and day_open > 0 else None
      range_pos = ((latest_price - day_low) / (day_high - day_low) * 100.0) if latest_price > 0 and day_high > day_low else None

      bias = "neutral"
      if direction == "CALL" and pass_n >= fail_n:
        bias = "bullish pressure"
      elif direction == "PUT" and pass_n >= fail_n:
        bias = "bearish pressure"

      tradeability_quality = _tradeability_label(score, rvol)
      confidence = _lens_confidence(score, rvol)
      scanner_pressure = f"{pass_n} valid / {fail_n} rejected"

      price_lens.append(
        {
          "symbol": symbol,
          "daily_move_pct": round(move_pct, 2) if move_pct is not None else None,
          "current_price": round(latest_price, 2) if latest_price > 0 else None,
          "intraday_range_position_pct": round(range_pos, 1) if range_pos is not None else None,
          "momentum_bias": bias,
          "scanner_pressure": scanner_pressure,
          "tradeability_quality": tradeability_quality,
          "confidence": confidence,
          "summary": (
            f"{symbol} is {_safe_pct_text(move_pct)} today, trading near "
            f"{(f'{range_pos:.0f}%' if range_pos is not None else 'n/a')} of its intraday range. "
            f"Momentum bias is {bias} with {scanner_pressure}."
          ),
        }
      )

      news_state = (runtime_state.get("news_by_symbol") if isinstance(runtime_state, dict) else {}) or {}
      symbol_news = news_state.get(symbol) if isinstance(news_state, dict) else None
      if isinstance(symbol_news, dict) and str(symbol_news.get("summary", "")).strip():
        headline_summary = str(symbol_news.get("summary", "") or "")
        freshness = str(symbol_news.get("freshness", "recent") or "recent")
        impact = str(symbol_news.get("impact", "mixed") or "mixed")
        why = str(symbol_news.get("why_it_matters", "") or "")
        news_confidence = round(_clamp(float(_safe_float(symbol_news.get("confidence"), confidence)), 0.2, 0.95), 2)
      else:
        headline_summary = f"No dominant fresh headline detected for {symbol}."
        freshness = "live tape"
        impact = "flow-led"
        why = "Current movement appears driven more by price action and scanner flow than a single event."
        news_confidence = round(_clamp(confidence - 0.1, 0.2, 0.9), 2)

      news_lens.append(
        {
          "symbol": symbol,
          "headline_summary": headline_summary,
          "why_it_matters": why,
          "likely_directional_impact": impact,
          "confidence": news_confidence,
          "freshness": freshness,
        }
      )

    trade_lens: list[dict[str, Any]] = []
    for row in list(trade_rows)[-6:]:
      ticker = _trade_ticker(row)
      if not ticker:
        continue
      pnl_usd = _pnl_usd_from_trade_row(row)
      trade_lens.append(
        {
          "symbol": ticker,
          "action": "closed_trade",
          "call_or_put": str(row.get("direction", "") or "").upper(),
          "contract_snapshot": str(row.get("option_symbol", "") or ""),
          "why_taken": "Confirmed scanner pressure with execution-quality checks.",
          "system_view": f"score={_safe_float(row.get('signal_score'), 0.0):.2f}, rvol={_safe_float(row.get('rvol'), 0.0):.2f}",
          "still_open": False,
          "closed_result": {
            "pnl_usd": round(pnl_usd, 2),
            "pnl_pct": round(_safe_float(row.get("pnl_pct"), 0.0) * 100.0, 2),
            "exit_reason": str(row.get("exit_reason", "") or ""),
          },
          "timestamp": str(row.get("timestamp", "") or ""),
        }
      )

    open_trade_meta = runtime_state.get("open_trade_meta") if isinstance(runtime_state, dict) else {}
    if isinstance(open_trade_meta, dict):
      for symbol, meta in list(open_trade_meta.items())[:6]:
        ticker = str((meta or {}).get("ticker", "") or "").upper() or _extract_underlying(str(symbol))
        trade_lens.append(
          {
            "symbol": ticker,
            "action": "position_active",
            "call_or_put": str((meta or {}).get("direction", "") or "").upper(),
            "contract_snapshot": str(symbol),
            "why_taken": "Active position from validated setup.",
            "system_view": f"state={str((meta or {}).get('trade_state', 'unproven') or 'unproven')}",
            "still_open": True,
            "closed_result": None,
            "timestamp": str((meta or {}).get("timestamp", "") or ""),
          }
        )

    indices = _fetch_snapshots(["SPY", "QQQ", "IWM"])
    idx_moves: dict[str, float] = {}
    for idx in ("SPY", "QQQ", "IWM"):
      snap = indices.get(idx, {}) if isinstance(indices, dict) else {}
      daily = snap.get("dailyBar", {}) if isinstance(snap, dict) else {}
      latest = _safe_float((snap.get("latestTrade") or {}).get("p"), _safe_float(daily.get("c"), 0.0))
      day_open = _safe_float(daily.get("o"), 0.0)
      if latest > 0 and day_open > 0:
        idx_moves[idx] = round(((latest - day_open) / day_open) * 100.0, 2)

    pos_idx = sum(1 for v in idx_moves.values() if v > 0)
    neg_idx = sum(1 for v in idx_moves.values() if v < 0)
    if pos_idx >= 2:
      regime_label = "trend_up"
      regime_text = "broad risk-on trend"
    elif neg_idx >= 2:
      regime_label = "trend_down"
      regime_text = "broad risk-off tape"
    else:
      regime_label = "chop"
      regime_text = "mixed/choppy session"

    market_lens = {
      "index_trend": idx_moves,
      "breadth": f"{pos_idx} rising / {neg_idx} falling major indices",
      "sectors_leading_lagging": "Tech/growth proxies leading when QQQ outperforms; defensives leading when IWM lags.",
      "volatility_regime": regime_text,
      "summary": f"Market lens: {regime_text}; index moves {idx_moves}.",
    }

    regime_lens = {
      "regime": regime_label,
      "language": regime_text,
      "confidence": round(_clamp(0.35 + (abs(sum(idx_moves.values())) / 6.0), 0.2, 0.9), 2),
    }

    setup_valid = int(scan_summary.get("setup_valid_count", scan_summary.get("pass_count", 0)) or 0)
    bot_lens = {
      "watching_symbols": len({_scan_symbol(r) for r in scan_rows if _scan_symbol(r)}),
      "trade_ready": setup_valid,
      "active_positions": len(open_trade_meta) if isinstance(open_trade_meta, dict) else 0,
      "strongest_pressure_symbols": [str(item.get("symbol", "") or "") for item in list(scan_summary.get("top_setup_valid") or [])[:3]],
      "risk_mode": "reduced" if regime_label == "chop" else "normal",
      "daily_pnl_usd": round(float(trade_summary.get("total_pnl_usd", 0.0) or 0.0), 2),
      "summary": (
        f"Bot is watching {len({_scan_symbol(r) for r in scan_rows if _scan_symbol(r)})} symbols, "
        f"{setup_valid} trade-ready, {len(open_trade_meta) if isinstance(open_trade_meta, dict) else 0} active positions."
      ),
    }

    rotation_order = ["price_lens", "news_lens", "trade_lens", "market_lens", "regime_lens", "bot_lens"]
    rotation_idx = now.minute % len(rotation_order)
    rotation_focus = rotation_order[rotation_idx]

    return {
      "layer": "public_market_intelligence",
      "generated_at": now.strftime("%Y-%m-%d %H:%M:%S ET"),
      "rotation_focus": rotation_focus,
      "price_lens": price_lens,
      "news_lens": news_lens,
      "trade_lens": trade_lens,
      "market_lens": market_lens,
      "regime_lens": regime_lens,
      "bot_lens": bot_lens,
    }


def _build_lisa_ingestion_layer(public_layer: dict[str, Any], internal_layer: dict[str, Any]) -> dict[str, Any]:
    now = _now_et()
    scan_rows = _today_scan_rows()
    trade_rows = _today_trade_rows()
    runtime_state = load_bot_state()
    published_keys = _published_signal_keys()

    symbol_state_packets: list[dict[str, Any]] = []
    direction_conflict_packets: list[dict[str, Any]] = []
    fail_family_counts: dict[str, int] = {}
    symbol_direction: dict[str, dict[str, int]] = {}

    for row in scan_rows:
      symbol = _scan_symbol(row)
      if not symbol:
        continue
      result = str(row.get("result", "") or "").lower()
      direction = str(row.get("direction", "") or "").upper()
      d = symbol_direction.setdefault(symbol, {"CALL": 0, "PUT": 0, "PASS": 0, "FAIL": 0})
      if result == "pass":
        d["PASS"] += 1
        if direction in ("CALL", "PUT"):
          d[direction] += 1
      elif result == "fail":
        d["FAIL"] += 1
        family = _scan_fail_family(str(row.get("reason", "") or "unknown"))
        fail_family_counts[family] = fail_family_counts.get(family, 0) + 1

    for symbol, d in sorted(symbol_direction.items(), key=lambda item: (item[1].get("PASS", 0), -item[1].get("FAIL", 0)), reverse=True)[:20]:
      has_conflict = d.get("CALL", 0) > 0 and d.get("PUT", 0) > 0
      top_state = "CALL" if d.get("CALL", 0) >= d.get("PUT", 0) else "PUT"
      packet_id = _signal_key("symbol_state_packet", symbol, "current_state")
      symbol_state_packets.append(
        {
          "packet_id": packet_id,
          "packet_type": "symbol_state_packet",
          "symbol": symbol,
          "current_direction": top_state,
          "pass_count": int(d.get("PASS", 0)),
          "fail_count": int(d.get("FAIL", 0)),
          "direction_conflict": has_conflict,
          "generated_at": now.strftime("%Y-%m-%d %H:%M:%S ET"),
        }
      )
      if has_conflict:
        direction_conflict_packets.append(
          {
            "packet_id": _signal_key("direction_conflict_packet", symbol, "direction_conflict"),
            "packet_type": "direction_conflict_packet",
            "symbol": symbol,
            "counts": {"CALL": int(d.get("CALL", 0)), "PUT": int(d.get("PUT", 0))},
            "generated_at": now.strftime("%Y-%m-%d %H:%M:%S ET"),
          }
        )

    fail_summary_packet = {
      "packet_id": _signal_key("fail_summary_packet", "ALL", "fail_family_grouped"),
      "packet_type": "fail_summary_packet",
      "fail_family_counts": dict(sorted(fail_family_counts.items(), key=lambda item: item[1], reverse=True)[:10]),
      "generated_at": now.strftime("%Y-%m-%d %H:%M:%S ET"),
    }

    trade_event_packets: list[dict[str, Any]] = []
    for row in list(trade_rows)[-20:]:
      symbol = _trade_ticker(row)
      if not symbol:
        continue
      title = f"{symbol}|{row.get('timestamp', '')}|{row.get('exit_reason', '')}"
      trade_event_packets.append(
        {
          "packet_id": _signal_key("trade_event_packet", symbol, title),
          "packet_type": "trade_event_packet",
          "symbol": symbol,
          "direction": str(row.get("direction", "") or "").upper(),
          "option_symbol": str(row.get("option_symbol", "") or ""),
          "entry_price": round(_safe_float(row.get("entry_price"), 0.0), 4),
          "exit_price": round(_safe_float(row.get("exit_price"), 0.0), 4),
          "pnl_pct": round(_safe_float(row.get("pnl_pct"), 0.0) * 100.0, 2),
          "exit_reason": str(row.get("exit_reason", "") or ""),
          "timestamp": str(row.get("timestamp", "") or ""),
        }
      )

    news_packets: list[dict[str, Any]] = []
    for row in list(public_layer.get("news_lens") or [])[:20]:
      symbol = str(row.get("symbol", "") or "")
      if not symbol:
        continue
      news_packets.append(
        {
          "packet_id": _signal_key("news_packet", symbol, str(row.get("headline_summary", "") or "")),
          "packet_type": "news_packet",
          "symbol": symbol,
          "headline_summary": str(row.get("headline_summary", "") or ""),
          "impact": str(row.get("likely_directional_impact", "") or ""),
          "freshness": str(row.get("freshness", "") or ""),
          "confidence": float(_safe_float(row.get("confidence"), 0.0)),
        }
      )

    market_lens = public_layer.get("market_lens") if isinstance(public_layer, dict) else {}
    regime_lens = public_layer.get("regime_lens") if isinstance(public_layer, dict) else {}
    market_regime_packet = {
      "packet_id": _signal_key("market_regime_packet", "ALL", str(regime_lens.get("regime", "chop") or "chop")),
      "packet_type": "market_regime_packet",
      "regime": str(regime_lens.get("regime", "chop") or "chop"),
      "volatility_regime": str((market_lens or {}).get("volatility_regime", "mixed") or "mixed"),
      "index_trend": dict((market_lens or {}).get("index_trend") or {}),
      "confidence": float(_safe_float(regime_lens.get("confidence"), 0.0)),
      "generated_at": now.strftime("%Y-%m-%d %H:%M:%S ET"),
    }

    all_packets = symbol_state_packets + direction_conflict_packets + [fail_summary_packet] + trade_event_packets + news_packets + [market_regime_packet]
    published = set(published_keys)
    deduped_packets = [p for p in all_packets if str(p.get("packet_id", "")) not in published]

    return {
      "layer": "lisa_ingestion",
      "schema_version": "2.0.0",
      "generated_at": now.strftime("%Y-%m-%d %H:%M:%S ET"),
      "packet_count": len(deduped_packets),
      "packets": deduped_packets,
      "channels": {
        "symbol_state_packet": len(symbol_state_packets),
        "direction_conflict_packet": len(direction_conflict_packets),
        "trade_event_packet": len(trade_event_packets),
        "news_packet": len(news_packets),
        "market_regime_packet": 1,
      },
      "internal_snapshot": {
        "entry_eligibility": dict((internal_layer.get("entry_eligibility") if isinstance(internal_layer, dict) else {}) or {}),
        "execution_state": dict((internal_layer.get("execution_state") if isinstance(internal_layer, dict) else {}) or {}),
      },
    }


def _build_three_layer_payload() -> dict[str, Any]:
    internal_layer = _build_internal_trader_layer()
    public_layer = _build_public_livestream_layer()
    lisa_layer = _build_lisa_ingestion_layer(public_layer, internal_layer)
    return {
      "generated_at": _now_et().strftime("%Y-%m-%d %H:%M:%S ET"),
      "layers": {
        "internal_trader": internal_layer,
        "public_market_intelligence": public_layer,
        "lisa_ingestion": lisa_layer,
      },
    }


def _filtered_payload_delta(payload: dict[str, Any]) -> dict[str, Any]:
    published = _published_signal_keys()
    if not published:
        return payload
    rows = list(payload.get("signals") or [])
    filtered = [row for row in rows if str(row.get("signal_id", "")) not in published]
    out = dict(payload)
    out["signals"] = filtered
    out["signal_count"] = len(filtered)
    out["delta_from_last_publish"] = True
    return out


def _rows_with_min_n(rows: list[dict[str, Any]], min_n: int) -> int:
    total = 0
    for row in rows:
        if int(_safe_float(row.get("n"), 0)) >= int(min_n):
            total += 1
    return total


def _negative_cluster_count(rows: list[dict[str, Any]], *, min_n: int = 6) -> int:
    total = 0
    for row in rows:
        n = int(_safe_float(row.get("n"), 0))
        exp = _safe_float(row.get("conservative_expectancy_usd"), 0.0)
        if n >= min_n and exp < 0:
            total += 1
    return total


def _selection_controls_active() -> bool:
    blocked_hours = list(getattr(config, "ENTRY_BLOCKED_HOURS_ET", ()) or ())
    min_signal = float(_safe_float(getattr(config, "MIN_SIGNAL_SCORE", 5.0), 5.0))
    max_spread = float(_safe_float(getattr(config, "ENTRY_MAX_QUOTE_SPREAD_PCT", 18.0), 18.0))
    return bool(blocked_hours) or min_signal > 5.0 or max_spread < 18.0


def _build_roadmap_status_payload() -> dict[str, Any]:
    report = _load_latest_trade_report()
    runtime_state = load_bot_state()
    lisa_feed_exists = LISA_FEED_JSON_PATH.exists()
    lisa_publish_exists = LISA_FEED_PUBLISHED_PATH.exists()

    metadata = report.get("metadata") if isinstance(report, dict) else {}
    overall = report.get("overall") if isinstance(report, dict) else {}
    joint = report.get("joint_tradeability") if isinstance(report, dict) else {}
    stop = report.get("stop_loss_geometry") if isinstance(report, dict) else {}

    closed_trades = int(_safe_float((metadata or {}).get("closed_trade_count"), 0))
    by_ticker = list(report.get("by_ticker") or []) if isinstance(report, dict) else []
    by_hour = list(report.get("by_entry_hour_et") or []) if isinstance(report, dict) else []
    ticker_min_n = _rows_with_min_n(by_ticker, 8)
    hour_min_n = _rows_with_min_n(by_hour, 6)

    conservative_exp = _safe_float((overall or {}).get("conservative_expectancy_usd"), None)
    ticker_hour_rows = list((joint or {}).get("ticker_x_hour", [])) if isinstance(joint, dict) else []
    negative_clusters = _negative_cluster_count(ticker_hour_rows, min_n=6)
    stop_count = int(_safe_float((stop or {}).get("stop_loss_trade_count"), 0))

    phase1_complete = closed_trades >= 60 and ticker_min_n >= 3 and hour_min_n >= 3
    controls_active = _selection_controls_active()
    phase2_complete = phase1_complete and controls_active

    if phase1_complete:
        phase1_status = "completed"
    elif closed_trades > 0:
        phase1_status = "in_progress"
    else:
        phase1_status = "not_started"

    if phase2_complete:
        phase2_status = "completed"
    elif phase1_complete:
        phase2_status = "in_progress"
    else:
        phase2_status = "blocked"

    if phase2_complete and stop_count >= 20:
        phase3_status = "in_progress"
    elif phase2_complete:
        phase3_status = "not_started"
    else:
        phase3_status = "blocked"

    if lisa_feed_exists and lisa_publish_exists:
        phase4_status = "completed"
    elif lisa_feed_exists:
        phase4_status = "in_progress"
    else:
        phase4_status = "not_started"

    promotion_ready = bool(
        conservative_exp is not None and conservative_exp > 0 and closed_trades >= 100 and negative_clusters <= 2
    )
    if promotion_ready:
        phase5_status = "completed"
    elif closed_trades >= 60:
        phase5_status = "in_progress"
    else:
        phase5_status = "blocked"

    phases = [
        {
            "id": "phase1_data_accrual_freeze",
            "title": "Phase 1: Data Accrual Freeze",
            "status": phase1_status,
            "checklist": [
                {"item": "Closed trades >= 60", "done": closed_trades >= 60},
                {"item": "Ticker buckets with n>=8 >= 3", "done": ticker_min_n >= 3},
                {"item": "Hour buckets with n>=6 >= 3", "done": hour_min_n >= 3},
                {
                    "item": "Keep qty=1 and avoid order-logic changes",
                    "done": True,
                    "note": "Validation discipline rule",
                },
            ],
        },
        {
            "id": "phase2_selection_cuts",
            "title": "Phase 2: Selection-Layer Cuts Only",
            "status": phase2_status,
            "checklist": [
                {"item": "Phase 1 complete", "done": phase1_complete},
                {"item": "Selection controls activated", "done": controls_active},
                {
                    "item": "Cuts prioritized by hour -> spread -> symbol -> score bucket",
                    "done": controls_active,
                },
            ],
        },
        {
            "id": "phase3_stop_geometry_decision",
            "title": "Phase 3: Cluster-Aware Stop Geometry Decision",
            "status": phase3_status,
            "checklist": [
                {"item": "Phase 2 complete", "done": phase2_complete},
                {"item": "Stop-loss sample >= 20 trades", "done": stop_count >= 20},
                {
                    "item": "Adjust STOP_LOSS_USD only if clusters persist post-cuts",
                    "done": False,
                },
            ],
        },
        {
            "id": "phase4_lisa_operationalization",
            "title": "Phase 4: LISA Feed Operationalization",
            "status": phase4_status,
            "checklist": [
                {"item": "Feed JSON generated", "done": lisa_feed_exists},
                {"item": "Feed published snapshot exists", "done": lisa_publish_exists},
                {"item": "Delta workflow available", "done": True},
            ],
        },
        {
            "id": "phase5_promotion_gate",
            "title": "Phase 5: Promotion Gate",
            "status": phase5_status,
            "checklist": [
                {
                    "item": "Conservative expectancy positive",
                    "done": conservative_exp is not None and conservative_exp > 0,
                },
                {"item": "Closed trades >= 100", "done": closed_trades >= 100},
                {"item": "Negative symbol-hour clusters <= 2", "done": negative_clusters <= 2},
            ],
        },
    ]

    next_actions: list[str] = []
    if phase1_status != "completed":
        next_actions.append("Accrue more closed trades before applying aggressive selection cuts.")
    elif phase2_status != "completed":
        next_actions.append("Apply only selection-layer controls: worst hours, spread regimes, symbols, score buckets.")
    elif phase3_status != "completed":
        next_actions.append("Evaluate stop-loss geometry only after post-cut cluster persistence is confirmed.")
    elif phase4_status != "completed":
        next_actions.append("Generate and publish LISA feed packets on a repeatable cadence.")
    else:
        next_actions.append("Track promotion metrics weekly; avoid order-logic churn until gate is reached.")

    return {
        "roadmap_version": "v1",
        "generated_at": _now_et().strftime("%Y-%m-%d %H:%M:%S ET"),
        "metrics": {
            "closed_trades": closed_trades,
            "conservative_expectancy_usd": conservative_exp,
            "ticker_buckets_n_ge_8": ticker_min_n,
            "hour_buckets_n_ge_6": hour_min_n,
            "negative_symbol_hour_clusters": negative_clusters,
            "stop_loss_trade_count": stop_count,
            "selection_controls_active": controls_active,
        },
        "phases": phases,
        "next_actions": next_actions,
        "runtime_hints": {
            "strategy_profile": str(runtime_state.get("strategy_profile", "balanced") or "balanced"),
            "manual_stop": bool(runtime_state.get("manual_stop", False)),
            "dry_run": bool(runtime_state.get("dry_run", False)),
        },
    }


@app.get("/api/lisa/feed")
def api_lisa_feed():
    try:
        payload = _synthesize_lisa_signals()
        if str(request.args.get("delta", "0")) == "1":
            payload = _filtered_payload_delta(payload)
        if str(request.args.get("persist", "0")) == "1":
            _persist_lisa_feed(payload)
        return jsonify(payload)
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@app.get("/api/layers/all")
def api_layers_all():
    try:
        return jsonify(_build_three_layer_payload())
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@app.get("/api/layer/internal")
def api_layer_internal():
    try:
        return jsonify(_build_internal_trader_layer())
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@app.get("/api/layer/public")
def api_layer_public():
    try:
        return jsonify(_build_public_livestream_layer())
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@app.get("/api/layer/lisa-ingestion")
def api_layer_lisa_ingestion():
    try:
        internal_layer = _build_internal_trader_layer()
        public_layer = _build_public_livestream_layer()
        return jsonify(_build_lisa_ingestion_layer(public_layer, internal_layer))
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@app.get("/api/roadmap-status")
def api_roadmap_status():
    try:
      return jsonify(_build_roadmap_status_payload())
    except Exception as exc:  # noqa: BLE001
      return jsonify({"error": str(exc)}), 500


@app.post("/api/lisa/feed/generate")
def api_lisa_feed_generate():
    try:
        ok, err, status = _verify_control_token()
        if not ok:
            return jsonify({"error": err}), status
        payload = _synthesize_lisa_signals()
        if str(request.args.get("delta", "0")) == "1":
            payload = _filtered_payload_delta(payload)
        details = _persist_lisa_feed(payload)
        return jsonify({"ok": True, "details": details, "generated_at": payload.get("generated_at")})
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@app.get("/api/lisa/feed/export")
def api_lisa_feed_export():
    try:
        fmt = str(request.args.get("format", "json") or "json").lower()
        payload = _synthesize_lisa_signals()
        if str(request.args.get("delta", "0")) == "1":
            payload = _filtered_payload_delta(payload)

        if fmt == "ndjson":
            body = "\n".join(json.dumps(row, ensure_ascii=False) for row in list(payload.get("signals") or []))
            if body:
                body += "\n"
            response = Response(body, mimetype="application/x-ndjson")
            response.headers["Content-Disposition"] = "attachment; filename=autobott_lisa_feed.ndjson"
            return response

        body = json.dumps(payload, indent=2)
        response = Response(body + "\n", mimetype="application/json")
        response.headers["Content-Disposition"] = "attachment; filename=autobott_lisa_feed.json"
        return response
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@app.post("/api/lisa/feed/publish")
def api_lisa_feed_publish():
    try:
        ok, err, status = _verify_control_token()
        if not ok:
            return jsonify({"error": err}), status
        payload = _synthesize_lisa_signals()
        payload = _filtered_payload_delta(payload) if str(request.args.get("delta", "0")) == "1" else payload
        _persist_lisa_feed(payload)
        signal_keys = [str(row.get("signal_id", "")) for row in list(payload.get("signals") or []) if str(row.get("signal_id", "")).strip()]
        publish_payload = {
            "published_at": _now_et().strftime("%Y-%m-%d %H:%M:%S ET"),
            "signal_count": len(signal_keys),
            "signal_keys": signal_keys,
        }
        LISA_FEED_PUBLISHED_PATH.write_text(json.dumps(publish_payload, indent=2) + "\n", encoding="utf-8")
        return jsonify({"ok": True, "published": publish_payload})
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@app.get("/lisa-feed")
def lisa_feed_page():
    return render_template_string(
        """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>LISA Feed</title>
  <style>
    :root {
      --bg:#081019;
      --panel:rgba(14,23,35,.88);
      --panel-strong:rgba(21,34,51,.96);
      --text:#ebf3fb;
      --muted:#95abc1;
      --border:rgba(141,172,206,.25);
      --green:#25d366;
      --red:#ff5d66;
      --yellow:#ffbf4a;
      --cyan:#31cbff;
      --radius:16px;
      --shadow:0 14px 34px rgba(2,8,20,.45);
    }
    * { box-sizing:border-box; }
    body {
      margin:0;
      color:var(--text);
      font-family:"Avenir Next","Nunito Sans","Segoe UI",Tahoma,sans-serif;
      background:
        radial-gradient(1200px 500px at -10% -20%, rgba(49,203,255,.16), transparent 45%),
        radial-gradient(900px 420px at 110% -10%, rgba(37,211,102,.12), transparent 45%),
        linear-gradient(145deg, #050a11 0%, #0b1524 45%, #0a111d 100%);
      min-height:100vh;
    }
    .wrap { max-width:1320px; margin:0 auto; padding:16px; }
    .header { display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap; margin-bottom:14px; }
    .title { font-size:clamp(20px, 2.5vw, 31px); font-weight:800; line-height:1.1; }
    .muted { color:var(--muted); }
    .actions { display:flex; gap:8px; flex-wrap:wrap; }
    .btn {
      border:1px solid var(--border);
      border-radius:12px;
      padding:10px 12px;
      color:var(--text);
      text-decoration:none;
      font-weight:700;
      background:rgba(127,156,191,.15);
      cursor:pointer;
    }
    .grid { display:grid; grid-template-columns:repeat(3, minmax(0, 1fr)); gap:12px; margin-bottom:12px; }
    .card {
      background:var(--panel);
      border:1px solid var(--border);
      border-radius:var(--radius);
      padding:14px;
      box-shadow:var(--shadow);
      backdrop-filter:blur(4px);
    }
    .card.strong { background:var(--panel-strong); }
    .pill {
      border:1px solid var(--border);
      border-radius:999px;
      padding:3px 8px;
      font-size:11px;
      text-transform:uppercase;
      letter-spacing:.4px;
    }
    .sev-high { color:var(--red); border-color:rgba(255,93,102,.45); }
    .sev-medium { color:var(--yellow); border-color:rgba(255,191,74,.45); }
    .sev-low { color:var(--green); border-color:rgba(37,211,102,.45); }
    .toolbar { display:flex; flex-wrap:wrap; gap:8px; margin-bottom:10px; }
    .cols { display:grid; grid-template-columns:repeat(2, minmax(0, 1fr)); gap:12px; }
    .signal-list { display:grid; gap:8px; }
    .signal-card {
      border:1px solid var(--border);
      border-radius:12px;
      padding:10px;
      background:rgba(7,14,24,.56);
    }
    .signal-head { display:flex; justify-content:space-between; gap:8px; align-items:flex-start; margin-bottom:6px; }
    .signal-title { font-weight:800; }
    .brief-list { display:grid; gap:6px; }
    .brief-item {
      border:1px solid var(--border);
      border-radius:10px;
      background:rgba(6,12,20,.52);
      padding:8px 10px;
      font-size:13px;
    }
    .metric-row { display:flex; flex-wrap:wrap; gap:8px; margin-top:6px; }
    .metric-chip {
      border:1px solid var(--border);
      border-radius:999px;
      padding:3px 8px;
      font-size:12px;
      color:var(--muted);
      background:rgba(18,29,43,.6);
    }
    textarea {
      width:100%;
      min-height:380px;
      border:1px solid var(--border);
      border-radius:12px;
      background:rgba(5,10,17,.7);
      color:var(--text);
      padding:10px;
      font-family:ui-monospace,SFMono-Regular,Menlo,monospace;
      font-size:12px;
      resize:vertical;
    }
    .small { font-size:12px; }
    @media (max-width: 980px) {
      .grid { grid-template-columns:1fr; }
      .cols { grid-template-columns:1fr; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="header">
      <div>
        <div class="title">LISA Feed</div>
        <div class="muted">AutoBott knowledge synthesis feed for upstream LISA ingestion.</div>
      </div>
      <div class="actions">
        <a class="btn" href="/">Dashboard</a>
        <a class="btn" href="/roadmap">Roadmap</a>
        <a class="btn" href="/reports">Reports</a>
      </div>
    </div>

    <div class="toolbar">
      <button class="btn" onclick="generateFeed()">Generate Latest Feed</button>
      <button class="btn" onclick="downloadJson()">Export JSON</button>
      <button class="btn" onclick="downloadNdjson()">Export NDJSON</button>
      <button class="btn" onclick="savePacket()">Save Latest Packet To Disk</button>
      <button class="btn" onclick="publishFeed()">Mark As Published</button>
      <button class="btn" onclick="refreshFeed(true)">Load Unpublished Only</button>
    </div>

    <div class="grid">
      <div class="card strong"><div class="muted small">Feed Name</div><div id="feed-name">--</div></div>
      <div class="card strong"><div class="muted small">Generated At</div><div id="feed-time">--</div></div>
      <div class="card strong"><div class="muted small">Signal Count</div><div id="feed-count">0</div></div>
    </div>

    <div class="card" style="margin-bottom:12px;">
      <div class="muted" style="margin-bottom:8px;">Human Briefing — Price + Scanner Context</div>
      <div id="human-briefing" class="brief-list"></div>
    </div>

    <div class="cols">
      <div class="card">
        <div class="muted" style="margin-bottom:8px;">Section A — LISA-ready signals</div>
        <div id="signals" class="signal-list"></div>
      </div>

      <div class="card">
        <div class="muted" style="margin-bottom:8px;">Section B — Raw machine payload</div>
        <textarea id="payload" readonly></textarea>
      </div>
    </div>
  </div>

  <script>
    let latestPayload = null;

    function esc(v) {
      return String(v || "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/\"/g, "&quot;")
        .replace(/'/g, "&#39;");
    }

    async function fetchJson(url, opts) {
      try {
        const res = await fetch(url, opts || {});
        const body = await res.json();
        if (!res.ok) return { error: body.error || "request failed" };
        return body;
      } catch {
        return { error: "request failed" };
      }
    }

    function severityClass(sev) {
      const v = String(sev || "").toLowerCase();
      if (v === "high") return "sev-high";
      if (v === "medium") return "sev-medium";
      return "sev-low";
    }

    function metricPairs(metrics) {
      const src = metrics && typeof metrics === "object" ? metrics : {};
      const keys = Object.keys(src).slice(0, 4);
      if (!keys.length) return "";
      return keys.map((k) => `${esc(k)}: ${esc(src[k])}`).join(" | ");
    }

    function asNum(v) {
      const n = Number(v);
      return Number.isFinite(n) ? n : null;
    }

    function fmtUsd(v) {
      const n = asNum(v);
      if (n === null) return "n/a";
      return `$${n.toFixed(2)}`;
    }

    function fmtPct(v) {
      const n = asNum(v);
      if (n === null) return "n/a";
      return `${n >= 0 ? "+" : ""}${n.toFixed(2)}%`;
    }

    function metricChip(label, value) {
      return `<span class="metric-chip">${esc(label)}: ${esc(value)}</span>`;
    }

    function renderBriefing(payload) {
      const wrap = document.getElementById("human-briefing");
      if (!wrap) return;
      const lines = Array.isArray(payload && payload.human_briefing) ? payload.human_briefing : [];
      if (!lines.length) {
        wrap.innerHTML = '<div class="muted">No human briefing lines available yet.</div>';
        return;
      }
      wrap.innerHTML = lines.map((line) => `<div class="brief-item">${esc(line)}</div>`).join("");
    }

    function renderSignals(payload) {
      const wrap = document.getElementById("signals");
      if (!wrap) return;
      const rows = Array.isArray(payload && payload.signals) ? payload.signals : [];
      if (!rows.length) {
        wrap.innerHTML = '<div class="muted">No knowledge signals available.</div>';
        return;
      }
      const ordered = rows.slice().sort((a, b) => {
        const aPrice = String(a && a.category || "") === "price_intelligence" ? 1 : 0;
        const bPrice = String(b && b.category || "") === "price_intelligence" ? 1 : 0;
        if (aPrice !== bPrice) return bPrice - aPrice;
        return Number(b && b.severity_score || 0) - Number(a && a.severity_score || 0);
      });
      wrap.innerHTML = ordered.map((row) => {
        const metrics = row && typeof row.metrics === "object" ? row.metrics : {};
        const isPrice = String(row.category || "") === "price_intelligence";
        const coreMetrics = isPrice
          ? `
            <div class="metric-row">
              ${metricChip("price", fmtUsd(metrics.latest_price))}
              ${metricChip("day", fmtPct(metrics.day_move_pct))}
              ${metricChip("range pos", asNum(metrics.range_position_pct) === null ? "n/a" : `${Number(metrics.range_position_pct).toFixed(0)}%`)}
              ${metricChip("scanner", `${Number(metrics.scanner_pass_count || 0)}p/${Number(metrics.scanner_fail_count || 0)}f`)}
            </div>
          `
          : `<div class="small muted">${metricPairs(metrics)}</div>`;
        return `
        <div class="signal-card">
          <div class="signal-head">
            <div>
              <div class="signal-title">${esc(row.title)}</div>
              <div class="small muted">${esc(row.category)} | ${esc(row.symbol)} | ${esc(row.signal_type)}</div>
            </div>
            <span class="pill ${severityClass(row.severity)}">${esc(row.severity)}</span>
          </div>
          <div class="small" style="margin-bottom:4px;">${esc(row.summary)}</div>
          <div class="small muted" style="margin-bottom:4px;">confidence=${esc(row.confidence)} | novelty=${esc(row.novelty)} | action=${esc(row.recommended_action)} | source=${esc(row.source_type || "-")}</div>
          ${coreMetrics}
        </div>
      `;
      }).join("");
    }

    function renderPayload(payload) {
      latestPayload = payload;
      const feedName = document.getElementById("feed-name");
      const feedTime = document.getElementById("feed-time");
      const feedCount = document.getElementById("feed-count");
      const payloadBox = document.getElementById("payload");
      if (feedName) feedName.textContent = payload && payload.feed_name ? payload.feed_name : "--";
      if (feedTime) feedTime.textContent = payload && payload.generated_at ? payload.generated_at : "--";
      if (feedCount) feedCount.textContent = String(payload && payload.signal_count ? payload.signal_count : 0);
      if (payloadBox) payloadBox.value = JSON.stringify(payload || {}, null, 2);
      renderBriefing(payload || {});
      renderSignals(payload || {});
    }

    async function refreshFeed(deltaOnly) {
      const data = await fetchJson(`/api/lisa/feed${deltaOnly ? "?delta=1" : ""}`);
      if (data && !data.error) {
        renderPayload(data);
      }
    }

    async function generateFeed() {
      const data = await fetchJson("/api/lisa/feed/generate", { method: "POST" });
      if (data && !data.error) {
        await refreshFeed(false);
      }
    }

    function downloadJson() {
      window.open("/api/lisa/feed/export?format=json", "_blank");
    }

    function downloadNdjson() {
      window.open("/api/lisa/feed/export?format=ndjson", "_blank");
    }

    async function savePacket() {
      const data = await fetchJson("/api/lisa/feed/generate", { method: "POST" });
      if (data && !data.error) {
        await refreshFeed(false);
      }
    }

    async function publishFeed() {
      const data = await fetchJson("/api/lisa/feed/publish", { method: "POST" });
      if (data && !data.error) {
        await refreshFeed(false);
      }
    }

    refreshFeed(false);
  </script>
</body>
</html>
        """
    )


@app.get("/roadmap")
def roadmap_page():
    return render_template_string(
        """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>AutoBott Roadmap</title>
  <style>
    :root {
      --bg:#081019; --panel:rgba(14,23,35,.88); --text:#ebf3fb; --muted:#95abc1;
      --border:rgba(141,172,206,.25); --radius:16px; --shadow:0 14px 34px rgba(2,8,20,.45);
      --green:#25d366; --yellow:#ffbf4a; --red:#ff5d66;
    }
    body { margin:0; color:var(--text); font-family:"Avenir Next","Nunito Sans","Segoe UI",Tahoma,sans-serif;
      background:linear-gradient(145deg, #050a11 0%, #0b1524 45%, #0a111d 100%); min-height:100vh; }
    .wrap { max-width:1200px; margin:0 auto; padding:16px; }
    .header { display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap; margin-bottom:14px; }
    .title { font-size:clamp(20px, 2.4vw, 30px); font-weight:800; }
    .btn { border:1px solid var(--border); border-radius:12px; padding:10px 12px; color:var(--text);
      text-decoration:none; font-weight:700; background:rgba(127,156,191,.15); }
    .grid { display:grid; grid-template-columns:repeat(2, minmax(0, 1fr)); gap:12px; }
    .card { background:var(--panel); border:1px solid var(--border); border-radius:var(--radius); padding:12px; box-shadow:var(--shadow); }
    .muted { color:var(--muted); }
    .phase { border:1px solid var(--border); border-radius:12px; padding:10px; margin-bottom:8px; background:rgba(7,14,24,.56); }
    .head { display:flex; justify-content:space-between; align-items:center; gap:8px; margin-bottom:6px; }
    .pill { border:1px solid var(--border); border-radius:999px; padding:3px 8px; font-size:11px; text-transform:uppercase; }
    .done { color:var(--green); }
    .todo { color:var(--muted); }
    .st-completed { color:var(--green); border-color:rgba(37,211,102,.45); }
    .st-in_progress { color:var(--yellow); border-color:rgba(255,191,74,.45); }
    .st-blocked, .st-not_started { color:var(--red); border-color:rgba(255,93,102,.45); }
    @media (max-width: 980px) { .grid { grid-template-columns:1fr; } }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="header">
      <div>
        <div class="title">Roadmap Status</div>
        <div class="muted">Validation-to-deployment checklist with live completion state.</div>
      </div>
      <div style="display:flex; gap:8px; flex-wrap:wrap;">
        <a class="btn" href="/">Dashboard</a>
        <a class="btn" href="/lisa-feed">LISA Feed</a>
      </div>
    </div>

    <div class="grid">
      <div class="card">
        <div class="muted" style="margin-bottom:8px;">Phase Checklist</div>
        <div id="phases">Loading...</div>
      </div>
      <div class="card">
        <div class="muted" style="margin-bottom:8px;">Metrics</div>
        <pre id="metrics" style="white-space:pre-wrap; margin:0; font-family:ui-monospace,SFMono-Regular,Menlo,monospace; font-size:12px;">--</pre>
        <div class="muted" style="margin:10px 0 6px;">Next Actions</div>
        <div id="next-actions">--</div>
      </div>
    </div>
  </div>
  <script>
    function esc(v){ return String(v||"").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;"); }
    function cls(st){ return `pill st-${String(st||"")}`; }
    async function load(){
      try {
        const r = await fetch('/api/roadmap-status');
        const d = await r.json();
        if(!r.ok){ throw new Error(d.error || 'request failed'); }
        const phases = Array.isArray(d.phases) ? d.phases : [];
        const phaseHtml = phases.map(p => `
          <div class="phase">
            <div class="head"><div>${esc(p.title)}</div><span class="${cls(p.status)}">${esc(String(p.status||'').replace('_',' '))}</span></div>
            ${(Array.isArray(p.checklist)?p.checklist:[]).map(c=>`<div class="${c.done?'done':'todo'}">${c.done?'✓':'•'} ${esc(c.item)}${c.note?` <span class='muted'>(${esc(c.note)})</span>`:''}</div>`).join('')}
          </div>
        `).join('');
        document.getElementById('phases').innerHTML = phaseHtml || '<div class="muted">No phases</div>';
        document.getElementById('metrics').textContent = JSON.stringify(d.metrics || {}, null, 2);
        const actions = Array.isArray(d.next_actions) ? d.next_actions : [];
        document.getElementById('next-actions').innerHTML = actions.length ? actions.map(a=>`<div>• ${esc(a)}</div>`).join('') : '<div class="muted">None</div>';
      } catch (e) {
        document.getElementById('phases').innerHTML = `<div class='muted'>${esc(e.message || 'load failed')}</div>`;
      }
    }
    load();
  </script>
</body>
</html>
        """
    )


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


  @app.get("/api/trades/export")
  def api_trades_export():
    try:
      if not TRADES_CSV.exists():
        return jsonify({"error": f"trades file not found at {TRADES_CSV}"}), 404
      body = TRADES_CSV.read_text(encoding="utf-8")
      return Response(
        body,
        mimetype="text/csv",
        headers={
          "Content-Disposition": "attachment; filename=trades.csv",
          "X-Trades-Path": str(TRADES_CSV),
        },
      )
    except Exception as exc:  # noqa: BLE001
      return jsonify({"error": str(exc)}), 500


  @app.get("/api/runtime-paths")
  def api_runtime_paths():
    try:
      return jsonify(
        {
          "data_dir": str(config.DATA_DIR),
          "trades_csv_path": str(TRADES_CSV),
          "trades_csv_exists": bool(TRADES_CSV.exists()),
          "trades_csv_size_bytes": int(TRADES_CSV.stat().st_size) if TRADES_CSV.exists() else 0,
          "scan_log_csv_path": str(SCAN_LOG_CSV),
          "scan_log_exists": bool(SCAN_LOG_CSV.exists()),
          "state_json_path": str(config.STATE_JSON_PATH),
          "state_json_exists": bool(config.STATE_JSON_PATH.exists()),
        }
      )
    except Exception as exc:  # noqa: BLE001
      return jsonify({"error": str(exc)}), 500


@app.get("/api/scanlog")
def api_scanlog():
  try:
    _last_ts, rows = _latest_scan_loop_rows(limit=1000)
    today_rows = _today_scan_rows()
    runtime_state = load_bot_state()
    last_entry_debug = runtime_state.get("last_entry_debug") if isinstance(runtime_state, dict) else {}
    raw_outcomes = last_entry_debug.get("signal_outcomes") if isinstance(last_entry_debug, dict) else {}
    signal_outcomes = raw_outcomes if isinstance(raw_outcomes, dict) else {}
    passed: list[dict[str, Any]] = []
    for row in rows:
      if str(row.get("result", "")).lower() != "pass":
        continue
      passed.append(row)

    # Keep one final state row per symbol in this loop.
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in passed:
      symbol = str(row.get("symbol", "") or "").upper()
      if not symbol or symbol in seen:
        continue
      seen.add(symbol)
      deduped.append(row)

    deduped.sort(
      key=lambda item: (
        float(_safe_float(item.get("signal_score"), 0.0)),
        float(_safe_float(item.get("rvol"), 0.0)),
      ),
      reverse=True,
    )
    for row in deduped:
      symbol_upper = str(row.get("symbol", "") or "").upper()
      outcome = signal_outcomes.get(symbol_upper, {}) if symbol_upper else {}
      disposition = str(outcome.get("disposition", "") or "").strip() or "setup_pass"
      disposition_detail = str(outcome.get("detail", "") or "").strip()
      row["stage"] = "setup_pass"
      row["post_setup_disposition"] = disposition
      row["post_setup_detail"] = disposition_detail
      row["final_state"] = disposition
      row["state_timeline"] = _timeline_for_symbol(
        today_rows,
        str(row.get("symbol", "") or ""),
        max_items=5,
      )
    return jsonify(deduped[:30])
  except Exception as exc:  # noqa: BLE001
    return jsonify({"error": str(exc)}), 500


@app.route("/api/scanfails")
def api_scanfails():
    """Return latest-loop final scan failures with stage labels."""
    try:
        _last_ts, rows = _latest_scan_loop_rows(limit=1000)
        fails = [r for r in rows if str(r.get("result", "")).lower() == "fail"]
        out: list[dict[str, Any]] = []

        def _is_cooldown_row(row: dict[str, Any]) -> bool:
            reason = str(row.get("reason", "") or "").strip().lower()
            stage = _scan_fail_stage(reason)
            return stage == "cooldown_skip" or reason.startswith("cooldown_skip:")

        # Dedupe by symbol, but prioritize actionable rejects over routine cooldown rows.
        seen_symbols: set[str] = set()
        actionable: list[dict[str, Any]] = []
        cooldown: list[dict[str, Any]] = []
        for row in fails:
            symbol = str(row.get("symbol", "") or "").upper()
            key = symbol or f"__row_{len(seen_symbols)}"
            if key in seen_symbols:
                continue
            seen_symbols.add(key)
            if _is_cooldown_row(row):
                cooldown.append(row)
            else:
                actionable.append(row)

        selected: list[dict[str, Any]] = actionable[:20]
        if len(selected) < 20 and cooldown:
            selected.append(
                {
                    "timestamp": _to_ct_label(_now_et()),
                    "symbol": "*",
                    "reason": (
                        f"suppressed {len(cooldown)} cooldown_skip row(s); "
                        "showing actionable rejects first"
                    ),
                    "stage": "cooldown_summary",
                    "final_state": "setup_reject",
                }
            )

        for row in selected[:20]:
            ts_raw = str(row.get("timestamp", "") or "")
            ts_dt = _parse_ts(ts_raw)
            patched = dict(row)
            if not str(patched.get("stage", "")).strip():
                patched["stage"] = _scan_fail_stage(patched.get("reason", ""))
            if not str(patched.get("final_state", "")).strip():
                patched["final_state"] = "setup_reject"
            if ts_raw:
                patched["timestamp"] = _to_ct_label(ts_dt) or ts_raw
            out.append(patched)
        return jsonify(out)
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@app.route("/api/scansummary")
def api_scansummary():
    """Return stage-aware counts and reasons from the latest scan loop."""
    try:
        last_ts, same_loop = _latest_scan_loop_rows(limit=1000)
        if not same_loop:
          runtime_state = load_bot_state()
          last_entry_debug = runtime_state.get("last_entry_debug") if isinstance(runtime_state, dict) else {}
          if isinstance(last_entry_debug, dict) and last_entry_debug:
            signal_detected = int(_safe_float(last_entry_debug.get("signal_detected_count"), 0))
            eligible_count = int(_safe_float(last_entry_debug.get("entry_stage4_eligible_count"), 0))
            rejected_count = int(_safe_float(last_entry_debug.get("entry_stage4_reject_count"), 0))
            orders_submitted = int(_safe_float(last_entry_debug.get("entry_orders_submitted"), 0))
            orders_filled = int(_safe_float(last_entry_debug.get("entries_filled"), 0))
            today_trades = _today_trade_rows()
            trade_summary = _build_trade_report_summary(today_trades)
            raw_reasons = last_entry_debug.get("entry_stage4_reject_reasons")
            reject_reasons = raw_reasons if isinstance(raw_reasons, dict) else {}
            return jsonify(
              {
                "universe_candidates": signal_detected,
                "signal_detected_count": signal_detected,
                "setup_passed_count": signal_detected,
                "scanner_pass_count": signal_detected,
                "rejected_count": 0,
                "entry_eligible_count": eligible_count,
                "entry_rejected_count": rejected_count,
                "order_submitted_count": orders_submitted,
                "order_filled_count": orders_filled,
                "orders_submitted_count": orders_submitted,
                "orders_filled_count": orders_filled,
                "orders_buy_filled_count": orders_filled,
                "orders_sell_filled_count": 0,
                "orders_total_filled_count": orders_filled,
                "orders_rejected_or_canceled_count": 0,
                "trades_filled_count": orders_filled,
                "real_pass_count": orders_filled,
                "realized_pnl_usd": 0.0,
                "stage_pipeline": {
                  "signal_detected": signal_detected,
                  "entry_eligible": eligible_count,
                  "order_submitted": orders_submitted,
                  "order_filled": orders_filled,
                },
                "stage_fail_counts": {},
                "stage4_entry_reject_reasons": reject_reasons,
                "entry_stage4_source": "runtime_entry_debug_no_scanlog",
                "entry_stage4_fresh": True,
                "entry_stage4_loop_ts": str(last_entry_debug.get("loop_ts_et", "") or ""),
                "index_bias": str(last_entry_debug.get("index_bias", "both") or "both"),
                "chop_filter_active": bool(last_entry_debug.get("chop_filter_active", False)),
                "chop_filter_reason": str(last_entry_debug.get("chop_filter_reason", "") or ""),
                "chop_weak_signal_share": float(last_entry_debug.get("chop_weak_signal_share", 0.0) or 0.0),
                "chop_recent_option_exits": int(last_entry_debug.get("chop_recent_option_exits", 0) or 0),
                "chop_pause_until": str(last_entry_debug.get("chop_pause_until", "") or ""),
                "avg_time_to_first_green_seconds": trade_summary.get("avg_time_to_first_green_seconds"),
                "option_no_progress_exit_count": int(trade_summary.get("option_no_progress_exit_count", 0) or 0),
                "option_momentum_stall_exit_count": int(trade_summary.get("option_momentum_stall_exit_count", 0) or 0),
                "weak_index_bias_trade_count": int(trade_summary.get("weak_index_bias_trade_count", 0) or 0),
                "top_fail_reason": "Scan log unavailable; using runtime entry telemetry",
                "setup_valid_count": signal_detected,
                "pass_count": signal_detected,
                "fail_count": 0,
                "top_reason": "Scan log unavailable",
                "last_scan": str(last_entry_debug.get("loop_ts_et", "") or ""),
              }
            )
            return jsonify(
                {
                    "universe_candidates": 0,
              "signal_detected_count": 0,
                    "setup_passed_count": 0,
                    "rejected_count": 0,
                    "entry_eligible_count": 0,
              "order_submitted_count": 0,
              "order_filled_count": 0,
              "real_pass_count": 0,
                    "stage_fail_counts": {},
                    "top_fail_reason": "No scan data yet",
                    "last_scan": "",
                }
            )

        pass_rows = [r for r in same_loop if str(r.get("result", "") or "").lower() == "pass"]
        fail_rows = [r for r in same_loop if str(r.get("result", "") or "").lower() == "fail"]
        pass_count = len(pass_rows)
        fail_count = len(fail_rows)
        reasons = [str(r.get("reason", "") or "") for r in fail_rows]
        top_reason = max(set(reasons), key=reasons.count) if reasons else ""

        stage_fail_counts: dict[str, int] = {}
        for row in fail_rows:
            stage = _scan_fail_stage(row.get("reason", ""))
            stage_fail_counts[stage] = stage_fail_counts.get(stage, 0) + 1

        stage_pipeline = {
          "stage1_universe_candidates": len(same_loop),
          "stage2_direction_passed": pass_count,
          "stage3_setup_passed": pass_count,
          "signal_detected": len(same_loop),
        }

        runtime_state = load_bot_state()
        last_entry_debug = runtime_state.get("last_entry_debug") if isinstance(runtime_state, dict) else {}
        entry_loop_ts = str(last_entry_debug.get("loop_ts_et", "") or "") if isinstance(last_entry_debug, dict) else ""

        entry_stage4_eligible_count = pass_count
        entry_stage4_reject_count = 0
        entry_stage4_reject_reasons: dict[str, int] = {}
        entry_stage4_source = "proxy_scanner_pass"
        entry_debug_fresh = False

        broker_telemetry = _fetch_broker_order_telemetry()
        orders_submitted_count = int(broker_telemetry.get("option_orders_today", 0) or 0) if broker_telemetry.get("ok") else 0
        orders_buy_filled_count = int(broker_telemetry.get("option_buy_fills_today", 0) or 0) if broker_telemetry.get("ok") else 0
        orders_sell_filled_count = int(broker_telemetry.get("option_sell_fills_today", 0) or 0) if broker_telemetry.get("ok") else 0
        orders_total_filled_count = int(broker_telemetry.get("option_filled_orders_today", 0) or 0) if broker_telemetry.get("ok") else 0
        orders_rejected_or_canceled_count = (
          int(broker_telemetry.get("option_rejected_or_canceled_today", 0) or 0) if broker_telemetry.get("ok") else 0
        )

        today_trades = _today_trade_rows()
        realized_pnl_usd = round(sum(_pnl_usd_from_trade_row(row) for row in today_trades), 2)
        trade_summary = _build_trade_report_summary(today_trades)

        scan_dt = _parse_ts(str(last_ts))
        entry_dt = _parse_ts(entry_loop_ts)
        if scan_dt is not None and entry_dt is not None:
          entry_debug_fresh = abs((scan_dt - entry_dt).total_seconds()) <= 180

        if entry_debug_fresh and isinstance(last_entry_debug, dict):
          entry_stage4_eligible_count = int(_safe_float(last_entry_debug.get("entry_stage4_eligible_count"), 0))
          entry_stage4_reject_count = int(_safe_float(last_entry_debug.get("entry_stage4_reject_count"), 0))
          raw_reasons = last_entry_debug.get("entry_stage4_reject_reasons")
          if isinstance(raw_reasons, dict):
            for k, v in raw_reasons.items():
              key = str(k or "").strip()
              if not key:
                continue
              entry_stage4_reject_reasons[key] = int(_safe_float(v, 0))
          entry_stage4_source = "runtime_entry_debug"

        stage_pipeline["stage4_entry_eligible"] = entry_stage4_eligible_count
        stage_pipeline["entry_eligible"] = entry_stage4_eligible_count
        stage_pipeline["order_submitted"] = orders_submitted_count
        stage_pipeline["order_filled"] = orders_total_filled_count

        return jsonify(
            {
                "universe_candidates": len(same_loop),
          "signal_detected_count": len(same_loop),
                "setup_passed_count": pass_count,
            "scanner_pass_count": pass_count,
                "rejected_count": fail_count,
                "entry_eligible_count": entry_stage4_eligible_count,
                "entry_rejected_count": entry_stage4_reject_count,
                "orders_submitted_count": orders_submitted_count,
                "orders_filled_count": orders_total_filled_count,
                "orders_buy_filled_count": orders_buy_filled_count,
                "orders_sell_filled_count": orders_sell_filled_count,
                "orders_total_filled_count": orders_total_filled_count,
          "order_submitted_count": orders_submitted_count,
          "order_filled_count": orders_total_filled_count,
                "orders_rejected_or_canceled_count": orders_rejected_or_canceled_count,
                "trades_filled_count": orders_total_filled_count,
          "real_pass_count": orders_total_filled_count,
                "realized_pnl_usd": realized_pnl_usd,
                "stage_pipeline": stage_pipeline,
                "stage_fail_counts": stage_fail_counts,
                "stage4_entry_reject_reasons": entry_stage4_reject_reasons,
                "entry_stage4_source": entry_stage4_source,
                "entry_stage4_fresh": entry_debug_fresh,
                "entry_stage4_loop_ts": _to_ct_label(entry_dt) if entry_dt else entry_loop_ts,
                "index_bias": str(last_entry_debug.get("index_bias", "both") or "both") if isinstance(last_entry_debug, dict) else "both",
                "chop_filter_active": bool(last_entry_debug.get("chop_filter_active", False)) if isinstance(last_entry_debug, dict) else False,
                "chop_filter_reason": str(last_entry_debug.get("chop_filter_reason", "") or "") if isinstance(last_entry_debug, dict) else "",
                "chop_weak_signal_share": float(last_entry_debug.get("chop_weak_signal_share", 0.0) or 0.0) if isinstance(last_entry_debug, dict) else 0.0,
                "chop_recent_option_exits": int(last_entry_debug.get("chop_recent_option_exits", 0) or 0) if isinstance(last_entry_debug, dict) else 0,
                "chop_pause_until": str(last_entry_debug.get("chop_pause_until", "") or "") if isinstance(last_entry_debug, dict) else "",
                "avg_time_to_first_green_seconds": trade_summary.get("avg_time_to_first_green_seconds"),
                "option_no_progress_exit_count": int(trade_summary.get("option_no_progress_exit_count", 0) or 0),
                "option_momentum_stall_exit_count": int(trade_summary.get("option_momentum_stall_exit_count", 0) or 0),
                "weak_index_bias_trade_count": int(trade_summary.get("weak_index_bias_trade_count", 0) or 0),
                "top_fail_reason": top_reason,
                # Backward-compatible aliases for existing clients.
                "setup_valid_count": pass_count,
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
        trader_thread_last_crash_raw = str(runtime_state.get("trader_thread_last_crash_et", "") or "")
        trader_thread_last_crash_dt = _parse_ts(trader_thread_last_crash_raw)
        trader_thread_last_crash_msg = str(runtime_state.get("trader_thread_last_crash", "") or "")
        independent_stoploss_last_trigger_raw = str(runtime_state.get("independent_stoploss_last_trigger_et", "") or "")
        independent_stoploss_last_trigger_dt = _parse_ts(independent_stoploss_last_trigger_raw)
        independent_stoploss_last_symbol = str(runtime_state.get("independent_stoploss_last_symbol", "") or "")
        independent_stoploss_last_unrealized_usd = _safe_float(
          runtime_state.get("independent_stoploss_last_unrealized_usd"),
          0.0,
        )
        independent_stoploss_last_qty = int(_safe_float(runtime_state.get("independent_stoploss_last_qty"), 0))
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
                "entry_window_label": _entry_window_label_for_display(),
                "catalyst_mode_active": catalyst_mode_active,
                "catalyst_mode_reason": catalyst_mode_reason,
                "catalyst_mode_until": catalyst_mode_until,
                "can_enter_now": len(blockers) == 0,
                "blockers": blockers,
                "trader_loop_alive": bool(trader_loop_alive),
                "trader_loop_stale_after_seconds": loop_stale_after,
                "trader_heartbeat_et": _to_ct_label(heartbeat_dt) if heartbeat_dt else "",
                "trader_heartbeat_age_seconds": heartbeat_age_seconds,
                "trader_thread_last_crash_et": _to_ct_label(trader_thread_last_crash_dt) if trader_thread_last_crash_dt else "",
                "trader_thread_last_crash": trader_thread_last_crash_msg,
                "independent_stoploss_last_trigger_et": (
                  _to_ct_label(independent_stoploss_last_trigger_dt) if independent_stoploss_last_trigger_dt else ""
                ),
                "independent_stoploss_last_symbol": independent_stoploss_last_symbol,
                "independent_stoploss_last_unrealized_usd": independent_stoploss_last_unrealized_usd,
                "independent_stoploss_last_qty": independent_stoploss_last_qty,
                "last_alpaca_auth_error_et": _to_ct_label(last_auth_error_dt) if last_auth_error_dt else "",
                "last_alpaca_auth_error": last_auth_error_msg,
                "alpaca_auth_error_recent": auth_error_recent,
                "last_entry_debug": last_entry_debug if isinstance(last_entry_debug, dict) else {},
                "last_exit_debug": last_exit_debug if isinstance(last_exit_debug, dict) else {},
                "runtime_state_updated_at_iso": str(runtime_state.get("_state_updated_at_iso", "") or ""),
                "runtime_state_keys": len(runtime_state.keys()) if isinstance(runtime_state, dict) else 0,
                "runtime_state_file": str(config.STATE_JSON_PATH),
                "runtime_state_file_exists": bool(config.STATE_JSON_PATH.exists()),
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
                "entry_window_label": _entry_window_label_for_display(),
                "catalyst_mode_active": False,
                "catalyst_mode_reason": "",
                "catalyst_mode_until": "",
                "can_enter_now": False,
                "blockers": ["status_unavailable"],
                "trader_loop_alive": False,
                "trader_loop_stale_after_seconds": max(60, int(config.LOOP_INTERVAL_SECONDS) * 4),
                "trader_heartbeat_et": "",
                "trader_heartbeat_age_seconds": None,
                "trader_thread_last_crash_et": "",
                "trader_thread_last_crash": "",
                "independent_stoploss_last_trigger_et": "",
                "independent_stoploss_last_symbol": "",
                "independent_stoploss_last_unrealized_usd": 0.0,
                "independent_stoploss_last_qty": 0,
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


@app.post("/api/control/close-all-positions")
def api_close_all_positions():
    try:
        ok, err, status = _verify_control_token()
        if not ok:
            return jsonify({"error": err}), status
        
        from broker import AlpacaBroker
        broker = AlpacaBroker(API_KEY, SECRET_KEY, PAPER)
        total, closed, results = broker.close_all_positions()
        
        return jsonify({
            "ok": True,
            "total_positions": total,
            "closed_count": closed,
            "results": results,
            "message": f"Closed {closed} of {total} positions"
        })
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
  stock_window = _watch_stock_window_config(request.args.get("window", "1D"))
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

    stock_series_map = _fetch_intraday_stock_series(
      list(dict.fromkeys(underlyings)),
      limit=int(stock_window.get("limit", 120)),
      timeframe=str(stock_window.get("timeframe", "5Min")),
      lookback_minutes=int(stock_window.get("lookback_minutes", 390)),
    )
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
    return jsonify(
      {
        "rows": payload_rows,
        "generated_at": _now_et().strftime("%Y-%m-%d %H:%M:%S ET"),
        "stock_window": stock_window,
      }
    )
  except Exception as exc:  # noqa: BLE001
    return jsonify({"error": str(exc)}), 500


@app.get("/api/watch/history")
def api_watch_history():
    history_window = _watch_history_window_config(request.args.get("window", "1M"))
    lookback_minutes = history_window.get("lookback_minutes")
    now_et = _now_et()
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
            if lookback_minutes is not None:
                age_minutes = (now_et - dt).total_seconds() / 60.0
                if age_minutes > float(lookback_minutes):
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
                "history_window": history_window,
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


@app.get("/api/reports/morning")
def api_morning_report():
    try:
        return jsonify(_build_morning_report_payload())
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@app.get("/api/reports/evening")
def api_evening_report():
    try:
        return jsonify(_build_evening_report_payload())
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@app.get("/api/signals")
def api_signals():
  """Public machine-readable signal endpoint for LISA source polling."""
  try:
    return jsonify(
      {
        "source": "autobott",
        "generated_at": _now_et().strftime("%Y-%m-%dT%H:%M:%S%z"),
        "count": 1,
        "signals": [
          {
            "id": int(time.time()),
            "lane": "market",
            "signal_kind": "source_heartbeat",
            "confidence": 0.91,
            "score": 59,
            "impact_level": "low",
            "trend": "neutral",
            "velocity": "steady",
            "action_hint": "autobott source healthy",
            "tags": ["autobott", "heartbeat"],
            "source_class": "source_api",
            "observed_fact": "AutoBott API heartbeat is healthy",
          }
        ],
      }
    )
  except Exception as exc:  # noqa: BLE001
    return jsonify({"error": str(exc)}), 500


@app.get("/reports")
def reports_page():
    return render_template_string(
        """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Trading Reports</title>
  <style>
    :root {
      --bg:#081019;
      --panel:rgba(14,23,35,.88);
      --panel-strong:rgba(21,34,51,.96);
      --text:#ebf3fb;
      --muted:#95abc1;
      --border:rgba(141,172,206,.25);
      --green:#25d366;
      --red:#ff5d66;
      --cyan:#31cbff;
      --yellow:#ffbf4a;
      --radius:16px;
      --shadow:0 14px 34px rgba(2,8,20,.45);
    }
    * { box-sizing:border-box; }
    body {
      margin:0;
      color:var(--text);
      font-family:"Avenir Next","Nunito Sans","Segoe UI",Tahoma,sans-serif;
      background:
        radial-gradient(1200px 500px at -10% -20%, rgba(49,203,255,.16), transparent 45%),
        radial-gradient(900px 420px at 110% -10%, rgba(37,211,102,.12), transparent 45%),
        linear-gradient(145deg, #050a11 0%, #0b1524 45%, #0a111d 100%);
      min-height:100vh;
    }
    .wrap { max-width:1280px; margin:0 auto; padding:16px; }
    .header { display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap; margin-bottom:14px; }
    .title { font-size:clamp(20px, 2.5vw, 31px); font-weight:800; line-height:1.1; }
    .muted { color:var(--muted); }
    .actions { display:flex; gap:8px; flex-wrap:wrap; }
    .btn {
      border:1px solid var(--border);
      border-radius:12px;
      padding:10px 12px;
      color:var(--text);
      text-decoration:none;
      font-weight:700;
      background:rgba(127,156,191,.15);
    }
    .grid { display:grid; grid-template-columns:repeat(2, minmax(0, 1fr)); gap:12px; }
    .card {
      background:var(--panel);
      border:1px solid var(--border);
      border-radius:var(--radius);
      padding:14px;
      box-shadow:var(--shadow);
      backdrop-filter:blur(4px);
    }
    .card.strong { background:var(--panel-strong); }
    .section-title { display:flex; justify-content:space-between; gap:10px; align-items:center; margin-bottom:10px; }
    .section-title h2 { margin:0; font-size:14px; letter-spacing:.7px; color:var(--muted); }
    .pill {
      border:1px solid var(--border);
      border-radius:999px;
      padding:4px 8px;
      font-size:11px;
      text-transform:uppercase;
      letter-spacing:.5px;
    }
    .pill.ready, .pill.finalized { color:var(--green); border-color:rgba(37,211,102,.45); }
    .pill.building, .pill.live { color:var(--yellow); border-color:rgba(255,191,74,.45); }
    .pill.waiting_for_signals { color:var(--red); border-color:rgba(255,93,102,.45); }
    .metrics { display:grid; grid-template-columns:repeat(4, minmax(0, 1fr)); gap:10px; margin-bottom:12px; }
    .metric {
      background:rgba(7,14,24,.6);
      border:1px solid var(--border);
      border-radius:12px;
      padding:10px;
    }
    .metric .label { font-size:11px; color:var(--muted); text-transform:uppercase; letter-spacing:.55px; margin-bottom:5px; }
    .metric .value { font-family:ui-monospace,SFMono-Regular,Menlo,monospace; font-size:21px; font-weight:700; }
    .metric .value.pos { color:var(--green); }
    .metric .value.neg { color:var(--red); }
    .two-col { display:grid; grid-template-columns:repeat(2, minmax(0, 1fr)); gap:12px; }
    .list { display:grid; gap:8px; }
    .list-item {
      border:1px solid var(--border);
      border-radius:12px;
      padding:10px;
      background:rgba(7,14,24,.56);
    }
    .list-title { font-weight:700; margin-bottom:4px; }
    table { width:100%; border-collapse:collapse; font-size:13px; }
    th, td { border-bottom:1px solid var(--border); padding:8px 6px; text-align:left; vertical-align:top; }
    th { color:var(--muted); font-size:12px; letter-spacing:.35px; }
    .table-wrap { overflow-x:auto; -webkit-overflow-scrolling:touch; }
    .pos { color:var(--green); }
    .neg { color:var(--red); }
    .mono { font-family:ui-monospace,SFMono-Regular,Menlo,monospace; }
    @media (max-width: 980px) {
      .grid, .two-col { grid-template-columns:1fr; }
      .metrics { grid-template-columns:repeat(2, minmax(0, 1fr)); }
    }
    @media (max-width: 640px) {
      .wrap { padding:12px; }
      .metrics { grid-template-columns:1fr; }
      .btn { width:100%; text-align:center; }
      .actions { width:100%; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="header">
      <div>
        <div class="title">Trading Reports</div>
        <div class="muted">Morning plan before the open and an evening summary after the session.</div>
      </div>
      <div class="actions">
        <a class="btn" href="/">Dashboard</a>
        <a class="btn" href="/roadmap">Roadmap</a>
        <a class="btn" href="/lisa-feed">LISA Feed</a>
        <a class="btn" href="/watch">Watch Page</a>
      </div>
    </div>

    <div class="grid">
      <div class="card strong">
        <div class="section-title">
          <h2>MORNING REPORT</h2>
          <span id="morning-status" class="pill">Loading</span>
        </div>
        <div id="morning-report" class="muted">Loading morning report...</div>
      </div>

      <div class="card strong">
        <div class="section-title">
          <h2>EVENING REPORT</h2>
          <span id="evening-status" class="pill">Loading</span>
        </div>
        <div id="evening-report" class="muted">Loading evening report...</div>
      </div>
    </div>
  </div>

  <script>
    function escapeHtml(value) {
      return String(value || "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/\"/g, "&quot;")
        .replace(/'/g, "&#39;");
    }
    function fmtMoney(value) {
      const n = Number(value);
      if (Number.isNaN(n)) return "--";
      return n.toLocaleString(undefined, { style: "currency", currency: "USD", maximumFractionDigits: 2 });
    }
    function fmtPct(value) {
      const n = Number(value);
      if (Number.isNaN(n)) return "--";
      return `${n >= 0 ? "+" : ""}${n.toFixed(2)}%`;
    }
    function cls(value) {
      const n = Number(value);
      if (Number.isNaN(n)) return "";
      if (n > 0) return "pos";
      if (n < 0) return "neg";
      return "";
    }
    async function fetchJson(url) {
      try {
        const res = await fetch(url);
        const body = await res.json();
        if (!res.ok) return { error: body.error || "request failed" };
        return body;
      } catch {
        return { error: "request failed" };
      }
    }
    function listHtml(items) {
      const rows = Array.isArray(items) ? items : [];
      if (!rows.length) return `<div class="muted">No items yet.</div>`;
      return `<div class="list">${rows.map((item) => `<div class="list-item">${escapeHtml(item)}</div>`).join("")}</div>`;
    }
    function topReasonRows(rows, labelKey) {
      const data = Array.isArray(rows) ? rows : [];
      if (!data.length) return `<div class="muted">No rows yet.</div>`;
      return `
        <div class="table-wrap">
          <table>
            <thead><tr><th>${labelKey}</th><th>Count</th></tr></thead>
            <tbody>
              ${data.map((row) => `<tr><td>${escapeHtml(row.reason || "-")}</td><td class="mono">${Number(row.count || 0)}</td></tr>`).join("")}
            </tbody>
          </table>
        </div>
      `;
    }
    function signalTable(rows) {
      const data = Array.isArray(rows) ? rows : [];
      if (!data.length) return `<div class="muted">No candidates staged yet.</div>`;
      return `
        <div class="table-wrap">
          <table>
            <thead><tr><th>Symbol</th><th>Dir</th><th>Score</th><th>RVOL</th><th>Reason</th></tr></thead>
            <tbody>
              ${data.map((row) => `
                <tr>
                  <td class="mono">${escapeHtml(row.symbol || "-")}</td>
                  <td>${escapeHtml(row.direction || "-")}</td>
                  <td class="mono">${Number(row.signal_score || 0).toFixed(2)}</td>
                  <td class="mono">${Number(row.rvol || 0).toFixed(2)}</td>
                  <td>${escapeHtml(row.reason || "-")}</td>
                </tr>
              `).join("")}
            </tbody>
          </table>
        </div>
      `;
    }
    function tickerTable(rows) {
      const data = Array.isArray(rows) ? rows : [];
      if (!data.length) return `<div class="muted">No ticker scorecards yet.</div>`;
      return `
        <div class="table-wrap">
          <table>
            <thead><tr><th>Ticker</th><th>Trades</th><th>Win Rate</th><th>Total P&L</th></tr></thead>
            <tbody>
              ${data.map((row) => `
                <tr>
                  <td class="mono">${escapeHtml(row.ticker || "-")}</td>
                  <td class="mono">${Number(row.trades || 0)}</td>
                  <td class="mono">${Number(row.win_rate_pct || 0).toFixed(1)}%</td>
                  <td class="mono ${cls(row.total_pnl_usd)}">${fmtMoney(row.total_pnl_usd)}</td>
                </tr>
              `).join("")}
            </tbody>
          </table>
        </div>
      `;
    }
    function tradeCard(label, trade) {
      if (!trade) return `<div class="muted">No trade available.</div>`;
      return `
        <div class="list-item">
          <div class="list-title">${escapeHtml(label)}: ${escapeHtml(trade.ticker || "-")}</div>
          <div class="muted">${escapeHtml(trade.timestamp || "-")}</div>
          <div class="mono ${cls(trade.pnl_usd)}" style="margin-top:6px;">${fmtMoney(trade.pnl_usd)} | ${fmtPct(trade.pnl_pct)}</div>
          <div style="margin-top:6px;">${escapeHtml(trade.exit_reason || "-")}</div>
        </div>
      `;
    }
    function renderMorning(data) {
      const statusEl = document.getElementById("morning-status");
      const wrap = document.getElementById("morning-report");
      if (!statusEl || !wrap) return;
      if (!data || data.error) {
        statusEl.className = "pill waiting_for_signals";
        statusEl.textContent = "Error";
        wrap.innerHTML = `<div class="muted">${escapeHtml(data && data.error ? data.error : "request failed")}</div>`;
        return;
      }
      statusEl.className = `pill ${escapeHtml(data.status || "")}`;
      statusEl.textContent = String(data.status || "unknown").replaceAll("_", " ");
      const perf = data.recent_performance || {};
      const scan = data.premarket_scan_summary || {};
      const stagedDirectionCounts = data.staged_direction_counts || {};
      wrap.innerHTML = `
        <div class="muted" style="margin-bottom:10px;">Generated ${escapeHtml(data.generated_at || "-")} | Target ready by ${escapeHtml(data.report_ready_time || "-")} | Last scan ${escapeHtml(data.last_scan_at || "--")}</div>
        <div class="metrics">
          <div class="metric"><div class="label">Staged Signals</div><div class="value">${Number(data.staged_signal_count || 0)}</div></div>
          <div class="metric"><div class="label">Premarket Setup-Valid</div><div class="value">${Number(scan.setup_valid_count ?? scan.pass_count || 0)}</div></div>
          <div class="metric"><div class="label">5-Day P&L</div><div class="value ${cls(perf.total_pnl_usd)}">${fmtMoney(perf.total_pnl_usd)}</div></div>
          <div class="metric"><div class="label">5-Day Win Rate</div><div class="value ${cls((Number(perf.win_rate_pct || 0)) - 50)}">${Number(perf.win_rate_pct || 0).toFixed(1)}%</div></div>
        </div>
        <div class="two-col">
          <div>
            <div class="section-title"><h2>Trading Plan</h2></div>
            ${listHtml(data.recommendations || [])}
          </div>
          <div>
            <div class="section-title"><h2>Bias + Scan Pressure</h2></div>
            <div class="list">
              <div class="list-item">CALL bias: <span class="mono">${Number(stagedDirectionCounts.CALL || 0)}</span></div>
              <div class="list-item">PUT bias: <span class="mono">${Number(stagedDirectionCounts.PUT || 0)}</span></div>
              <div class="list-item">Premarket failures: <span class="mono">${Number(scan.fail_count || 0)}</span></div>
              <div class="list-item">Premarket scan runs: <span class="mono">${Number(data.scan_runs || 0)}</span></div>
            </div>
          </div>
        </div>
        <div class="section-title" style="margin-top:12px;"><h2>Staged Signals</h2></div>
        ${signalTable(data.staged_signals || [])}
        <div class="two-col" style="margin-top:12px;">
          <div>
            <div class="section-title"><h2>Top Premarket Rejections</h2></div>
            ${topReasonRows(scan.top_fail_reasons || [], "Reason")}
          </div>
          <div>
            <div class="section-title"><h2>Recent Ticker Leaders</h2></div>
            ${tickerTable(data.ticker_leaders || [])}
          </div>
        </div>
      `;
    }
    function renderEvening(data) {
      const statusEl = document.getElementById("evening-status");
      const wrap = document.getElementById("evening-report");
      if (!statusEl || !wrap) return;
      if (!data || data.error) {
        statusEl.className = "pill waiting_for_signals";
        statusEl.textContent = "Error";
        wrap.innerHTML = `<div class="muted">${escapeHtml(data && data.error ? data.error : "request failed")}</div>`;
        return;
      }
      statusEl.className = `pill ${escapeHtml(data.status || "")}`;
      statusEl.textContent = String(data.status || "unknown").replaceAll("_", " ");
      const trades = data.trade_summary || {};
      const scans = data.scan_summary || {};
      const dailyReview = data.daily_review || {};
      const telemetry = data.telemetry || {};
      const runtimeTrades = telemetry.runtime_trades || {};
      const brokerActivity = telemetry.broker_activity || {};
      const dataHealth = telemetry.data_health || {};
      const telemetryAlerts = Array.isArray(telemetry.alerts) ? telemetry.alerts : [];
      const brokerSellFills = Number(brokerActivity.option_sell_fills_today || 0);
      wrap.innerHTML = `
        <div class="muted" style="margin-bottom:10px;">Generated ${escapeHtml(data.generated_at || "-")} | Closed trades ${Number(trades.total_trades || 0)} | Broker sell fills ${brokerSellFills} | Scan rows ${Number(scans.total_rows || 0)}</div>
        <div class="metrics">
          <div class="metric"><div class="label">Today P&L</div><div class="value ${cls(trades.total_pnl_usd)}">${fmtMoney(trades.total_pnl_usd)}</div></div>
          <div class="metric"><div class="label">Win Rate</div><div class="value ${cls((Number(trades.win_rate_pct || 0)) - 50)}">${Number(trades.win_rate_pct || 0).toFixed(1)}%</div></div>
          <div class="metric"><div class="label">Scanner Setup-Valid</div><div class="value">${Number(scans.setup_valid_count ?? scans.pass_count || 0)}</div></div>
          <div class="metric"><div class="label">Scanner Fails</div><div class="value">${Number(scans.fail_count || 0)}</div></div>
        </div>
        <div class="two-col">
          <div>
            <div class="section-title"><h2>Evening Summary</h2></div>
            ${listHtml(data.recommendations || [])}
          </div>
          <div>
            <div class="section-title"><h2>Best / Worst Trade</h2></div>
            <div class="list">
              ${tradeCard("Best", trades.best_trade)}
              ${tradeCard("Worst", trades.worst_trade)}
            </div>
          </div>
        </div>
        <div class="two-col" style="margin-top:12px;">
          <div>
            <div class="section-title"><h2>Exit Reasons</h2></div>
            ${topReasonRows(trades.exit_reasons || [], "Exit")}
          </div>
          <div>
            <div class="section-title"><h2>Top Setup-Valid Signals</h2></div>
            ${signalTable(scans.top_setup_valid || scans.top_passes || [])}
          </div>
        </div>
        <div class="two-col" style="margin-top:12px;">
          <div>
            <div class="section-title"><h2>Daily Self-Checks</h2></div>
            ${listHtml((dailyReview.checks || []).map((item) => `${item.name}: ${item.detail}`))}
          </div>
          <div>
            <div class="section-title"><h2>Ticker Leaders</h2></div>
            ${tickerTable(data.ticker_leaders || [])}
          </div>
        </div>
        <div class="section-title" style="margin-top:12px;"><h2>Telemetry Health</h2></div>
        <div class="two-col">
          <div>
            ${listHtml(
              telemetryAlerts.length
                ? telemetryAlerts
                : [
                    `Runtime closed trades: ${Number(runtimeTrades.closed_count || 0)} (${escapeHtml(runtimeTrades.day || "n/a")})`,
                    `Runtime trade P&L: ${fmtMoney(runtimeTrades.total_pnl_usd || 0)}`,
                    `Broker option fills: ${Number(brokerActivity.option_filled_orders_today || 0)} (buy ${Number(brokerActivity.option_buy_fills_today || 0)} / sell ${Number(brokerActivity.option_sell_fills_today || 0)})`,
                  ]
            )}
          </div>
          <div>
            ${listHtml([
              `trades.csv exists: ${String((dataHealth.trades_csv || {}).exists ? "yes" : "no")}`,
              `trades.csv modified: ${String((dataHealth.trades_csv || {}).modified_at || "n/a")}`,
              `scan_log.csv exists: ${String((dataHealth.scan_log_csv || {}).exists ? "yes" : "no")}`,
              `scan_log.csv modified: ${String((dataHealth.scan_log_csv || {}).modified_at || "n/a")}`,
              `Trader heartbeat: ${String(dataHealth.last_trader_heartbeat_et || "n/a")}`,
              brokerActivity.ok ? "Broker activity probe: ok" : `Broker activity probe: ${escapeHtml(String(brokerActivity.error || "failed"))}`,
            ])}
          </div>
        </div>
      `;
    }
    async function refreshReports() {
      const [morning, evening] = await Promise.all([
        fetchJson("/api/reports/morning"),
        fetchJson("/api/reports/evening"),
      ]);
      renderMorning(morning);
      renderEvening(evening);
    }
    refreshReports();
    setInterval(refreshReports, 30000);
  </script>
</body>
</html>
        """
    )


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
    .range-tabs { display:flex; flex-wrap:wrap; gap:8px; margin-top:10px; }
    .range-btn {
      border:1px solid var(--border);
      border-radius:10px;
      padding:6px 10px;
      font-weight:700;
      font-size:12px;
      cursor:pointer;
      background:rgba(127,156,191,.15);
      color:var(--text);
    }
    .range-btn.active {
      background:rgba(42,199,255,.22);
      border-color:rgba(42,199,255,.45);
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
        <a class="btn" href="/reports">Reports</a>
        <a class="btn" href="/lisa-feed">LISA Feed</a>
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
        <div class="muted" style="margin-top:10px;">Stock chart range:</div>
        <div class="range-tabs" id="watch-range-tabs">
          <button class="range-btn" data-window="1H" onclick="setWatchWindow('1H')">1H</button>
          <button class="range-btn" data-window="1D" onclick="setWatchWindow('1D')">1D</button>
          <button class="range-btn" data-window="1W" onclick="setWatchWindow('1W')">1W</button>
          <button class="range-btn" data-window="1M" onclick="setWatchWindow('1M')">1M</button>
        </div>
      </div>
      <div id="open-rows" class="grid"></div>
    </div>

    <div id="panel-history" class="hidden">
      <div class="card">
        <div class="muted" style="margin-bottom:8px;">Trade history range:</div>
        <div class="range-tabs" id="history-range-tabs">
          <button class="range-btn" data-window="1H" onclick="setHistoryWindow('1H')">1H</button>
          <button class="range-btn" data-window="1D" onclick="setHistoryWindow('1D')">1D</button>
          <button class="range-btn" data-window="1W" onclick="setHistoryWindow('1W')">1W</button>
          <button class="range-btn" data-window="1M" onclick="setHistoryWindow('1M')">1M</button>
          <button class="range-btn" data-window="ALL" onclick="setHistoryWindow('ALL')">ALL</button>
        </div>
      </div>
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
    let watchStockWindow = localStorage.getItem("watchStockWindow") || "1D";
    let watchStockWindowLabel = "5m, 1 day";
    let watchHistoryWindow = localStorage.getItem("watchHistoryWindow") || "1M";

    function applyWatchWindowButtons() {
      const buttons = Array.from(document.querySelectorAll("#watch-range-tabs .range-btn"));
      buttons.forEach((btn) => {
        const key = String(btn.getAttribute("data-window") || "").toUpperCase();
        btn.className = key === watchStockWindow ? "range-btn active" : "range-btn";
      });
    }

    function setWatchWindow(windowKey) {
      watchStockWindow = String(windowKey || "1D").toUpperCase();
      localStorage.setItem("watchStockWindow", watchStockWindow);
      applyWatchWindowButtons();
      refreshOpen();
    }

    function applyHistoryWindowButtons() {
      const buttons = Array.from(document.querySelectorAll("#history-range-tabs .range-btn"));
      buttons.forEach((btn) => {
        const key = String(btn.getAttribute("data-window") || "").toUpperCase();
        btn.className = key === watchHistoryWindow ? "range-btn active" : "range-btn";
      });
    }

    function setHistoryWindow(windowKey) {
      watchHistoryWindow = String(windowKey || "1M").toUpperCase();
      localStorage.setItem("watchHistoryWindow", watchHistoryWindow);
      applyHistoryWindowButtons();
      refreshHistory();
    }

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
      const body = await fetchJson("/api/watchlist-control", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
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
          <div class="muted" style="font-size:12px; margin:8px 0 6px;">Underlying Stock Price (${watchStockWindowLabel})</div>
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
      const body = await fetchJson(`/api/watch/open?window=${encodeURIComponent(watchStockWindow)}`);
      if (body.error) return;
      const stockWindow = body.stock_window || {};
      watchStockWindowLabel = String(stockWindow.label || watchStockWindowLabel || "stock");
      watchStockWindow = String(stockWindow.key || watchStockWindow || "1D").toUpperCase();
      applyWatchWindowButtons();
      renderOpen(body.rows || []);
    }
    async function refreshHistory() {
      const body = await fetchJson(`/api/watch/history?window=${encodeURIComponent(watchHistoryWindow)}`);
      if (body.error) return;
      const historyWindow = body.history_window || {};
      watchHistoryWindow = String(historyWindow.key || watchHistoryWindow || "1M").toUpperCase();
      applyHistoryWindowButtons();
      renderHistory(body);
    }
    async function refreshAll() {
      await Promise.all([loadWatchlist(), refreshOpen(), refreshHistory()]);
    }
    applyWatchWindowButtons();
    applyHistoryWindowButtons();
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
        <a href="/roadmap" class="ctrl-btn" style="text-decoration:none;">ROADMAP</a>
        <a href="/lisa-feed" class="ctrl-btn" style="text-decoration:none;">LISA FEED</a>
        <a href="/reports" class="ctrl-btn" style="text-decoration:none;">REPORTS</a>
        <a href="/watch" class="ctrl-btn" style="text-decoration:none;">WATCH PAGE</a>
        <div class="paper">{{ "PAPER MODE" if paper else "LIVE MODE" }}</div>
      </div>
    </div>

    <div class="card section">
      <h3>TRADING CONTROL</h3>
      <div class="ctrl">
        <button class="ctrl-btn stop" onclick="setTradingControl('stop')">STOP TRADING</button>
        <button class="ctrl-btn start" onclick="setTradingControl('start')">START TRADING</button>
        <button class="ctrl-btn" style="background-color: #d32f2f;" onclick="closeAllPositions()">CLOSE ALL POSITIONS</button>
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
      <div class="card strong"><div class="label">Market Status</div><div id="market-status" class="num">--</div><div id="entry-window-status" class="kpi-sub">Entry Window: --</div><div id="catalyst-mode-status" class="kpi-sub">Catalyst Mode: --</div><div id="trader-loop-status" class="kpi-sub">Trader Loop: --</div><div id="independent-stoploss-status" class="kpi-sub">Independent SL: --</div><div id="blockers-status" class="kpi-sub">Blockers: --</div></div>
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
        Current loop final rejects (actionable first, up to 20):
      </div>
      <table id="scan-fails-table">
        <thead><tr><th>Time</th><th>Symbol</th><th>Stage</th><th>Reason</th></tr></thead>
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
      <h3>SCANNER - Current Loop Final Setup Passes</h3>
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
      if (slice.length === 0) { el.textContent = "No final setup passes in latest scan loop"; return; }
      const timelineText = (arr) => {
        if (!Array.isArray(arr) || !arr.length) return "-";
        return arr.join(" > ");
      };
      const timelineDetails = (arr) => {
        if (!Array.isArray(arr) || !arr.length) return "-";
        const latest = String(arr[0] || "-");
        if (arr.length === 1) return latest;
        const extra = arr.slice(1).map(item => `<div style="margin-top:4px">${item}</div>`).join("");
        return `
          <details style="cursor:pointer;">
            <summary style="color:#9ab0c9">${latest} <span style="color:#6f849c">(+${arr.length - 1} more)</span></summary>
            <div style="margin-top:6px; color:#9ab0c9">${extra}</div>
          </details>
        `;
      };
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
              <div><span class="mobile-k">State</span> <span class="mobile-v">${s.final_state || "setup_pass"}</span></div>
              <div><span class="mobile-k">Disposition</span> <span class="mobile-v">${s.post_setup_detail || "-"}</span></div>
              <div><span class="mobile-k">Reason</span> <span class="mobile-v">${s.reason || "-"}</span></div>
              <div><span class="mobile-k">Timeline</span> <span class="mobile-v">${timelineDetails(s.state_timeline)}</span></div>
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
          <td>${s.final_state || "setup_pass"}</td>
          <td>${s.post_setup_detail || "-"}</td>
          <td>${s.reason || "-"}</td>
          <td style="max-width:360px; white-space:normal; color:#9ab0c9; font-size:11px">${timelineDetails(s.state_timeline)}</td>
        </tr>`).join("");
      el.innerHTML = `<table><thead><tr><th>Time</th><th>Symbol</th><th>Dir</th><th>RVOL</th><th>RSI</th><th>IVR %</th><th>Final State</th><th>Disposition Detail</th><th>Reason</th><th>Recent Timeline</th></tr></thead><tbody>${rows}</tbody></table>`;
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
      try {
        const res = await fetch(endpoint, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
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

    async function closeAllPositions() {
      const confirmed = window.confirm("Are you sure you want to close ALL positions? This action cannot be undone.");
      if (!confirmed) {
        return;
      }
      
      try {
        const res = await fetch("/api/control/close-all-positions", {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
        });
        const body = await res.json();
        if (!res.ok || body.error) {
          alert(`Close all positions failed: ${body.error || "request failed"}`);
          return;
        }
        const resultRows = Array.isArray(body.results)
          ? body.results.map((r) => {
              const symbol = String((r && r.symbol) || "unknown");
              if (r && r.error) return symbol + " (ERROR: " + String(r.error) + ")";
              return symbol + " closed";
            })
          : [];
        const detailText = resultRows.length ? resultRows.join("\\n") : "No position details returned";
        alert("Success: " + String(body.message || "request completed") + "\\n\\nDetails:\\n" + detailText);
      } catch (e) {
        alert(`Close all positions request failed: ${e.message}`);
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
      const res = await fetch("/api/runtime-control", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
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
          const crashMsg = String(status.trader_thread_last_crash || "").trim();
          const crashShort = crashMsg ? ` | last crash: ${crashMsg.slice(0, 90)}` : "";
          loopEl.textContent = `Trader Loop: NOT RUNNING (${age})${crashShort}`;
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
      const independentStoplossEl = document.getElementById("independent-stoploss-status");
      if (independentStoplossEl) {
        if (status.error) {
          independentStoplossEl.textContent = "Independent SL: status unavailable";
          independentStoplossEl.style.color = "var(--muted)";
        } else {
          const triggerAt = String(status.independent_stoploss_last_trigger_et || "").trim();
          const symbol = String(status.independent_stoploss_last_symbol || "").trim();
          const qty = Number(status.independent_stoploss_last_qty || 0);
          const lossUsd = Number(status.independent_stoploss_last_unrealized_usd || 0);
          if (triggerAt && symbol) {
            independentStoplossEl.textContent = (
              `Independent SL: ${symbol} qty=${qty} loss=${fmtMoney(lossUsd)} at ${triggerAt}`
            );
            independentStoplossEl.style.color = "var(--yellow)";
          } else {
            independentStoplossEl.textContent = "Independent SL: no trigger recorded";
            independentStoplossEl.style.color = "var(--muted)";
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
          const setupValid = Number(scansummary.setup_valid_count ?? scansummary.setup_passed_count ?? scansummary.scanner_pass_count ?? scansummary.pass_count ?? 0);
          const rejected = Number(scansummary.rejected_count ?? scansummary.fail_count ?? 0);
          const candidates = Number(scansummary.universe_candidates ?? (setupValid + rejected));
          const entryEligible = Number(scansummary.entry_eligible_count ?? setupValid);
          const entryRejected = Number(scansummary.entry_rejected_count ?? 0);
          const brokerFillsTotal = Number(
            scansummary.orders_total_filled_count
            ?? scansummary.trades_filled_count
            ?? scansummary.orders_filled_count
            ?? 0
          );
          const brokerBuyFills = Number(scansummary.orders_buy_filled_count ?? scansummary.orders_filled_count ?? 0);
          const brokerSellFills = Number(scansummary.orders_sell_filled_count ?? 0);
          const ordersSubmitted = Number(scansummary.orders_submitted_count ?? 0);
          const ordersRejectedOrCanceled = Number(scansummary.orders_rejected_or_canceled_count ?? 0);
          const realizedPnlUsd = Number(scansummary.realized_pnl_usd ?? 0);
          const color = brokerFillsTotal > 0 ? "#00c853" : "#ff9800";
          const marketOpen = Boolean(status && !status.error && status.market_open);
          const stageFailCounts = scansummary.stage_fail_counts && typeof scansummary.stage_fail_counts === "object"
            ? scansummary.stage_fail_counts
            : {};
          const stageFailText = Object.entries(stageFailCounts)
            .sort((a, b) => Number(b[1]) - Number(a[1]))
            .slice(0, 4)
            .map(([k, v]) => `${k}: ${v}`)
            .join(" | ");
          const stage4RejectReasons = scansummary.stage4_entry_reject_reasons && typeof scansummary.stage4_entry_reject_reasons === "object"
            ? scansummary.stage4_entry_reject_reasons
            : {};
          const stage4RejectText = Object.entries(stage4RejectReasons)
            .sort((a, b) => Number(b[1]) - Number(a[1]))
            .slice(0, 4)
            .map(([k, v]) => `${k}: ${v}`)
            .join(" | ");
          const stage4Source = String(scansummary.entry_stage4_source || "proxy_scanner_pass");
          const stage4Fresh = Boolean(scansummary.entry_stage4_fresh);
          sumEl.innerHTML = `
              <span style="color:${color}">✓ Broker Fills Today: ${brokerFillsTotal} (buy ${brokerBuyFills} / sell ${brokerSellFills})</span>
              &nbsp;|&nbsp;
              <span style="color:#888">Scanner Pass (signal-valid): ${setupValid}</span>
              &nbsp;|&nbsp;
              <span style="color:#888">Universe Candidates: ${candidates}</span>
              &nbsp;|&nbsp;
              <span style="color:#888">Eligible: ${entryEligible}</span>
              &nbsp;|&nbsp;
              <span style="color:#888">Orders Submitted: ${ordersSubmitted}</span>
              &nbsp;|&nbsp;
              <span style="color:#888">Orders Rejected/Canceled: ${ordersRejectedOrCanceled}</span>
              &nbsp;|&nbsp;
              <span style="color:#888">Setup Rejected: ${rejected}</span>
              &nbsp;|&nbsp;
              <span style="color:#888">Entry Rejected: ${entryRejected}</span>
              &nbsp;|&nbsp;
              <span style="color:${realizedPnlUsd >= 0 ? "#00c853" : "#ff5252"}">Realized P&L: ${fmtMoney(realizedPnlUsd)}</span>
              &nbsp;|&nbsp;
              Last Scan: ${scansummary.last_scan || "—"}
              <br><small style="color:#888">Top fail reason: ${scansummary.top_fail_reason || scansummary.top_reason || "—"}</small>
              <br><small style="color:#888">Stage rejects: ${stageFailText || "—"}</small>
              <br><small style="color:#888">Stage4 entry rejects: ${stage4RejectText || "—"}</small>
              <br><small style="color:#888">Stage4 source: ${stage4Source}${stage4Fresh ? " (fresh)" : " (fallback/proxy)"}</small>
                ${!stage4Fresh ? `<br><small style="color:#8fa1b8">Fallback/proxy mode: scanner pass does not imply a live entry trigger for this loop.</small>` : ""}
              ${candidates === 0 ? `<br><small style="color:#8fa1b8">No active scan loop yet${marketOpen ? "" : " (market closed)"}.</small>` : ""}
          `;
        } else {
          const fallbackScanRows = Array.isArray(scanlog) ? scanlog.length : 0;
          const fallbackRejectRows = Array.isArray(scanfails) ? scanfails.length : 0;
          if (fallbackScanRows > 0 || fallbackRejectRows > 0) {
            sumEl.textContent = `Scan summary unavailable (scan rows=${fallbackScanRows}, rejects=${fallbackRejectRows}).`;
          } else {
            sumEl.textContent = "No scan data yet";
          }
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
              <td style="color:#8db8ff; font-size:11px">${f.stage || "setup_reject"}</td>
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
