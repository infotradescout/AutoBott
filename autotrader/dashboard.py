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
DASHBOARD_DIR = Path(__file__).resolve().parent
LISA_FEED_JSON_PATH = DASHBOARD_DIR / "autobott_lisa_feed.json"
LISA_FEED_NDJSON_PATH = DASHBOARD_DIR / "autobott_lisa_feed.ndjson"
LISA_FEED_PUBLISHED_PATH = DASHBOARD_DIR / "autobott_lisa_feed_published.json"
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
  # Token auth intentionally disabled per operator preference.
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
        "pass_count": len(pass_rows),
        "fail_count": sum(fail_reasons.values()),
        "unique_symbols": len(symbols),
        "direction_counts": direction_counts,
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
    if int(scan_summary.get("pass_count", 0)) == 0:
        notes.append("Nothing passed the scanner today. Check whether filters were appropriately strict for the tape.")
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


def _build_evening_report_payload() -> dict[str, Any]:
    now = _now_et()
    runtime_state = load_bot_state()
    today_trades = _today_trade_rows()
    today_scans = _today_scan_rows()
    trade_summary = _build_trade_report_summary(today_trades)
    scan_summary = _build_scan_report_summary(today_scans)
    daily_review = _build_daily_review_payload()
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


def _synthesize_lisa_signals() -> dict[str, Any]:
    report = _load_latest_trade_report()
    runtime_state = load_bot_state()
    published_keys = _published_signal_keys()

    signals: list[dict[str, Any]] = []
    now_label = _now_et().strftime("%Y-%m-%d %H:%M:%S ET")

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
        "schema_version": "1.0.0",
        "generated_at": now_label,
        "source_system": "autobott",
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


@app.post("/api/lisa/feed/generate")
def api_lisa_feed_generate():
    try:
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

    function renderSignals(payload) {
      const wrap = document.getElementById("signals");
      if (!wrap) return;
      const rows = Array.isArray(payload && payload.signals) ? payload.signals : [];
      if (!rows.length) {
        wrap.innerHTML = '<div class="muted">No knowledge signals available.</div>';
        return;
      }
      wrap.innerHTML = rows.map((row) => `
        <div class="signal-card">
          <div class="signal-head">
            <div>
              <div class="signal-title">${esc(row.title)}</div>
              <div class="small muted">${esc(row.category)} | ${esc(row.symbol)} | ${esc(row.signal_type)}</div>
            </div>
            <span class="pill ${severityClass(row.severity)}">${esc(row.severity)}</span>
          </div>
          <div class="small" style="margin-bottom:4px;">${esc(row.summary)}</div>
          <div class="small muted" style="margin-bottom:4px;">confidence=${esc(row.confidence)} | novelty=${esc(row.novelty)} | action=${esc(row.recommended_action)}</div>
          <div class="small muted">${metricPairs(row.metrics)}</div>
        </div>
      `).join("");
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
          <div class="metric"><div class="label">Premarket Passes</div><div class="value">${Number(scan.pass_count || 0)}</div></div>
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
      wrap.innerHTML = `
        <div class="muted" style="margin-bottom:10px;">Generated ${escapeHtml(data.generated_at || "-")} | Closed trades ${Number(trades.total_trades || 0)} | Scan rows ${Number(scans.total_rows || 0)}</div>
        <div class="metrics">
          <div class="metric"><div class="label">Today P&L</div><div class="value ${cls(trades.total_pnl_usd)}">${fmtMoney(trades.total_pnl_usd)}</div></div>
          <div class="metric"><div class="label">Win Rate</div><div class="value ${cls((Number(trades.win_rate_pct || 0)) - 50)}">${Number(trades.win_rate_pct || 0).toFixed(1)}%</div></div>
          <div class="metric"><div class="label">Scanner Passes</div><div class="value">${Number(scans.pass_count || 0)}</div></div>
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
            <div class="section-title"><h2>Top Passing Signals</h2></div>
            ${signalTable(scans.top_passes || [])}
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
