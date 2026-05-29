"""AutoBott dashboard v2.

Alpaca is the source of truth for account, positions, filled orders, and
intraday P/L. Local CSV/state files are shown only as context.
"""

from __future__ import annotations

import csv
import json
import os
import re
import socket
import threading
import time as time_module
import traceback
from collections import Counter, defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, time
from pathlib import Path
from typing import Any

import pytz
import requests
from flask import Flask, jsonify, render_template_string, request

from env_config import load_runtime_env

load_runtime_env()
try:
    from autotrader import config
except ImportError:
    import config  # type: ignore

from state_store import load_bot_state, save_bot_state
from trading_control import load_trading_control, set_dry_run, set_manual_stop
from watchlist_control import load_watchlist_control, update_watchlist_control

API_KEY = str(os.getenv("ALPACA_API_KEY") or "").strip()
SECRET_KEY = str(os.getenv("ALPACA_SECRET_KEY") or "").strip()
PAPER = bool(getattr(config, "PAPER", True))
BASE_URL = "https://paper-api.alpaca.markets" if PAPER else "https://api.alpaca.markets"
HEADERS = {"APCA-API-KEY-ID": API_KEY, "APCA-API-SECRET-KEY": SECRET_KEY}
CONTROL_TOKEN = str(getattr(config, "DASHBOARD_CONTROL_TOKEN", "") or "").strip()
EASTERN = pytz.timezone(str(getattr(config, "EASTERN_TZ", "US/Eastern") or "US/Eastern"))
DISPLAY_TZ = pytz.timezone(str(os.getenv("DASHBOARD_DISPLAY_TZ", "America/Chicago") or "America/Chicago"))
SCAN_LOG_CSV = Path(getattr(config, "SCAN_LOG_CSV_PATH"))
RUNTIME_OVERRIDES_PATH = Path(getattr(config, "DATA_DIR")) / "runtime_parameter_overrides.json"
RUNTIME_PRESETS_PATH = Path(getattr(config, "DATA_DIR")) / "runtime_parameter_presets.json"

RUNTIME_PARAM_SPECS: dict[str, dict[str, Any]] = {
    "MIN_SIGNAL_SCORE": {"type": "float", "min": 0.0, "max": 20.0},
    "DIRECTION_CONVICTION_MIN": {"type": "float", "min": 0.0, "max": 1.0},
    "RVOL_MIN": {"type": "float", "min": 0.0, "max": 10.0},
    "ATR_PCT_MIN": {"type": "float", "min": 0.0, "max": 20.0},
    "MOVEMENT_FORCE_MIN_PCT": {"type": "float", "min": 0.0, "max": 10.0},
    "FAST_START_MIN_SIGNAL_SCORE": {"type": "float", "min": 0.0, "max": 20.0},
    "FAST_START_MIN_DIRECTION_SCORE": {"type": "float", "min": 0.0, "max": 1.0},
    "FAST_START_MIN_RVOL": {"type": "float", "min": 0.0, "max": 10.0},
    "FAST_START_MIN_ABS_ROC_PCT": {"type": "float", "min": 0.0, "max": 10.0},
    "FAST_START_MIN_VWAP_DISTANCE_PCT": {"type": "float", "min": 0.0, "max": 10.0},
    "ENTRY_MAX_QUOTE_SPREAD_PCT": {"type": "float", "min": 0.1, "max": 100.0},
    "MAX_PREMIUM_PER_TRADE_USD": {"type": "float", "min": 0.0, "max": 1000000.0},
    "MAX_CONTRACTS_PER_ENTRY": {"type": "int", "min": 0, "max": 1000},
    "ENTRY_LIMIT_ATTEMPTS": {"type": "int", "min": 1, "max": 20},
    "ENABLE_ENTRY_MARKET_FALLBACK": {"type": "bool"},
    "PORTFOLIO_ALLOCATION_PCT": {"type": "float", "min": 0.0, "max": 100.0},
    "MAX_PREMIUM_PER_TRADE_PCT_OF_ALLOCATION": {"type": "float", "min": 0.0, "max": 100.0},
    "MAX_CONCURRENT_TRADES": {"type": "int", "min": 0, "max": 1000},
    "MAX_SAME_DIRECTION_POSITIONS": {"type": "int", "min": 0, "max": 1000},
    "STOP_LOSS_PCT": {"type": "float", "min": 0.0, "max": 1.0},
    "DAILY_LOSS_LIMIT_USD": {"type": "float", "min": 0.0, "max": 1000000.0},
}

app = Flask(__name__)


def _env_bool(name: str, default: bool) -> bool:
    raw = str(os.getenv(name, "") or "").strip().lower()
    if not raw:
        return bool(default)
    return raw in {"1", "true", "yes", "y", "on"}


def _env_int(name: str, default: int, *, minimum: int, maximum: int) -> int:
    raw = str(os.getenv(name, "") or "").strip()
    if not raw:
        return max(minimum, min(maximum, int(default)))
    try:
        value = int(float(raw))
    except ValueError:
        value = int(default)
    return max(minimum, min(maximum, value))


TRADER_HEARTBEAT_STALE_SECONDS = _env_int(
    "TRADER_HEARTBEAT_STALE_SECONDS",
    1800,
    minimum=300,
    maximum=86400,
)
ENABLE_EMBEDDED_TRADER_FALLBACK = _env_bool("ENABLE_EMBEDDED_TRADER_FALLBACK", True)

_embedded_trader_lock = threading.Lock()
_embedded_trader_thread: threading.Thread | None = None


def _key_hint(value: str) -> str:
    token = str(value or "").strip()
    if len(token) <= 8:
        return token[:2] + "***" if token else ""
    return f"{token[:4]}...{token[-4:]}"


def _deployment_meta() -> dict[str, Any]:
    meta = {
        "service_name": str(os.getenv("RENDER_SERVICE_NAME", "") or ""),
        "service_id": str(os.getenv("RENDER_SERVICE_ID", "") or ""),
        "instance_id": str(os.getenv("RENDER_INSTANCE_ID", "") or ""),
        "git_commit": str(os.getenv("RENDER_GIT_COMMIT", "") or ""),
        "git_branch": str(os.getenv("RENDER_GIT_BRANCH", "") or ""),
        "external_url": str(os.getenv("RENDER_EXTERNAL_URL", "") or ""),
        "region": str(os.getenv("RENDER_REGION", "") or ""),
        "hostname": socket.gethostname(),
        "alpaca_key_hint": _key_hint(API_KEY),
        "paper": bool(PAPER),
    }
    return {key: value for key, value in meta.items() if value not in ("", None)}


def _patch_runtime_state(updates: dict[str, Any]) -> None:
    try:
        state = load_bot_state()
        if not isinstance(state, dict):
            state = {}
        state.update(updates)
        save_bot_state(state)
    except Exception as exc:  # noqa: BLE001
        print(f"[dashboard_v2] runtime state patch failed: {exc}")


def _apply_boot_auto_resume_for_direct_dashboard() -> None:
    if not _env_bool("AUTO_RESUME_TRADING_ON_BOOT", True):
        return
    try:
        control = load_trading_control()
        if bool(control.get("manual_stop", False)):
            updated = set_manual_stop(False, reason="boot_auto_resume_dashboard_v2")
            print(
                "[dashboard_v2] AUTO_RESUME_TRADING_ON_BOOT cleared manual_stop "
                f"(previous reason={str(control.get('reason', '') or '')!r}, "
                f"updated_at={str(updated.get('updated_at_et', '') or '')!r})."
            )
    except Exception as exc:  # noqa: BLE001
        print(f"[dashboard_v2] boot auto-resume failed: {exc}")


def _run_trader_forever_embedded() -> None:
    restart_count = 0
    while True:
        restart_count += 1
        _patch_runtime_state(
            {
                "trader_thread_last_start_et": _now_et().isoformat(),
                "trader_thread_restart_count": restart_count,
                "embedded_trader_runner": "dashboard_v2_fallback",
            }
        )
        try:
            from main import main as trader_main  # local import to avoid circular import during app bootstrap

            trader_main()
        except Exception as exc:  # noqa: BLE001
            print(f"[dashboard_v2] Embedded trader crashed: {exc}")
            traceback.print_exc()
            _patch_runtime_state(
                {
                    "trader_thread_last_crash_et": _now_et().isoformat(),
                    "trader_thread_last_crash": str(exc)[:500],
                }
            )
        finally:
            _patch_runtime_state({"trader_thread_last_stop_et": _now_et().isoformat()})
        time_module.sleep(30)


def _heartbeat_age_seconds(state: dict[str, Any]) -> int | None:
    heartbeat = _parse_dt(state.get("last_trader_heartbeat_et"))
    if heartbeat is None:
        return None
    return max(0, int((_now_et() - heartbeat).total_seconds()))


def _start_embedded_trader_if_stale(trigger: str) -> dict[str, Any]:
    global _embedded_trader_thread
    if not ENABLE_EMBEDDED_TRADER_FALLBACK:
        return {"enabled": False, "started": False, "reason": "disabled"}
    with _embedded_trader_lock:
        if _embedded_trader_thread is not None and _embedded_trader_thread.is_alive():
            return {"enabled": True, "started": False, "reason": "already_running"}
        _apply_boot_auto_resume_for_direct_dashboard()
        control = load_trading_control()
        if not isinstance(control, dict):
            control = {}
        if bool(control.get("manual_stop", False)):
            return {"enabled": True, "started": False, "reason": "manual_stop_active"}
        state = load_bot_state()
        if not isinstance(state, dict):
            state = {}
        age = _heartbeat_age_seconds(state)
        if age is not None and age < TRADER_HEARTBEAT_STALE_SECONDS:
            return {"enabled": True, "started": False, "reason": "heartbeat_fresh", "heartbeat_age_seconds": age}
        _embedded_trader_thread = threading.Thread(
            target=_run_trader_forever_embedded,
            name="embedded-trader-fallback",
            daemon=True,
        )
        _embedded_trader_thread.start()
        now_iso = _now_et().isoformat()
        _patch_runtime_state(
            {
                "embedded_trader_boot_at_et": now_iso,
                "embedded_trader_boot_trigger": str(trigger or ""),
                "embedded_trader_boot_prev_heartbeat_age_seconds": age,
            }
        )
        print(
            "[dashboard_v2] Embedded trader fallback started "
            f"(trigger={trigger}, previous_heartbeat_age_seconds={age})."
        )
        return {
            "enabled": True,
            "started": True,
            "trigger": str(trigger or ""),
            "started_at_et": now_iso,
            "previous_heartbeat_age_seconds": age,
        }


@dataclass(frozen=True)
class AlpacaResult:
    ok: bool
    data: Any
    status_code: int = 200
    error: str = ""


def _now_et() -> datetime:
    return datetime.now(EASTERN)


def _today_start_et() -> datetime:
    now = _now_et()
    return EASTERN.localize(datetime.combine(now.date(), time(0, 0, 0)))


def _iso_utc(dt: datetime) -> str:
    return dt.astimezone(pytz.UTC).isoformat().replace("+00:00", "Z")


def _money(value: Any) -> float:
    try:
        return round(float(value), 2)
    except (TypeError, ValueError):
        return 0.0


def _num(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _parse_dt(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            return EASTERN.localize(dt)
        return dt.astimezone(EASTERN)
    except ValueError:
        pass
    for suffix, zone in ((" EDT", EASTERN), (" EST", EASTERN), (" CDT", DISPLAY_TZ), (" CST", DISPLAY_TZ)):
        if raw.upper().endswith(suffix.strip()):
            base = raw[: -len(suffix)].strip()
            try:
                return zone.localize(datetime.strptime(base, "%Y-%m-%d %H:%M:%S")).astimezone(EASTERN)
            except ValueError:
                return None
    try:
        return EASTERN.localize(datetime.strptime(raw, "%Y-%m-%d %H:%M:%S"))
    except ValueError:
        return None


def _fmt_local(value: Any) -> str:
    dt = _parse_dt(value)
    if dt is None:
        return str(value or "")
    local = dt.astimezone(DISPLAY_TZ)
    return f"{local.strftime('%H:%M:%S')} {local.tzname()}"


def _alpaca(method: str, path: str, *, params: dict[str, Any] | None = None, timeout: int = 12) -> AlpacaResult:
    if not API_KEY or not SECRET_KEY:
        return AlpacaResult(False, None, 503, "Alpaca API keys are missing")
    try:
        response = requests.request(method, f"{BASE_URL}{path}", headers=HEADERS, params=params or {}, timeout=timeout)
        if response.status_code >= 400:
            return AlpacaResult(False, None, response.status_code, response.text[:1000])
        if not response.text:
            return AlpacaResult(True, None, response.status_code, "")
        return AlpacaResult(True, response.json(), response.status_code, "")
    except Exception as exc:  # noqa: BLE001
        return AlpacaResult(False, None, 500, str(exc))


def _read_csv_tail(path: Path, limit: int = 2500) -> list[dict[str, str]]:
    if not path.exists():
        return []
    try:
        with path.open("r", newline="", encoding="utf-8") as handle:
            return list(deque(csv.DictReader(handle), maxlen=max(1, int(limit))))
    except Exception:
        return []


def _read_csv_last_row(path: Path) -> dict[str, str]:
    rows = _read_csv_tail(path, limit=1)
    if not rows:
        return {}
    return dict(rows[-1])


def _read_json_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _underlying_from_option(symbol: str) -> str:
    match = re.match(r"^([A-Z.]+)\d{6}[CP]\d{8}$", str(symbol or "").upper())
    return match.group(1) if match else str(symbol or "").upper()


def _is_option_symbol(symbol: str) -> bool:
    return bool(re.match(r"^[A-Z.]+\d{6}[CP]\d{8}$", str(symbol or "").upper()))


def _order_fill_price(order: dict[str, Any]) -> float:
    for key in ("filled_avg_price", "limit_price"):
        value = _num(order.get(key))
        if value > 0:
            return value
    return 0.0


def _all_orders_today() -> list[dict[str, Any]]:
    result = _alpaca(
        "GET",
        "/v2/orders",
        params={"status": "all", "after": _iso_utc(_today_start_et()), "limit": 500, "direction": "desc", "nested": "false"},
    )
    if not result.ok or not isinstance(result.data, list):
        return []
    orders = []
    for order in result.data:
        submitted = _parse_dt(order.get("submitted_at") or order.get("created_at"))
        if submitted is not None and submitted.date() == _now_et().date():
            orders.append(order)
    return orders


def _realized_from_orders(orders: list[dict[str, Any]]) -> dict[str, Any]:
    chronological = sorted(orders, key=lambda item: str(item.get("filled_at") or item.get("submitted_at") or ""))
    lots: dict[str, list[dict[str, Any]]] = defaultdict(list)
    closed: list[dict[str, Any]] = []

    for order in chronological:
        if str(order.get("status", "") or "").lower() != "filled":
            continue
        symbol = str(order.get("symbol", "") or "").upper()
        if not _is_option_symbol(symbol):
            continue
        side = str(order.get("side", "") or "").lower()
        qty = _num(order.get("filled_qty") or order.get("qty"))
        price = _order_fill_price(order)
        if qty <= 0 or price <= 0:
            continue
        order_time = _parse_dt(order.get("filled_at") or order.get("submitted_at") or "")
        if side == "buy":
            lots[symbol].append({"qty": qty, "price": price, "filled_at": order_time})
            continue
        if side != "sell":
            continue
        remaining = qty
        realized = 0.0
        cost_basis = 0.0
        paired_qty = 0.0
        first_entry_at = None
        while remaining > 0 and lots[symbol]:
            lot = lots[symbol][0]
            use_qty = min(remaining, lot["qty"])
            realized += (price - lot["price"]) * use_qty * 100.0
            cost_basis += lot["price"] * use_qty * 100.0
            paired_qty += use_qty
            lot_time = lot.get("filled_at")
            if isinstance(lot_time, datetime) and (first_entry_at is None or lot_time < first_entry_at):
                first_entry_at = lot_time
            lot["qty"] -= use_qty
            remaining -= use_qty
            if lot["qty"] <= 0.000001:
                lots[symbol].pop(0)
        if paired_qty > 0:
            hold_seconds = 0
            if isinstance(first_entry_at, datetime) and isinstance(order_time, datetime):
                hold_seconds = max(0, int((order_time - first_entry_at).total_seconds()))
            closed.append(
                {
                    "symbol": symbol,
                    "underlying": _underlying_from_option(symbol),
                    "qty": round(paired_qty, 4),
                    "entry_price": round(cost_basis / (paired_qty * 100.0), 4) if paired_qty > 0 else 0.0,
                    "sell_price": round(price, 4),
                    "realized_pnl_usd": round(realized, 2),
                    "realized_pnl_pct": round((realized / cost_basis) * 100.0, 2) if cost_basis > 0 else 0.0,
                    "entry_time": first_entry_at.isoformat() if isinstance(first_entry_at, datetime) else "",
                    "filled_at": str(order.get("filled_at") or order.get("submitted_at") or ""),
                    "hold_seconds": hold_seconds,
                }
            )

    wins = [item for item in closed if float(item["realized_pnl_usd"]) > 0]
    losses = [item for item in closed if float(item["realized_pnl_usd"]) < 0]
    gross_profit = round(sum(float(item["realized_pnl_usd"]) for item in wins), 2)
    gross_loss = round(abs(sum(float(item["realized_pnl_usd"]) for item in losses)), 2)
    by_underlying: dict[str, float] = defaultdict(float)
    for item in closed:
        by_underlying[str(item["underlying"])] += float(item["realized_pnl_usd"])
    return {"closed_trades": closed, "closed_count": len(closed), "wins": len(wins), "losses": len(losses), "win_rate_pct": round((len(wins) / len(closed)) * 100.0, 2) if closed else 0.0, "realized_pnl_usd": round(sum(float(item["realized_pnl_usd"]) for item in closed), 2), "gross_profit_usd": gross_profit, "gross_loss_usd": gross_loss, "profit_factor": round(gross_profit / gross_loss, 4) if gross_loss > 0 else None, "best_trade": max(wins, key=lambda item: float(item["realized_pnl_usd"]), default=None), "least_bad_trade": max(losses, key=lambda item: float(item["realized_pnl_usd"]), default=None), "worst_trade": min(closed, key=lambda item: float(item["realized_pnl_usd"]), default=None), "by_underlying": sorted([{"symbol": key, "pnl_usd": round(value, 2)} for key, value in by_underlying.items()], key=lambda item: float(item["pnl_usd"]), reverse=True)}


def _positions() -> list[dict[str, Any]]:
    result = _alpaca("GET", "/v2/positions")
    if not result.ok or not isinstance(result.data, list):
        return []
    positions = []
    for pos in result.data:
        symbol = str(pos.get("symbol", "") or "").upper()
        positions.append({"symbol": symbol, "underlying": _underlying_from_option(symbol), "qty": _num(pos.get("qty")), "avg_entry_price": _num(pos.get("avg_entry_price")), "current_price": _num(pos.get("current_price")), "market_value": _money(pos.get("market_value")), "unrealized_pl": _money(pos.get("unrealized_pl")), "unrealized_plpc": round(_num(pos.get("unrealized_plpc")) * 100.0, 2), "asset_class": str(pos.get("asset_class", "") or "")})
    return sorted(positions, key=lambda item: float(item["unrealized_pl"]), reverse=True)


def _account() -> dict[str, Any]:
    result = _alpaca("GET", "/v2/account")
    if not result.ok or not isinstance(result.data, dict):
        return {"error": result.error or "account unavailable"}
    raw = result.data
    return {
        "id": str(raw.get("id", "") or ""),
        "account_number": str(raw.get("account_number", "") or ""),
        "currency": str(raw.get("currency", "") or ""),
        "equity": _money(raw.get("equity")),
        "cash": _money(raw.get("cash")),
        "buying_power": _money(raw.get("buying_power")),
        "portfolio_value": _money(raw.get("portfolio_value")),
        "last_equity": _money(raw.get("last_equity")),
        "status": str(raw.get("status", "") or ""),
        "trading_blocked": bool(raw.get("trading_blocked", False)),
        "account_blocked": bool(raw.get("account_blocked", False)),
        "transfers_blocked": bool(raw.get("transfers_blocked", False)),
        "pattern_day_trader": bool(raw.get("pattern_day_trader", False)),
        "options_approved_level": str(raw.get("options_approved_level", "") or ""),
        "options_trading_level": str(raw.get("options_trading_level", "") or ""),
        "mode": "paper" if PAPER else "live",
    }


def _clock() -> dict[str, Any]:
    result = _alpaca("GET", "/v2/clock")
    if not result.ok or not isinstance(result.data, dict):
        return {"is_open": None, "error": result.error or "clock unavailable"}
    return result.data


def _scanner_summary() -> dict[str, Any]:
    rows = _read_csv_tail(SCAN_LOG_CSV, limit=5000)
    today = _now_et().date()
    today_rows = []
    for row in rows:
        dt = _parse_dt(row.get("timestamp", ""))
        if dt is not None and dt.date() == today:
            today_rows.append(row)
    pass_rows = [r for r in today_rows if str(r.get("result", "") or "").lower() == "pass"]
    fail_rows = [r for r in today_rows if str(r.get("result", "") or "").lower() == "fail"]
    reason_counts = Counter(str(r.get("reason", "") or "unknown") for r in fail_rows)
    return {"scan_rows_today": len(today_rows), "candidate_passes": len(pass_rows), "passes": 0, "fails": len(fail_rows), "pass_rate_pct": 0.0, "last_scan": str(today_rows[-1].get("timestamp", "") or "") if today_rows else "", "top_fail_reasons": [{"reason": key, "count": value} for key, value in reason_counts.most_common(8)], "recent_rows": list(reversed(today_rows[-20:]))}


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _int_value(value: Any) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _entry_debug_summary(state: dict[str, Any]) -> dict[str, Any]:
    debug = _as_dict(state.get("last_entry_debug"))
    reasons = _as_dict(debug.get("entry_stage4_reject_reasons"))
    top_reasons = [
        {"reason": str(reason), "count": _int_value(count)}
        for reason, count in sorted(reasons.items(), key=lambda item: _int_value(item[1]), reverse=True)[:8]
    ]
    top_label = "--"
    if top_reasons:
        first = top_reasons[0]
        top_label = f"{first['reason']} ({first['count']})"
    return {
        "loop_ts_et": str(debug.get("loop_ts_et", "") or ""),
        "signals_considered": _int_value(debug.get("signals_considered")),
        "entry_stage4_eligible_count": _int_value(debug.get("entry_stage4_eligible_count")),
        "entry_stage4_reject_count": _int_value(debug.get("entry_stage4_reject_count")),
        "entry_orders_submitted": _int_value(debug.get("entry_orders_submitted")),
        "entries_filled": _int_value(debug.get("entries_filled")),
        "top_reject_reason": top_label,
        "top_reject_reasons": top_reasons,
        "trade_through_kpi": _as_dict(debug.get("trade_through_kpi")),
        "raw": debug,
    }


def _truth_loss_profile(realized: dict[str, Any]) -> dict[str, Any]:
    losses = [
        item
        for item in list(realized.get("closed_trades") or [])
        if _num(item.get("realized_pnl_usd")) < 0
    ]
    if not losses:
        return {}

    causes: Counter[str] = Counter()
    ticker_losses: Counter[str] = Counter()
    diagnoses: list[dict[str, Any]] = []
    quick_seconds = int(getattr(config, "ADAPTIVE_LOSS_QUICK_SECONDS", 180) or 180)
    for item in losses[-int(getattr(config, "ADAPTIVE_LOSS_CAUSE_WINDOW", 12) or 12) :]:
        ticker = str(item.get("underlying", "") or "").upper()
        hold_seconds = int(_num(item.get("hold_seconds")))
        pnl_pct = _num(item.get("realized_pnl_pct"))
        if hold_seconds > 0 and hold_seconds <= quick_seconds:
            cause = "rapid_stopout"
            action = "require stronger momentum before next entry"
        elif ticker and sum(1 for loss in losses if str(loss.get("underlying", "") or "").upper() == ticker) >= 2:
            cause = "repeated_ticker_loss"
            action = "block ticker for the day"
        elif pnl_pct <= -10:
            cause = "large_option_decay_or_wrong_way"
            action = "raise signal and direction gates"
        else:
            cause = "timing_or_decay"
            action = "raise signal quality and cool ticker"
        causes[cause] += 1
        if ticker:
            ticker_losses[ticker] += 1
        diagnoses.append(
            {
                "ticker": ticker,
                "symbol": str(item.get("symbol", "") or ""),
                "cause": cause,
                "action": action,
                "hold_seconds": hold_seconds,
                "realized_pnl_usd": _money(item.get("realized_pnl_usd")),
                "realized_pnl_pct": round(pnl_pct, 4),
            }
        )

    dominant_cause = causes.most_common(1)[0][0] if causes else ""
    loss_count = len(diagnoses)
    min_signal = min(
        float(getattr(config, "ADAPTIVE_LOSS_MAX_SIGNAL_SCORE", 9.2)),
        float(getattr(config, "ADAPTIVE_LOSS_MIN_SIGNAL_SCORE", 7.8))
        + loss_count * float(getattr(config, "ADAPTIVE_LOSS_SIGNAL_SCORE_ADD_PER_LOSS", 0.15)),
    )
    min_direction = float(getattr(config, "ADAPTIVE_LOSS_MIN_DIRECTION_SCORE", 0.65))
    max_spread = float(getattr(config, "ADAPTIVE_LOSS_MAX_SPREAD_PCT", 4.0))
    return {
        "source": "dashboard_alpaca_truth",
        "source_closed_count": int(realized.get("closed_count", 0) or 0),
        "loss_count": loss_count,
        "dominant_cause": dominant_cause,
        "causes": dict(causes),
        "ticker_losses": dict(ticker_losses),
        "min_signal_score": round(min_signal, 4),
        "min_direction_score": round(min_direction, 4),
        "max_spread_pct": round(max_spread, 4),
        "diagnoses": diagnoses[-5:],
        "last_diagnosis": diagnoses[-1] if diagnoses else {},
        "updated_at_et": _now_et().isoformat(),
    }


def _runtime() -> dict[str, Any]:
    state = load_bot_state()
    if not isinstance(state, dict):
        state = {}
    control = load_trading_control()
    if not isinstance(control, dict):
        control = {}
    replay_events_path = Path(getattr(config, "DATA_DIR")) / "replay_auto_promote_events.csv"
    trainer_status_path = Path(str(getattr(config, "SYNTHETIC_TRAINER_STATUS_PATH", Path(getattr(config, "DATA_DIR")) / "synthetic_trainer_status.json")))
    tuner_status_path = Path(str(getattr(config, "SYNTHETIC_TUNER_STATUS_PATH", Path(getattr(config, "DATA_DIR")) / "synthetic_tuner_status.json")))
    learning_summary_path = Path(getattr(config, "DATA_DIR")) / "decision_learning_summary.json"
    trainer_status = _read_json_file(trainer_status_path)
    tuner_status = _read_json_file(tuner_status_path)
    learning_summary = _read_json_file(learning_summary_path)
    age = _heartbeat_age_seconds(state)
    stale = bool(age is None or age >= TRADER_HEARTBEAT_STALE_SECONDS)
    return {
        "heartbeat_age_seconds": age,
        "heartbeat_label": f"{age}s ago" if age is not None else "unknown",
        "trader_loop_stale": stale,
        "trader_loop_stale_after_seconds": TRADER_HEARTBEAT_STALE_SECONDS,
        "manual_stop": bool(control.get("manual_stop", False)),
        "dry_run": bool(control.get("dry_run", False)),
        "control": control,
        "state_updated_at": str(state.get("_state_updated_at_iso", "") or ""),
        "broker_truth_day_pnl_usd": _num(state.get("broker_truth_day_pnl_usd")),
        "broker_truth_closed_count": _int_value(state.get("broker_truth_closed_count")),
        "broker_truth_last_error": str(state.get("broker_truth_last_error", "") or ""),
        "trader_thread_last_start_et": str(state.get("trader_thread_last_start_et", "") or ""),
        "trader_thread_last_stop_et": str(state.get("trader_thread_last_stop_et", "") or ""),
        "trader_thread_restart_count": _int_value(state.get("trader_thread_restart_count")),
        "trader_thread_last_crash_et": str(state.get("trader_thread_last_crash_et", "") or ""),
        "trader_thread_last_crash": str(state.get("trader_thread_last_crash", "") or ""),
        "embedded_trader_boot_at_et": str(state.get("embedded_trader_boot_at_et", "") or ""),
        "embedded_trader_boot_trigger": str(state.get("embedded_trader_boot_trigger", "") or ""),
        "embedded_trader_boot_prev_heartbeat_age_seconds": state.get("embedded_trader_boot_prev_heartbeat_age_seconds"),
        "adaptive_loss": {
            "active": bool(state.get("adaptive_loss_active", False)),
            "blocked_tickers": list(state.get("adaptive_loss_blocked_tickers") or []),
            "profile": _as_dict(state.get("adaptive_loss_profile")),
        },
        "replay_auto_promote": _as_dict(state.get("replay_auto_promote_status")),
        "replay_auto_promote_events_path": str(replay_events_path),
        "replay_auto_promote_last_event": _read_csv_last_row(replay_events_path),
        "entry_debug": _entry_debug_summary(state),
        "learning": {
            "summary_path": str(learning_summary_path),
            "summary_exists": learning_summary_path.exists(),
            "quality_verdict": str(_as_dict(learning_summary.get("learning_quality")).get("verdict", "") or ""),
            "recent_quality": _as_dict(learning_summary.get("recent_quality")),
            "rollback_signal": _as_dict(learning_summary.get("rollback_signal")),
            "persisted_decisions": _int_value(_as_dict(learning_summary.get("totals")).get("persisted_decisions")),
            "score_total": _int_value(_as_dict(learning_summary.get("totals")).get("score_total")),
        },
        "pattern_guard": {
            "override_disabled_until_iso": str(state.get("pattern_override_disabled_until_iso", "") or ""),
            "override_disable_reason": str(state.get("pattern_override_disable_reason", "") or ""),
            "shadow_stats": _as_dict(state.get("shadow_pattern_stats")),
            "suppressed_symbols": list(state.get("learning_suppressed_symbols") or []),
            "suppression_reason": str(state.get("learning_suppressed_symbols_reason", "") or ""),
            "suppression_refreshed_at_iso": str(state.get("learning_suppressed_symbols_refreshed_at_iso", "") or ""),
        },
        "synthetic_trainer": {
            "path": str(trainer_status_path),
            "exists": trainer_status_path.exists(),
            "last_updated_et": str(trainer_status.get("updated_at_et", "") or ""),
            "rows_written_total": _int_value(trainer_status.get("rows_written_total")),
            "wins": _int_value(trainer_status.get("wins")),
            "losses": _int_value(trainer_status.get("losses")),
            "win_rate_pct": round(_num(trainer_status.get("win_rate_pct")), 2),
            "recent_trades": list(trainer_status.get("recent_trades") or [])[:8],
        },
        "synthetic_tuner": {
            "path": str(tuner_status_path),
            "exists": tuner_status_path.exists(),
            "last_updated_et": str(tuner_status.get("updated_at_et", "") or ""),
            "trades_seen": _int_value(tuner_status.get("trades_seen")),
            "wins": _int_value(tuner_status.get("wins")),
            "losses": _int_value(tuner_status.get("losses")),
            "win_rate_pct": round(_num(tuner_status.get("win_rate_pct")), 2),
            "last_applied_overrides": _as_dict(tuner_status.get("last_applied_overrides")),
        },
    }


def _compact_order(order: dict[str, Any]) -> dict[str, Any]:
    return {"symbol": str(order.get("symbol", "") or ""), "side": str(order.get("side", "") or ""), "type": str(order.get("type", "") or ""), "status": str(order.get("status", "") or ""), "qty": _num(order.get("qty")), "filled_qty": _num(order.get("filled_qty")), "filled_avg_price": _num(order.get("filled_avg_price")), "limit_price": _num(order.get("limit_price")), "display_time": _fmt_local(order.get("filled_at") or order.get("submitted_at")), "submitted_at": str(order.get("submitted_at", "") or ""), "filled_at": str(order.get("filled_at", "") or "")}


def _truth_payload() -> dict[str, Any]:
    embedded_boot = _start_embedded_trader_if_stale("api_truth")
    all_orders = _all_orders_today()
    filled_orders = [o for o in all_orders if str(o.get("status", "") or "").lower() == "filled"]
    filled_option_orders = [o for o in filled_orders if _is_option_symbol(str(o.get("symbol", "") or ""))]
    filled_option_entry_orders = [
        o for o in filled_option_orders if str(o.get("side", "") or "").lower() == "buy"
    ]
    realized = _realized_from_orders(filled_option_orders)
    positions = _positions()
    unrealized = round(sum(float(p.get("unrealized_pl", 0.0)) for p in positions), 2)
    realized["open_unrealized_pnl_usd"] = unrealized
    realized["total_intraday_pnl_usd"] = round(float(realized["realized_pnl_usd"]) + unrealized, 2)
    runtime = _runtime()
    trade_kpi = _as_dict(_as_dict(runtime.get("entry_debug")).get("trade_through_kpi")) if isinstance(runtime, dict) else {}
    truth_profile = _truth_loss_profile(realized)
    adaptive_loss = runtime.get("adaptive_loss") if isinstance(runtime, dict) else {}
    if isinstance(adaptive_loss, dict) and truth_profile and not adaptive_loss.get("profile"):
        adaptive_loss["active"] = True
        adaptive_loss["profile"] = truth_profile
        adaptive_loss["blocked_tickers"] = sorted((truth_profile.get("ticker_losses") or {}).keys())
    scanner = _scanner_summary()
    scanner["passes"] = len(filled_option_entry_orders)
    scanner["trade_passes"] = len(filled_option_entry_orders)
    scanner["fails"] = max(0, int(scanner.get("scan_rows_today", 0) or 0) - int(scanner["passes"]))
    scanner["pass_rate_pct"] = round((float(scanner["passes"]) / float(scanner["scan_rows_today"])) * 100.0, 2) if int(scanner.get("scan_rows_today", 0) or 0) > 0 else 0.0
    return {
        "generated_at_et": _now_et().isoformat(),
        "mode": "paper" if PAPER else "live",
        "source_of_truth": "alpaca_orders_positions",
        "deployment": _deployment_meta(),
        "embedded_trader_fallback": embedded_boot,
        "account": _account(),
        "clock": _clock(),
        "runtime": runtime,
        "execution_bus": {
            "raw_scan_rows_count": _int_value(trade_kpi.get("raw_scan_rows_count")),
            "scanner_candidate_count": _int_value(trade_kpi.get("scanner_candidate_count")),
            "scanner_failed_count": _int_value(trade_kpi.get("scanner_failed_count")),
            "execution_candidate_count": _int_value(trade_kpi.get("execution_candidate_count")),
            "execution_rejected_count": _int_value(trade_kpi.get("execution_rejected_count")),
            "execution_rejected_count_by_reason": _as_dict(trade_kpi.get("execution_rejected_count_by_reason")),
            "trade_attempted_count": _int_value(trade_kpi.get("trade_attempted_count", trade_kpi.get("trade_attempts"))),
            "trade_resting_count": _int_value(trade_kpi.get("trade_resting_count")),
            "trade_filled_count": _int_value(trade_kpi.get("trade_filled_count", trade_kpi.get("fills"))),
            "zero_trade_cycle_reason": str(trade_kpi.get("zero_trade_cycle_reason", "") or ""),
        },
        "positions": positions,
        "orders": {
            "submitted_today": len(all_orders),
            "filled_today": len(filled_orders),
            "filled_option_orders_today": len(filled_option_orders),
            "filled_option_entry_orders_today": len(filled_option_entry_orders),
            "status_counts": dict(Counter(str(o.get("status", "") or "unknown") for o in all_orders)),
            "side_counts": dict(Counter(str(o.get("side", "") or "unknown") for o in filled_orders)),
            "recent": [_compact_order(o) for o in all_orders[:50]],
        },
        "realized": realized,
        "scanner": scanner,
    }


def _verify_control_token() -> tuple[bool, str, int]:
    if not CONTROL_TOKEN:
        if PAPER:
            return True, "", 200
        return False, "Dashboard control token is not configured.", 503
    supplied = str(request.headers.get("X-Control-Token") or request.args.get("token") or "").strip()
    if not supplied and request.is_json:
        payload = request.get_json(silent=True) or {}
        if isinstance(payload, dict):
            supplied = str(payload.get("token", "") or "").strip()
    if not supplied:
        return False, "Missing control token.", 401
    if supplied != CONTROL_TOKEN:
        return False, "Invalid control token.", 403
    return True, "", 200


def _control_payload() -> dict[str, Any]:
    state = load_trading_control()
    if not isinstance(state, dict):
        state = {}
    return {
        "manual_stop": bool(state.get("manual_stop", False)),
        "dry_run": bool(state.get("dry_run", False)),
        "strategy_profile": str(state.get("strategy_profile", "") or ""),
        "updated_at_et": str(state.get("updated_at_et", "") or ""),
        "reason": str(state.get("reason", "") or ""),
    }


def _read_runtime_overrides_file() -> dict[str, Any]:
    if not RUNTIME_OVERRIDES_PATH.exists():
        return {"overrides": {}, "updated_at_et": "", "reason": ""}
    try:
        payload = json.loads(RUNTIME_OVERRIDES_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {"overrides": {}, "updated_at_et": "", "reason": ""}
    if not isinstance(payload, dict):
        return {"overrides": {}, "updated_at_et": "", "reason": ""}
    overrides = payload.get("overrides", {})
    if not isinstance(overrides, dict):
        overrides = {}
    return {
        "overrides": overrides,
        "updated_at_et": str(payload.get("updated_at_et", "") or ""),
        "reason": str(payload.get("reason", "") or ""),
    }


def _coerce_runtime_param(name: str, raw: Any) -> tuple[bool, Any, str]:
    spec = RUNTIME_PARAM_SPECS.get(name)
    if not spec:
        return False, None, "unsupported parameter"
    ptype = spec.get("type")
    if ptype == "bool":
        if isinstance(raw, bool):
            return True, raw, ""
        text = str(raw or "").strip().lower()
        if text in {"1", "true", "yes", "on"}:
            return True, True, ""
        if text in {"0", "false", "no", "off"}:
            return True, False, ""
        return False, None, "invalid bool"
    try:
        if ptype == "int":
            value = int(float(raw))
        else:
            value = float(raw)
    except (TypeError, ValueError):
        return False, None, f"invalid {ptype}"
    minimum = spec.get("min")
    maximum = spec.get("max")
    if minimum is not None and value < minimum:
        return False, None, f"must be >= {minimum}"
    if maximum is not None and value > maximum:
        return False, None, f"must be <= {maximum}"
    return True, value, ""


def _runtime_parameters_payload() -> dict[str, Any]:
    file_payload = _read_runtime_overrides_file()
    active_overrides = file_payload.get("overrides", {})
    if not isinstance(active_overrides, dict):
        active_overrides = {}
    rows: list[dict[str, Any]] = []
    for name in sorted(RUNTIME_PARAM_SPECS.keys()):
        current = getattr(config, name, None)
        rows.append(
            {
                "name": name,
                "current_value": current,
                "override_value": active_overrides.get(name),
                "runtime_source": "override" if name in active_overrides else "default_or_env",
                "spec": dict(RUNTIME_PARAM_SPECS.get(name) or {}),
            }
        )
    return {
        "updated_at_et": str(file_payload.get("updated_at_et", "") or ""),
        "reason": str(file_payload.get("reason", "") or ""),
        "parameters": rows,
    }


def _apply_runtime_overrides(overrides: dict[str, Any], *, reason: str) -> dict[str, Any]:
    cleaned: dict[str, Any] = {}
    errors: dict[str, str] = {}
    for raw_name, raw_value in dict(overrides or {}).items():
        name = str(raw_name or "").strip().upper()
        ok, value, err = _coerce_runtime_param(name, raw_value)
        if not ok:
            errors[name or str(raw_name)] = err
            continue
        cleaned[name] = value
    if errors:
        return {"ok": False, "errors": errors}
    for name, value in cleaned.items():
        setattr(config, name, value)
    payload = {
        "overrides": cleaned,
        "updated_at_et": _now_et().isoformat(),
        "reason": str(reason or "runtime_apply"),
    }
    RUNTIME_OVERRIDES_PATH.parent.mkdir(parents=True, exist_ok=True)
    RUNTIME_OVERRIDES_PATH.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return {"ok": True, "applied": cleaned, "updated_at_et": payload["updated_at_et"]}


def _broker_close_all_positions() -> dict[str, Any]:
    try:
        from broker import AlpacaBroker

        broker = AlpacaBroker(API_KEY, SECRET_KEY, paper=PAPER)
        cancel_result = broker.cancel_all_open_orders()
        total, closed, results = broker.close_all_positions()
        return {
            "ok": True,
            "cancel_result": str(cancel_result),
            "positions_seen": total,
            "close_orders_submitted": closed,
            "results": results,
        }
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": str(exc)[:500]}


@app.after_request
def _no_cache(response):
    if str(getattr(response, "status", "") or "") and str(response.status).startswith("2"):
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    return response


@app.get("/healthz")
def healthz():
    runtime = _runtime()
    embedded_boot = _start_embedded_trader_if_stale("healthz")
    return jsonify(
        {
            "ok": True,
            "service": "autobott-dashboard-v2",
            "paper": PAPER,
            "alpaca_key_present": bool(API_KEY),
            "alpaca_secret_present": bool(SECRET_KEY),
            "runtime": runtime,
            "deployment": _deployment_meta(),
            "embedded_trader_fallback": embedded_boot,
        }
    )


@app.get("/api/truth")
def api_truth():
    return jsonify(_truth_payload())


@app.get("/api/trading-control")
def api_trading_control():
    return jsonify(_control_payload())


@app.post("/api/trading-control/stop")
def api_trading_control_stop():
    ok, err, status = _verify_control_token()
    if not ok:
        return jsonify({"ok": False, "error": err}), status
    payload = request.get_json(silent=True) if request.is_json else {}
    if not isinstance(payload, dict):
        payload = {}
    reason = str(payload.get("reason", "") or "manual_stop_dashboard_v2")
    set_manual_stop(True, reason=reason)
    return jsonify({"ok": True, **_control_payload()})


@app.post("/api/trading-control/start")
def api_trading_control_start():
    ok, err, status = _verify_control_token()
    if not ok:
        return jsonify({"ok": False, "error": err}), status
    payload = request.get_json(silent=True) if request.is_json else {}
    if not isinstance(payload, dict):
        payload = {}
    reason = str(payload.get("reason", "") or "manual_start_dashboard_v2")
    set_manual_stop(False, reason=reason)
    return jsonify({"ok": True, **_control_payload()})


@app.post("/api/control/close-all-positions")
def api_control_close_all_positions():
    ok, err, status = _verify_control_token()
    if not ok:
        return jsonify({"ok": False, "error": err}), status
    set_manual_stop(True, reason="close_all_positions_dashboard_v2")
    close_result = _broker_close_all_positions()
    return jsonify({"ok": bool(close_result.get("ok")), "control": _control_payload(), "close": close_result})


@app.post("/api/control/reset-adaptive-loss")
def api_control_reset_adaptive_loss():
    ok, err, status = _verify_control_token()
    if not ok:
        return jsonify({"ok": False, "error": err}), status
    state = load_bot_state()
    if not isinstance(state, dict):
        state = {}
    state["adaptive_loss_active"] = False
    state["adaptive_loss_blocked_tickers"] = []
    state["adaptive_loss_profile"] = {}
    save_bot_state(state)
    return jsonify({"ok": True, "runtime": _runtime()})


@app.get("/api/runtime/parameters")
def api_runtime_parameters():
    return jsonify({"ok": True, **_runtime_parameters_payload()})


@app.post("/api/runtime/parameters/preview")
def api_runtime_parameters_preview():
    ok, err, status = _verify_control_token()
    if not ok:
        return jsonify({"ok": False, "error": err}), status
    payload = request.get_json(silent=True) if request.is_json else {}
    if not isinstance(payload, dict):
        payload = {}
    overrides = payload.get("overrides", {})
    if not isinstance(overrides, dict):
        return jsonify({"ok": False, "error": "overrides must be an object"}), 400
    preview: dict[str, Any] = {}
    errors: dict[str, str] = {}
    for raw_name, raw_value in overrides.items():
        name = str(raw_name or "").strip().upper()
        accepted, value, detail = _coerce_runtime_param(name, raw_value)
        if accepted:
            preview[name] = value
        else:
            errors[name or str(raw_name)] = detail
    return jsonify({"ok": len(errors) == 0, "preview": preview, "errors": errors})


@app.post("/api/runtime/parameters/apply")
def api_runtime_parameters_apply():
    ok, err, status = _verify_control_token()
    if not ok:
        return jsonify({"ok": False, "error": err}), status
    payload = request.get_json(silent=True) if request.is_json else {}
    if not isinstance(payload, dict):
        payload = {}
    overrides = payload.get("overrides", {})
    if not isinstance(overrides, dict):
        return jsonify({"ok": False, "error": "overrides must be an object"}), 400
    reason = str(payload.get("reason", "") or "dashboard_runtime_apply")
    result = _apply_runtime_overrides(overrides, reason=reason)
    if not bool(result.get("ok")):
        return jsonify(result), 400
    return jsonify({"ok": True, "result": result, **_runtime_parameters_payload()})


@app.post("/api/runtime/parameters/revert")
def api_runtime_parameters_revert():
    ok, err, status = _verify_control_token()
    if not ok:
        return jsonify({"ok": False, "error": err}), status
    payload = {
        "overrides": {},
        "updated_at_et": _now_et().isoformat(),
        "reason": "dashboard_runtime_revert",
    }
    RUNTIME_OVERRIDES_PATH.parent.mkdir(parents=True, exist_ok=True)
    RUNTIME_OVERRIDES_PATH.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    for name in RUNTIME_PARAM_SPECS.keys():
        try:
            import importlib
            cfg = importlib.import_module("config")
            if hasattr(cfg, name):
                setattr(config, name, getattr(cfg, name))
        except Exception:
            continue
    return jsonify({"ok": True, **_runtime_parameters_payload()})


@app.post("/api/runtime/presets/save")
def api_runtime_presets_save():
    ok, err, status = _verify_control_token()
    if not ok:
        return jsonify({"ok": False, "error": err}), status
    payload = request.get_json(silent=True) if request.is_json else {}
    if not isinstance(payload, dict):
        payload = {}
    name = str(payload.get("name", "") or "").strip()
    if not name:
        return jsonify({"ok": False, "error": "name is required"}), 400
    current = _read_runtime_overrides_file()
    presets = {}
    if RUNTIME_PRESETS_PATH.exists():
        try:
            existing = json.loads(RUNTIME_PRESETS_PATH.read_text(encoding="utf-8"))
            if isinstance(existing, dict):
                presets = existing
        except Exception:
            presets = {}
    presets[name] = {
        "overrides": dict(current.get("overrides", {}) or {}),
        "saved_at_et": _now_et().isoformat(),
    }
    RUNTIME_PRESETS_PATH.parent.mkdir(parents=True, exist_ok=True)
    RUNTIME_PRESETS_PATH.write_text(json.dumps(presets, indent=2, sort_keys=True), encoding="utf-8")
    return jsonify({"ok": True, "preset": name, "count": len(presets)})


@app.post("/api/runtime/presets/load")
def api_runtime_presets_load():
    ok, err, status = _verify_control_token()
    if not ok:
        return jsonify({"ok": False, "error": err}), status
    payload = request.get_json(silent=True) if request.is_json else {}
    if not isinstance(payload, dict):
        payload = {}
    name = str(payload.get("name", "") or "").strip()
    if not name or not RUNTIME_PRESETS_PATH.exists():
        return jsonify({"ok": False, "error": "preset not found"}), 404
    try:
        presets = json.loads(RUNTIME_PRESETS_PATH.read_text(encoding="utf-8"))
    except Exception:
        presets = {}
    if not isinstance(presets, dict) or name not in presets:
        return jsonify({"ok": False, "error": "preset not found"}), 404
    selected = presets.get(name, {})
    overrides = selected.get("overrides", {}) if isinstance(selected, dict) else {}
    if not isinstance(overrides, dict):
        return jsonify({"ok": False, "error": "invalid preset payload"}), 400
    result = _apply_runtime_overrides(overrides, reason=f"dashboard_preset_load:{name}")
    if not bool(result.get("ok")):
        return jsonify(result), 400
    return jsonify({"ok": True, "preset": name, "result": result, **_runtime_parameters_payload()})


@app.get("/api/runtime/presets")
def api_runtime_presets_list():
    if not RUNTIME_PRESETS_PATH.exists():
        return jsonify({"ok": True, "presets": []})
    try:
        payload = json.loads(RUNTIME_PRESETS_PATH.read_text(encoding="utf-8"))
    except Exception:
        payload = {}
    if not isinstance(payload, dict):
        payload = {}
    rows = []
    for name, item in payload.items():
        if not isinstance(item, dict):
            continue
        overrides = item.get("overrides", {})
        rows.append(
            {
                "name": str(name),
                "saved_at_et": str(item.get("saved_at_et", "") or ""),
                "override_count": len(overrides) if isinstance(overrides, dict) else 0,
            }
        )
    rows.sort(key=lambda row: str(row.get("saved_at_et", "")), reverse=True)
    return jsonify({"ok": True, "presets": rows})


@app.post("/api/runtime/symbols/mute")
def api_runtime_symbols_mute():
    ok, err, status = _verify_control_token()
    if not ok:
        return jsonify({"ok": False, "error": err}), status
    payload = request.get_json(silent=True) if request.is_json else {}
    if not isinstance(payload, dict):
        payload = {}
    ticker = str(payload.get("ticker", "") or "").strip().upper()
    if not ticker:
        return jsonify({"ok": False, "error": "ticker is required"}), 400
    control = load_watchlist_control()
    tickers = list(control.get("tickers") or [])
    if ticker not in tickers:
        tickers.append(ticker)
    mode = str(control.get("mode", "exclude_listed") or "exclude_listed").strip().lower()
    if mode not in {"exclude_listed", "only_listed"}:
        mode = "exclude_listed"
    updated = update_watchlist_control(mode=mode, tickers=tickers, reason=f"dashboard_mute:{ticker}")
    return jsonify({"ok": True, "watchlist_control": updated})


@app.post("/api/runtime/symbols/solo")
def api_runtime_symbols_solo():
    ok, err, status = _verify_control_token()
    if not ok:
        return jsonify({"ok": False, "error": err}), status
    payload = request.get_json(silent=True) if request.is_json else {}
    if not isinstance(payload, dict):
        payload = {}
    tickers_raw = payload.get("tickers", [])
    if isinstance(tickers_raw, str):
        tickers = [tickers_raw]
    elif isinstance(tickers_raw, list):
        tickers = [str(item or "") for item in tickers_raw]
    else:
        return jsonify({"ok": False, "error": "tickers must be list or string"}), 400
    updated = update_watchlist_control(mode="only_listed", tickers=tickers, reason="dashboard_solo")
    return jsonify({"ok": True, "watchlist_control": updated})


@app.post("/api/runtime/orders/cancel-open-entries")
def api_runtime_orders_cancel_open_entries():
    ok, err, status = _verify_control_token()
    if not ok:
        return jsonify({"ok": False, "error": err}), status
    try:
        from broker import AlpacaBroker

        broker = AlpacaBroker(API_KEY, SECRET_KEY, paper=PAPER)
        canceled = broker.cancel_all_open_orders()
        return jsonify({"ok": True, "canceled": canceled})
    except Exception as exc:  # noqa: BLE001
        return jsonify({"ok": False, "error": str(exc)[:500]}), 500


@app.post("/api/runtime/trading/pause")
def api_runtime_trading_pause():
    ok, err, status = _verify_control_token()
    if not ok:
        return jsonify({"ok": False, "error": err}), status
    set_manual_stop(True, reason="dashboard_runtime_pause")
    return jsonify({"ok": True, **_control_payload()})


@app.post("/api/runtime/trading/resume")
def api_runtime_trading_resume():
    ok, err, status = _verify_control_token()
    if not ok:
        return jsonify({"ok": False, "error": err}), status
    set_manual_stop(False, reason="dashboard_runtime_resume")
    set_dry_run(False, reason="dashboard_runtime_resume")
    return jsonify({"ok": True, **_control_payload()})


@app.post("/api/runtime/trading/flatten")
def api_runtime_trading_flatten():
    ok, err, status = _verify_control_token()
    if not ok:
        return jsonify({"ok": False, "error": err}), status
    set_manual_stop(True, reason="dashboard_runtime_flatten")
    close_result = _broker_close_all_positions()
    return jsonify({"ok": bool(close_result.get("ok")), "control": _control_payload(), "close": close_result})


@app.get("/api/runtime-debug")
def api_runtime_debug():
    embedded_boot = _start_embedded_trader_if_stale("api_runtime_debug")
    state = load_bot_state()
    if not isinstance(state, dict):
        state = {}
    return jsonify(
        {
            "generated_at_et": _now_et().isoformat(),
            "deployment": _deployment_meta(),
            "embedded_trader_fallback": embedded_boot,
            "state_updated_at": str(state.get("_state_updated_at_iso", "") or ""),
            "runtime": _runtime(),
            "last_entry_debug": _as_dict(state.get("last_entry_debug")),
            "last_exit_debug": _as_dict(state.get("last_exit_debug")),
            "open_trade_meta_count": len(_as_dict(state.get("open_trade_meta"))),
            "ticker_loss_cooldown_until": _as_dict(state.get("ticker_loss_cooldown_until")),
            "bad_fill_tracker": _as_dict(state.get("bad_fill_tracker")),
        }
    )


@app.get("/")
def index():
    return render_template_string(HTML)


HTML = r'''
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>AutoBott Mixer Control Room</title>
  <style>
    :root{--bg:#07111f;--panel:#101c2d;--line:#27384f;--text:#eef5ff;--muted:#8ea1b8;--green:#21d07a;--red:#ff4d5e;--yellow:#ffd166;--blue:#59a7ff}
    *{box-sizing:border-box} body{margin:0;background:radial-gradient(circle at top,#102039 0,#07111f 45%,#040913 100%);color:var(--text);font:14px/1.4 system-ui,Segoe UI,Arial,sans-serif}
    .wrap{max-width:1600px;margin:0 auto;padding:18px}.grid{display:grid;grid-template-columns:repeat(12,1fr);gap:12px}
    .card{background:linear-gradient(180deg,#13243a,var(--panel));border:1px solid var(--line);border-radius:14px;padding:12px}
    .span3{grid-column:span 3}.span4{grid-column:span 4}.span6{grid-column:span 6}.span8{grid-column:span 8}.span12{grid-column:span 12}
    .label{color:var(--muted);font-size:11px;text-transform:uppercase;letter-spacing:.08em;font-weight:800}.big{font-size:26px;font-weight:900}
    .row{display:flex;justify-content:space-between;gap:8px;border-bottom:1px solid rgba(255,255,255,.07);padding:6px 0}.row:last-child{border-bottom:0}
    .btn{border:1px solid var(--line);background:#162840;color:var(--text);padding:6px 10px;border-radius:8px;cursor:pointer}
    .btn.red{background:#3a1a21}.btn.green{background:#173325}.btn.yellow{background:#3b3218}
    input,select{background:#0f1d31;color:var(--text);border:1px solid var(--line);border-radius:8px;padding:6px}
    table{width:100%;border-collapse:collapse} th,td{padding:6px;border-bottom:1px solid rgba(255,255,255,.06);font-size:12px;text-align:left}
    .h{font-weight:800;margin-bottom:8px}.muted{color:var(--muted)}
    @media(max-width:1100px){.span3,.span4,.span6,.span8{grid-column:span 12}}
  </style>
</head>
<body>
<div class="wrap">
  <div class="grid">
    <div class="card span12">
      <div class="h">Master Strip</div>
      <div style="display:flex;gap:8px;flex-wrap:wrap;align-items:center">
        <input id="token" placeholder="Control token">
        <button class="btn" onclick="saveToken()">Save Token</button>
        <span id="modePill" class="muted">mode: --</span>
        <span id="updated" class="muted">--</span>
      </div>
      <div id="masterRows"></div>
      <div style="display:flex;gap:8px;flex-wrap:wrap;margin-top:8px">
        <button class="btn yellow" onclick="guarded('/api/runtime/trading/pause')">Pause</button>
        <button class="btn green" onclick="guarded('/api/runtime/trading/resume')">Resume</button>
        <button class="btn red" onclick="guarded('/api/runtime/orders/cancel-open-entries')">Cancel Open Entries</button>
        <button class="btn red" onclick="guarded('/api/runtime/trading/flatten')">Flatten</button>
      </div>
    </div>
    <div class="card span4"><div class="h">Execution Bus</div><div id="busRows"></div></div>
    <div class="card span4"><div class="h">Preset Panel</div>
      <div style="display:flex;gap:6px"><input id="presetName" placeholder="Preset name"><button class="btn" onclick="savePreset()">Save</button></div>
      <div style="display:flex;gap:6px;margin-top:6px"><select id="presetSelect"></select><button class="btn" onclick="loadPreset()">Load</button></div>
      <div id="presetRows" class="muted" style="margin-top:8px"></div>
    </div>
    <div class="card span4"><div class="h">Paper Trade-Through</div>
      <div id="truthRows"></div>
    </div>
    <div class="card span6"><div class="h">Entry Mixer Faders</div><div id="entryParams"></div></div>
    <div class="card span6"><div class="h">Options Contract Mixer</div><div id="contractParams"></div></div>
    <div class="card span12">
      <div class="h">Symbol Channel Strips</div>
      <table><thead><tr><th>Symbol</th><th>Last Reason</th><th>Action</th></tr></thead><tbody id="symbolRows"></tbody></table>
    </div>
  </div>
</div>
<script>
const $=(id)=>document.getElementById(id);
const money=(v)=>Number(v||0).toLocaleString(undefined,{style:'currency',currency:'USD'});
const pct=(v)=>`${Number(v||0).toFixed(2)}%`;
const row=(k,v)=>`<div class="row"><span class="muted">${k}</span><b>${v}</b></div>`;
let paramState={};
const ENTRY_KEYS=["MIN_SIGNAL_SCORE","DIRECTION_CONVICTION_MIN","RVOL_MIN","ATR_PCT_MIN","MOVEMENT_FORCE_MIN_PCT","FAST_START_MIN_SIGNAL_SCORE","FAST_START_MIN_DIRECTION_SCORE","FAST_START_MIN_RVOL","FAST_START_MIN_ABS_ROC_PCT","FAST_START_MIN_VWAP_DISTANCE_PCT"];
const CONTRACT_KEYS=["ENTRY_MAX_QUOTE_SPREAD_PCT","MAX_PREMIUM_PER_TRADE_USD","MAX_CONTRACTS_PER_ENTRY","ENTRY_LIMIT_ATTEMPTS","ENABLE_ENTRY_MARKET_FALLBACK"];
function token(){return localStorage.getItem("dash_token")||""}
function saveToken(){localStorage.setItem("dash_token",$("token").value.trim())}
async function api(url,opt={}){
  const headers=Object.assign({"Content-Type":"application/json"},opt.headers||{});
  const t=token(); if(t) headers["X-Control-Token"]=t;
  const res=await fetch(url,Object.assign({method:"GET",headers},opt));
  const j=await res.json().catch(()=>({ok:false,error:"bad json"})); if(!res.ok) throw new Error(j.error||res.statusText); return j;
}
async function guarded(url,body={}){
  if(!confirm(`Confirm ${url}?`)) return;
  try{await api(url,{method:"POST",body:JSON.stringify(body)}); await loadAll();}catch(e){alert(e.message)}
}
function renderParamGroup(targetId,keys){
  const rows=(paramState.parameters||[]).filter(p=>keys.includes(p.name));
  $(targetId).innerHTML=rows.map(p=>`<div class="row"><span>${p.name}</span><span><input id="p_${p.name}" value="${p.override_value??p.current_value}" style="width:140px"></span></div>`).join("")+
  `<div style="margin-top:8px;display:flex;gap:8px"><button class="btn" onclick="previewApply(${JSON.stringify(keys).replace(/"/g,'&quot;')})">Preview</button><button class="btn green" onclick="applyGroup(${JSON.stringify(keys).replace(/"/g,'&quot;')})">Apply</button><button class="btn" onclick="revertOverrides()">Revert All</button></div>`;
}
async function previewApply(keys){
  const overrides={}; keys.forEach(k=>overrides[k]=$(`p_${k}`)?.value);
  try{const r=await api("/api/runtime/parameters/preview",{method:"POST",body:JSON.stringify({overrides})}); alert(`Preview ok=${r.ok}\nErrors=${JSON.stringify(r.errors||{})}`)}catch(e){alert(e.message)}
}
async function applyGroup(keys){
  const overrides={}; keys.forEach(k=>overrides[k]=$(`p_${k}`)?.value);
  await guarded("/api/runtime/parameters/apply",{overrides,reason:"mixer_apply"});
}
async function revertOverrides(){await guarded("/api/runtime/parameters/revert",{})}
async function savePreset(){const name=$("presetName").value.trim(); if(!name) return alert("Preset name required"); await guarded("/api/runtime/presets/save",{name})}
async function loadPreset(){const name=$("presetSelect").value; if(!name) return; await guarded("/api/runtime/presets/load",{name})}
async function muteTicker(t){await guarded("/api/runtime/symbols/mute",{ticker:t})}
async function soloTicker(t){await guarded("/api/runtime/symbols/solo",{tickers:[t]})}
async function loadAll(){
  $("token").value=token();
  const [truth,params,presets]=await Promise.all([api("/api/truth"),api("/api/runtime/parameters"),api("/api/runtime/presets")]);
  paramState=params;
  $("updated").textContent=new Date().toLocaleTimeString();
  $("modePill").textContent=`mode: ${(truth.mode||'--').toUpperCase()}`;
  const rt=truth.runtime||{}, bus=truth.execution_bus||{}, acct=truth.account||{}, real=truth.realized||{}, ord=truth.orders||{};
  $("masterRows").innerHTML=
    row("Trading Status", rt.manual_stop?"PAUSED":"ON")+
    row("Paper/Live",(truth.mode||"").toUpperCase())+
    row("Dry Run", rt.dry_run?"ON":"OFF")+
    row("Open Positions", (truth.positions||[]).length)+
    row("Orders Submitted Today", ord.submitted_today||0)+
    row("Filled Orders Today", ord.filled_today||0)+
    row("Realized P/L", money(real.realized_pnl_usd||0))+
    row("Unrealized P/L", money(real.open_unrealized_pnl_usd||0))+
    row("Win Rate", pct(real.win_rate_pct||0))+
    row("Zero Trade Warning", bus.zero_trade_cycle_reason||"none");
  $("busRows").innerHTML=
    row("raw_scan_rows_count", bus.raw_scan_rows_count||0)+
    row("scanner_candidate_count", bus.scanner_candidate_count||0)+
    row("scanner_failed_count", bus.scanner_failed_count||0)+
    row("execution_candidate_count", bus.execution_candidate_count||0)+
    row("trade_attempted_count", bus.trade_attempted_count||0)+
    row("trade_resting_count", bus.trade_resting_count||0)+
    row("trade_filled_count", bus.trade_filled_count||0)+
    row("zero_trade_cycle_reason", bus.zero_trade_cycle_reason||"n/a");
  $("truthRows").innerHTML=
    row("Equity", money(acct.equity||0))+
    row("Buying Power", money(acct.buying_power||0))+
    row("Direction Accuracy", pct((bus.direction_accuracy_pct||0)))+
    row("Loss Causes", JSON.stringify((bus.loss_cause_breakdown||{})));
  $("presetSelect").innerHTML=`<option value="">Select preset</option>`+(presets.presets||[]).map(p=>`<option>${p.name}</option>`).join("");
  $("presetRows").textContent=`Saved presets: ${(presets.presets||[]).length}`;
  renderParamGroup("entryParams",ENTRY_KEYS);
  renderParamGroup("contractParams",CONTRACT_KEYS);
  const top=((truth.scanner||{}).recent_rows||[]).slice(0,20);
  $("symbolRows").innerHTML=top.map(r=>{const s=(r.symbol||r.ticker||"").toUpperCase(); return `<tr><td>${s}</td><td>${r.reason||""}</td><td><button class="btn" onclick="muteTicker('${s}')">Mute</button> <button class="btn" onclick="soloTicker('${s}')">Solo</button></td></tr>`}).join("") || `<tr><td colspan="3" class="muted">No symbols</td></tr>`;
}
loadAll().catch(e=>alert(e.message)); setInterval(()=>loadAll().catch(()=>{}),10000);
</script>
</body>
</html>
'''


if __name__ == "__main__":
    # Fallback: if this module is run directly as the service start command,
    # still run the trader loop in-process so the dashboard is not "view-only".
    if _env_bool("ENABLE_EMBEDDED_TRADER_ON_DASHBOARD", True):
        boot = _start_embedded_trader_if_stale("__main__")
        if boot.get("started"):
            print("[dashboard_v2] Embedded trader thread started from __main__.")

    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=False)



