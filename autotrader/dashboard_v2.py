"""AutoBott dashboard v2.

Alpaca is the source of truth for account, positions, filled orders, and
intraday P/L. Local CSV/state files are shown only as context.
"""

from __future__ import annotations

import csv
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
from trading_control import load_trading_control, set_manual_stop

API_KEY = str(os.getenv("ALPACA_API_KEY") or "").strip()
SECRET_KEY = str(os.getenv("ALPACA_SECRET_KEY") or "").strip()
PAPER = bool(getattr(config, "PAPER", True))
BASE_URL = "https://paper-api.alpaca.markets" if PAPER else "https://api.alpaca.markets"
HEADERS = {"APCA-API-KEY-ID": API_KEY, "APCA-API-SECRET-KEY": SECRET_KEY}
CONTROL_TOKEN = str(getattr(config, "DASHBOARD_CONTROL_TOKEN", "") or "").strip()
EASTERN = pytz.timezone(str(getattr(config, "EASTERN_TZ", "US/Eastern") or "US/Eastern"))
DISPLAY_TZ = pytz.timezone(str(os.getenv("DASHBOARD_DISPLAY_TZ", "America/Chicago") or "America/Chicago"))
SCAN_LOG_CSV = Path(getattr(config, "SCAN_LOG_CSV_PATH"))

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
        "entry_debug": _entry_debug_summary(state),
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
<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>AutoBott Command Center</title><style>:root{--bg:#07111f;--panel:#101c2d;--panel2:#13243a;--line:#27384f;--text:#eef5ff;--muted:#8ea1b8;--green:#21d07a;--red:#ff4d5e;--yellow:#ffd166;--blue:#59a7ff}*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at top,#102039 0,#07111f 45%,#040913 100%);color:var(--text);font:14px/1.4 system-ui,Segoe UI,Arial,sans-serif}.wrap{max-width:1500px;margin:0 auto;padding:24px}.top{display:flex;justify-content:space-between;gap:18px;align-items:flex-start;margin-bottom:18px}.title h1{margin:0;font-size:32px;letter-spacing:-.04em}.sub{color:var(--muted);margin-top:3px}.pill{display:inline-flex;align-items:center;gap:8px;border:1px solid var(--line);border-radius:999px;padding:8px 12px;background:rgba(255,255,255,.04);font-weight:700}.paper{background:#ffd166;color:#1a1400;border:0}.live{background:#ff4d5e;color:white;border:0}.grid{display:grid;grid-template-columns:repeat(12,1fr);gap:14px}.card{background:linear-gradient(180deg,var(--panel2),var(--panel));border:1px solid var(--line);border-radius:18px;padding:16px;box-shadow:0 14px 40px rgba(0,0,0,.22)}.span3{grid-column:span 3}.span4{grid-column:span 4}.span6{grid-column:span 6}.span12{grid-column:span 12}.label{color:var(--muted);font-size:12px;text-transform:uppercase;letter-spacing:.08em;font-weight:800}.big{font-size:30px;font-weight:900;letter-spacing:-.03em;margin-top:6px}.green{color:var(--green)}.red{color:var(--red)}.yellow{color:var(--yellow)}.blue{color:var(--blue)}.muted{color:var(--muted)}.row{display:flex;justify-content:space-between;gap:12px;border-bottom:1px solid rgba(255,255,255,.06);padding:8px 0}.row:last-child{border-bottom:0}table{width:100%;border-collapse:collapse}th,td{text-align:left;border-bottom:1px solid rgba(255,255,255,.07);padding:9px 8px;font-size:13px}th{color:var(--muted);text-transform:uppercase;font-size:11px;letter-spacing:.08em}tr:last-child td{border-bottom:0}.statusdot{display:inline-block;width:9px;height:9px;border-radius:50%;background:var(--muted);margin-right:7px}.statusdot.on{background:var(--green);box-shadow:0 0 15px var(--green)}.banner{border:1px solid rgba(255,209,102,.35);background:rgba(255,209,102,.10);color:#ffe7a3;border-radius:16px;padding:12px 14px;margin-bottom:14px;font-weight:700}.nowrap{white-space:nowrap}@media(max-width:1100px){.span3,.span4,.span6{grid-column:span 12}.top{display:block}}</style></head><body><div class="wrap"><div class="top"><div class="title"><h1>AutoBott Command Center</h1><div class="sub">Alpaca-first truth dashboard. Local logs are context, not the source of truth.</div></div><div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap"><span id="mode" class="pill paper">PAPER</span><span id="clock" class="pill"><span class="statusdot"></span>Loading</span><span id="updated" class="pill">--</span></div></div><div class="banner">Truth source: Alpaca account + orders + positions. The old Trades Today/P&L cards were log-based and could lie when trade logging missed pairs.</div><div class="grid"><div class="card span3"><div class="label">Equity</div><div id="equity" class="big">--</div><div class="muted">Alpaca account</div></div><div class="card span3"><div class="label">Buying Power</div><div id="bp" class="big">--</div><div class="muted">Available capital</div></div><div class="card span3"><div class="label">Realized P/L Today</div><div id="realized" class="big">--</div><div class="muted">Filled option order pairs</div></div><div class="card span3"><div class="label">Total P/L Today</div><div id="totalpnl" class="big">--</div><div class="muted">Realized + open unrealized</div></div><div class="card span3"><div class="label">Filled Orders</div><div id="filledOrders" class="big">--</div><div class="muted">Alpaca filled orders today</div></div><div class="card span3"><div class="label">Closed Trades</div><div id="closedTrades" class="big">--</div><div class="muted">Paired option legs</div></div><div class="card span3"><div class="label">Win Rate</div><div id="winRate" class="big">--</div><div class="muted">Closed paired trades</div></div><div class="card span3"><div class="label">Open Positions</div><div id="openPositions" class="big">--</div><div class="muted">Live Alpaca positions</div></div><div class="card span4"><div class="label">Runtime</div><div id="runtimeRows"></div></div><div class="card span4"><div class="label">Scanner</div><div id="scannerRows"></div></div><div class="card span4"><div class="label">Best / Worst</div><div id="bestWorst"></div></div><div class="card span6"><div class="label">Open Positions</div><div style="overflow:auto"><table><thead><tr><th>Symbol</th><th>Qty</th><th>Entry</th><th>Current</th><th>P/L</th></tr></thead><tbody id="positionsBody"></tbody></table></div></div><div class="card span6"><div class="label">P/L by Underlying</div><div style="overflow:auto"><table><thead><tr><th>Symbol</th><th>P/L</th></tr></thead><tbody id="underlyingBody"></tbody></table></div></div><div class="card span12"><div class="label">Recent Alpaca Orders</div><div style="overflow:auto"><table><thead><tr><th>Time</th><th>Symbol</th><th>Side</th><th>Type</th><th>Status</th><th>Qty</th><th>Filled</th><th>Price</th></tr></thead><tbody id="ordersBody"></tbody></table></div></div><div class="card span12"><div class="label">Recent Scanner Rows</div><div style="overflow:auto"><table><thead><tr><th>Time</th><th>Symbol</th><th>Result</th><th>Direction</th><th>Score</th><th>RVOL</th><th>Reason</th></tr></thead><tbody id="scannerBody"></tbody></table></div></div></div></div><script>const $=(id)=>document.getElementById(id);const money=(v)=>Number(v||0).toLocaleString(undefined,{style:'currency',currency:'USD'});const cls=(v)=>Number(v||0)>=0?'green':'red';function pct(v){return `${Number(v||0).toFixed(2)}%`}function row(k,v){return `<div class="row"><span class="muted">${k}</span><b>${v}</b></div>`}function safe(v){return (v===null||v===undefined||v==='')?'--':v}function setMoneyCard(id,val){const el=$(id);el.textContent=money(val);el.className='big '+cls(val)}function render(p){$('mode').textContent=p.mode==='live'?'LIVE':'PAPER';$('mode').className='pill '+(p.mode==='live'?'live':'paper');const isOpen=p.clock&&p.clock.is_open===true;$('clock').innerHTML=`<span class="statusdot ${isOpen?'on':''}"></span>${isOpen?'MARKET OPEN':'MARKET CLOSED'}`;$('updated').textContent=new Date().toLocaleTimeString();$('equity').textContent=money(p.account.equity);$('bp').textContent=money(p.account.buying_power);setMoneyCard('realized',p.realized.realized_pnl_usd);setMoneyCard('totalpnl',p.realized.total_intraday_pnl_usd);$('filledOrders').textContent=p.orders.filled_today;$('closedTrades').textContent=p.realized.closed_count;$('winRate').textContent=pct(p.realized.win_rate_pct);$('winRate').className='big '+(Number(p.realized.win_rate_pct)>=50?'green':'yellow');$('openPositions').textContent=p.positions.length;const rt=p.runtime;$('runtimeRows').innerHTML=row('Trader heartbeat',rt.heartbeat_label)+row('Manual stop',rt.manual_stop?'ON':'OFF')+row('Dry run',rt.dry_run?'ON':'OFF')+row('Truth source',p.source_of_truth);const sc=p.scanner;$('scannerRows').innerHTML=row('Scan rows today',sc.scan_rows_today)+row('Passes',sc.passes)+row('Fails',sc.fails)+row('Pass rate',pct(sc.pass_rate_pct))+row('Last scan',safe(sc.last_scan));const best=p.realized.best_trade,worst=p.realized.worst_trade;$('bestWorst').innerHTML=row('Best',best?`${best.underlying} ${money(best.realized_pnl_usd)}`:'--')+row('Worst',worst?`${worst.underlying} ${money(worst.realized_pnl_usd)}`:'--')+row('Profit factor',safe(p.realized.profit_factor));$('positionsBody').innerHTML=p.positions.map(x=>`<tr><td>${x.symbol}<div class="muted">${x.underlying}</div></td><td>${x.qty}</td><td>${money(x.avg_entry_price)}</td><td>${money(x.current_price)}</td><td class="${cls(x.unrealized_pl)}"><b>${money(x.unrealized_pl)}</b><div>${pct(x.unrealized_plpc)}</div></td></tr>`).join('')||'<tr><td colspan="5" class="muted">No open positions</td></tr>';$('underlyingBody').innerHTML=p.realized.by_underlying.map(x=>`<tr><td>${x.symbol}</td><td class="${cls(x.pnl_usd)}"><b>${money(x.pnl_usd)}</b></td></tr>`).join('')||'<tr><td colspan="2" class="muted">No paired closed trades yet</td></tr>';$('ordersBody').innerHTML=p.orders.recent.map(o=>`<tr><td class="nowrap">${o.display_time}</td><td>${o.symbol}</td><td class="${o.side==='buy'?'blue':'yellow'}">${o.side}</td><td>${o.type}</td><td>${o.status}</td><td>${o.qty}</td><td>${o.filled_qty}</td><td>${money(o.filled_avg_price||o.limit_price)}</td></tr>`).join('')||'<tr><td colspan="8" class="muted">No orders today</td></tr>';$('scannerBody').innerHTML=sc.recent_rows.map(r=>`<tr><td>${safe(r.timestamp)}</td><td>${safe(r.symbol||r.ticker)}</td><td class="${String(r.result).toLowerCase()==='pass'?'green':'red'}">${safe(r.result)}</td><td>${safe(r.direction)}</td><td>${safe(r.signal_score)}</td><td>${safe(r.rvol)}</td><td>${safe(r.reason)}</td></tr>`).join('')||'<tr><td colspan="7" class="muted">No scanner rows today</td></tr>'}async function load(){try{const res=await fetch('/api/truth',{cache:'no-store'});render(await res.json())}catch(e){console.error(e);$('updated').textContent='Load failed'}}load();setInterval(load,10000);</script></body></html>
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
