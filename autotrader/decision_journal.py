"""Operator-friendly decision journal for AutoBott.

This module is read-only. It does not trade, mutate state, or tune strategy.
It consolidates the app's decision surfaces into one explanation stream:
- scanner pass/reject decisions
- entry accuracy decisions
- VIX-derived proxy sidecar decisions
- runtime/control decisions
- Alpaca order outcomes when keys are available
"""

from __future__ import annotations

import csv
import os
import re
from collections import Counter, deque
from datetime import datetime
from pathlib import Path
from typing import Any

import pytz
import requests

try:
    from autotrader import config
except ImportError:
    import config  # type: ignore

try:
    from entry_decision_trace import build_entry_decision_trace
except ImportError:
    from autotrader.entry_decision_trace import build_entry_decision_trace  # type: ignore

try:
    from state_store import load_bot_state
    from trading_control import load_trading_control
except ImportError:
    from autotrader.state_store import load_bot_state  # type: ignore
    from autotrader.trading_control import load_trading_control  # type: ignore

EASTERN = pytz.timezone(str(getattr(config, "EASTERN_TZ", "US/Eastern") or "US/Eastern"))
API_KEY = str(os.getenv("ALPACA_API_KEY") or "").strip()
SECRET_KEY = str(os.getenv("ALPACA_SECRET_KEY") or "").strip()
PAPER = bool(getattr(config, "PAPER", True))
BASE_URL = "https://paper-api.alpaca.markets" if PAPER else "https://api.alpaca.markets"
HEADERS = {"APCA-API-KEY-ID": API_KEY, "APCA-API-SECRET-KEY": SECRET_KEY}


def _now_et() -> datetime:
    return datetime.now(EASTERN)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _parse_ts(value: Any) -> datetime | None:
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
    for suffix in (" EDT", " EST", " CDT", " CST"):
        if raw.upper().endswith(suffix.strip()):
            base = raw[: -len(suffix)].strip()
            try:
                dt = datetime.strptime(base, "%Y-%m-%d %H:%M:%S")
                return EASTERN.localize(dt)
            except ValueError:
                return None
    try:
        return EASTERN.localize(datetime.strptime(raw, "%Y-%m-%d %H:%M:%S"))
    except ValueError:
        return None


def _read_tail(path: Path, limit: int = 250) -> list[dict[str, str]]:
    if not path.exists():
        return []
    try:
        with path.open("r", newline="", encoding="utf-8") as handle:
            return list(deque(csv.DictReader(handle), maxlen=max(1, int(limit))))
    except Exception:
        return []


def _stage_from_reason(reason: str) -> str:
    raw = str(reason or "").strip()
    if ":" in raw:
        prefix = raw.split(":", 1)[0].strip()
        if prefix:
            return prefix
    lowered = raw.lower()
    if "rvol" in lowered:
        return "volume_filter"
    if "direction" in lowered:
        return "direction_filter"
    if "vwap" in lowered:
        return "structure_filter"
    if "roc" in lowered or "movement" in lowered:
        return "momentum_filter"
    if "spread" in lowered or "quote" in lowered:
        return "execution_filter"
    if "earnings" in lowered or "news" in lowered:
        return "event_filter"
    if "contract" in lowered or "option" in lowered:
        return "option_filter"
    return "decision"


def _classify_scan(row: dict[str, str]) -> dict[str, Any]:
    result = str(row.get("result", "") or "").lower()
    symbol = str(row.get("symbol", "") or row.get("ticker", "") or "").upper()
    direction = str(row.get("direction", "") or "").upper()
    reason = str(row.get("reason", "") or "")
    passed = result == "pass"
    if passed:
        summary = f"{symbol} {direction or 'SETUP'} passed scanner: {reason}"
        action = "candidate sent to entry filters"
    else:
        stage = _stage_from_reason(reason)
        summary = f"{symbol} rejected at {stage}: {reason}"
        action = "no entry considered"
    return {
        "source": "scanner",
        "timestamp": str(row.get("timestamp", "") or ""),
        "symbol": symbol,
        "direction": direction,
        "decision": "pass" if passed else "reject",
        "stage": "scanner_pass" if passed else _stage_from_reason(reason),
        "summary": summary,
        "action": action,
        "reason": reason,
        "metrics": {
            "signal_score": _safe_float(row.get("signal_score"), 0.0),
            "rvol": _safe_float(row.get("rvol"), 0.0),
            "rsi": _safe_float(row.get("rsi"), 0.0),
            "roc": _safe_float(row.get("roc"), 0.0),
            "iv_rank": _safe_float(row.get("iv_rank"), 0.0),
        },
    }


def _classify_entry(item: dict[str, Any]) -> dict[str, Any]:
    symbol = str(item.get("symbol", "") or "").upper()
    direction = str(item.get("direction", "") or "").upper()
    status = str(item.get("accuracy_status", "") or "")
    blocker = str(item.get("final_blocker", "") or "")
    aligned = int(item.get("aligned_count", 0) or 0)
    required = int(item.get("required_count", 4) or 4)
    if status == "approved":
        summary = f"{symbol} {direction} entry direction approved: {aligned}/{required} checks aligned."
        action = "eligible for contract/quote/order checks"
    elif status == "scanner_rejected":
        summary = f"{symbol} never reached entry gate because scanner rejected it."
        action = "no entry"
    else:
        summary = f"{symbol} {direction} held as watch-only: {blocker}"
        action = "wait for cleaner direction alignment"
    return {
        "source": "entry_accuracy",
        "timestamp": str(item.get("timestamp", "") or ""),
        "symbol": symbol,
        "direction": direction,
        "decision": status,
        "stage": "entry_direction_gate",
        "summary": summary,
        "action": action,
        "reason": blocker or str(item.get("reason", "") or ""),
        "metrics": {
            "signal_score": item.get("signal_score"),
            "direction_score": item.get("direction_score"),
            "rvol": item.get("rvol"),
            "roc": item.get("roc"),
            "aligned_count": aligned,
            "required_count": required,
            "vwap_state": item.get("vwap_state"),
            "ema_state": item.get("ema_state"),
        },
        "checks": item.get("checks", {}),
    }


def _classify_vix_proxy(row: dict[str, str]) -> dict[str, Any]:
    decision = str(row.get("decision", "") or "").strip() or "unknown"
    direction = str(row.get("direction", "") or "").upper()
    option_symbol = str(row.get("option_symbol", "") or "")
    proxy = str(row.get("proxy_underlying", "") or "VIXY")
    reason = str(row.get("reason", "") or "")
    if decision == "submitted_buy":
        summary = f"VIX proxy submitted {direction} buy on {option_symbol}: {reason}"
        action = "order sent to Alpaca"
    elif decision == "skip":
        summary = f"VIX proxy skipped {direction or 'trade'} on {proxy}: {reason}"
        action = "no volatility proxy entry"
    elif decision == "error":
        summary = f"VIX proxy error: {reason}"
        action = "needs log review"
    else:
        summary = f"VIX proxy decision {decision}: {reason}"
        action = "observe"
    return {
        "source": "vix_proxy",
        "timestamp": str(row.get("timestamp", "") or ""),
        "symbol": proxy,
        "direction": direction,
        "decision": decision,
        "stage": "volatility_proxy_regime",
        "summary": summary,
        "action": action,
        "reason": reason,
        "metrics": {
            "vix_level": _safe_float(row.get("vix_level"), 0.0),
            "average_level": _safe_float(row.get("average_level"), 19.0),
            "proxy_underlying_price": _safe_float(row.get("proxy_underlying_price"), 0.0),
            "strike": _safe_float(row.get("strike"), 0.0),
            "ask": _safe_float(row.get("ask"), 0.0),
            "bid": _safe_float(row.get("bid"), 0.0),
            "spread_pct": _safe_float(row.get("spread_pct"), 0.0),
            "qty": _safe_float(row.get("qty"), 0.0),
        },
    }


def _runtime_decisions() -> list[dict[str, Any]]:
    decisions: list[dict[str, Any]] = []
    now = _now_et().isoformat()
    try:
        control = load_trading_control()
        if not isinstance(control, dict):
            control = {}
    except Exception as exc:  # noqa: BLE001
        control = {"error": str(exc)}
    try:
        state = load_bot_state()
        if not isinstance(state, dict):
            state = {}
    except Exception as exc:  # noqa: BLE001
        state = {"error": str(exc)}

    manual_stop = bool(control.get("manual_stop", False))
    dry_run = bool(control.get("dry_run", False))
    heartbeat_raw = state.get("last_trader_heartbeat_et")
    heartbeat_dt = _parse_ts(heartbeat_raw)
    heartbeat_age = None
    if heartbeat_dt is not None:
        heartbeat_age = int((_now_et() - heartbeat_dt).total_seconds())

    decisions.append(
        {
            "source": "runtime_control",
            "timestamp": now,
            "symbol": "SYSTEM",
            "direction": "",
            "decision": "manual_stop_on" if manual_stop else "manual_stop_off",
            "stage": "trading_control",
            "summary": "Manual stop is ON, so new entries should be blocked." if manual_stop else "Manual stop is OFF, so new entries are allowed by this control.",
            "action": "do not enter trades" if manual_stop else "entry allowed if all other gates pass",
            "reason": str(control.get("reason", "") or ""),
            "metrics": {"manual_stop": manual_stop, "dry_run": dry_run, "heartbeat_age_seconds": heartbeat_age},
        }
    )
    decisions.append(
        {
            "source": "runtime_control",
            "timestamp": now,
            "symbol": "SYSTEM",
            "direction": "",
            "decision": "dry_run_on" if dry_run else "dry_run_off",
            "stage": "trading_control",
            "summary": "Dry run is ON, so the bot should simulate instead of placing live/paper orders." if dry_run else "Dry run is OFF, so approved entries can submit paper/live orders.",
            "action": "simulate only" if dry_run else "orders may be submitted if approved",
            "reason": str(control.get("reason", "") or ""),
            "metrics": {"manual_stop": manual_stop, "dry_run": dry_run, "heartbeat_age_seconds": heartbeat_age},
        }
    )
    if heartbeat_age is not None:
        decisions.append(
            {
                "source": "runtime_state",
                "timestamp": str(heartbeat_raw or now),
                "symbol": "SYSTEM",
                "direction": "",
                "decision": "healthy" if heartbeat_age < 120 else "stale_heartbeat",
                "stage": "trader_loop",
                "summary": f"Trader heartbeat age is {heartbeat_age}s.",
                "action": "normal" if heartbeat_age < 120 else "check Render logs for stalled trader loop",
                "reason": "runtime_state.last_trader_heartbeat_et",
                "metrics": {"heartbeat_age_seconds": heartbeat_age},
            }
        )
    return decisions


def _compact_order(order: dict[str, Any]) -> dict[str, Any]:
    symbol = str(order.get("symbol", "") or "")
    side = str(order.get("side", "") or "")
    status = str(order.get("status", "") or "")
    order_type = str(order.get("type", "") or "")
    filled_avg_price = _safe_float(order.get("filled_avg_price"), 0.0)
    submitted_at = str(order.get("submitted_at", "") or "")
    filled_at = str(order.get("filled_at", "") or "")
    if status == "filled":
        summary = f"Alpaca filled {side.upper()} {symbol} at {filled_avg_price:.2f}."
        action = "execution confirmed"
    elif status in {"canceled", "expired", "rejected"}:
        summary = f"Alpaca {status} {side.upper()} {symbol}."
        action = "order did not become active position/fill"
    else:
        summary = f"Alpaca order {status}: {side.upper()} {symbol}."
        action = "monitor order"
    return {
        "source": "alpaca_order",
        "timestamp": filled_at or submitted_at,
        "symbol": symbol,
        "direction": side.upper(),
        "decision": status,
        "stage": "broker_execution",
        "summary": summary,
        "action": action,
        "reason": str(order.get("reject_reason", "") or order.get("cancel_reason", "") or ""),
        "metrics": {
            "qty": _safe_float(order.get("qty"), 0.0),
            "filled_qty": _safe_float(order.get("filled_qty"), 0.0),
            "filled_avg_price": filled_avg_price,
            "limit_price": _safe_float(order.get("limit_price"), 0.0),
            "type": order_type,
        },
    }


def _alpaca_order_decisions(limit: int = 30) -> list[dict[str, Any]]:
    if not API_KEY or not SECRET_KEY:
        return []
    try:
        today_start = EASTERN.localize(datetime.combine(_now_et().date(), datetime.min.time())).astimezone(pytz.UTC).isoformat().replace("+00:00", "Z")
        resp = requests.get(
            f"{BASE_URL}/v2/orders",
            headers=HEADERS,
            params={"status": "all", "after": today_start, "limit": max(1, min(100, int(limit))), "direction": "desc", "nested": "false"},
            timeout=12,
        )
        resp.raise_for_status()
        payload = resp.json()
        if not isinstance(payload, list):
            return []
        return [_compact_order(order) for order in payload]
    except Exception:
        return []


def build_decision_journal(limit: int = 100) -> dict[str, Any]:
    decisions: list[dict[str, Any]] = []
    decisions.extend(_runtime_decisions())

    scan_rows = _read_tail(Path(config.SCAN_LOG_CSV_PATH), limit=200)
    scan_rows.reverse()
    decisions.extend(_classify_scan(row) for row in scan_rows[:40])

    try:
        entry_trace = build_entry_decision_trace(limit=40)
        for item in entry_trace.get("items", []):
            decisions.append(_classify_entry(item))
    except Exception as exc:  # noqa: BLE001
        decisions.append(
            {
                "source": "entry_accuracy",
                "timestamp": _now_et().isoformat(),
                "symbol": "SYSTEM",
                "direction": "",
                "decision": "error",
                "stage": "entry_trace",
                "summary": f"Entry trace failed: {exc}",
                "action": "check entry_decision_trace.py",
                "reason": str(exc),
                "metrics": {},
            }
        )

    vix_path = Path(getattr(config, "VIXW_REGIME_LOG_CSV_PATH", Path(config.DATA_DIR) / "vixw_regime_log.csv"))
    vix_rows = _read_tail(vix_path, limit=60)
    vix_rows.reverse()
    decisions.extend(_classify_vix_proxy(row) for row in vix_rows[:30])
    decisions.extend(_alpaca_order_decisions(limit=30))

    def sort_key(item: dict[str, Any]) -> float:
        dt = _parse_ts(item.get("timestamp"))
        return dt.timestamp() if dt is not None else 0.0

    decisions.sort(key=sort_key, reverse=True)
    decisions = decisions[: max(1, min(500, int(limit)))]

    source_counts = Counter(str(item.get("source", "unknown")) for item in decisions)
    decision_counts = Counter(str(item.get("decision", "unknown")) for item in decisions)
    stage_counts = Counter(str(item.get("stage", "unknown")) for item in decisions)
    blockers = Counter(str(item.get("reason", "") or item.get("summary", "")) for item in decisions if str(item.get("decision", "")).lower() in {"reject", "watch_only", "scanner_rejected", "skip", "error", "rejected", "canceled", "expired"})

    return {
        "generated_at_et": _now_et().isoformat(),
        "summary": {
            "total_decisions": len(decisions),
            "source_counts": dict(source_counts),
            "decision_counts": dict(decision_counts),
            "stage_counts": dict(stage_counts),
            "top_blockers": [{"reason": key, "count": value} for key, value in blockers.most_common(12)],
        },
        "decisions": decisions,
    }


if __name__ == "__main__":
    import json

    print(json.dumps(build_decision_journal(limit=100), indent=2))
