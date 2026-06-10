"""Deterministic local reconciliation for broker option fills vs trades.csv.

This utility is read-only: it fetches recent orders (optional), compares them
against local closed-trade rows, and writes an evidence JSON report.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pytz

import config

OPTION_SYMBOL_RE = re.compile(r"^[A-Z.]+\d{6}[CP]\d{8}$")
EASTERN = pytz.timezone(config.EASTERN_TZ)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _is_option_symbol(symbol: str) -> bool:
    return bool(OPTION_SYMBOL_RE.match(str(symbol or "").upper().strip()))


def _parse_ts(value: str) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = EASTERN.localize(dt)
        return dt.astimezone(EASTERN)
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


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _normalize_order(order: Any) -> dict[str, Any]:
    if isinstance(order, dict):
        return {
            "id": str(order.get("id", "") or ""),
            "symbol": str(order.get("symbol", "") or "").upper(),
            "status": str(order.get("status", "") or "").lower(),
            "side": str(order.get("side", "") or "").lower(),
            "filled_qty": _safe_float(order.get("filled_qty"), 0.0),
            "submitted_at": str(order.get("submitted_at", "") or ""),
            "filled_at": str(order.get("filled_at", "") or ""),
            "source": str(order.get("source", "") or ""),
        }

    return {
        "id": str(getattr(order, "id", "") or ""),
        "symbol": str(getattr(order, "symbol", "") or "").upper(),
        "status": str(getattr(order, "status", "") or "").lower(),
        "side": str(getattr(order, "side", "") or "").lower(),
        "filled_qty": _safe_float(getattr(order, "filled_qty", 0.0), 0.0),
        "submitted_at": str(getattr(order, "submitted_at", "") or ""),
        "filled_at": str(getattr(order, "filled_at", "") or ""),
        "source": str(getattr(order, "client_order_id", "") or ""),
    }


def build_reconciliation_report(
    *,
    orders: list[Any],
    trades_csv_rows: list[dict[str, str]],
    target_day: date,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    now = generated_at.astimezone(EASTERN) if generated_at else datetime.now(EASTERN)

    normalized_orders = [_normalize_order(order) for order in orders]
    normalized_orders.sort(key=lambda row: (str(row.get("submitted_at", "")), str(row.get("id", ""))))

    broker_rows_today: list[dict[str, Any]] = []
    broker_sell_filled_symbols: set[str] = set()
    broker_status_counts: dict[str, int] = {}

    for row in normalized_orders:
        symbol = str(row.get("symbol", "") or "")
        if not _is_option_symbol(symbol):
            continue
        submitted_dt = _parse_ts(str(row.get("submitted_at", "") or ""))
        if submitted_dt is None or submitted_dt.date() != target_day:
            continue
        broker_rows_today.append(row)
        status = str(row.get("status", "") or "").lower()
        broker_status_counts[status] = broker_status_counts.get(status, 0) + 1
        if status in {"filled", "partially_filled"} and _safe_float(row.get("filled_qty"), 0.0) > 0:
            if str(row.get("side", "") or "").lower() == "sell":
                broker_sell_filled_symbols.add(symbol)

    trades_rows_today: list[dict[str, str]] = []
    trade_symbols_today: set[str] = set()
    for row in trades_csv_rows:
        ts_dt = _parse_ts(str(row.get("timestamp", "") or ""))
        if ts_dt is None or ts_dt.date() != target_day:
            continue
        symbol = str(row.get("option_symbol", "") or "").upper().strip()
        if not _is_option_symbol(symbol):
            continue
        trades_rows_today.append(row)
        trade_symbols_today.add(symbol)

    missing_symbols = sorted(symbol for symbol in broker_sell_filled_symbols if symbol not in trade_symbols_today)

    option_filled_orders_today = sum(
        1
        for row in broker_rows_today
        if str(row.get("status", "") or "") in {"filled", "partially_filled"}
        and _safe_float(row.get("filled_qty"), 0.0) > 0
    )
    option_sell_fills_today = sum(
        1
        for row in broker_rows_today
        if str(row.get("status", "") or "") in {"filled", "partially_filled"}
        and _safe_float(row.get("filled_qty"), 0.0) > 0
        and str(row.get("side", "") or "") == "sell"
    )
    option_buy_fills_today = sum(
        1
        for row in broker_rows_today
        if str(row.get("status", "") or "") in {"filled", "partially_filled"}
        and _safe_float(row.get("filled_qty"), 0.0) > 0
        and str(row.get("side", "") or "") == "buy"
    )

    return {
        "report_type": "trade_reconciliation_v1",
        "generated_at": now.isoformat(),
        "target_day": target_day.isoformat(),
        "read_only": True,
        "execution_allowed": False,
        "mutation_allowed": False,
        "live_export_allowed": False,
        "broker": {
            "option_orders_today": len(broker_rows_today),
            "option_filled_orders_today": option_filled_orders_today,
            "option_buy_fills_today": option_buy_fills_today,
            "option_sell_fills_today": option_sell_fills_today,
            "status_counts": broker_status_counts,
            "symbols_with_sell_fills": sorted(broker_sell_filled_symbols),
        },
        "trades_csv": {
            "path": str(config.TRADES_CSV_PATH),
            "option_trade_rows_today": len(trades_rows_today),
            "option_symbols_today": sorted(trade_symbols_today),
        },
        "comparison": {
            "sell_fills_minus_trade_rows": int(option_sell_fills_today - len(trades_rows_today)),
            "missing_symbols_in_trades_csv": missing_symbols,
            "is_balanced": len(missing_symbols) == 0 and option_sell_fills_today == len(trades_rows_today),
        },
        "samples": {
            "broker_orders_today": broker_rows_today[:25],
            "trades_csv_rows_today": trades_rows_today[:25],
        },
    }


def _fetch_recent_orders(limit: int) -> list[Any]:
    from broker import AlpacaBroker
    from env_config import get_required_env, load_runtime_env

    load_runtime_env()
    api_key = get_required_env("ALPACA_API_KEY")
    secret_key = get_required_env("ALPACA_SECRET_KEY")
    broker = AlpacaBroker(api_key=api_key, secret_key=secret_key, paper=bool(config.PAPER))
    return broker.get_recent_orders(limit=max(1, int(limit)))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only reconciliation for broker fills vs trades.csv")
    parser.add_argument("--date", default="", help="Target day in YYYY-MM-DD (default: today ET)")
    parser.add_argument("--broker", action="store_true", help="Fetch live broker orders (read-only API call)")
    parser.add_argument("--orders-json", default="", help="Optional orders JSON file path (array payload)")
    parser.add_argument("--limit", type=int, default=500, help="Broker order fetch limit")
    parser.add_argument("--output", default="", help="Optional output path for evidence JSON")
    return parser.parse_args()


def _default_output_path(target_day: date) -> Path:
    configured_root = Path(config.DATA_DIR)
    configured_dir = configured_root / "service_logs"
    try:
        configured_dir.mkdir(parents=True, exist_ok=True)
        test_file = configured_dir / ".write_probe"
        test_file.write_text("ok", encoding="utf-8")
        test_file.unlink(missing_ok=True)
        return configured_dir / f"trade_reconciliation_{target_day.isoformat()}.json"
    except Exception:
        local_dir = Path(__file__).resolve().parent / "service_logs"
        local_dir.mkdir(parents=True, exist_ok=True)
        return local_dir / f"trade_reconciliation_{target_day.isoformat()}.json"


def main() -> int:
    args = _parse_args()
    target_day = datetime.now(EASTERN).date()
    if str(args.date or "").strip():
        target_day = datetime.fromisoformat(str(args.date).strip()).date()

    orders: list[Any] = []
    if bool(args.broker):
        orders.extend(_fetch_recent_orders(limit=max(1, int(args.limit))))

    orders_json_path = Path(str(args.orders_json or "").strip()).expanduser() if str(args.orders_json or "").strip() else None
    if orders_json_path is not None:
        payload = json.loads(orders_json_path.read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            raise ValueError("orders-json must contain a JSON array")
        orders.extend(payload)

    trades_rows = _read_csv_rows(config.TRADES_CSV_PATH)
    report = build_reconciliation_report(orders=orders, trades_csv_rows=trades_rows, target_day=target_day)

    out_path = (
        Path(str(args.output).strip()).expanduser()
        if str(args.output or "").strip()
        else _default_output_path(target_day)
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(
        json.dumps(
            {
                "ok": True,
                "report_path": str(out_path),
                "target_day": report["target_day"],
                "sell_fills": report["broker"]["option_sell_fills_today"],
                "trade_rows": report["trades_csv"]["option_trade_rows_today"],
                "missing_symbols": report["comparison"]["missing_symbols_in_trades_csv"],
                "is_balanced": report["comparison"]["is_balanced"],
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
