"""Daily proof snapshot for AutoBott paper/live performance evidence.

This module is intentionally read-only. It does not import broker clients,
place orders, change controls, alter strategy state, or write runtime state.

Run from repo root:
    python autotrader/proof_snapshot.py
    python autotrader/proof_snapshot.py --date 2026-05-01
    python autotrader/proof_snapshot.py --output autotrader/reports/proof_snapshot.json

The output is structured JSON suitable for screenshots, investor proof logs,
Discord summaries, or later LISA ingestion.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

try:
    import pytz
except Exception:  # noqa: BLE001
    pytz = None

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

try:
    from autotrader import config
except ImportError:
    import config  # type: ignore


@dataclass(frozen=True)
class ProofPaths:
    trades_csv: Path
    scan_log_csv: Path


def _now_et() -> datetime:
    tz_name = str(getattr(config, "EASTERN_TZ", "US/Eastern") or "US/Eastern")
    if pytz is not None:
        try:
            return datetime.now(pytz.timezone(tz_name))
        except Exception:  # noqa: BLE001
            pass
    return datetime.now()


def _parse_date(value: str | None) -> date:
    if value is None or not str(value).strip():
        return _now_et().date()
    raw = str(value).strip()
    try:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    except ValueError as exc:
        raise SystemExit(f"Invalid --date value {raw!r}. Expected YYYY-MM-DD.") from exc


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return float(default)
    if math.isnan(parsed) or math.isinf(parsed):
        return float(default)
    return parsed


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return int(default)


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    try:
        with path.open("r", newline="", encoding="utf-8") as handle:
            return list(csv.DictReader(handle))
    except Exception as exc:  # noqa: BLE001
        return [{"_read_error": str(exc), "_path": str(path)}]


def _parse_timestamp(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None

    eastern = None
    central = None
    if pytz is not None:
        try:
            eastern = pytz.timezone(str(getattr(config, "EASTERN_TZ", "US/Eastern") or "US/Eastern"))
            central = pytz.timezone(str(getattr(config, "CENTRAL_TZ", "US/Central") or "US/Central"))
        except Exception:  # noqa: BLE001
            eastern = None
            central = None

    # ISO timestamps, including Z suffix.
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if parsed.tzinfo is None and eastern is not None:
            parsed = eastern.localize(parsed)
        return parsed.astimezone(eastern) if eastern is not None and parsed.tzinfo is not None else parsed
    except ValueError:
        pass

    # Common runtime format: 2026-04-30 09:33:12 EDT
    for suffix, zone in ((" EDT", eastern), (" EST", eastern), (" CDT", central), (" CST", central), (" UTC", pytz.UTC if pytz is not None else None)):
        if raw.upper().endswith(suffix.strip()):
            base = raw[: -len(suffix)].strip()
            try:
                parsed = datetime.strptime(base, "%Y-%m-%d %H:%M:%S")
                if zone is not None:
                    return zone.localize(parsed).astimezone(eastern) if eastern is not None else zone.localize(parsed)
                return parsed
            except ValueError:
                pass

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(raw, fmt)
            return eastern.localize(parsed) if eastern is not None else parsed
        except ValueError:
            continue
    return None


def _row_date(row: dict[str, str]) -> date | None:
    explicit = str(row.get("date", "") or "").strip()
    if explicit:
        try:
            return datetime.strptime(explicit[:10], "%Y-%m-%d").date()
        except ValueError:
            pass
    for key in ("timestamp", "entry_time", "exit_time"):
        parsed = _parse_timestamp(row.get(key, ""))
        if parsed is not None:
            return parsed.date()
    return None


def _pnl_usd(row: dict[str, str]) -> float:
    for key in ("realized_pnl_usd", "paper_reported_pnl_usd", "conservative_executable_pnl_usd"):
        raw = str(row.get(key, "") or "").strip()
        if raw:
            return _safe_float(raw, 0.0)

    entry = _safe_float(row.get("entry_price"), 0.0)
    exit_price = _safe_float(row.get("exit_price"), 0.0)
    qty = _safe_int(row.get("qty"), 0)
    if entry > 0 and exit_price > 0 and qty > 0:
        return (exit_price - entry) * qty * 100.0
    return 0.0


def _pnl_pct(row: dict[str, str]) -> float:
    for key in ("pnl_pct", "paper_reported_pnl_pct", "conservative_executable_pnl_pct"):
        raw = str(row.get(key, "") or "").strip()
        if raw:
            value = _safe_float(raw, 0.0)
            # Stored pnl_pct is usually a decimal fraction. Some reports may store percent points.
            return value * 100.0 if abs(value) <= 5.0 else value
    entry = _safe_float(row.get("entry_price"), 0.0)
    exit_price = _safe_float(row.get("exit_price"), 0.0)
    if entry > 0 and exit_price > 0:
        return ((exit_price - entry) / entry) * 100.0
    return 0.0


def _round_money(value: float) -> float:
    return round(float(value), 2)


def _summarize_trades(rows: list[dict[str, str]]) -> dict[str, Any]:
    total = len(rows)
    pnls = [_pnl_usd(row) for row in rows]
    pnl_pcts = [_pnl_pct(row) for row in rows]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]
    flats = [p for p in pnls if p == 0]

    by_ticker: dict[str, list[float]] = defaultdict(list)
    by_direction: dict[str, list[float]] = defaultdict(list)
    by_exit_reason: Counter[str] = Counter()
    best_trade: dict[str, Any] | None = None
    worst_trade: dict[str, Any] | None = None

    cumulative = 0.0
    peak = 0.0
    max_drawdown = 0.0

    for row, pnl, pnl_pct in zip(rows, pnls, pnl_pcts, strict=False):
        ticker = str(row.get("ticker", "") or row.get("symbol", "") or "UNKNOWN").upper()
        direction = str(row.get("direction", "") or "UNKNOWN").upper()
        exit_reason = str(row.get("exit_reason", "") or "unknown").strip() or "unknown"
        by_ticker[ticker].append(pnl)
        by_direction[direction].append(pnl)
        by_exit_reason[exit_reason] += 1

        cumulative += pnl
        peak = max(peak, cumulative)
        max_drawdown = min(max_drawdown, cumulative - peak)

        item = {
            "ticker": ticker,
            "direction": direction,
            "option_symbol": str(row.get("option_symbol", "") or ""),
            "qty": _safe_int(row.get("qty"), 0),
            "pnl_usd": _round_money(pnl),
            "pnl_pct": round(pnl_pct, 2),
            "exit_reason": exit_reason,
            "entry_time": str(row.get("entry_time", "") or row.get("timestamp", "") or ""),
            "exit_time": str(row.get("exit_time", "") or ""),
        }
        if best_trade is None or pnl > float(best_trade["pnl_usd"]):
            best_trade = item
        if worst_trade is None or pnl < float(worst_trade["pnl_usd"]):
            worst_trade = item

    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else None
    avg_win = (sum(wins) / len(wins)) if wins else 0.0
    avg_loss = (sum(losses) / len(losses)) if losses else 0.0

    ticker_rows = []
    for ticker, values in by_ticker.items():
        ticker_rows.append(
            {
                "ticker": ticker,
                "trades": len(values),
                "pnl_usd": _round_money(sum(values)),
                "win_rate_pct": round((len([v for v in values if v > 0]) / len(values)) * 100.0, 2) if values else 0.0,
            }
        )
    ticker_rows.sort(key=lambda item: float(item["pnl_usd"]), reverse=True)

    direction_rows = []
    for direction, values in by_direction.items():
        direction_rows.append(
            {
                "direction": direction,
                "trades": len(values),
                "pnl_usd": _round_money(sum(values)),
                "win_rate_pct": round((len([v for v in values if v > 0]) / len(values)) * 100.0, 2) if values else 0.0,
            }
        )
    direction_rows.sort(key=lambda item: float(item["pnl_usd"]), reverse=True)

    return {
        "closed_trades": total,
        "wins": len(wins),
        "losses": len(losses),
        "flats": len(flats),
        "win_rate_pct": round((len(wins) / total) * 100.0, 2) if total else 0.0,
        "total_pnl_usd": _round_money(sum(pnls)),
        "avg_trade_pnl_usd": _round_money(sum(pnls) / total) if total else 0.0,
        "avg_trade_pnl_pct": round(sum(pnl_pcts) / total, 2) if total else 0.0,
        "gross_profit_usd": _round_money(gross_profit),
        "gross_loss_usd": _round_money(gross_loss),
        "profit_factor": round(profit_factor, 4) if profit_factor is not None else None,
        "avg_win_usd": _round_money(avg_win),
        "avg_loss_usd": _round_money(avg_loss),
        "max_closed_trade_drawdown_usd": _round_money(max_drawdown),
        "best_trade": best_trade,
        "worst_trade": worst_trade,
        "by_ticker": ticker_rows,
        "by_direction": direction_rows,
        "by_exit_reason": [{"exit_reason": key, "count": value} for key, value in by_exit_reason.most_common()],
    }


def _summarize_scans(rows: list[dict[str, str]]) -> dict[str, Any]:
    total = len(rows)
    passes = []
    failures = []
    by_symbol: Counter[str] = Counter()
    by_reason: Counter[str] = Counter()
    top_signals: list[dict[str, Any]] = []

    for row in rows:
        symbol = str(row.get("symbol", "") or row.get("ticker", "") or "UNKNOWN").upper()
        result = str(row.get("result", "") or "").strip().lower()
        by_symbol[symbol] += 1
        if result == "pass":
            passes.append(row)
            top_signals.append(
                {
                    "symbol": symbol,
                    "direction": str(row.get("direction", "") or "").upper(),
                    "signal_score": round(_safe_float(row.get("signal_score"), 0.0), 2),
                    "rvol": round(_safe_float(row.get("rvol"), 0.0), 2),
                    "roc": round(_safe_float(row.get("roc"), 0.0), 4),
                    "reason": str(row.get("reason", "") or ""),
                    "timestamp": str(row.get("timestamp", "") or ""),
                }
            )
        else:
            failures.append(row)
            reason = str(row.get("reason", "") or "unknown").strip() or "unknown"
            by_reason[reason] += 1

    top_signals.sort(key=lambda item: (float(item["signal_score"]), float(item["rvol"])), reverse=True)

    return {
        "scan_rows": total,
        "pass_rows": len(passes),
        "fail_rows": len(failures),
        "pass_rate_pct": round((len(passes) / total) * 100.0, 2) if total else 0.0,
        "symbols_scanned": len(by_symbol),
        "top_symbols_by_scan_count": [{"symbol": key, "count": value} for key, value in by_symbol.most_common(12)],
        "top_failure_reasons": [{"reason": key, "count": value} for key, value in by_reason.most_common(12)],
        "top_pass_signals": top_signals[:12],
    }


def build_snapshot(target_date: date, paths: ProofPaths | None = None) -> dict[str, Any]:
    resolved_paths = paths or ProofPaths(
        trades_csv=Path(config.TRADES_CSV_PATH),
        scan_log_csv=Path(config.SCAN_LOG_CSV_PATH),
    )
    trade_rows_all = _read_csv(resolved_paths.trades_csv)
    scan_rows_all = _read_csv(resolved_paths.scan_log_csv)

    trade_read_errors = [row for row in trade_rows_all if "_read_error" in row]
    scan_read_errors = [row for row in scan_rows_all if "_read_error" in row]

    trade_rows = [row for row in trade_rows_all if "_read_error" not in row and _row_date(row) == target_date]
    scan_rows = [row for row in scan_rows_all if "_read_error" not in row and _row_date(row) == target_date]

    return {
        "metadata": {
            "generated_at_et": _now_et().isoformat(),
            "target_date": target_date.isoformat(),
            "paper_mode": bool(getattr(config, "PAPER", True)),
            "trades_csv": str(resolved_paths.trades_csv),
            "scan_log_csv": str(resolved_paths.scan_log_csv),
            "trade_read_errors": trade_read_errors,
            "scan_read_errors": scan_read_errors,
        },
        "trade_summary": _summarize_trades(trade_rows),
        "scan_summary": _summarize_scans(scan_rows),
    }


def _write_snapshot(snapshot: dict[str, Any], output: Path | None) -> None:
    payload = json.dumps(snapshot, indent=2, sort_keys=False)
    if output is None:
        print(payload)
        return
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(payload + "\n", encoding="utf-8")
    print(f"Wrote proof snapshot to {output}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Emit a read-only AutoBott daily proof snapshot.")
    parser.add_argument("--date", dest="date_value", default=None, help="Trading date to summarize in YYYY-MM-DD format. Defaults to today in ET.")
    parser.add_argument("--trades-csv", default=None, help="Optional override path for trades.csv.")
    parser.add_argument("--scan-log-csv", default=None, help="Optional override path for scan_log.csv.")
    parser.add_argument("--output", default=None, help="Optional JSON output path. Prints to stdout when omitted.")
    args = parser.parse_args()

    target_date = _parse_date(args.date_value)
    paths = ProofPaths(
        trades_csv=Path(args.trades_csv) if args.trades_csv else Path(config.TRADES_CSV_PATH),
        scan_log_csv=Path(args.scan_log_csv) if args.scan_log_csv else Path(config.SCAN_LOG_CSV_PATH),
    )
    snapshot = build_snapshot(target_date, paths)
    _write_snapshot(snapshot, Path(args.output) if args.output else None)


if __name__ == "__main__":
    main()
