from __future__ import annotations

import argparse
import json
from datetime import UTC, date, datetime, time as daytime, timedelta
from pathlib import Path
from typing import Any

from .execution_config import load_alpaca_execution_config
from .phase1_alpaca_client import AlpacaPaperClient
from .phase1_alpaca_config import load_alpaca_paper_config
from .phase1_engine import build_decision_card
from .phase1_snapshot_capture import CaptureRules, capture_symbol_snapshot
from .phase1_snapshot_capture import _market_timezone_info
from .phase1_validate import _decision_input_from_snapshot, _load_snapshot
from .position_store import load_open_positions
from .runtime_control import arm_paper_execution, load_runtime_state
from .runtime_paths import phase1_snapshots_root


class _ProbeDataClient:
    def __init__(self, client: Any) -> None:
        self._client = client

    def get_account(self) -> dict[str, Any]:
        return self._client.get_account()

    def get_latest_stock_quotes(self, symbols: list[str]) -> dict[str, dict[str, Any]]:
        return self._client.get_latest_stock_quotes(symbols)

    def get_option_chain_snapshots(self, symbol: str) -> dict[str, dict[str, Any]]:
        return self._client.get_option_chain_snapshots(symbol)

    def get_stock_bars(
        self,
        symbols: list[str],
        *,
        start: datetime,
        end: datetime,
        timeframe: str = "1Min",
        limit: int = 35,
    ) -> dict[str, list[dict[str, Any]]]:
        bars = self._client.get_stock_bars(symbols, start=start, end=end, timeframe=timeframe, limit=limit)
        resolved = {symbol.upper(): list(rows) for symbol, rows in bars.items()}
        for symbol in symbols:
            symbol_key = symbol.upper()
            if len(resolved.get(symbol_key, [])) >= 30:
                continue
            fallback_rows = self._fallback_stock_bars(symbol_key, timeframe=timeframe, limit=limit, start_date=end.date())
            if fallback_rows:
                resolved[symbol_key] = fallback_rows
        return resolved

    def _fallback_stock_bars(self, symbol: str, *, timeframe: str, limit: int, start_date: date) -> list[dict[str, Any]]:
        for probe_time in _fallback_probe_market_times(start_date, count=7):
            fallback_end = probe_time
            fallback_start = fallback_end - timedelta(minutes=max(40, limit + 5))
            bars = self._client.get_stock_bars([symbol], start=fallback_start, end=fallback_end, timeframe=timeframe, limit=limit)
            rows = list(bars.get(symbol.upper(), []))
            if len(rows) >= 30:
                return rows
        return []


def run_paper_readiness_probe(
    *,
    symbol: str = "SPY",
    client: Any | None = None,
    corpus_root: str | Path | None = None,
    scheduled_market_time: datetime | None = None,
    captured_at_utc: datetime | None = None,
    rules: CaptureRules | None = None,
) -> dict[str, Any]:
    config = load_alpaca_paper_config()
    execution_config = load_alpaca_execution_config()
    runtime_state = load_runtime_state()
    positions = load_open_positions()
    probe_market_time = scheduled_market_time or datetime.now(tz=UTC)
    response: dict[str, Any] = {
        "ok": False,
        "symbol": symbol.upper(),
        "paper_config_valid": False,
        "paper_execution_config_valid": False,
        "credentials_present": bool(config.api_key and config.secret_key),
        "runtime_execution_enabled": runtime_state.execution_enabled,
        "runtime_kill_switch_enabled": runtime_state.kill_switch_enabled,
        "runtime_live_mode_enabled": runtime_state.live_mode_enabled,
        "open_position_count": len(positions),
        "order_placement_configured": bool(execution_config.allow_order_placement),
        "paper_trade_through_enabled": bool(execution_config.paper_trade_all_passed_signals),
        "paper_execution_ready": False,
        "execution_blockers": [],
        "probe_market_time": probe_market_time.isoformat(),
    }

    try:
        validated = config.validate()
        response["paper_config_valid"] = True
    except Exception as exc:
        response["status"] = "config_invalid"
        response["detail"] = str(exc)
        return response

    try:
        validated_execution_config = execution_config.validate()
        response["paper_execution_config_valid"] = True
        response["effective_max_open_positions"] = validated_execution_config.effective_max_open_positions()
        response["effective_max_new_entry_attempts_per_loop"] = validated_execution_config.effective_max_new_entry_attempts_per_loop()
    except Exception as exc:
        validated_execution_config = None
        response["execution_config_detail"] = str(exc)

    resolved_client = _ProbeDataClient(client or AlpacaPaperClient(validated))
    resolved_symbol = symbol.upper()

    try:
        account = resolved_client.get_account()
        quotes = resolved_client.get_latest_stock_quotes([resolved_symbol, "SPY", "QQQ"])
        option_snapshots = resolved_client.get_option_chain_snapshots(resolved_symbol)
    except Exception as exc:
        response["status"] = "paper_connectivity_failed"
        response["detail"] = str(exc)
        return response

    response["account_status"] = str(account.get("status", "unknown"))
    response["quote_symbols"] = sorted(quotes.keys())
    response["option_snapshot_count"] = len(option_snapshots)

    try:
        snapshot_path = _capture_probe_snapshot(
            symbol=resolved_symbol,
            corpus_root=Path(corpus_root) if corpus_root is not None else phase1_snapshots_root(),
            scheduled_market_time=probe_market_time,
            captured_at_utc=captured_at_utc or datetime.now(tz=UTC),
            data_client=resolved_client,
            rules=rules or CaptureRules(),
        )
        snapshot = _load_snapshot(Path(snapshot_path))
        decision = build_decision_card(_decision_input_from_snapshot(snapshot))
    except Exception as exc:
        response["status"] = "snapshot_or_decision_failed"
        response["detail"] = str(exc)
        return response

    response.update(
        {
            "ok": True,
            "snapshot_path": snapshot_path,
            "option_chain_count": len(snapshot.get("option_chain", [])),
            "decision_status": decision.decision.value,
            "trade_setup": decision.trade_setup.value,
            "execution_layer": decision.execution_layer.value,
            "selected_contract": decision.selected_contract.option_symbol if decision.selected_contract else None,
        }
    )
    blockers = _execution_blockers(validated_execution_config, runtime_state)
    response["execution_blockers"] = blockers
    response["paper_execution_ready"] = not blockers
    response["status"] = "paper_trading_ready" if not blockers else "paper_data_ready_execution_blocked"
    return response


def _execution_blockers(execution_config: Any, runtime_state: Any) -> list[str]:
    blockers: list[str] = []
    if execution_config is None:
        blockers.append("execution_config_invalid")
        return blockers
    if not execution_config.allow_order_placement:
        blockers.append("order_placement_disabled")
    if runtime_state.kill_switch_enabled:
        blockers.append("kill_switch_enabled")
    if not runtime_state.execution_enabled:
        blockers.append("runtime_execution_disabled")
    if runtime_state.live_mode_enabled:
        blockers.append("live_mode_should_be_disabled_for_paper")
    return blockers


def _capture_probe_snapshot(
    *,
    symbol: str,
    corpus_root: Path,
    scheduled_market_time: datetime,
    captured_at_utc: datetime,
    data_client: Any,
    rules: CaptureRules,
) -> str:
    return capture_symbol_snapshot(
        symbol=symbol,
        corpus_root=corpus_root,
        scheduled_market_time=scheduled_market_time,
        captured_at_utc=captured_at_utc,
        corpus_type="paper_capture",
        market_timezone="America/New_York",
        volatility_proxy_symbol="UVXY",
        data_client=data_client,
        rules=rules,
    )


def _fallback_probe_market_times(start_date: date, *, count: int) -> list[datetime]:
    fallback_dates: list[datetime] = []
    cursor = start_date
    while len(fallback_dates) < count:
        cursor = _previous_regular_trading_day(cursor)
        fallback_dates.append(datetime.combine(cursor, daytime(15, 45), tzinfo=_market_timezone_info("America/New_York", cursor)).astimezone(UTC))
    return fallback_dates


def _previous_regular_trading_day(value: date) -> date:
    cursor = value - timedelta(days=1)
    while not _is_regular_trading_day(cursor):
        cursor -= timedelta(days=1)
    return cursor


def _is_regular_trading_day(value: date) -> bool:
    return value.weekday() < 5 and value not in _us_market_holidays(value.year)


def _us_market_holidays(year: int) -> set[date]:
    holidays = {
        _observed_date(date(year, 1, 1)),
        _nth_weekday_of_month(year, 1, 0, 3),
        _nth_weekday_of_month(year, 2, 0, 3),
        _good_friday(year),
        _last_weekday_of_month(year, 5, 0),
        _observed_date(date(year, 6, 19)),
        _observed_date(date(year, 7, 4)),
        _nth_weekday_of_month(year, 9, 0, 1),
        _nth_weekday_of_month(year, 11, 3, 4),
        _observed_date(date(year, 12, 25)),
    }
    return holidays


def _observed_date(value: date) -> date:
    if value.weekday() == 5:
        return value - timedelta(days=1)
    if value.weekday() == 6:
        return value + timedelta(days=1)
    return value


def _nth_weekday_of_month(year: int, month: int, weekday: int, occurrence: int) -> date:
    cursor = date(year, month, 1)
    while cursor.weekday() != weekday:
        cursor += timedelta(days=1)
    cursor += timedelta(weeks=occurrence - 1)
    return cursor


def _last_weekday_of_month(year: int, month: int, weekday: int) -> date:
    if month == 12:
        cursor = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        cursor = date(year, month + 1, 1) - timedelta(days=1)
    while cursor.weekday() != weekday:
        cursor -= timedelta(days=1)
    return cursor


def _good_friday(year: int) -> date:
    return _easter_sunday(year) - timedelta(days=2)


def _easter_sunday(year: int) -> date:
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Probe AutoBott paper-market readiness with real Alpaca paper data.")
    parser.add_argument("--symbol", default="SPY", help="Primary symbol to probe.")
    parser.add_argument("--corpus-root", help="Optional capture output root.")
    parser.add_argument("--arm-runtime", action="store_true", help="Arm paper runtime execution before probing readiness.")
    parser.add_argument("--arm-reason", default="paper_readiness_cli", help="Reason to record if arming paper runtime.")
    parser.add_argument(
        "--require-trading-ready",
        action="store_true",
        help="Return a non-zero exit code unless the final status is paper_trading_ready.",
    )
    args = parser.parse_args(argv)
    if args.arm_runtime:
        arm_paper_execution(reason=args.arm_reason)
    result = run_paper_readiness_probe(symbol=args.symbol, corpus_root=args.corpus_root)
    print(json.dumps(result, indent=2, sort_keys=True))
    if args.require_trading_ready and result.get("status") != "paper_trading_ready":
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
