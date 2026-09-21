from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import dataclass
from datetime import UTC, datetime, time as daytime
from typing import Any, Callable

from .hosted_policy import HOSTED_PRIORITY_SYMBOLS, is_hosted_paper_runtime
from .phase1_snapshot_capture import _market_timezone_info
from .paper_readiness import _is_regular_trading_day
from .trading_cycle_v2 import TradingCycleResult, run_trading_cycle


@dataclass(frozen=True)
class SessionRunResult:
    started_at: datetime
    finished_at: datetime
    cycles_completed: int
    symbols: list[str]
    cycle_results: list[dict[str, Any]]

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "started_at": self.started_at.astimezone(UTC).isoformat(),
            "finished_at": self.finished_at.astimezone(UTC).isoformat(),
            "cycles_completed": self.cycles_completed,
            "symbols": self.symbols,
            "cycle_results": self.cycle_results,
        }


def run_trading_session(
    *,
    symbols: list[str],
    interval_seconds: int,
    start_time: daytime | None = None,
    end_time: daytime | None = None,
    market_timezone: str = "UTC",
    max_cycles: int | None = None,
    symbol_batch_size: int | None = None,
    continuous_window: bool = False,
    now_fn: Callable[[], datetime] | None = None,
    sleep_fn: Callable[[float], None] = time.sleep,
    cycle_runner: Callable[..., TradingCycleResult] = run_trading_cycle,
    cycle_kwargs: dict[str, Any] | None = None,
    on_cycle_complete: Callable[[dict[str, Any]], None] | None = None,
) -> SessionRunResult:
    started_at = _now(now_fn)
    results: list[dict[str, Any]] = []
    cycles_completed = 0
    kwargs = cycle_kwargs or {}

    while True:
        current = _now(now_fn)
        current_local = _session_local_datetime(current, market_timezone)
        current_time = current_local.time().replace(tzinfo=None)
        if max_cycles is not None and cycles_completed >= max_cycles:
            break
        if not _is_regular_trading_day(current_local.date()):
            if _should_wait_for_next_window(continuous_window=continuous_window, max_cycles=max_cycles):
                sleep_fn(interval_seconds)
                continue
            break
        if start_time and current_time < start_time:
            sleep_fn(interval_seconds)
            continue
        if end_time and current_time > end_time:
            if _should_wait_for_next_window(continuous_window=continuous_window, max_cycles=max_cycles):
                sleep_fn(interval_seconds)
                continue
            break

        cycle_symbols = _cycle_symbols(symbols, cycle_index=cycles_completed, batch_size=symbol_batch_size)
        try:
            result = cycle_runner(symbols=cycle_symbols, **kwargs)
        except Exception as exc:
            # A single bad cycle (e.g. a data-feed hiccup) must not end the
            # whole session -- keep the loop alive and retry next interval.
            cycle_payload = {"error": f"{type(exc).__name__}: {exc}", "symbols": cycle_symbols}
            results.append(cycle_payload)
            cycles_completed += 1
            if on_cycle_complete is not None:
                on_cycle_complete(cycle_payload)
            if max_cycles is not None and cycles_completed >= max_cycles:
                break
            sleep_fn(interval_seconds)
            continue
        cycle_payload = result.to_json_dict()
        results.append(cycle_payload)
        cycles_completed += 1
        if on_cycle_complete is not None:
            on_cycle_complete(cycle_payload)

        if max_cycles is not None and cycles_completed >= max_cycles:
            break
        sleep_fn(interval_seconds)

    finished_at = _now(now_fn)
    return SessionRunResult(
        started_at=started_at,
        finished_at=finished_at,
        cycles_completed=cycles_completed,
        symbols=[symbol.upper() for symbol in symbols],
        cycle_results=results,
    )


def _cycle_symbols(symbols: list[str], *, cycle_index: int, batch_size: int | None) -> list[str]:
    normalized = [symbol.upper() for symbol in symbols]
    if batch_size is None or batch_size <= 0 or batch_size >= len(normalized):
        return normalized
    configured_priority = (
        list(HOSTED_PRIORITY_SYMBOLS)
        if is_hosted_paper_runtime()
        else [
            symbol.strip().upper()
            for symbol in (os.getenv("AUTOBOTT_SESSION_PRIORITY_SYMBOLS") or "").split(",")
            if symbol.strip()
        ]
    )
    priority = [symbol for symbol in configured_priority if symbol in normalized][:batch_size]
    rotating = [symbol for symbol in normalized if symbol not in set(priority)]
    rotating_batch_size = batch_size - len(priority)
    if rotating_batch_size <= 0 or not rotating:
        return priority
    start = (cycle_index * rotating_batch_size) % len(rotating)
    end = start + rotating_batch_size
    batch = rotating[start:end] if end <= len(rotating) else rotating[start:] + rotating[: end - len(rotating)]
    return priority + batch


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run repeated AutoBott trading cycles on a fixed cadence.")
    parser.add_argument("--symbols", nargs="+", required=True, help="Ticker list, for example: AAPL MSFT NVDA")
    parser.add_argument("--interval-seconds", type=int, default=300, help="Seconds between trading cycles.")
    parser.add_argument("--max-cycles", type=int, default=1, help="Maximum cycles to run in this session.")
    args = parser.parse_args(argv)

    result = run_trading_session(
        symbols=args.symbols,
        interval_seconds=args.interval_seconds,
        max_cycles=args.max_cycles,
    )
    print(json.dumps(result.to_json_dict(), indent=2, sort_keys=True))
    return 0


def _now(now_fn: Callable[[], datetime] | None) -> datetime:
    return (now_fn or (lambda: datetime.now(tz=UTC)))()


def _session_local_datetime(current: datetime, market_timezone: str) -> datetime:
    if market_timezone.strip().upper() == "UTC":
        return current.astimezone(UTC)
    tz = _market_timezone_info(market_timezone, current.astimezone(UTC).date())
    return current.astimezone(tz)


def _should_wait_for_next_window(*, continuous_window: bool, max_cycles: int | None) -> bool:
    return continuous_window


if __name__ == "__main__":
    raise SystemExit(main())
