from __future__ import annotations

import argparse
import json
import time
from dataclasses import dataclass
from datetime import UTC, datetime, time as daytime
from typing import Any, Callable

from .phase1_snapshot_capture import _market_timezone_info
from .paper_readiness import _is_regular_trading_day
from .trading_cycle import TradingCycleResult, run_trading_cycle


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
        result = cycle_runner(symbols=cycle_symbols, **kwargs)
        results.append(result.to_json_dict())
        cycles_completed += 1

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
    start = (cycle_index * batch_size) % len(normalized)
    end = start + batch_size
    if end <= len(normalized):
        return normalized[start:end]
    return normalized[start:] + normalized[: end - len(normalized)]


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
