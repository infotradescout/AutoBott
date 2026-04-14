"""Pure trading-session rule helpers used by runtime and tests."""

from __future__ import annotations

from datetime import datetime


def _minutes_from_hhmm(value: str) -> int:
    raw = str(value or "").strip()
    parts = raw.split(":", 1)
    if len(parts) != 2:
        raise ValueError(f"invalid HH:MM value: {value!r}")
    hour = int(parts[0])
    minute = int(parts[1])
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise ValueError(f"invalid HH:MM value: {value!r}")
    return hour * 60 + minute


def _is_at_or_after(now_et: datetime, hhmm: str) -> bool:
    return (now_et.hour * 60 + now_et.minute) >= _minutes_from_hhmm(hhmm)


def should_trigger_stop_loss(unrealized_usd: float | None, stop_loss_usd: float) -> bool:
    """Return True when unrealized loss is at or beyond configured USD cap."""
    if unrealized_usd is None:
        return False
    cap = abs(float(stop_loss_usd or 0.0))
    if cap <= 0:
        return False
    return float(unrealized_usd) <= -cap


def should_force_same_day_exit(entry_time_et: datetime | None, now_et: datetime) -> bool:
    """Return True when a position was opened prior to the current session day."""
    if entry_time_et is None:
        return False
    return entry_time_et.date() < now_et.date()


def premarket_scan_decision(
    now_et: datetime,
    *,
    signals_day: str,
    last_scan_at: datetime | None,
    scan_runs: int,
    max_runs: int,
    interval_seconds: int,
    window_start: str,
    window_end: str,
    entry_open_time: str,
) -> dict[str, object]:
    """
    Decide whether to run a premarket scan on this loop tick.

    Returns keys:
      - should_scan: bool
      - reset_day: bool
      - today_tag: str
      - effective_scan_runs: int
      - effective_last_scan_at: datetime | None
    """
    today_tag = now_et.date().isoformat()
    reset_day = str(signals_day or "") != today_tag
    effective_scan_runs = 0 if reset_day else int(scan_runs or 0)
    effective_last_scan_at = None if reset_day else last_scan_at

    premarket_window_open = _is_at_or_after(now_et, window_start) and not _is_at_or_after(now_et, window_end)
    before_entry_open = not _is_at_or_after(now_et, entry_open_time)
    effective_max_runs = max(0, int(max_runs or 0))
    run_budget_ok = (effective_max_runs == 0) or (effective_scan_runs < effective_max_runs)
    cadence_seconds = max(15, int(interval_seconds or 0))
    run_due = (
        effective_last_scan_at is None
        or int((now_et - effective_last_scan_at).total_seconds()) >= cadence_seconds
    )

    should_scan = bool(premarket_window_open and before_entry_open and run_budget_ok and run_due)
    return {
        "should_scan": should_scan,
        "reset_day": reset_day,
        "today_tag": today_tag,
        "effective_scan_runs": effective_scan_runs,
        "effective_last_scan_at": effective_last_scan_at,
    }
