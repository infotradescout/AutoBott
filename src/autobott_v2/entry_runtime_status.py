"""Redacted, observational entry status. Never authorizes or submits a trade."""
from __future__ import annotations

import json
import math
from datetime import UTC, datetime
from typing import Any

SCHEMA = "entry_status.v1"
# Only fixed code-owned labels enter host logs. Broker errors, symbols, order IDs,
# account data, paths, URLs and arbitrary provider strings must not be copied.
REJECTION_CODES = frozenset({
    "daily_pnl_unavailable", "kill_switch_enabled", "execution_disabled",
    "single_leg_real_entries_disabled", "max_open_positions_reached",
    "max_new_entry_attempts_reached", "entry_admission_rejected",
})


def _count(value: Any) -> int | None:
    return value if type(value) is int and 0 <= value <= 1_000_000_000 else None


def _size(value: Any) -> int | None:
    return len(value) if isinstance(value, list) else None


def _timestamp(value: Any) -> str | None:
    if not isinstance(value, str) or len(value) > 48:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.astimezone(UTC).isoformat() if parsed.tzinfo else None
    except (ValueError, OverflowError):
        return None


def _finite(value: Any) -> bool:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        return False
    try:
        return math.isfinite(float(value))
    except (ValueError, OverflowError):
        return False


def build_entry_status(cycle: Any, *, observed_at: datetime | None = None) -> dict[str, Any]:
    """Describe the last completed cycle, not market-wide opportunity or fills."""
    observed = observed_at or datetime.now(UTC)
    if observed.tzinfo is None:
        raise ValueError("entry_status_requires_aware_observation")
    row = cycle if isinstance(cycle, dict) else {}
    outcomes = row.get("execution_outcomes")
    accounting_rows = [item for item in outcomes if isinstance(item, dict)
                       and item.get("disposition") == "trade_outcome_learning_summary"] if isinstance(outcomes, list) else []
    accounting = accounting_rows[0] if len(accounting_rows) == 1 else {}
    plan = accounting.get("reconciliation")
    plan = plan if isinstance(plan, dict) else {}
    raw_rejections = row.get("execution_rejected_count_by_reason")
    rejections: dict[str, int] = {}
    valid_rejections = isinstance(raw_rejections, dict)
    if valid_rejections:
        for code, value in raw_rejections.items():
            count = _count(value)
            if count is None:
                valid_rejections = False
                continue
            safe_code = code if code in REJECTION_CODES else "other"
            rejections[safe_code] = rejections.get(safe_code, 0) + count
    result: dict[str, Any] = {
        "schema": SCHEMA,
        "observed_at": observed.astimezone(UTC).isoformat(),
        "cycle_started_at": _timestamp(row.get("started_at")),
        "cycle_finished_at": _timestamp(row.get("finished_at")),
        "decisions_count": _size(row.get("decisions")),
        "candidates_count": _count(row.get("scanner_candidates_count")),
        "entry_attempts_count": _count(row.get("trade_attempted_count")),
        "entry_submissions_count": _size(row.get("orders_submitted")),
        "rejections_count": sum(rejections.values()) if valid_rejections else None,
        "rejection_counts": rejections,
        "accounting_available": len(accounting_rows) == 1 and accounting.get("ok") is True
                                and _finite(accounting.get("daily_realized_pnl")),
        "unresolved_count": _size(plan.get("unresolved")),
        "conflicts_count": _size(plan.get("conflicts")),
        "historical_duplicates_count": _size(plan.get("historical_duplicates")),
        "status": "UNKNOWN",
        "reason_code": "incomplete_cycle_evidence",
        "reason": "The last scan did not provide complete entry-status evidence.",
    }
    def state(status: str, code: str, reason: str) -> dict[str, Any]:
        result.update(status=status, reason_code=code, reason=reason)
        return result
    if row.get("error"):
        return state("ERROR", "cycle_error", "The last scan reported an error; its entry result is not confirmed.")
    reconciliation_block = plan.get("requires_reconciliation") is True or any(
        (result[key] or 0) > 0 for key in ("unresolved_count", "conflicts_count", "historical_duplicates_count")
    )
    if len(accounting_rows) == 1 and (reconciliation_block or accounting.get("error") == "outcome_journal_reconciliation_required"):
        return state("BLOCKED", "outcome_journal_reconciliation_required",
                     "New entries are blocked: AutoBott's trade records do not reconcile with the broker.")
    if rejections.get("daily_pnl_unavailable", 0) > 0 or (len(accounting_rows) == 1 and not result["accounting_available"]):
        return state("BLOCKED", "daily_pnl_unavailable",
                     "New entries are blocked: broker-derived daily trading results are unavailable.")
    if not result["cycle_finished_at"] or not valid_rejections or not result["accounting_available"]:
        return result
    candidates, submitted = result["candidates_count"], result["entry_submissions_count"]
    if candidates is None or submitted is None:
        return result
    if submitted > 0:
        return state("SUBMITTED", "entry_submission_recorded",
                     "The last scan recorded an entry submission. This is not confirmation of a fill or profit.")
    if candidates == 0:
        return state("NO_CANDIDATES", "no_scanner_candidates",
                     "No candidate passed the configured scanner rules in the last scan. This does not mean the market had no opportunities.")
    return state("NO_ENTRIES", "candidates_without_submission",
                 "The last scan found candidates, but no new entry submission was recorded.")


def emit_entry_status(status: dict[str, Any]) -> None:
    """Best-effort host diagnostic; failures must never interrupt monitoring."""
    try:
        print("AUTOBOTT_ENTRY_STATUS " + json.dumps(status, allow_nan=False, sort_keys=True), flush=True)
    except Exception:
        # Observability is not an execution gate or a reason to stop the runner.
        pass
