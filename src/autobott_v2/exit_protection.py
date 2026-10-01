"""Pure operator visibility derived from an existing monitor observation."""
from __future__ import annotations

from typing import Any


MESSAGES = {
    "attention_required": "Exit protection needs attention. Review the exit monitor issues.",
    "awaiting_fill": "Exit orders are awaiting fills. Positions are not confirmed closed.",
    "awaiting_reconciliation": "The broker reported an exit fill. Position reconciliation is still pending.",
    "monitoring": "The exit monitor completed its latest observation without pending exit issues.",
    "disabled": "The exit monitor is disabled.",
    "unavailable": "The exit monitor observation is unavailable.",
}


def exit_protection_summary(observation: dict[str, Any] | None) -> dict[str, Any]:
    counts = dict.fromkeys(("failed_count", "uncertain_count", "blocked_count", "pending_count",
                           "partially_filled_count", "reported_fill_count"), 0)
    issues = []
    if not isinstance(observation, dict):
        status = "unavailable"
    elif observation.get("enabled") is False:
        status = "disabled"
    elif observation.get("enabled") is not True or not isinstance(observation.get("actions"), list):
        status = "unavailable"
    else:
        existing = observation.get("exit_observations")
        for action in observation["actions"] + (existing if isinstance(existing, list) else []):
            # Entry cleanup actions are not exit candidates.
            if not isinstance(action, dict) or "exit_status" not in action:
                continue
            outcome = str(action.get("exit_status") or "").lower()
            if outcome == "pending" and str(action.get("state") or "").lower() == "partially_filled":
                outcome = "partially_filled"
            if outcome in {"rejected", "failed", "canceled", "cancelled", "expired", "draft", "approved"}:
                kind, message = "failed", "The requested exit was not accepted."
            elif outcome == "blocked":
                kind, message = "blocked", "The requested exit is blocked. Review safety controls and monitor data."
            elif outcome == "uncertain":
                kind, message = "uncertain", "The exit outcome or order identity requires review."
            elif action.get("error") or action.get("journal_error"):
                kind, message = "uncertain", "The exit observation or durable record requires review."
            elif outcome in {"pending", "submitted", "accepted", "new", "pending_new", "pending_replace", "pending_cancel"}:
                kind, message = "pending", "The exit order is awaiting a fill."
            elif outcome == "partially_filled":
                kind, message = "partially_filled", "The exit order is partially filled. A remaining position may still be open."
            elif outcome in {"broker_reported_filled", "filled"}:
                kind, message = "reported_fill", "The broker reported a fill. Position absence is not yet confirmed."
            elif outcome == "position_not_open":
                continue
            else:
                kind, message = "uncertain", "The exit outcome requires review."
            counts[kind + "_count"] += 1
            issues.append({"symbol": action.get("symbol"), "reason": action.get("reason"),
                           "status": kind, "message": message})
        if any(counts[k] for k in ("failed_count", "uncertain_count", "blocked_count")):
            status = "attention_required"
        elif counts["pending_count"] or counts["partially_filled_count"]:
            status = "awaiting_fill"
        elif counts["reported_fill_count"]:
            status = "awaiting_reconciliation"
        elif observation.get("ok") is not True:
            # A non-exit cleanup error still makes the overall observation unhealthy.
            status = "unavailable"
        else:
            status = "monitoring"
    return {"status": status, "message": MESSAGES[status], **counts, "issues": issues}
