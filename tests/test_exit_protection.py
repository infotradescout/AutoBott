import pytest

from autobott_v2.exit_protection import exit_protection_summary


def observation(*actions, **kwargs):
    return {"enabled": True, "checked": 1, "ok": True, "actions": list(actions), **kwargs}


@pytest.mark.parametrize("outcome,expected,count", [
    ("pending", "awaiting_fill", "pending_count"),
    ("partially_filled", "awaiting_fill", "partially_filled_count"),
    ("broker_reported_filled", "awaiting_reconciliation", "reported_fill_count"),
    ("position_not_open", "monitoring", None),
    ("blocked", "attention_required", "blocked_count"),
    ("uncertain", "attention_required", "uncertain_count"),
])
def test_explicit_outcomes(outcome, expected, count):
    result = exit_protection_summary(observation(
        {"symbol": "SYNTHETIC", "reason": "stop_loss", "exit_status": outcome, "submitted": False}))
    assert result["status"] == expected
    if count:
        assert result[count] == 1


def test_pending_funding_and_entry_cleanup_do_not_invent_exit_failure():
    result = exit_protection_summary(observation(
        {"symbol": "SYNTHETIC", "reason": "primary_profit_funds_runner", "exit_status": "pending",
         "submitted": False, "funding_exit_blocked": True},
        {"symbol": "SYNTHETIC", "reason": "stale_linked_entry_canceled", "submitted": False}))
    assert result["status"] == "awaiting_fill"
    assert result["failed_count"] == result["blocked_count"] == 0
    assert len(result["issues"]) == 1


def test_partial_evidence_and_journal_error_are_conservative_and_redacted():
    action = {"symbol": "SYNTHETIC", "reason": "take_profit", "exit_status": "pending",
              "state": "partially_filled", "broker_order_id": "private-order"}
    assert exit_protection_summary(observation(action))["partially_filled_count"] == 1
    action["journal_error"] = "private-error-secret"
    result = exit_protection_summary(observation(action))
    assert result["status"] == "attention_required"
    assert "private" not in str(result)


def test_disabled_missing_and_nonexit_error():
    assert exit_protection_summary(None)["status"] == "unavailable"
    assert exit_protection_summary({})["status"] == "unavailable"
    assert exit_protection_summary({"enabled": True, "actions": []})["status"] == "unavailable"
    assert exit_protection_summary(observation(enabled=False))["status"] == "disabled"
    result = exit_protection_summary(observation(
        {"reason": "stale_linked_entry_cancel_failed", "error": "private-error"}, ok=False))
    assert result["status"] == "unavailable"
    assert result["failed_count"] == 0
