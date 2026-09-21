import sqlite3

import pytest

from autobott_v2 import portfolio_budget as budget
from test_portfolio_budget import intent, reserve


def test_released_never_submitted_intent_can_retry_with_a_new_token(tmp_path):
    path = tmp_path / "premium.sqlite"
    ledger = budget.PremiumLedger(path)
    _, original = reserve(ledger, [intent(1, 200)])
    ledger.release_unattempted(original)
    restarted = budget.PremiumLedger(path)
    receipt, retry = reserve(restarted, [intent(1, 200)])
    assert receipt["uncertain_submission_dollars"] == 0
    assert receipt["remaining_dollars"] == 800
    assert retry[0]["client"] != original[0]["client"]
    assert retry[0]["key"] == original[0]["key"]
    with sqlite3.connect(path) as connection:
        archived = connection.execute("SELECT client,state FROM released_reservations").fetchall()
    assert archived == [(original[0]["client"], "not_submitted")]


def test_old_token_cannot_claim_or_release_the_retried_reservation(tmp_path):
    ledger = budget.PremiumLedger(tmp_path / "premium.sqlite")
    _, original = reserve(ledger, [intent(1, 1000)])
    ledger.release_unattempted(original)
    _, retry = reserve(ledger, [intent(1, 1000)])
    ledger.release_unattempted(original)
    with pytest.raises(budget.BudgetBlocked, match="portfolio_reservation_not_available"):
        ledger.mark_attempted(original[0])
    ledger.mark_attempted(retry[0])
    with pytest.raises(budget.BudgetBlocked, match="portfolio_premium_budget_exceeded"):
        reserve(ledger, [intent(2, 1)])


def test_attempted_order_stays_reserved_even_after_cleanup_and_retry(tmp_path):
    ledger = budget.PremiumLedger(tmp_path / "premium.sqlite")
    _, original = reserve(ledger, [intent(1, 1000)])
    ledger.mark_attempted(original[0])
    ledger.release_unattempted(original)
    with pytest.raises(budget.BudgetBlocked, match="portfolio_underlying_already_reserved"):
        reserve(ledger, [intent(1, 1000)])
