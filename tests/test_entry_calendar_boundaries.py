"""Synthetic regression: a recently closed bar is not permission after close."""
from datetime import UTC, datetime, timedelta
from test_entry_market_context import Provider, minute_rows, snapshot, assess


def test_completed_session_bar_does_not_authorize_after_session_close():
    close = datetime(2026, 7, 1, 20, 0, tzinfo=UTC)
    row = snapshot(Provider(rows=minute_rows(at=close)), at=close)
    result = assess(row, at=close + timedelta(seconds=30))
    assert result["status"] != "confirmed", result
