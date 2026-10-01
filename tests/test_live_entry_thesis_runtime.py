"""Real v2 cycle shell with synthetic observations and a fake broker only."""
import pytest
from test_entry_market_timing import EntryTape, run_cycle


@pytest.mark.parametrize("when", ["capture_and_refresh", "refresh_only"])
def test_v2_cannot_buy_call_after_price_breaches_completed_signal_bar(tmp_path, monkeypatch, when):
    original = EntryTape.get_latest_stock_quotes

    def reversed_price(self, symbols):
        rows = original(self, symbols)
        if when == "capture_and_refresh" or self.stock_calls > 1:
            for quote in rows.values():
                quote["bp"], quote["ap"] = 103.70, 103.80
        return rows

    monkeypatch.setattr(EntryTape, "get_latest_stock_quotes", reversed_price)
    result, broker, client = run_cycle(tmp_path, monkeypatch, pair=True, v2=True)
    assert result.scanner_candidates_count == 1
    assert broker.submitted == [], "A cheap option cannot rescue an invalidated bullish setup"
    assert result.trade_attempted_count == 0
    assert any(row["reason"] == "entry_signal_price_invalidated" for row in result.skipped)
    assert bool(client.refreshes) is (when == "refresh_only")


def test_v2_intact_entry_retains_primary_and_exit_metadata(tmp_path, monkeypatch):
    result, broker, client = run_cycle(tmp_path, monkeypatch, pair=True, v2=True)
    assert result.trade_attempted_count == 1
    assert len(broker.submitted) == 2
    primary = broker.submitted[0]
    assert primary.option_symbol == "AAPL260703C00105000"
    assert primary.take_profit_price == 3.75
    assert primary.stop_loss_price == 1.375
    admission = next(row for row in result.execution_outcomes if row["disposition"] == "entry_market_revalidated")
    assert admission["live_signal_thesis"]["at_capture"]["status"] == "not_invalidated"
    assert admission["live_signal_thesis"]["at_refresh"]["status"] == "not_invalidated"
