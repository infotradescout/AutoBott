"""Executable counterexamples for refreshed quote admission; synthetic only."""
from dataclasses import replace

import pytest

from autobott_v2 import trading_cycle
from autobott_v2.phase1_validate import _contract_from_payload
from test_primary_fill_linkage import linked_case
import test_entry_market_timing as timing


@pytest.mark.parametrize("leg", ["primary", "runner"])
@pytest.mark.parametrize("v2", [False, True])
def test_one_collapsed_bid_blocks_both_legs_with_original_ask_unchanged(monkeypatch, tmp_path, leg, v2):
    base = timing.V2EntryTape if v2 else timing.EntryTape
    selected = []

    class CollapsedBidTape(base):
        def get_latest_option_quotes(self, symbols):
            quotes = super().get_latest_option_quotes(symbols)
            selected.extend(symbols)
            assert len(symbols) == 2
            for symbol, quote in quotes.items():
                primary = symbol.endswith("C00105000")
                if primary == (leg == "primary"):
                    before = dict(quote)
                    quote["bp"] *= .1
                    assert quote["ap"] == before["ap"]
                    assert (quote["ap"] - quote["bp"]) / ((quote["ap"] + quote["bp"]) / 2) > .25
            return quotes

    monkeypatch.setattr(timing, "V2EntryTape" if v2 else "EntryTape", CollapsedBidTape)
    result, broker, _ = timing.run_cycle(tmp_path, monkeypatch, pair=True, v2=v2)
    assert result.scanner_candidates_count == 1
    assert len(selected) == 2
    assert broker.submitted == []
    assert result.trade_attempted_count == 0
    expected = "entry_primary_contract_no_longer_eligible" if leg == "primary" else "entry_pair_no_longer_eligible"
    assert any(row["reason"] == expected for row in result.skipped), result.skipped


def test_refreshed_snapshot_derives_mid_and_spread_from_new_bid_ask(tmp_path):
    case = linked_case(tmp_path)
    original = _contract_from_payload(case["snapshot"]["option_chain"][0])
    changed = replace(original, bid=original.bid * .1, ask=original.ask)
    assert changed.mid == pytest.approx((changed.bid + changed.ask) / 2)
    assert changed.spread_pct == pytest.approx((changed.ask - changed.bid) / changed.mid)
    assert changed.spread_pct > original.spread_pct
    assert changed.ask == original.ask


@pytest.mark.parametrize("v2", [False, True])
def test_optional_null_loss_guard_does_not_abort_an_otherwise_valid_paper_pair(monkeypatch, tmp_path, v2):
    monkeypatch.setattr(trading_cycle, "sync_trade_outcomes_from_broker", lambda *args, **kwargs: {
        "ok": True, "daily_realized_pnl": 0.0, "blocked_underlyings": None,
    })
    result, broker, _ = timing.run_cycle(tmp_path, monkeypatch, pair=True, v2=v2)
    assert result.trade_attempted_count == 1, result.skipped
    assert len(broker.submitted) == 2
    assert broker.submitted[0].option_symbol == "AAPL260703C00105000"
