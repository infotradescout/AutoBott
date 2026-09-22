from datetime import timedelta
import json

import pytest

from autobott_v2.primary_followthrough import poll_primary_observations
from autobott_v2.primary_quality_runtime import evaluate_completed_primary_watches
from ranked_entry_fixtures import AT, run_combined
from test_primary_quality_runtime import quality_rules
from test_ranked_entry_allocation import native_card


@pytest.mark.parametrize("primary_bid,expected", [(1.10, "pass"), (.60, "fail")])
def test_ranked_budgeted_primary_is_evaluated_after_manual_exit_without_runner_masking(monkeypatch, tmp_path, primary_bid, expected):
    # This is a fixed synthetic protocol, never a production default or a
    # threshold tuned to an observed market trade.
    protocol = quality_rules()
    monkeypatch.setenv("AUTOBOTT_ENTRY_QUALITY_RULES_JSON", json.dumps(protocol.to_json_dict()))
    result, broker, ledger, transport, trace = run_combined(monkeypatch, tmp_path, native_card, observe=True)
    assert len(transport.posts) == 2, result.skipped
    root = tmp_path / "artifacts" / "primary_followthrough"
    watches = list(root.glob("*.json"))
    assert len(watches) == 1
    recorded = json.loads(watches[0].read_text())
    assert recorded["fill_capture_status"] == "filled"
    assert recorded["quality_protocol"]["rules_hash"] == protocol.config_hash
    primary = "STRONG261002C00100000"
    runner = "STRONG261002C00105000"
    assert recorded["case"]["fills"][0]["option_symbol"] == primary
    assert recorded["case"]["fills"][0]["price"] == .82
    # A manual exit must not become the observation's stopping condition.
    # The observer has no position-store dependency and must never query this.
    def forbidden_positions():
        raise AssertionError("post-exit observation depended on an open position")
    broker.list_open_positions = forbidden_positions
    calls = []
    class PathQuotes:
        option_feed = "indicative"
        stock_feed = "iex"
        def __init__(self, when):
            self.when = when
        def get_latest_option_quotes(self, symbols):
            calls.append(list(symbols))
            assert symbols == [primary]
            return {
                primary: {"bp": primary_bid, "ap": primary_bid+.03, "t": self.when.isoformat()},
                runner: {"bp": 9.0, "ap": 9.1, "t": self.when.isoformat()},
            }
        def get_latest_stock_quotes(self, symbols):
            return {symbol: {"bp": 101.0, "ap": 101.02, "t": self.when.isoformat()} for symbol in symbols}
    for offset in (60, 120, 180, 1800):
        when = AT + timedelta(seconds=2+offset)
        poll = poll_primary_observations(root, PathQuotes(when), now_fn=lambda: when)
        assert poll["observed"] == 1 and not poll["errors"], poll
    scored = evaluate_completed_primary_watches(root)
    assert scored["evaluated"] == 1, scored
    assert scored["quality_statuses"] == {expected: 1}, scored
    assert scored["broker_reads"] == scored["broker_writes"] == 0
    final = json.loads(watches[0].read_text())
    evaluation = final["entry_quality_evaluation"]
    assert evaluation["quality"]["evidence_kind"] == "broker_recorded_fill"
    assert evaluation["quality"]["status"] == expected
    assert evaluation["edge_established"] is False
    assert len(final["case"]["fills"]) == 1
    assert calls == [[primary]] * 4
    before = watches[0].read_bytes()
    repeated = evaluate_completed_primary_watches(root)
    assert repeated["evaluated"] == 0 and repeated["already_evaluated"] == 1
    assert watches[0].read_bytes() == before
    assert len(transport.posts) == 2
