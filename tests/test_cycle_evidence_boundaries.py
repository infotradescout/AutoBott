"""Native cycle and stored-watch regressions with synthetic providers only."""
from datetime import UTC, datetime, timedelta
import json
from types import SimpleNamespace

import pytest

from autobott_v2 import trading_cycle as shell
from autobott_v2 import trading_cycle_v2 as adapter
from autobott_v2.bar_timing import aware_utc
from autobott_v2.primary_fill_capture import bind_primary_submission
from autobott_v2.primary_runtime_evidence import poll_primary_runtime_evidence_once
from test_primary_quality_runtime import prepared_watch
from test_trading_cycle import FakeBroker


def isolate(monkeypatch, tmp_path):
    monkeypatch.setenv("AUTOBOTT_DATA_ROOT", str(tmp_path / "state"))
    monkeypatch.setenv("AUTOBOTT_ARTIFACTS_ROOT", str(tmp_path / "artifacts"))
    monkeypatch.setenv("AUTOBOTT_GATE_PATH", str(tmp_path / "gate.json"))


@pytest.mark.parametrize("explicit_cutoff", [False, True])
@pytest.mark.parametrize("explicit_receipt", [False, True])
def test_native_shell_advances_live_cutoff_without_rewriting_explicit_times(
    monkeypatch, tmp_path, explicit_cutoff, explicit_receipt,
):
    isolate(monkeypatch, tmp_path)
    start = datetime(2026, 9, 22, 15, 0, tzinfo=UTC)
    clock = [start]
    calls = []

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return clock[0].astimezone(tz) if tz else clock[0].replace(tzinfo=None)

    def capture(**kwargs):
        calls.append((clock[0], kwargs))
        clock[0] += timedelta(seconds=93)
        raise ValueError("synthetic_provider_unavailable")

    monkeypatch.setattr(shell, "datetime", Clock)
    monkeypatch.setattr(shell, "capture_symbol_snapshot", capture)
    kwargs = {}
    if explicit_cutoff:
        kwargs["scheduled_market_time"] = start
    if explicit_receipt:
        kwargs["captured_at_utc"] = start
    result = shell.run_trading_cycle(
        symbols=["FIRST", "SECOND"], broker=FakeBroker(), data_client=SimpleNamespace(),
        corpus_root=tmp_path / "corpus", execution_log_path=str(tmp_path / "execution.jsonl"),
        **kwargs,
    )
    assert len(calls) == 2
    assert not result.orders_submitted
    assert calls[1][0] - calls[0][0] == timedelta(seconds=93)
    for actual_start, supplied in calls:
        assert supplied["scheduled_market_time"] == (start if explicit_cutoff else actual_start)
        assert supplied["captured_at_utc"] == (start if explicit_receipt else actual_start)


class WatchQuotes:
    """Only quote reads are supported; unexpected operations fail immediately."""
    stock_feed = "synthetic-sip"

    def __init__(self, when, *, option_feed, bid):
        self.when = when
        self.option_feed = option_feed
        self.bid = bid
        self.option_calls = []

    def get_latest_option_quotes(self, symbols):
        self.option_calls.append(list(symbols))
        return {symbol: {"bp": self.bid, "ap": self.bid + .04, "t": self.when.isoformat()}
                for symbol in symbols}

    def get_latest_stock_quotes(self, symbols):
        return {symbol: {"bp": 103.5, "ap": 103.52, "t": self.when.isoformat()}
                for symbol in symbols}


def linked_watch(monkeypatch, tmp_path):
    isolate(monkeypatch, tmp_path)
    original_root, watch_id, case, _, submission, broker, rules = prepared_watch(tmp_path / "prepared")
    root = tmp_path / "artifacts" / "primary_followthrough"
    root.parent.mkdir(parents=True, exist_ok=True)
    original_root.rename(root)
    bind_primary_submission(root, watch_id, submission, account_scope="alpaca:paper:synthetic-account")
    # Supply the complete cycle configuration while keeping all broker access
    # in the read-only test double. Its submit/cancel methods raise.
    broker.config = FakeBroker().config
    path = root / (watch_id + ".json")
    return root, path, case, broker, rules


@pytest.mark.parametrize("mode", ["legacy", "v2", "ranked"])
def test_late_fill_is_anchored_before_first_quote_in_the_same_native_cycle(
    monkeypatch, tmp_path, mode,
):
    root, path, case, broker, _ = linked_watch(monkeypatch, tmp_path)
    before = json.loads(path.read_text())
    now = aware_utc(before["window_end"]) + timedelta(seconds=20)
    broker.order["filled_at"] = now.isoformat()
    quotes = WatchQuotes(now, option_feed=case["refresh"]["options_feed"], bid=2.6)
    monkeypatch.setenv("AUTOBOTT_PRIMARY_OBSERVATION_SECONDS", "180")
    monkeypatch.setattr(shell, "_entry_check_now", lambda: now)
    monkeypatch.setattr(adapter, "portfolio_mode_enabled", lambda: mode == "ranked")
    run = shell.run_trading_cycle if mode == "legacy" else adapter.run_trading_cycle
    result = run(symbols=[], broker=broker, data_client=quotes, corpus_root=tmp_path / "corpus",
                 execution_log_path=str(tmp_path / "execution.jsonl"))
    row = json.loads(path.read_text())
    assert row["fill_capture_status"] == "filled", result.execution_outcomes
    assert aware_utc(row["fill_window_start"]) == now
    assert aware_utc(row["window_end"]) == now + timedelta(seconds=180)
    assert row["status"] == "observing"
    assert quotes.option_calls == [[row["primary_option_symbol"]]], result.execution_outcomes
    assert any(aware_utc(point["timestamp"]) == now for point in row["case"]["outcome_snapshots"])
    dispositions = [item["disposition"] for item in result.execution_outcomes]
    assert dispositions.index("primary_fill_capture_poll") < dispositions.index("primary_observation_poll")
    assert not result.orders_submitted


@pytest.mark.parametrize("bid,expected", [(3.3, "pass"), (2.6, "fail")])
def test_after_hours_continuation_scores_real_bound_watch_without_another_entry_cycle(
    monkeypatch, tmp_path, bid, expected,
):
    root, path, case, broker, rules = linked_watch(monkeypatch, tmp_path)
    fill_at = aware_utc(broker.order["filled_at"])
    # The evaluator must use the watch's pre-bound protocol, not this later
    # environment setting. No post-outcome threshold selection is permitted.
    monkeypatch.setenv("AUTOBOTT_ENTRY_QUALITY_RULES_JSON", "invalid-later-configuration")
    last = None
    for seconds in (0, 60, 120, 180):
        now = fill_at + timedelta(seconds=seconds)
        quotes = WatchQuotes(now, option_feed=case["refresh"]["options_feed"], bid=bid)
        last = poll_primary_runtime_evidence_once(root=root, broker=broker, data_client=quotes,
                                                 now_fn=lambda: now)
        assert last["fills"]["errors"] == []
        assert last["observations"]["errors"] == []
        assert last["entry_quality"]["errors"] == []
        assert last["trading_actions"] == 0
    assert last["entry_quality"]["evaluated"] == 1, last
    assert last["entry_quality"]["quality_statuses"] == {expected: 1}
    row = json.loads(path.read_text())
    assert row["entry_quality_evaluation"]["rules_hash"] == rules.config_hash
    assert row["entry_quality_evaluation"]["quality"]["status"] == expected
    assert row["entry_quality_evaluation"]["edge_established"] is False
    assert row["status"] == "window_closed"
    saved = path.read_bytes()
    repeated = poll_primary_runtime_evidence_once(root=root, broker=broker, data_client=quotes,
                                                 now_fn=lambda: now + timedelta(seconds=60))
    assert repeated["entry_quality"]["evaluated"] == 0
    assert repeated["entry_quality"]["already_evaluated"] == 1
    assert path.read_bytes() == saved
