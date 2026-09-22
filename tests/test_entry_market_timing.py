"""Synthetic timing regressions; no real accounts, orders, or performance claims."""
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime, timedelta
import json
from pathlib import Path

import pytest

import autobott_v2.trading_cycle as cycle
from autobott_v2.phase1_snapshot_capture import CaptureRules, capture_symbol_snapshot
from autobott_v2.phase1_alpaca_client import AlpacaPaperClient
from autobott_v2.runtime_control import default_runtime_state, save_runtime_state
from test_phase1_alpaca_client import _config
from test_phase1_snapshot_capture import FakeCaptureClient
from test_trading_cycle import FakeBroker, CoreRunnerDataClient

START = datetime(2026, 7, 1, 15, 35, tzinfo=UTC)


def test_latest_stock_quote_request_does_not_reuse_prior_scan_value(monkeypatch):
    client = AlpacaPaperClient(_config())
    replies = iter([{"quotes": {"SPY": {"bp": 100, "ap": 101, "t": START.isoformat()}}},
                    {"quotes": {"SPY": {"bp": 102, "ap": 103, "t": (START + timedelta(seconds=2)).isoformat()}}}])
    calls = []
    def fetch(*args):
        calls.append(args)
        return next(replies)
    monkeypatch.setattr(client, "_get_json_with_retry", fetch)
    assert client.get_latest_stock_quotes(["SPY"])["SPY"]["bp"] == 100
    assert client.get_latest_stock_quotes(["SPY"])["SPY"]["bp"] == 102
    assert len(calls) == 2


def test_missing_new_quote_never_falls_back_to_old_cached_quote(monkeypatch):
    client = AlpacaPaperClient(_config())
    replies = iter([{"quotes": {"SPY": {"bp": 100, "ap": 101}}}, {"quotes": {}}])
    monkeypatch.setattr(client, "_get_json_with_retry", lambda *args: next(replies))
    client.get_latest_stock_quotes(["SPY"])
    assert client.get_latest_stock_quotes(["SPY"]) == {}


@pytest.mark.parametrize("timeframe, seconds", [("1Min", 60), ("1Hour", 3600)])
def test_capture_uses_only_completed_bars_for_underlying_and_context(tmp_path, timeframe, seconds):
    class Bars(FakeCaptureClient):
        def get_stock_bars(self, symbols, *, start, end, timeframe="1Min", limit=35):
            raw = super().get_stock_bars(symbols, start=start, end=end, limit=limit)
            for rows in raw.values():
                for i, row in enumerate(rows):
                    row["t"] = (end - timedelta(seconds=(len(rows) - 1 - i) * seconds)).isoformat()
                # Last bar is still forming at the requested observation time.
                rows[-1].update(o=800, h=900, l=800, c=900)
            return raw
    at = datetime(2026, 6, 30, 13, 30, tzinfo=UTC)
    file = capture_symbol_snapshot(symbol="SPY", corpus_root=tmp_path,
        scheduled_market_time=at, captured_at_utc=at, corpus_type="test_fixture",
        market_timezone="America/New_York", volatility_proxy_symbol="VIXY",
        data_client=Bars(), rules=CaptureRules(bar_timeframe=timeframe))
    row = json.loads(Path(file).read_text())
    for bars in [row["market_bars"], *[row["context"][k] for k in ("spy_bars", "qqq_bars", "vix_bars")]]:
        assert len(bars) >= 30
        assert all(datetime.fromisoformat(b["timestamp"].replace("Z", "+00:00")) + timedelta(seconds=seconds) <= at for b in bars)
        assert all(b["close"] != 900 for b in bars)


class EntryTape(CoreRunnerDataClient):
    option_feed = "indicative"
    def __init__(self, defect=None):
        self.defect = defect
        self.refreshes = []
        self.stock_calls = 0

    def get_latest_stock_quotes(self, symbols):
        self.stock_calls += 1
        rows = super().get_latest_stock_quotes(symbols)
        if self.stock_calls > 1 and self.defect == "stale_stock":
            for q in rows.values(): q["t"] = (START - timedelta(minutes=10)).isoformat()
        return rows

    def get_latest_option_quotes(self, symbols):
        self.refreshes.append(list(symbols))
        chain = self.get_option_chain_snapshots("AAPL")
        quotes = {symbol: deepcopy(chain[symbol]["latestQuote"]) for symbol in symbols}
        for symbol, quote in quotes.items():
            if self.defect == "stale": quote["t"] = (START - timedelta(minutes=10)).isoformat()
            elif self.defect == "future": quote["t"] = (START + timedelta(minutes=10)).isoformat()
            elif self.defect == "timestamp_missing": quote.pop("t")
            elif self.defect == "crossed": quote["bp"] = quote["ap"] + .1
            elif self.defect == "chased": quote["bp"] += 1; quote["ap"] += 1
            elif self.defect == "wide": quote["bp"] *= .5
            elif self.defect == "regressed": quote["t"] = (START - timedelta(seconds=1)).isoformat()
            elif self.defect == "zero_bid": quote["bp"] = 0
        if self.defect == "missing": return {}
        if self.defect == "missing_runner": return {s:q for s,q in quotes.items() if s.endswith("C00105000")}
        return quotes


def run_cycle(tmp_path, monkeypatch, *, defect=None, pair=False, v2=False, now=None, clock=None):
    monkeypatch.setenv("AUTOBOTT_DATA_ROOT", str(tmp_path / "state"))
    monkeypatch.setenv("AUTOBOTT_CORE_RUNNER_ENABLED", "true" if pair else "false")
    save_runtime_state(default_runtime_state())
    monkeypatch.setattr(cycle, "load_open_positions", lambda: [])
    monkeypatch.setattr(cycle, "_entry_check_now", clock or (lambda: now or START + timedelta(seconds=10)), raising=False)
    broker, client = FakeBroker(), (V2EntryTape(defect) if v2 else EntryTape(defect))
    run = cycle.run_trading_cycle
    if v2:
        from autobott_v2.trading_cycle_v2 import run_trading_cycle
        run = run_trading_cycle
    result = run(symbols=["AAPL"], broker=broker, data_client=client,
        scheduled_market_time=START, captured_at_utc=START,
        corpus_root=tmp_path / "corpus", decision_log_path=tmp_path / "decisions.jsonl",
        execution_log_path=str(tmp_path / "execution.jsonl"), rules=CaptureRules())
    return result, broker, client


@pytest.mark.parametrize("defect", ["stale", "future", "timestamp_missing", "missing", "crossed", "chased", "wide", "stale_stock"])
def test_bad_refreshed_entry_quotes_cannot_reach_broker(tmp_path, monkeypatch, defect):
    result, broker, client = run_cycle(tmp_path, monkeypatch, defect=defect)
    assert result.scanner_candidates_count == 1
    assert broker.submitted == []
    assert result.trade_attempted_count == 0
    assert client.refreshes
    assert any(row["reason"].startswith("entry_") for row in result.skipped)


def test_good_entry_still_submits_exact_primary(tmp_path, monkeypatch):
    result, broker, client = run_cycle(tmp_path, monkeypatch)
    assert result.trade_attempted_count == 1
    assert [intent.option_symbol for intent in broker.submitted] == ["AAPL260703C00105000"]
    assert client.refreshes == [["AAPL260703C00105000"]]


def test_missing_runner_quote_prevents_both_entries(tmp_path, monkeypatch):
    result, broker, client = run_cycle(tmp_path, monkeypatch, defect="missing_runner", pair=True)
    assert result.scanner_candidates_count == 1
    assert broker.submitted == []
    assert len(client.refreshes[0]) == 2


def test_expired_decision_cannot_submit_even_with_current_quote_response(tmp_path, monkeypatch):
    result, broker, _ = run_cycle(tmp_path, monkeypatch, now=START + timedelta(minutes=10))
    assert result.scanner_candidates_count == 1
    assert broker.submitted == []
    assert result.trade_attempted_count == 0


class V2EntryTape(EntryTape):
    def get_stock_bars(self, symbols, **kwargs):
        rows = super().get_stock_bars(symbols, **kwargs)
        for values in rows.values():
            for row in values:
                # Synthetic directional tape with consistent per-bar range.
                row.update(o=row["c"]-.03, h=row["c"]+.08, l=row["c"]-.08)
        return rows


@pytest.mark.parametrize("defect", ["stale", "missing_runner", "chased", "regressed", "zero_bid"])
def test_v2_shell_rejects_changed_pair_before_any_broker_entry(tmp_path, monkeypatch, defect):
    result, broker, client = run_cycle(tmp_path, monkeypatch, defect=defect, pair=True, v2=True)
    assert result.scanner_candidates_count == 1
    assert broker.submitted == []
    assert result.trade_attempted_count == 0
    assert len(client.refreshes[0]) == 2


def test_v2_valid_pair_retains_primary_and_exit_metadata(tmp_path, monkeypatch):
    result, broker, client = run_cycle(tmp_path, monkeypatch, pair=True, v2=True)
    assert result.trade_attempted_count == 1
    assert len(broker.submitted) == 2
    assert broker.submitted[0].option_symbol == "AAPL260703C00105000"
    assert broker.submitted[0].take_profit_price == 3.75
    assert broker.submitted[0].stop_loss_price == 1.375
    evidence = next(r for r in result.execution_outcomes if r["disposition"] == "entry_market_revalidated")
    assert evidence["options_feed"] == "indicative"
    assert evidence["executable_fill_verified"] is False


def test_rejected_quote_does_not_consume_setup_cooldown(tmp_path, monkeypatch):
    bad, broker, _ = run_cycle(tmp_path, monkeypatch, defect="stale")
    assert broker.submitted == []
    good, broker, _ = run_cycle(tmp_path, monkeypatch)
    assert good.trade_attempted_count == 1
    assert len(broker.submitted) == 1


@pytest.mark.parametrize("seconds, allowed", [(30, True), (30.000001, False)])
def test_source_quote_age_boundary_is_exact(tmp_path, monkeypatch, seconds, allowed):
    result, broker, _ = run_cycle(tmp_path, monkeypatch, now=START + timedelta(seconds=seconds))
    assert bool(broker.submitted) is allowed


def test_time_spent_refreshing_counts_against_entry_deadline(tmp_path, monkeypatch):
    times = iter([START + timedelta(seconds=10), START + timedelta(seconds=40)])
    result, broker, client = run_cycle(tmp_path, monkeypatch, clock=lambda: next(times))
    assert result.scanner_candidates_count == 1
    assert client.refreshes
    assert broker.submitted == []
    assert result.trade_attempted_count == 0


def test_backward_clock_during_refresh_cannot_pass(tmp_path, monkeypatch):
    times = iter([START + timedelta(seconds=10), START + timedelta(seconds=9)])
    result, broker, _ = run_cycle(tmp_path, monkeypatch, clock=lambda: next(times))
    assert broker.submitted == []
    assert any(r["reason"] == "entry_clock_regressed" for r in result.skipped)


def test_candidate_filter_excludes_old_quotes_without_rewriting_source():
    from autobott_v2.entry_admission import filter_entry_quote_candidates
    from autobott_v2.phase1_validate import _decision_input_from_snapshot
    from test_phase1_campaign_runner import _snapshot, BASE_TIME
    row = _snapshot(BASE_TIME)
    row["option_chain"][0]["quote_timestamp"] = (BASE_TIME - timedelta(seconds=31)).isoformat()
    before = deepcopy(row)
    original = _decision_input_from_snapshot(row)
    filtered, proof = filter_entry_quote_candidates(original, row)
    assert len(original.option_chain) == 2
    assert len(filtered.option_chain) == 1
    assert filtered.option_chain[0].option_symbol == row["option_chain"][1]["option_symbol"]
    assert proof["rejected"][0]["reason"] == "entry_stale_market_evidence"
    assert row == before


def test_candidate_filter_does_not_select_ambiguous_duplicate_symbol():
    from autobott_v2.entry_admission import filter_entry_quote_candidates
    from autobott_v2.phase1_validate import _decision_input_from_snapshot
    from test_phase1_campaign_runner import _snapshot, BASE_TIME
    row = _snapshot(BASE_TIME)
    row["option_chain"].append(deepcopy(row["option_chain"][0]))
    filtered, proof = filter_entry_quote_candidates(_decision_input_from_snapshot(row), row)
    assert len(filtered.option_chain) == 1
    assert len(proof["rejected"]) == 2


def test_schedule_gaps_are_not_created_by_different_receipt_delays(tmp_path):
    from autobott_v2.phase1_snapshot_capture import write_snapshot_day_manifest
    at = datetime(2026, 6, 30, 13, 30, tzinfo=UTC)
    for minute, delay in [(0, .1), (2, 2.0)]:
        times = iter([100, 100 + delay])
        capture_symbol_snapshot(symbol="SPY", corpus_root=tmp_path,
            scheduled_market_time=at + timedelta(minutes=minute), captured_at_utc=at + timedelta(minutes=minute),
            corpus_type="test_fixture", market_timezone="America/New_York", volatility_proxy_symbol="VIXY",
            data_client=FakeCaptureClient(), rules=CaptureRules(), monotonic_fn=lambda: next(times))
    manifest = write_snapshot_day_manifest(tmp_path / "2026-06-30" / "SPY", capture_interval_seconds=60)
    assert manifest["missing_intervals"] == ["09:31:00"]
