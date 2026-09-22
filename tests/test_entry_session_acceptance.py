"""Whole native cycles on synthetic time; the fixture broker refuses every order.

Run through scripts/validate_offline.py so sockets, credentials and subprocesses
remain guarded independently of these synthetic provider adapters.
"""
from copy import deepcopy
from datetime import datetime, timedelta
import json
from pathlib import Path
import time

import pytest

from autobott_v2 import trading_cycle as shell
from autobott_v2 import trading_cycle_v2
from autobott_v2.entry_schedule_context import EntryScheduleSource
from autobott_v2.entry_sector_context import SectorCatalog, SectorContextSource
from autobott_v2.phase1_alpaca_client import AlpacaPaperClient
from autobott_v2.phase1_snapshot_capture import CaptureRules
from autobott_v2.runtime_control import default_runtime_state, save_runtime_state
from autobott_v2.session_runner import run_trading_session
from test_entry_market_context import Provider, minute_rows
from test_entry_market_timing import START, V2EntryTape
from test_entry_schedule_context import calendar, fomc_calendar
from test_entry_sector_context import CSV, peer_rows
from test_phase1_alpaca_client import _config
from test_trading_cycle import FakeBroker


class SyntheticClock:
    def __init__(self):
        self.current = START
        self.sleeps = []

    def now(self):
        return self.current

    def monotonic(self):
        return (self.current - START).total_seconds()

    def advance(self, seconds):
        self.current += timedelta(seconds=seconds)

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.advance(seconds)


class RefusingSyntheticBroker(FakeBroker):
    """No transport exists: reached intents are counted and then refused."""
    def __init__(self):
        super().__init__()
        self.refused_intents = []

    def submit_order(self, intent, **kwargs):
        self.refused_intents.append(intent)
        raise RuntimeError("synthetic_fixture_order_refused")


class NativeContextTape(V2EntryTape):
    requires_entry_context = True
    get_entry_context = AlpacaPaperClient.get_entry_context

    def __init__(self, clock, *, fail_refresh=False, fail_initial=False, latency=2):
        super().__init__()
        self.clock, self.cutoff = clock, START
        self.config = _config()
        self.calls = []
        self.fed_calls = 0
        self.fail_refresh = fail_refresh
        self.fail_initial = fail_initial
        self.latency = latency
        self._entry_schedule_source = EntryScheduleSource(
            self.exchange, public_fetch=self.bls, fomc_fetch=self.fed,
            now_fn=clock.now)
        catalog = SectorCatalog(lambda: CSV, now_fn=clock.now)
        self._entry_sector_source = SectorContextSource(
            lambda path, params: self._get_json_with_retry(
                self.config.data_base_url, path, params), catalog=catalog)

    def exchange(self, params):
        self.calls.append("exchange")
        assert params == {"start": "2026-07-01", "end": "2026-07-01"}
        self.clock.advance(self.latency)
        return [{"date": "2026-07-01", "open": "09:30", "close": "16:00"}]

    def bls(self):
        self.calls.append("bls")
        self.clock.advance(self.latency)
        return calendar()

    def fed(self, day):
        self.calls.append("fomc")
        self.fed_calls += 1
        self.clock.advance(self.latency)
        if self.fail_initial or (self.fail_refresh and self.fed_calls >= 2):
            raise TimeoutError("synthetic_fed_timeout")
        return fomc_calendar(day)

    def _get_json_with_retry(self, base, path, params):
        assert base == "https://data.alpaca.markets"
        assert path in {"/v2/stocks/bars", "/v1beta1/news"}
        self.calls.append((path, params.get("symbols")))
        if path == "/v2/stocks/bars" and params["symbols"] == "XLK":
            return {"bars": {"XLK": peer_rows(at=self.cutoff)}, "next_page_token": None}
        return Provider(rows=minute_rows(at=self.cutoff))(path, params)

    def get_stock_bars(self, symbols, **kwargs):
        rows = super().get_stock_bars(symbols, **kwargs)
        for values in rows.values():
            for row in values:
                when = datetime.fromisoformat(row["t"].replace("Z", "+00:00"))
                row["t"] = (when + (self.cutoff - START)).isoformat()
        return rows

    def get_latest_stock_quotes(self, symbols):
        rows = super().get_latest_stock_quotes(symbols)
        for quote in rows.values():
            quote["t"] = self.clock.now().isoformat()
        return rows

    def get_option_chain_snapshots(self, symbol):
        rows = super().get_option_chain_snapshots(symbol)
        for row in rows.values():
            row["latestQuote"]["t"] = self.clock.now().isoformat()
        return rows


@pytest.mark.parametrize("mode", ["warm", "refresh_failure", "cold_failure", "slow", "stale"])
def test_native_session_cache_expiry_and_unavailable_refresh(tmp_path, monkeypatch, mode):
    clock = SyntheticClock()
    tape = NativeContextTape(clock, fail_refresh=mode == "refresh_failure",
        fail_initial=mode == "cold_failure", latency=31 if mode == "stale" else 11 if mode == "slow" else 2)
    cycles = 18 if mode in {"warm", "refresh_failure"} else 1
    broker = RefusingSyntheticBroker()
    monkeypatch.setenv("AUTOBOTT_DATA_ROOT", str(tmp_path / "state"))
    monkeypatch.setenv("AUTOBOTT_ARTIFACTS_ROOT", str(tmp_path / "artifacts"))
    monkeypatch.setenv("AUTOBOTT_CORE_RUNNER_ENABLED", "false")
    save_runtime_state(default_runtime_state())
    monkeypatch.setattr(shell, "load_open_positions", lambda: [])
    monkeypatch.setattr(shell, "_entry_check_now", clock.now)
    capture = shell.capture_symbol_snapshot
    monkeypatch.setattr(shell, "capture_symbol_snapshot", lambda **kw:
                        capture(**kw, monotonic_fn=clock.monotonic))
    receipts = []

    def cycle_runner(*, symbols):
        tape.cutoff = clock.now()
        started = clock.now()
        wall_start = time.perf_counter()
        result = trading_cycle_v2.run_trading_cycle(
            symbols=symbols, broker=broker, data_client=tape,
            scheduled_market_time=started, captured_at_utc=started,
            corpus_root=tmp_path / "corpus",
            decision_log_path=tmp_path / "decisions.jsonl",
            execution_log_path=str(tmp_path / "executions.jsonl"), rules=CaptureRules())
        snapshots = [json.loads(Path(path).read_text(encoding="utf-8")) for path in result.snapshot_paths]
        receipts.append({"start": started.isoformat(), "finish": clock.now().isoformat(),
                         "wall_seconds": time.perf_counter() - wall_start,
                         "schedules": [row["entry_context"]["schedule"] for row in snapshots],
                         "admissions": [row for row in result.execution_outcomes
                                        if row["disposition"] == "entry_market_revalidated"],
                         "skipped": deepcopy(result.skipped)})
        assert not any(row["reason"] == "snapshot_or_decision_failed" for row in result.skipped), result.skipped
        return result

    session = run_trading_session(symbols=["AAPL"], interval_seconds=60,
        max_cycles=cycles, now_fn=clock.now, sleep_fn=clock.sleep, cycle_runner=cycle_runner)
    assert session.cycles_completed == cycles
    (tmp_path / "session-diagnostic.json").write_text(json.dumps(receipts, indent=2), encoding="utf-8")
    assert not any("error" in result for result in session.cycle_results), [result["error"] for result in session.cycle_results if "error" in result]
    assert len(receipts) == cycles and clock.sleeps == [60] * (cycles - 1)
    assert all(len(row["schedules"]) == 1 for row in receipts)
    assert broker.submitted == [], "Every synthetic broker submission must stay refused"
    if cycles == 18:
        assert receipts[0]["schedules"][0]["status"] == "observed"
        assert receipts[0]["admissions"], receipts[0]
        # Warm cycles reuse the original receipt without relabeling its freshness.
        first_known = receipts[0]["schedules"][0]["received_at"]
        assert receipts[1]["schedules"][0]["received_at"] == first_known
        assert receipts[15]["schedules"][0]["received_at"] == first_known
        if mode == "refresh_failure":
            assert tape.fed_calls == 3
            assert receipts[16]["schedules"][0]["status"] == "unavailable"
            assert not receipts[16]["admissions"] and not receipts[17]["admissions"]
            assert receipts[16]["schedules"][0]["reason"] == "TimeoutError"
        else:
            assert tape.fed_calls == 2
            assert receipts[16]["schedules"][0]["status"] == "observed"
            assert receipts[16]["schedules"][0]["received_at"] != first_known
        # Existing cooldown/volatility checks stay active in the repeated cycle.
        assert any(row["reason"] == "setup_event_already_traded" for row in receipts[1]["skipped"])
        assert receipts[1]["start"] == (START + timedelta(seconds=66)).isoformat()
    elif mode == "slow":
        assert tape.fed_calls == 1 and len(broker.refused_intents) == 1
        assert receipts[0]["admissions"], receipts[0]
        assert clock.now() == START + timedelta(seconds=33)
        assert receipts[0]["schedules"][0]["status"] == "observed"
        admission = receipts[0]["admissions"][0]
        assert admission["completed_bars"]["completed_bar_ages_seconds"]["underlying"] == 33
        assert admission["completed_bars"]["max_completed_bar_age_seconds"] == 90
        assert admission["signal_quote_age_seconds"] == 0
        assert all(quote["quote_age_seconds"] == 0 for quote in admission["quotes"])
    else:
        assert tape.fed_calls == 1 and not broker.refused_intents
        assert not receipts[0]["admissions"], receipts[0]
        assert receipts[0]["skipped"], receipts[0]
        if mode == "cold_failure":
            assert receipts[0]["schedules"][0]["status"] == "unavailable"
            assert receipts[0]["schedules"][0]["reason"] == "TimeoutError"
            assert any(row["reason"] == "entry_context_not_confirmed" for row in receipts[0]["skipped"])
        else:
            assert receipts[0]["schedules"][0]["status"] == "observed"
            assert clock.now() == START + timedelta(seconds=93)
            assert receipts[0]["skipped"] == [{"symbol": "AAPL", "reason": "entry_stale_completed_bar",
                "detail": "underlying", "reasons": ["entry_stale_completed_bar"]}]
            assert tape.refreshes == [], "Stale completed bars must reject before quote refresh or fixture broker dispatch"
    # Current runner waits after completion. Do not claim a hard 60-second start cadence.
    report = {"synthetic": True, "cycles": cycles, "interval_after_completion_seconds": 60,
              "real_orders": 0, "refused_synthetic_intents": len(broker.refused_intents),
              "fed_reads": tape.fed_calls, "provider_calls": tape.calls, "cycles_observed": receipts}
    (tmp_path / "session-acceptance.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
