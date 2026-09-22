"""Continuation orchestration only; no live accounts, prices, or order APIs."""
from datetime import UTC, datetime
import json

import pytest

from autobott_v2 import primary_runtime_evidence as runtime


NOW = datetime(2026, 9, 22, 19, 59, tzinfo=UTC)


class NoTradingClient:
    def __getattr__(self, name):
        raise AssertionError(f"continuation must not invoke a trading API: {name}")


def empty_quality():
    return {
        "checked": 0, "evaluated": 0, "already_evaluated": 0,
        "not_configured": 0, "not_ready": 0, "errors": [],
        "quality_statuses": {}, "underlying_diagnostics": {},
        "broker_reads": 0, "broker_writes": 0, "journal_writes": 0,
        "edge_established": False,
    }


def arrange(monkeypatch, tmp_path, *, fail=None, quality=None):
    root = tmp_path / "watches"
    root.mkdir()
    (root / "watch.json").write_text(json.dumps({"status": "active"}))
    broker, data = NoTradingClient(), NoTradingClient()
    clock = lambda: NOW
    calls = []
    quality = empty_quality() if quality is None else quality
    summaries = {
        "fills": {"checked": 1, "filled": 1, "errors": []},
        "observations": {"checked": 1, "window_closed": 1, "errors": []},
        "entry_quality": quality,
        "development_metrics": {"checked": 1, "not_development": 1, "errors": []},
    }

    def stage(name, client=None):
        def invoke(received_root, *args, **kwargs):
            assert received_root == root
            if client is None:
                # Quality scoring receives the stored watch, not fresh rules
                # from the current environment or a new observation window.
                assert args == () and kwargs == {}
            else:
                assert args == (client,) and kwargs == {"now_fn": clock}
            calls.append(name)
            if name == fail:
                raise RuntimeError(f"synthetic_{name}_failure")
            return summaries[name]
        return invoke

    monkeypatch.setattr(runtime, "poll_primary_fills", stage("fills", broker))
    monkeypatch.setattr(runtime, "poll_primary_observations", stage("observations", data))
    monkeypatch.setattr(runtime, "evaluate_completed_primary_watches", stage("entry_quality"), raising=False)
    monkeypatch.setattr(runtime, "materialize_primary_development_metrics", stage("development_metrics"))
    return root, broker, data, clock, calls, summaries


@pytest.mark.parametrize("root_state", ["missing", "empty", "no_watch_files"])
def test_empty_store_reports_no_quality_without_touching_dependencies(monkeypatch, tmp_path, root_state):
    root = tmp_path / "watches"
    if root_state != "missing":
        root.mkdir()
    if root_state == "no_watch_files":
        (root / ".primary-observation.lock").write_text("")

    def forbidden(*args, **kwargs):
        raise AssertionError("empty store must not construct clients or evaluate")

    for name in ("AlpacaExecutionBroker", "AlpacaPaperClient", "poll_primary_fills",
                 "poll_primary_observations", "evaluate_completed_primary_watches",
                 "materialize_primary_development_metrics"):
        monkeypatch.setattr(runtime, name, forbidden, raising=False)
    result = runtime.poll_primary_runtime_evidence_once(root=root)
    assert result["entry_quality"] == empty_quality()
    assert result["trading_actions"] == 0
    assert result["fills"]["checked"] == result["observations"]["checked"] == 0


def test_scoring_follows_fills_and_final_quotes_without_reloading_protocol(monkeypatch, tmp_path):
    root, broker, data, clock, calls, summaries = arrange(monkeypatch, tmp_path)
    monkeypatch.setenv("AUTOBOTT_ENTRY_QUALITY_RULES_JSON", "must-not-reload-current-environment")
    result = runtime.poll_primary_runtime_evidence_once(
        root=root, broker=broker, data_client=data, now_fn=clock)
    assert calls == ["fills", "observations", "entry_quality", "development_metrics"]
    for name, summary in summaries.items():
        assert result[name] is summary
    assert result["trading_actions"] == 0


def test_watch_closed_by_this_poll_is_scored_in_this_same_call(monkeypatch, tmp_path):
    root, broker, data, clock, calls, _ = arrange(monkeypatch, tmp_path)
    path = root / "watch.json"

    def observe(received_root, received_data, *, now_fn):
        assert received_root == root and received_data is data and now_fn is clock
        calls.append("observations")
        path.write_text(json.dumps({"status": "window_closed", "final_quote_recorded": True}))
        return {"window_closed": 1, "errors": []}

    def evaluate(received_root):
        assert received_root == root
        calls.append("entry_quality")
        row = json.loads(path.read_text())
        assert row["status"] == "window_closed" and row["final_quote_recorded"]
        return {**empty_quality(), "checked": 1, "evaluated": 1}

    monkeypatch.setattr(runtime, "poll_primary_observations", observe)
    monkeypatch.setattr(runtime, "evaluate_completed_primary_watches", evaluate, raising=False)
    result = runtime.poll_primary_runtime_evidence_once(
        root=root, broker=broker, data_client=data, now_fn=clock)
    assert result["entry_quality"]["evaluated"] == 1
    assert calls == ["fills", "observations", "entry_quality", "development_metrics"]


@pytest.mark.parametrize("failed", ["fills", "observations", "entry_quality", "development_metrics"])
def test_failure_in_one_stage_does_not_suppress_other_evidence_work(monkeypatch, tmp_path, failed):
    root, broker, data, clock, calls, summaries = arrange(monkeypatch, tmp_path, fail=failed)
    result = runtime.poll_primary_runtime_evidence_once(
        root=root, broker=broker, data_client=data, now_fn=clock)
    assert calls == ["fills", "observations", "entry_quality", "development_metrics"]
    assert result[failed]["errors"] == [{"reason": f"RuntimeError:synthetic_{failed}_failure"}]
    assert result[failed]["broker_writes"] == 0
    for name, summary in summaries.items():
        if name != failed:
            assert result[name] is summary
    assert result["trading_actions"] == 0
    if failed == "entry_quality":
        assert result[failed]["evaluated"] == 0
        assert result[failed]["quality_statuses"] == {}
        assert result[failed]["edge_established"] is False


@pytest.mark.parametrize("status", ["pass", "fail", "unscorable"])
def test_evaluator_results_are_exposed_without_reclassifying_missing_or_failed_evidence(monkeypatch, tmp_path, status):
    quality = {**empty_quality(), "checked": 1, "evaluated": 1, "quality_statuses": {status: 1}}
    root, broker, data, clock, _, _ = arrange(monkeypatch, tmp_path, quality=quality)
    result = runtime.poll_primary_runtime_evidence_once(
        root=root, broker=broker, data_client=data, now_fn=clock)
    assert result["entry_quality"] is quality
    assert result["entry_quality"]["quality_statuses"] == {status: 1}
    assert result["entry_quality"]["edge_established"] is False


def test_empty_quality_summaries_do_not_share_mutable_state(tmp_path):
    first = runtime.poll_primary_runtime_evidence_once(root=tmp_path / "missing")
    first["entry_quality"]["errors"].append({"reason": "synthetic"})
    first["entry_quality"]["quality_statuses"]["unscorable"] = 1
    second = runtime.poll_primary_runtime_evidence_once(root=tmp_path / "missing")
    assert second["entry_quality"] == empty_quality()
