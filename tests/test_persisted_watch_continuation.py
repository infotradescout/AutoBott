"""Persisted-watch lifetime must not depend on new-registration settings."""
from datetime import timedelta
import json

import pytest

from autobott_v2 import trading_cycle as shell
from autobott_v2 import trading_cycle_v2 as adapter
from autobott_v2.bar_timing import aware_utc
from test_cycle_evidence_boundaries import linked_watch, WatchQuotes


@pytest.mark.parametrize("mode", ["legacy", "v2", "ranked"])
@pytest.mark.parametrize("setting", [None, "invalid", "0", "30"])
def test_saved_watch_keeps_bound_duration_and_polls_when_registration_changes(
    monkeypatch, tmp_path, mode, setting,
):
    root, path, case, broker, rules = linked_watch(monkeypatch, tmp_path)
    before = json.loads(path.read_text())
    now = aware_utc(before["window_end"]) + timedelta(seconds=20)
    broker.order["filled_at"] = now.isoformat()
    quotes = WatchQuotes(now, option_feed=case["refresh"]["options_feed"], bid=2.6)
    monkeypatch.delenv("AUTOBOTT_PRIMARY_DEVELOPMENT_CAPTURE", raising=False)
    if setting is None:
        monkeypatch.delenv("AUTOBOTT_PRIMARY_OBSERVATION_SECONDS", raising=False)
    else:
        monkeypatch.setenv("AUTOBOTT_PRIMARY_OBSERVATION_SECONDS", setting)
    monkeypatch.setattr(shell, "_entry_check_now", lambda: now)
    monkeypatch.setattr(adapter, "portfolio_mode_enabled", lambda: mode == "ranked")

    def forbidden_registration(*args, **kwargs):
        raise AssertionError("continuation must not create or re-register a watch")

    monkeypatch.setattr(shell, "register_primary_observation", forbidden_registration)
    run = shell.run_trading_cycle if mode == "legacy" else adapter.run_trading_cycle
    result = run(
        symbols=[], broker=broker, data_client=quotes,
        corpus_root=tmp_path / "corpus", execution_log_path=str(tmp_path / "execution.jsonl"),
    )
    after = json.loads(path.read_text())
    assert after["fill_capture_status"] == "filled", result.execution_outcomes
    assert aware_utc(after["fill_window_start"]) == now
    assert aware_utc(after["window_end"]) == now + timedelta(seconds=180)
    assert after["rules"] == before["rules"]
    assert after["quality_protocol"] == before["quality_protocol"]
    assert after["quality_protocol"]["rules_hash"] == rules.config_hash
    assert after["status"] == "observing"
    assert quotes.option_calls == [[after["primary_option_symbol"]]]
    assert any(aware_utc(point["timestamp"]) == now for point in after["case"]["outcome_snapshots"])
    assert list(root.glob("*.json")) == [path]
    assert not result.orders_submitted
    stages = {row["disposition"]: row for row in result.execution_outcomes}
    assert stages["primary_fill_capture_poll"]["filled"] == 1
    assert stages["primary_fill_capture_poll"]["errors"] == []
    assert stages["primary_fill_capture_poll"]["broker_writes"] == 0
    assert stages["primary_observation_poll"]["observed"] == 1
    assert stages["primary_observation_poll"]["errors"] == []
    assert stages["primary_observation_poll"]["broker_writes"] == 0
    if setting in {"invalid", "0"}:
        assert stages["primary_observation_config_invalid"]["error_type"] == "ValueError"
    else:
        assert "primary_observation_config_invalid" not in stages
