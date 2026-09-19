"""Persistent capture integration. All prices, clocks and broker doubles synthetic."""
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
import json
from pathlib import Path
import pytest

from autobott_v2.primary_followthrough import (
    PrimaryObservationRules, configured_observation_rules, register_primary_observation,
    poll_primary_observations, export_primary_observations,
)
from test_primary_entry_study import make_case, assess, protocol, PRIMARY
from test_entry_market_timing import START, run_cycle


class Quotes:
    option_feed = "indicative"
    stock_feed = "synthetic-sip"
    def __init__(self, at, *, bid=3.2, missing=False, stock_mid=103.5, stock_missing=False):
        self.at, self.bid, self.missing = at, bid, missing
        self.stock_mid, self.stock_missing = stock_mid, stock_missing
        self.calls = []
        self.stock_calls = []
    def get_latest_option_quotes(self, symbols):
        self.calls.append(list(symbols))
        return {} if self.missing else {s:{"bp":self.bid,"ap":self.bid+.05,"t":self.at.isoformat()} for s in symbols}
    def get_latest_stock_quotes(self, symbols):
        self.stock_calls.append(list(symbols))
        if self.stock_missing:
            return {}
        return {s:{"bp":self.stock_mid-.01,"ap":self.stock_mid+.01,"t":self.at.isoformat()} for s in symbols}


def watch(tmp_path, **rule_changes):
    case = make_case(tmp_path / "input")
    result = assess(case)
    assert result["quality"]["status"] == "pass"
    root = tmp_path / "watch"
    rules = replace(PrimaryObservationRules(180), **rule_changes)
    identity = register_primary_observation(root, case["snapshot"], result["admission"], rules)
    return root, identity, case, result, rules


def test_persistent_registration_is_idempotent_and_never_invents_fill(tmp_path):
    root, identity, case, result, rules = watch(tmp_path)
    assert register_primary_observation(root,case["snapshot"],result["admission"],rules)==identity
    assert len(list(root.glob("*.json"))) == 1
    row = json.loads((root / (identity + ".json")).read_text())
    assert row["observation_window_basis"] == "admission_pending_fill"
    tape = export_primary_observations(root,source_kind="synthetic")
    assert tape["cases"][0]["fills"] == []
    assert tape["cohort_scope"] == "admitted_entries_only"
    assert tape["entry_edge_established"] is False


def test_restart_and_manual_exit_do_not_truncate_observation_path(tmp_path):
    root, identity, _, _, _ = watch(tmp_path)
    at = START + timedelta(seconds=10)
    # No position store or open-position input exists. Reopening the root is
    # sufficient after restart, even if the owner has already manually exited.
    for seconds in (60,120,180):
        now = at + timedelta(seconds=seconds)
        q = Quotes(now)
        poll_primary_observations(Path(str(root)),q,now_fn=lambda:now)
        assert q.calls == [[PRIMARY]]
    tape = export_primary_observations(root,source_kind="synthetic")
    assert tape["capture_statuses"][identity] == "window_closed"
    result = assess(tape["cases"][0])
    assert result["quality"]["status"] == "pass", result
    assert result["quality"]["evidence_kind"] == "simulated_fill"
    assert assess(tape["cases"][0],protocol(fill_basis="recorded_primary_fill"))["status"] == "missing_or_ambiguous_fill"


def test_runner_is_not_queried_to_mask_primary_result(tmp_path):
    root, _, _, _, _ = watch(tmp_path)
    now=START+timedelta(seconds=70);q=Quotes(now,bid=2)
    poll_primary_observations(root,q,now_fn=lambda:now)
    assert q.calls == [[PRIMARY]]


def test_direct_underlying_is_recorded_separately_from_option_path(tmp_path):
    root, _, _, _, _ = watch(tmp_path)
    now = START + timedelta(seconds=70)
    q = Quotes(now, stock_mid=104.25)
    result = poll_primary_observations(root, q, now_fn=lambda: now)
    assert result["underlying_observed"] == 1
    assert q.stock_calls == [["AAPL"]]
    point = export_primary_observations(root, source_kind="synthetic")["cases"][0]["outcome_snapshots"][0]
    assert point["option_chain"][0]["option_symbol"] == PRIMARY
    assert point["underlying_chain"] == [{
        "symbol": "AAPL", "bid": 104.24, "ask": 104.26, "quote_timestamp": now.isoformat()
    }]


def test_underlying_quote_failure_does_not_erase_option_evidence(tmp_path):
    root, _, _, _, _ = watch(tmp_path)
    now = START + timedelta(seconds=70)
    q = Quotes(now, stock_missing=True)
    result = poll_primary_observations(root, q, now_fn=lambda: now)
    assert result["observed"] == 1 and result["errors"] == []
    assert result["underlying_observed"] == 0 and result["underlying_errors"]
    point = export_primary_observations(root, source_kind="synthetic")["cases"][0]["outcome_snapshots"][0]
    assert point["option_chain"][0]["option_symbol"] == PRIMARY
    assert point["underlying_chain"] == []
    assert point["underlying_data_issue"] == "missing_observed_quote"


def test_proxy_signal_is_not_compared_in_incompatible_price_units(tmp_path):
    case = make_case(tmp_path / "input")
    result = assess(case)
    admission = deepcopy(result["admission"])
    admission["signal_symbol"] = "SPY"
    admission["underlying_reference_basis"] = "captured_index_estimate_with_fresh_proxy"
    admission["live_signal_thesis"]["at_refresh"]["signal_symbol"] = "SPY"
    root = tmp_path / "watch"
    identity = register_primary_observation(root, case["snapshot"], admission, PrimaryObservationRules(180))
    row = json.loads((root / (identity + ".json")).read_text())
    assert row["underlying_followthrough"]["status"] == "not_applicable"
    now = START + timedelta(seconds=70)
    q = Quotes(now)
    poll_primary_observations(root, q, now_fn=lambda: now)
    assert q.stock_calls == []


def test_missing_quote_is_recorded_not_silently_dropped(tmp_path):
    root, _, _, _, _ = watch(tmp_path)
    now=START+timedelta(seconds=70);q=Quotes(now,missing=True)
    result=poll_primary_observations(root,q,now_fn=lambda:now)
    assert result["observed"]==1 and result["errors"]
    point=export_primary_observations(root,source_kind="synthetic")["cases"][0]["outcome_snapshots"][0]
    assert point["option_chain"]==[] and point["data_issue"]


def test_late_restart_closes_window_without_fabricating_earlier_prices(tmp_path):
    root, _, _, _, _=watch(tmp_path)
    now=START+timedelta(hours=1)
    result=poll_primary_observations(root,Quotes(now),now_fn=lambda:now)
    assert result["window_closed"]==1
    evaluated=assess(export_primary_observations(root,source_kind="synthetic")["cases"][0])
    assert evaluated["quality"]["status"]=="unscorable"


def test_polling_cadence_and_closed_windows_do_not_make_extra_requests(tmp_path):
    root, _, _, _, _=watch(tmp_path)
    now=START+timedelta(seconds=70);q=Quotes(now)
    poll_primary_observations(root,q,now_fn=lambda:now)
    assert poll_primary_observations(root,q,now_fn=lambda:now)["observed"]==0
    assert len(q.calls)==1
    later=START+timedelta(seconds=190)
    poll_primary_observations(root,Quotes(later),now_fn=lambda:later)
    assert poll_primary_observations(root,q,now_fn=lambda:later+timedelta(seconds=30))["observed"]==0


def test_existing_observation_window_cannot_be_redefined(tmp_path):
    root, _, case, result, rules=watch(tmp_path)
    with pytest.raises(ValueError,match="cannot_change_existing"):
        register_primary_observation(root,case["snapshot"],result["admission"],replace(rules,window_seconds=240))


def test_snapshot_binding_prevents_registering_other_entries(tmp_path):
    root, _, case, result, rules=watch(tmp_path)
    other=deepcopy(case["snapshot"]);other["ticker"]="OTHER"
    with pytest.raises(ValueError):register_primary_observation(root,other,result["admission"],rules)


def test_observer_capacity_and_disk_limits_are_explicit(tmp_path):
    root, _, case, result, rules=watch(tmp_path,max_active=1)
    other=deepcopy(result["admission"])
    other["checked_at"]=(START+timedelta(seconds=11)).isoformat()
    other["recorded_refresh"]["received_at"]=other["checked_at"]
    with pytest.raises(ValueError,match="capacity"):
        register_primary_observation(root,case["snapshot"],other,rules)
    with pytest.raises(ValueError,match="store_size"):
        register_primary_observation(tmp_path/"tiny",case["snapshot"],result["admission"],replace(rules,max_store_bytes=1))


def test_config_is_opt_in_and_rejects_invalid_windows(monkeypatch):
    monkeypatch.delenv("AUTOBOTT_PRIMARY_OBSERVATION_SECONDS",raising=False)
    assert configured_observation_rules() is None
    monkeypatch.setenv("AUTOBOTT_PRIMARY_OBSERVATION_SECONDS","1800")
    assert configured_observation_rules().window_seconds==1800
    for value in ("0","-1","abc","86401"):
        monkeypatch.setenv("AUTOBOTT_PRIMARY_OBSERVATION_SECONDS",value)
        with pytest.raises(ValueError):configured_observation_rules()


def test_cycle_records_observation_without_changing_primary_order(tmp_path,monkeypatch):
    monkeypatch.setenv("AUTOBOTT_PRIMARY_OBSERVATION_SECONDS","180")
    monkeypatch.setenv("AUTOBOTT_ARTIFACTS_ROOT",str(tmp_path/"evidence"))
    result,broker,_=run_cycle(tmp_path,monkeypatch,pair=True,v2=True)
    assert len(broker.submitted)==2
    assert broker.submitted[0].option_symbol==PRIMARY
    assert any(r["disposition"]=="primary_observation_registered" for r in result.execution_outcomes)
    tape=export_primary_observations(tmp_path/"evidence"/"primary_followthrough",source_kind="synthetic")
    assert len(tape["cases"])==1
    assert tape["cases"][0]["fills"]==[]


def test_cycle_binds_quality_protocol_before_submission(tmp_path, monkeypatch):
    monkeypatch.setenv("AUTOBOTT_PRIMARY_OBSERVATION_SECONDS", "180")
    monkeypatch.setenv("AUTOBOTT_ARTIFACTS_ROOT", str(tmp_path / "evidence"))
    monkeypatch.setenv("AUTOBOTT_ENTRY_QUALITY_RULES_JSON", json.dumps({
        "protocol_id": "synthetic-runtime-quality",
        "holding_seconds": 180,
        "target_return_pct": .20,
        "max_adverse_return_pct": .15,
        "persistence_seconds": 60,
        "max_quote_age_seconds": 30,
        "max_observation_gap_seconds": 60,
        "round_trip_fee_per_contract": 0,
        "contract_multiplier": 100,
    }))
    result, broker, _ = run_cycle(tmp_path, monkeypatch, pair=True, v2=True)
    assert len(broker.submitted) == 2
    files = list((tmp_path / "evidence" / "primary_followthrough").glob("*.json"))
    assert len(files) == 1
    row = json.loads(files[0].read_text())
    assert row["quality_protocol"]["rules"]["protocol_id"] == "synthetic-runtime-quality"
    assert row["quality_protocol"]["rules_hash"]
    assert any(r["disposition"] == "primary_entry_quality_poll" for r in result.execution_outcomes)


def test_observer_failure_does_not_remove_existing_entry_or_exit_guards(tmp_path,monkeypatch):
    monkeypatch.setenv("AUTOBOTT_PRIMARY_OBSERVATION_SECONDS","bad")
    result,broker,_=run_cycle(tmp_path,monkeypatch,pair=True,v2=True)
    assert len(broker.submitted)==2
    assert any(r["disposition"]=="primary_observation_config_invalid" for r in result.execution_outcomes)
    failed,broker,_=run_cycle(tmp_path/"second",monkeypatch,pair=True,v2=True,defect="stale")
    assert broker.submitted==[]


def test_cached_stale_quote_cannot_manufacture_persistence(tmp_path):
    root, _, _, _, _=watch(tmp_path)
    cached=Quotes(START)
    for seconds in (70,130,190):
        now=START+timedelta(seconds=seconds)
        poll_primary_observations(root,cached,now_fn=lambda:now)
    result=assess(export_primary_observations(root,source_kind="synthetic")["cases"][0])
    assert result["quality"]["status"]=="unscorable"


def test_changed_feed_is_preserved_and_rejected_by_study(tmp_path):
    root, _, _, _, _=watch(tmp_path)
    for seconds in (70,130,190):
        now=START+timedelta(seconds=seconds)
        q=Quotes(now);q.option_feed="opra"
        poll_primary_observations(root,q,now_fn=lambda:now)
    result=assess(export_primary_observations(root,source_kind="synthetic")["cases"][0])
    assert result["status"]=="missing_outcome_evidence"
    assert result["reason"]=="mixed_or_unknown_outcome_feed"


def test_corrupt_watch_identity_does_not_initiate_market_request(tmp_path):
    root, identity, _, _, _=watch(tmp_path)
    path=root/(identity+".json");row=json.loads(path.read_text());row["watch_id"]="different"
    path.write_text(json.dumps(row))
    q=Quotes(START+timedelta(seconds=70))
    with pytest.raises(ValueError,match="identity_mismatch"):
        poll_primary_observations(root,q,now_fn=lambda:q.at)
    assert q.calls==[]
