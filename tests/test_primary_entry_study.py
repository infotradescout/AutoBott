"""Synthetic study contracts plus real v2 shell/selector/admission integration."""
from copy import deepcopy
from dataclasses import asdict, replace
from datetime import timedelta
import json
from pathlib import Path
import pytest

import autobott_v2.primary_entry_study as study
from autobott_v2.core_runner import CoreRunnerRules
from autobott_v2.entry_admission import EntryMarketRules
from autobott_v2.entry_quality import EntryQualityRules
from autobott_v2.phase1_models import Phase1Rules
from autobott_v2.phase1_snapshot_capture import CaptureRules, capture_symbol_snapshot
from test_entry_market_timing import START, V2EntryTape, run_cycle

PRIMARY = "AAPL260703C00105000"


def protocol(**changes):
    value = study.PrimaryStudyProtocol(
        "synthetic-test-not-production-default", (START-timedelta(days=1)).isoformat(),
        START.isoformat(), (START+timedelta(hours=1)).isoformat(), "refresh_ask_shadow", 30, 1,
        EntryQualityRules("synthetic-opportunity",180,.20,.15,60,30,60,0,100),
        Phase1Rules(), CoreRunnerRules(), EntryMarketRules())
    return replace(value, **changes)


def outcomes(at, bids=(3.2,3.2,3.2)):
    return [{"ticker":"AAPL","timestamp":(at+timedelta(seconds=60*(i+1))).isoformat(),
             "source":{"options_feed":"indicative"},
             "option_chain":[{"option_symbol":PRIMARY,"bid":bid,"ask":bid+.05,
                              "quote_timestamp":(at+timedelta(seconds=60*(i+1))).isoformat()}]}
            for i,bid in enumerate(bids)]


def make_case(tmp_path):
    client = V2EntryTape()
    file = capture_symbol_snapshot(symbol="AAPL",corpus_root=tmp_path,
        scheduled_market_time=START,captured_at_utc=START,corpus_type="test_fixture",
        market_timezone="America/New_York",volatility_proxy_symbol="VIXY",
        data_client=client,rules=CaptureRules(),monotonic_fn=lambda:0)
    snapshot = json.loads(Path(file).read_text())
    symbols = [q["option_symbol"] for q in snapshot["option_chain"]]
    received = START+timedelta(seconds=10)
    return {"sample_id":"sample-1","snapshot":snapshot,
            "refresh":{"requested_at":received.isoformat(),"received_at":received.isoformat(),
                "options_feed":"indicative","option_quotes":client.get_latest_option_quotes(symbols),
                "stock_quotes":client.get_latest_stock_quotes(["AAPL"]),
                "authorized_prices":{q["option_symbol"]:q["ask"] for q in snapshot["option_chain"]}},
            "fills":[{"option_symbol":PRIMARY,"price":2.6,"timestamp":received.isoformat(),
                      "side":"buy_to_open","quantity":1,"broker_order_id":"synthetic-only-order"}],
            "outcome_snapshots":outcomes(received)}


def tape(case):
    return {"schema_version":"primary_entry_tape.v1","source_kind":"synthetic",
            "cohort_scope":"all_recorded_scans","cases":[case]}


def assess(case, rules=None, **kwargs):
    return study.evaluate_primary_case(case,rules or protocol(),engine=kwargs.get("engine","v2"))


def test_real_selector_and_admission_produce_primary_opportunity(tmp_path):
    result=assess(make_case(tmp_path))
    assert result["status"]=="evaluated", result
    assert result["primary_option_symbol"]==PRIMARY
    assert result["runner_option_symbol"]!=PRIMARY
    assert result["quality"]["status"]=="pass"
    assert result["quality"]["evidence_kind"]=="simulated_fill"
    assert result["entry_price"]==2.6
    assert result["admission"]["executable_fill_verified"] is False


def test_runner_winner_cannot_rescue_primary_that_never_moves(tmp_path):
    case=make_case(tmp_path)
    case["outcome_snapshots"]=outcomes(START+timedelta(seconds=10),(2.5,2.5,2.5))
    for row in case["outcome_snapshots"]:
        row["option_chain"].append({"option_symbol":"AAPL260703C00115000","bid":20,"ask":21,"quote_timestamp":row["timestamp"]})
    assert assess(case)["quality"]["status"]=="fail"


def test_actual_fill_price_is_not_replaced_with_quote_assumption(tmp_path):
    case=make_case(tmp_path)
    case["fills"][0]["price"]=4
    actual=assess(case,protocol(fill_basis="recorded_primary_fill"))
    assert actual["entry_price"]==4
    assert actual["quality"]["status"]=="fail"
    assert actual["quality"]["evidence_kind"]=="broker_recorded_fill"
    assert assess(case)["quality"]["status"]=="pass"


@pytest.mark.parametrize("mode",["none","duplicate","wrong_symbol"])
def test_missing_or_ambiguous_fill_is_never_invented(tmp_path,mode):
    case=make_case(tmp_path)
    if mode=="none":case["fills"]=[]
    elif mode=="duplicate":case["fills"]*=2
    else:case["fills"][0]["option_symbol"]="ANOTHER"
    result=assess(case,protocol(fill_basis="recorded_primary_fill"))
    assert result["status"]=="missing_or_ambiguous_fill"
    assert result["quality"] is None


@pytest.mark.parametrize("change",[{"side":"sell_to_close"},{"quantity":2},{"quantity":True},
                                   {"broker_order_id":""},{"price":0},{"timestamp":START.isoformat()}])
def test_invalid_recorded_fill_cannot_be_scored(tmp_path,change):
    case=make_case(tmp_path);case["fills"][0].update(change)
    assert assess(case,protocol(fill_basis="recorded_primary_fill"))["status"]=="invalid_evidence"


def test_manual_exit_does_not_end_primary_observation_window(tmp_path):
    case=make_case(tmp_path)
    before=assess(case,protocol(fill_basis="recorded_primary_fill"))
    case["manual_exit"]={"timestamp":(START+timedelta(seconds=15)).isoformat(),"price":2.4}
    assert assess(case,protocol(fill_basis="recorded_primary_fill"))==before


def test_drawdown_before_recovery_is_a_failed_entry(tmp_path):
    case=make_case(tmp_path);case["outcome_snapshots"]=outcomes(START+timedelta(seconds=10),(2,3.2,3.2))
    result=assess(case)
    assert result["quality"]["status"]=="fail"
    assert result["quality"]["reason"]=="drawdown_before_sustained_opportunity"


def test_brief_target_touch_is_not_an_opportunity(tmp_path):
    case=make_case(tmp_path);case["outcome_snapshots"]=outcomes(START+timedelta(seconds=10),(3.2,2.5,2.5))
    assert assess(case)["quality"]["reason"]=="target_not_persistent"


def test_window_truncation_stays_unscorable(tmp_path):
    case=make_case(tmp_path);case["outcome_snapshots"].pop()
    assert assess(case)["quality"]["status"]=="unscorable"


def test_future_quotes_cannot_influence_primary_choice_or_admission(tmp_path):
    case=make_case(tmp_path);original=assess(case)
    case["outcome_snapshots"]=outcomes(START+timedelta(seconds=10),(100,100,100))
    changed=assess(case)
    for key in ("decision","primary_option_symbol","runner_option_symbol","admission","entry_price"):
        assert changed[key]==original[key]


def test_no_candidate_is_counted_not_simulated_as_entry(tmp_path):
    case=make_case(tmp_path);case["snapshot"]["context"]["blackout_event"]=True
    result=study.run_primary_study(tape(case),protocol(),engine="v2")
    assert result["summary"]["sample_statuses"]=={"no_candidate":1}
    assert result["summary"]["primary_quality"]["pass_rate_scorable"] is None
    assert result["summary"]["confirmed_opportunities_per_recorded_sample"]==0


def test_required_runner_absence_is_preserved(tmp_path):
    case=make_case(tmp_path)
    case["snapshot"]["option_chain"]=[q for q in case["snapshot"]["option_chain"] if q["option_symbol"]==PRIMARY]
    assert assess(case)["status"]=="no_pair"


def test_missing_refresh_not_reconstructed_from_future_path(tmp_path):
    case=make_case(tmp_path);case.pop("refresh")
    assert assess(case)["status"]=="missing_refresh_evidence"


def test_comparator_without_selected_contract_quotes_remains_unknown(tmp_path):
    case=make_case(tmp_path);case["refresh"]["option_quotes"].pop(PRIMARY)
    assert assess(case)["status"]=="missing_refresh_evidence"


def test_stale_refresh_is_rejected_by_same_runtime_admission(tmp_path):
    case=make_case(tmp_path)
    case["refresh"]["option_quotes"][PRIMARY]["t"]=(START-timedelta(minutes=5)).isoformat()
    assert assess(case)["status"]=="admission_rejected"


def test_feed_switch_cannot_manufacture_an_option_return(tmp_path):
    case=make_case(tmp_path);case["outcome_snapshots"][0]["source"]["options_feed"]="opra"
    assert assess(case)["reason"]=="mixed_or_unknown_outcome_feed"


def test_holdout_boundary_censoring_is_visible(tmp_path):
    case=make_case(tmp_path)
    result=assess(case,protocol(evaluation_end=(START+timedelta(seconds=100)).isoformat()))
    assert result["status"]=="boundary_censored"
    assert result["quality"] is None


def test_development_outcome_windows_cannot_overlap_holdout():
    with pytest.raises(ValueError,match="overlap"):
        protocol(development_last_entry_at=(START-timedelta(seconds=10)).isoformat())


def test_protocol_roundtrip_requires_all_persisted_parameters():
    p=protocol();payload=asdict(p)
    assert study.PrimaryStudyProtocol.from_dict(payload).config_hash==p.config_hash
    payload["decision"].pop("min_confidence")
    with pytest.raises(ValueError):study.PrimaryStudyProtocol.from_dict(payload)


def test_duplicate_ids_and_equivalent_snapshot_times_are_rejected(tmp_path):
    case=make_case(tmp_path);data=tape(case);data["cases"].append(deepcopy(case))
    with pytest.raises(ValueError,match="duplicate_sample_id"):study.run_primary_study(data,protocol(),engine="v2")
    data["cases"][1]["sample_id"]="other"
    data["cases"][1]["snapshot"]["timestamp"]=case["snapshot"]["timestamp"].replace("Z","+00:00")
    with pytest.raises(ValueError,match="duplicate_snapshot_identity"):study.run_primary_study(data,protocol(),engine="v2")


def test_complete_inputs_remain_unchanged(tmp_path):
    data=tape(make_case(tmp_path));before=deepcopy(data)
    study.run_primary_study(data,protocol(),engine="v2")
    assert data==before


def test_manifest_is_written_before_evaluation_and_never_overwritten(tmp_path,monkeypatch):
    data=tape(make_case(tmp_path));output=tmp_path/"study"
    original=study.evaluate_primary_case
    def checked(*a,**kw):
        manifest=json.loads((output/"manifest.json").read_text())
        assert manifest["protocol_hash"]==protocol().config_hash
        assert manifest["preregistration_verified"] is False
        return original(*a,**kw)
    monkeypatch.setattr(study,"evaluate_primary_case",checked)
    study.run_primary_study(data,protocol(),engine="v2",output_dir=output)
    with pytest.raises(FileExistsError):study.run_primary_study(data,protocol(),engine="v2",output_dir=output)


def test_report_preserves_unknown_outcome_denominator(tmp_path):
    data=tape(make_case(tmp_path));data["cases"][0]["outcome_snapshots"].pop()
    report=study.run_primary_study(data,protocol(),engine="v2")
    assert report["summary"]["samples_recorded"]==1
    assert report["summary"]["primary_quality"]["unscorable"]==1
    assert report["summary"]["primary_quality"]["pass_rate_scorable"] is None


def test_comparison_requires_same_tape_and_protocol(tmp_path):
    data=tape(make_case(tmp_path));base=study.run_primary_study(data,protocol(),engine="legacy")
    candidate=study.run_primary_study(data,protocol(),engine="v2")
    comparison=study.compare_primary_studies(base,candidate)
    assert comparison["edge_established"] is False
    changed=deepcopy(candidate);changed["manifest"]["protocol_hash"]="different"
    with pytest.raises(ValueError):study.compare_primary_studies(base,changed)
    changed=deepcopy(candidate);changed["manifest"]["tape_hash"]="different"
    with pytest.raises(ValueError):study.compare_primary_studies(base,changed)


def test_cli_produces_a_complete_report_without_a_broker(tmp_path,capsys):
    data=tape(make_case(tmp_path));protocol_path=tmp_path/"protocol.json";tape_path=tmp_path/"tape.json"
    protocol_path.write_text(json.dumps(asdict(protocol())));tape_path.write_text(json.dumps(data))
    assert study.main(["--tape",str(tape_path),"--protocol",str(protocol_path),"--engine","v2","--output",str(tmp_path/"result")])==0
    report=json.loads((tmp_path/"result"/"report.json").read_text())
    assert report["summary"]["primary_quality"]["passes"]==1
    assert report["manifest"]["source_kind"]=="synthetic"


def test_runtime_to_study_retains_exact_primary_and_admission(tmp_path,monkeypatch):
    result,broker,_=run_cycle(tmp_path,monkeypatch,pair=True,v2=True)
    event=next(r for r in result.execution_outcomes if r["disposition"]=="entry_market_revalidated")
    snapshot=json.loads(Path(result.snapshot_paths[0]).read_text())
    received=study.aware_utc(event["checked_at"])
    case=study.case_from_runtime_evidence(sample_id="from-runtime",snapshot=snapshot,
        admission_event=event,outcome_snapshots=outcomes(received),fills=[])
    offline=assess(case)
    assert offline["primary_option_symbol"]==broker.submitted[0].option_symbol
    assert offline["runner_option_symbol"]==broker.submitted[1].option_symbol
    assert offline["admission"]["quotes"]==event["quotes"]
    assert offline["admission"]["completed_bars"]==event["completed_bars"]
    assert offline["quality"]["status"]=="pass"
    broken=deepcopy(snapshot);broken["ticker"]="ANOTHER"
    with pytest.raises(ValueError,match="bound"):
        study.case_from_runtime_evidence(sample_id="x",snapshot=broken,admission_event=event,outcome_snapshots=[],fills=[])


def test_study_does_not_inherit_poisoned_deployment_settings(tmp_path,monkeypatch):
    data=tape(make_case(tmp_path));before=study.run_primary_study(data,protocol(),engine="v2")
    monkeypatch.setenv("RENDER","true");monkeypatch.setenv("AUTOBOTT_CORE_MIN_ABS_DELTA","0.99")
    monkeypatch.setenv("AUTOBOTT_ENTRY_LIMIT_EXTRA","100")
    assert study.run_primary_study(data,protocol(),engine="v2")==before


def test_same_broker_fill_cannot_count_as_multiple_good_entries(tmp_path):
    case=make_case(tmp_path);data=tape(case)
    other=deepcopy(case);other["sample_id"]="next-scan"
    other["snapshot"]["timestamp"]=(START+timedelta(seconds=1)).isoformat()
    data["cases"].append(other)
    with pytest.raises(ValueError,match="duplicate_recorded_fill"):
        study.run_primary_study(data,protocol(fill_basis="recorded_primary_fill"),engine="v2")


def test_admitted_only_cohort_cannot_disguise_itself_as_all_scans(tmp_path):
    data=tape(make_case(tmp_path));data["cohort_scope"]="admitted_entries_only"
    report=study.run_primary_study(data,protocol(),engine="v2")
    assert report["manifest"]["cohort_scope"]=="admitted_entries_only"
    assert report["manifest"]["cohort_completeness_verified"] is False
    assert report["manifest"]["independent_samples_assumed"] is False
    data.pop("cohort_scope")
    with pytest.raises(ValueError):study.run_primary_study(data,protocol(),engine="v2")


def test_compare_cli_creates_report_and_refuses_changed_results(tmp_path,capsys):
    data=tape(make_case(tmp_path));files=[]
    for engine in ("legacy","v2"):
        folder=tmp_path/engine
        study.run_primary_study(data,protocol(),engine=engine,output_dir=folder)
        files.append(str(folder/"report.json"))
    assert study.main(["--compare",*files,"--output",str(tmp_path/"comparison")])==0
    report=json.loads(Path(files[1]).read_text());report["results"][0]["reason"]="altered"
    Path(files[1]).write_text(json.dumps(report))
    with pytest.raises(ValueError,match="integrity"):
        study.main(["--compare",*files,"--output",str(tmp_path/"corrupt")])
    assert not (tmp_path/"corrupt").exists()


def test_minimum_paired_evidence_is_not_inferred_from_observed_results(tmp_path):
    data=tape(make_case(tmp_path));p=protocol(minimum_paired_scorable=100)
    a=study.run_primary_study(data,p,engine="legacy")
    b=study.run_primary_study(data,p,engine="v2")
    assert study.compare_primary_studies(a,b)["conclusion"]=="insufficient_paired_evidence"


@pytest.mark.parametrize("feed",[None,"unreported",""])
def test_unknown_feed_does_not_become_valid_merely_by_matching_labels(tmp_path,feed):
    case=make_case(tmp_path)
    case["refresh"]["options_feed"]=feed
    case["snapshot"]["source"]["options_feed"]=feed
    for row in case["outcome_snapshots"]:row["source"]["options_feed"]=feed
    assert assess(case)["reason"]=="known_options_feed_required"


def test_entry_snapshot_and_refresh_cannot_mix_option_feeds(tmp_path):
    case=make_case(tmp_path);case["snapshot"]["source"]["options_feed"]="opra"
    assert assess(case)["reason"]=="entry_snapshot_and_refresh_feed_mismatch"
