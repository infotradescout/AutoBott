"""Full native input and fake-broker entry checks for sector/calendar context."""
from datetime import timedelta
from copy import deepcopy
import json
import pytest
from autobott_v2.entry_market_context import fetch_entry_context
from autobott_v2.entry_schedule_context import EntryScheduleSource, attach_entry_schedule
from autobott_v2.entry_sector_context import SectorContextSource, attach_sector_context
from test_entry_schedule_context import source, calendar, event, fomc_calendar
from test_entry_sector_context import catalog, PeerProvider, peer_rows
from test_entry_market_context import Provider, AT, assess
from test_entry_market_timing import EntryTape, run_cycle


@pytest.mark.parametrize("scenario,allowed",[("leaders",True),("sector_faster",False),("event",False),("closed",False),("unknown_calendar",False)])
def test_real_cycle_requires_sector_and_calendar_context(tmp_path,monkeypatch,scenario,allowed):
    def context(self,symbol,*,signal_symbol,cutoff):
        result=fetch_entry_context(Provider(),symbol,signal_symbol=signal_symbol,cutoff=cutoff)
        content=calendar(event(at=(AT+timedelta(minutes=5)).strftime("%Y%m%dT%H%M%SZ"),params="")) if scenario=="event" else None
        if scenario=="unknown_calendar":content="provider failure"
        result=attach_entry_schedule(result,source(text=content,rows=[] if scenario=="closed" else None),cutoff)
        result=attach_sector_context(result,SectorContextSource(PeerProvider(peer_rows(.2 if scenario=="sector_faster" else .02)),catalog=catalog()))
        return result
    monkeypatch.setattr(EntryTape,"requires_entry_context",True,raising=False)
    monkeypatch.setattr(EntryTape,"get_entry_context",context,raising=False)
    result,broker,_=run_cycle(tmp_path,monkeypatch,pair=True,v2=True)
    assert bool(broker.submitted) is allowed,result.skipped
    if allowed:
        assert len(broker.submitted)==2 and broker.submitted[0].option_symbol=="AAPL260703C00105000"
        assert broker.submitted[0].take_profit_price==3.75 and broker.submitted[0].stop_loss_price==1.375
        admission=next(r for r in result.execution_outcomes if r["disposition"]=="entry_market_revalidated")
        evidence=admission["entry_context_evidence"]["at_refresh"]
        assert evidence["sector_evidence"]["status"]=="confirmed"
        assert evidence["schedule_evidence"]["status"]=="observed_no_listed_event_block"
    else:
        assert result.trade_attempted_count==0
        assert any(r["reason"]=="entry_context_not_confirmed" for r in result.skipped)


def test_development_capture_ignores_stale_scoring_config_and_keeps_raw_watch(tmp_path, monkeypatch):
    def context(self, symbol, *, signal_symbol, cutoff):
        result = fetch_entry_context(Provider(), symbol, signal_symbol=signal_symbol, cutoff=cutoff)
        result = attach_entry_schedule(result, source(rows=None), cutoff)
        result = attach_sector_context(
            result, SectorContextSource(PeerProvider(peer_rows(.02)), catalog=catalog()))
        return result

    monkeypatch.setattr(EntryTape, "requires_entry_context", True, raising=False)
    monkeypatch.setattr(EntryTape, "get_entry_context", context, raising=False)
    monkeypatch.delenv("AUTOBOTT_PRIMARY_OBSERVATION_SECONDS", raising=False)
    monkeypatch.setenv("AUTOBOTT_PRIMARY_DEVELOPMENT_CAPTURE", "session")
    monkeypatch.setenv("AUTOBOTT_ARTIFACTS_ROOT", str(tmp_path / "artifacts"))
    monkeypatch.setenv("AUTOBOTT_ENTRY_QUALITY_RULES_JSON", json.dumps({
        "protocol_id": "stale-should-not-score-development",
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
    warning = next(row for row in result.execution_outcomes
                   if row["disposition"] == "primary_entry_quality_config_invalid")
    assert warning["detail"] == "development_capture_does_not_accept_scoring_protocol"

    files = list((tmp_path / "artifacts" / "primary_followthrough").glob("*.json"))
    assert len(files) == 1
    watch = json.loads(files[0].read_text())
    assert watch["quality_protocol"] is None
    assert watch["rules"]["end_basis"] == "session_close"
    assert watch["observation_window_basis"] == "admission_pending_fill_session_close"
    assert watch["window_end"].endswith("20:00:00+00:00")


@pytest.mark.parametrize("adapter",["paper","capture"])
def test_native_adapters_request_only_approved_inputs_and_reuse_shared_context(monkeypatch,adapter):
    import autobott_v2.entry_schedule_context as schedules
    import autobott_v2.entry_sector_context as sectors
    from autobott_v2.phase1_alpaca_client import AlpacaPaperClient
    from autobott_v2.phase1_snapshot_capture import AlpacaMarketDataClient
    from test_phase1_alpaca_client import _config
    Schedule, Sector=EntryScheduleSource,SectorContextSource
    monkeypatch.setattr(schedules,"EntryScheduleSource",lambda read:Schedule(read,public_fetch=lambda:calendar(),fomc_fetch=fomc_calendar,now_fn=lambda:AT))
    monkeypatch.setattr(sectors,"SectorContextSource",lambda read:Sector(read,catalog=catalog()))
    calls=[];provider=Provider();peer=PeerProvider()
    def read(base,path,params):
        calls.append((base,path,deepcopy(params)))
        if path=="/v2/calendar":
            assert base=="https://paper-api.alpaca.markets"
            assert params=={"start":"2026-07-01","end":"2026-07-01"}
            return [{"date":"2026-07-01","open":"09:30","close":"16:00"}]
        assert base=="https://data.alpaca.markets"
        if path=="/v2/stocks/bars" and params["symbols"]=="XLK":return peer(path,params)
        return provider(path,params)
    if adapter=="paper":
        client=AlpacaPaperClient(_config())
        monkeypatch.setattr(client,"_get_json_with_retry",read)
    else:
        client=AlpacaMarketDataClient.__new__(AlpacaMarketDataClient)
        client.data_url="https://data.alpaca.markets";client.trading_url="https://paper-api.alpaca.markets";client.stock_feed="iex"
        monkeypatch.setattr(client,"_get_json_with_retry",lambda path,params,base_url=None:read(base_url or client.data_url,path,params))
    for _ in range(2):
        packet=client.get_entry_context("AAPL",signal_symbol="AAPL",cutoff=AT)
        packet["received_at"]=AT.isoformat()
        result=assess({"ticker":"AAPL","timestamp":AT.isoformat(),"entry_context":packet})
        assert packet["schema_version"]=="entry_context.v3" and result["status"]=="confirmed",result
    assert sum(p=="/v2/calendar" for _,p,_ in calls)==1
    assert len(peer.calls)==1
    assert all(p in {"/v2/calendar","/v2/stocks/bars","/v1beta1/news"} for _,p,_ in calls)


def test_study_reports_new_context_coverage_without_backdating(tmp_path):
    from autobott_v2.primary_entry_study import run_primary_study
    from test_primary_entry_study import make_case, protocol, tape
    from test_entry_sector_context import sector_snapshot
    case=make_case(tmp_path)
    case["snapshot"]["entry_context"]=sector_snapshot()["entry_context"]
    report=run_primary_study(tape(case),protocol(),engine="v2")
    assert report["results"][0]["status"]=="evaluated",report["results"][0]
    assert report["manifest"]["cases_with_recorded_schedule"]==1
    assert report["manifest"]["cases_with_recorded_sector_context"]==1
    assert report["manifest"]["entry_sector_policy"]["id"]=="matched_sector_leadership.v1"
    assert report["manifest"]["source_provenance_verified"] is False
    assert report["summary"]["edge_established"] is False
    del case["snapshot"]["entry_context"]
    legacy=run_primary_study(tape(case),protocol(),engine="v2")
    assert legacy["manifest"]["cases_with_recorded_schedule"]==0
    assert legacy["manifest"]["cases_with_recorded_sector_context"]==0
