"""Synthetic contracts for provider-context use in real capture/admission."""
import json
from pathlib import Path
from datetime import UTC, datetime
import pytest
from autobott_v2.phase1_snapshot_capture import capture_symbol_snapshot, CaptureRules
from test_phase1_snapshot_capture import FakeCaptureClient
from test_entry_market_timing import EntryTape, run_cycle


def test_capture_retains_provider_context_instead_of_discarding_it(tmp_path):
    class Client(FakeCaptureClient):
        requires_entry_context = True
        def get_entry_context(self, symbol, *, signal_symbol, cutoff):
            return {"schema_version":"entry_context.v1", "status":"unavailable",
                    "reason":"synthetic_provider_denied", "symbol":symbol,
                    "signal_symbol":signal_symbol, "cutoff":cutoff.isoformat()}
    at=datetime(2026,6,30,13,30,tzinfo=UTC)
    path=capture_symbol_snapshot(symbol="SPY",corpus_root=tmp_path,scheduled_market_time=at,
        captured_at_utc=at,corpus_type="test_fixture",market_timezone="America/New_York",
        volatility_proxy_symbol="VIXY",data_client=Client(),rules=CaptureRules(),monotonic_fn=lambda:0)
    row=json.loads(Path(path).read_text())
    assert row.get("entry_context",{}).get("reason")=="synthetic_provider_denied"


def test_native_context_failure_cannot_submit_a_broker_entry(tmp_path, monkeypatch):
    monkeypatch.setattr(EntryTape,"requires_entry_context",True,raising=False)
    def unavailable(self,symbol,*,signal_symbol,cutoff):
        return {"schema_version":"entry_context.v1","status":"unavailable","reason":"provider_failed"}
    monkeypatch.setattr(EntryTape,"get_entry_context",unavailable,raising=False)
    result,broker,_=run_cycle(tmp_path,monkeypatch,pair=True,v2=True)
    assert broker.submitted==[], "A native feed failure is not a cleared entry context"


@pytest.mark.parametrize("scenario,expected",[("confirmed",True),("opposing",False),("fresh_news",False),("weak_volume",False)])
def test_real_cycle_uses_context_to_allow_or_reject_actual_primary(tmp_path,monkeypatch,scenario,expected):
    from datetime import timedelta
    from autobott_v2.entry_market_context import fetch_entry_context
    from test_entry_market_context import Provider, minute_rows, article
    from test_entry_market_timing import START
    rows=minute_rows("bearish" if scenario=="opposing" else "bullish")
    if scenario=="weak_volume":rows[-1]["v"]=1
    news=[article(at=START-timedelta(seconds=30))] if scenario=="fresh_news" else []
    provider=Provider(rows=rows,news=news)
    def context(self,symbol,*,signal_symbol,cutoff):
        return fetch_entry_context(provider,symbol,signal_symbol=signal_symbol,cutoff=cutoff)
    monkeypatch.setattr(EntryTape,"requires_entry_context",True,raising=False)
    monkeypatch.setattr(EntryTape,"get_entry_context",context,raising=False)
    result,broker,_=run_cycle(tmp_path,monkeypatch,pair=True,v2=True)
    assert result.scanner_candidates_count==1
    assert bool(broker.submitted) is expected
    if expected:
        assert len(broker.submitted)==2
        assert broker.submitted[0].option_symbol=="AAPL260703C00105000"
        assert broker.submitted[0].take_profit_price==3.75
        assert broker.submitted[0].stop_loss_price==1.375
        admission=next(r for r in result.execution_outcomes if r["disposition"]=="entry_market_revalidated")
        assert admission["entry_context_evidence"]["at_refresh"]["status"]=="confirmed"
    else:
        assert any(r["reason"]=="entry_context_not_confirmed" for r in result.skipped)


def test_context_trigger_rechecks_stock_price_after_quote_refresh(tmp_path,monkeypatch):
    from autobott_v2.entry_market_context import fetch_entry_context
    from test_entry_market_context import Provider
    original=EntryTape.get_latest_stock_quotes
    def quotes(self,symbols):
        result=original(self,symbols)
        if self.stock_calls>1:
            for q in result.values():q.update(bp=104.30,ap=104.32)
        return result
    def context(self,symbol,*,signal_symbol,cutoff):
        return fetch_entry_context(Provider(),symbol,signal_symbol=signal_symbol,cutoff=cutoff)
    monkeypatch.setattr(EntryTape,"get_latest_stock_quotes",quotes)
    monkeypatch.setattr(EntryTape,"requires_entry_context",True,raising=False)
    monkeypatch.setattr(EntryTape,"get_entry_context",context,raising=False)
    result,broker,client=run_cycle(tmp_path,monkeypatch,pair=True,v2=True)
    assert client.refreshes
    assert broker.submitted==[]
    assert any(r.get("detail")=="current_quote_lost_intraday_trigger" for r in result.skipped)


def test_native_paper_adapter_uses_market_data_only(monkeypatch):
    from autobott_v2.phase1_alpaca_client import AlpacaPaperClient
    from test_phase1_alpaca_client import _config
    from test_entry_market_context import Provider, AT
    provider=Provider();client=AlpacaPaperClient(_config())
    def read(base,path,params):
        assert base=="https://data.alpaca.markets"
        return provider(path,params)
    monkeypatch.setattr(client,"_get_json_with_retry",read)
    ctx=client.get_entry_context("AAPL",signal_symbol="AAPL",cutoff=AT)
    assert client.requires_entry_context is True and ctx["status"]=="observed"
    assert [p for p,_ in provider.calls]==["/v1beta1/news","/v2/stocks/bars"]


def test_study_replays_the_same_recorded_context_without_network(tmp_path):
    from test_primary_entry_study import make_case, assess
    from test_entry_market_context import snapshot
    case=make_case(tmp_path)
    case["snapshot"]["entry_context"]=snapshot()["entry_context"]
    result=assess(case)
    assert result["status"]=="evaluated",result
    assert result["admission"]["entry_context_evidence"]["at_refresh"]["status"]=="confirmed"
    case["snapshot"]["entry_context"]["intraday"]["bars"][-1]["volume"]=0
    result=assess(case)
    assert result["status"]=="admission_rejected"
    assert result["quality"] is None
