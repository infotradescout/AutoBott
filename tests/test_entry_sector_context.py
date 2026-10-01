"""Synthetic issuer classification and matched sector leadership contracts."""
from copy import deepcopy
from datetime import timedelta
import pytest
from autobott_v2.entry_sector_context import (SectorCatalog, SectorContextSource, parse_holdings,
    attach_sector_context, assess_sector_context, HOLDINGS_URL)
from test_entry_market_context import AT, minute_rows, assess
from test_entry_schedule_context import scheduled_snapshot

CSV = ('iShares Russell 3000 ETF\nFund Holdings as of,"Jun 30, 2026"\n\n'
       'Ticker,Name,Sector,Asset Class\nAAPL,APPLE,Information Technology,Equity\n'
       'INTC,INTEL,Information Technology,Equity\nNKE,NIKE,Consumer Discretionary,Equity\n'
       'SOFI,SOFI,Financials,Equity\nUSD,CASH,Cash and/or Derivatives,Cash\n')


def catalog(text=CSV,at=AT):
    return SectorCatalog(lambda:text,now_fn=lambda:at)


def peer_rows(step=.02,at=AT):
    result=minute_rows(at=at)
    for i,row in enumerate(result):
        op=200+i*step;cl=op+step*.8
        row.update(o=op,c=cl,h=max(op,cl)+.01,l=min(op,cl)-.01)
    return result


class PeerProvider:
    def __init__(self,rows=None):self.rows=peer_rows() if rows is None else rows;self.calls=[]
    def __call__(self,path,params):
        self.calls.append((path,dict(params)))
        assert path=="/v2/stocks/bars" and params["symbols"]=="XLK"
        return {"bars":{"XLK":deepcopy(self.rows)},"next_page_token":None}


def sector_snapshot(*,direction="bullish",peer_step=.02,at=AT):
    row=scheduled_snapshot(at=at)
    if direction=="bearish":
        from autobott_v2.bar_timing import completed_stock_bars
        row["entry_context"]["intraday"]["bars"]=completed_stock_bars(minute_rows(direction,at=at),cutoff=at,timeframe="1Min",lookback=30,minimum=6)
    source=SectorContextSource(PeerProvider(peer_rows(peer_step,at)),catalog=catalog(at=at))
    row["entry_context"]=attach_sector_context(row["entry_context"],source)
    return row


def test_issuer_fields_define_sector_without_guessing_company_name():
    record=catalog().get("INTC")
    assert record["sector"]=="Information Technology" and record["benchmark"]=="XLK"
    assert record["source_url"]==HOLDINGS_URL and record["asof"]=="2026-06-30"
    with pytest.raises(ValueError,match="not_in_observed_holdings"):catalog().get("UNMAPPED")


@pytest.mark.parametrize("direction,step,expected",[("bullish",.02,"confirmed"),("bullish",.2,"wait"),
    ("bearish",-.02,"confirmed"),("bearish",-.2,"wait")])
def test_direction_requires_same_window_sector_leadership(direction,step,expected):
    row=sector_snapshot(direction=direction,peer_step=step)
    before=deepcopy(row)
    result=assess(row,direction)
    assert result["status"]==expected,result
    assert result["sector_evidence"]["whole_market_ranking_performed"] is False
    assert row==before


def test_equal_proportional_returns_are_not_false_leadership():
    row=sector_snapshot();own=row["entry_context"]["intraday"]["bars"]
    row["entry_context"]["sector_context"]["bars"]=deepcopy(own)
    for bar in row["entry_context"]["sector_context"]["bars"]:
        for key in ("open","high","low","close"):bar[key]*=2
    result=assess(row)
    assert result["status"]=="wait" and result["sector_evidence"]["relative_return"]==0


def test_missing_sector_endpoint_is_not_replaced_by_another_minute():
    row=sector_snapshot();row["entry_context"]["sector_context"]["bars"].pop()
    assert assess(row)["reason"]=="sector_observation_window_not_matched"


def test_internal_missing_sector_minute_remains_unknown():
    row=sector_snapshot();row["entry_context"]["sector_context"]["bars"].pop(-3)
    assert assess(row)["reason"]=="sector_observation_window_not_matched"


def test_future_classification_does_not_backfill_old_decision():
    row=sector_snapshot()
    row["entry_context"]["sector_context"]["classification"]["known_at"]=(AT+timedelta(seconds=1)).isoformat()
    assert assess(row)["reason"]=="sector_classification_not_known_or_stale"


@pytest.mark.parametrize("field,value",[("sector","Made up"),("benchmark","XLF"),("symbol","OTHER"),("source_url","https://example.test")])
def test_classification_binding_cannot_be_substituted(field,value):
    row=sector_snapshot();row["entry_context"]["sector_context"]["classification"][field]=value
    with pytest.raises(ValueError):assess(row)


def test_feed_mismatch_does_not_manufacture_relative_strength():
    row=sector_snapshot();row["entry_context"]["sector_context"]["stock_feed"]="sip"
    with pytest.raises(ValueError,match="feed"):assess(row)


def test_new_native_context_requires_sector_capsule():
    row=sector_snapshot();del row["entry_context"]["sector_context"]
    with pytest.raises(ValueError,match="native_sector_required"):assess(row)


def test_native_sector_error_is_not_a_flat_benchmark():
    row=sector_snapshot();row["entry_context"]["sector_context"]["status"]="unavailable"
    assert assess(row)["reason"]=="sector_source_unavailable"


def test_broad_fund_is_explicitly_not_a_stock_sector_comparison():
    row=scheduled_snapshot();ctx=row["entry_context"]
    ctx.update(symbol="SPY",signal_symbol="SPY");ctx["intraday"]["symbol"]="SPY";row["ticker"]="SPY"
    class NoCatalog:
        def get(self,*_):pytest.fail("must not classify broad fund as one sector")
    row["entry_context"]=attach_sector_context(ctx,SectorContextSource(lambda *_:pytest.fail("no peer call"),catalog=NoCatalog()))
    result=assess(row)
    assert result["status"]=="confirmed" and result["sector_evidence"]["status"]=="not_applicable"


def test_sector_request_retains_exact_raw_feed_cutoff_and_caches_only_identical_window():
    provider=PeerProvider();src=SectorContextSource(provider,catalog=catalog())
    context=scheduled_snapshot()["entry_context"]
    a=src.collect(context);a["bars"].clear()
    b=src.collect(context)
    assert b["bars"] and len(provider.calls)==1
    _,params=provider.calls[0]
    assert params["feed"]=="iex" and params["timeframe"]=="1Min" and params["adjustment"]=="raw" and params["asof"]=="-"
    assert params["end"]==AT.isoformat()


def test_catalog_cache_is_public_immutable_by_copy_and_bounded():
    calls=[]
    def fetch():calls.append(1);return CSV
    cat=SectorCatalog(fetch,now_fn=lambda:AT)
    a=cat.get("AAPL");a["sector"]="BAD"
    assert cat.get("AAPL")["sector"]=="Information Technology" and len(calls)==1
    cat._now=lambda:AT+timedelta(hours=7)
    cat.get("INTC");assert len(calls)==2


@pytest.mark.parametrize("text",[CSV.replace("Jun 30, 2026","Aug 01, 2026"),CSV.replace("Jun 30, 2026","May 01, 2026")])
def test_stale_and_future_holdings_are_unknown(text):
    with pytest.raises(ValueError,match="stale_or_future"):catalog(text).get("AAPL")


@pytest.mark.parametrize("text",[CSV.replace("Sector","NotSector"),CSV.replace("Information Technology","Imaginary"),
    CSV+"AAPL,APPLE,Financials,Equity\n",CSV.replace("iShares Russell 3000 ETF","Other Fund")])
def test_malformed_or_conflicting_classifications_fail(text):
    with pytest.raises(ValueError):parse_holdings(text)


def test_symbol_aliases_are_not_guessed():
    cat=catalog(CSV+"BRK B,BERKSHIRE,Financials,Equity\n")
    with pytest.raises(ValueError):cat.get("BRK.B")
