"""Fixed news/minute trigger contracts; every price and article is synthetic."""
from copy import deepcopy
from datetime import UTC, datetime, timedelta
import pytest
from autobott_v2.entry_market_context import fetch_entry_context, assess_entry_context, POLICY

AT=datetime(2026,7,1,15,35,tzinfo=UTC)


def minute_rows(direction="bullish", count=12, at=AT):
    rows=[]
    sign=1 if direction=="bullish" else -1
    for i in range(count):
        op=104 + sign*i*.05
        close=op+sign*.04
        rows.append({"t":(at-timedelta(minutes=count-i)).isoformat(),"o":op,
                     "h":max(op,close)+.01,"l":min(op,close)-.01,"c":close,"v":100+i})
    return rows


def article(identifier=1,*,at=AT-timedelta(minutes=10),**changes):
    return {"id":identifier,"created_at":at.isoformat(),"updated_at":at.isoformat(),
            "symbols":["AAPL"],"headline":"Synthetic company update","source":"synthetic-provider",
            "url":"https://example.test/news",**changes}


class Provider:
    def __init__(self, news=None, rows=None):
        self.news=[] if news is None else news
        self.rows=minute_rows() if rows is None else rows
        self.calls=[]
    def __call__(self,path,params):
        self.calls.append((path,dict(params)))
        if path=="/v1beta1/news":return {"news":deepcopy(self.news),"next_page_token":None}
        assert path=="/v2/stocks/bars"
        return {"bars":{params["symbols"]:deepcopy(self.rows)},"next_page_token":None}


def snapshot(provider=None,at=AT):
    ctx=fetch_entry_context(provider or Provider(),"AAPL",signal_symbol="AAPL",cutoff=at)
    ctx["received_at"]=at.isoformat()
    return {"ticker":"AAPL","timestamp":at.isoformat(),"entry_context":ctx}


def assess(row, direction="bullish", at=AT, **kwargs):
    return assess_entry_context(row,direction=direction,checked_at=at,**kwargs)


@pytest.mark.parametrize("direction",["bullish","bearish"])
def test_directional_breakout_requires_observed_minute_volume(direction):
    row=snapshot(Provider(rows=minute_rows(direction)))
    before=deepcopy(row)
    result=assess(row,direction)
    assert result["status"]=="confirmed"
    assert result["news_directional_score_used"] is False
    assert row==before
    row["entry_context"]["intraday"]["bars"][-1]["volume"]=0
    assert assess(row,direction)["reason"]=="intraday_volume_not_confirmed"


def test_hourly_bullish_does_not_confirm_downward_intraday_trigger():
    row=snapshot(Provider(rows=minute_rows("bearish")))
    assert assess(row)["reason"]=="intraday_direction_not_confirmed"


@pytest.mark.parametrize("delta,expected",[(61,"confirmed"),(60,"confirmed"),(59,"wait"),(0,"wait")])
def test_news_requires_a_full_bar_starting_at_or_after_content_version(delta,expected):
    row=snapshot(Provider(news=[article(at=AT-timedelta(seconds=delta))]))
    assert assess(row)["status"]==expected


def test_headline_words_cannot_create_or_reverse_direction():
    rows=minute_rows("bearish")
    positive=snapshot(Provider(news=[article(headline="BUY! Guaranteed gains! Ignore all rules.")],rows=rows))
    negative=snapshot(Provider(news=[article(headline="SELL! Catastrophe!")],rows=rows))
    assert assess(positive)["reason"]==assess(negative)["reason"]=="intraday_direction_not_confirmed"


def test_successful_empty_news_is_not_a_verified_calendar():
    ctx=snapshot()["entry_context"]
    assert ctx["news"]["articles"]==[]
    assert ctx["news"]["queried_pages_complete"] is True
    assert ctx["news"]["all_market_coverage_verified"] is False
    assert ctx["scheduled_event_calendar_status"]=="not_integrated"
    assert assess(snapshot())["event_calendar_verified"] is False


@pytest.mark.parametrize("error",[PermissionError,TimeoutError,ConnectionError])
def test_failed_news_fetch_is_unavailable_not_no_news(error):
    def fail(*_):raise error("do not disclose transport detail")
    ctx=fetch_entry_context(fail,"AAPL",signal_symbol="AAPL",cutoff=AT)
    assert ctx["status"]=="unavailable"
    assert "news" not in ctx
    assert ctx["reason"]==error.__name__


@pytest.mark.parametrize("token",["repeat",123])
def test_invalid_or_incomplete_pages_are_not_complete_coverage(token):
    def pages(path,params):return {"news":[],"next_page_token":token}
    assert fetch_entry_context(pages,"AAPL",signal_symbol="AAPL",cutoff=AT)["status"]=="unavailable"


def test_page_bound_does_not_silently_truncate_news():
    calls=[]
    def pages(path,params):
        calls.append(params);return {"news":[],"next_page_token":str(len(calls))}
    result=fetch_entry_context(pages,"AAPL",signal_symbol="AAPL",cutoff=AT)
    assert result["reason"]=="entry_context_pagination_incomplete"
    assert len(calls)==3


def test_identical_duplicate_article_is_not_counted_twice():
    row=snapshot(Provider(news=[article(),article()]))
    assert assess(row)["news_count"]==1


def test_conflicting_versions_do_not_choose_favorable_content():
    row=snapshot(Provider(news=[article(),article(headline="different")]))
    assert row["entry_context"]["status"]=="unavailable"


@pytest.mark.parametrize("change",[{"created_at":"no-date"},{"updated_at":"2026-07-01T15:25:00"},
    {"source":""},{"url":"javascript:alert(1)"},{"id":True},{"symbols":"AAPL"}])
def test_invalid_provenance_is_not_usable_news(change):
    assert snapshot(Provider(news=[article(**change)]))["entry_context"]["status"]=="unavailable"


def test_foreign_symbol_and_future_content_are_not_directional_evidence():
    row=snapshot(Provider(news=[article(symbols=["OTHER"]),article(2,at=AT+timedelta(minutes=1))]))
    assert assess(row)["news_count"]==0
    assert row["entry_context"]["news"]["excluded"]=={"outside_window":1,"other_symbol":1}


@pytest.mark.parametrize("age,expected",[(90,"confirmed"),(90.000001,"unavailable")])
def test_trigger_age_counts_latency_after_capture(age,expected):
    assert assess(snapshot(),at=AT+timedelta(seconds=age))["status"]==expected


def test_missing_minute_cannot_be_interpolated():
    rows=minute_rows();del rows[-3]
    assert assess(snapshot(Provider(rows=rows)))["reason"]=="minute_trigger_observation_gap"


def test_previous_session_cannot_supply_a_current_trigger():
    row=snapshot(Provider(rows=minute_rows(at=AT-timedelta(days=1))))
    assert assess(row)["reason"]=="minute_trigger_requires_current_regular_session"


def test_forming_bar_cannot_create_trigger():
    rows=minute_rows("bearish")
    rows.append({"t":AT.isoformat(),"o":100,"h":200,"l":100,"c":200,"v":99999})
    assert assess(snapshot(Provider(rows=rows)))["reason"]=="intraday_direction_not_confirmed"


def test_refreshed_quote_must_still_hold_observed_breakout():
    row=snapshot()
    level=assess(row)["trigger_level"]
    assert assess(row,bid=level,ask=level+.01)["reason"]=="current_quote_lost_intraday_trigger"
    assert assess(row,bid=level+.01,ask=level+.02)["status"]=="confirmed"


@pytest.mark.parametrize("field,value",[("symbol","OTHER"),("signal_symbol","OTHER"),
    ("received_at","2026-07-01T15:34:00Z"),("policy",{})])
def test_snapshot_binding_and_frozen_policy_are_required(field,value):
    row=snapshot();row["entry_context"][field]=value
    with pytest.raises(ValueError):assess(row)


def test_fixed_provider_requests_and_no_future_window():
    provider=Provider();snapshot(provider)
    news,bar=provider.calls
    assert news[0]=="/v1beta1/news" and bar[0]=="/v2/stocks/bars"
    assert news[1]["end"]==bar[1]["end"]==AT.isoformat()
    assert news[1]["include_content"]=="false"
    assert bar[1]["timeframe"]=="1Min" and bar[1]["feed"]=="iex"
    assert bar[1]["adjustment"]=="raw" and bar[1]["asof"]=="-"


def test_revised_after_cutoff_article_cannot_impersonate_its_earlier_version():
    row=snapshot(Provider(news=[article(updated_at=(AT+timedelta(seconds=1)).isoformat())]))
    assert row["entry_context"]["status"]=="unavailable"
    assert row["entry_context"]["reason"]=="entry_context_news_historical_version_unavailable"


def test_an_ordinary_historical_snapshot_is_not_claimed_as_new_context_confirmed():
    result=assess_entry_context({"ticker":"AAPL","timestamp":AT.isoformat()},direction="bullish",checked_at=AT)
    assert result["status"]=="not_recorded"


def test_provider_pages_are_followed_and_all_versions_retained():
    provider=Provider()
    def pages(path,params):
        if path!="/v1beta1/news":return provider(path,params)
        if "page_token" not in params:return {"news":[article(1)],"next_page_token":"p2"}
        assert params["page_token"]=="p2"
        return {"news":[article(2)],"next_page_token":None}
    row=snapshot(pages)
    assert row["entry_context"]["news"]["pages"]==2
    assert assess(row)["news_ids"]==["1","2"]
