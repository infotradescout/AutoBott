"""Bounded read-only news and minute-bar inputs for candidate entry timing.

Provider content is data, never instructions. No account reads or order writes.
The queried feed is not an earnings calendar or all-market news coverage.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import timedelta
import hashlib
import json
from typing import Any
from zoneinfo import ZoneInfo

from .bar_timing import aware_utc, completed_stock_bars

POLICY = {"id": "news_minute_trigger.v1", "timeframe": "1Min", "minimum_bars": 6,
          "breakout_lookback_bars": 3, "volume_lookback_bars": 5,
          "max_bar_age_seconds": 90, "news_lookback_hours": 24}
NY = ZoneInfo("America/New_York")


def digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value,sort_keys=True,allow_nan=False,separators=(",",":")).encode()).hexdigest()


def _pages(fetch: Callable, path: str, params: dict, key: str, *, max_pages: int) -> tuple[list, int]:
    result, tokens, token = [], set(), None
    for page in range(max_pages):
        current = dict(params)
        if token is not None:
            current["page_token"] = token
        payload = fetch(path, current)
        if not isinstance(payload, Mapping) or not isinstance(payload.get(key), list):
            raise ValueError("entry_context_provider_shape_invalid")
        result.extend(payload[key])
        token = payload.get("next_page_token")
        if not token:
            return result, page + 1
        if not isinstance(token, str) or token in tokens or len(token) > 4096:
            raise ValueError("entry_context_pagination_invalid")
        tokens.add(token)
    raise ValueError("entry_context_pagination_incomplete")


def _news(rows: list, symbol: str, start, cutoff) -> tuple[list[dict], dict]:
    if len(rows) > 150:
        raise ValueError("entry_context_news_bound")
    indexed, excluded = {}, {"outside_window": 0, "other_symbol": 0}
    for raw in rows:
        if not isinstance(raw, Mapping):
            raise ValueError("entry_context_news_record_invalid")
        symbols = raw.get("symbols")
        if not isinstance(symbols,list) or any(not isinstance(s,str) for s in symbols):
            raise ValueError("entry_context_news_symbols_invalid")
        if symbol not in {s.upper() for s in symbols}:
            excluded["other_symbol"] += 1
            continue
        created, updated = aware_utc(raw.get("created_at")), aware_utc(raw.get("updated_at"))
        if updated < created:
            raise ValueError("entry_context_news_clock_invalid")
        if created <= cutoff < updated:
            raise ValueError("entry_context_news_historical_version_unavailable")
        if updated > cutoff or created > cutoff or updated < start:
            excluded["outside_window"] += 1
            continue
        identifier, headline, source, url = raw.get("id"), raw.get("headline"), raw.get("source"), raw.get("url")
        if (isinstance(identifier,bool) or not isinstance(identifier,(int,str)) or not str(identifier).strip()
                or not isinstance(headline,str) or not headline.strip() or len(headline)>2000
                or not isinstance(source,str) or not source.strip() or len(source)>200
                or not isinstance(url,str) or len(url)>2000 or not url.startswith(("https://","http://"))):
            raise ValueError("entry_context_news_provenance_invalid")
        item = {"id":str(identifier), "headline":headline, "source":source, "url":url,
                "symbols":sorted(set(symbols)), "created_at":created.isoformat(), "updated_at":updated.isoformat()}
        prior = indexed.get(item["id"])
        if prior is not None and prior != item:
            raise ValueError("entry_context_conflicting_news_version")
        indexed[item["id"]] = item
    return sorted(indexed.values(),key=lambda r:(r["updated_at"],r["id"])), excluded


def fetch_entry_context(fetch: Callable, symbol: str, *, signal_symbol: str, cutoff, stock_feed: str = "iex") -> dict:
    symbol, signal_symbol = symbol.upper(), signal_symbol.upper()
    cutoff = aware_utc(cutoff)
    base = {"schema_version":"entry_context.v1", "policy":dict(POLICY), "symbol":symbol,
            "signal_symbol":signal_symbol, "cutoff":cutoff.isoformat(), "stock_feed":stock_feed,
            "scheduled_event_calendar_status":"not_integrated", "source_authenticity_verified":False}
    try:
        if stock_feed not in {"iex","sip"}:
            raise ValueError("entry_context_stock_feed_invalid")
        start = cutoff-timedelta(hours=POLICY["news_lookback_hours"])
        news_rows, pages = _pages(fetch,"/v1beta1/news",{
            "symbols":symbol,"start":start.isoformat(),"end":cutoff.isoformat(),
            "limit":"50","sort":"desc","include_content":"false","exclude_contentless":"false"},"news",max_pages=3)
        articles, excluded = _news(news_rows,symbol,start,cutoff)
        # Single symbol per request: the provider limit is total, not per symbol.
        def bar_page(path, params):
            payload=fetch(path,params)
            if not isinstance(payload,Mapping) or not isinstance(payload.get("bars"),Mapping):
                raise ValueError("entry_context_bar_response_invalid")
            rows=payload["bars"].get(signal_symbol,[])
            return {"rows":rows,"next_page_token":payload.get("next_page_token")}
        rows, bar_pages = _pages(bar_page,"/v2/stocks/bars",{
            "symbols":signal_symbol,"start":(cutoff-timedelta(minutes=45)).isoformat(),
            "end":cutoff.isoformat(),"timeframe":"1Min","feed":stock_feed,
            "sort":"asc","limit":"1000","adjustment":"raw","asof":"-"},"rows",max_pages=2)
        if len(rows)>2000:
            raise ValueError("entry_context_bar_response_bound")
        minute_bars = completed_stock_bars(rows,cutoff=cutoff,timeframe="1Min",lookback=30,minimum=6)
        context={**base,"status":"observed", "news":{"status":"observed","provider":"alpaca_news",
                    "window_start":start.isoformat(),"window_end":cutoff.isoformat(),
                    "queried_pages_complete":True,"pages":pages,"articles":articles,"excluded":excluded,
                    "all_market_coverage_verified":False},
                 "intraday":{"timestamp_semantics":"interval_start","completed_bars_only":True,
                    "timeframe":"1Min","symbol":signal_symbol,"pages":bar_pages,"bars":minute_bars}}
        if len(json.dumps(context,allow_nan=False).encode())>1_000_000:
            raise ValueError("entry_context_size_bound")
        return context
    except Exception as exc:
        # Never turn request/authentication/pagination failure into zero news.
        return {**base,"status":"unavailable","reason":str(exc) if isinstance(exc,ValueError)
                and str(exc).startswith("entry_context_") else type(exc).__name__}


def assess_entry_context(snapshot: Mapping, *, direction: str, checked_at, bid=None, ask=None) -> dict:
    """Fixed candidate: a minute breakout with volume and post-news confirmation.

    This is an unproven entry hypothesis, not a forecast or a profitable-edge claim.
    Old snapshots without this capsule remain explicitly legacy/unmeasured.
    """
    from statistics import median
    from .hosted_policy import signal_proxy_for
    from .quote_observation import observed_quote_fields
    context=snapshot.get("entry_context")
    if context is None:
        return {"status":"not_recorded", "reason":"legacy_snapshot_without_entry_context"}
    if not isinstance(context,Mapping) or context.get("schema_version") not in {"entry_context.v1", "entry_context.v2", "entry_context.v3"}:
        raise ValueError("entry_context_schema_invalid")
    if context.get("status")!="observed":
        return {"status":"unavailable","reason":"provider_context_unavailable"}
    if context.get("policy")!=POLICY or direction not in {"bullish","bearish"}:
        raise ValueError("entry_context_policy_or_direction_invalid")
    ticker=snapshot["ticker"].upper()
    if context.get("symbol")!=ticker or context.get("signal_symbol")!=signal_proxy_for(ticker):
        raise ValueError("entry_context_symbol_mismatch")
    cutoff=aware_utc(context["cutoff"])
    receipt=aware_utc(context["received_at"])
    now=aware_utc(checked_at)
    if not cutoff<=receipt==aware_utc(snapshot["timestamp"])<=now:
        raise ValueError("entry_context_receipt_clock_mismatch")
    news=context["news"]
    if (news.get("status")!="observed" or news.get("queried_pages_complete") is not True
            or aware_utc(news["window_end"])!=cutoff
            or aware_utc(news["window_start"])!=cutoff-timedelta(hours=24)):
        raise ValueError("entry_context_news_coverage_unknown")
    articles,excluded=_news(news["articles"],ticker,cutoff-timedelta(hours=24),cutoff)
    if any(excluded.values()) or len(articles)!=len(news["articles"]):
        raise ValueError("entry_context_news_versions_invalid")
    stream=context["intraday"]
    if (stream.get("timeframe")!="1Min" or stream.get("symbol")!=context["signal_symbol"]
            or stream.get("timestamp_semantics")!="interval_start" or stream.get("completed_bars_only") is not True):
        raise ValueError("entry_context_minute_bar_contract_invalid")
    if not isinstance(stream.get("bars"),list) or not 6<=len(stream["bars"])<=30:
        raise ValueError("entry_context_minute_bar_count_invalid")
    bars=completed_stock_bars(stream["bars"],cutoff=cutoff,timeframe="1Min",lookback=30,minimum=6)
    recent=bars[-6:]
    times=[aware_utc(r["timestamp"]) for r in recent]
    market_date=now.astimezone(NY).date()
    local_now=now.astimezone(NY)
    if local_now.weekday() >= 5 or not (9,30) <= (local_now.hour,local_now.minute) < (16,0):
        return {"status":"unavailable","reason":"entry_check_outside_regular_session"}
    from .entry_schedule_context import assess_entry_schedule
    if context["schema_version"] in {"entry_context.v2", "entry_context.v3"} and "schedule" not in context:
        raise ValueError("entry_context_native_schedule_required")
    schedule_evidence=assess_entry_schedule(context.get("schedule"), checked_at=now,
        snapshot_received_at=receipt, bar_times=times)
    if schedule_evidence["status"] not in {"not_recorded", "observed_no_listed_event_block"}:
        return {"status":schedule_evidence["status"],"reason":schedule_evidence["reason"],
                "schedule_evidence":schedule_evidence}
    if any(t.astimezone(NY).date()!=market_date or not (9,30)<=(t.astimezone(NY).hour,t.astimezone(NY).minute)<(16,0) for t in times):
        return {"status":"unavailable","reason":"minute_trigger_requires_current_regular_session"}
    if any((b-a).total_seconds()!=60 for a,b in zip(times,times[1:])):
        return {"status":"unavailable","reason":"minute_trigger_observation_gap"}
    age=(now-(times[-1]+timedelta(minutes=1))).total_seconds()
    if not 0<=age<=POLICY["max_bar_age_seconds"]:
        return {"status":"unavailable","reason":"minute_trigger_stale"}
    current=bars[-1]
    level=(max(r["high"] for r in bars[-4:-1]) if direction=="bullish"
           else min(r["low"] for r in bars[-4:-1]))
    baseline=median(r["volume"] for r in bars[-6:-1])
    response={"policy_id":POLICY["id"],"direction":direction,"signal_symbol":context["signal_symbol"],
              "trigger_bar_start":times[-1].isoformat(),"trigger_level":level,
              "trigger_close":current["close"],"last_volume":current["volume"],"baseline_volume":baseline,
              "news_ids":[r["id"] for r in articles],"news_count":len(articles),
              "news_directional_score_used":False,"event_calendar_verified":False,
              "context_hash":digest(context),"bar_age_seconds":age,"schedule_evidence":schedule_evidence}
    latest_version=max((aware_utc(r["updated_at"]) for r in articles),default=None)
    if latest_version is not None and latest_version>times[-1]:
        return {**response,"status":"wait","reason":"post_news_completed_bar_required",
                "latest_news_version_at":latest_version.isoformat()}
    breakout=(current["close"]>level and current["close"]>current["open"] if direction=="bullish"
              else current["close"]<level and current["close"]<current["open"])
    if not breakout:
        return {**response,"status":"wait","reason":"intraday_direction_not_confirmed"}
    if baseline<=0 or current["volume"]<baseline:
        return {**response,"status":"wait","reason":"intraday_volume_not_confirmed"}
    if bid is not None or ask is not None:
        observed_quote_fields({"bp":bid,"ap":ask,"t":now.isoformat()},allow_zero_bid=False)
        if context["signal_symbol"]==ticker:
            holds=(bid>level if direction=="bullish" else ask<level)
            if not holds:
                return {**response,"status":"wait","reason":"current_quote_lost_intraday_trigger"}
        else:
            response["current_quote_trigger_check"]="not_comparable_proxy_units"
    from .entry_sector_context import assess_sector_context
    if context["schema_version"] == "entry_context.v3" and "sector_context" not in context:
        raise ValueError("entry_context_native_sector_required")
    sector_evidence=assess_sector_context(context, direction=direction, own_bars=bars,
                                        checked_at=now, receipt=receipt)
    response["sector_evidence"]=sector_evidence
    if sector_evidence["status"] not in {"not_recorded","not_applicable","confirmed"}:
        return {**response,"status":sector_evidence["status"],"reason":sector_evidence["reason"]}
    return {**response,"status":"confirmed","reason":"completed_minute_breakout_with_volume"}
