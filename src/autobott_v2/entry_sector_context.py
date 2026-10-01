"""Provider-labeled sector context, with matched-window relative leadership.

Fund holdings classify only their covered stocks. Sector ETF comparisons are
contextual proxies, not claims of identical industries or profitable alpha.
"""
from __future__ import annotations
from collections.abc import Callable, Mapping
from copy import deepcopy
import csv
from datetime import UTC, datetime, timedelta
from decimal import Decimal
import hashlib
import io
import re
import threading
import urllib.request

from .bar_timing import aware_utc, completed_stock_bars
from .entry_schedule_context import _NoRedirect, digest, NY

HOLDINGS_URL = "https://www.ishares.com/us/products/239714/ishares-russell-3000-etf/latest-holdings.csv"
BENCHMARKS = {"Information Technology":"XLK", "Financials":"XLF", "Health Care":"XLV",
              "Communication":"XLC", "Communication Services":"XLC", "Industrials":"XLI",
              "Consumer Discretionary":"XLY", "Consumer Staples":"XLP", "Energy":"XLE",
              "Real Estate":"XLRE", "Materials":"XLB", "Utilities":"XLU"}
DIVERSIFIED_OR_PROXY = frozenset({"SPY","QQQ","IWM","DIA","VIX","VIXW","VXX","UVXY"})
POLICY = {"id":"matched_sector_leadership.v1", "bars":6, "timeframe":"1Min",
          "holdings_max_age_days":7, "catalog_cache_seconds":21600,
          "relative_requirement":"strictly_same_side_no_fitted_threshold"}
MAX_BYTES = 2_000_000


def fetch_holdings_text():
    request=urllib.request.Request(HOLDINGS_URL,headers={"User-Agent":"AutoBott sector reader","Accept":"text/csv"})
    with urllib.request.build_opener(_NoRedirect).open(request,timeout=5) as response:
        raw=response.read(MAX_BYTES+1)
    if len(raw)>MAX_BYTES:raise ValueError("sector_catalog_size_limit")
    return raw.decode("utf-8-sig")


def parse_holdings(text: str) -> dict:
    if not isinstance(text,str) or len(text.encode())>MAX_BYTES:
        raise ValueError("sector_catalog_size_limit")
    rows=list(csv.reader(io.StringIO(text.lstrip("\ufeff"))))
    dates=[r[1] for r in rows if len(r)>=2 and r[0]=="Fund Holdings as of"]
    headers=[i for i,r in enumerate(rows) if {"Ticker","Sector","Asset Class"}.issubset(r)]
    if not rows or rows[0]!=["iShares Russell 3000 ETF"] or len(dates)!=1 or len(headers)!=1:
        raise ValueError("sector_catalog_source_shape_invalid")
    asof=datetime.strptime(dates[0],"%b %d, %Y").date()
    header=rows[headers[0]]
    if len(header)!=len(set(header)) or len(rows)>5000:
        raise ValueError("sector_catalog_header_or_count_invalid")
    records={}
    for values in rows[headers[0]+1:]:
        if len(values)!=len(header):continue  # Provider footnotes are not holdings.
        row=dict(zip(header,values))
        if row["Asset Class"]!="Equity":continue
        symbol,sector=row["Ticker"],row["Sector"]
        if not re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,14}",symbol):continue
        if sector not in BENCHMARKS:raise ValueError("sector_catalog_unknown_classification")
        record={"symbol":symbol,"sector":sector,"benchmark":BENCHMARKS[sector]}
        if symbol in records and records[symbol]!=record:
            raise ValueError("sector_catalog_conflicting_symbol")
        records[symbol]=record
    if not records:raise ValueError("sector_catalog_no_classifications")
    return {"asof":asof.isoformat(),"records":records,"source_url":HOLDINGS_URL,
            "source_sha256":hashlib.sha256(text.encode()).hexdigest()}


class SectorCatalog:
    """Public source only: one shared immutable-by-copy catalog, no accounts."""
    def __init__(self, fetch: Callable | None = None, now_fn: Callable | None = None):
        self._fetch=fetch or fetch_holdings_text
        self._now=now_fn or (lambda:datetime.now(UTC))
        self._cached=None
        self._lock=threading.RLock()

    def get(self,symbol: str) -> dict:
        with self._lock:
            before=aware_utc(self._now())
            if self._cached is None or not 0 <= (before-aware_utc(self._cached["known_at"])).total_seconds() <= POLICY["catalog_cache_seconds"]:
                parsed=parse_holdings(self._fetch())
                after=aware_utc(self._now())
                if after<before:raise ValueError("sector_catalog_clock_regressed")
                self._cached={**parsed,"known_at":after.isoformat()}
            catalog=self._cached
            classification=catalog["records"].get(symbol)
            if classification is None:raise ValueError("sector_symbol_not_in_observed_holdings")
            if not 0 <= (before.astimezone(NY).date()-datetime.fromisoformat(catalog["asof"]).date()).days <= POLICY["holdings_max_age_days"]:
                raise ValueError("sector_classification_stale_or_future")
            return deepcopy({**classification,**{k:v for k,v in catalog.items() if k!="records"}})


_DEFAULT_CATALOG=SectorCatalog()


class SectorContextSource:
    def __init__(self,fetch: Callable,*,catalog: SectorCatalog | None = None):
        self._fetch=fetch
        self._catalog=catalog or _DEFAULT_CATALOG
        self._bars={}

    def collect(self,context: Mapping) -> dict:
        symbol,signal=context["symbol"],context["signal_symbol"]
        base={"schema_version":"entry_sector.v1","policy":deepcopy(POLICY),"symbol":symbol,
              "signal_symbol":signal,"source_authenticity_verified":False}
        try:
            if symbol in DIVERSIFIED_OR_PROXY or symbol in BENCHMARKS.values() or signal!=symbol:
                return {**base,"status":"not_applicable","reason":"no_single_stock_sector_comparison"}
            classification=self._catalog.get(symbol)
            cutoff=aware_utc(context["cutoff"])
            feed=context["stock_feed"]
            if feed not in {"iex","sip"}:raise ValueError("sector_stock_feed_invalid")
            key=(classification["benchmark"],cutoff.isoformat(),feed)
            if key not in self._bars:
                from .entry_market_context import _pages
                def page(path,params):
                    response=self._fetch(path,params)
                    if not isinstance(response,Mapping) or not isinstance(response.get("bars"),Mapping):
                        raise ValueError("sector_bar_response_invalid")
                    return {"rows":response["bars"].get(classification["benchmark"],[]),
                            "next_page_token":response.get("next_page_token")}
                rows,_=_pages(page,"/v2/stocks/bars",{"symbols":classification["benchmark"],
                    "start":(cutoff-timedelta(minutes=45)).isoformat(),"end":cutoff.isoformat(),
                    "timeframe":"1Min","sort":"asc","limit":"1000","feed":feed,
                    "adjustment":"raw","asof":"-"},"rows",max_pages=2)
                if len(rows)>2000:raise ValueError("sector_bar_count_limit")
                bars=completed_stock_bars(rows,cutoff=cutoff,timeframe="1Min",lookback=30,minimum=6)
                if len(self._bars)>=24:self._bars.clear()
                self._bars[key]=deepcopy(bars)
            return {**base,"status":"observed","classification":classification,
                    "cutoff":cutoff.isoformat(),"stock_feed":feed,"bars":deepcopy(self._bars[key])}
        except Exception as exc:
            return {**base,"status":"unavailable","reason":str(exc) if isinstance(exc,ValueError)
                    and str(exc).startswith("sector_") else type(exc).__name__}


def attach_sector_context(context: dict, source: SectorContextSource) -> dict:
    if context.get("status")!="observed" or context.get("schedule",{}).get("status")!="observed":return context
    return {**context,"schema_version":"entry_context.v3","sector_context":source.collect(context)}


def assess_sector_context(context: Mapping, *, direction: str, own_bars: list, checked_at, receipt) -> dict:
    sector=context.get("sector_context")
    if sector is None:return {"status":"not_recorded","reason":"legacy_context_without_sector"}
    if not isinstance(sector,Mapping) or sector.get("schema_version")!="entry_sector.v1" or sector.get("policy")!=POLICY:
        raise ValueError("sector_schema_or_policy_invalid")
    if direction not in {"bullish","bearish"}:raise ValueError("sector_direction_required")
    symbol=context["symbol"]
    if sector.get("symbol")!=symbol or sector.get("signal_symbol")!=context["signal_symbol"]:
        raise ValueError("sector_symbol_identity_mismatch")
    if symbol in DIVERSIFIED_OR_PROXY or symbol in BENCHMARKS.values() or context["signal_symbol"]!=symbol:
        if sector.get("status")!="not_applicable":raise ValueError("sector_broad_proxy_contract_invalid")
        return {"status":"not_applicable","reason":"no_single_stock_sector_comparison"}
    if sector.get("status")!="observed":return {"status":"unavailable","reason":"sector_source_unavailable"}
    now,received=aware_utc(checked_at),aware_utc(receipt)
    classification=sector["classification"]
    known=aware_utc(classification["known_at"])
    if (classification.get("sector") not in BENCHMARKS or classification.get("symbol")!=symbol or classification.get("source_url")!=HOLDINGS_URL
            or classification.get("benchmark")!=BENCHMARKS.get(classification.get("sector"))):
        raise ValueError("sector_classification_identity_mismatch")
    age=(now.astimezone(NY).date()-datetime.fromisoformat(classification["asof"]).date()).days
    if not known<=received<=now or not 0<=age<=POLICY["holdings_max_age_days"]:
        return {"status":"unavailable","reason":"sector_classification_not_known_or_stale"}
    if sector["stock_feed"] not in {"iex","sip"} or sector["stock_feed"]!=context["stock_feed"] or aware_utc(sector["cutoff"])!=aware_utc(context["cutoff"]):
        raise ValueError("sector_window_or_feed_mismatch")
    if not isinstance(sector.get("bars"),list) or not 6<=len(sector["bars"])<=30:
        raise ValueError("sector_bar_contract_invalid")
    bars=completed_stock_bars(sector["bars"],cutoff=aware_utc(context["cutoff"]),timeframe="1Min",lookback=30,minimum=6)
    by_time={aware_utc(b["timestamp"]):b for b in bars}
    selected=own_bars[-6:]
    stamps=[aware_utc(b["timestamp"]) for b in selected]
    if len(selected)!=6 or any(t not in by_time for t in stamps):
        return {"status":"unavailable","reason":"sector_observation_window_not_matched"}
    if not 0 <= (now-(stamps[-1]+timedelta(minutes=1))).total_seconds()<=90:
        return {"status":"unavailable","reason":"sector_matched_window_stale"}
    stock_start,stock_end=(Decimal(str(selected[i]["close"])) for i in (0,-1))
    peer_start,peer_end=(Decimal(str(by_time[stamps[i]]["close"])) for i in (0,-1))
    stock_return=stock_end/stock_start-1
    peer_return=peer_end/peer_start-1
    # Exact decimal cross-products avoid a numerical tie becoming a signal.
    lead=stock_end*peer_start-peer_end*stock_start
    supported=(lead>0 if direction=="bullish" else lead<0) if direction in {"bullish","bearish"} else False
    return {"status":"confirmed" if supported else "wait","reason":"stock_leads_sector_in_direction" if supported else "stock_not_leading_sector_in_direction",
            "policy_id":POLICY["id"],"sector":classification["sector"],"benchmark":classification["benchmark"],
            "classification_asof":classification["asof"],"classification_known_at":known.isoformat(),
            "window_start":stamps[0].isoformat(),"window_end":stamps[-1].isoformat(),
            "stock_return":float(stock_return),"sector_return":float(peer_return),
            "relative_return":float(stock_return-peer_return),"context_hash":digest(sector),
            "whole_market_ranking_performed":False,"industry_peer_equivalence_verified":False}
