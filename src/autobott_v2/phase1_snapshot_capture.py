from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import UTC, date, datetime, time as daytime, timedelta, timezone, tzinfo
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .core_runner import CoreRunnerRules, load_core_runner_rules
from .hosted_policy import HOSTED_CAPTURE_OPTION_QUOTE_FILES, is_hosted_paper_runtime, signal_proxy_for
from .options_math import solve_iv_and_greeks
from .phase1_alpaca_client import _merge_option_contract_metadata, _option_chain_request_symbols
from .phase1_config import AlpacaReadOnlyConfig, load_alpaca_read_only_config
from .phase1_snapshot_contract import validate_market_snapshot


DAY_MANIFEST_SCHEMA_VERSION = "phase1_snapshot_day_manifest.v1"
DEFAULT_MARKET_TIMEZONE = "America/New_York"
DEFAULT_VOLATILITY_PROXY_SYMBOL = "VIXY"
DEFAULT_LOOKBACK_BARS = 35
DEFAULT_IV_HISTORY_LIMIT = 60
SNAPSHOT_SCHEMA_VERSION = "phase1.snapshot.v1"


@dataclass(frozen=True)
class CaptureRules:
    lookback_bars: int = DEFAULT_LOOKBACK_BARS
    bar_timeframe: str = "1Min"
    lookback_calendar_days: int = 0
    iv_history_limit: int = DEFAULT_IV_HISTORY_LIMIT
    option_chain_max_contracts_per_type: int = 8
    option_chain_max_dte: int = 45
    option_chain_min_dte: int = 1
    tactical_min_dte: int = 1
    tactical_max_dte: int = 3
    rider_min_dte: int = 7
    rider_max_dte: int = 30
    # Capture far enough OTM to retain a truly convex runner. Primary contract
    # selection still applies its tighter strategy-level strike-distance rule.
    max_strike_distance_pct: float = 0.35


class AlpacaMarketDataClient:
    def __init__(self, config: AlpacaReadOnlyConfig | None = None, *, feed: str = "indicative", stock_feed: str = "iex") -> None:
        self.config = config or load_alpaca_read_only_config()
        if not self.config.has_credentials:
            raise ValueError("alpaca_credentials_missing")
        self.data_url = (self.config.data_url or "https://data.alpaca.markets").rstrip("/")
        self.trading_url = (
            self.config.base_url
            or ("https://paper-api.alpaca.markets" if self.config.paper else "https://api.alpaca.markets")
        ).rstrip("/")
        self.feed = feed
        self.stock_feed = stock_feed
        self._option_contract_metadata_cache: dict[tuple[str, str, str, str], dict[str, dict[str, Any]]] = {}

    def get_stock_bars(self, symbols: list[str], *, start: datetime, end: datetime, timeframe: str = "1Min", limit: int = 35, feed: str | None = None) -> dict[str, list[dict[str, Any]]]:
        payload = self._get_json(
            "/v2/stocks/bars",
            {
                "symbols": ",".join(symbols),
                "timeframe": timeframe,
                "start": _isoformat_z(start),
                "end": _isoformat_z(end),
                "limit": str(limit),
                "sort": "asc",
                "feed": feed or self.stock_feed,
            },
        )
        bars = payload.get("bars", {})
        return {symbol.upper(): list(rows) for symbol, rows in bars.items()}

    def get_latest_stock_quotes(self, symbols: list[str]) -> dict[str, dict[str, Any]]:
        payload = self._get_json(
            "/v2/stocks/quotes/latest",
            {
                "symbols": ",".join(symbols),
            },
        )
        quotes = payload.get("quotes", {})
        return {symbol.upper(): dict(row) for symbol, row in quotes.items()}

    def get_option_chain_snapshots(self, symbol: str) -> dict[str, dict[str, Any]]:
        underlying_symbol, root_symbol = _option_chain_request_symbols(symbol)
        today = datetime.now(UTC).date()
        base_params = {
            "feed": self.feed,
            "limit": "1000",
            "expiration_date_gte": (today + timedelta(days=1)).isoformat(),
            "expiration_date_lte": (today + timedelta(days=45)).isoformat(),
        }
        metadata = self._get_option_contract_metadata(
            underlying_symbol,
            expiration_date_gte=base_params["expiration_date_gte"],
            expiration_date_lte=base_params["expiration_date_lte"],
            root_symbol=root_symbol,
        )
        if root_symbol is not None:
            base_params["root_symbol"] = root_symbol
        snapshots: dict[str, dict[str, Any]] = {}
        page_token: str | None = None
        seen_page_tokens: set[str] = set()
        while True:
            params = dict(base_params)
            if page_token:
                params["page_token"] = page_token
            payload = self._get_json_with_retry(f"/v1beta1/options/snapshots/{underlying_symbol}", params)
            page = payload.get("snapshots") or payload.get("option_snapshots") or {}
            snapshots.update({option_symbol.upper(): dict(row) for option_symbol, row in page.items()})
            next_page_token = payload.get("next_page_token")
            if not next_page_token:
                break
            if next_page_token in seen_page_tokens:
                raise ValueError("option_chain_pagination_token_cycle")
            seen_page_tokens.add(next_page_token)
            page_token = next_page_token
        return {
            option_symbol: _merge_option_contract_metadata(snapshot, metadata[option_symbol])
            for option_symbol, snapshot in snapshots.items()
            if option_symbol in metadata
        }

    def _get_option_contract_metadata(
        self,
        symbol: str,
        *,
        expiration_date_gte: str,
        expiration_date_lte: str,
        root_symbol: str | None = None,
    ) -> dict[str, dict[str, Any]]:
        base_params = {
            "underlying_symbols": symbol.upper(),
            "status": "active",
            "expiration_date_gte": expiration_date_gte,
            "expiration_date_lte": expiration_date_lte,
            "limit": "10000",
        }
        if symbol.upper() in {"VIX", "VIXW"}:
            base_params["style"] = "european"
        if root_symbol is not None:
            base_params["root_symbol"] = root_symbol
        cache_key = (symbol.upper(), expiration_date_gte, expiration_date_lte, root_symbol or "")
        cached = self._option_contract_metadata_cache.get(cache_key)
        if cached is not None:
            return cached
        metadata: dict[str, dict[str, Any]] = {}
        page_token: str | None = None
        seen_page_tokens: set[str] = set()
        while True:
            params = dict(base_params)
            if page_token:
                params["page_token"] = page_token
            payload = self._get_json_with_retry("/v2/options/contracts", params, base_url=self.trading_url)
            for contract in payload.get("option_contracts") or []:
                if contract.get("tradable") is not True:
                    continue
                if symbol.upper() in {"VIX", "VIXW"}:
                    if str(contract.get("style") or "").lower() != "european":
                        continue
                    contract_root = str(contract.get("root_symbol") or "").upper()
                    if root_symbol is not None and contract_root != root_symbol:
                        continue
                    if root_symbol is None and contract_root and contract_root not in {"VIX", "VIXW"}:
                        continue
                option_symbol = str(contract.get("symbol") or "").upper()
                if option_symbol:
                    metadata[option_symbol] = dict(contract)
            next_page_token = payload.get("next_page_token")
            if not next_page_token:
                break
            if next_page_token in seen_page_tokens:
                raise ValueError("option_contract_pagination_token_cycle")
            seen_page_tokens.add(next_page_token)
            page_token = next_page_token
        if not metadata:
            raise ValueError(f"option_contract_metadata_empty:{symbol.upper()}")
        self._option_contract_metadata_cache[cache_key] = metadata
        return metadata

    def _get_json(self, path: str, params: dict[str, str], *, base_url: str | None = None) -> dict[str, Any]:
        query = urllib.parse.urlencode(params)
        request = urllib.request.Request(
            f"{base_url or self.data_url}{path}?{query}",
            headers={
                "APCA-API-KEY-ID": str(self.config.api_key),
                "APCA-API-SECRET-KEY": str(self.config.secret_key),
                "Accept": "application/json",
            },
            method="GET",
        )
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))

    def _get_json_with_retry(
        self,
        path: str,
        params: dict[str, str],
        *,
        base_url: str | None = None,
    ) -> dict[str, Any]:
        for attempt in range(3):
            try:
                return self._get_json(path, params, base_url=base_url)
            except urllib.error.HTTPError as exc:
                if exc.code != 429 and exc.code < 500:
                    raise
                if attempt == 2:
                    raise
            except (urllib.error.URLError, TimeoutError):
                if attempt == 2:
                    raise
            time.sleep(0.25 * (2**attempt))
        raise RuntimeError("alpaca_request_retry_exhausted")


def capture_snapshot_session(
    *,
    symbols: list[str],
    corpus_root: str | Path,
    interval_seconds: int,
    start_time: str | daytime,
    end_time: str | daytime,
    corpus_type: str = "paper_capture",
    trading_date: str | date | None = None,
    market_timezone: str = DEFAULT_MARKET_TIMEZONE,
    volatility_proxy_symbol: str = DEFAULT_VOLATILITY_PROXY_SYMBOL,
    data_client: Any | None = None,
    rules: CaptureRules | None = None,
    sleep_fn: Any = time.sleep,
    now_fn: Any | None = None,
    max_iterations: int | None = None,
) -> dict[str, Any]:
    resolved_rules = rules or CaptureRules()
    client = data_client or AlpacaMarketDataClient()
    initial_now = _current_time(now_fn)
    tz = _market_timezone_info(market_timezone, initial_now.date())
    today_market = initial_now.astimezone(tz).date()
    session_date = _resolve_trading_date(trading_date, today_market)
    scheduled_times = _scheduled_market_times(
        session_date=session_date,
        start_time=_coerce_time(start_time),
        end_time=_coerce_time(end_time),
        interval_seconds=interval_seconds,
        tz=tz,
    )

    if max_iterations is not None:
        scheduled_times = scheduled_times[:max_iterations]
    if not scheduled_times:
        raise ValueError("no_scheduled_capture_intervals")

    written_snapshots: list[str] = []
    finalized_manifests: dict[str, Any] = {}
    for scheduled_market_time in scheduled_times:
        current_utc = _current_time(now_fn).astimezone(UTC)
        if current_utc < scheduled_market_time.astimezone(UTC):
            sleep_fn((scheduled_market_time.astimezone(UTC) - current_utc).total_seconds())
            current_utc = _current_time(now_fn).astimezone(UTC)
        for symbol in symbols:
            written_snapshots.append(
                capture_symbol_snapshot(
                    symbol=symbol,
                    corpus_root=corpus_root,
                    scheduled_market_time=scheduled_market_time,
                    captured_at_utc=current_utc,
                    corpus_type=corpus_type,
                    market_timezone=market_timezone,
                    volatility_proxy_symbol=volatility_proxy_symbol,
                    data_client=client,
                    rules=resolved_rules,
                )
            )

    for symbol in symbols:
        symbol_dir = Path(corpus_root) / session_date.isoformat() / symbol.upper()
        finalized_manifests[symbol.upper()] = write_snapshot_day_manifest(
            symbol_dir,
            trading_date=session_date,
            symbol=symbol.upper(),
            source="alpaca",
            capture_interval_seconds=interval_seconds,
            corpus_type=corpus_type,
        )

    return {
        "schema_version": "phase1_snapshot_capture_run.v1",
        "corpus_root": str(Path(corpus_root)),
        "symbols": [symbol.upper() for symbol in symbols],
        "trading_date": session_date.isoformat(),
        "corpus_type": corpus_type,
        "market_timezone": market_timezone,
        "snapshots_written": len(written_snapshots),
        "snapshot_paths": written_snapshots,
        "manifests": finalized_manifests,
    }


def capture_symbol_snapshot(
    *,
    symbol: str,
    corpus_root: str | Path,
    scheduled_market_time: datetime,
    captured_at_utc: datetime,
    corpus_type: str,
    market_timezone: str,
    volatility_proxy_symbol: str,
    data_client: Any,
    rules: CaptureRules,
) -> str:
    symbol = symbol.upper()
    tz = _market_timezone_info(market_timezone, scheduled_market_time.date())
    market_date = scheduled_market_time.astimezone(tz).date()
    as_of_utc = scheduled_market_time.astimezone(UTC)
    trading_date = market_date.isoformat()
    symbol_dir = Path(corpus_root) / trading_date / symbol
    snapshot_dir = symbol_dir / "snapshots"
    option_quote_dir = symbol_dir / "option_quotes"
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    option_quote_dir.mkdir(parents=True, exist_ok=True)

    signal_symbol = signal_proxy_for(symbol)
    context_symbols = _context_symbols(symbol, volatility_proxy_symbol)
    bar_symbols = sorted({signal_symbol, *context_symbols.values()})
    lookback_start = (
        as_of_utc - timedelta(days=rules.lookback_calendar_days)
        if rules.lookback_calendar_days > 0
        else as_of_utc - timedelta(minutes=max(40, rules.lookback_bars + 5))
    )
    bars: dict[str, list[dict[str, Any]]] = {}
    for bar_symbol in bar_symbols:
        bars[bar_symbol.upper()] = _fetch_stock_bars_with_retries(
            data_client,
            bar_symbol,
            start=lookback_start,
            end=as_of_utc,
            lookback_bars=rules.lookback_bars,
            timeframe=rules.bar_timeframe,
            lookback_calendar_days=rules.lookback_calendar_days,
        )
    quotes = data_client.get_latest_stock_quotes(bar_symbols)
    option_snapshots = data_client.get_option_chain_snapshots(symbol)

    signal_bars = _normalize_stock_bars(signal_symbol, bars, rules.lookback_bars)
    spy_bars = _normalize_stock_bars(context_symbols["spy"], bars, rules.lookback_bars)
    qqq_bars = _normalize_stock_bars(context_symbols["qqq"], bars, rules.lookback_bars)
    vix_bars = _normalize_stock_bars(context_symbols["vix"], bars, rules.lookback_bars)
    signal_quote = _normalize_stock_quote(signal_symbol, quotes, fallback_price=signal_bars[-1]["close"])
    if signal_symbol != symbol:
        underlying_price = _estimate_index_underlying_price(symbol, option_snapshots)
        underlying_bars = _rescale_proxy_bars(signal_bars, target_close=underlying_price)
        underlying_quote = _synthetic_index_quote(
            symbol,
            underlying_price,
            quote_timestamp=signal_quote["quote_timestamp"],
        )
    else:
        underlying_bars = signal_bars
        underlying_quote = signal_quote
    normalized_option_chain = _normalize_option_chain(
        symbol=symbol,
        option_snapshots=option_snapshots,
        underlying_price=float(underlying_quote["last"]),
        as_of_date=market_date,
        rules=rules,
        select_subset=False,
    )
    option_chain = _select_chain_subset(normalized_option_chain, float(underlying_quote["last"]), market_date, rules)
    manual_mirror_chain = _select_manual_mirror_candidates(
        normalized_option_chain,
        max_contract_cost=_manual_mirror_capture_max_contract_cost(),
    )
    iv_history = _load_iv_history(Path(corpus_root), symbol, limit=rules.iv_history_limit)
    if not iv_history:
        iv_history = [round(sum(float(contract["implied_volatility"]) for contract in option_chain) / len(option_chain), 4)]

    payload = {
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "source": {
            "name": "alpaca",
            "environment": "paper" if corpus_type == "paper_capture" else "production_capture",
            "latency_assumption": "retail_api_latency",
            "corpus_type": corpus_type,
        },
        "captured_at": captured_at_utc.astimezone(UTC).isoformat(),
        "market_timezone": market_timezone,
        "timestamp_utc": _isoformat_z(as_of_utc),
        "timestamp_market": scheduled_market_time.astimezone(tz).isoformat(),
        "ticker": symbol,
        "timestamp": _isoformat_z(as_of_utc),
        "underlying_quote": underlying_quote,
        "market_bars": underlying_bars,
        "option_chain": option_chain,
        # Dashboard-only candidates. The execution engine reads option_chain;
        # this separate list makes the affordable Manual Mirror panel reliable
        # without turning its display cap into a paper-execution constraint.
        "manual_mirror_chain": manual_mirror_chain,
        "context": {
            "spy_bars": spy_bars,
            "qqq_bars": qqq_bars,
            "vix_bars": vix_bars,
            "blackout_event": False,
            "event_labels": [],
        },
        "iv_history": iv_history,
        "cycle_profile": {
            "median_valley_to_peak_bars": None,
            "median_peak_to_valley_bars": None,
            "bars_since_last_valley": None,
            "bars_since_last_peak": None,
            "expected_holding_days": None,
            "cycle_confidence": "unknown",
            "last_pivot_type": "unknown",
        },
    }
    validate_market_snapshot(payload)

    filename = f"{scheduled_market_time.astimezone(tz).strftime('%H%M%S')}.json"
    snapshot_path = snapshot_dir / filename
    option_quote_path = option_quote_dir / filename
    snapshot_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    if _capture_option_quote_files_enabled():
        option_quote_path.write_text(
            json.dumps(
                {
                    "schema_version": "phase1_option_quote_capture.v1",
                    "captured_at": captured_at_utc.astimezone(UTC).isoformat(),
                    "timestamp_utc": _isoformat_z(as_of_utc),
                    "timestamp_market": scheduled_market_time.astimezone(tz).isoformat(),
                    "ticker": symbol,
                    "contract_count": len(option_chain),
                    "contracts": option_chain,
                },
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )
    return str(snapshot_path)


def _capture_option_quote_files_enabled() -> bool:
    if is_hosted_paper_runtime():
        return HOSTED_CAPTURE_OPTION_QUOTE_FILES
    value = os.getenv("AUTOBOTT_CAPTURE_OPTION_QUOTE_FILES")
    if value is None:
        return True
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _fetch_stock_bars_with_retries(
    data_client: Any,
    symbol: str,
    *,
    start: datetime,
    end: datetime,
    lookback_bars: int,
    timeframe: str = "1Min",
    lookback_calendar_days: int = 0,
) -> list[dict[str, Any]]:
    symbol_key = symbol.upper()
    best_rows: list[dict[str, Any]] = []
    windows = (
        [
            start,
            end - timedelta(days=max(lookback_calendar_days * 2, 7)),
            end - timedelta(days=max(lookback_calendar_days * 3, 21)),
        ]
        if lookback_calendar_days > 0
        else [
            start,
            end - timedelta(minutes=max(90, lookback_bars * 3)),
            end - timedelta(minutes=max(180, lookback_bars * 6)),
            end - timedelta(minutes=max(390, lookback_bars * 12)),
        ]
    )
    for window_start in windows:
        try:
            kwargs = {
                "start": window_start,
                "end": end,
                "limit": lookback_bars,
            }
            if timeframe != "1Min":
                kwargs["timeframe"] = timeframe
            payload = data_client.get_stock_bars([symbol_key], **kwargs)
        except Exception:
            continue
        rows = list(payload.get(symbol_key, []))
        if len(rows) > len(best_rows):
            best_rows = rows
        if len(rows) >= 30:
            return rows
    return best_rows


def write_snapshot_day_manifest(
    symbol_dir: str | Path,
    *,
    trading_date: str | date | None = None,
    symbol: str | None = None,
    source: str = "alpaca",
    decision_schema_version: str = "phase1_decision_card.v1",
    capture_interval_seconds: int = 60,
    corpus_type: str = "paper_capture",
    data_quality_flags: list[str] | None = None,
) -> dict[str, Any]:
    symbol_path = Path(symbol_dir)
    snapshots_dir = symbol_path / "snapshots"
    option_quotes_dir = symbol_path / "option_quotes"
    snapshot_paths = sorted(snapshots_dir.glob("*.json"))
    option_quote_paths = sorted(option_quotes_dir.glob("*.json"))

    if not snapshot_paths:
        raise ValueError(f"no snapshots found under {snapshots_dir}")

    snapshots = [_read_snapshot(path) for path in snapshot_paths]
    timestamps = sorted(_parse_datetime(snapshot["timestamp"]) for snapshot in snapshots)
    snapshot_schema_versions = {snapshot["schema_version"] for snapshot in snapshots}
    if len(snapshot_schema_versions) != 1:
        raise ValueError("mixed snapshot schema versions within a single day capture are not supported")

    resolved_symbol = symbol or str(snapshots[0]["ticker"])
    resolved_date = _resolve_trading_date(trading_date, timestamps[0].date())
    missing_intervals = _detect_missing_intervals(timestamps, capture_interval_seconds)
    combined_flags = set(data_quality_flags or [])
    if missing_intervals:
        combined_flags.add("missing_intervals_detected")
    if len(option_quote_paths) < len(snapshot_paths):
        combined_flags.add("option_quote_coverage_incomplete")

    first_snapshot = snapshots[0]
    manifest_tz = _market_timezone_info(first_snapshot.get("market_timezone", DEFAULT_MARKET_TIMEZONE), resolved_date)
    manifest = {
        "schema_version": DAY_MANIFEST_SCHEMA_VERSION,
        "corpus_type": corpus_type,
        "symbol": resolved_symbol,
        "trading_date": resolved_date.isoformat(),
        "source": source,
        "snapshot_schema_version": next(iter(snapshot_schema_versions)),
        "decision_schema_version": decision_schema_version,
        "market_timezone": first_snapshot.get("market_timezone", DEFAULT_MARKET_TIMEZONE),
        "capture_start": timestamps[0].astimezone(manifest_tz).time().replace(tzinfo=None).isoformat(),
        "capture_end": timestamps[-1].astimezone(manifest_tz).time().replace(tzinfo=None).isoformat(),
        "capture_interval_seconds": capture_interval_seconds,
        "snapshots_captured": len(snapshot_paths),
        "option_quotes_captured": len(option_quote_paths),
        "missing_intervals": [item.astimezone(manifest_tz).time().replace(tzinfo=None).isoformat() for item in missing_intervals],
        "data_quality_flags": sorted(combined_flags),
        "file_hashes": _file_hashes(symbol_path, snapshot_paths + option_quote_paths),
    }
    manifest_path = symbol_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Capture or finalize a Phase 1 manifest-backed snapshot corpus.")
    parser.add_argument("--symbol-dir", help="Finalize one symbol/day directory containing snapshots/ and option_quotes/.")
    parser.add_argument("--source", default="alpaca", help="Capture source label to write into the manifest.")
    parser.add_argument("--capture-interval-seconds", type=int, default=60, help="Expected snapshot cadence.")
    parser.add_argument("--symbols", nargs="*", help="Symbols to capture into a corpus, for example: SPY QQQ")
    parser.add_argument("--corpus-root", help="Root output directory for captured day/symbol snapshot files.")
    parser.add_argument("--interval-seconds", type=int, default=60, help="Capture cadence for live session capture.")
    parser.add_argument("--start-time", default="09:30", help="Market-session start time in HH:MM.")
    parser.add_argument("--end-time", default="16:00", help="Market-session end time in HH:MM.")
    parser.add_argument("--trading-date", help="Optional YYYY-MM-DD session date. Defaults to the current market date.")
    parser.add_argument("--market-timezone", default=DEFAULT_MARKET_TIMEZONE, help="Timezone used for market session scheduling.")
    parser.add_argument("--volatility-proxy-symbol", default=DEFAULT_VOLATILITY_PROXY_SYMBOL, help="Proxy symbol used for vix_bars context capture.")
    parser.add_argument("--max-iterations", type=int, help="Limit captured intervals for smoke tests or partial sessions.")
    parser.add_argument(
        "--corpus-type",
        default="paper_capture",
        choices=("test_fixture", "paper_capture", "historical_replay", "production_capture"),
        help="Corpus provenance flag used by campaign safety rules.",
    )
    args = parser.parse_args(argv)

    if args.symbol_dir:
        manifest = write_snapshot_day_manifest(
            args.symbol_dir,
            source=args.source,
            capture_interval_seconds=args.capture_interval_seconds,
            corpus_type=args.corpus_type,
        )
        print(json.dumps(manifest, indent=2, sort_keys=True))
        return 0

    if not args.symbols or not args.corpus_root:
        parser.error("either --symbol-dir or both --symbols and --corpus-root are required")

    result = capture_snapshot_session(
        symbols=args.symbols,
        corpus_root=args.corpus_root,
        interval_seconds=args.interval_seconds,
        start_time=args.start_time,
        end_time=args.end_time,
        trading_date=args.trading_date,
        corpus_type=args.corpus_type,
        market_timezone=args.market_timezone,
        volatility_proxy_symbol=args.volatility_proxy_symbol,
        max_iterations=args.max_iterations,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def _current_time(now_fn: Any | None) -> datetime:
    if now_fn is None:
        return datetime.now(UTC)
    return now_fn()


def _scheduled_market_times(
    *,
    session_date: date,
    start_time: daytime,
    end_time: daytime,
    interval_seconds: int,
    tz: ZoneInfo,
) -> list[datetime]:
    if interval_seconds <= 0:
        raise ValueError("interval_seconds_must_be_positive")
    start = datetime.combine(session_date, start_time, tzinfo=tz)
    end = datetime.combine(session_date, end_time, tzinfo=tz)
    if end < start:
        raise ValueError("end_time_before_start_time")
    scheduled: list[datetime] = []
    current = start
    while current <= end:
        scheduled.append(current)
        current += timedelta(seconds=interval_seconds)
    return scheduled


def _normalize_stock_bars(symbol: str, bars: dict[str, list[dict[str, Any]]], lookback_bars: int) -> list[dict[str, Any]]:
    rows = list(bars.get(symbol.upper(), []))
    if len(rows) < 30:
        raise ValueError(f"insufficient_bars_for_symbol:{symbol}")
    normalized = [
        {
            "timestamp": _normalize_timestamp(bar.get("t") or bar.get("timestamp")),
            "open": round(float(bar.get("o") or bar.get("open")), 4),
            "high": round(float(bar.get("h") or bar.get("high")), 4),
            "low": round(float(bar.get("l") or bar.get("low")), 4),
            "close": round(float(bar.get("c") or bar.get("close")), 4),
            "volume": int(bar.get("v") or bar.get("volume") or 0),
        }
        for bar in rows[-lookback_bars:]
    ]
    return normalized


def _normalize_stock_quote(symbol: str, quotes: dict[str, dict[str, Any]], *, fallback_price: float) -> dict[str, Any]:
    quote = quotes.get(symbol.upper())
    if not quote:
        return {
            "symbol": symbol.upper(),
            "bid": round(fallback_price, 4),
            "ask": round(fallback_price, 4),
            "last": round(fallback_price, 4),
            "spread": 0.0,
            "spread_pct": 0.0,
            "quote_timestamp": datetime.now(UTC).isoformat(),
        }
    bid = float(quote.get("bp") or quote.get("bid_price") or fallback_price)
    ask = float(quote.get("ap") or quote.get("ask_price") or fallback_price)
    last = float(quote.get("last") or quote.get("ap") or quote.get("ask_price") or fallback_price)
    mid = (bid + ask) / 2 if bid > 0 and ask > 0 else max(last, fallback_price)
    spread = max(0.0, ask - bid)
    return {
        "symbol": symbol.upper(),
        "bid": round(max(0.01, bid), 4),
        "ask": round(max(0.01, ask), 4),
        "last": round(max(0.01, last), 4),
        "spread": round(spread, 4),
        "spread_pct": round(spread / mid, 4) if mid > 0 else 0.0,
        "quote_timestamp": _normalize_timestamp(quote.get("t") or quote.get("timestamp") or datetime.now(UTC).isoformat()),
    }


def _normalize_option_chain(
    *,
    symbol: str,
    option_snapshots: dict[str, dict[str, Any]],
    underlying_price: float,
    as_of_date: date,
    rules: CaptureRules,
    select_subset: bool = True,
) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for option_symbol, snapshot in option_snapshots.items():
        quote = snapshot.get("latestQuote") or snapshot.get("latest_quote") or snapshot.get("quote") or {}
        greeks = snapshot.get("greeks") or {}
        details = snapshot.get("details") or snapshot.get("option_details") or {}
        latest_trade = snapshot.get("latestTrade") or snapshot.get("latest_trade") or {}
        expiration = details.get("expiration_date") or details.get("expiration") or _expiration_from_occ(option_symbol)
        option_type = details.get("type") or details.get("option_type") or _option_type_from_occ(option_symbol)
        strike = float(details.get("strike_price") or details.get("strike") or _strike_from_occ(option_symbol))
        if not expiration or not option_type:
            continue
        dte = (date.fromisoformat(str(expiration)) - as_of_date).days
        strike_distance_pct = abs(strike - underlying_price) / underlying_price if underlying_price > 0 else 1.0
        if dte < rules.option_chain_min_dte or dte > rules.option_chain_max_dte:
            continue
        if symbol.upper() not in {"VIX", "VIXW"} and strike_distance_pct > rules.max_strike_distance_pct:
            continue
        bid = float(quote.get("bp") or quote.get("bid_price") or 0.0)
        ask = float(quote.get("ap") or quote.get("ask_price") or 0.0)
        if bid < 0 or ask < 0 or (ask > 0 and ask < bid):
            continue
        last = latest_trade.get("p") if latest_trade.get("p") is not None else latest_trade.get("price")
        mid = (bid + ask) / 2 if bid > 0 and ask > 0 else float(last or 0.0)
        spread = max(0.0, ask - bid)
        iv = (
            greeks.get("iv")
            if greeks.get("iv") is not None
            else snapshot.get("implied_volatility", snapshot.get("impliedVolatility"))
        )
        delta = greeks.get("delta")
        theta = greeks.get("theta")
        vega = greeks.get("vega")
        if iv is None or delta is None or theta is None or vega is None:
            if symbol.upper() in {"VIX", "VIXW"}:
                # VIX options are priced from VIX futures term structure, not a
                # stock-style spot Black-Scholes input. Provider Greeks are
                # required; manufacturing them from VIXY would create false
                # contract rankings.
                continue
            # Alpaca's indicative options feed does not reliably return Greeks/IV.
            # Fall back to solving them from the observed market price so contract
            # selection (which targets specific deltas) still has real values.
            solved = solve_iv_and_greeks(
                price=mid,
                s=underlying_price,
                k=strike,
                dte_days=dte,
                option_type=str(option_type).lower(),
            )
            if solved is None:
                continue
            iv, delta, theta, vega = solved
        daily_bar = snapshot.get("dailyBar") or snapshot.get("daily_bar") or {}
        volume_value = daily_bar.get("v") if daily_bar.get("v") is not None else daily_bar.get("volume")
        normalized.append(
            {
                "option_symbol": option_symbol,
                "underlying": symbol.upper(),
                "expiration": str(expiration),
                "strike": round(strike, 4),
                "option_type": str(option_type).lower(),
                "bid": round(bid, 4),
                "ask": round(ask, 4),
                "last": round(float(last), 4) if last is not None else round(mid, 4),
                "spread": round(spread, 4),
                "spread_pct": round(spread / mid, 4) if mid > 0 else 0.0,
                "quote_timestamp": _normalize_timestamp(quote.get("t") or quote.get("timestamp") or latest_trade.get("t") or latest_trade.get("timestamp") or datetime.now(UTC).isoformat()),
                "volume": int(volume_value or 0),
                "volume_available": volume_value is not None,
                "open_interest": int(snapshot.get("open_interest") or snapshot.get("openInterest") or 0),
                "delta": round(float(delta), 4),
                "theta": round(float(theta), 4),
                "vega": round(float(vega), 4),
                "implied_volatility": round(float(iv), 4),
                "iv_percentile": None,
                "realized_volatility": None,
            }
        )
    if select_subset:
        return _select_chain_subset(normalized, underlying_price, as_of_date, rules)
    if not normalized:
        raise ValueError("empty_option_chain_after_normalization")
    return normalized


def _select_chain_subset(
    contracts: list[dict[str, Any]],
    underlying_price: float,
    as_of_date: date,
    rules: CaptureRules,
) -> list[dict[str, Any]]:
    if not contracts:
        raise ValueError("empty_option_chain_after_normalization")
    selected: list[dict[str, Any]] = []
    for option_type in ("call", "put"):
        by_type = [contract for contract in contracts if contract["option_type"] == option_type]
        bucket_size = rules.option_chain_max_contracts_per_type // 2
        tactical = _select_bucket_with_runner_candidates(
            [
                contract
                for contract in by_type
                if rules.tactical_min_dte <= _dte(contract, as_of_date) <= rules.tactical_max_dte
            ],
            target_delta=0.55,
            underlying_price=underlying_price,
            max_contracts=bucket_size,
        )
        rider = _select_bucket_with_runner_candidates(
            [
                contract
                for contract in by_type
                if rules.rider_min_dte
                <= _dte(contract, as_of_date)
                <= min(rules.rider_max_dte, rules.option_chain_max_dte)
            ],
            target_delta=0.45,
            underlying_price=underlying_price,
            max_contracts=bucket_size,
        )
        selected.extend(tactical)
        selected.extend(rider)
    deduped: dict[str, dict[str, Any]] = {contract["option_symbol"]: contract for contract in selected}
    final = sorted(deduped.values(), key=lambda contract: (contract["expiration"], contract["option_type"], contract["strike"]))
    if not final:
        raise ValueError("empty_option_chain_after_filtering")
    return final


def _select_bucket_with_runner_candidates(
    contracts: list[dict[str, Any]],
    *,
    target_delta: float,
    underlying_price: float,
    max_contracts: int,
) -> list[dict[str, Any]]:
    """Keep decision-quality primaries and their same-expiration runners.

    The downstream entry contract requires a cheaper, farther-OTM second leg.
    Keeping only contracts near the primary target delta deletes that leg from
    a dense live chain, so each bucket reserves space for matched candidates.
    """

    if max_contracts <= 0:
        return []
    pair_rules = load_core_runner_rules()
    ranked_cores = sorted(
        contracts,
        key=lambda contract: (
            _distance_from_target_delta(contract, target_delta),
            abs(contract["strike"] - underlying_price),
            float(contract["spread_pct"]) if contract.get("spread_pct") is not None else 1.0,
        ),
    )
    selected: list[dict[str, Any]] = []
    selected_symbols: set[str] = set()
    max_pairs = max_contracts // 2
    pairs_added = 0
    for core in ranked_cores:
        if pairs_added >= max_pairs:
            break
        if not _capture_core_is_eligible(core, pair_rules):
            continue
        runner = _best_capture_runner(core, contracts, rules=pair_rules)
        if runner is None:
            continue
        pair_symbols = {str(core["option_symbol"]), str(runner["option_symbol"])}
        if pair_symbols & selected_symbols:
            continue
        selected.extend((core, runner))
        selected_symbols.update(pair_symbols)
        pairs_added += 1

    for contract in ranked_cores:
        if len(selected) >= max_contracts:
            break
        symbol = str(contract["option_symbol"])
        if symbol in selected_symbols:
            continue
        selected.append(contract)
        selected_symbols.add(symbol)
    return selected[:max_contracts]


def _best_capture_runner(
    core: dict[str, Any],
    contracts: list[dict[str, Any]],
    *,
    rules: CoreRunnerRules,
) -> dict[str, Any] | None:
    core_type = str(core["option_type"])
    core_delta = abs(float(core["delta"]))
    core_ask = float(core["ask"])
    core_strike = float(core["strike"])
    candidates = []
    for candidate in contracts:
        if candidate["option_symbol"] == core["option_symbol"]:
            continue
        if candidate["expiration"] != core["expiration"] or candidate["option_type"] != core_type:
            continue
        bid = float(candidate["bid"])
        ask = float(candidate["ask"])
        strike = float(candidate["strike"])
        spread_pct = float(candidate["spread_pct"]) if candidate.get("spread_pct") is not None else 1.0
        if (
            bid <= 0
            or ask < bid
            or spread_pct > rules.runner_max_spread_pct
            or int(candidate.get("open_interest") or 0) < rules.runner_min_open_interest
            or (
                bool(candidate.get("volume_available", True))
                and int(candidate.get("volume") or 0) < rules.runner_min_volume
            )
            or ask >= core_ask
            or ask > core_ask * rules.runner_max_cost_ratio
        ):
            continue
        if abs(float(candidate["delta"])) >= core_delta:
            continue
        if core_type == "call" and strike <= core_strike:
            continue
        if core_type == "put" and strike >= core_strike:
            continue
        candidates.append(candidate)
    if not candidates:
        return None
    return min(
        candidates,
        key=lambda candidate: (
            -abs(float(candidate["delta"])),
            float(candidate["spread_pct"]) if candidate.get("spread_pct") is not None else 1.0,
            -int(candidate.get("open_interest") or 0),
            -int(candidate.get("volume") or 0),
        ),
    )


def _capture_core_is_eligible(contract: dict[str, Any], rules: CoreRunnerRules) -> bool:
    bid = float(contract["bid"])
    ask = float(contract["ask"])
    spread_pct = float(contract["spread_pct"]) if contract.get("spread_pct") is not None else 1.0
    return (
        0 < bid <= ask
        and spread_pct <= rules.core_max_spread_pct
        and int(contract.get("open_interest") or 0) >= rules.core_min_open_interest
        and (
            not bool(contract.get("volume_available", True))
            or int(contract.get("volume") or 0) >= rules.core_min_volume
        )
        and abs(float(contract["delta"])) >= rules.core_min_abs_delta
    )


def _manual_mirror_capture_max_contract_cost() -> float:
    if is_hosted_paper_runtime():
        return 100.0
    value = os.getenv("AUTOBOTT_MANUAL_MIRROR_MAX_CONTRACT_COST")
    if value is None or not value.strip():
        return 100.0
    return max(0.01, float(value))


def _select_manual_mirror_candidates(
    contracts: list[dict[str, Any]],
    *,
    max_contract_cost: float,
) -> list[dict[str, Any]]:
    """Keep one liquid affordable contract per type and expiration for display."""

    rules = load_core_runner_rules()
    eligible = [
        contract
        for contract in contracts
        if 0 < float(contract["bid"]) <= float(contract["ask"])
        and float(contract["ask"]) * 100 <= max_contract_cost
        and float(contract.get("spread_pct") or 0.0) <= rules.runner_max_spread_pct
        and int(contract.get("open_interest") or 0) >= rules.runner_min_open_interest
        and (
            not bool(contract.get("volume_available", True))
            or int(contract.get("volume") or 0) >= rules.runner_min_volume
        )
    ]
    best_by_expiration: dict[tuple[str, str], dict[str, Any]] = {}
    for contract in eligible:
        key = (str(contract["option_type"]), str(contract["expiration"]))
        current = best_by_expiration.get(key)
        if current is None or _manual_mirror_capture_score(contract) < _manual_mirror_capture_score(current):
            best_by_expiration[key] = contract
    return sorted(
        best_by_expiration.values(),
        key=lambda contract: (str(contract["expiration"]), str(contract["option_type"]), float(contract["strike"])),
    )


def _manual_mirror_capture_score(contract: dict[str, Any]) -> tuple[float, ...]:
    return (
        abs(abs(float(contract["delta"])) - 0.50),
        float(contract.get("spread_pct") or 0.0),
        -float(contract.get("open_interest") or 0),
        -float(contract.get("volume") or 0),
        float(contract["ask"]),
    )


def _load_iv_history(corpus_root: Path, symbol: str, *, limit: int) -> list[float]:
    """Load IV observations across retained trading days, not only today."""

    history: list[float] = []
    paths = sorted(corpus_root.glob(f"*/{symbol.upper()}/snapshots/*.json"))
    for path in paths[-limit:]:
        payload = _read_snapshot(path)
        ivs = [float(contract["implied_volatility"]) for contract in payload.get("option_chain", []) if contract.get("implied_volatility") is not None]
        if ivs:
            history.append(round(sum(ivs) / len(ivs), 4))
    return history[-limit:]


def _estimate_index_underlying_price(symbol: str, option_snapshots: dict[str, dict[str, Any]]) -> float:
    """Estimate index spot from provider deltas when stock quotes do not exist.

    Alpaca's paper index-option rollout exposes contracts before it exposes the
    underlying through the stock quote API. Near-50-delta call/put strikes give
    a bounded spot estimate sufficient for strike-distance filtering. We never
    substitute the VIXY dollar price or synthesize missing option Greeks.
    """

    provider_prices: list[float] = []
    delta_strikes: list[tuple[float, float]] = []
    for snapshot in option_snapshots.values():
        for key in ("underlying_price", "underlyingPrice"):
            value = snapshot.get(key)
            if value is not None and float(value) > 0:
                provider_prices.append(float(value))
        underlying = snapshot.get("underlying_asset") or snapshot.get("underlyingAsset") or {}
        if isinstance(underlying, dict):
            value = underlying.get("price") or underlying.get("last")
            if value is not None and float(value) > 0:
                provider_prices.append(float(value))
        details = snapshot.get("details") or snapshot.get("option_details") or {}
        greeks = snapshot.get("greeks") or {}
        delta = greeks.get("delta")
        strike = details.get("strike_price") or details.get("strike")
        option_type = str(details.get("type") or details.get("option_type") or "").lower()
        if delta is None or strike is None or option_type not in {"call", "put"}:
            continue
        target = 0.50 if option_type == "call" else -0.50
        delta_strikes.append((abs(float(delta) - target), float(strike)))
    if provider_prices:
        ordered = sorted(provider_prices)
        return ordered[len(ordered) // 2]
    if not delta_strikes:
        raise ValueError(f"index_underlying_price_unavailable:{symbol.upper()}")
    nearest = sorted(delta_strikes)[: min(6, len(delta_strikes))]
    strikes = sorted(strike for _, strike in nearest)
    return strikes[len(strikes) // 2]


def _rescale_proxy_bars(bars: list[dict[str, Any]], *, target_close: float) -> list[dict[str, Any]]:
    if not bars or target_close <= 0 or float(bars[-1]["close"]) <= 0:
        raise ValueError("index_signal_proxy_scale_invalid")
    scale = target_close / float(bars[-1]["close"])
    return [
        {
            **bar,
            "open": round(float(bar["open"]) * scale, 4),
            "high": round(float(bar["high"]) * scale, 4),
            "low": round(float(bar["low"]) * scale, 4),
            "close": round(float(bar["close"]) * scale, 4),
        }
        for bar in bars
    ]


def _synthetic_index_quote(symbol: str, price: float, *, quote_timestamp: str) -> dict[str, Any]:
    rounded = round(price, 4)
    return {
        "symbol": symbol.upper(),
        "bid": rounded,
        "ask": rounded,
        "last": rounded,
        "spread": 0.0,
        "spread_pct": 0.0,
        "quote_timestamp": quote_timestamp,
    }


def _context_symbols(symbol: str, volatility_proxy_symbol: str) -> dict[str, str]:
    return {
        "spy": "SPY",
        "qqq": "QQQ",
        "vix": volatility_proxy_symbol.upper(),
    } if symbol.upper() not in {"SPY", "QQQ"} else {
        "spy": "SPY",
        "qqq": "QQQ",
        "vix": volatility_proxy_symbol.upper(),
    }


def _coerce_time(value: str | daytime) -> daytime:
    if isinstance(value, daytime):
        return value
    return datetime.strptime(value, "%H:%M").time()


def _resolve_trading_date(value: str | date | None, fallback: date) -> date:
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return date.fromisoformat(value)
    return fallback


def _detect_missing_intervals(timestamps: list[datetime], interval_seconds: int) -> list[datetime]:
    if len(timestamps) < 2:
        return []
    missing: list[datetime] = []
    expected_delta = timedelta(seconds=interval_seconds)
    unique_timestamps = sorted(set(timestamps))
    for previous, current in zip(unique_timestamps, unique_timestamps[1:]):
        cursor = previous + expected_delta
        while cursor < current:
            missing.append(cursor)
            cursor += expected_delta
    return missing


def _file_hashes(base_dir: Path, paths: list[Path]) -> dict[str, str]:
    hashes: dict[str, str] = {}
    for path in sorted(paths):
        relative = path.relative_to(base_dir).as_posix()
        hashes[relative] = hashlib.sha256(path.read_bytes()).hexdigest()
    return hashes


def _read_snapshot(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    validate_market_snapshot(payload)
    return payload


def _parse_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _normalize_timestamp(value: Any) -> str:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    if isinstance(value, str):
        return _parse_datetime(value).astimezone(UTC).isoformat()
    raise ValueError("timestamp_missing")


def _isoformat_z(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _expiration_from_occ(option_symbol: str) -> str | None:
    if len(option_symbol) < 15:
        return None
    date_code = option_symbol[-15:-9]
    try:
        return datetime.strptime(date_code, "%y%m%d").date().isoformat()
    except ValueError:
        return None


def _strike_from_occ(option_symbol: str) -> float:
    if len(option_symbol) < 8:
        return 0.0
    return int(option_symbol[-8:]) / 1000


def _option_type_from_occ(option_symbol: str) -> str | None:
    if len(option_symbol) < 9:
        return None
    marker = option_symbol[-9]
    if marker == "C":
        return "call"
    if marker == "P":
        return "put"
    return None


def _dte(contract: dict[str, Any], as_of_date: date) -> int:
    return (date.fromisoformat(contract["expiration"]) - as_of_date).days


def _distance_from_target_delta(contract: dict[str, Any], target: float) -> float:
    return abs(abs(float(contract["delta"])) - target)


def _market_timezone_info(name: str, reference_date: date) -> tzinfo:
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError:
        if name == DEFAULT_MARKET_TIMEZONE:
            return _us_eastern_fallback(reference_date)
        raise


def _us_eastern_fallback(reference_date: date) -> tzinfo:
    year = reference_date.year
    dst_start = _nth_weekday_of_month(year, 3, 6, 2)
    dst_end = _nth_weekday_of_month(year, 11, 6, 1)
    is_dst = dst_start <= reference_date < dst_end
    offset_hours = -4 if is_dst else -5
    return timezone(timedelta(hours=offset_hours), name=DEFAULT_MARKET_TIMEZONE)


def _nth_weekday_of_month(year: int, month: int, weekday: int, ordinal: int) -> date:
    current = date(year, month, 1)
    while current.weekday() != weekday:
        current += timedelta(days=1)
    current += timedelta(weeks=max(0, ordinal - 1))
    return current


if __name__ == "__main__":
    raise SystemExit(main())
