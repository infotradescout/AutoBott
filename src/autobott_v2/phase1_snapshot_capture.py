from __future__ import annotations

import argparse
import hashlib
import json
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import UTC, date, datetime, time as daytime, timedelta, timezone, tzinfo
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

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
    iv_history_limit: int = DEFAULT_IV_HISTORY_LIMIT
    option_chain_max_contracts_per_type: int = 8
    option_chain_max_dte: int = 30
    option_chain_min_dte: int = 1
    max_strike_distance_pct: float = 0.10


class AlpacaMarketDataClient:
    def __init__(self, config: AlpacaReadOnlyConfig | None = None, *, feed: str = "indicative") -> None:
        self.config = config or load_alpaca_read_only_config()
        if not self.config.has_credentials:
            raise ValueError("alpaca_credentials_missing")
        self.data_url = (self.config.data_url or "https://data.alpaca.markets").rstrip("/")
        self.feed = feed

    def get_stock_bars(self, symbols: list[str], *, start: datetime, end: datetime, timeframe: str = "1Min", limit: int = 35) -> dict[str, list[dict[str, Any]]]:
        payload = self._get_json(
            "/v2/stocks/bars",
            {
                "symbols": ",".join(symbols),
                "timeframe": timeframe,
                "start": _isoformat_z(start),
                "end": _isoformat_z(end),
                "limit": str(limit),
                "sort": "asc",
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
        payload = self._get_json(
            f"/v1beta1/options/snapshots/{symbol.upper()}",
            {
                "feed": self.feed,
            },
        )
        snapshots = payload.get("snapshots") or payload.get("option_snapshots") or {}
        return {option_symbol: dict(row) for option_symbol, row in snapshots.items()}

    def _get_json(self, path: str, params: dict[str, str]) -> dict[str, Any]:
        query = urllib.parse.urlencode(params)
        request = urllib.request.Request(
            f"{self.data_url}{path}?{query}",
            headers={
                "APCA-API-KEY-ID": str(self.config.api_key),
                "APCA-API-SECRET-KEY": str(self.config.secret_key),
                "Accept": "application/json",
            },
            method="GET",
        )
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))


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
    as_of_utc = scheduled_market_time.astimezone(UTC)
    trading_date = scheduled_market_time.date().isoformat()
    symbol_dir = Path(corpus_root) / trading_date / symbol
    snapshot_dir = symbol_dir / "snapshots"
    option_quote_dir = symbol_dir / "option_quotes"
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    option_quote_dir.mkdir(parents=True, exist_ok=True)

    context_symbols = _context_symbols(symbol, volatility_proxy_symbol)
    bar_symbols = sorted({symbol, *context_symbols.values()})
    lookback_start = as_of_utc - timedelta(minutes=max(40, rules.lookback_bars + 5))
    bars = data_client.get_stock_bars(bar_symbols, start=lookback_start, end=as_of_utc, limit=rules.lookback_bars)
    quotes = data_client.get_latest_stock_quotes(bar_symbols)
    option_snapshots = data_client.get_option_chain_snapshots(symbol)

    underlying_bars = _normalize_stock_bars(symbol, bars, rules.lookback_bars)
    spy_bars = _normalize_stock_bars(context_symbols["spy"], bars, rules.lookback_bars)
    qqq_bars = _normalize_stock_bars(context_symbols["qqq"], bars, rules.lookback_bars)
    vix_bars = _normalize_stock_bars(context_symbols["vix"], bars, rules.lookback_bars)
    underlying_quote = _normalize_stock_quote(symbol, quotes, fallback_price=underlying_bars[-1]["close"])
    option_chain = _normalize_option_chain(
        symbol=symbol,
        option_snapshots=option_snapshots,
        underlying_price=float(underlying_quote["last"]),
        as_of_date=scheduled_market_time.date(),
        rules=rules,
    )
    iv_history = _load_iv_history(symbol_dir, limit=rules.iv_history_limit)
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
        if strike_distance_pct > rules.max_strike_distance_pct:
            continue
        bid = float(quote.get("bp") or quote.get("bid_price") or 0.0)
        ask = float(quote.get("ap") or quote.get("ask_price") or 0.0)
        if bid < 0 or ask < 0 or (ask > 0 and ask < bid):
            continue
        last = latest_trade.get("p") if latest_trade.get("p") is not None else latest_trade.get("price")
        mid = (bid + ask) / 2 if bid > 0 and ask > 0 else float(last or 0.0)
        spread = max(0.0, ask - bid)
        iv = greeks.get("iv") if greeks.get("iv") is not None else snapshot.get("implied_volatility")
        delta = greeks.get("delta")
        theta = greeks.get("theta")
        vega = greeks.get("vega")
        if iv is None or delta is None or theta is None or vega is None:
            continue
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
                "volume": int(snapshot.get("dailyBar", {}).get("v") or snapshot.get("daily_bar", {}).get("volume") or 0),
                "open_interest": int(snapshot.get("open_interest") or snapshot.get("openInterest") or 0),
                "delta": round(float(delta), 4),
                "theta": round(float(theta), 4),
                "vega": round(float(vega), 4),
                "implied_volatility": round(float(iv), 4),
                "iv_percentile": None,
                "realized_volatility": None,
            }
        )
    return _select_chain_subset(normalized, underlying_price, as_of_date, rules)


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
        tactical = sorted(
            [contract for contract in by_type if 1 <= _dte(contract, as_of_date) <= 3],
            key=lambda contract: (_distance_from_target_delta(contract, 0.55), abs(contract["strike"] - underlying_price)),
        )[: rules.option_chain_max_contracts_per_type // 2]
        rider = sorted(
            [contract for contract in by_type if 7 <= _dte(contract, as_of_date) <= rules.option_chain_max_dte],
            key=lambda contract: (_distance_from_target_delta(contract, 0.45), abs(contract["strike"] - underlying_price)),
        )[: rules.option_chain_max_contracts_per_type // 2]
        selected.extend(tactical)
        selected.extend(rider)
    deduped: dict[str, dict[str, Any]] = {contract["option_symbol"]: contract for contract in selected}
    final = sorted(deduped.values(), key=lambda contract: (contract["expiration"], contract["option_type"], contract["strike"]))
    if not final:
        raise ValueError("empty_option_chain_after_filtering")
    return final


def _load_iv_history(symbol_dir: Path, *, limit: int) -> list[float]:
    snapshot_dir = symbol_dir / "snapshots"
    if not snapshot_dir.exists():
        return []
    history: list[float] = []
    for path in sorted(snapshot_dir.glob("*.json"))[-limit:]:
        payload = _read_snapshot(path)
        ivs = [float(contract["implied_volatility"]) for contract in payload.get("option_chain", []) if contract.get("implied_volatility") is not None]
        if ivs:
            history.append(round(sum(ivs) / len(ivs), 4))
    return history[-limit:]


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
