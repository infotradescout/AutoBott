from __future__ import annotations

import argparse
import json
import math
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any

from .phase1_alpaca_client import AlpacaPaperClient
from .phase1_alpaca_config import AlpacaPaperConfig, require_alpaca_paper_config
from .options_universe import resolve_symbol_universe
from .phase1_snapshot_capture import SNAPSHOT_SCHEMA_VERSION, write_snapshot_day_manifest
from .phase1_snapshot_contract import validate_market_snapshot
from .runtime_paths import data_root

DEFAULT_CONTEXT_SYMBOLS = {"spy": "SPY", "qqq": "QQQ", "vix": "VIXY"}
DECISION_POINT_TIME = time(15, 30)
DEFAULT_INTRADAY_INTERVAL_MINUTES = 15
RISK_FREE_RATE = 0.045
IV_REALIZED_VOL_MULTIPLIER = 1.10
REALIZED_VOL_WINDOW = 20
LOOKBACK_BARS = 35
IV_HISTORY_WINDOW = 20
TACTICAL_TARGET_DTE = 2
RIDER_TARGET_DTE = 20
SYNTHETIC_CHAIN_DTES = (0, 1, 2, 3, 5, 10, 20, 30)
STRIKE_STEPS_PCT = (-0.10, -0.07, -0.04, -0.02, 0.0, 0.02, 0.04, 0.07, 0.10)
CONTRACT_SPREAD_PCT = 0.06
CONTRACT_VOLUME = 500
CONTRACT_OPEN_INTEREST = 1000
BUFFER_LOOKBACK_DAYS = 150


def historical_backfill_root() -> Path:
    return data_root() / "phase1_historical_corpus"


def _norm_cdf(x: float) -> float:
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def black_scholes_price_and_greeks(
    *,
    spot: float,
    strike: float,
    dte_days: int,
    iv: float,
    option_type: str,
    risk_free_rate: float = RISK_FREE_RATE,
) -> dict[str, float]:
    if spot <= 0 or strike <= 0 or iv <= 0:
        raise ValueError("invalid_black_scholes_inputs")
    t = max(dte_days, 1) / 365.0
    sqrt_t = math.sqrt(t)
    d1 = (math.log(spot / strike) + (risk_free_rate + 0.5 * iv * iv) * t) / (iv * sqrt_t)
    d2 = d1 - iv * sqrt_t
    discounted_strike = strike * math.exp(-risk_free_rate * t)
    pdf_d1 = math.exp(-0.5 * d1 * d1) / math.sqrt(2 * math.pi)
    if option_type == "call":
        price = spot * _norm_cdf(d1) - discounted_strike * _norm_cdf(d2)
        delta = _norm_cdf(d1)
        theta = (-(spot * pdf_d1 * iv) / (2 * sqrt_t) - risk_free_rate * discounted_strike * _norm_cdf(d2)) / 365
    else:
        price = discounted_strike * _norm_cdf(-d2) - spot * _norm_cdf(-d1)
        delta = _norm_cdf(d1) - 1
        theta = (-(spot * pdf_d1 * iv) / (2 * sqrt_t) + risk_free_rate * discounted_strike * _norm_cdf(-d2)) / 365
    vega = spot * pdf_d1 * sqrt_t / 100
    return {"price": max(price, 0.01), "delta": delta, "theta": theta, "vega": vega}


def _annualized_realized_vol(closes: list[float], *, window: int = REALIZED_VOL_WINDOW) -> float:
    sample = closes[-(window + 1):]
    log_returns = [math.log(sample[i] / sample[i - 1]) for i in range(1, len(sample)) if sample[i - 1] > 0 and sample[i] > 0]
    if len(log_returns) < 2:
        return 0.20
    mean = sum(log_returns) / len(log_returns)
    variance = sum((value - mean) ** 2 for value in log_returns) / (len(log_returns) - 1)
    return math.sqrt(variance) * math.sqrt(252)


def _percentile_rank(history: list[float], value: float) -> float:
    if not history:
        return 0.5
    below_or_equal = sum(1 for item in history if item <= value)
    return round(below_or_equal / len(history), 4)


def _occ_symbol(symbol: str, expiration: date, option_type: str, strike: float) -> str:
    type_code = "C" if option_type == "call" else "P"
    strike_code = f"{int(round(strike * 1000)):08d}"
    return f"{symbol.upper()}{expiration.strftime('%y%m%d')}{type_code}{strike_code}"


def synthesize_option_chain(
    *,
    symbol: str,
    spot: float,
    as_of: date,
    iv: float,
    iv_history: list[float],
    quote_timestamp: datetime,
) -> list[dict[str, Any]]:
    iv_percentile = _percentile_rank(iv_history, iv)
    realized_volatility = round(iv / IV_REALIZED_VOL_MULTIPLIER, 4)
    contracts: list[dict[str, Any]] = []
    for option_type in ("call", "put"):
        for dte in SYNTHETIC_CHAIN_DTES:
            expiration = as_of + timedelta(days=dte)
            for pct in STRIKE_STEPS_PCT:
                strike = round(spot * (1 + pct), 2)
                if strike <= 0:
                    continue
                greeks = black_scholes_price_and_greeks(spot=spot, strike=strike, dte_days=dte, iv=iv, option_type=option_type)
                mid = greeks["price"]
                half_spread = max(0.01, round(mid * CONTRACT_SPREAD_PCT / 2, 4))
                bid = round(max(0.01, mid - half_spread), 2)
                ask = round(mid + half_spread, 2)
                contracts.append(
                    {
                        "option_symbol": _occ_symbol(symbol, expiration, option_type, strike),
                        "underlying": symbol.upper(),
                        "expiration": expiration.isoformat(),
                        "strike": strike,
                        "option_type": option_type,
                        "bid": bid,
                        "ask": ask,
                        "last": round(mid, 2),
                        "spread": round(ask - bid, 4),
                        "spread_pct": round((ask - bid) / mid, 4) if mid > 0 else 0.0,
                        "quote_timestamp": quote_timestamp.isoformat(),
                        "volume": CONTRACT_VOLUME,
                        "open_interest": CONTRACT_OPEN_INTEREST,
                        "delta": round(greeks["delta"], 4),
                        "theta": round(greeks["theta"], 4),
                        "vega": round(greeks["vega"], 4),
                        "implied_volatility": round(iv, 4),
                        "iv_percentile": iv_percentile,
                        "realized_volatility": realized_volatility,
                    }
                )
    return contracts


def _fetch_daily_bars(client: Any, symbols: list[str], start: datetime, end: datetime) -> dict[str, list[dict[str, Any]]]:
    raw = client.get_stock_bars(symbols, start=start, end=end, timeframe="1Day", limit=5000)
    normalized: dict[str, list[dict[str, Any]]] = {}
    for symbol in symbols:
        rows = raw.get(symbol.upper(), [])
        parsed = []
        for row in rows:
            raw_timestamp = row.get("t") or row.get("timestamp")
            bar_date = datetime.fromisoformat(str(raw_timestamp).replace("Z", "+00:00")).date()
            parsed.append(
                {
                    "date": bar_date,
                    "open": float(row.get("o") if row.get("o") is not None else row.get("open")),
                    "high": float(row.get("h") if row.get("h") is not None else row.get("high")),
                    "low": float(row.get("l") if row.get("l") is not None else row.get("low")),
                    "close": float(row.get("c") if row.get("c") is not None else row.get("close")),
                    "volume": int(row.get("v") or row.get("volume") or 0),
                }
            )
        normalized[symbol.upper()] = sorted(parsed, key=lambda item: item["date"])
    return normalized


def _fetch_intraday_bars(client: Any, symbols: list[str], start: datetime, end: datetime, *, interval_minutes: int) -> dict[str, list[dict[str, Any]]]:
    raw = client.get_stock_bars(symbols, start=start, end=end, timeframe=f"{interval_minutes}Min", limit=5000)
    normalized: dict[str, list[dict[str, Any]]] = {}
    for symbol in symbols:
        rows = raw.get(symbol.upper(), [])
        parsed = []
        for row in rows:
            raw_timestamp = row.get("t") or row.get("timestamp")
            timestamp = datetime.fromisoformat(str(raw_timestamp).replace("Z", "+00:00")).astimezone(UTC)
            parsed.append(
                {
                    "timestamp": timestamp,
                    "date": timestamp.date(),
                    "open": float(row.get("o") if row.get("o") is not None else row.get("open")),
                    "high": float(row.get("h") if row.get("h") is not None else row.get("high")),
                    "low": float(row.get("l") if row.get("l") is not None else row.get("low")),
                    "close": float(row.get("c") if row.get("c") is not None else row.get("close")),
                    "volume": int(row.get("v") or row.get("volume") or 0),
                }
            )
        normalized[symbol.upper()] = sorted(parsed, key=lambda item: item["timestamp"])
    return normalized


def _index_of_date(bars: list[dict[str, Any]], trading_date: date) -> int | None:
    for index, bar in enumerate(bars):
        if bar["date"] == trading_date:
            return index
    return None


def _index_of_timestamp_or_date(bars: list[dict[str, Any]], timestamp: datetime | None, trading_date: date) -> int | None:
    if timestamp is not None:
        for index, bar in enumerate(bars):
            if bar.get("timestamp") == timestamp:
                return index
    return _index_of_date(bars, trading_date)


def _indices_for_date(bars: list[dict[str, Any]], trading_date: date) -> list[int]:
    return [index for index, bar in enumerate(bars) if bar["date"] == trading_date]


def _bar_payload(bar: dict[str, Any]) -> dict[str, Any]:
    bar_timestamp = bar.get("timestamp") or datetime.combine(bar["date"], DECISION_POINT_TIME, tzinfo=UTC)
    return {
        "timestamp": bar_timestamp.isoformat(),
        "open": round(bar["open"], 4),
        "high": round(bar["high"], 4),
        "low": round(bar["low"], 4),
        "close": round(bar["close"], 4),
        "volume": bar["volume"],
    }


def _window(bars: list[dict[str, Any]], as_of_index: int, *, size: int) -> list[dict[str, Any]] | None:
    if as_of_index + 1 < size:
        return None
    return bars[as_of_index + 1 - size : as_of_index + 1]


def build_synthetic_snapshot(
    *,
    symbol: str,
    trading_date: date,
    bars_by_symbol: dict[str, list[dict[str, Any]]],
    context_symbols: dict[str, str],
    as_of_index: int | None = None,
) -> dict[str, Any] | None:
    symbol_bars = bars_by_symbol.get(symbol.upper(), [])
    resolved_as_of_index = as_of_index if as_of_index is not None else _index_of_date(symbol_bars, trading_date)
    if resolved_as_of_index is None:
        return None
    window = _window(symbol_bars, resolved_as_of_index, size=LOOKBACK_BARS)
    if window is None:
        return None

    context_windows: dict[str, list[dict[str, Any]]] = {}
    for key, context_symbol in context_symbols.items():
        context_bars = bars_by_symbol.get(context_symbol.upper(), [])
        as_of_timestamp = symbol_bars[resolved_as_of_index].get("timestamp")
        context_index = _index_of_timestamp_or_date(context_bars, as_of_timestamp, trading_date)
        if context_index is None:
            return None
        context_window = _window(context_bars, context_index, size=min(LOOKBACK_BARS, context_index + 1))
        if not context_window:
            return None
        context_windows[key] = context_window

    closes = [bar["close"] for bar in window]
    spot = closes[-1]
    iv = round(max(0.05, _annualized_realized_vol(closes) * IV_REALIZED_VOL_MULTIPLIER), 4)

    history_start = max(0, resolved_as_of_index - IV_HISTORY_WINDOW)
    iv_history = []
    for index in range(history_start, resolved_as_of_index + 1):
        history_window = _window(symbol_bars, index, size=LOOKBACK_BARS)
        if history_window is None:
            continue
        history_closes = [bar["close"] for bar in history_window]
        iv_history.append(round(max(0.05, _annualized_realized_vol(history_closes) * IV_REALIZED_VOL_MULTIPLIER), 4))
    if not iv_history:
        iv_history = [iv]

    timestamp = window[-1].get("timestamp") or datetime.combine(trading_date, DECISION_POINT_TIME, tzinfo=UTC)
    option_chain = synthesize_option_chain(
        symbol=symbol.upper(),
        spot=spot,
        as_of=trading_date,
        iv=iv,
        iv_history=iv_history,
        quote_timestamp=timestamp,
    )

    payload = {
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "source": {
            "name": "historical_synthesis",
            "environment": "backtesting",
            "latency_assumption": "retail_api_latency",
            "corpus_type": "historical_replay",
        },
        "captured_at": timestamp.isoformat(),
        "market_timezone": "America/New_York",
        "timestamp_utc": timestamp.isoformat(),
        "timestamp_market": timestamp.isoformat(),
        "ticker": symbol.upper(),
        "timestamp": timestamp.isoformat(),
        "underlying_quote": {
            "symbol": symbol.upper(),
            "bid": round(spot, 4),
            "ask": round(spot, 4),
            "last": round(spot, 4),
            "spread": 0.0,
            "spread_pct": 0.0,
            "quote_timestamp": timestamp.isoformat(),
        },
        "market_bars": [_bar_payload(bar) for bar in window],
        "option_chain": option_chain,
        "context": {
            "spy_bars": [_bar_payload(bar) for bar in context_windows["spy"]],
            "qqq_bars": [_bar_payload(bar) for bar in context_windows["qqq"]],
            "vix_bars": [_bar_payload(bar) for bar in context_windows["vix"]],
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
    return payload


def _write_snapshot_files(corpus_root: Path, symbol: str, trading_date: date, snapshot: dict[str, Any], *, capture_interval_seconds: int = 86400) -> Path:
    symbol_dir = corpus_root / trading_date.isoformat() / symbol.upper()
    snapshots_dir = symbol_dir / "snapshots"
    option_quotes_dir = symbol_dir / "option_quotes"
    snapshots_dir.mkdir(parents=True, exist_ok=True)
    option_quotes_dir.mkdir(parents=True, exist_ok=True)
    snapshot_timestamp = datetime.fromisoformat(str(snapshot["timestamp"]).replace("Z", "+00:00"))
    filename = f"{snapshot_timestamp.strftime('%H%M%S')}.json"
    (snapshots_dir / filename).write_text(json.dumps(snapshot, indent=2, sort_keys=True), encoding="utf-8")
    (option_quotes_dir / filename).write_text(
        json.dumps(
            {
                "schema_version": "phase1_option_quote_capture.v1",
                "captured_at": snapshot["captured_at"],
                "ticker": snapshot["ticker"],
                "contract_count": len(snapshot["option_chain"]),
                "contracts": snapshot["option_chain"],
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    write_snapshot_day_manifest(
        symbol_dir,
        trading_date=trading_date,
        symbol=symbol.upper(),
        source="historical_synthesis",
        capture_interval_seconds=capture_interval_seconds,
        corpus_type="historical_replay",
    )
    return symbol_dir


def run_historical_backfill(
    *,
    symbols: list[str],
    start_date: date,
    end_date: date,
    corpus_root: str | Path | None = None,
    client: Any | None = None,
    config: AlpacaPaperConfig | None = None,
    context_symbols: dict[str, str] | None = None,
    interval_minutes: int | None = None,
) -> dict[str, Any]:
    if not symbols:
        raise ValueError("symbols_required")
    if start_date > end_date:
        raise ValueError("start_date_after_end_date")

    resolved_config = (config or require_alpaca_paper_config()).validate()
    resolved_client = client or AlpacaPaperClient(resolved_config)
    ctx_symbols = context_symbols or DEFAULT_CONTEXT_SYMBOLS
    resolved_root = Path(corpus_root) if corpus_root is not None else historical_backfill_root()

    intraday = interval_minutes is not None
    if intraday and interval_minutes <= 0:
        raise ValueError("interval_minutes_must_be_positive")
    buffer_days = 10 if intraday else BUFFER_LOOKBACK_DAYS
    fetch_start = datetime.combine(start_date - timedelta(days=buffer_days), time.min, tzinfo=UTC)
    fetch_end = datetime.combine(end_date + timedelta(days=1), time.min, tzinfo=UTC)
    all_symbols = sorted({symbol.upper() for symbol in symbols} | {value.upper() for value in ctx_symbols.values()})
    bars_by_symbol = (
        _fetch_intraday_bars(resolved_client, all_symbols, fetch_start, fetch_end, interval_minutes=interval_minutes)
        if intraday
        else _fetch_daily_bars(resolved_client, all_symbols, fetch_start, fetch_end)
    )

    snapshots_written: dict[str, int] = {}
    skipped_days: dict[str, int] = {}
    for symbol in symbols:
        symbol_key = symbol.upper()
        symbol_bars = bars_by_symbol.get(symbol_key, [])
        if len(symbol_bars) < LOOKBACK_BARS:
            raise ValueError(f"insufficient_historical_bars:{symbol_key}")
        trading_dates = sorted({bar["date"] for bar in symbol_bars if start_date <= bar["date"] <= end_date})
        written = 0
        skipped = 0
        for trading_date in trading_dates:
            indices = _indices_for_date(symbol_bars, trading_date) if intraday else [_index_of_date(symbol_bars, trading_date)]
            for index in [item for item in indices if item is not None]:
                snapshot = build_synthetic_snapshot(
                    symbol=symbol_key,
                    trading_date=trading_date,
                    bars_by_symbol=bars_by_symbol,
                    context_symbols=ctx_symbols,
                    as_of_index=index,
                )
                if snapshot is None:
                    skipped += 1
                    continue
                _write_snapshot_files(
                    resolved_root,
                    symbol_key,
                    trading_date,
                    snapshot,
                    capture_interval_seconds=(interval_minutes * 60 if intraday and interval_minutes is not None else 86400),
                )
                written += 1
        snapshots_written[symbol_key] = written
        skipped_days[symbol_key] = skipped

    return {
        "schema_version": "phase1_historical_backfill.v1",
        "corpus_root": str(resolved_root),
        "corpus_type": "historical_replay",
        "symbols": [symbol.upper() for symbol in symbols],
        "context_symbols": ctx_symbols,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "snapshot_days_written": snapshots_written,
        "snapshot_days_skipped": skipped_days,
        "interval_minutes": interval_minutes,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Synthesize a historical Phase 1 snapshot corpus from real Alpaca stock bars plus a modeled option chain.")
    parser.add_argument("--symbols", nargs="+", required=True, help="Ticker list, or TOP_OPTIONS_100 for the full options universe.")
    parser.add_argument("--start", required=True, help="Inclusive YYYY-MM-DD start date.")
    parser.add_argument("--end", required=True, help="Inclusive YYYY-MM-DD end date.")
    parser.add_argument("--interval-minutes", type=int, help="Optional intraday bar interval, for example 30 for 30Min bars.")
    parser.add_argument("--corpus-root", help=f"Output root directory. Defaults to {historical_backfill_root()}.")
    args = parser.parse_args(argv)

    result = run_historical_backfill(
        symbols=resolve_symbol_universe(args.symbols),
        start_date=date.fromisoformat(args.start),
        end_date=date.fromisoformat(args.end),
        corpus_root=args.corpus_root,
        interval_minutes=args.interval_minutes,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
