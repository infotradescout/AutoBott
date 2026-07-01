from __future__ import annotations

import math
from datetime import date, datetime, timedelta, timezone

from autobott_v2.phase1_historical_backfill import (
    black_scholes_price_and_greeks,
    build_synthetic_snapshot,
    run_historical_backfill,
)
from autobott_v2.phase1_snapshot_corpus import load_snapshot_corpus
from autobott_v2.phase1_alpaca_config import AlpacaPaperConfig


def _paper_config() -> AlpacaPaperConfig:
    return AlpacaPaperConfig(
        env="paper",
        api_key="test-key",
        secret_key="test-secret",
        trading_base_url="https://paper-api.alpaca.markets",
        data_base_url="https://data.alpaca.markets",
        live_trading_enabled=False,
        paper_only=True,
        allow_order_placement=False,
    )


class FakeAlpacaClient:
    def __init__(self, series: dict[str, list[float]], start: date) -> None:
        self._series = series
        self._start = start

    def get_stock_bars(self, symbols, *, start, end, timeframe="1Day", limit=5000):
        bars: dict[str, list[dict]] = {}
        for symbol in symbols:
            closes = self._series[symbol.upper()]
            rows = []
            for offset, close in enumerate(closes):
                bar_date = self._start + timedelta(days=offset)
                bar_dt = datetime.combine(bar_date, datetime.min.time(), tzinfo=timezone.utc)
                if bar_dt < start or bar_dt > end:
                    continue
                rows.append(
                    {
                        "t": bar_dt.isoformat(),
                        "o": close - 0.1,
                        "h": close + 0.2,
                        "l": close - 0.2,
                        "c": close,
                        "v": 1_000_000,
                    }
                )
            bars[symbol.upper()] = rows
        return bars


def _trending_series(length: int, start_price: float, step: float) -> list[float]:
    return [round(start_price + index * step, 4) for index in range(length)]


def test_black_scholes_call_price_increases_with_spot() -> None:
    low = black_scholes_price_and_greeks(spot=95, strike=100, dte_days=20, iv=0.25, option_type="call")
    high = black_scholes_price_and_greeks(spot=110, strike=100, dte_days=20, iv=0.25, option_type="call")
    assert high["price"] > low["price"]
    assert 0 <= low["delta"] <= 1
    assert 0 <= high["delta"] <= 1


def test_black_scholes_price_increases_with_volatility() -> None:
    low_vol = black_scholes_price_and_greeks(spot=100, strike=100, dte_days=20, iv=0.15, option_type="call")
    high_vol = black_scholes_price_and_greeks(spot=100, strike=100, dte_days=20, iv=0.45, option_type="call")
    assert high_vol["price"] > low_vol["price"]


def test_black_scholes_put_call_parity_holds_approximately() -> None:
    call = black_scholes_price_and_greeks(spot=100, strike=100, dte_days=30, iv=0.25, option_type="call")
    put = black_scholes_price_and_greeks(spot=100, strike=100, dte_days=30, iv=0.25, option_type="put")
    t = 30 / 365.0
    discounted_strike = 100 * math.exp(-0.045 * t)
    lhs = call["price"] - put["price"]
    rhs = 100 - discounted_strike
    assert abs(lhs - rhs) < 0.05


def test_build_synthetic_snapshot_produces_valid_and_tradeable_snapshot() -> None:
    start = date(2024, 1, 1)
    length = 60
    bars_by_symbol = {
        "AAPL": [
            {"date": start + timedelta(days=i), "open": p - 0.1, "high": p + 0.2, "low": p - 0.2, "close": p, "volume": 1_000_000}
            for i, p in enumerate(_trending_series(length, 150.0, 0.6))
        ],
        "SPY": [
            {"date": start + timedelta(days=i), "open": p - 0.1, "high": p + 0.2, "low": p - 0.2, "close": p, "volume": 1_000_000}
            for i, p in enumerate(_trending_series(length, 470.0, 0.3))
        ],
        "QQQ": [
            {"date": start + timedelta(days=i), "open": p - 0.1, "high": p + 0.2, "low": p - 0.2, "close": p, "volume": 1_000_000}
            for i, p in enumerate(_trending_series(length, 400.0, 0.25))
        ],
        "VIXY": [
            {"date": start + timedelta(days=i), "open": p - 0.1, "high": p + 0.1, "low": p - 0.1, "close": p, "volume": 1_000_000}
            for i, p in enumerate(_trending_series(length, 14.0, -0.02))
        ],
    }
    trading_date = start + timedelta(days=45)

    snapshot = build_synthetic_snapshot(
        symbol="AAPL",
        trading_date=trading_date,
        bars_by_symbol=bars_by_symbol,
        context_symbols={"spy": "SPY", "qqq": "QQQ", "vix": "VIXY"},
    )

    assert snapshot is not None
    assert snapshot["ticker"] == "AAPL"
    assert len(snapshot["market_bars"]) == 35
    assert len(snapshot["option_chain"]) > 0
    assert all(contract["bid"] < contract["ask"] for contract in snapshot["option_chain"])
    assert any(contract["option_type"] == "call" for contract in snapshot["option_chain"])
    assert any(contract["option_type"] == "put" for contract in snapshot["option_chain"])


def test_run_historical_backfill_writes_campaign_ready_corpus(tmp_path) -> None:
    start = date(2024, 1, 1)
    length = 90
    series = {
        "AAPL": _trending_series(length, 150.0, 0.6),
        "SPY": _trending_series(length, 470.0, 0.3),
        "QQQ": _trending_series(length, 400.0, 0.25),
        "VIXY": _trending_series(length, 14.0, -0.02),
    }
    client = FakeAlpacaClient(series, start)
    backfill_start = start + timedelta(days=50)
    backfill_end = start + timedelta(days=55)

    result = run_historical_backfill(
        symbols=["AAPL"],
        start_date=backfill_start,
        end_date=backfill_end,
        corpus_root=tmp_path,
        client=client,
        config=_paper_config(),
    )

    assert result["snapshot_days_written"]["AAPL"] == 6
    corpus_summary = load_snapshot_corpus(tmp_path, symbols=["AAPL"])
    assert corpus_summary["quality"]["campaign_ready"], corpus_summary["quality"]["blocking_reasons"]
    assert corpus_summary["corpus_type"] == "historical_replay"
