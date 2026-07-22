from __future__ import annotations

import math
import statistics
from datetime import UTC, date, datetime, timedelta

import pytest

from autobott_v2.phase1_engine import _realized_volatility_from_bars, score_volatility
from autobott_v2.phase1_models import (
    DecisionInput,
    DirectionBias,
    DirectionResult,
    MarketBar,
    MarketContext,
    OptionContractSnapshot,
    OptionType,
)


@pytest.mark.parametrize(
    ("interval", "periods_per_year"),
    [
        (timedelta(minutes=1), 252 * 390),
        (timedelta(minutes=5), 252 * 78),
        (timedelta(hours=1), 252 * 6.5),
        (timedelta(days=1), 252),
    ],
)
def test_realized_volatility_annualizes_actual_bar_interval(interval, periods_per_year) -> None:
    returns = [0.002, -0.001, 0.0015, -0.002, 0.001]
    bars = _bars_from_returns(returns, interval=interval)

    realized = _realized_volatility_from_bars(bars)

    expected = statistics.stdev(returns) * math.sqrt(periods_per_year)
    assert realized == pytest.approx(expected, rel=1e-9)


def test_single_snapshot_iv_is_not_treated_as_percentile_history() -> None:
    bars = _bars_from_returns([0.001, -0.001] * 10, interval=timedelta(hours=1))
    option = OptionContractSnapshot(
        option_symbol="VXX260814C00050000",
        underlying="VXX",
        expiration=date(2026, 8, 14),
        strike=50.0,
        option_type=OptionType.CALL,
        bid=1.0,
        ask=1.1,
        last=1.05,
        volume=100,
        open_interest=500,
        delta=0.5,
        theta=-0.03,
        vega=0.08,
        implied_volatility=0.25,
    )
    decision_input = DecisionInput(
        ticker="VXX",
        timestamp=bars[-1].timestamp,
        market_bars=bars,
        option_chain=[option],
        context=MarketContext(),
        iv_history=[0.25],
    )
    direction = DirectionResult(DirectionBias.BULLISH, 0.5, 0.01, 0.01, 0.5, False, "test")

    result = score_volatility(decision_input, direction)

    assert result.iv_percentile is None


def test_constant_iv_history_uses_midrank_instead_of_false_maximum() -> None:
    bars = _bars_from_returns([0.001, -0.001] * 10, interval=timedelta(hours=1))
    option = OptionContractSnapshot(
        option_symbol="VIXW260814C00024000",
        underlying="VIX",
        expiration=date(2026, 8, 14),
        strike=24.0,
        option_type=OptionType.CALL,
        bid=1.0,
        ask=1.1,
        last=1.05,
        volume=100,
        open_interest=500,
        delta=0.5,
        theta=-0.03,
        vega=0.08,
        implied_volatility=0.25,
    )
    decision_input = DecisionInput(
        ticker="VIX",
        timestamp=bars[-1].timestamp,
        market_bars=bars,
        option_chain=[option],
        context=MarketContext(),
        iv_history=[0.25] * 5,
    )
    direction = DirectionResult(DirectionBias.BULLISH, 0.5, 0.01, 0.01, 0.5, False, "test")

    result = score_volatility(decision_input, direction)

    assert result.iv_percentile == 0.5
    assert result.score == pytest.approx(0.10)


def _bars_from_returns(returns: list[float], *, interval: timedelta) -> list[MarketBar]:
    timestamp = datetime(2026, 7, 1, 14, 0, tzinfo=UTC)
    prices = [100.0]
    for value in returns:
        prices.append(prices[-1] * (1 + value))
    return [
        MarketBar(
            timestamp=timestamp + interval * index,
            open=price,
            high=price * 1.001,
            low=price * 0.999,
            close=price,
            volume=1000,
        )
        for index, price in enumerate(prices)
    ]
