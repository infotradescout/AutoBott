from __future__ import annotations

from datetime import UTC, datetime, timedelta

from autobott_v2.phase1_models import CycleAssessment, CycleStatus, DirectionBias, MarketBar
from autobott_v2.signal_evidence import score_direction_evidence


def _bars(*, start: float, step: float, count: int = 35, last_wick: str | None = None):
    now = datetime(2026, 9, 1, 14, 0, tzinfo=UTC)
    result = []
    price = start
    for index in range(count):
        open_price = price
        close = price + step
        high = max(open_price, close) + abs(step) * 0.5 + 0.05
        low = min(open_price, close) - abs(step) * 0.5 - 0.05
        if index == count - 1 and last_wick == "failed_breakout":
            prior_high = max(bar.high for bar in result[-5:])
            high = prior_high + 0.50
            close = prior_high - 0.25
        elif index == count - 1 and last_wick == "failed_breakdown":
            prior_low = min(bar.low for bar in result[-5:])
            low = prior_low - 0.50
            close = prior_low + 0.25
        result.append(
            MarketBar(
                timestamp=now + timedelta(hours=index),
                open=open_price,
                high=high,
                low=low,
                close=close,
                volume=1000 + index * 10,
            )
        )
        price = close
    return result


def _cycle(**overrides):
    base = dict(
        status=CycleStatus.HIGH,
        trend_score=3,
        bars_since_last_valley=3,
        bars_since_last_peak=3,
        median_valley_to_peak_bars=10,
        median_peak_to_valley_bars=10,
        late_up_cycle=False,
        late_down_cycle=False,
        late_cycle=False,
        bearish_confirmation=False,
        bullish_confirmation=False,
        last_pivot_type="valley",
        reason="test",
        explanation="test",
    )
    base.update(overrides)
    return CycleAssessment(**base)


def test_strong_uptrend_produces_bullish_continuous_score() -> None:
    bars = _bars(start=100.0, step=0.35)

    result, evidence = score_direction_evidence(bars, bars, bars, _cycle())

    assert result.bias is DirectionBias.BULLISH
    assert result.score > 0.22
    assert evidence.medium_momentum > 0
    assert evidence.ema_alignment > 0


def test_strong_downtrend_produces_bearish_continuous_score() -> None:
    bars = _bars(start=120.0, step=-0.35)

    result, evidence = score_direction_evidence(bars, bars, bars, _cycle(trend_score=-3))

    assert result.bias is DirectionBias.BEARISH
    assert result.score < -0.22
    assert evidence.medium_momentum < 0
    assert evidence.ema_alignment < 0


def test_late_cycle_failed_breakout_can_reverse_stale_bullish_momentum() -> None:
    bars = _bars(start=100.0, step=0.25, last_wick="failed_breakout")
    cycle = _cycle(
        late_up_cycle=True,
        late_cycle=True,
        bearish_confirmation=True,
        bars_since_last_valley=9,
        last_pivot_type="valley",
    )

    result, evidence = score_direction_evidence(bars, bars, bars, cycle)

    assert result.bias is DirectionBias.BEARISH
    assert evidence.cycle_adjustment < 0
    assert evidence.reversal_adjustment < 0
    assert result.failed_breakout


def test_late_cycle_failed_breakdown_can_reverse_stale_bearish_momentum() -> None:
    bars = _bars(start=120.0, step=-0.25, last_wick="failed_breakdown")
    cycle = _cycle(
        trend_score=-3,
        late_down_cycle=True,
        late_cycle=True,
        bullish_confirmation=True,
        bars_since_last_peak=9,
        last_pivot_type="peak",
    )

    result, evidence = score_direction_evidence(bars, bars, bars, cycle)

    assert result.bias is DirectionBias.BULLISH
    assert evidence.cycle_adjustment > 0
    assert evidence.reversal_adjustment > 0


def test_flat_market_stays_neutral_instead_of_forcing_a_side() -> None:
    bars = _bars(start=100.0, step=0.0)

    result, evidence = score_direction_evidence(bars, bars, bars, _cycle(trend_score=0))

    assert result.bias is DirectionBias.NEUTRAL
    assert result.score == 0.0
    assert abs(evidence.composite_score) < 0.22
