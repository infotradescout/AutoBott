from __future__ import annotations

import math
from dataclasses import dataclass
from statistics import median

from .phase1_models import CycleAssessment, DirectionBias, DirectionResult, MarketBar


@dataclass(frozen=True)
class DirectionEvidence:
    medium_momentum: float
    short_momentum: float
    ema_alignment: float
    vwap_location: float
    relative_strength: float
    volume_impulse: float
    cycle_adjustment: float
    reversal_adjustment: float
    composite_score: float

    def to_reason_codes(self) -> list[str]:
        ranked = sorted(
            (
                ("medium_momentum", self.medium_momentum),
                ("short_momentum", self.short_momentum),
                ("ema_alignment", self.ema_alignment),
                ("vwap_location", self.vwap_location),
                ("relative_strength", self.relative_strength),
                ("volume_impulse", self.volume_impulse),
                ("cycle_adjustment", self.cycle_adjustment),
                ("reversal_adjustment", self.reversal_adjustment),
            ),
            key=lambda item: abs(item[1]),
            reverse=True,
        )
        return [f"{name}:{value:+.2f}" for name, value in ranked[:4] if abs(value) >= 0.05]


def score_direction_evidence(
    bars: list[MarketBar],
    spy_bars: list[MarketBar],
    qqq_bars: list[MarketBar],
    cycle: CycleAssessment,
    *,
    neutral_band: float = 0.22,
) -> tuple[DirectionResult, DirectionEvidence]:
    """Score direction from continuous market evidence instead of vote counting.

    Each price-based input is normalized by the instrument's recent hourly
    movement. That lets the same logic operate on VIX, ETFs, and ordinary
    equities without pretending a one-percent move means the same thing in
    every market.
    """

    if len(bars) < 21:
        result = DirectionResult(
            bias=DirectionBias.NEUTRAL,
            score=0.0,
            momentum=0.0,
            relative_strength=0.0,
            volume_confirmation=0.0,
            failed_breakout=False,
            explanation="neutral continuous-evidence score; insufficient bars",
        )
        evidence = DirectionEvidence(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
        return result, evidence

    closes = [bar.close for bar in bars]
    latest = closes[-1]
    atr_pct = max(_atr_percent(bars[-20:]), 0.0025)
    medium_raw = _pct_change(closes[-20], latest)
    short_raw = _pct_change(closes[-5], latest)
    benchmark_raw = _benchmark_momentum(spy_bars, qqq_bars)
    relative_raw = medium_raw - benchmark_raw

    medium = _squash(medium_raw / max(atr_pct * math.sqrt(20), 0.005))
    short = _squash(short_raw / max(atr_pct * math.sqrt(5), 0.003))

    ema9 = _ema(closes, 9)
    ema21 = _ema(closes, 21)
    ema_alignment = _squash(((ema9 - ema21) / latest) / atr_pct)

    vwap = _session_vwap(bars)
    vwap_location = _squash(((latest - vwap) / latest) / atr_pct) if vwap > 0 else 0.0
    relative = _squash(relative_raw / max(atr_pct * math.sqrt(20), 0.005))
    volume = _volume_impulse(bars)

    cycle_adjustment = 0.0
    if cycle.late_up_cycle:
        cycle_adjustment -= 0.35 if cycle.status.value in {"medium", "high"} else 0.15
    if cycle.late_down_cycle:
        cycle_adjustment += 0.35 if cycle.status.value in {"medium", "high"} else 0.15

    failed_breakout = _failed_breakout(bars)
    failed_breakdown = _failed_breakdown(bars)
    reversal_adjustment = 0.0
    if cycle.late_up_cycle and (cycle.bearish_confirmation or failed_breakout):
        reversal_adjustment -= 0.55
    if cycle.late_down_cycle and (cycle.bullish_confirmation or failed_breakdown):
        reversal_adjustment += 0.55

    # Trend/momentum carry most of the directional information. Cycle and
    # reversal evidence are additive context, not veto votes.
    base = (
        medium * 0.28
        + short * 0.15
        + ema_alignment * 0.20
        + vwap_location * 0.12
        + relative * 0.15
        + volume * 0.10
    )
    composite = _clamp(base + cycle_adjustment + reversal_adjustment, -1.0, 1.0)

    if abs(composite) < neutral_band:
        bias = DirectionBias.NEUTRAL
        score = 0.0
        mode = "conflicted"
    elif composite > 0:
        bias = DirectionBias.BULLISH
        score = composite
        mode = "reversal" if reversal_adjustment > 0.25 else "continuation"
    else:
        bias = DirectionBias.BEARISH
        score = composite
        mode = "reversal" if reversal_adjustment < -0.25 else "continuation"

    evidence = DirectionEvidence(
        medium_momentum=round(medium, 6),
        short_momentum=round(short, 6),
        ema_alignment=round(ema_alignment, 6),
        vwap_location=round(vwap_location, 6),
        relative_strength=round(relative, 6),
        volume_impulse=round(volume, 6),
        cycle_adjustment=round(cycle_adjustment, 6),
        reversal_adjustment=round(reversal_adjustment, 6),
        composite_score=round(composite, 6),
    )
    reasons = ", ".join(evidence.to_reason_codes()) or "no dominant evidence"
    result = DirectionResult(
        bias=bias,
        score=score,
        momentum=medium_raw,
        relative_strength=relative_raw,
        volume_confirmation=volume,
        failed_breakout=failed_breakout,
        explanation=(
            f"{bias.value} continuous-evidence {mode}; composite={composite:+.3f}; "
            f"atr_pct={atr_pct:.4f}; {reasons}."
        ),
    )
    return result, evidence


def _pct_change(start: float, end: float) -> float:
    return (end - start) / start if start else 0.0


def _squash(value: float) -> float:
    return math.tanh(value)


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _ema(values: list[float], period: int) -> float:
    if not values:
        return 0.0
    alpha = 2.0 / (period + 1.0)
    result = values[0]
    for value in values[1:]:
        result = alpha * value + (1.0 - alpha) * result
    return result


def _session_vwap(bars: list[MarketBar]) -> float:
    weighted = 0.0
    volume = 0
    for bar in bars:
        typical = (bar.high + bar.low + bar.close) / 3.0
        weighted += typical * max(bar.volume, 0)
        volume += max(bar.volume, 0)
    if volume <= 0:
        return bars[-1].close if bars else 0.0
    return weighted / volume


def _atr_percent(bars: list[MarketBar]) -> float:
    if len(bars) < 2:
        return 0.0
    true_ranges: list[float] = []
    previous_close = bars[0].close
    for bar in bars[1:]:
        true_ranges.append(
            max(
                bar.high - bar.low,
                abs(bar.high - previous_close),
                abs(bar.low - previous_close),
            )
        )
        previous_close = bar.close
    latest = bars[-1].close
    return (sum(true_ranges) / len(true_ranges)) / latest if latest > 0 and true_ranges else 0.0


def _benchmark_momentum(spy_bars: list[MarketBar], qqq_bars: list[MarketBar]) -> float:
    values: list[float] = []
    for bars in (spy_bars, qqq_bars):
        if len(bars) >= 20:
            values.append(_pct_change(bars[-20].close, bars[-1].close))
    return sum(values) / len(values) if values else 0.0


def _volume_impulse(bars: list[MarketBar]) -> float:
    if len(bars) < 11:
        return 0.0
    baseline = median(max(bar.volume, 0) for bar in bars[-11:-1])
    if baseline <= 0:
        return 0.0
    ratio = bars[-1].volume / baseline
    direction = 1.0 if bars[-1].close >= bars[-1].open else -1.0
    return direction * _squash((ratio - 1.0) / 0.75)


def _failed_breakout(bars: list[MarketBar]) -> bool:
    if len(bars) < 6:
        return False
    prior_high = max(bar.high for bar in bars[-6:-1])
    current = bars[-1]
    return current.high > prior_high and current.close < prior_high


def _failed_breakdown(bars: list[MarketBar]) -> bool:
    if len(bars) < 6:
        return False
    prior_low = min(bar.low for bar in bars[-6:-1])
    current = bars[-1]
    return current.low < prior_low and current.close > prior_low
