from __future__ import annotations

import math
from datetime import date

from .phase1_models import (
    ContractScore,
    DecisionCard,
    DecisionInput,
    DecisionStatus,
    DirectionBias,
    DirectionResult,
    MarketBar,
    OptionContractSnapshot,
    OptionType,
    Phase1Rules,
    RegimeLabel,
    RegimeResult,
    SelectedContract,
    VolatilityResult,
)


def build_decision_card(decision_input: DecisionInput, rules: Phase1Rules | None = None) -> DecisionCard:
    rules = rules or Phase1Rules()
    _validate_numeric_inputs(decision_input)

    if len(decision_input.market_bars) < rules.min_bars:
        regime = RegimeResult(RegimeLabel.RANGE, [RegimeLabel.RANGE], 0.0, "Not enough bars to classify regime.")
        direction = _neutral_direction("Not enough bars to score direction.")
        volatility = VolatilityResult(0.0, None, None, False, decision_input.context.blackout_event, "Not enough bars to score volatility.")
        return _card(decision_input, regime, direction, volatility, None, DecisionStatus.NO_TRADE, "insufficient_market_bars", 0.0)

    regime = classify_regime(decision_input.market_bars, decision_input.context.vix_bars, decision_input.context.spy_bars)
    direction = score_direction(decision_input.market_bars, decision_input.context.spy_bars, decision_input.context.qqq_bars)
    volatility = score_volatility(decision_input, direction)

    if RegimeLabel.RISK_OFF in regime.labels and direction.bias == DirectionBias.BULLISH:
        return _card(decision_input, regime, direction, volatility, None, DecisionStatus.BLOCKED_BY_REGIME, "bullish_options_blocked_in_risk_off", 0.0)

    if direction.bias == DirectionBias.NEUTRAL or abs(direction.score) < rules.min_direction_score:
        return _card(decision_input, regime, direction, volatility, None, DecisionStatus.NO_TRADE, "direction_not_strong_enough", abs(direction.score))

    if volatility.event_risk or volatility.iv_crush_risk or volatility.score < rules.min_volatility_score:
        confidence = _confidence(regime.score, direction.score, volatility.score, None)
        return _card(decision_input, regime, direction, volatility, None, DecisionStatus.BLOCKED_BY_VOLATILITY, "long_option_volatility_unfavorable", confidence)

    contract = select_contract(decision_input, direction, rules)
    if contract is None:
        confidence = _confidence(regime.score, direction.score, volatility.score, None)
        return _card(decision_input, regime, direction, volatility, None, DecisionStatus.BLOCKED_BY_SPREAD, "no_contract_passed_liquidity_greeks_filters", confidence)

    confidence = _confidence(regime.score, direction.score, volatility.score, contract.score)
    status = DecisionStatus.TRADE_CANDIDATE if confidence >= rules.min_confidence else DecisionStatus.NO_TRADE
    reason = None if status == DecisionStatus.TRADE_CANDIDATE else "confidence_below_threshold"
    return _card(decision_input, regime, direction, volatility, SelectedContract.from_contract(contract.contract), status, reason, confidence)


def classify_regime(bars: list[MarketBar], vix_bars: list[MarketBar], spy_bars: list[MarketBar]) -> RegimeResult:
    closes = [bar.close for bar in bars]
    recent = closes[-10:]
    older = closes[-30:-20]
    momentum = _pct_change(older[0], recent[-1]) if older else 0.0
    recent_range = (max(recent) - min(recent)) / recent[-1]
    realized_now = _realized_volatility(closes[-15:])
    realized_then = _realized_volatility(closes[-30:-15])

    labels: list[RegimeLabel] = []
    if abs(momentum) >= 0.015 and recent_range > 0.01:
        labels.append(RegimeLabel.TREND)
    else:
        labels.append(RegimeLabel.RANGE)

    if realized_then > 0 and realized_now > realized_then * 1.20:
        labels.append(RegimeLabel.VOLATILITY_EXPANSION)
    elif realized_then > 0 and realized_now < realized_then * 0.80:
        labels.append(RegimeLabel.VOLATILITY_COMPRESSION)

    risk_on = _risk_context_is_on(spy_bars, vix_bars)
    labels.append(RegimeLabel.RISK_ON if risk_on else RegimeLabel.RISK_OFF)
    primary = RegimeLabel.TREND if RegimeLabel.TREND in labels else RegimeLabel.RANGE
    score = min(1.0, abs(momentum) * 20 + min(recent_range * 10, 0.3))
    explanation = f"{primary.value} regime with {labels[-1].value} context; momentum={momentum:.4f}, realized_vol_now={realized_now:.4f}."
    return RegimeResult(primary=primary, labels=labels, score=score, explanation=explanation)


def score_direction(bars: list[MarketBar], spy_bars: list[MarketBar], qqq_bars: list[MarketBar]) -> DirectionResult:
    closes = [bar.close for bar in bars]
    momentum = _pct_change(closes[-20], closes[-1])
    benchmark_momentum = _benchmark_momentum(spy_bars, qqq_bars)
    relative_strength = momentum - benchmark_momentum
    volume_confirmation = _volume_confirmation(bars)
    failed_breakout = _failed_breakout(bars)

    raw = momentum * 10 + relative_strength * 5 + volume_confirmation * 0.15
    if failed_breakout:
        raw -= 0.35 if raw >= 0 else -0.10
    score = max(-1.0, min(1.0, raw))
    bias = DirectionBias.NEUTRAL
    if score >= 0.20:
        bias = DirectionBias.BULLISH
    elif score <= -0.20:
        bias = DirectionBias.BEARISH

    explanation = (
        f"{bias.value} score from momentum={momentum:.4f}, "
        f"relative_strength={relative_strength:.4f}, volume_confirmation={volume_confirmation:.2f}."
    )
    return DirectionResult(bias, score, momentum, relative_strength, volume_confirmation, failed_breakout, explanation)


def score_volatility(decision_input: DecisionInput, direction: DirectionResult) -> VolatilityResult:
    if not decision_input.option_chain:
        return VolatilityResult(-1.0, None, None, False, decision_input.context.blackout_event, "No option chain snapshot supplied.")

    target_type = OptionType.CALL if direction.bias == DirectionBias.BULLISH else OptionType.PUT
    ivs = [contract.implied_volatility for contract in decision_input.option_chain if contract.option_type == target_type]
    current_iv = sum(ivs) / len(ivs) if ivs else sum(c.implied_volatility for c in decision_input.option_chain) / len(decision_input.option_chain)
    iv_percentile = _percentile_rank(decision_input.iv_history, current_iv) if decision_input.iv_history else None
    realized = _realized_volatility([bar.close for bar in decision_input.market_bars[-20:]])
    iv_realized_ratio = current_iv / realized if realized > 0 else None
    event_risk = decision_input.context.blackout_event
    iv_crush_risk = bool(event_risk and current_iv >= 0.40)

    score = 0.0
    if iv_percentile is not None:
        score += 0.35 if iv_percentile <= 0.60 else -0.35
    if iv_realized_ratio is not None:
        score += 0.25 if iv_realized_ratio <= 1.40 else -0.25
    if event_risk:
        score -= 0.50
    if iv_crush_risk:
        score -= 0.50
    score = max(-1.0, min(1.0, score))
    explanation = f"vol_score={score:.2f}, iv_percentile={iv_percentile}, iv_realized_ratio={iv_realized_ratio}."
    return VolatilityResult(score, iv_percentile, iv_realized_ratio, iv_crush_risk, event_risk, explanation)


def select_contract(decision_input: DecisionInput, direction: DirectionResult, rules: Phase1Rules) -> ContractScore | None:
    option_type = OptionType.CALL if direction.bias == DirectionBias.BULLISH else OptionType.PUT
    underlying = decision_input.market_bars[-1].close
    candidates: list[ContractScore] = []
    for contract in decision_input.option_chain:
        if contract.option_type != option_type:
            continue
        score = _score_contract(contract, underlying, decision_input.timestamp.date(), rules)
        if score is not None:
            candidates.append(score)
    return max(candidates, key=lambda item: item.score) if candidates else None


def _score_contract(contract: OptionContractSnapshot, underlying: float, as_of: date, rules: Phase1Rules) -> ContractScore | None:
    dte = (contract.expiration - as_of).days
    strike_distance = abs(contract.strike - underlying) / underlying
    abs_delta = abs(contract.delta)
    reasons: list[str] = []

    if not (rules.min_dte <= dte <= rules.max_dte):
        return None
    if strike_distance > rules.max_strike_distance_pct:
        return None
    if contract.bid <= 0 or contract.ask <= 0 or contract.ask < contract.bid:
        return None
    if contract.spread_pct > rules.max_spread_pct:
        return None
    if contract.open_interest < rules.min_open_interest or contract.volume < rules.min_contract_volume:
        return None
    if not (rules.min_abs_delta <= abs_delta <= rules.max_abs_delta):
        return None
    if abs(contract.theta) > rules.max_theta_abs or contract.vega < rules.min_vega:
        return None

    reasons.append("expiration_strike_liquidity_greeks_passed")
    liquidity = min(1.0, contract.open_interest / 1000) * 0.30 + min(1.0, contract.volume / 200) * 0.20
    spread = max(0.0, 1 - contract.spread_pct / rules.max_spread_pct) * 0.25
    greek_fit = max(0.0, 1 - abs(abs_delta - 0.45)) * 0.25
    return ContractScore(contract=contract, score=round(liquidity + spread + greek_fit, 4), reasons=reasons)


def _card(
    decision_input: DecisionInput,
    regime: RegimeResult,
    direction: DirectionResult,
    volatility: VolatilityResult,
    selected_contract: SelectedContract | None,
    decision: DecisionStatus,
    blocked_reason: str | None,
    confidence_score: float,
) -> DecisionCard:
    explanation = "; ".join(part for part in [regime.explanation, direction.explanation, volatility.explanation, blocked_reason] if part)
    return DecisionCard(
        ticker=decision_input.ticker,
        timestamp=decision_input.timestamp,
        regime=regime,
        direction=direction,
        volatility=volatility,
        selected_contract=selected_contract,
        decision=decision,
        blocked_reason=blocked_reason,
        confidence_score=round(max(0.0, min(1.0, confidence_score)), 4),
        explanation=explanation,
    )


def _neutral_direction(explanation: str) -> DirectionResult:
    return DirectionResult(DirectionBias.NEUTRAL, 0.0, 0.0, 0.0, 0.0, False, explanation)


def _confidence(regime_score: float, direction_score: float, volatility_score: float, contract_score: float | None) -> float:
    contract_component = contract_score if contract_score is not None else 0.0
    return max(0.0, min(1.0, abs(direction_score) * 0.35 + max(0.0, volatility_score) * 0.25 + regime_score * 0.15 + contract_component * 0.25))


def _pct_change(start: float, end: float) -> float:
    return (end - start) / start if start else 0.0


def _returns(values: list[float]) -> list[float]:
    return [_pct_change(values[index - 1], values[index]) for index in range(1, len(values)) if values[index - 1] > 0]


def _realized_volatility(values: list[float]) -> float:
    returns = _returns(values)
    if len(returns) < 2:
        return 0.0
    mean = sum(returns) / len(returns)
    variance = sum((item - mean) ** 2 for item in returns) / (len(returns) - 1)
    return math.sqrt(variance) * math.sqrt(252)


def _benchmark_momentum(spy_bars: list[MarketBar], qqq_bars: list[MarketBar]) -> float:
    momentums = []
    for bars in (spy_bars, qqq_bars):
        if len(bars) >= 20:
            momentums.append(_pct_change(bars[-20].close, bars[-1].close))
    return sum(momentums) / len(momentums) if momentums else 0.0


def _risk_context_is_on(spy_bars: list[MarketBar], vix_bars: list[MarketBar]) -> bool:
    spy_ok = True
    vix_ok = True
    if len(spy_bars) >= 20:
        spy_ok = spy_bars[-1].close >= sum(bar.close for bar in spy_bars[-20:]) / 20
    if len(vix_bars) >= 5:
        vix_ok = vix_bars[-1].close <= vix_bars[-5].close * 1.10
    return spy_ok and vix_ok


def _volume_confirmation(bars: list[MarketBar]) -> float:
    if len(bars) < 21:
        return 0.0
    average_volume = sum(bar.volume for bar in bars[-21:-1]) / 20
    if average_volume <= 0:
        return 0.0
    return max(-1.0, min(1.0, (bars[-1].volume / average_volume) - 1))


def _failed_breakout(bars: list[MarketBar]) -> bool:
    if len(bars) < 21:
        return False
    prior_high = max(bar.high for bar in bars[-21:-1])
    last = bars[-1]
    return last.high > prior_high and last.close < prior_high


def _percentile_rank(history: list[float], current: float) -> float:
    ordered = [value for value in history if value >= 0]
    if not ordered:
        return 0.0
    below_or_equal = sum(1 for value in ordered if value <= current)
    return round(below_or_equal / len(ordered), 4)


def _validate_numeric_inputs(decision_input: DecisionInput) -> None:
    for bar in decision_input.market_bars + decision_input.context.spy_bars + decision_input.context.qqq_bars + decision_input.context.vix_bars:
        if min(bar.open, bar.high, bar.low, bar.close) <= 0 or bar.volume < 0:
            raise ValueError("Market bars must contain positive OHLC values and non-negative volume.")
    for contract in decision_input.option_chain:
        if contract.bid < 0 or contract.ask < 0 or contract.strike <= 0 or contract.implied_volatility < 0:
            raise ValueError("Option chain snapshot contains invalid quote, strike, or volatility values.")
