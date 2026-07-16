from __future__ import annotations

import hashlib
import math
from datetime import date

from .phase1_models import (
    ContractScore,
    CycleAssessment,
    CycleStatus,
    CycleProfile,
    DecisionCard,
    DecisionInput,
    DecisionStatus,
    DirectionBias,
    ExecutionLayer,
    DirectionResult,
    MarketBar,
    OptionContractSnapshot,
    OptionType,
    Phase1Rules,
    PHASE1_DECISION_CARD_SCHEMA_VERSION,
    RegimeLabel,
    RegimeResult,
    SelectedContract,
    TradeSetup,
    VolatilityResult,
)


def build_decision_card(decision_input: DecisionInput, rules: Phase1Rules | None = None) -> DecisionCard:
    rules = rules or Phase1Rules()
    _validate_numeric_inputs(decision_input)

    if len(decision_input.market_bars) < rules.min_bars:
        regime = RegimeResult(RegimeLabel.RANGE, [RegimeLabel.RANGE], 0.0, "Not enough bars to classify regime.")
        direction = _neutral_direction("Not enough bars to score direction.")
        cycle = _empty_cycle_assessment("Not enough bars to score cycle timing.")
        volatility = VolatilityResult(0.0, None, None, False, decision_input.context.blackout_event, "Not enough bars to score volatility.")
        return _card(decision_input, regime, direction, cycle, volatility, None, None, None, TradeSetup.NO_TRADE, ExecutionLayer.NONE, DecisionStatus.NO_TRADE, "insufficient_market_bars", 0.0)

    regime = classify_regime(decision_input.market_bars, decision_input.context.vix_bars, decision_input.context.spy_bars)
    cycle = analyze_cycle(decision_input.market_bars, decision_input.cycle_profile, rules)
    direction = score_direction(decision_input.market_bars, decision_input.context.spy_bars, decision_input.context.qqq_bars, cycle, rules)
    volatility = score_volatility(decision_input, direction)
    setup = determine_trade_setup(direction, cycle)

    if RegimeLabel.RISK_OFF in regime.labels and direction.bias == DirectionBias.BULLISH:
        return _card(decision_input, regime, direction, cycle, volatility, None, None, None, setup, ExecutionLayer.NONE, DecisionStatus.BLOCKED_BY_REGIME, "bullish_options_blocked_in_risk_off", 0.0)

    if direction.bias == DirectionBias.NEUTRAL or abs(direction.score) < rules.min_direction_score:
        return _card(decision_input, regime, direction, cycle, volatility, None, None, None, setup, ExecutionLayer.NONE, DecisionStatus.NO_TRADE, "direction_not_strong_enough", abs(direction.score))

    if volatility.event_risk or volatility.iv_crush_risk or volatility.score < rules.min_volatility_score:
        confidence = _confidence(regime.score, direction.score, volatility.score, None)
        return _card(decision_input, regime, direction, cycle, volatility, None, None, None, setup, ExecutionLayer.NONE, DecisionStatus.BLOCKED_BY_VOLATILITY, "long_option_volatility_unfavorable", confidence)

    tactical_contract = select_contract(decision_input, direction, volatility, rules, ExecutionLayer.TACTICAL, setup, cycle)
    rider_contract = select_contract(decision_input, direction, volatility, rules, ExecutionLayer.RIDER, setup, cycle)
    selected_layer = _selected_layer(tactical_contract, rider_contract)
    contract = _selected_contract_score(selected_layer, tactical_contract, rider_contract)
    if contract is None:
        confidence = _confidence(regime.score, direction.score, volatility.score, None)
        return _card(decision_input, regime, direction, cycle, volatility, None, None, None, setup, ExecutionLayer.NONE, DecisionStatus.BLOCKED_BY_SPREAD, "no_contract_passed_edge_liquidity_risk_reward_filters", confidence)

    confidence = _confidence(regime.score, direction.score, volatility.score, contract.score)
    status = DecisionStatus.TRADE_CANDIDATE if confidence >= rules.min_confidence else DecisionStatus.NO_TRADE
    reason = None if status == DecisionStatus.TRADE_CANDIDATE else "confidence_below_threshold"
    selected_contract = SelectedContract.from_score(contract, rules)
    return _card(
        decision_input,
        regime,
        direction,
        cycle,
        volatility,
        selected_contract,
        SelectedContract.from_score(tactical_contract, rules) if tactical_contract else None,
        SelectedContract.from_score(rider_contract, rules) if rider_contract else None,
        setup,
        selected_layer,
        status,
        reason,
        confidence,
    )


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


def analyze_cycle(bars: list[MarketBar], cycle_profile: CycleProfile, rules: Phase1Rules | None = None) -> CycleAssessment:
    rules = rules or Phase1Rules()
    closes = [bar.close for bar in bars]
    vwap = _session_vwap(bars)
    ema9 = _ema(closes, 9)
    ema21 = _ema(closes, 21)
    trend_score = 0
    trend_score += 1 if closes[-1] >= vwap else -1
    trend_score += 1 if ema9 >= ema21 else -1
    trend_score += 1 if _has_higher_highs_and_lows(bars) else -1 if _has_lower_highs_and_lows(bars) else 0

    valley_bars = cycle_profile.bars_since_last_valley
    peak_bars = cycle_profile.bars_since_last_peak
    valley_median = cycle_profile.median_valley_to_peak_bars
    peak_median = cycle_profile.median_peak_to_valley_bars

    late_up_cycle = bool(valley_bars is not None and valley_median and valley_bars >= math.ceil(valley_median * 0.75))
    late_down_cycle = bool(peak_bars is not None and peak_median and peak_bars >= math.ceil(peak_median * 0.75))
    bearish_confirmation = _failed_breakout(bars) or closes[-1] < vwap
    bullish_confirmation = _failed_breakdown(bars) or closes[-1] > vwap
    status = cycle_profile.cycle_confidence
    late_cycle = late_up_cycle or late_down_cycle
    if status == CycleStatus.UNKNOWN and not any(value is not None for value in (valley_bars, peak_bars, valley_median, peak_median)):
        reason = "No cycle_profile supplied; reversal timing disabled"
    elif status in {CycleStatus.UNKNOWN, CycleStatus.LOW}:
        reason = "Cycle timing available but not reliable enough for reversal-first logic"
    else:
        reason = "Cycle timing available for deterministic late-cycle checks"
    explanation = (
        f"cycle_status={status.value}, trend_score={trend_score}, late_up_cycle={late_up_cycle}, late_down_cycle={late_down_cycle}, "
        f"bearish_confirmation={bearish_confirmation}, bullish_confirmation={bullish_confirmation}."
    )
    return CycleAssessment(
        status=status,
        trend_score=trend_score,
        bars_since_last_valley=valley_bars,
        bars_since_last_peak=peak_bars,
        median_valley_to_peak_bars=valley_median,
        median_peak_to_valley_bars=peak_median,
        late_up_cycle=late_up_cycle,
        late_down_cycle=late_down_cycle,
        late_cycle=late_cycle,
        bearish_confirmation=bearish_confirmation,
        bullish_confirmation=bullish_confirmation,
        last_pivot_type=cycle_profile.last_pivot_type,
        reason=reason,
        explanation=explanation,
    )


def score_direction(
    bars: list[MarketBar],
    spy_bars: list[MarketBar],
    qqq_bars: list[MarketBar],
    cycle: CycleAssessment,
    rules: Phase1Rules | None = None,
) -> DirectionResult:
    rules = rules or Phase1Rules()
    closes = [bar.close for bar in bars]
    momentum = _pct_change(closes[-20], closes[-1])
    benchmark_momentum = _benchmark_momentum(spy_bars, qqq_bars)
    relative_strength = momentum - benchmark_momentum
    volume_confirmation = _volume_confirmation(bars)
    failed_breakout = _failed_breakout(bars)
    failed_breakdown = _failed_breakdown(bars)
    score = 0.0
    bias = DirectionBias.NEUTRAL
    mode = "no-trade"

    if cycle.trend_score >= 2 and not cycle.late_up_cycle:
        bias = DirectionBias.BULLISH
        raw = min(1.0, 0.45 + min(0.25, max(0.0, momentum) * 6) + min(0.15, max(0.0, relative_strength) * 4) + max(0.0, volume_confirmation) * 0.10)
        score = max(0.0, min(1.0, raw))
        mode = "momentum-continuation"
    elif cycle.trend_score <= -2 and not cycle.late_down_cycle:
        bias = DirectionBias.BEARISH
        raw = min(1.0, 0.45 + min(0.25, max(0.0, -momentum) * 6) + min(0.15, max(0.0, -relative_strength) * 4) + max(0.0, volume_confirmation) * 0.10)
        score = -max(0.0, min(1.0, raw))
        mode = "momentum-continuation"
    elif cycle.late_up_cycle and cycle.bearish_confirmation and momentum >= rules.reversal_min_move_pct:
        bias = DirectionBias.BEARISH
        raw = 0.45 + min(0.20, max(0.0, momentum) * 4) + min(0.15, max(0.0, relative_strength) * 4)
        if failed_breakout:
            raw += 0.10
        score = -max(0.0, min(1.0, raw))
        mode = "mean-reversion"
    elif cycle.late_down_cycle and cycle.bullish_confirmation and momentum <= -rules.reversal_min_move_pct:
        bias = DirectionBias.BULLISH
        raw = 0.45 + min(0.20, max(0.0, -momentum) * 4) + min(0.15, max(0.0, -relative_strength) * 4)
        if failed_breakdown:
            raw += 0.10
        score = max(0.0, min(1.0, raw))
        mode = "mean-reversion"

    explanation = (
        f"{bias.value} {mode} score from momentum={momentum:.4f}, "
        f"trend_score={cycle.trend_score}, late_up_cycle={cycle.late_up_cycle}, late_down_cycle={cycle.late_down_cycle}, "
        f"relative_strength={relative_strength:.4f}, "
        f"volume_confirmation={volume_confirmation:.2f}."
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


def determine_trade_setup(direction: DirectionResult, cycle: CycleAssessment) -> TradeSetup:
    if direction.bias == DirectionBias.BULLISH and "momentum-continuation" in direction.explanation:
        return TradeSetup.BULLISH_CONTINUATION
    if direction.bias == DirectionBias.BEARISH and "momentum-continuation" in direction.explanation:
        return TradeSetup.BEARISH_CONTINUATION
    if direction.bias == DirectionBias.BULLISH and cycle.late_down_cycle:
        return TradeSetup.LATE_CYCLE_BULLISH_REVERSAL
    if direction.bias == DirectionBias.BEARISH and cycle.late_up_cycle:
        return TradeSetup.LATE_CYCLE_BEARISH_REVERSAL
    return TradeSetup.NO_TRADE


def select_contract(
    decision_input: DecisionInput,
    direction: DirectionResult,
    volatility: VolatilityResult,
    rules: Phase1Rules,
    layer: ExecutionLayer,
    setup: TradeSetup,
    cycle: CycleAssessment,
) -> ContractScore | None:
    if not _layer_allowed(layer, setup, cycle):
        return None
    option_type = OptionType.CALL if direction.bias == DirectionBias.BULLISH else OptionType.PUT
    underlying = decision_input.market_bars[-1].close
    candidates: list[ContractScore] = []
    for contract in decision_input.option_chain:
        if contract.option_type != option_type:
            continue
        score = _score_contract(contract, underlying, decision_input.timestamp.date(), direction, volatility, rules, layer, decision_input.cycle_profile)
        if score is not None:
            candidates.append(score)
    return max(candidates, key=lambda item: item.score) if candidates else None


def _score_contract(
    contract: OptionContractSnapshot,
    underlying: float,
    as_of: date,
    direction: DirectionResult,
    volatility: VolatilityResult,
    rules: Phase1Rules,
    layer: ExecutionLayer,
    cycle_profile: CycleProfile,
) -> ContractScore | None:
    dte = (contract.expiration - as_of).days
    strike_distance = abs(contract.strike - underlying) / underlying
    abs_delta = abs(contract.delta)
    reasons: list[str] = []

    if contract.bid <= 0 or contract.ask <= 0 or contract.ask < contract.bid:
        return None
    if contract.spread_pct > rules.max_spread_pct:
        return None
    if contract.open_interest < rules.min_open_interest:
        return None
    if contract.volume_available and contract.volume < rules.min_contract_volume:
        return None
    if strike_distance > rules.max_strike_distance_pct:
        return None
    min_abs_delta = rules.intraday_min_abs_delta if layer == ExecutionLayer.TACTICAL else rules.min_abs_delta
    max_abs_delta = rules.intraday_max_abs_delta if layer == ExecutionLayer.TACTICAL else rules.max_abs_delta
    if not (min_abs_delta <= abs_delta <= max_abs_delta):
        return None
    if abs(contract.theta) > rules.max_theta_abs or contract.vega < rules.min_vega:
        return None
    min_dte, max_dte = _target_dte_window(layer, rules, cycle_profile)
    if not (min_dte <= dte <= max_dte):
        return None

    reward_risk_ratio = _planned_reward_risk_ratio(contract, rules)
    if reward_risk_ratio < rules.min_reward_risk_ratio:
        return None

    reasons.append("liquidity_passed")
    if not contract.volume_available:
        reasons.append("volume_unavailable_open_interest_used")
    reasons.append("risk_reward_passed")
    reasons.append("exit_rule_defined")
    reasons.append(f"{layer.value}_window_passed")
    open_interest_liquidity = min(1.0, contract.open_interest / 1000)
    volume_liquidity = min(1.0, contract.volume / 200) if contract.volume_available else open_interest_liquidity
    liquidity = open_interest_liquidity * 0.55 + volume_liquidity * 0.45
    bid_ask_quality = max(0.0, 1 - contract.spread_pct / rules.max_spread_pct)
    edge_fit = _contract_edge_fit(abs_delta, direction, volatility, layer)
    risk_reward = min(1.0, reward_risk_ratio / 1.25)
    exit_quality = _exit_quality(contract, rules)
    layer_fit = 1.0 if layer == ExecutionLayer.TACTICAL else _rider_dte_fit(dte, cycle_profile)
    score = liquidity * 0.25 + edge_fit * 0.25 + risk_reward * 0.20 + bid_ask_quality * 0.15 + exit_quality * 0.05 + layer_fit * 0.10
    return ContractScore(contract=contract, score=round(score, 4), reward_risk_ratio=round(reward_risk_ratio, 4), reasons=reasons)


def _planned_reward_risk_ratio(contract: OptionContractSnapshot, rules: Phase1Rules) -> float:
    round_trip_spread_cost = contract.ask - contract.bid
    planned_reward = contract.mid * rules.target_profit_pct - round_trip_spread_cost
    planned_risk = contract.mid * rules.stop_loss_pct + round_trip_spread_cost
    if planned_reward <= 0 or planned_risk <= 0:
        return 0.0
    return planned_reward / planned_risk


def _contract_edge_fit(abs_delta: float, direction: DirectionResult, volatility: VolatilityResult, layer: ExecutionLayer) -> float:
    direction_strength = min(1.0, abs(direction.score))
    target_delta = 0.55 if layer == ExecutionLayer.TACTICAL else 0.45 + min(0.10, direction_strength * 0.15)
    delta_fit = max(0.0, 1 - abs(abs_delta - target_delta) / 0.30)
    volatility_fit = max(0.0, volatility.score)
    return min(1.0, delta_fit * 0.75 + direction_strength * 0.15 + volatility_fit * 0.10)


def _exit_quality(contract: OptionContractSnapshot, rules: Phase1Rules) -> float:
    theta_quality = max(0.0, 1 - abs(contract.theta) / rules.max_theta_abs)
    vega_quality = min(1.0, contract.vega / max(rules.min_vega * 5, rules.min_vega))
    return theta_quality * 0.70 + vega_quality * 0.30


def _card(
    decision_input: DecisionInput,
    regime: RegimeResult,
    direction: DirectionResult,
    cycle: CycleAssessment,
    volatility: VolatilityResult,
    selected_contract: SelectedContract | None,
    tactical_contract: SelectedContract | None,
    rider_contract: SelectedContract | None,
    trade_setup: TradeSetup,
    execution_layer: ExecutionLayer,
    decision: DecisionStatus,
    blocked_reason: str | None,
    confidence_score: float,
) -> DecisionCard:
    reason_codes = _reason_codes(direction, cycle, selected_contract, tactical_contract, rider_contract, trade_setup, execution_layer, blocked_reason)
    explanation = "; ".join(part for part in [regime.explanation, cycle.explanation, direction.explanation, volatility.explanation, blocked_reason] if part)
    return DecisionCard(
        schema_version=PHASE1_DECISION_CARD_SCHEMA_VERSION,
        decision_id=_decision_id(decision_input, trade_setup),
        ticker=decision_input.ticker,
        timestamp=decision_input.timestamp,
        regime=regime,
        direction=direction,
        cycle=cycle,
        volatility=volatility,
        selected_contract=selected_contract,
        tactical_contract=tactical_contract,
        rider_contract=rider_contract,
        trade_setup=trade_setup,
        execution_layer=execution_layer,
        decision=decision,
        blocked_reason=blocked_reason,
        reason_codes=reason_codes,
        confidence_score=round(max(0.0, min(1.0, confidence_score)), 4),
        explanation=explanation,
    )


def _neutral_direction(explanation: str) -> DirectionResult:
    return DirectionResult(DirectionBias.NEUTRAL, 0.0, 0.0, 0.0, 0.0, False, explanation)


def _empty_cycle_assessment(explanation: str) -> CycleAssessment:
    return CycleAssessment(CycleStatus.UNKNOWN, 0, None, None, None, None, False, False, False, False, False, "unknown", explanation, explanation)


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


def _failed_breakdown(bars: list[MarketBar]) -> bool:
    if len(bars) < 21:
        return False
    prior_low = min(bar.low for bar in bars[-21:-1])
    last = bars[-1]
    return last.low < prior_low and last.close > prior_low


def _session_vwap(bars: list[MarketBar]) -> float:
    total_volume = sum(bar.volume for bar in bars)
    if total_volume <= 0:
        return bars[-1].close
    total_value = sum(((bar.high + bar.low + bar.close) / 3) * bar.volume for bar in bars)
    return total_value / total_volume


def _ema(values: list[float], period: int) -> float:
    if not values:
        return 0.0
    multiplier = 2 / (period + 1)
    ema = values[0]
    for value in values[1:]:
        ema = ((value - ema) * multiplier) + ema
    return ema


def _has_higher_highs_and_lows(bars: list[MarketBar]) -> bool:
    recent = bars[-5:]
    return all(recent[index].high > recent[index - 1].high and recent[index].low > recent[index - 1].low for index in range(1, len(recent)))


def _has_lower_highs_and_lows(bars: list[MarketBar]) -> bool:
    recent = bars[-5:]
    return all(recent[index].high < recent[index - 1].high and recent[index].low < recent[index - 1].low for index in range(1, len(recent)))


def _range_position(values: list[float]) -> float:
    if not values:
        return 0.5
    low = min(values)
    high = max(values)
    if high <= low:
        return 0.5
    return (values[-1] - low) / (high - low)


def _target_dte_window(layer: ExecutionLayer, rules: Phase1Rules, cycle_profile: CycleProfile) -> tuple[int, int]:
    if layer == ExecutionLayer.TACTICAL:
        return rules.intraday_min_dte, rules.intraday_max_dte
    if cycle_profile.expected_holding_days:
        min_dte = max(rules.rider_min_dte, cycle_profile.expected_holding_days * 2)
        max_dte = min(rules.max_dte, cycle_profile.expected_holding_days * 4)
        if min_dte <= max_dte:
            return min_dte, max_dte
    return rules.rider_min_dte, rules.rider_max_dte


def _rider_dte_fit(dte: int, cycle_profile: CycleProfile) -> float:
    if not cycle_profile.expected_holding_days:
        return 0.7
    target = cycle_profile.expected_holding_days * 3
    width = max(2, cycle_profile.expected_holding_days)
    return max(0.0, 1 - abs(dte - target) / width)


def _selected_layer(tactical_contract: ContractScore | None, rider_contract: ContractScore | None) -> ExecutionLayer:
    if tactical_contract is not None and rider_contract is not None:
        return ExecutionLayer.BOTH
    if tactical_contract is not None:
        return ExecutionLayer.TACTICAL
    if rider_contract is not None:
        return ExecutionLayer.RIDER
    return ExecutionLayer.NONE


def _selected_contract_score(
    layer: ExecutionLayer,
    tactical_contract: ContractScore | None,
    rider_contract: ContractScore | None,
) -> ContractScore | None:
    if layer in {ExecutionLayer.TACTICAL, ExecutionLayer.BOTH}:
        return tactical_contract
    if layer == ExecutionLayer.RIDER:
        return rider_contract
    return None


def _layer_allowed(layer: ExecutionLayer, setup: TradeSetup, cycle: CycleAssessment) -> bool:
    if layer == ExecutionLayer.TACTICAL:
        if setup in {TradeSetup.LATE_CYCLE_BULLISH_REVERSAL, TradeSetup.LATE_CYCLE_BEARISH_REVERSAL}:
            return cycle.status in {CycleStatus.MEDIUM, CycleStatus.HIGH}
        return True
    if layer == ExecutionLayer.RIDER:
        if setup in {TradeSetup.LATE_CYCLE_BULLISH_REVERSAL, TradeSetup.LATE_CYCLE_BEARISH_REVERSAL}:
            return cycle.status in {CycleStatus.MEDIUM, CycleStatus.HIGH}
        if setup in {TradeSetup.BULLISH_CONTINUATION, TradeSetup.BEARISH_CONTINUATION}:
            return cycle.status in {CycleStatus.MEDIUM, CycleStatus.HIGH} or abs(cycle.trend_score) >= 3
    return False


def _decision_id(decision_input: DecisionInput, trade_setup: TradeSetup) -> str:
    seed = "|".join(
        [
            decision_input.ticker,
            decision_input.timestamp.isoformat(),
            trade_setup.value,
            str(decision_input.market_bars[-1].close if decision_input.market_bars else "missing_close"),
        ]
    )
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()[:16]


def _reason_codes(
    direction: DirectionResult,
    cycle: CycleAssessment,
    selected_contract: SelectedContract | None,
    tactical_contract: SelectedContract | None,
    rider_contract: SelectedContract | None,
    trade_setup: TradeSetup,
    execution_layer: ExecutionLayer,
    blocked_reason: str | None,
) -> list[str]:
    codes: list[str] = []
    if cycle.trend_score >= 1:
        codes.append("trend_above_vwap")
    else:
        codes.append("trend_below_vwap")
    if cycle.trend_score >= 2:
        codes.append("ema_bull_stack")
    elif cycle.trend_score <= -2:
        codes.append("ema_bear_stack")
    if cycle.trend_score >= 3:
        codes.append("higher_high_higher_low")
    elif cycle.trend_score <= -3:
        codes.append("lower_high_lower_low")
    if cycle.status == CycleStatus.UNKNOWN:
        codes.append("cycle_context_missing")
    if cycle.late_up_cycle:
        codes.append("late_up_cycle_detected")
    if cycle.late_down_cycle:
        codes.append("late_down_cycle_detected")
    if trade_setup in {TradeSetup.LATE_CYCLE_BULLISH_REVERSAL, TradeSetup.LATE_CYCLE_BEARISH_REVERSAL}:
        if cycle.status in {CycleStatus.MEDIUM, CycleStatus.HIGH}:
            codes.append("cycle_confidence_reversal_allowed")
        else:
            codes.append("cycle_confidence_blocks_reversal")
        if cycle.bearish_confirmation or cycle.bullish_confirmation:
            codes.append("reversal_confirmation_present")
    if trade_setup == TradeSetup.NO_TRADE and (cycle.late_up_cycle or cycle.late_down_cycle) and not (cycle.bearish_confirmation or cycle.bullish_confirmation):
        codes.append("reversal_confirmation_missing")
    if rider_contract is not None:
        codes.append("rider_dte_target_met")
    if selected_contract is not None and execution_layer in {ExecutionLayer.TACTICAL, ExecutionLayer.BOTH} and tactical_contract is not None:
        codes.append("selected_tactical_priority")
    if blocked_reason is not None:
        codes.append(blocked_reason)
    return codes


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
