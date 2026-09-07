from __future__ import annotations

from .phase1_engine import (
    _card,
    _confidence,
    _contract_filter_diagnostics,
    _empty_cycle_assessment,
    _neutral_direction,
    _selected_contract_score,
    _selected_layer,
    _validate_numeric_inputs,
    analyze_cycle,
    classify_regime,
    determine_trade_setup,
    score_volatility,
    select_contract,
)
from .phase1_models import (
    CycleStatus,
    DecisionCard,
    DecisionInput,
    DecisionStatus,
    DirectionBias,
    ExecutionLayer,
    Phase1Rules,
    RegimeLabel,
    RegimeResult,
    SelectedContract,
    TradeSetup,
    VolatilityResult,
)
from .signal_evidence import score_direction_evidence


def build_decision_card(decision_input: DecisionInput, rules: Phase1Rules | None = None) -> DecisionCard:
    """Build the existing decision-card contract with the rebuilt direction brain."""

    rules = rules or Phase1Rules()
    _validate_numeric_inputs(decision_input)

    if len(decision_input.market_bars) < rules.min_bars:
        regime = RegimeResult(
            RegimeLabel.RANGE,
            [RegimeLabel.RANGE],
            0.0,
            "Not enough bars to classify regime.",
        )
        direction = _neutral_direction("Not enough bars to score direction.")
        cycle = _empty_cycle_assessment("Not enough bars to score cycle timing.")
        volatility = VolatilityResult(
            0.0,
            None,
            None,
            False,
            decision_input.context.blackout_event,
            "Not enough bars to score volatility.",
        )
        return _card(
            decision_input,
            regime,
            direction,
            cycle,
            volatility,
            None,
            None,
            None,
            TradeSetup.NO_TRADE,
            ExecutionLayer.NONE,
            DecisionStatus.NO_TRADE,
            "insufficient_market_bars",
            0.0,
        )

    regime = classify_regime(
        decision_input.market_bars,
        decision_input.context.vix_bars,
        decision_input.context.spy_bars,
    )
    cycle = analyze_cycle(decision_input.market_bars, decision_input.cycle_profile, rules)
    direction, evidence = score_direction_evidence(
        decision_input.market_bars,
        decision_input.context.spy_bars,
        decision_input.context.qqq_bars,
        cycle,
        neutral_band=rules.min_direction_score,
    )
    volatility = score_volatility(decision_input, direction)
    setup = determine_trade_setup_v2(direction, cycle)

    risk_off_exempt = decision_input.ticker.upper() in {
        symbol.upper() for symbol in rules.risk_off_bullish_exempt_symbols
    }
    if RegimeLabel.RISK_OFF in regime.labels and direction.bias == DirectionBias.BULLISH and not risk_off_exempt:
        return _card(
            decision_input,
            regime,
            direction,
            cycle,
            volatility,
            None,
            None,
            None,
            setup,
            ExecutionLayer.NONE,
            DecisionStatus.BLOCKED_BY_REGIME,
            "bullish_options_blocked_in_risk_off",
            0.0,
        )

    if direction.bias == DirectionBias.NEUTRAL or abs(evidence.composite_score) < rules.min_direction_score:
        return _card(
            decision_input,
            regime,
            direction,
            cycle,
            volatility,
            None,
            None,
            None,
            setup,
            ExecutionLayer.NONE,
            DecisionStatus.NO_TRADE,
            "direction_evidence_conflicted",
            abs(evidence.composite_score),
        )

    if volatility.event_risk or volatility.iv_crush_risk or volatility.score < rules.min_volatility_score:
        confidence = _confidence(regime.score, direction.score, volatility.score, None)
        return _card(
            decision_input,
            regime,
            direction,
            cycle,
            volatility,
            None,
            None,
            None,
            setup,
            ExecutionLayer.NONE,
            DecisionStatus.BLOCKED_BY_VOLATILITY,
            "long_option_volatility_unfavorable",
            confidence,
        )

    tactical_contract = select_contract(
        decision_input,
        direction,
        volatility,
        rules,
        ExecutionLayer.TACTICAL,
        setup,
        cycle,
    )
    rider_contract = select_contract(
        decision_input,
        direction,
        volatility,
        rules,
        ExecutionLayer.RIDER,
        setup,
        cycle,
    )
    selected_layer = _selected_layer(tactical_contract, rider_contract)
    contract = _selected_contract_score(selected_layer, tactical_contract, rider_contract)
    if contract is None:
        confidence = _confidence(regime.score, direction.score, volatility.score, None)
        contract_diagnostics = _contract_filter_diagnostics(
            decision_input,
            direction,
            rules,
            setup,
            cycle,
        )
        return _card(
            decision_input,
            regime,
            direction,
            cycle,
            volatility,
            None,
            None,
            None,
            setup,
            ExecutionLayer.NONE,
            DecisionStatus.BLOCKED_BY_SPREAD,
            "no_contract_passed_edge_liquidity_risk_reward_filters",
            confidence,
            contract_diagnostics=contract_diagnostics,
        )

    confidence = _confidence(regime.score, direction.score, volatility.score, contract.score)
    status = DecisionStatus.TRADE_CANDIDATE if confidence >= rules.min_confidence else DecisionStatus.NO_TRADE
    reason = None if status == DecisionStatus.TRADE_CANDIDATE else "confidence_below_threshold"
    selected_contract = SelectedContract.from_score(contract, rules)
    card = _card(
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
    # The explanation already carries the strongest continuous evidence. Keep
    # the stable DecisionCard schema instead of creating another incompatible
    # payload generation during the trading-brain migration.
    return card


def determine_trade_setup_v2(direction, cycle) -> TradeSetup:
    explanation = direction.explanation.lower()
    if direction.bias == DirectionBias.BULLISH:
        if "reversal" in explanation or cycle.late_down_cycle:
            return TradeSetup.LATE_CYCLE_BULLISH_REVERSAL
        return TradeSetup.BULLISH_CONTINUATION
    if direction.bias == DirectionBias.BEARISH:
        if "reversal" in explanation or cycle.late_up_cycle:
            return TradeSetup.LATE_CYCLE_BEARISH_REVERSAL
        return TradeSetup.BEARISH_CONTINUATION
    return TradeSetup.NO_TRADE
