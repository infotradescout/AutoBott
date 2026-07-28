from __future__ import annotations

import json
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone

from autobott_v2.phase1_config import load_alpaca_read_only_config
from autobott_v2.phase1_engine import build_decision_card
from autobott_v2.phase1_ledger import LearningLedger
from autobott_v2.phase1_models import (
    CycleProfile,
    CycleStatus,
    DecisionInput,
    DecisionStatus,
    ExecutionLayer,
    MarketBar,
    MarketContext,
    OptionContractSnapshot,
    OptionType,
    Phase1Rules,
    TradeSetup,
)


BASE_TIME = datetime(2026, 6, 1, 15, 30, tzinfo=timezone.utc)


def _bars(start: float, step: float, volume: int = 1000) -> list[MarketBar]:
    bars: list[MarketBar] = []
    for index in range(35):
        close = start + index * step
        timestamp = BASE_TIME - timedelta(minutes=34 - index)
        bars.append(
            MarketBar(
                timestamp=timestamp,
                open=close - 0.05,
                high=close + 0.25,
                low=close - 0.25,
                close=close,
                volume=volume + index * 10,
            )
        )
    return bars


def _late_up_reversal_bars() -> list[MarketBar]:
    bars = _bars(200.0, 0.35)
    prior_high = max(bar.high for bar in bars[-21:-1])
    last = bars[-1]
    bars[-1] = MarketBar(
        timestamp=last.timestamp,
        open=last.open,
        high=prior_high + 1.0,
        low=last.low - 0.6,
        close=prior_high - 0.4,
        volume=last.volume + 500,
    )
    return bars


def _late_down_reversal_bars() -> list[MarketBar]:
    bars = _bars(230.0, -0.35)
    prior_low = min(bar.low for bar in bars[-21:-1])
    last = bars[-1]
    bars[-1] = MarketBar(
        timestamp=last.timestamp,
        open=last.open,
        high=last.high + 0.6,
        low=prior_low - 1.0,
        close=prior_low + 0.4,
        volume=last.volume + 500,
    )
    return bars


def _contract(
    *,
    option_symbol: str = "AAPL260619C00215000",
    option_type: OptionType = OptionType.CALL,
    expiration: date = date(2026, 6, 19),
    bid: float = 4.90,
    ask: float = 5.10,
    strike: float = 215.0,
    delta: float = 0.48,
    iv: float = 0.25,
    volume: int = 50,
    open_interest: int = 500,
    theta: float = -0.04,
    vega: float = 0.08,
) -> OptionContractSnapshot:
    return OptionContractSnapshot(
        option_symbol=option_symbol,
        underlying="AAPL",
        expiration=expiration,
        strike=strike,
        option_type=option_type,
        bid=bid,
        ask=ask,
        last=5.0,
        volume=volume,
        open_interest=open_interest,
        delta=delta,
        theta=theta,
        vega=vega,
        implied_volatility=iv,
    )


def _input(
    *,
    chain: list[OptionContractSnapshot] | None = None,
    blackout: bool = False,
    start: float = 200.0,
    step: float = 0.45,
    bars: list[MarketBar] | None = None,
    cycle_profile: CycleProfile | None = None,
) -> DecisionInput:
    bars = bars if bars is not None else _bars(start, step)
    context = MarketContext(
        spy_bars=_bars(500.0, 0.20),
        qqq_bars=_bars(430.0, 0.15),
        vix_bars=_bars(16.0, -0.02),
        blackout_event=blackout,
        event_labels=["earnings"] if blackout else [],
    )
    return DecisionInput(
        ticker="AAPL",
        timestamp=BASE_TIME,
        market_bars=bars,
        option_chain=chain if chain is not None else [_contract(), _contract(option_symbol="AAPL260619P00215000", option_type=OptionType.PUT)],
        context=context,
        iv_history=[0.18, 0.20, 0.23, 0.27, 0.31],
        cycle_profile=cycle_profile or CycleProfile(),
    )


def test_phase1_builds_trade_candidate_without_order() -> None:
    card = build_decision_card(_input())

    assert card.decision == DecisionStatus.TRADE_CANDIDATE
    assert card.selected_contract is not None
    assert card.schema_version == "phase1_decision_card.v1"
    assert len(card.decision_id) == 16
    assert card.trade_setup == TradeSetup.BULLISH_CONTINUATION
    assert card.execution_layer == ExecutionLayer.RIDER
    assert card.cycle.status == CycleStatus.UNKNOWN
    assert card.cycle.reason == "No cycle_profile supplied; reversal timing disabled"
    assert "cycle_context_missing" in card.reason_codes
    payload = json.dumps(card.to_json_dict()).lower()
    assert "paper_order" not in payload
    assert "order_id" not in payload


def test_risk_off_exemption_allows_qualified_volatility_signal_only() -> None:
    base = _input()
    risk_off_context = MarketContext(
        spy_bars=_bars(500.0, -0.35),
        qqq_bars=_bars(430.0, -0.30),
        vix_bars=_bars(16.0, 0.08),
    )
    rules = Phase1Rules(risk_off_bullish_exempt_symbols=("VXX", "UVXY", "VIX"))

    volatility_card = build_decision_card(
        replace(base, ticker="VXX", context=risk_off_context),
        rules,
    )
    equity_card = build_decision_card(
        replace(base, ticker="AAPL", context=risk_off_context),
        rules,
    )

    assert volatility_card.decision is not DecisionStatus.BLOCKED_BY_REGIME
    assert equity_card.decision is DecisionStatus.BLOCKED_BY_REGIME


def test_phase1_blocks_event_iv_crush_risk() -> None:
    card = build_decision_card(_input(chain=[_contract(iv=0.55)], blackout=True))

    assert card.decision == DecisionStatus.BLOCKED_BY_VOLATILITY
    assert card.blocked_reason == "long_option_volatility_unfavorable"
    assert card.volatility.iv_crush_risk is True


def test_phase1_blocks_when_contract_spread_is_too_wide() -> None:
    card = build_decision_card(_input(chain=[_contract(option_type=OptionType.PUT, bid=4.0, ask=6.0)]))

    assert card.decision == DecisionStatus.BLOCKED_BY_SPREAD
    assert card.execution_layer == ExecutionLayer.NONE
    assert card.selected_contract is None


def test_phase1_buys_calls_for_down_stretched_reversal_setup() -> None:
    card = build_decision_card(
        _input(
            bars=_late_down_reversal_bars(),
            chain=[_contract(option_symbol="AAPL260612C00215000", expiration=date(2026, 6, 12))],
            cycle_profile=CycleProfile(
                median_peak_to_valley_bars=20,
                bars_since_last_peak=18,
                expected_holding_days=4,
                cycle_confidence=CycleStatus.MEDIUM,
                last_pivot_type="peak",
            ),
        )
    )

    assert card.decision == DecisionStatus.TRADE_CANDIDATE
    assert card.selected_contract is not None
    assert card.direction.bias == "bullish"
    assert card.selected_contract.option_type == OptionType.CALL
    assert card.trade_setup == TradeSetup.LATE_CYCLE_BULLISH_REVERSAL
    assert "mean-reversion" in card.direction.explanation


def test_phase1_buys_puts_for_up_stretched_reversal_setup() -> None:
    card = build_decision_card(
        _input(
            bars=_late_up_reversal_bars(),
            chain=[_contract(option_symbol="AAPL260612P00215000", option_type=OptionType.PUT, expiration=date(2026, 6, 12))],
            cycle_profile=CycleProfile(
                median_valley_to_peak_bars=20,
                bars_since_last_valley=18,
                expected_holding_days=4,
                cycle_confidence=CycleStatus.MEDIUM,
                last_pivot_type="valley",
            ),
        )
    )

    assert card.decision == DecisionStatus.TRADE_CANDIDATE
    assert card.selected_contract is not None
    assert card.direction.bias == "bearish"
    assert card.selected_contract.option_type == OptionType.PUT
    assert card.trade_setup == TradeSetup.LATE_CYCLE_BEARISH_REVERSAL
    assert "mean-reversion" in card.direction.explanation


def test_phase1_prioritizes_edge_liquidity_and_risk_reward_over_farther_dte() -> None:
    nearer_better_contract = _contract(
        option_symbol="AAPL260612C00215000",
        expiration=date(2026, 6, 12),
        bid=4.95,
        ask=5.05,
        delta=0.52,
        volume=350,
        open_interest=2500,
        theta=-0.03,
        vega=0.10,
    )
    farther_weaker_contract = _contract(
        option_symbol="AAPL260710C00215000",
        expiration=date(2026, 7, 10),
        bid=4.60,
        ask=5.40,
        delta=0.28,
        volume=25,
        open_interest=150,
        theta=-0.12,
        vega=0.04,
    )

    card = build_decision_card(_input(chain=[farther_weaker_contract, nearer_better_contract]))

    assert card.selected_contract is not None
    assert card.selected_contract.option_symbol == "AAPL260612C00215000"
    assert "rider_window_passed" in card.selected_contract.score_reasons


def test_phase1_blocks_when_reward_risk_ratio_is_too_weak() -> None:
    card = build_decision_card(_input(chain=[_contract(bid=4.56, ask=5.44)]))

    assert card.decision == DecisionStatus.BLOCKED_BY_SPREAD
    assert card.blocked_reason == "no_contract_passed_edge_liquidity_risk_reward_filters"
    assert card.selected_contract is None
    assert len(card.contract_diagnostics) == 1
    diagnostic = card.contract_diagnostics[0]
    assert diagnostic["option_symbol"] == "AAPL260619C00215000"
    rider = next(layer for layer in diagnostic["layers"] if layer["layer"] == "rider")
    assert rider["rejection_reasons"] == ["reward_risk_below_min"]
    assert "contract_filter:reward_risk_below_min" in card.reason_codes
    assert "tactical_contract_rejections[" in card.explanation


def test_phase1_rejects_deep_itm_vxx_put_that_is_mostly_intrinsic_value() -> None:
    bars = _bars(23.0, -0.03)
    contract = replace(
        _contract(
            option_symbol="VXX260807P00025000",
            option_type=OptionType.PUT,
            expiration=date(2026, 8, 7),
            strike=25.0,
            bid=3.05,
            ask=3.43,
            delta=-0.60,
            theta=-0.02,
            vega=0.08,
        ),
        underlying="VXX",
    )
    decision_input = replace(
        _input(chain=[contract], bars=bars),
        ticker="VXX",
    )
    rules = Phase1Rules(
        rider_min_dte=7,
        rider_max_dte=90,
        max_strike_distance_pct=0.20,
    )

    card = build_decision_card(decision_input, rules)

    assert card.decision == DecisionStatus.BLOCKED_BY_SPREAD
    assert card.selected_contract is None
    diagnostic = card.contract_diagnostics[0]
    assert diagnostic["intrinsic_value"] == 3.02
    assert diagnostic["intrinsic_value_ratio"] > 0.90
    rider = next(layer for layer in diagnostic["layers"] if layer["layer"] == "rider")
    assert "intrinsic_value_ratio_above_max" in rider["rejection_reasons"]
    assert "contract_filter:intrinsic_value_ratio_above_max" in card.reason_codes


def test_vix_uses_normalized_vega_floor_without_weakening_other_contract_gates() -> None:
    bars = _bars(18.0, 0.10)
    expiration = date(2026, 6, 8)
    vix_contract = replace(
        _contract(
            option_symbol="VIXW260608C00021000",
            expiration=expiration,
            strike=21.0,
            bid=0.95,
            ask=1.05,
            delta=0.52,
            theta=-0.03,
            vega=0.006,
        ),
        underlying="VIX",
    )
    rules = Phase1Rules(
        intraday_min_dte=5,
        intraday_max_dte=10,
        rider_min_dte=14,
        rider_max_dte=45,
        risk_off_bullish_exempt_symbols=("VIX",),
    )
    vix_input = replace(
        _input(bars=bars, chain=[vix_contract]),
        ticker="VIX",
        iv_history=[],
    )

    vix_card = build_decision_card(vix_input, rules)
    equity_card = build_decision_card(
        replace(
            vix_input,
            ticker="AAPL",
            option_chain=[replace(vix_contract, option_symbol="AAPL260608C00021000", underlying="AAPL")],
        ),
        rules,
    )

    assert vix_card.decision == DecisionStatus.TRADE_CANDIDATE
    assert vix_card.selected_contract is not None
    assert equity_card.decision == DecisionStatus.BLOCKED_BY_SPREAD
    tactical = next(
        layer
        for layer in equity_card.contract_diagnostics[0]["layers"]
        if layer["layer"] == "tactical"
    )
    assert "normalized_vega_below_min" in tactical["rejection_reasons"]
    assert tactical["limits"]["effective_min_vega"] == 0.01


def test_phase1_persists_accepted_and_rejected_cards(tmp_path) -> None:
    ledger_path = tmp_path / "ledger.jsonl"
    ledger = LearningLedger(ledger_path)
    ledger.append(build_decision_card(_input()))
    ledger.append(build_decision_card(_input(chain=[_contract(bid=4.0, ask=6.0)])))

    rows = [json.loads(line) for line in ledger_path.read_text(encoding="utf-8").splitlines()]
    assert [row["decision"] for row in rows] == [
        DecisionStatus.TRADE_CANDIDATE.value,
        DecisionStatus.BLOCKED_BY_SPREAD.value,
    ]
    assert all(row["ticker"] == "AAPL" for row in rows)
    assert all(row["forward_outcomes"] == {
        "after_5m": None,
        "after_15m": None,
        "after_30m": None,
        "after_1h": None,
    } for row in rows)
    assert all(row["execution_layer"] == ExecutionLayer.RIDER.value for row in rows[:1])


def test_no_cycle_profile_extended_up_move_does_not_auto_put() -> None:
    card = build_decision_card(_input(start=200.0, step=0.45, chain=[_contract()]))

    assert card.direction.bias == "bullish"
    assert card.trade_setup != TradeSetup.LATE_CYCLE_BEARISH_REVERSAL
    assert card.selected_contract is not None
    assert card.selected_contract.option_type == OptionType.CALL


def test_no_cycle_profile_extended_down_move_does_not_auto_call() -> None:
    card = build_decision_card(_input(start=230.0, step=-0.45, chain=[_contract(option_symbol="AAPL260619P00215000", option_type=OptionType.PUT)]))

    assert card.direction.bias == "bearish"
    assert card.trade_setup != TradeSetup.LATE_CYCLE_BULLISH_REVERSAL
    assert card.selected_contract is not None
    assert card.selected_contract.option_type == OptionType.PUT


def test_late_up_cycle_without_reversal_confirmation_does_not_put() -> None:
    card = build_decision_card(
        _input(
            bars=_bars(200.0, 0.35),
            chain=[_contract(option_symbol="AAPL260612P00215000", option_type=OptionType.PUT, expiration=date(2026, 6, 12))],
            cycle_profile=CycleProfile(
                median_valley_to_peak_bars=20,
                bars_since_last_valley=18,
                expected_holding_days=4,
                cycle_confidence=CycleStatus.MEDIUM,
                last_pivot_type="valley",
            ),
        )
    )

    assert card.trade_setup == TradeSetup.NO_TRADE
    assert card.decision == DecisionStatus.NO_TRADE


def test_late_down_cycle_without_reversal_confirmation_does_not_call() -> None:
    card = build_decision_card(
        _input(
            bars=_bars(230.0, -0.35),
            chain=[_contract(option_symbol="AAPL260612C00215000", expiration=date(2026, 6, 12))],
            cycle_profile=CycleProfile(
                median_peak_to_valley_bars=20,
                bars_since_last_peak=18,
                expected_holding_days=4,
                cycle_confidence=CycleStatus.MEDIUM,
                last_pivot_type="peak",
            ),
        )
    )

    assert card.trade_setup == TradeSetup.NO_TRADE
    assert card.decision == DecisionStatus.NO_TRADE


def test_selected_contract_matches_execution_layer_when_both_are_available() -> None:
    tactical = _contract(
        option_symbol="AAPL260602C00215000",
        expiration=date(2026, 6, 2),
        delta=0.56,
        volume=200,
        open_interest=900,
    )
    rider = _contract(
        option_symbol="AAPL260619C00215000",
        expiration=date(2026, 6, 19),
        delta=0.50,
        volume=250,
        open_interest=1200,
    )

    card = build_decision_card(
        _input(
            chain=[tactical, rider],
            cycle_profile=CycleProfile(expected_holding_days=6),
        )
    )

    assert card.execution_layer == ExecutionLayer.BOTH
    assert card.selected_contract is not None
    assert card.tactical_contract is not None
    assert card.rider_contract is not None
    assert card.selected_contract.option_symbol == card.tactical_contract.option_symbol
    assert "selected_tactical_priority" in card.reason_codes


def test_cycle_confidence_low_blocks_late_cycle_reversal() -> None:
    card = build_decision_card(
        _input(
            bars=_late_up_reversal_bars(),
            chain=[_contract(option_symbol="AAPL260612P00215000", option_type=OptionType.PUT, expiration=date(2026, 6, 12))],
            cycle_profile=CycleProfile(
                median_valley_to_peak_bars=20,
                bars_since_last_valley=18,
                expected_holding_days=4,
                cycle_confidence=CycleStatus.LOW,
                last_pivot_type="valley",
            ),
        )
    )

    assert card.decision == DecisionStatus.BLOCKED_BY_SPREAD
    assert "cycle_confidence_blocks_reversal" in card.reason_codes


def test_cycle_confidence_unknown_blocks_late_cycle_reversal() -> None:
    card = build_decision_card(
        _input(
            bars=_late_down_reversal_bars(),
            chain=[_contract(option_symbol="AAPL260612C00215000", expiration=date(2026, 6, 12))],
            cycle_profile=CycleProfile(
                median_peak_to_valley_bars=20,
                bars_since_last_peak=18,
                expected_holding_days=4,
                cycle_confidence=CycleStatus.UNKNOWN,
                last_pivot_type="peak",
            ),
        )
    )

    assert card.decision == DecisionStatus.BLOCKED_BY_SPREAD
    assert "cycle_confidence_blocks_reversal" in card.reason_codes


def test_cycle_confidence_high_allows_reversal_with_confirmation() -> None:
    card = build_decision_card(
        _input(
            bars=_late_up_reversal_bars(),
            chain=[_contract(option_symbol="AAPL260612P00215000", option_type=OptionType.PUT, expiration=date(2026, 6, 12))],
            cycle_profile=CycleProfile(
                median_valley_to_peak_bars=20,
                bars_since_last_valley=18,
                expected_holding_days=4,
                cycle_confidence=CycleStatus.HIGH,
                last_pivot_type="valley",
            ),
        )
    )

    assert card.decision == DecisionStatus.TRADE_CANDIDATE
    assert "cycle_confidence_reversal_allowed" in card.reason_codes


def test_phase1_loads_old_alpaca_env_names_read_only(monkeypatch) -> None:
    monkeypatch.setenv("APCA_API_KEY_ID", "key")
    monkeypatch.setenv("APCA_API_SECRET_KEY", "secret")
    monkeypatch.setenv("APCA_API_BASE_URL", "https://paper-api.alpaca.markets")
    monkeypatch.setenv("APCA_API_DATA_URL", "https://data.alpaca.markets")

    config = load_alpaca_read_only_config()

    assert config.has_credentials is True
    assert config.paper is True
    assert config.base_url == "https://paper-api.alpaca.markets"
