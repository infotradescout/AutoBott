from __future__ import annotations

import json
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
    card = build_decision_card(_input(chain=[_contract(option_type=OptionType.PUT, bid=4.56, ask=5.44)]))

    assert card.decision == DecisionStatus.BLOCKED_BY_SPREAD
    assert card.blocked_reason == "no_contract_passed_edge_liquidity_risk_reward_filters"
    assert card.selected_contract is None


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
