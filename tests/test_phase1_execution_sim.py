from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timedelta, timezone

from autobott_v2.phase1_engine import build_decision_card
from autobott_v2.phase1_execution_sim import simulate_execution
from autobott_v2.phase1_models import (
    CycleProfile,
    CycleStatus,
    DecisionInput,
    ExecutionLayer,
    MarketBar,
    MarketContext,
    OptionContractSnapshot,
    OptionType,
)


BASE_TIME = datetime(2026, 6, 1, 15, 30, tzinfo=timezone.utc)


def _bars(start: float, step: float) -> list[MarketBar]:
    bars: list[MarketBar] = []
    for index in range(35):
        close = start + index * step
        bars.append(
            MarketBar(
                timestamp=BASE_TIME - timedelta(minutes=34 - index),
                open=close - 0.05,
                high=close + 0.25,
                low=close - 0.25,
                close=close,
                volume=1000 + index * 20,
            )
        )
    return bars


def _contract(
    *,
    symbol: str,
    option_type: OptionType,
    expiration: date,
    bid: float = 4.9,
    ask: float = 5.1,
    delta: float = 0.55,
    volume: int = 200,
    open_interest: int = 900,
    implied_volatility: float = 0.01,
) -> OptionContractSnapshot:
    return OptionContractSnapshot(
        option_symbol=symbol,
        underlying="AAPL",
        expiration=expiration,
        strike=215.0,
        option_type=option_type,
        bid=bid,
        ask=ask,
        last=(bid + ask) / 2,
        volume=volume,
        open_interest=open_interest,
        delta=delta,
        theta=-0.04,
        vega=0.08,
        implied_volatility=implied_volatility,
    )


def _card(chain: list[OptionContractSnapshot]):
    decision_input = DecisionInput(
        ticker="AAPL",
        timestamp=BASE_TIME,
        market_bars=_bars(200.0, 0.45),
        option_chain=chain,
        context=MarketContext(
            spy_bars=_bars(500.0, 0.2),
            qqq_bars=_bars(430.0, 0.15),
            vix_bars=_bars(16.0, -0.02),
        ),
        iv_history=[0.01, 0.02, 0.03, 0.04],
        cycle_profile=CycleProfile(expected_holding_days=6, cycle_confidence=CycleStatus.HIGH),
    )
    return build_decision_card(decision_input)


def _execution_ready_card():
    return _card(
        [
            _contract(symbol="AAPL260602C00215000", option_type=OptionType.CALL, expiration=date(2026, 6, 2), delta=0.56),
            _contract(symbol="AAPL260619C00215000", option_type=OptionType.CALL, expiration=date(2026, 6, 19), delta=0.48),
        ]
    )


def test_wide_bid_ask_rejects_contract() -> None:
    card = _execution_ready_card()
    assert card.tactical_contract is not None
    card = replace(
        card,
        selected_contract=replace(card.tactical_contract, bid=4.0, ask=6.0, mid=5.0, spread_pct=0.4),
        tactical_contract=replace(card.tactical_contract, bid=4.0, ask=6.0, mid=5.0, spread_pct=0.4),
        execution_layer=ExecutionLayer.TACTICAL,
    )

    event = simulate_execution(card)[0]

    assert event.filled is False
    assert event.exit_reason == "wide_bid_ask_rejects_contract"


def test_stale_quote_rejects_contract() -> None:
    card = _execution_ready_card()

    event = simulate_execution(card, quote_age_seconds=90)[0]

    assert event.filled is False
    assert event.exit_reason == "stale_quote_rejects_contract"


def test_zero_bid_rejects_contract() -> None:
    card = _execution_ready_card()
    assert card.tactical_contract is not None
    card = replace(
        card,
        selected_contract=replace(card.tactical_contract, bid=0.0, mid=2.55, spread_pct=1.0),
        tactical_contract=replace(card.tactical_contract, bid=0.0, mid=2.55, spread_pct=1.0),
        execution_layer=ExecutionLayer.TACTICAL,
    )

    event = simulate_execution(card)[0]

    assert event.filled is False
    assert event.exit_reason == "zero_bid_rejects_contract"


def test_low_volume_contract_rejected() -> None:
    card = _execution_ready_card()
    assert card.tactical_contract is not None
    card = replace(
        card,
        selected_contract=replace(card.tactical_contract, volume=1, open_interest=5),
        tactical_contract=replace(card.tactical_contract, volume=1, open_interest=5),
        execution_layer=ExecutionLayer.TACTICAL,
    )

    event = simulate_execution(card)[0]

    assert event.filled is False
    assert event.exit_reason == "low_volume_contract_rejected"


def test_midpoint_fill_does_not_count_when_order_not_filled() -> None:
    card = _execution_ready_card()
    assert card.tactical_contract is not None
    card = replace(
        card,
        selected_contract=replace(card.tactical_contract, bid=0.0, mid=2.55, spread_pct=1.0),
        tactical_contract=replace(card.tactical_contract, bid=0.0, mid=2.55, spread_pct=1.0),
        execution_layer=ExecutionLayer.TACTICAL,
    )

    event = simulate_execution(card)[0]

    assert event.filled is False
    assert event.entry_fill_model == "rejected"
    assert event.entry_fill_price is None
    assert event.pnl is None


def test_both_layer_creates_separate_tactical_and_rider_outcomes() -> None:
    card = _execution_ready_card()
    assert card.execution_layer == ExecutionLayer.BOTH
    events = simulate_execution(card, exit_option_bid=5.8, exit_option_ask=6.0, exit_reason="target")

    assert len(events) == 2
    assert {event.leg_role.value for event in events if event.leg_role is not None} == {"tactical", "rider"}
    rider_event = next(event for event in events if event.leg_role and event.leg_role.value == "rider")
    assert rider_event.parent_decision_id == card.decision_id
