from __future__ import annotations

from datetime import UTC, datetime

from autobott_v2.ghost_trades import append_ghost_trade, load_ghost_trades, observe_ghost_trades
from autobott_v2.phase1_models import (
    CycleAssessment,
    CycleStatus,
    DecisionCard,
    DecisionInput,
    DecisionStatus,
    DirectionBias,
    DirectionResult,
    ExecutionLayer,
    MarketBar,
    MarketContext,
    OptionContractSnapshot,
    OptionType,
    RegimeLabel,
    RegimeResult,
    SelectedContract,
    TradeSetup,
    VolatilityResult,
)


def _contract(mid: float) -> SelectedContract:
    return SelectedContract(
        option_symbol="AAPL260710C00105000",
        option_type=OptionType.CALL,
        expiration=datetime(2026, 7, 10, tzinfo=UTC).date(),
        strike=105.0,
        bid=mid - 0.05,
        ask=mid + 0.05,
        mid=mid,
        spread_pct=0.10,
        delta=0.55,
        theta=-0.05,
        vega=0.10,
        implied_volatility=0.30,
        volume=500,
        open_interest=1000,
        contract_score=0.8,
        reward_risk_ratio=1.0,
        target_exit_mid=round(mid * 1.5, 2),
        stop_exit_mid=round(mid * 0.55, 2),
        exit_rule="test",
        score_reasons=["test"],
    )


def _decision(mid: float) -> DecisionCard:
    return DecisionCard(
        schema_version="phase1_decision_card.v1",
        decision_id="decision-1",
        ticker="AAPL",
        timestamp=datetime(2026, 7, 9, 15, 0, tzinfo=UTC),
        regime=RegimeResult(RegimeLabel.TREND, [RegimeLabel.TREND], 1.0, "trend"),
        direction=DirectionResult(DirectionBias.BULLISH, 1.0, 0.02, 0.01, 0.5, False, "bullish"),
        cycle=CycleAssessment(CycleStatus.UNKNOWN, 2, None, None, None, None, False, False, False, False, False, "unknown", "cycle", "cycle"),
        volatility=VolatilityResult(0.5, None, None, False, False, "vol"),
        selected_contract=_contract(mid),
        tactical_contract=_contract(mid),
        rider_contract=None,
        trade_setup=TradeSetup.BULLISH_CONTINUATION,
        execution_layer=ExecutionLayer.TACTICAL,
        decision=DecisionStatus.TRADE_CANDIDATE,
        blocked_reason=None,
        reason_codes=["selected_tactical_priority"],
        confidence_score=0.8,
        explanation="test",
    )


def _decision_input(mid: float) -> DecisionInput:
    now = datetime(2026, 7, 9, 15, 30, tzinfo=UTC)
    bar = MarketBar(now, 100.0, 101.0, 99.0, 100.0, 1000)
    return DecisionInput(
        ticker="AAPL",
        timestamp=now,
        market_bars=[bar] * 30,
        option_chain=[
            OptionContractSnapshot(
                option_symbol="AAPL260710C00105000",
                underlying="AAPL",
                expiration=datetime(2026, 7, 10, tzinfo=UTC).date(),
                strike=105.0,
                option_type=OptionType.CALL,
                bid=mid - 0.05,
                ask=mid + 0.05,
                last=None,
                volume=500,
                open_interest=1000,
                delta=0.55,
                theta=-0.05,
                vega=0.10,
                implied_volatility=0.30,
            )
        ],
        context=MarketContext(),
    )


def test_ghost_trade_entry_and_observation(tmp_path) -> None:
    path = tmp_path / "ghost_trades.jsonl"
    entry = append_ghost_trade(_decision(2.5), reason="contract_cost_above_real_money_cap", max_real_cost=100.0, journal_path=path)
    observations = observe_ghost_trades(_decision_input(3.0), journal_path=path)
    rows = load_ghost_trades(journal_path=path)

    assert entry["notional"] == 250.0
    assert observations[0]["pnl"] == 50.0
    assert observations[0]["result"] == "winner"
    assert [row["event_type"] for row in rows] == ["ghost_entry", "ghost_observation"]
