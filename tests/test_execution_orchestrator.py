from __future__ import annotations

from datetime import UTC, datetime

import pytest

from autobott_v2.execution_config import AlpacaExecutionConfig
from autobott_v2.execution_models import BrokerEnvironment, ExecutionOrder, ExecutionState
from autobott_v2.execution_orchestrator import (
    ExecutionRejectedError,
    build_trade_intent_from_decision,
    submit_decision_to_broker,
)
from autobott_v2.phase1_models import (
    CycleAssessment,
    CycleStatus,
    DecisionCard,
    DecisionStatus,
    DirectionBias,
    DirectionResult,
    ExecutionLayer,
    RegimeLabel,
    RegimeResult,
    SelectedContract,
    TradeSetup,
    VolatilityResult,
)


def _decision_card(**overrides) -> DecisionCard:
    base = DecisionCard(
        schema_version="phase1_decision_card.v1",
        decision_id="decision-123",
        ticker="AAPL",
        timestamp=datetime(2026, 7, 1, 15, 30, tzinfo=UTC),
        regime=RegimeResult(RegimeLabel.TREND, [RegimeLabel.TREND, RegimeLabel.RISK_ON], 0.8, "trend"),
        direction=DirectionResult(DirectionBias.BULLISH, 0.8, 0.03, 0.02, 0.5, False, "bullish"),
        cycle=CycleAssessment(CycleStatus.MEDIUM, 3, None, None, None, None, False, False, False, False, True, "unknown", "ok", "ok"),
        volatility=VolatilityResult(0.2, 0.3, 1.1, False, False, "ok"),
        selected_contract=SelectedContract(
            option_symbol="AAPL260117C00190000",
            option_type="call",
            expiration=datetime(2026, 1, 17, tzinfo=UTC).date(),
            strike=190.0,
            bid=2.4,
            ask=2.6,
            mid=2.5,
            spread_pct=0.08,
            open_interest=1000,
            volume=500,
            delta=0.55,
            theta=-0.08,
            vega=0.11,
            implied_volatility=0.24,
            contract_score=0.9,
            reward_risk_ratio=1.2,
            target_exit_mid=3.75,
            stop_exit_mid=1.75,
            exit_rule="tp/sl",
            score_reasons=["liquid"],
        ),
        tactical_contract=None,
        rider_contract=None,
        trade_setup=TradeSetup.BULLISH_CONTINUATION,
        execution_layer=ExecutionLayer.TACTICAL,
        decision=DecisionStatus.TRADE_CANDIDATE,
        blocked_reason=None,
        reason_codes=["trend_ok"],
        confidence_score=0.82,
        explanation="ok",
    )
    values = base.__dict__ | overrides
    return DecisionCard(**values)


def _config(**overrides) -> AlpacaExecutionConfig:
    base = AlpacaExecutionConfig(
        environment=BrokerEnvironment.PAPER,
        api_key="paper-key",
        secret_key="paper-secret",
        trading_base_url="https://paper-api.alpaca.markets",
        data_base_url="https://data.alpaca.markets",
        allow_live_trading=False,
        allow_order_placement=True,
        max_position_cost=1000.0,
        max_daily_loss=500.0,
        max_open_positions=3,
    )
    values = base.__dict__ | overrides
    return AlpacaExecutionConfig(**values)


class FakeBroker:
    def __init__(self) -> None:
        self.config = _config()
        self.last_intent = None

    def submit_order(self, intent, *, current_daily_realized_pnl=0.0, open_positions=0):
        self.last_intent = intent
        return ExecutionOrder(
            order_id="order-1",
            client_order_id="autobott-order-1",
            intent=intent,
            state=ExecutionState.SUBMITTED,
            submitted_at=datetime(2026, 7, 1, 15, 31, tzinfo=UTC),
            broker_order_id="alpaca-order-1",
        )


def test_build_trade_intent_from_decision_maps_selected_contract() -> None:
    intent = build_trade_intent_from_decision(_decision_card())
    assert intent.symbol == "AAPL"
    assert intent.option_symbol == "AAPL260117C00190000"
    assert intent.limit_price == 2.5
    assert intent.take_profit_price == 3.75


def test_build_trade_intent_from_decision_rejects_non_trade_candidate() -> None:
    with pytest.raises(ValueError, match="decision_not_trade_candidate"):
        build_trade_intent_from_decision(_decision_card(decision=DecisionStatus.NO_TRADE))


def test_submit_decision_to_broker_writes_journal_and_returns_order(tmp_path) -> None:
    broker = FakeBroker()
    journal_path = tmp_path / "execution_orders.jsonl"

    order = submit_decision_to_broker(
        _decision_card(),
        broker=broker,
        journal_path=str(journal_path),
    )

    assert order.broker_order_id == "alpaca-order-1"
    assert broker.last_intent is not None
    lines = journal_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    assert '"event_type": "risk_check"' in lines[0]
    assert '"event_type": "order_submission"' in lines[1]


def test_submit_decision_to_broker_raises_exact_risk_rejection(tmp_path) -> None:
    broker = FakeBroker()
    broker.config = _config(allow_order_placement=False)
    journal_path = tmp_path / "execution_orders.jsonl"

    with pytest.raises(ExecutionRejectedError) as excinfo:
        submit_decision_to_broker(
            _decision_card(),
            broker=broker,
            journal_path=str(journal_path),
        )

    assert excinfo.value.reason == "order_placement_disabled"
    assert excinfo.value.reasons == ("order_placement_disabled",)
    lines = journal_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    assert '"event_type": "risk_check"' in lines[0]
