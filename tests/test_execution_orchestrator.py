from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime

import pytest

from autobott_v2.core_runner import CoreRunnerPair
from autobott_v2.execution_config import AlpacaExecutionConfig
from autobott_v2.execution_models import BrokerEnvironment, ExecutionOrder, ExecutionState
from autobott_v2.execution_orchestrator import (
    ExecutionRejectedError,
    build_trade_intent_from_decision,
    submit_core_runner_to_broker,
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
        self.intents = []
        self.mleg_calls = []

    def submit_order(self, intent, *, current_daily_realized_pnl=0.0, open_positions=0):
        self.last_intent = intent
        self.intents.append(intent)
        sequence = len(self.intents)
        return ExecutionOrder(
            order_id=f"order-{sequence}",
            client_order_id=f"autobott-order-{sequence}",
            intent=intent,
            state=ExecutionState.SUBMITTED,
            submitted_at=datetime(2026, 7, 1, 15, 31, tzinfo=UTC),
            broker_order_id=f"alpaca-order-{sequence}",
        )

    def submit_mleg_order(self, intents, *, current_daily_realized_pnl=0.0, open_positions=0):
        self.mleg_calls.append(tuple(intents))
        return tuple(
            self.submit_order(
                intent,
                current_daily_realized_pnl=current_daily_realized_pnl,
                open_positions=open_positions + index,
            )
            for index, intent in enumerate(intents)
        )


def test_build_trade_intent_from_decision_uses_marketable_paper_limit() -> None:
    intent = build_trade_intent_from_decision(_decision_card())
    assert intent.symbol == "AAPL"
    assert intent.option_symbol == "AAPL260117C00190000"
    assert intent.limit_price == 2.6
    assert intent.take_profit_price == 3.75


def test_build_trade_intent_from_decision_caps_marketable_limit_at_position_cost() -> None:
    decision = _decision_card(
        selected_contract=_decision_card().selected_contract.__class__(
            **(_decision_card().selected_contract.__dict__ | {"bid": 0.91, "ask": 1.05, "mid": 0.98})
        )
    )

    intent = build_trade_intent_from_decision(decision, max_position_cost=100.0)

    assert intent.limit_price == 1.0


def test_build_trade_intent_from_decision_can_disable_extra_marketable_cents(monkeypatch) -> None:
    monkeypatch.setenv("AUTOBOTT_ENTRY_LIMIT_EXTRA", "0.01")

    intent = build_trade_intent_from_decision(_decision_card())

    assert intent.limit_price == 2.61


def test_build_trade_intent_from_decision_can_use_passive_mid(monkeypatch) -> None:
    monkeypatch.setenv("AUTOBOTT_ENTRY_LIMIT_STYLE", "mid")

    intent = build_trade_intent_from_decision(_decision_card())

    assert intent.limit_price == 2.5


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


def test_submit_core_runner_uses_two_distinct_contracts_under_group_budget(tmp_path, monkeypatch) -> None:
    import autobott_v2.execution_orchestrator as orchestrator

    broker = FakeBroker()
    selected = _decision_card().selected_contract
    assert selected is not None
    primary = replace(
        selected,
        option_symbol="AAPL260117C00195000",
        strike=195.0,
        bid=0.60,
        ask=0.70,
        mid=0.65,
        delta=0.40,
        target_exit_mid=0.98,
        stop_exit_mid=0.36,
    )
    runner = replace(
        selected,
        option_symbol="AAPL260117C00200000",
        strike=200.0,
        bid=0.20,
        ask=0.25,
        mid=0.225,
        delta=0.15,
        target_exit_mid=0.45,
        stop_exit_mid=0.07,
    )
    pair = CoreRunnerPair(primary, runner, estimated_group_cost=95.0)
    monkeypatch.setattr(orchestrator, "upsert_open_position_from_order", lambda *args, **kwargs: None)
    journal_path = tmp_path / "execution_orders.jsonl"

    primary_order, runner_order = submit_core_runner_to_broker(
        _decision_card(),
        pair,
        broker=broker,
        journal_path=str(journal_path),
    )

    assert [intent.quantity for intent in broker.intents] == [1, 1]
    assert [intent.option_symbol for intent in broker.intents] == [primary.option_symbol, runner.option_symbol]
    assert sum(intent.estimated_notional for intent in broker.intents) == 95.0
    assert primary_order.intent.metadata["leg_role"] == "primary"
    assert runner_order.intent.metadata["leg_role"] == "runner"
    assert primary_order.intent.metadata["trade_group_id"] == runner_order.intent.metadata["trade_group_id"]
    assert len(broker.mleg_calls) == 1
    assert len(journal_path.read_text(encoding="utf-8").splitlines()) == 4


def test_submit_core_runner_rejects_actual_debit_above_pair_budget(tmp_path) -> None:
    broker = FakeBroker()
    selected = _decision_card().selected_contract
    assert selected is not None
    primary = replace(
        selected,
        option_symbol="AAPL260117C00195000",
        strike=195.0,
        bid=0.60,
        ask=0.70,
        mid=0.65,
        target_exit_mid=1.05,
        stop_exit_mid=0.38,
    )
    runner = replace(
        selected,
        option_symbol="AAPL260117C00200000",
        strike=200.0,
        bid=0.20,
        ask=0.25,
        mid=0.225,
        target_exit_mid=0.50,
        stop_exit_mid=0.08,
    )
    pair = CoreRunnerPair(primary, runner, estimated_group_cost=90.0, max_group_cost=90.0)

    with pytest.raises(ExecutionRejectedError, match="core_runner_group_cost_exceeds_budget"):
        submit_core_runner_to_broker(
            _decision_card(),
            pair,
            broker=broker,
            journal_path=str(tmp_path / "execution_orders.jsonl"),
        )

    assert broker.intents == []


def test_submit_core_runner_fails_closed_without_atomic_mleg_support(tmp_path) -> None:
    class SingleLegOnlyBroker:
        def __init__(self) -> None:
            self.config = _config()
            self.single_leg_submissions = []

        def submit_order(self, intent, *, current_daily_realized_pnl=0.0, open_positions=0):
            self.single_leg_submissions.append(intent)
            raise AssertionError("single-leg submission must not be attempted")

    broker = SingleLegOnlyBroker()
    selected = _decision_card().selected_contract
    assert selected is not None
    primary = replace(
        selected,
        option_symbol="AAPL260117C00195000",
        strike=195.0,
        bid=0.60,
        ask=0.70,
        mid=0.65,
        target_exit_mid=1.05,
        stop_exit_mid=0.38,
    )
    runner = replace(
        selected,
        option_symbol="AAPL260117C00200000",
        strike=200.0,
        bid=0.20,
        ask=0.25,
        mid=0.225,
        target_exit_mid=0.50,
        stop_exit_mid=0.08,
    )
    pair = CoreRunnerPair(primary, runner, estimated_group_cost=95.0)

    with pytest.raises(ExecutionRejectedError, match="core_runner_atomic_submission_unavailable"):
        submit_core_runner_to_broker(
            _decision_card(),
            pair,
            broker=broker,
            journal_path=str(tmp_path / "execution_orders.jsonl"),
        )

    assert broker.single_leg_submissions == []
