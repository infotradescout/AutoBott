import json
from datetime import datetime, timezone

from autobott_v2.engine import evaluate_trade
from autobott_v2.models import (
    AccountState,
    DecisionReasonCode,
    MarketState,
    RiskRules,
    TradingSignal,
)
from autobott_v2.replay import serialize_decision


def _valid_market_state() -> MarketState:
    return MarketState(
        symbol="AAPL",
        timestamp=datetime(2026, 1, 1, 15, 30, tzinfo=timezone.utc),
        last_price=210.0,
        volatility_regime="normal",
    )


def _valid_account_state() -> AccountState:
    return AccountState(
        equity=10000.0,
        buying_power=5000.0,
        realized_pnl=0.0,
    )


def _valid_risk_rules() -> RiskRules:
    return RiskRules(
        max_position_fraction=0.1,
        max_units_per_trade=2,
        max_daily_loss=300.0,
    )


def _valid_signal() -> TradingSignal:
    return TradingSignal(
        signal_id="sig-001",
        strategy_id="breakout-v1",
        symbol="AAPL",
        side="buy",
        confidence=0.8,
        timestamp=datetime(2026, 1, 1, 15, 30, tzinfo=timezone.utc),
        expected_entry_price=400.0,
    )


def test_valid_signal_creates_paper_decision_only() -> None:
    decision = evaluate_trade(
        market_state=_valid_market_state(),
        account_state=_valid_account_state(),
        risk_rules=_valid_risk_rules(),
        signal=_valid_signal(),
    )

    assert decision.accepted is True
    assert decision.reason_code == DecisionReasonCode.APPROVED_PAPER_ORDER
    assert decision.paper_order is not None
    assert decision.replay_payload["execution_mode"] == "paper_only"


def test_reject_missing_account_state() -> None:
    decision = evaluate_trade(
        market_state=_valid_market_state(),
        account_state=None,
        risk_rules=_valid_risk_rules(),
        signal=_valid_signal(),
    )

    assert decision.accepted is False
    assert decision.reason_code == DecisionReasonCode.REJECT_MISSING_ACCOUNT_STATE


def test_reject_missing_market_state() -> None:
    decision = evaluate_trade(
        market_state=None,
        account_state=_valid_account_state(),
        risk_rules=_valid_risk_rules(),
        signal=_valid_signal(),
    )

    assert decision.accepted is False
    assert decision.reason_code == DecisionReasonCode.REJECT_MISSING_MARKET_STATE


def test_reject_missing_timestamp() -> None:
    signal = _valid_signal()
    signal = TradingSignal(**{**signal.__dict__, "timestamp": None})

    decision = evaluate_trade(
        market_state=_valid_market_state(),
        account_state=_valid_account_state(),
        risk_rules=_valid_risk_rules(),
        signal=signal,
    )

    assert decision.accepted is False
    assert decision.reason_code == DecisionReasonCode.REJECT_MISSING_TIMESTAMP


def test_reject_missing_strategy_identity() -> None:
    signal = _valid_signal()
    signal = TradingSignal(**{**signal.__dict__, "strategy_id": ""})

    decision = evaluate_trade(
        market_state=_valid_market_state(),
        account_state=_valid_account_state(),
        risk_rules=_valid_risk_rules(),
        signal=signal,
    )

    assert decision.accepted is False
    assert decision.reason_code == DecisionReasonCode.REJECT_MISSING_STRATEGY_IDENTITY


def test_reject_insufficient_buying_power() -> None:
    account = AccountState(equity=1000.0, buying_power=100.0, realized_pnl=0.0)
    signal = TradingSignal(**{**_valid_signal().__dict__, "expected_entry_price": 200.0})

    decision = evaluate_trade(
        market_state=_valid_market_state(),
        account_state=account,
        risk_rules=_valid_risk_rules(),
        signal=signal,
    )

    assert decision.accepted is False
    assert decision.reason_code == DecisionReasonCode.REJECT_INSUFFICIENT_BUYING_POWER


def test_reject_position_size_too_large() -> None:
    rules = RiskRules(max_position_fraction=0.8, max_units_per_trade=1, max_daily_loss=300.0)
    signal = TradingSignal(**{**_valid_signal().__dict__, "expected_entry_price": 100.0})

    decision = evaluate_trade(
        market_state=_valid_market_state(),
        account_state=_valid_account_state(),
        risk_rules=rules,
        signal=signal,
    )

    assert decision.accepted is False
    assert decision.reason_code == DecisionReasonCode.REJECT_POSITION_SIZE_TOO_LARGE


def test_reject_max_loss_exceeded() -> None:
    account = AccountState(equity=10000.0, buying_power=5000.0, realized_pnl=-350.0)

    decision = evaluate_trade(
        market_state=_valid_market_state(),
        account_state=account,
        risk_rules=_valid_risk_rules(),
        signal=_valid_signal(),
    )

    assert decision.accepted is False
    assert decision.reason_code == DecisionReasonCode.REJECT_MAX_LOSS_EXCEEDED


def test_every_decision_is_replayable_json() -> None:
    accepted_decision = evaluate_trade(
        market_state=_valid_market_state(),
        account_state=_valid_account_state(),
        risk_rules=_valid_risk_rules(),
        signal=_valid_signal(),
    )
    rejected_decision = evaluate_trade(
        market_state=None,
        account_state=_valid_account_state(),
        risk_rules=_valid_risk_rules(),
        signal=_valid_signal(),
    )

    accepted_payload = serialize_decision(accepted_decision)
    rejected_payload = serialize_decision(rejected_decision)

    accepted_data = json.loads(accepted_payload)
    rejected_data = json.loads(rejected_payload)

    assert accepted_data["reason_code"] == DecisionReasonCode.APPROVED_PAPER_ORDER.value
    assert rejected_data["reason_code"] == DecisionReasonCode.REJECT_MISSING_MARKET_STATE.value


def test_no_live_order_or_broker_connector_path_exists() -> None:
    decision = evaluate_trade(
        market_state=_valid_market_state(),
        account_state=_valid_account_state(),
        risk_rules=_valid_risk_rules(),
        signal=_valid_signal(),
    )

    payload = serialize_decision(decision).lower()
    assert "live" not in payload or "paper_only" in payload
    assert "alpaca" not in payload
    assert "broker" not in payload
