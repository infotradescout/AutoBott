from __future__ import annotations

from datetime import datetime, timezone

from autobott_v2.execution_journal import append_execution_outcome, append_order_submission, append_risk_check, load_execution_journal
from autobott_v2.execution_models import (
    BrokerEnvironment,
    ExecutionOrder,
    ExecutionState,
    OrderSide,
    RiskCheckResult,
    TradeIntent,
)


def _intent() -> TradeIntent:
    return TradeIntent(
        symbol="AAPL",
        option_symbol="AAPL260117C00190000",
        side=OrderSide.BUY_TO_OPEN,
        quantity=1,
        limit_price=2.5,
        generated_at=datetime(2026, 7, 1, 15, 30, tzinfo=timezone.utc),
        environment=BrokerEnvironment.PAPER,
        take_profit_price=3.75,
        stop_loss_price=1.75,
        decision_id="decision-123",
        thesis_id="AAPL:bullish_continuation:tactical",
    )


def test_execution_journal_appends_risk_check_and_submission(tmp_path) -> None:
    path = tmp_path / "execution_orders.jsonl"
    intent = _intent()
    risk_check = RiskCheckResult(
        approved=True,
        reasons=(),
        estimated_notional=250.0,
        normalized_limit_price=2.5,
    )
    order = ExecutionOrder(
        order_id="order-1",
        client_order_id="autobott-order-1",
        intent=intent,
        state=ExecutionState.SUBMITTED,
        submitted_at=datetime(2026, 7, 1, 15, 31, tzinfo=timezone.utc),
        broker_order_id="alpaca-order-1",
    )

    append_risk_check(intent, risk_check, journal_path=path)
    append_execution_outcome(
        decision_id=intent.decision_id,
        thesis_id=intent.thesis_id,
        symbol=intent.symbol,
        disposition="scanner_candidate",
        detail="bullish_continuation:tactical",
        payload={"selected_contract": intent.option_symbol},
        journal_path=path,
    )
    append_order_submission(order, journal_path=path)
    rows = load_execution_journal(journal_path=path)

    assert [row["event_type"] for row in rows] == ["risk_check", "execution_outcome", "order_submission"]
    assert rows[0]["payload"]["risk_check"]["approved"] is True
    assert rows[1]["payload"]["disposition"] == "scanner_candidate"
    assert rows[2]["payload"]["broker_order_id"] == "alpaca-order-1"
