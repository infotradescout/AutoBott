from __future__ import annotations

import json
from datetime import datetime, timezone

from autobott_v2.phase1_models import CycleStatus, ExecutionLayer, LifecycleStatus, Phase1LedgerEvent, TradeSetup
from autobott_v2.phase1_scorecard import bucket_is_authorized, create_ledger_event, load_phase1_gate, update_phase1_gate


BASE_TIME = datetime(2026, 6, 1, 15, 30, tzinfo=timezone.utc)


def _event(
    *,
    decision_id: str = "abc123def4567890",
    trade_setup: TradeSetup = TradeSetup.BULLISH_CONTINUATION,
    execution_layer: ExecutionLayer = ExecutionLayer.TACTICAL,
    filled: bool = True,
    pnl: float | None = 1.0,
) -> Phase1LedgerEvent:
    return create_ledger_event(
        decision_id=decision_id,
        ticker="AAPL",
        timestamp=BASE_TIME,
        trade_setup=trade_setup,
        execution_layer=execution_layer,
        cycle_confidence=CycleStatus.MEDIUM,
        selected_contract=None,
        filled=filled,
        lifecycle_status=LifecycleStatus.CLOSED if filled and pnl is not None else LifecycleStatus.REJECTED if not filled else LifecycleStatus.OPEN,
        entry_underlying_price=215.0 if filled else None,
        entry_option_bid=4.9 if filled else None,
        entry_option_ask=5.1 if filled else None,
        entry_option_mid=5.0 if filled else None,
        entry_fill_price=5.0 if filled else None,
        exit_fill_price=6.0 if pnl is not None else None,
        exit_reason="target" if pnl is not None else None,
        option_return_pct=0.2 if pnl is not None else None,
        pnl=pnl,
        hold_minutes=20 if pnl is not None else None,
    )


def test_missing_gate_file_disables_phase1_trading(tmp_path) -> None:
    evaluation = load_phase1_gate(tmp_path / "missing.json")

    assert evaluation.enabled is False
    assert evaluation.reason == "missing_gate_file_disables_phase1_trading"


def test_invalid_gate_file_disables_phase1_trading(tmp_path) -> None:
    gate_path = tmp_path / "bad_gate.json"
    gate_path.write_text("{not-json", encoding="utf-8")

    evaluation = load_phase1_gate(gate_path)

    assert evaluation.enabled is False
    assert evaluation.reason == "invalid_gate_file_disables_phase1_trading"


def test_unfilled_decision_does_not_count_as_trade(tmp_path) -> None:
    gate_path = tmp_path / "gate.json"
    update_phase1_gate([_event(filled=False, pnl=None)], gate_path)
    gate = json.loads(gate_path.read_text(encoding="utf-8"))

    assert gate["sample_size"] == 0
    assert gate["trading_enabled"] is False
    assert gate["trade_stats"]["closed_trades"] == 0


def test_profitable_tactical_bucket_does_not_unlock_rider_bucket(tmp_path) -> None:
    gate_path = tmp_path / "gate.json"
    events = [
        _event(decision_id=f"tact-{index}", execution_layer=ExecutionLayer.TACTICAL, pnl=1.0)
        for index in range(50)
    ]

    update_phase1_gate(events, gate_path)
    evaluation = load_phase1_gate(gate_path)

    assert evaluation.enabled is False
    assert evaluation.reason.startswith("under_sampled_")
    assert bucket_is_authorized(evaluation.gate, TradeSetup.BULLISH_CONTINUATION, ExecutionLayer.RIDER) is False


def test_selected_contract_is_null_when_execution_layer_none(tmp_path) -> None:
    event = _event(filled=False, pnl=None)
    payload = event.to_json_dict()

    assert payload["selected_contract"] is None


def test_scorecard_update_is_idempotent(tmp_path) -> None:
    gate_path = tmp_path / "gate.json"
    events = [_event(decision_id=f"id-{index}", pnl=1.0) for index in range(50)]

    first = update_phase1_gate(events, gate_path)
    second = update_phase1_gate(events, gate_path)

    assert first == second


def test_gate_aggregation_never_auto_enables_live_trading(tmp_path) -> None:
    gate_path = tmp_path / "gate.json"
    events = []
    events.extend(
        _event(decision_id=f"bull-tact-{index}", trade_setup=TradeSetup.BULLISH_CONTINUATION, execution_layer=ExecutionLayer.TACTICAL, pnl=1.0)
        for index in range(25)
    )
    events.extend(
        _event(decision_id=f"bull-rider-{index}", trade_setup=TradeSetup.BULLISH_CONTINUATION, execution_layer=ExecutionLayer.RIDER, pnl=1.0)
        for index in range(25)
    )
    events.extend(
        _event(decision_id=f"bear-tact-{index}", trade_setup=TradeSetup.BEARISH_CONTINUATION, execution_layer=ExecutionLayer.TACTICAL, pnl=1.0)
        for index in range(25)
    )
    events.extend(
        _event(decision_id=f"bear-rider-{index}", trade_setup=TradeSetup.BEARISH_CONTINUATION, execution_layer=ExecutionLayer.RIDER, pnl=1.0)
        for index in range(25)
    )
    update_phase1_gate(events, gate_path)
    gate = json.loads(gate_path.read_text(encoding="utf-8"))

    assert gate["live_enabled"] is False
    assert gate["allow_live_trading"] is False
