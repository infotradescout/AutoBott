from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, time
from threading import Barrier, Thread

import pytest

from autobott_v2.execution_cycle import BrokerOrderState, CycleLifecycleState, ManagedOrder
from autobott_v2.vix_trader import (
    AuthoritativeCboeCalendar,
    SettlementType,
    TradingSession,
    VixPreflightRequest,
    VixContractMetadata,
    VixProduct,
    VixStrategyConfig,
    classify_vix_session,
    create_vix_cycle,
    append_vix_cycle,
    load_vix_cycles,
    load_vix_strategy_config,
    save_vix_strategy_config,
    validate_vix_preflight,
    vix_cycle_analytics,
)


BASE_TIME = datetime(2026, 7, 13, 15, 0, tzinfo=UTC)


def _calendar(**changes) -> AuthoritativeCboeCalendar:
    values = {"holidays": frozenset(), "early_closes": {}}
    values.update(changes)
    return AuthoritativeCboeCalendar(**values)


def _contract(option_type: str, **changes) -> VixContractMetadata:
    values = {
        "option_symbol": f"VIXW-20260722-{option_type.upper()}-18",
        "product": VixProduct.VIXW,
        "option_type": option_type,
        "expiration": date(2026, 7, 22),
        "strike": 18.0 if option_type == "call" else 17.0,
        "settlement_type": SettlementType.AM,
        "source": "broker",
        "observed_at": BASE_TIME,
    }
    values.update(changes)
    return VixContractMetadata(**values)


def _config(**changes) -> VixStrategyConfig:
    config = VixStrategyConfig(
        minimum_full_trading_sessions_remaining=3,
        maximum_days_to_expiration=10,
        maximum_combined_debit=10.0,
        maximum_cycle_allocation=2_000.0,
        first_leg_exit_target_pct=0.30,
        second_leg_management_rule="test_mean_reversion_rule",
        maximum_additions=1,
        maximum_additional_capital=500.0,
        addition_sizing=1,
        addition_trigger="test_reversal_trigger",
    )
    return replace(config, **changes)


def _request(**changes) -> VixPreflightRequest:
    request = VixPreflightRequest(
        spot_vix=17.5,
        product=VixProduct.VIXW,
        call_product=VixProduct.VIXW,
        put_product=VixProduct.VIXW,
        call_expiration=date(2026, 7, 22),
        put_expiration=date(2026, 7, 22),
        settlement_type=SettlementType.AM,
        intended_session=TradingSession.REGULAR,
        actual_timestamp=BASE_TIME,
        call_strike=18.0,
        put_strike=17.0,
        call_quantity=2,
        put_quantity=2,
        call_debit=2.0,
        put_debit=2.0,
        client_request_id="cycle-one",
        call_contract=_contract("call"),
        put_contract=_contract("put"),
    )
    return replace(request, **changes)


def _issue_codes(request: VixPreflightRequest, config: VixStrategyConfig | None = None) -> set[str]:
    return {issue.code for issue in validate_vix_preflight(request, config or _config(), calendar=_calendar()).issues}


def test_valid_pair_passes_preflight_and_retains_expiration_truth() -> None:
    result = validate_vix_preflight(_request(), _config(), calendar=_calendar())
    assert result.passed is True
    assert result.actual_session is TradingSession.REGULAR
    assert result.final_tradable_timestamp.astimezone(UTC).date() == date(2026, 7, 21)
    assert result.automatic_exit_deadline < result.final_tradable_timestamp


def test_unprovided_strategy_numbers_fail_closed_instead_of_becoming_defaults() -> None:
    result = validate_vix_preflight(_request(), calendar=_calendar())
    assert result.passed is False
    assert "strategy_configuration_incomplete" in {issue.code for issue in result.issues}


@pytest.mark.parametrize(
    ("changes", "code"),
    [
        ({"call_product": VixProduct.VIX}, "vix_vixw_mismatch"),
        ({"put_expiration": date(2026, 7, 29)}, "mismatched_expirations"),
        ({"settlement_type": SettlementType.PM}, "wrong_settlement_assumption"),
        ({"actual_timestamp": datetime(2026, 7, 13, 23, 0, tzinfo=UTC)}, "extended_hours_entry_blocked"),
        ({"call_expiration": date(2026, 7, 15), "put_expiration": date(2026, 7, 15)}, "too_few_trading_sessions"),
        ({"existing_cycle_ids": ("existing",)}, "duplicate_cycle"),
        ({"overlapping_expirations": (date(2026, 7, 22),)}, "overlapping_exposure"),
        ({"prior_client_request_ids": ("cycle-one",)}, "duplicate_order"),
    ],
)
def test_preflight_blocks_execution_mistakes(changes, code) -> None:
    assert code in _issue_codes(_request(**changes))


def test_excessive_cycle_capital_is_blocked() -> None:
    assert "cycle_capital_exceeded" in _issue_codes(_request(call_quantity=10, put_quantity=10))


def test_authorized_override_is_audited_not_silently_normalized() -> None:
    request = _request(
        actual_timestamp=datetime(2026, 7, 13, 23, 0, tzinfo=UTC),
        requested_override_codes=("extended_hours_entry_blocked", "intended_actual_session_mismatch"),
        override_actor="thomas",
    )
    result = validate_vix_preflight(
        request,
        _config(),
        calendar=_calendar(),
        authorized_override_actor="dashboard_operator",
        allowed_override_codes={"extended_hours_entry_blocked", "intended_actual_session_mismatch"},
    )
    assert result.passed is True
    assert {row["code"] for row in result.override_audit} == {"extended_hours_entry_blocked", "intended_actual_session_mismatch"}
    assert {row["actor"] for row in result.override_audit} == {"dashboard_operator"}


def test_blocked_cycle_cannot_look_entry_ready() -> None:
    cycle = create_vix_cycle(_request(call_product=VixProduct.VIX), _config(), calendar=_calendar())
    assert cycle.execution_cycle.lifecycle_state is CycleLifecycleState.PREFLIGHT_BLOCKED
    assert cycle.execution_cycle.next_required_action == "correct_preflight_issues"


def test_opposite_leg_addition_requires_first_exit_and_respects_caps() -> None:
    config = _config()
    cycle = create_vix_cycle(_request(), config, calendar=_calendar())
    with pytest.raises(ValueError, match="first_leg_exit_required"):
        cycle.add_opposite_leg(leg="put", quantity=1, debit=1.0, reason="mean reversion", trigger_condition_met=True, trigger_evidence={"spot_vix": 18.5}, config=config)
    cycle.first_leg_sold = "call"
    cycle.execution_cycle.lifecycle_state = CycleLifecycleState.FIRST_LEG_EXITED
    addition = cycle.add_opposite_leg(leg="put", quantity=1, debit=1.0, reason="mean reversion", trigger_condition_met=True, trigger_evidence={"spot_vix": 18.5}, config=config)
    assert addition["capital"] == 100.0
    with pytest.raises(ValueError, match="maximum_additions_reached"):
        cycle.add_opposite_leg(leg="put", quantity=1, debit=1.0, reason="duplicate", trigger_condition_met=True, trigger_evidence={"spot_vix": 18.5}, config=config)


def test_analytics_separate_strategy_result_from_execution_quality() -> None:
    cycle = create_vix_cycle(_request(), _config(), calendar=_calendar())
    call_entry = ManagedOrder("call-entry", "call", "entry", requested_quantity=2)
    put_entry = ManagedOrder("put-entry", "put", "entry", requested_quantity=2)
    call_exit = ManagedOrder("call-exit", "call", "exit", requested_quantity=2)
    call_entry.apply_broker_update(state=BrokerOrderState.FILLED, filled_quantity=2, fill_price=2.0)
    put_entry.apply_broker_update(state=BrokerOrderState.FILLED, filled_quantity=2, fill_price=2.0)
    call_exit.apply_broker_update(state=BrokerOrderState.FILLED, filled_quantity=2, fill_price=2.6)
    cycle.execution_cycle.orders.extend([call_entry, put_entry, call_exit])
    cycle.apply_market_estimates({"put": 1.8}, source="broker")
    cycle.execution_deviations.append({"code": "delayed_exit", "measurable_cost": None})
    report = vix_cycle_analytics(cycle)
    assert report["strategy_performance"]["combined_cycle_pnl"] == 80.0
    assert report["execution_quality"]["deviation_count"] == 1
    assert report["execution_quality"]["unknown_deviation_costs_are_estimated"] is False


def test_session_classifier_distinguishes_regular_and_global() -> None:
    calendar = _calendar()
    assert classify_vix_session(BASE_TIME, calendar=calendar) is TradingSession.REGULAR
    assert classify_vix_session(datetime(2026, 7, 14, 2, 0, tzinfo=UTC), calendar=calendar) is TradingSession.GLOBAL


@pytest.mark.parametrize("value", [0.0, -1.0, float("nan"), float("inf")])
def test_debits_must_be_positive_and_finite(value: float) -> None:
    assert "invalid_debit" in _issue_codes(_request(call_debit=value))


def test_authoritative_contract_metadata_cannot_be_replaced_by_descriptions() -> None:
    assert "authoritative_contract_metadata_required" in _issue_codes(_request(call_contract=None))
    assert "contract_strike_mismatch" in _issue_codes(_request(call_contract=_contract("call", strike=19.0)))


def test_timestamp_and_override_identity_must_come_from_server_context() -> None:
    assert "untrusted_timestamp" in _issue_codes(_request(timestamp_source="client"))
    request = _request(requested_override_codes=("extended_hours_entry_blocked",), override_actor="self-claimed")
    assert "unauthorized_override_request" in _issue_codes(request)


def test_calendar_models_sunday_friday_holiday_and_early_close() -> None:
    holiday = date(2026, 7, 20)
    early_day = date(2026, 7, 21)
    calendar = _calendar(holidays=frozenset({holiday}), early_closes={early_day: time(13, 0)})
    assert classify_vix_session(datetime(2026, 7, 19, 23, 0, tzinfo=UTC), calendar=calendar) is TradingSession.CLOSED
    assert classify_vix_session(datetime(2026, 7, 17, 23, 0, tzinfo=UTC), calendar=calendar) is TradingSession.CLOSED
    assert classify_vix_session(datetime(2026, 7, 20, 15, 0, tzinfo=UTC), calendar=calendar) is TradingSession.CLOSED
    assert classify_vix_session(datetime(2026, 7, 21, 16, 0, tzinfo=UTC), calendar=calendar) is TradingSession.REGULAR
    assert classify_vix_session(datetime(2026, 7, 21, 18, 30, tzinfo=UTC), calendar=calendar) is TradingSession.CLOSED


def test_configuration_round_trips_and_environment_overrides(tmp_path) -> None:
    path = tmp_path / "config.json"
    save_vix_strategy_config(_config(), path=path)
    loaded = load_vix_strategy_config(path=path, environ={"AUTOBOTT_VIX_MAX_DTE": "7"})
    assert loaded.maximum_days_to_expiration == 7
    assert loaded.maximum_combined_debit == 10.0


def test_cycle_store_duplicate_check_is_atomic(tmp_path) -> None:
    path = tmp_path / "cycles.jsonl"
    barrier = Barrier(2)
    outcomes: list[str] = []

    def write() -> None:
        cycle = create_vix_cycle(_request(), _config(), calendar=_calendar())
        barrier.wait()
        try:
            append_vix_cycle(cycle, path=path)
            outcomes.append("saved")
        except ValueError as exc:
            outcomes.append(str(exc))

    threads = [Thread(target=write), Thread(target=write)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert sorted(outcomes) == ["duplicate_client_request_id", "saved"]
    assert len(load_vix_cycles(path=path)) == 1


def test_addition_enforces_trigger_sizing_and_cumulative_cap() -> None:
    config = _config(maximum_additions=2, maximum_additional_capital=150.0)
    cycle = create_vix_cycle(_request(), config, calendar=_calendar())
    cycle.first_leg_sold = "call"
    cycle.execution_cycle.lifecycle_state = CycleLifecycleState.FIRST_LEG_EXITED
    with pytest.raises(ValueError, match="configured_addition_trigger_not_proven"):
        cycle.add_opposite_leg(leg="put", quantity=1, debit=1.0, reason="no trigger", trigger_condition_met=False, trigger_evidence={}, config=config)
    with pytest.raises(ValueError, match="addition_sizing_mismatch"):
        cycle.add_opposite_leg(leg="put", quantity=2, debit=1.0, reason="wrong size", trigger_condition_met=True, trigger_evidence={"signal": True}, config=config)
    cycle.add_opposite_leg(leg="put", quantity=1, debit=1.0, reason="valid", trigger_condition_met=True, trigger_evidence={"signal": True}, config=config)
    with pytest.raises(ValueError, match="additional_capital_exceeded"):
        cycle.add_opposite_leg(leg="put", quantity=1, debit=1.0, reason="over cap", trigger_condition_met=True, trigger_evidence={"signal": True}, config=config)
