from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime

import pytest

from autobott_v2.execution_cycle import CycleLifecycleState
from autobott_v2.vix_trader import (
    SettlementType,
    TradingSession,
    VixPreflightRequest,
    VixProduct,
    VixStrategyConfig,
    classify_vix_session,
    create_vix_cycle,
    validate_vix_preflight,
    vix_cycle_analytics,
)


BASE_TIME = datetime(2026, 7, 13, 15, 0, tzinfo=UTC)


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
    )
    return replace(request, **changes)


def _issue_codes(request: VixPreflightRequest, config: VixStrategyConfig | None = None) -> set[str]:
    return {issue.code for issue in validate_vix_preflight(request, config or _config()).issues}


def test_valid_pair_passes_preflight_and_retains_expiration_truth() -> None:
    result = validate_vix_preflight(_request(), _config())
    assert result.passed is True
    assert result.actual_session is TradingSession.REGULAR
    assert result.final_tradable_timestamp.astimezone(UTC).date() == date(2026, 7, 21)
    assert result.automatic_exit_deadline < result.final_tradable_timestamp


def test_unprovided_strategy_numbers_fail_closed_instead_of_becoming_defaults() -> None:
    result = validate_vix_preflight(_request())
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
    result = validate_vix_preflight(request, _config())
    assert result.passed is True
    assert {row["code"] for row in result.override_audit} == {"extended_hours_entry_blocked", "intended_actual_session_mismatch"}


def test_blocked_cycle_cannot_look_entry_ready() -> None:
    cycle = create_vix_cycle(_request(call_product=VixProduct.VIX), _config())
    assert cycle.execution_cycle.lifecycle_state is CycleLifecycleState.PREFLIGHT_BLOCKED
    assert cycle.execution_cycle.next_required_action == "correct_preflight_issues"


def test_opposite_leg_addition_requires_first_exit_and_respects_caps() -> None:
    config = _config()
    cycle = create_vix_cycle(_request(), config)
    with pytest.raises(ValueError, match="first_leg_exit_required"):
        cycle.add_opposite_leg(leg="put", quantity=1, debit=1.0, reason="mean reversion", config=config)
    cycle.first_leg_sold = "call"
    addition = cycle.add_opposite_leg(leg="put", quantity=1, debit=1.0, reason="mean reversion", config=config)
    assert addition["capital"] == 100.0
    with pytest.raises(ValueError, match="maximum_additions_reached"):
        cycle.add_opposite_leg(leg="put", quantity=1, debit=1.0, reason="duplicate", config=config)


def test_analytics_separate_strategy_result_from_execution_quality() -> None:
    cycle = create_vix_cycle(_request(), _config())
    cycle.execution_cycle.capital_committed = 800.0
    cycle.realized_pnl = 120.0
    cycle.unrealized_pnl = -40.0
    cycle.execution_deviations.append({"code": "delayed_exit", "measurable_cost": None})
    report = vix_cycle_analytics(cycle)
    assert report["strategy_performance"]["combined_cycle_pnl"] == 80.0
    assert report["execution_quality"]["deviation_count"] == 1
    assert report["execution_quality"]["unknown_deviation_costs_are_estimated"] is False


def test_session_classifier_distinguishes_regular_and_global() -> None:
    assert classify_vix_session(BASE_TIME) is TradingSession.REGULAR
    assert classify_vix_session(datetime(2026, 7, 14, 2, 0, tzinfo=UTC)) is TradingSession.GLOBAL
