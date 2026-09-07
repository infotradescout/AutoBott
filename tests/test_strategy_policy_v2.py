from __future__ import annotations

import autobott_v2.session_runner as session_runner
import autobott_v2.trading_cycle as legacy_cycle
from autobott_v2.core_runner import CoreRunnerRules
from autobott_v2.strategy_policy import HOSTED_STRATEGY_POLICY, StrategyPolicy
from autobott_v2.trading_cycle_v2 import run_trading_cycle


def test_default_hosted_strategy_policy_is_valid() -> None:
    policy = HOSTED_STRATEGY_POLICY

    assert policy.validate() is policy
    assert policy.version == "hosted-core-runner-v2"
    assert (policy.tactical_min_dte, policy.tactical_max_dte) == (5, 10)
    assert (policy.rider_min_dte, policy.rider_max_dte) == (14, 45)
    assert policy.runner_min_abs_delta < policy.runner_target_abs_delta < policy.runner_max_abs_delta
    assert policy.runner_max_abs_delta > policy.core_min_abs_delta


def test_runner_delta_window_may_overlap_core_floor_but_not_actual_core() -> None:
    rules = CoreRunnerRules(
        core_min_abs_delta=0.25,
        runner_min_abs_delta=0.10,
        runner_target_abs_delta=0.20,
        runner_max_abs_delta=0.35,
    )

    assert rules.validate() is rules


def test_invalid_policy_windows_fail_closed() -> None:
    try:
        StrategyPolicy(tactical_min_dte=10, tactical_max_dte=5).validate()
    except ValueError as exc:
        assert str(exc) == "invalid_strategy_dte_windows"
    else:
        raise AssertionError("invalid DTE policy should fail closed")


def test_autonomous_session_defaults_to_v2_cycle() -> None:
    assert session_runner.run_trading_cycle is run_trading_cycle
    assert session_runner.run_trading_cycle is not legacy_cycle.run_trading_cycle
