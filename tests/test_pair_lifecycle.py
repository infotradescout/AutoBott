from __future__ import annotations

from autobott_v2.pair_lifecycle import (
    PairAction,
    PairLegMark,
    PairLifecycleRules,
    PairLifecycleState,
    evaluate_pair_lifecycle,
)


def test_holds_pair_before_primary_has_funded_runner() -> None:
    decision = evaluate_pair_lifecycle(
        primary=PairLegMark(entry_price=1.00, current_price=1.20),
        runner=PairLegMark(entry_price=0.25, current_price=0.20),
    )

    assert decision.action is PairAction.HOLD
    assert not decision.runner_funded


def test_harvests_primary_only_after_profit_pays_runner_and_buffer() -> None:
    decision = evaluate_pair_lifecycle(
        primary=PairLegMark(entry_price=1.00, current_price=1.30),
        runner=PairLegMark(entry_price=0.25, current_price=0.20),
        rules=PairLifecycleRules(funding_buffer_dollars=5.0),
    )

    assert decision.action is PairAction.EXIT_PRIMARY
    assert decision.reason == "primary_profit_funds_runner"
    assert decision.runner_funded
    assert decision.primary_pnl == 30.0
    assert decision.runner_cost == 25.0
    assert decision.funding_surplus == 0.0


def test_pair_loss_exits_both_before_runner_is_funded() -> None:
    decision = evaluate_pair_lifecycle(
        primary=PairLegMark(entry_price=1.00, current_price=0.55),
        runner=PairLegMark(entry_price=0.25, current_price=0.10),
        rules=PairLifecycleRules(max_pair_loss_pct=0.35),
    )

    assert decision.action is PairAction.EXIT_BOTH
    assert decision.reason == "pair_max_loss_reached"


def test_funded_runner_is_not_sold_just_because_it_doubles() -> None:
    decision = evaluate_pair_lifecycle(
        primary=None,
        runner=PairLegMark(entry_price=0.25, current_price=0.50, peak_return_pct=1.0),
        state=PairLifecycleState(
            primary_open=False,
            runner_open=True,
            primary_realized_pnl=30.0,
        ),
    )

    assert decision.action is PairAction.HOLD
    assert decision.runner_funded


def test_funded_runner_exits_on_large_drawdown_from_peak() -> None:
    decision = evaluate_pair_lifecycle(
        primary=None,
        runner=PairLegMark(entry_price=0.25, current_price=0.41, peak_return_pct=1.25),
        state=PairLifecycleState(
            primary_open=False,
            runner_open=True,
            primary_realized_pnl=30.0,
        ),
        rules=PairLifecycleRules(
            funded_runner_trailing_activation_pct=0.75,
            funded_runner_trailing_drawdown_pct=0.35,
        ),
    )

    assert decision.action is PairAction.EXIT_RUNNER
    assert decision.reason == "funded_runner_trailing_drawdown"


def test_unfunded_runner_keeps_a_hard_loss_limit() -> None:
    decision = evaluate_pair_lifecycle(
        primary=None,
        runner=PairLegMark(entry_price=0.25, current_price=0.05),
        state=PairLifecycleState(
            primary_open=False,
            runner_open=True,
            primary_realized_pnl=0.0,
        ),
        rules=PairLifecycleRules(unfunded_runner_stop_loss_pct=0.70),
    )

    assert decision.action is PairAction.EXIT_RUNNER
    assert decision.reason == "unfunded_runner_stop_loss"
