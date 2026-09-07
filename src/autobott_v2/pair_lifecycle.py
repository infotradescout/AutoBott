from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class PairAction(str, Enum):
    HOLD = "hold"
    EXIT_PRIMARY = "exit_primary"
    EXIT_RUNNER = "exit_runner"
    EXIT_BOTH = "exit_both"


@dataclass(frozen=True)
class PairLifecycleRules:
    """Manage a core plus convex runner as one risk unit.

    The primary exists to monetize the initial directional move. The runner
    exists to preserve convex upside after the primary has paid for it.
    """

    funding_buffer_dollars: float = 5.0
    max_pair_loss_pct: float = 0.35
    unfunded_runner_stop_loss_pct: float = 0.70
    funded_runner_trailing_activation_pct: float = 0.75
    funded_runner_trailing_drawdown_pct: float = 0.35
    catastrophic_runner_stop_loss_pct: float = 0.90

    def validate(self) -> "PairLifecycleRules":
        if self.funding_buffer_dollars < 0:
            raise ValueError("funding_buffer_dollars_must_be_nonnegative")
        for name, value in (
            ("max_pair_loss_pct", self.max_pair_loss_pct),
            ("unfunded_runner_stop_loss_pct", self.unfunded_runner_stop_loss_pct),
            ("funded_runner_trailing_activation_pct", self.funded_runner_trailing_activation_pct),
            ("funded_runner_trailing_drawdown_pct", self.funded_runner_trailing_drawdown_pct),
            ("catastrophic_runner_stop_loss_pct", self.catastrophic_runner_stop_loss_pct),
        ):
            if not 0 < value < 1:
                raise ValueError(f"{name}_must_be_between_zero_and_one")
        if self.catastrophic_runner_stop_loss_pct <= self.unfunded_runner_stop_loss_pct:
            raise ValueError("catastrophic_runner_stop_must_be_wider_than_unfunded_stop")
        return self


@dataclass(frozen=True)
class PairLegMark:
    entry_price: float
    current_price: float
    quantity: int = 1
    peak_return_pct: float | None = None

    def __post_init__(self) -> None:
        if self.entry_price <= 0:
            raise ValueError("entry_price_must_be_positive")
        if self.current_price < 0:
            raise ValueError("current_price_must_be_nonnegative")
        if self.quantity <= 0:
            raise ValueError("quantity_must_be_positive")

    @property
    def entry_cost(self) -> float:
        return self.entry_price * self.quantity * 100.0

    @property
    def market_value(self) -> float:
        return self.current_price * self.quantity * 100.0

    @property
    def pnl(self) -> float:
        return self.market_value - self.entry_cost

    @property
    def return_pct(self) -> float:
        return (self.current_price - self.entry_price) / self.entry_price


@dataclass(frozen=True)
class PairLifecycleState:
    primary_open: bool = True
    runner_open: bool = True
    primary_realized_pnl: float = 0.0


@dataclass(frozen=True)
class PairLifecycleDecision:
    action: PairAction
    reason: str
    pair_entry_cost: float
    pair_mark_value: float
    pair_pnl: float
    pair_return_pct: float
    primary_pnl: float
    runner_pnl: float
    runner_cost: float
    runner_funded: bool
    funding_surplus: float


def evaluate_pair_lifecycle(
    *,
    primary: PairLegMark | None,
    runner: PairLegMark | None,
    state: PairLifecycleState | None = None,
    rules: PairLifecycleRules | None = None,
) -> PairLifecycleDecision:
    """Return one coordinated lifecycle decision for a core-runner pair.

    Before the primary is harvested, the pair is risk-managed on combined
    debit. The primary is harvested only when its profit can recover the
    runner's original cost plus a small buffer. After that point the runner is
    treated as funded and is managed for convex upside rather than a fixed
    take-profit target.
    """

    resolved_state = state or PairLifecycleState()
    resolved_rules = (rules or PairLifecycleRules()).validate()

    if resolved_state.primary_open and primary is None:
        raise ValueError("primary_mark_required_while_primary_open")
    if resolved_state.runner_open and runner is None:
        raise ValueError("runner_mark_required_while_runner_open")

    pair_entry_cost = sum(
        leg.entry_cost
        for leg, is_open in ((primary, resolved_state.primary_open), (runner, resolved_state.runner_open))
        if leg is not None and is_open
    )
    pair_mark_value = sum(
        leg.market_value
        for leg, is_open in ((primary, resolved_state.primary_open), (runner, resolved_state.runner_open))
        if leg is not None and is_open
    )
    pair_pnl = pair_mark_value - pair_entry_cost + resolved_state.primary_realized_pnl
    pair_return_pct = pair_pnl / pair_entry_cost if pair_entry_cost > 0 else 0.0

    primary_pnl = primary.pnl if primary is not None and resolved_state.primary_open else resolved_state.primary_realized_pnl
    runner_pnl = runner.pnl if runner is not None and resolved_state.runner_open else 0.0
    runner_cost = runner.entry_cost if runner is not None else 0.0
    funding_surplus = primary_pnl - runner_cost - resolved_rules.funding_buffer_dollars
    runner_funded = resolved_state.primary_realized_pnl >= runner_cost or (
        resolved_state.primary_open and funding_surplus >= 0
    )

    if resolved_state.primary_open and resolved_state.runner_open and pair_return_pct <= -resolved_rules.max_pair_loss_pct:
        return _decision(
            PairAction.EXIT_BOTH,
            "pair_max_loss_reached",
            pair_entry_cost,
            pair_mark_value,
            pair_pnl,
            pair_return_pct,
            primary_pnl,
            runner_pnl,
            runner_cost,
            runner_funded,
            funding_surplus,
        )

    if resolved_state.primary_open and resolved_state.runner_open and funding_surplus >= 0:
        return _decision(
            PairAction.EXIT_PRIMARY,
            "primary_profit_funds_runner",
            pair_entry_cost,
            pair_mark_value,
            pair_pnl,
            pair_return_pct,
            primary_pnl,
            runner_pnl,
            runner_cost,
            True,
            funding_surplus,
        )

    if resolved_state.runner_open and runner is not None:
        if runner_funded:
            if runner.return_pct <= -resolved_rules.catastrophic_runner_stop_loss_pct:
                return _decision(
                    PairAction.EXIT_RUNNER,
                    "funded_runner_catastrophic_stop",
                    pair_entry_cost,
                    pair_mark_value,
                    pair_pnl,
                    pair_return_pct,
                    primary_pnl,
                    runner_pnl,
                    runner_cost,
                    True,
                    funding_surplus,
                )
            peak = runner.peak_return_pct
            if (
                peak is not None
                and peak >= resolved_rules.funded_runner_trailing_activation_pct
                and runner.return_pct <= peak - resolved_rules.funded_runner_trailing_drawdown_pct
            ):
                return _decision(
                    PairAction.EXIT_RUNNER,
                    "funded_runner_trailing_drawdown",
                    pair_entry_cost,
                    pair_mark_value,
                    pair_pnl,
                    pair_return_pct,
                    primary_pnl,
                    runner_pnl,
                    runner_cost,
                    True,
                    funding_surplus,
                )
        elif runner.return_pct <= -resolved_rules.unfunded_runner_stop_loss_pct:
            return _decision(
                PairAction.EXIT_RUNNER,
                "unfunded_runner_stop_loss",
                pair_entry_cost,
                pair_mark_value,
                pair_pnl,
                pair_return_pct,
                primary_pnl,
                runner_pnl,
                runner_cost,
                False,
                funding_surplus,
            )

    return _decision(
        PairAction.HOLD,
        "pair_holds",
        pair_entry_cost,
        pair_mark_value,
        pair_pnl,
        pair_return_pct,
        primary_pnl,
        runner_pnl,
        runner_cost,
        runner_funded,
        funding_surplus,
    )


def _decision(
    action: PairAction,
    reason: str,
    pair_entry_cost: float,
    pair_mark_value: float,
    pair_pnl: float,
    pair_return_pct: float,
    primary_pnl: float,
    runner_pnl: float,
    runner_cost: float,
    runner_funded: bool,
    funding_surplus: float,
) -> PairLifecycleDecision:
    return PairLifecycleDecision(
        action=action,
        reason=reason,
        pair_entry_cost=round(pair_entry_cost, 2),
        pair_mark_value=round(pair_mark_value, 2),
        pair_pnl=round(pair_pnl, 2),
        pair_return_pct=round(pair_return_pct, 6),
        primary_pnl=round(primary_pnl, 2),
        runner_pnl=round(runner_pnl, 2),
        runner_cost=round(runner_cost, 2),
        runner_funded=runner_funded,
        funding_surplus=round(funding_surplus, 2),
    )
