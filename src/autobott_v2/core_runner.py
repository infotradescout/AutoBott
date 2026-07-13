from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Iterable

from .phase1_models import OptionContractSnapshot, OptionType, SelectedContract


@dataclass(frozen=True)
class CoreRunnerRules:
    """Risk and liquidity rules for one primary plus one convex runner."""

    max_group_cost: float = 100.0
    runner_max_cost_ratio: float = 0.40
    core_max_spread_pct: float = 0.18
    runner_max_spread_pct: float = 0.25
    core_min_open_interest: int = 100
    runner_min_open_interest: int = 50
    core_min_volume: int = 10
    runner_min_volume: int = 1
    core_min_abs_delta: float = 0.25
    primary_target_profit_pct: float = 0.50
    primary_stop_loss_pct: float = 0.45
    runner_target_profit_pct: float = 1.00
    runner_stop_loss_pct: float = 0.70

    def validate(self) -> "CoreRunnerRules":
        if self.max_group_cost <= 0:
            raise ValueError("core_runner_max_group_cost_must_be_positive")
        if not 0 < self.runner_max_cost_ratio < 1:
            raise ValueError("runner_max_cost_ratio_must_be_between_zero_and_one")
        if self.core_max_spread_pct <= 0 or self.runner_max_spread_pct <= 0:
            raise ValueError("core_runner_spread_caps_must_be_positive")
        if min(
            self.core_min_open_interest,
            self.runner_min_open_interest,
            self.core_min_volume,
            self.runner_min_volume,
        ) < 0:
            raise ValueError("core_runner_liquidity_minimums_must_be_nonnegative")
        if not 0 < self.core_min_abs_delta < 1:
            raise ValueError("core_min_abs_delta_must_be_between_zero_and_one")
        return self


@dataclass(frozen=True)
class CoreRunnerPair:
    primary: SelectedContract
    runner: SelectedContract
    estimated_group_cost: float
    max_group_cost: float = 100.0

    def __post_init__(self) -> None:
        if self.primary.option_symbol == self.runner.option_symbol:
            raise ValueError("runner_must_use_distinct_contract")


def load_core_runner_rules() -> CoreRunnerRules:
    return CoreRunnerRules(
        max_group_cost=float(os.getenv("AUTOBOTT_MAX_TRADE_GROUP_COST", "100")),
        runner_max_cost_ratio=float(os.getenv("AUTOBOTT_RUNNER_MAX_COST_RATIO", "0.40")),
        core_max_spread_pct=float(os.getenv("AUTOBOTT_CORE_MAX_SPREAD_PCT", "0.18")),
        runner_max_spread_pct=float(os.getenv("AUTOBOTT_RUNNER_MAX_SPREAD_PCT", "0.25")),
        core_min_open_interest=int(os.getenv("AUTOBOTT_CORE_MIN_OPEN_INTEREST", "100")),
        runner_min_open_interest=int(os.getenv("AUTOBOTT_RUNNER_MIN_OPEN_INTEREST", "50")),
        core_min_volume=int(os.getenv("AUTOBOTT_CORE_MIN_VOLUME", "10")),
        runner_min_volume=int(os.getenv("AUTOBOTT_RUNNER_MIN_VOLUME", "1")),
        core_min_abs_delta=float(os.getenv("AUTOBOTT_CORE_MIN_ABS_DELTA", "0.25")),
        primary_target_profit_pct=float(os.getenv("AUTOBOTT_CORE_TARGET_PROFIT_PCT", "0.50")),
        primary_stop_loss_pct=float(os.getenv("AUTOBOTT_CORE_STOP_LOSS_PCT", "0.45")),
        runner_target_profit_pct=float(os.getenv("AUTOBOTT_RUNNER_TARGET_PROFIT_PCT", "1.00")),
        runner_stop_loss_pct=float(os.getenv("AUTOBOTT_RUNNER_STOP_LOSS_PCT", "0.70")),
    ).validate()


def select_core_runner_pair(
    selected_primary: SelectedContract,
    option_chain: Iterable[OptionContractSnapshot],
    *,
    rules: CoreRunnerRules | None = None,
) -> CoreRunnerPair | None:
    """Select two distinct contracts whose combined ask debit stays under budget.

    The engine-selected contract is preferred when it fits. When it does not,
    the selector can step farther OTM to find a still-useful primary while
    preserving enough budget for a cheaper runner.
    """

    resolved = (rules or load_core_runner_rules()).validate()
    option_type = _option_type_value(selected_primary.option_type)
    expiration = selected_primary.expiration
    chain = [
        contract
        for contract in option_chain
        if _option_type_value(contract.option_type) == option_type and contract.expiration == expiration
    ]
    core_candidates = [contract for contract in chain if _core_is_eligible(contract, resolved)]
    runner_candidates = [contract for contract in chain if _runner_is_liquid(contract, resolved)]
    pairs: list[tuple[tuple[float, ...], CoreRunnerPair]] = []

    for core in core_candidates:
        for runner in runner_candidates:
            if not _is_valid_runner(core, runner, resolved):
                continue
            estimated_group_cost = round((core.ask + runner.ask) * 100, 2)
            if estimated_group_cost > resolved.max_group_cost:
                continue
            primary = _selected_contract(
                core,
                target_profit_pct=resolved.primary_target_profit_pct,
                stop_loss_pct=resolved.primary_stop_loss_pct,
                role="primary",
                max_group_cost=resolved.max_group_cost,
            )
            selected_runner = _selected_contract(
                runner,
                target_profit_pct=resolved.runner_target_profit_pct,
                stop_loss_pct=resolved.runner_stop_loss_pct,
                role="runner",
                max_group_cost=resolved.max_group_cost,
            )
            score = (
                0.0 if core.option_symbol == selected_primary.option_symbol else 1.0,
                abs(abs(core.delta) - 0.50),
                abs(core.strike - selected_primary.strike),
                -abs(runner.delta),
                core.spread_pct + runner.spread_pct,
                -estimated_group_cost,
            )
            pairs.append(
                (
                    score,
                    CoreRunnerPair(
                        primary,
                        selected_runner,
                        estimated_group_cost,
                        max_group_cost=resolved.max_group_cost,
                    ),
                )
            )

    if not pairs:
        return None
    return min(pairs, key=lambda item: item[0])[1]


def runner_is_funded(
    *,
    primary_realized_pnl: float,
    runner_entry_price: float,
    runner_quantity: int = 1,
    fees: float = 0.0,
) -> bool:
    if runner_entry_price < 0 or runner_quantity <= 0 or fees < 0:
        raise ValueError("invalid_runner_funding_inputs")
    runner_cost = runner_entry_price * runner_quantity * 100 + fees
    return primary_realized_pnl >= runner_cost


def _core_is_eligible(contract: OptionContractSnapshot, rules: CoreRunnerRules) -> bool:
    return (
        0 < contract.bid <= contract.ask
        and contract.spread_pct <= rules.core_max_spread_pct
        and contract.open_interest >= rules.core_min_open_interest
        and contract.volume >= rules.core_min_volume
        and abs(contract.delta) >= rules.core_min_abs_delta
    )


def _runner_is_liquid(contract: OptionContractSnapshot, rules: CoreRunnerRules) -> bool:
    return (
        0 < contract.bid <= contract.ask
        and contract.spread_pct <= rules.runner_max_spread_pct
        and contract.open_interest >= rules.runner_min_open_interest
        and contract.volume >= rules.runner_min_volume
    )


def _is_valid_runner(
    core: OptionContractSnapshot,
    runner: OptionContractSnapshot,
    rules: CoreRunnerRules,
) -> bool:
    if runner.option_symbol == core.option_symbol:
        return False
    if runner.ask >= core.ask or runner.ask > core.ask * rules.runner_max_cost_ratio:
        return False
    if abs(runner.delta) >= abs(core.delta):
        return False
    if _option_type_value(core.option_type) == OptionType.CALL.value:
        return runner.strike > core.strike
    return runner.strike < core.strike


def _selected_contract(
    contract: OptionContractSnapshot,
    *,
    target_profit_pct: float,
    stop_loss_pct: float,
    role: str,
    max_group_cost: float,
) -> SelectedContract:
    return SelectedContract(
        option_symbol=contract.option_symbol,
        option_type=contract.option_type,
        expiration=contract.expiration,
        strike=contract.strike,
        bid=contract.bid,
        ask=contract.ask,
        mid=contract.mid,
        spread_pct=contract.spread_pct,
        open_interest=contract.open_interest,
        volume=contract.volume,
        delta=contract.delta,
        theta=contract.theta,
        vega=contract.vega,
        implied_volatility=contract.implied_volatility,
        contract_score=0.0,
        reward_risk_ratio=0.0,
        target_exit_mid=round(contract.mid * (1 + target_profit_pct), 4),
        stop_exit_mid=round(contract.mid * (1 - stop_loss_pct), 4),
        exit_rule=(
            f"{role}_take_profit_at_{int(target_profit_pct * 100)}pct_gain_"
            f"or_stop_at_{int(stop_loss_pct * 100)}pct_loss_on_mid"
        ),
        score_reasons=[
            f"core_runner_{role}",
            "combined_debit_budget_enforced",
            f"max_group_cost={max_group_cost:.2f}",
        ],
    )


def _option_type_value(value: OptionType | str) -> str:
    return value.value if isinstance(value, OptionType) else str(value).lower()
