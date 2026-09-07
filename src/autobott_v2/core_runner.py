from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Iterable

from .hosted_policy import is_hosted_paper_runtime
from .phase1_models import OptionContractSnapshot, OptionType, SelectedContract
from .strategy_policy import HOSTED_STRATEGY_POLICY


@dataclass(frozen=True)
class CoreRunnerRules:
    """Risk, liquidity, and convexity rules for one primary plus one runner."""

    runner_max_cost_ratio: float = 0.40
    runner_target_cost_ratio: float = 0.25
    core_max_spread_pct: float = 0.18
    runner_max_spread_pct: float = 0.25
    core_min_open_interest: int = 100
    runner_min_open_interest: int = 50
    core_min_volume: int = 10
    runner_min_volume: int = 1
    core_min_abs_delta: float = 0.25
    runner_min_abs_delta: float = 0.10
    runner_max_abs_delta: float = 0.35
    runner_target_abs_delta: float = 0.20
    primary_target_profit_pct: float = 0.50
    primary_stop_loss_pct: float = 0.45
    runner_target_profit_pct: float = 1.00
    runner_stop_loss_pct: float = 0.70

    def validate(self) -> "CoreRunnerRules":
        if not 0 < self.runner_max_cost_ratio < 1:
            raise ValueError("runner_max_cost_ratio_must_be_between_zero_and_one")
        if not 0 < self.runner_target_cost_ratio <= self.runner_max_cost_ratio:
            raise ValueError("runner_target_cost_ratio_must_fit_runner_cost_cap")
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
        if not 0 < self.runner_min_abs_delta <= self.runner_target_abs_delta <= self.runner_max_abs_delta < self.core_min_abs_delta:
            raise ValueError("runner_delta_window_invalid")
        return self


@dataclass(frozen=True)
class CoreRunnerPair:
    primary: SelectedContract
    runner: SelectedContract
    estimated_group_cost: float
    runner_cost_ratio: float = 0.0

    def __post_init__(self) -> None:
        if self.primary.option_symbol == self.runner.option_symbol:
            raise ValueError("runner_must_use_distinct_contract")


def load_core_runner_rules() -> CoreRunnerRules:
    if is_hosted_paper_runtime():
        policy = HOSTED_STRATEGY_POLICY
        return CoreRunnerRules(
            runner_max_cost_ratio=policy.runner_max_cost_ratio,
            runner_target_cost_ratio=policy.runner_target_cost_ratio,
            core_max_spread_pct=policy.core_max_spread_pct,
            runner_max_spread_pct=policy.runner_max_spread_pct,
            core_min_open_interest=policy.core_min_open_interest,
            runner_min_open_interest=policy.runner_min_open_interest,
            core_min_volume=policy.core_min_volume,
            runner_min_volume=policy.runner_min_volume,
            core_min_abs_delta=policy.core_min_abs_delta,
            runner_min_abs_delta=policy.runner_min_abs_delta,
            runner_max_abs_delta=policy.runner_max_abs_delta,
            runner_target_abs_delta=policy.runner_target_abs_delta,
            # These legacy fields only populate compatibility metadata. Actual
            # pair exits are controlled by pair_lifecycle.py.
            primary_target_profit_pct=0.50,
            primary_stop_loss_pct=0.45,
            runner_target_profit_pct=1.00,
            runner_stop_loss_pct=policy.unfunded_runner_stop_loss_pct,
        ).validate()
    return CoreRunnerRules(
        runner_max_cost_ratio=float(os.getenv("AUTOBOTT_RUNNER_MAX_COST_RATIO", "0.40")),
        runner_target_cost_ratio=float(os.getenv("AUTOBOTT_RUNNER_TARGET_COST_RATIO", "0.25")),
        core_max_spread_pct=float(os.getenv("AUTOBOTT_CORE_MAX_SPREAD_PCT", "0.18")),
        runner_max_spread_pct=float(os.getenv("AUTOBOTT_RUNNER_MAX_SPREAD_PCT", "0.25")),
        core_min_open_interest=int(os.getenv("AUTOBOTT_CORE_MIN_OPEN_INTEREST", "100")),
        runner_min_open_interest=int(os.getenv("AUTOBOTT_RUNNER_MIN_OPEN_INTEREST", "50")),
        core_min_volume=int(os.getenv("AUTOBOTT_CORE_MIN_VOLUME", "10")),
        runner_min_volume=int(os.getenv("AUTOBOTT_RUNNER_MIN_VOLUME", "1")),
        core_min_abs_delta=float(os.getenv("AUTOBOTT_CORE_MIN_ABS_DELTA", "0.25")),
        runner_min_abs_delta=float(os.getenv("AUTOBOTT_RUNNER_MIN_ABS_DELTA", "0.10")),
        runner_max_abs_delta=float(os.getenv("AUTOBOTT_RUNNER_MAX_ABS_DELTA", "0.35")),
        runner_target_abs_delta=float(os.getenv("AUTOBOTT_RUNNER_TARGET_ABS_DELTA", "0.20")),
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
    """Select one useful primary plus one distinct, cheaper convex runner.

    The engine-selected primary is preferred. The runner must remain cheaper
    and farther out-of-the-money, but it must retain enough delta to participate
    in a real directional move instead of becoming a near-zero-delta lottery.
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
            runner_cost_ratio = runner.ask / core.ask
            primary = _selected_contract(
                core,
                target_profit_pct=resolved.primary_target_profit_pct,
                stop_loss_pct=resolved.primary_stop_loss_pct,
                role="primary",
            )
            selected_runner = _selected_contract(
                runner,
                target_profit_pct=resolved.runner_target_profit_pct,
                stop_loss_pct=resolved.runner_stop_loss_pct,
                role="runner",
            )
            score = (
                0.0 if core.option_symbol == selected_primary.option_symbol else 1.0,
                abs(abs(core.delta) - 0.50),
                abs(core.strike - selected_primary.strike),
                abs(runner_cost_ratio - resolved.runner_target_cost_ratio),
                abs(abs(runner.delta) - resolved.runner_target_abs_delta),
                core.spread_pct + runner.spread_pct,
                -float(runner.open_interest),
                -float(runner.volume),
            )
            pairs.append(
                (
                    score,
                    CoreRunnerPair(
                        primary=primary,
                        runner=selected_runner,
                        estimated_group_cost=estimated_group_cost,
                        runner_cost_ratio=round(runner_cost_ratio, 4),
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
        and (not contract.volume_available or contract.volume >= rules.core_min_volume)
        and abs(contract.delta) >= rules.core_min_abs_delta
    )


def _runner_is_liquid(contract: OptionContractSnapshot, rules: CoreRunnerRules) -> bool:
    abs_delta = abs(contract.delta)
    return (
        0 < contract.bid <= contract.ask
        and contract.spread_pct <= rules.runner_max_spread_pct
        and contract.open_interest >= rules.runner_min_open_interest
        and (not contract.volume_available or contract.volume >= rules.runner_min_volume)
        and rules.runner_min_abs_delta <= abs_delta <= rules.runner_max_abs_delta
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
) -> SelectedContract:
    if role == "primary":
        exit_rule = "primary_harvest_when_profit_funds_runner_then_retain_runner"
    else:
        exit_rule = "runner_hold_for_convex_upside_after_funding_with_trailing_and_dte_risk_controls"
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
        exit_rule=exit_rule,
        score_reasons=[
            f"core_runner_{role}",
            "paper_pair_price_unrestricted",
            "convex_runner_delta_window",
            "pair_lifecycle_exit_policy",
        ],
        volume_available=contract.volume_available,
    )


def _option_type_value(value: OptionType | str) -> str:
    return value.value if isinstance(value, OptionType) else str(value).lower()
