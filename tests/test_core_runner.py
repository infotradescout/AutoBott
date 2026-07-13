from __future__ import annotations

from datetime import date

from autobott_v2.core_runner import CoreRunnerRules, runner_is_funded, select_core_runner_pair
from autobott_v2.phase1_models import OptionContractSnapshot, OptionType, SelectedContract


def _selected_primary(*, ask: float = 1.05, mid: float = 1.00) -> SelectedContract:
    return SelectedContract(
        option_symbol="VIX260715C00017000",
        option_type=OptionType.CALL,
        expiration=date(2026, 7, 15),
        strike=17.0,
        bid=round(mid * 2 - ask, 2),
        ask=ask,
        mid=mid,
        spread_pct=round((ask - (mid * 2 - ask)) / mid, 4),
        open_interest=1000,
        volume=500,
        delta=0.55,
        theta=-0.05,
        vega=0.10,
        implied_volatility=0.50,
        contract_score=0.90,
        reward_risk_ratio=1.0,
        target_exit_mid=1.50,
        stop_exit_mid=0.55,
        exit_rule="primary",
        score_reasons=["primary"],
    )


def _snapshot(symbol: str, strike: float, bid: float, ask: float, delta: float) -> OptionContractSnapshot:
    return OptionContractSnapshot(
        option_symbol=symbol,
        underlying="VIX",
        option_type=OptionType.CALL,
        expiration=date(2026, 7, 15),
        strike=strike,
        bid=bid,
        ask=ask,
        last=None,
        volume=200,
        open_interest=500,
        delta=delta,
        theta=-0.02,
        vega=0.05,
        implied_volatility=0.60,
    )


def test_pair_steps_down_to_cheaper_primary_and_stays_under_total_budget() -> None:
    chain = [
        _snapshot("VIX260715C00017000", 17.0, 0.95, 1.05, 0.55),
        _snapshot("VIX260715C00018000", 18.0, 0.55, 0.65, 0.40),
        _snapshot("VIX260715C00020000", 20.0, 0.20, 0.25, 0.15),
    ]

    pair = select_core_runner_pair(_selected_primary(), chain, rules=CoreRunnerRules(max_group_cost=100.0))

    assert pair is not None
    assert pair.primary.option_symbol == "VIX260715C00018000"
    assert pair.runner.option_symbol == "VIX260715C00020000"
    assert pair.estimated_group_cost == 90.0
    assert pair.primary.option_symbol != pair.runner.option_symbol


def test_pair_prefers_engine_selected_primary_when_full_pair_fits() -> None:
    selected = _selected_primary(ask=0.70, mid=0.65)
    chain = [
        _snapshot("VIX260715C00017000", 17.0, 0.60, 0.70, 0.55),
        _snapshot("VIX260715C00020000", 20.0, 0.20, 0.25, 0.15),
    ]

    pair = select_core_runner_pair(selected, chain)

    assert pair is not None
    assert pair.primary.option_symbol == selected.option_symbol
    assert pair.estimated_group_cost == 95.0


def test_missing_pair_under_budget_fails_closed() -> None:
    chain = [
        _snapshot("VIX260715C00017000", 17.0, 0.95, 1.05, 0.55),
        _snapshot("VIX260715C00020000", 20.0, 0.35, 0.40, 0.15),
    ]

    assert select_core_runner_pair(_selected_primary(), chain) is None


def test_runner_funding_uses_realized_primary_profit() -> None:
    assert runner_is_funded(primary_realized_pnl=27.0, runner_entry_price=0.25, fees=2.0)
    assert not runner_is_funded(primary_realized_pnl=24.0, runner_entry_price=0.25, fees=2.0)
