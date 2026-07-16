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


def _snapshot(
    symbol: str,
    strike: float,
    bid: float,
    ask: float,
    delta: float,
    *,
    volume: int = 200,
    volume_available: bool = True,
) -> OptionContractSnapshot:
    return OptionContractSnapshot(
        option_symbol=symbol,
        underlying="VIX",
        option_type=OptionType.CALL,
        expiration=date(2026, 7, 15),
        strike=strike,
        bid=bid,
        ask=ask,
        last=None,
        volume=volume,
        open_interest=500,
        delta=delta,
        theta=-0.02,
        vega=0.05,
        implied_volatility=0.60,
        volume_available=volume_available,
    )


def test_pair_keeps_engine_primary_even_when_pair_cost_exceeds_100() -> None:
    chain = [
        _snapshot("VIX260715C00017000", 17.0, 0.95, 1.05, 0.55),
        _snapshot("VIX260715C00018000", 18.0, 0.55, 0.65, 0.40),
        _snapshot("VIX260715C00020000", 20.0, 0.20, 0.25, 0.15),
    ]

    pair = select_core_runner_pair(_selected_primary(), chain, rules=CoreRunnerRules())

    assert pair is not None
    assert pair.primary.option_symbol == "VIX260715C00017000"
    assert pair.runner.option_symbol == "VIX260715C00020000"
    assert pair.estimated_group_cost == 130.0
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


def test_missing_structurally_valid_runner_fails_closed() -> None:
    chain = [
        _snapshot("VIX260715C00017000", 17.0, 0.95, 1.05, 0.55),
        _snapshot("VIX260715C00020000", 20.0, 0.45, 0.50, 0.15),
    ]

    assert select_core_runner_pair(_selected_primary(), chain) is None


def test_pair_uses_open_interest_when_live_snapshot_volume_is_unavailable() -> None:
    chain = [
        _snapshot(
            "VIX260715C00017000",
            17.0,
            0.95,
            1.05,
            0.55,
            volume=0,
            volume_available=False,
        ),
        _snapshot(
            "VIX260715C00020000",
            20.0,
            0.20,
            0.25,
            0.15,
            volume=0,
            volume_available=False,
        ),
    ]

    assert select_core_runner_pair(_selected_primary(), chain) is not None


def test_runner_funding_uses_realized_primary_profit() -> None:
    assert runner_is_funded(primary_realized_pnl=27.0, runner_entry_price=0.25, fees=2.0)
    assert not runner_is_funded(primary_realized_pnl=24.0, runner_entry_price=0.25, fees=2.0)
