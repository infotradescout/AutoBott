from dataclasses import asdict, replace
from datetime import UTC, date, datetime, timedelta

import pytest

from autobott_v2.core_runner import load_core_runner_rules, select_core_runner_pair
from autobott_v2.phase1_engine import select_contract
from autobott_v2.phase1_models import (CycleAssessment, CycleStatus, DecisionInput, DirectionBias,
    DirectionResult, ExecutionLayer, MarketBar, MarketContext, OptionContractSnapshot, OptionType,
    SelectedContract, TradeSetup, VolatilityResult)
from autobott_v2.phase1_snapshot_capture import _normalize_option_chain
from autobott_v2.strategy_policy import HOSTED_STRATEGY_POLICY, StrategyPolicy
from autobott_v2.trading_cycle import _hosted_capture_rules, _hosted_execution_rules


MONDAY = date(2026, 9, 21)


def raw_pair(expiration, kind="call", *, wide=False):
    code = "C" if kind == "call" else "P"
    direction = 1 if kind == "call" else -1
    rows = {}
    for strike, delta, bid, ask in [(100, .55, .78, .82), (100 + direction * 5, .20, .20, .22)]:
        name = f"TEST{expiration:%y%m%d}{code}{strike * 1000:08d}"
        rows[name] = {
            "latestQuote": {"bp": .10 if wide else bid, "ap": 2.0 if wide else ask,
                            "t": "2026-09-21T15:00:00Z"},
            "greeks": {"delta": delta * direction, "theta": -.002, "vega": .10, "iv": .25},
            "details": {"expiration_date": expiration.isoformat(), "strike_price": strike, "type": kind},
            "dailyBar": {"v": 500}, "open_interest": 1000,
        }
    return rows


def decision_input(rows, at):
    contracts = [OptionContractSnapshot(
        option_symbol=r["option_symbol"], underlying=r["underlying"], expiration=date.fromisoformat(r["expiration"]),
        strike=r["strike"], option_type=OptionType(r["option_type"]), bid=r["bid"], ask=r["ask"], last=r["last"],
        volume=r["volume"], open_interest=r["open_interest"], delta=r["delta"], theta=r["theta"],
        vega=r["vega"], implied_volatility=r["implied_volatility"], volume_available=r["volume_available"],
    ) for r in rows]
    stamp = datetime.combine(at, datetime.min.time(), tzinfo=UTC) + timedelta(hours=15)
    bars = [MarketBar(stamp - timedelta(hours=34-i), 100, 100.1, 99.9, 100, 100000) for i in range(35)]
    return DecisionInput("TEST", stamp, bars, contracts, MarketContext())


def score(input_, rules, kind):
    bullish = kind == "call"
    direction = DirectionResult(DirectionBias.BULLISH if bullish else DirectionBias.BEARISH,
        .8 if bullish else -.8, .8, .5, .2, False, "synthetic fixed direction")
    cycle = CycleAssessment(CycleStatus.MEDIUM, 3 if bullish else -3, 2, 2, 10, 10,
        False, False, False, False, False, "valley", "fixture", "fixture")
    volatility = VolatilityResult(.8, .5, 1.0, False, False, "synthetic fixed volatility")
    return select_contract(input_, direction, volatility, rules, ExecutionLayer.TACTICAL,
        TradeSetup.BULLISH_CONTINUATION if bullish else TradeSetup.BEARISH_CONTINUATION, cycle)


@pytest.mark.parametrize("kind", ["call", "put"])
def test_observed_weekly_chain_reaches_native_tactical_selector_and_pair(monkeypatch, kind):
    monkeypatch.setenv("RENDER", "true")
    capture, execution = _hosted_capture_rules(), _hosted_execution_rules()
    source = {**raw_pair(date(2026, 9, 25), kind), **raw_pair(date(2026, 10, 2), kind),
              **raw_pair(date(2026, 10, 9), kind)}
    old_rows = _normalize_option_chain(symbol="TEST", option_snapshots=source, underlying_price=100,
        as_of_date=MONDAY, rules=replace(capture, tactical_max_dte=10))
    assert score(decision_input(old_rows, MONDAY), replace(execution, intraday_max_dte=10), kind) is None
    new_rows = _normalize_option_chain(symbol="TEST", option_snapshots=source, underlying_price=100,
        as_of_date=MONDAY, rules=capture)
    input_ = decision_input(new_rows, MONDAY)
    chosen = score(input_, execution, kind)
    assert chosen is not None, "listed eleven-day weekly contract still inaccessible"
    assert chosen.contract.expiration == date(2026, 10, 2)
    assert all(r["expiration"] != "2026-09-25" for r in new_rows)
    primary = SelectedContract.from_score(chosen, execution)
    pair = select_core_runner_pair(primary, input_.option_chain, rules=load_core_runner_rules())
    assert pair is not None
    assert pair.primary.option_symbol == primary.option_symbol
    assert pair.runner.option_symbol != primary.option_symbol
    assert pair.runner.expiration == primary.expiration


@pytest.mark.parametrize("offset", range(5))
def test_same_listed_friday_is_reachable_each_weekday(monkeypatch, offset):
    monkeypatch.setenv("RENDER", "true")
    at = MONDAY + timedelta(days=offset)
    rows = _normalize_option_chain(symbol="TEST", option_snapshots=raw_pair(date(2026, 10, 2)),
        underlying_price=100, as_of_date=at, rules=_hosted_capture_rules())
    assert score(decision_input(rows, at), _hosted_execution_rules(), "call") is not None


@pytest.mark.parametrize("dte,allowed", [(0, False), (4, False), (5, True), (10, True),
                                         (11, True), (12, True), (13, True), (14, False)])
def test_actual_tactical_selector_preserves_bounds(monkeypatch, dte, allowed):
    monkeypatch.setenv("RENDER", "true")
    expiration = MONDAY + timedelta(days=dte)
    # Full normalization permits inspection of out-of-window contracts; only
    # the native execution rules decide tactical eligibility in this test.
    capture = replace(_hosted_capture_rules(), option_chain_min_dte=0)
    rows = _normalize_option_chain(symbol="TEST", option_snapshots=raw_pair(expiration),
        underlying_price=100, as_of_date=MONDAY, rules=capture, select_subset=False)
    assert (score(decision_input(rows, MONDAY), _hosted_execution_rules(), "call") is not None) is allowed


def test_forward_extension_does_not_relax_spreads(monkeypatch):
    monkeypatch.setenv("RENDER", "true")
    rows = _normalize_option_chain(symbol="TEST", option_snapshots=raw_pair(date(2026, 10, 2), wide=True),
        underlying_price=100, as_of_date=MONDAY, rules=_hosted_capture_rules(), select_subset=False)
    assert score(decision_input(rows, MONDAY), _hosted_execution_rules(), "call") is None


def test_only_primary_maximum_changes_in_central_policy():
    before = asdict(StrategyPolicy(tactical_max_dte=10))
    after = asdict(HOSTED_STRATEGY_POLICY)
    assert {key for key in before if before[key] != after[key]} == {"tactical_max_dte"}
    assert after["tactical_max_dte"] == 13
    assert after["tactical_min_dte"] == 5
    assert after["rider_min_dte"] == 14
    assert after["max_open_legs"] == 6
    assert after["max_position_cost"] == 1000
    assert after["max_daily_loss"] == 750
    assert after["exit_min_dte"] == 2
