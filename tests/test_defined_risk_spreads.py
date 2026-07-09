from __future__ import annotations

import json
from datetime import UTC, date, datetime

from autobott_v2.defined_risk_spreads import (
    DefinedRiskSpreadRules,
    append_defined_risk_spread_candidate,
    select_defined_risk_spread,
)
from autobott_v2.phase1_models import (
    CycleProfile,
    DecisionInput,
    DirectionBias,
    MarketBar,
    MarketContext,
    OptionContractSnapshot,
    OptionType,
)


def _contract(symbol: str, strike: float, option_type: OptionType, bid: float, ask: float, delta: float) -> OptionContractSnapshot:
    return OptionContractSnapshot(
        option_symbol=symbol,
        underlying="AAPL",
        expiration=date(2026, 7, 3),
        strike=strike,
        option_type=option_type,
        bid=bid,
        ask=ask,
        last=(bid + ask) / 2,
        volume=200,
        open_interest=1000,
        delta=delta,
        theta=-0.02,
        vega=0.05,
        implied_volatility=0.25,
    )


def _decision_input(option_chain: list[OptionContractSnapshot]) -> DecisionInput:
    bars = [
        MarketBar(
            timestamp=datetime(2026, 7, 1, 15, idx, tzinfo=UTC),
            open=100 + idx * 0.1,
            high=100 + idx * 0.15,
            low=100 + idx * 0.05,
            close=100 + idx * 0.1,
            volume=100000,
        )
        for idx in range(35)
    ]
    return DecisionInput(
        ticker="AAPL",
        timestamp=datetime(2026, 7, 1, 15, 35, tzinfo=UTC),
        market_bars=bars,
        option_chain=option_chain,
        context=MarketContext(),
        cycle_profile=CycleProfile(),
    )


def test_select_defined_risk_spread_builds_bull_put_credit_spread() -> None:
    decision_input = _decision_input(
        [
            _contract("AAPL260703P00100000", 100, OptionType.PUT, 0.45, 0.50, -0.35),
            _contract("AAPL260703P00099000", 99, OptionType.PUT, 0.18, 0.22, -0.18),
        ]
    )

    candidate = select_defined_risk_spread(decision_input, DirectionBias.BULLISH, rules=DefinedRiskSpreadRules())

    assert candidate is not None
    assert candidate.strategy == "bull_put_spread"
    assert candidate.short_leg.option_symbol == "AAPL260703P00100000"
    assert candidate.long_leg.option_symbol == "AAPL260703P00099000"
    assert candidate.net_credit == 0.23
    assert candidate.max_risk == 77.0
    assert candidate.profit_target_debit == 0.12


def test_select_defined_risk_spread_builds_bear_call_credit_spread() -> None:
    decision_input = _decision_input(
        [
            _contract("AAPL260703C00100000", 100, OptionType.CALL, 0.46, 0.50, 0.36),
            _contract("AAPL260703C00101000", 101, OptionType.CALL, 0.17, 0.21, 0.18),
        ]
    )

    candidate = select_defined_risk_spread(decision_input, DirectionBias.BEARISH, rules=DefinedRiskSpreadRules())

    assert candidate is not None
    assert candidate.strategy == "bear_call_spread"
    assert candidate.short_leg.option_symbol == "AAPL260703C00100000"
    assert candidate.long_leg.option_symbol == "AAPL260703C00101000"


def test_append_defined_risk_spread_candidate_persists_jsonl(tmp_path) -> None:
    candidate = select_defined_risk_spread(
        _decision_input(
            [
                _contract("AAPL260703P00100000", 100, OptionType.PUT, 0.45, 0.50, -0.35),
                _contract("AAPL260703P00099000", 99, OptionType.PUT, 0.18, 0.22, -0.18),
            ]
        ),
        DirectionBias.BULLISH,
    )

    append_defined_risk_spread_candidate(candidate, decision_id="decision-1", journal_path=tmp_path / "spreads.jsonl")

    rows = [json.loads(line) for line in (tmp_path / "spreads.jsonl").read_text(encoding="utf-8").splitlines()]
    assert rows[0]["schema_version"] == "defined_risk_spread.v1"
    assert rows[0]["decision_id"] == "decision-1"
    assert rows[0]["candidate"]["strategy"] == "bull_put_spread"
