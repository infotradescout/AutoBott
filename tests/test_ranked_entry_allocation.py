from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime, timedelta
import json
from types import SimpleNamespace

import pytest

from autobott_v2 import trading_cycle as legacy
from autobott_v2 import trading_cycle_v2 as adapter
from autobott_v2.phase1_models import (DecisionCard, DecisionStatus, DirectionBias, DirectionResult,
    RegimeLabel, RegimeResult, CycleAssessment, CycleStatus, VolatilityResult, TradeSetup,
    ExecutionLayer, SelectedContract, OptionContractSnapshot, OptionType, ContractScore, Phase1Rules)
from autobott_v2.ranked_entry_scan import RankedCapturePlan
from ranked_entry_fixtures import AT, run_combined


@dataclass
class Card:
    ticker: str
    confidence_score: float
    selected_contract: object
    decision: DecisionStatus = DecisionStatus.TRADE_CANDIDATE
    reason_codes: list = field(default_factory=list)
    blocked_reason: str | None = None


def fake_card(symbol, score, ask=1, contract=None):
    return Card(symbol, score, SimpleNamespace(option_symbol=contract or symbol + "261002C00100000",
        ask=ask, contract_score=.8, reward_risk_ratio=1, spread_pct=.05))


def test_ranked_candidates_refresh_and_non_candidates_reuse_capture():
    cards = {"WEAK": fake_card("WEAK", .6), "STRONG": fake_card("STRONG", .9),
             "NONE": replace(fake_card("NONE", .99), decision=DecisionStatus.NO_TRADE)}
    calls = []
    def capture(*, symbol, **kwargs):
        calls.append((symbol, kwargs))
        return symbol
    plan = RankedCapturePlan(capture=capture, load=lambda path: path.name,
        make_input=lambda name: SimpleNamespace(ticker=name), build=lambda value, rules: cards[value.ticker],
        execution_rules=lambda: None, capture_args=lambda: {}, original_priority=lambda symbols, _: symbols)
    assert plan.rank_symbols(["WEAK", "NONE", "STRONG"], {}) == ["STRONG", "WEAK", "NONE"]
    for symbol in ("STRONG", "WEAK", "NONE"):
        plan.capture_for_execution(symbol=symbol)
    assert [symbol for symbol, _ in calls] == ["WEAK", "NONE", "STRONG", "STRONG", "WEAK"]
    assert "captured_at_utc" in calls[3][1]


@pytest.mark.parametrize("change,reason", [("price", "ranked_candidate_original_price_allowance_exceeded"),
                                         ("contract", "ranked_candidate_contract_changed")])
def test_rank_is_not_permission_to_switch_or_increase_price(change, reason):
    old = fake_card("TEST", .9)
    current = fake_card("TEST", .95, ask=1.1 if change == "price" else 1,
                        contract="TEST261002C00105000" if change == "contract" else None)
    plan = RankedCapturePlan(capture=lambda **_: "unused", load=lambda _: None, make_input=lambda _: None,
        build=lambda *_: current, execution_rules=lambda: None, capture_args=lambda: {}, original_priority=lambda symbols, _: symbols)
    plan.rows["TEST"] = {"decision": old, "candidate": True}
    observed = plan.build_for_execution(SimpleNamespace(ticker="TEST"), None)
    assert observed.decision is DecisionStatus.NO_TRADE
    assert observed.blocked_reason == reason
    assert current.decision is DecisionStatus.TRADE_CANDIDATE
    assert old.selected_contract.ask == 1


def contracts(symbol):
    return [OptionContractSnapshot(
        option_symbol=f"{symbol}261002C{strike * 1000:08d}", underlying=symbol, expiration=date(2026, 10, 2),
        strike=strike, option_type=OptionType.CALL, bid=bid, ask=ask, last=(bid+ask)/2,
        volume=500, open_interest=1000, delta=delta, theta=-.002, vega=.1, implied_volatility=.25)
        for strike, bid, ask, delta in [(100, .78, .82, .55), (105, .20, .22, .20)]]


def native_card(input_, rules):
    """Fixed ranking scores isolate allocation; they are not a fitted strategy."""
    symbol = input_.ticker
    primary = next(c for c in input_.option_chain if c.strike == 100)
    selected = SelectedContract.from_score(ContractScore(primary, .8, 1, []), Phase1Rules())
    return DecisionCard(schema_version="phase1_decision_card.v1", decision_id=symbol + "-decision", ticker=symbol,
        timestamp=input_.timestamp, regime=RegimeResult(RegimeLabel.TREND, [RegimeLabel.TREND], .8, "fixture"),
        direction=DirectionResult(DirectionBias.BULLISH, .8, .8, .4, .2, False, "fixture"),
        cycle=CycleAssessment(CycleStatus.MEDIUM, 3, 2, 2, 10, 10, False, False, False, False, False, "valley", "fixture", "fixture"),
        volatility=VolatilityResult(.8, .5, 1, False, False, "fixture"), selected_contract=selected,
        tactical_contract=selected, rider_contract=None, trade_setup=TradeSetup.BULLISH_CONTINUATION,
        execution_layer=ExecutionLayer.TACTICAL, decision=DecisionStatus.TRADE_CANDIDATE,
        blocked_reason=None, reason_codes=[], confidence_score=.9 if symbol == "STRONG" else .6, explanation="synthetic ranking input")


def test_actual_cycle_allocates_to_higher_scoring_later_symbol_before_six_slot_cutoff(monkeypatch, tmp_path):
    result, broker, ledger, transport, trace = run_combined(monkeypatch, tmp_path, native_card)
    assert result.symbols == ["STRONG", "WEAK"]
    assert len(result.orders_submitted) == 2, result.skipped
    assert all(row["symbol"] == "STRONG" for row in result.orders_submitted)
    assert len(transport.posts) == 2
    assert all(row["type"] == "limit" for row in transport.posts)
    assert result.execution_rejected_count_by_reason["portfolio_premium_budget_exceeded"] == 1
    assert result.trade_attempted_count == 1
    assert trace[:3] == [("capture", "WEAK"), ("capture", "STRONG"), ("capture", "STRONG")]
    assert next(i for i, row in enumerate(trace) if row[0] == "refresh") < next(i for i, row in enumerate(trace) if row[0] == "POST")
    ranked = next(row for row in result.execution_outcomes if row["disposition"] == "ranked_entry_scan")
    assert ranked["symbols"][0]["symbol"] == "STRONG"
    assert not ranked["ranking_is_profitability_evidence"]
    accounting = next(row for row in result.execution_outcomes if row["disposition"] == "trade_outcome_learning_summary")
    assert accounting["ok"]
    revalidated = [r for r in result.execution_outcomes if r["disposition"] == "entry_market_revalidated"]
    assert len(revalidated) == 1
    assert revalidated[0]["primary_option_symbol"] == result.orders_submitted[0]["option_symbol"]


@pytest.mark.parametrize("defect", ["stale", "chased"])
def test_ranked_budgeted_candidate_still_cannot_bypass_final_quote_admission(monkeypatch, tmp_path, defect):
    result, broker, ledger, transport, trace = run_combined(monkeypatch, tmp_path, native_card, refresh_defect=defect)
    assert result.scanner_candidates_count == 2
    assert result.orders_submitted == []
    assert result.trade_attempted_count == 0
    assert not transport.posts
    assert any(event[0] == "refresh" for event in trace)
    assert all(row["reason"].startswith("entry_") for row in result.skipped), result.skipped


def test_budgeted_pair_binds_and_observes_only_its_actual_primary_fill(monkeypatch, tmp_path):
    from autobott_v2.primary_fill_capture import poll_primary_fills
    result, broker, ledger, transport, trace = run_combined(monkeypatch, tmp_path, native_card, observe=True)
    assert len(transport.posts) == 2, result.skipped
    root = tmp_path / "artifacts" / "primary_followthrough"
    watches = list(root.glob("*.json"))
    assert len(watches) == 1, result.execution_outcomes
    observed = poll_primary_fills(root, broker, now_fn=lambda: AT+timedelta(seconds=3))
    assert observed["filled"] == 1, observed
    row = json.loads(watches[0].read_text())
    assert row["fill_capture_status"] == "filled"
    assert row["case"]["fills"][0]["option_symbol"] == "STRONG261002C00100000"
    assert row["case"]["fills"][0]["price"] == .82
    assert len(row["case"]["fills"]) == 1
    assert len(transport.posts) == 2
