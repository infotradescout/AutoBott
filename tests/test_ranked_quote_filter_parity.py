from copy import deepcopy
from dataclasses import asdict, replace
from datetime import timedelta

import pytest

from autobott_v2.entry_admission import filter_entry_quote_candidates
from autobott_v2.phase1_models import DecisionInput, MarketContext
from autobott_v2.ranked_entry_scan import RankedCapturePlan
from ranked_entry_fixtures import AT
from test_ranked_entry_allocation import contracts, fake_card


def fixture():
    stale = contracts("TEST")[0]
    fresh = replace(stale, option_symbol="TEST261002C00101000", strike=101)
    input_ = DecisionInput("TEST", AT, [], [stale, fresh], MarketContext())
    rows = [{**asdict(contract), "expiration": contract.expiration.isoformat(),
        "option_type": contract.option_type.value, "quote_timestamp": stamp.isoformat()}
        for contract, stamp in [(stale, AT-timedelta(minutes=5)), (fresh, AT)]]
    source = {"ticker": "TEST", "timestamp": AT.isoformat(),
        "underlying_quote": {"bid": 99.99, "ask": 100.01, "quote_timestamp": AT.isoformat()},
        "option_chain": rows}
    return input_, source


def plan_for(input_, source):
    return RankedCapturePlan(capture=lambda **_: "TEST", load=lambda _: source,
        make_input=lambda _: input_, build=lambda value, rules: fake_card("TEST", .9, contract=value.option_chain[0].option_symbol),
        execution_rules=lambda: None, capture_args=lambda: {}, original_priority=lambda symbols, _: symbols,
        filter_candidates=filter_entry_quote_candidates)


def test_ranked_identity_uses_the_same_fresh_contract_set_as_native_admission():
    input_, source = fixture()
    unchanged = deepcopy(source)
    plan = plan_for(input_, source)
    assert plan.rank_symbols(["TEST"], {}) == ["TEST"]
    assert plan.rows["TEST"]["candidate"]
    assert plan.rows["TEST"]["decision"].selected_contract.option_symbol == "TEST261002C00101000"
    assert plan.summary[0]["entry_quote_filter"]["eligible_contracts"] == 1
    assert plan.summary[0]["entry_quote_filter"]["rejected"][0]["option_symbol"] == "TEST261002C00100000"
    assert source == unchanged
    assert len(input_.option_chain) == 2


@pytest.mark.parametrize("defect", ["stale", "crossed"])
def test_invalid_underlying_quote_is_not_ranked_as_an_admitted_candidate(defect):
    input_, source = fixture()
    if defect == "stale":
        source["underlying_quote"]["quote_timestamp"] = (AT-timedelta(minutes=5)).isoformat()
    else:
        source["underlying_quote"]["bid"] = 101
    plan = plan_for(input_, source)
    assert plan.rank_symbols(["TEST"], {}) == ["TEST"]
    assert not plan.rows["TEST"]["candidate"]
    assert plan.summary[0]["selected_contract"] is None
    assert plan.summary[0]["error_type"] == "EntryMarketRejected"
