"""Synthetic quote-provenance and entry-permission regressions."""
from copy import deepcopy
from dataclasses import replace
from datetime import date
import pytest

from autobott_v2.phase1_snapshot_capture import CaptureRules, _normalize_option_chain, _normalize_stock_quote
from autobott_v2.phase1_execution_sim import simulate_execution
from autobott_v2.phase1_models import DecisionStatus
from test_phase1_execution_sim import _execution_ready_card

SYMBOL = "TEST260925C00100000"
STAMP = "2026-09-16T14:00:00Z"


def option_snapshot():
    return {
        "latestQuote": {"bp": .95, "ap": 1.05, "t": STAMP},
        "latestTrade": {"p": 1.0, "t": STAMP},
        "details": {"expiration_date": "2026-09-25", "type": "call", "strike_price": 100},
        "greeks": {"iv": .3, "delta": .55, "theta": -.02, "vega": .1},
        "dailyBar": {"v": 100}, "open_interest": 500,
    }


def normalize(snapshot, diagnostics=None):
    kwargs = {} if diagnostics is None else {"quote_rejections": diagnostics}
    return _normalize_option_chain(symbol="TEST", option_snapshots={SYMBOL: snapshot},
        underlying_price=100, as_of_date=date(2026, 9, 16), rules=CaptureRules(),
        select_subset=False, **kwargs)


@pytest.mark.parametrize("stamp", [None, "", "not-a-time", "2026-09-16T14:00:00"])
def test_missing_or_invalid_quote_time_cannot_borrow_trade_time_or_now(stamp):
    raw = option_snapshot()
    raw["latestQuote"].pop("t")
    if stamp is not None:
        raw["latestQuote"]["t"] = stamp
    with pytest.raises(ValueError):
        normalize(raw)


def test_old_quote_time_is_preserved_despite_new_trade():
    raw = option_snapshot()
    raw["latestQuote"]["t"] = "2026-09-15T14:00:00Z"
    assert normalize(raw)[0]["quote_timestamp"].startswith("2026-09-15T14:00:00")


@pytest.mark.parametrize("change", [
    {"bp": float("nan")}, {"ap": float("inf")}, {"bp": True},
    {"bp": -1}, {"bp": 2}, {"ap": 0}, {"bp": None},
])
def test_invalid_option_quotes_never_enter_candidate_chain(change):
    raw = option_snapshot()
    raw["latestQuote"].update(change)
    with pytest.raises(ValueError):
        normalize(raw)


def test_real_zero_bid_remains_observed_not_replaced_by_last_trade():
    raw = option_snapshot()
    raw["latestQuote"]["bp"] = 0
    assert normalize(raw)[0]["bid"] == 0


def test_valid_quote_and_input_identity_preserved():
    raw = option_snapshot()
    before = deepcopy(raw)
    result = normalize(raw)[0]
    assert result["option_symbol"] == SYMBOL
    assert (result["bid"], result["ask"]) == (.95, 1.05)
    assert raw == before


@pytest.mark.parametrize("raw", [
    {}, {"bp": 99, "ap": 101}, {"bp": 99, "ap": 101, "t": "bad"},
    {"bp": 0, "ap": 101, "t": STAMP}, {"bp": 102, "ap": 101, "t": STAMP},
    {"bp": float("nan"), "ap": 101, "t": STAMP},
    {"ap": 101, "t": STAMP},
])
def test_missing_stock_quote_is_not_fabricated_from_previous_bar(raw):
    with pytest.raises(ValueError):
        _normalize_stock_quote("TEST", {"TEST": raw}, fallback_price=100)


def test_valid_stock_quote_uses_observed_time_and_prices():
    result = _normalize_stock_quote("TEST", {"TEST": {"bp": 99, "ap": 101, "t": STAMP}}, fallback_price=300)
    assert (result["bid"], result["ask"]) == (99, 101)
    assert result["quote_timestamp"].startswith("2026-09-16T14:00:00")


@pytest.mark.parametrize("status", [s for s in DecisionStatus if s != DecisionStatus.TRADE_CANDIDATE])
def test_unapproved_card_with_selected_contract_cannot_create_a_simulated_entry(status):
    card = replace(_execution_ready_card(), decision=status)
    assert card.selected_contract is not None
    events = simulate_execution(card)
    assert events
    assert all(not event.filled for event in events)
    assert all(event.exit_reason == "decision_not_trade_candidate" for event in events)


def test_approved_card_can_still_fill_under_same_simulation_rules():
    card = _execution_ready_card()
    assert card.decision == DecisionStatus.TRADE_CANDIDATE
    assert any(event.filled for event in simulate_execution(card))


def test_invalid_quote_exclusion_preserves_machine_readable_reason():
    good = option_snapshot()
    bad = deepcopy(good)
    bad["latestQuote"].pop("t")
    rejected = []
    rows = _normalize_option_chain(symbol="TEST", option_snapshots={SYMBOL: good, "TEST260925C00105000": bad},
        underlying_price=100, as_of_date=date(2026, 9, 16), rules=CaptureRules(),
        select_subset=False, quote_rejections=rejected)
    assert [row["option_symbol"] for row in rows] == [SYMBOL]
    assert rejected == [{"option_symbol": "TEST260925C00105000", "reason": "missing_observed_quote_timestamp"}]


def test_quote_aliases_and_timezone_offsets_describe_same_observation():
    from autobott_v2.quote_observation import observed_quote_fields
    a = observed_quote_fields({"bp": 1, "ap": 2, "t": STAMP}, allow_zero_bid=False)
    b = observed_quote_fields({"bid_price": 1, "ask_price": 2, "timestamp": "2026-09-16T09:00:00-05:00"}, allow_zero_bid=False)
    assert a == b


def test_invalid_primary_field_cannot_be_hidden_by_a_secondary_alias():
    from autobott_v2.quote_observation import observed_quote_fields
    with pytest.raises(ValueError):
        observed_quote_fields({"bp": None, "bid_price": 1, "ap": 2, "t": STAMP}, allow_zero_bid=False)


def test_capture_uses_quote_receipt_time_not_earlier_scan_schedule(tmp_path):
    import json
    from datetime import UTC, datetime, timedelta
    from autobott_v2.phase1_snapshot_capture import capture_symbol_snapshot
    from autobott_v2.phase1_validate import _decision_input_from_snapshot
    from autobott_v2.phase1_engine_v2 import build_decision_card
    from test_phase1_snapshot_capture import FakeCaptureClient
    start = datetime(2026, 6, 30, 13, 30, tzinfo=UTC)
    readings = iter([100.0, 105.0])
    file = capture_symbol_snapshot(symbol="SPY", corpus_root=tmp_path,
        scheduled_market_time=start, captured_at_utc=start, corpus_type="test_fixture",
        market_timezone="America/New_York", volatility_proxy_symbol="VIXY",
        data_client=FakeCaptureClient(), rules=CaptureRules(), monotonic_fn=lambda: next(readings))
    row = json.loads(__import__("pathlib").Path(file).read_text())
    observed = datetime.fromisoformat(row["timestamp"].replace("Z", "+00:00"))
    assert observed == start + timedelta(seconds=5)
    assert row["scheduled_timestamp_utc"] == "2026-06-30T13:30:00Z"
    assert row["capture_elapsed_seconds"] == 5
    parsed = _decision_input_from_snapshot(row)
    assert parsed.timestamp == observed
    assert build_decision_card(parsed).timestamp == observed
    assert all(b.timestamp <= observed for b in parsed.market_bars)


@pytest.mark.parametrize("readings", [[105.0, 100.0], [100.0, float("nan")]])
def test_capture_rejects_invalid_elapsed_clock(tmp_path, readings):
    from datetime import UTC, datetime
    from autobott_v2.phase1_snapshot_capture import capture_symbol_snapshot
    from test_phase1_snapshot_capture import FakeCaptureClient
    start = datetime(2026, 6, 30, 13, 30, tzinfo=UTC)
    values = iter(readings)
    with pytest.raises(ValueError, match="invalid_capture_elapsed_time"):
        capture_symbol_snapshot(symbol="SPY", corpus_root=tmp_path,
            scheduled_market_time=start, captured_at_utc=start, corpus_type="test_fixture",
            market_timezone="America/New_York", volatility_proxy_symbol="VIXY",
            data_client=FakeCaptureClient(), rules=CaptureRules(), monotonic_fn=lambda: next(values))
