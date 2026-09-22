"""Synthetic fill-linkage contracts; no broker calls or journal writes."""
from copy import deepcopy
from datetime import timedelta
import pytest

from autobott_v2.primary_entry_study import digest, run_primary_study
from test_primary_entry_study import make_case, protocol, tape, PRIMARY


def test_recorded_market_cannot_score_an_unlinked_supplied_fill(tmp_path):
    source = tape(make_case(tmp_path))
    source["source_kind"] = "recorded_market"  # Deliberately adversarial label.
    with pytest.raises(ValueError, match="linked_primary_fill_required"):
        run_primary_study(source, protocol(fill_basis="recorded_primary_fill"), engine="v2")


def linked_case(tmp_path):
    case = make_case(tmp_path)
    at = case["refresh"]["received_at"]
    binding = {"decision_id": "synthetic-admission-1", "primary_option_symbol": PRIMARY,
               "checked_at": at, "snapshot_hash": digest(case["snapshot"])}
    receipt = {"schema_version": "primary_submission_receipt.v1",
               "decision_id": binding["decision_id"], "option_symbol": PRIMARY,
               "snapshot_hash": binding["snapshot_hash"], "admission_checked_at": at,
               "leg_role": "primary", "side": "buy_to_open", "quantity": 1,
               "account_scope": "alpaca:paper:synthetic-account", "submitted_at": at,
               "broker_order_id": "synthetic-only-order", "client_order_id": "synthetic-client"}
    observation = {"schema_version": "broker_order_observation.v1",
                   "account_scope": receipt["account_scope"], "received_at": at,
                   "order": {"id": receipt["broker_order_id"], "client_order_id": receipt["client_order_id"],
                             "symbol": PRIMARY, "side": "buy", "position_intent": "buy_to_open",
                             "qty": "1", "filled_qty": "1", "filled_avg_price": "2.6",
                             "filled_at": at, "status": "filled"}}
    case.update(recorded_admission=binding, primary_submission_receipts=[receipt],
                broker_order_observations=[observation])
    return case


def link(case):
    from autobott_v2.primary_fill_linkage import linked_primary_fill
    return linked_primary_fill(case, PRIMARY)


def test_linked_fill_retains_primary_price_and_does_not_modify_sources(tmp_path):
    case = linked_case(tmp_path)
    before = deepcopy(case)
    result = link(case)
    assert result["fill"] == case["fills"][0]
    assert result["identity_links_consistent"] is True
    assert result["source_authenticity_verified"] is False
    assert result["broker_writes"] == result["journal_writes"] == 0
    assert case == before


@pytest.mark.parametrize("field,value", [
    ("account_scope", ""), ("account_scope", "alpaca:live:synthetic-account"),
    ("decision_id", "another-decision"), ("option_symbol", "runner"),
    ("snapshot_hash", "wrong-snapshot"), ("leg_role", "runner"),
    ("side", "sell_to_close"), ("quantity", 2), ("quantity", True),
    ("broker_order_id", ""), ("client_order_id", ""),
    ("submitted_at", "2026-07-01T00:00:00+00:00"),
    ("admission_checked_at", "2026-07-01T00:00:00+00:00"),
    ("schema_version", "unknown"),
])
def test_mismatched_submission_is_not_inferred_by_symbol_or_time(tmp_path, field, value):
    case = linked_case(tmp_path)
    case["primary_submission_receipts"][0][field] = value
    with pytest.raises(ValueError):
        link(case)


@pytest.mark.parametrize("field,value", [
    ("id", "another-order"), ("client_order_id", "another-client"),
    ("symbol", "runner"), ("side", "sell"), ("position_intent", "sell_to_close"),
    ("status", "partially_filled"), ("status", "canceled"),
    ("qty", "2"), ("filled_qty", "0.5"), ("filled_qty", True),
    ("filled_avg_price", "NaN"), ("filled_avg_price", "Infinity"),
    ("filled_avg_price", "-1"), ("filled_avg_price", "1e9999"),
    ("filled_at", "2026-07-01T00:00:00+00:00"),
    ("filled_at", "2026-07-01T23:59:00+00:00"),
])
def test_broker_order_mismatches_and_incomplete_quantities_cannot_score(tmp_path, field, value):
    case = linked_case(tmp_path)
    case["broker_order_observations"][0]["order"][field] = value
    with pytest.raises(ValueError):
        link(case)


@pytest.mark.parametrize("key", ["recorded_admission", "primary_submission_receipts", "broker_order_observations"])
def test_missing_link_is_not_backfilled(tmp_path, key):
    case = linked_case(tmp_path)
    del case[key]
    with pytest.raises(ValueError):
        link(case)


@pytest.mark.parametrize("key", ["primary_submission_receipts", "broker_order_observations"])
def test_duplicate_links_are_ambiguous_even_when_identical(tmp_path, key):
    case = linked_case(tmp_path)
    case[key] *= 2
    with pytest.raises(ValueError):
        link(case)


def test_cross_account_order_is_rejected(tmp_path):
    case = linked_case(tmp_path)
    case["broker_order_observations"][0]["account_scope"] = "alpaca:paper:another-account"
    with pytest.raises(ValueError, match="account_scope_mismatch"):
        link(case)


def test_supplied_fill_cannot_override_broker_price(tmp_path):
    case = linked_case(tmp_path)
    case["fills"][0]["price"] = 1.0
    with pytest.raises(ValueError, match="conflicts_with_linked_broker_order"):
        link(case)


def test_linker_derives_fill_without_manually_supplied_fill(tmp_path):
    case = linked_case(tmp_path)
    case["fills"] = []
    assert link(case)["fill"]["price"] == 2.6


def test_linked_study_uses_actual_price_without_changing_opportunity_rules(tmp_path):
    from test_primary_entry_study import assess
    case = linked_case(tmp_path)
    result = assess(case, protocol(fill_basis="linked_primary_fill"))
    assert result["status"] == "evaluated", result
    assert result["entry_price"] == 2.6
    assert result["fill_model"] == "account_scoped_submission_and_broker_order"
    assert result["quality"]["status"] == "pass"
    case["fills"][0]["price"] = 4
    case["broker_order_observations"][0]["order"]["filled_avg_price"] = "4"
    result = assess(case, protocol(fill_basis="linked_primary_fill"))
    assert result["entry_price"] == 4
    assert result["quality"]["status"] == "fail"


def test_unlinked_case_remains_in_the_study_denominator(tmp_path):
    source = tape(make_case(tmp_path))
    source["source_kind"] = "recorded_market"  # Synthetic adversarial fixture.
    report = run_primary_study(source, protocol(fill_basis="linked_primary_fill"), engine="v2")
    assert report["summary"]["samples_recorded"] == 1
    assert report["summary"]["sample_statuses"] == {"invalid_evidence": 1}
    assert report["results"][0]["quality"] is None
    assert report["manifest"]["fill_linkage_required"] is True
    assert report["manifest"]["source_provenance_verified"] is False
    assert report["summary"]["edge_established"] is False


def test_runtime_export_preserves_exact_admission_binding(tmp_path):
    from autobott_v2.primary_entry_study import case_from_runtime_evidence
    case = linked_case(tmp_path)
    capsule = {**case["refresh"], "snapshot_hash": digest(case["snapshot"])}
    admission = {**case["recorded_admission"], "recorded_refresh": capsule}
    exported = case_from_runtime_evidence(sample_id="runtime-case", snapshot=case["snapshot"],
        admission_event=admission, outcome_snapshots=case["outcome_snapshots"], fills=[])
    assert exported["recorded_admission"] == case["recorded_admission"]
    assert exported["fills"] == []
    assert "primary_submission_receipts" not in exported
    assert "broker_order_observations" not in exported


def test_changing_quotes_after_fill_does_not_change_the_linkage(tmp_path):
    case = linked_case(tmp_path)
    expected = link(case)
    for observation in case["outcome_snapshots"]:
        for quote in observation["option_chain"]:
            quote["bid"], quote["ask"] = 500, 501
    assert link(case) == expected


@pytest.mark.parametrize("same_account", [True, False])
def test_study_duplicate_guard_uses_account_and_order_identity(tmp_path, monkeypatch, same_account):
    import autobott_v2.primary_entry_study as study
    first = linked_case(tmp_path)
    second = deepcopy(first)
    second["sample_id"] = "sample-2"
    second["snapshot"]["ticker"] = "ANOTHER"  # Aggregation-only fixture below.
    source = tape(first)
    source["cases"].append(second)

    def evaluated(case, fixed_protocol, *, engine):
        scope = "alpaca:paper:first" if same_account or case["sample_id"] == first["sample_id"] else "alpaca:paper:second"
        return {"sample_id": case["sample_id"], "status": "missing_outcome_evidence", "quality": None,
                "fill_linkage": {"account_scope": scope, "fill": {"broker_order_id": "shared-id"}}}

    monkeypatch.setattr(study, "evaluate_primary_case", evaluated)
    rules = protocol(fill_basis="linked_primary_fill")
    if same_account:
        with pytest.raises(ValueError, match="duplicate_linked_primary_fill_identity"):
            study.run_primary_study(source, rules, engine="v2")
    else:
        assert study.run_primary_study(source, rules, engine="v2")["summary"]["samples_recorded"] == 2
