"""Runtime primary-entry quality tests. All orders, prices, and accounts are synthetic."""
from copy import deepcopy
from datetime import timedelta
import json

import pytest

from autobott_v2.bar_timing import aware_utc
from autobott_v2.entry_quality import EntryQualityRules
from autobott_v2.primary_fill_capture import bind_primary_submission, poll_primary_fills
from autobott_v2.primary_followthrough import PrimaryObservationRules, register_primary_observation
from autobott_v2.primary_quality_runtime import (
    configured_entry_quality_rules, evaluate_completed_primary_watches,
)
from test_primary_fill_capture import ReadOnlyBroker
from test_primary_fill_linkage import linked_case


def quality_rules(**changes):
    values = {
        "protocol_id": "synthetic-predeclared-quality",
        "holding_seconds": 180,
        "target_return_pct": .20,
        "max_adverse_return_pct": .15,
        "persistence_seconds": 60,
        "max_quote_age_seconds": 30,
        "max_observation_gap_seconds": 60,
        "round_trip_fee_per_contract": 0,
        "contract_multiplier": 100,
    }
    values.update(changes)
    return EntryQualityRules(**values)


def prepared_watch(tmp_path, *, rules=None):
    case = linked_case(tmp_path / "source")
    admission = {
        **case["recorded_admission"],
        "recorded_refresh": {
            **case["refresh"],
            "snapshot_hash": case["recorded_admission"]["snapshot_hash"],
        },
    }
    root = tmp_path / "watch"
    qrules = rules or quality_rules()
    watch_id = register_primary_observation(
        root, case["snapshot"], admission, PrimaryObservationRules(180),
        quality_rules=qrules,
    )
    receipt = case["primary_submission_receipts"][0]
    intent = type("Intent", (), {
        "environment": "paper", "side": "buy_to_open",
        "metadata": {"leg_role": "primary"}, "quantity": 1,
        "decision_id": receipt["decision_id"], "option_symbol": receipt["option_symbol"],
    })()
    submission = type("Submitted", (), {
        "intent": intent, "broker_order_id": receipt["broker_order_id"],
        "client_order_id": receipt["client_order_id"],
        "submitted_at": receipt["submitted_at"],
    })()
    broker = ReadOnlyBroker(case["broker_order_observations"][0]["order"])
    return root, watch_id, case, admission, submission, broker, qrules


def load(root, watch_id):
    return json.loads((root / (watch_id + ".json")).read_text())


def save(root, watch_id, row):
    (root / (watch_id + ".json")).write_text(json.dumps(row))


def test_quality_config_has_no_defaults_and_requires_exact_fields(monkeypatch):
    monkeypatch.delenv("AUTOBOTT_ENTRY_QUALITY_RULES_JSON", raising=False)
    assert configured_entry_quality_rules() is None
    rules = quality_rules()
    monkeypatch.setenv("AUTOBOTT_ENTRY_QUALITY_RULES_JSON", json.dumps(rules.to_json_dict()))
    assert configured_entry_quality_rules() == rules
    bad = rules.to_json_dict()
    bad["after_the_fact_override"] = 1
    monkeypatch.setenv("AUTOBOTT_ENTRY_QUALITY_RULES_JSON", json.dumps(bad))
    with pytest.raises(ValueError, match="exact_entry_quality_rule_fields"):
        configured_entry_quality_rules()


def test_registration_binds_quality_rules_and_refuses_later_redefinition(tmp_path):
    root, watch_id, case, admission, _, _, rules = prepared_watch(tmp_path)
    row = load(root, watch_id)
    assert row["quality_protocol"]["rules"] == rules.to_json_dict()
    assert row["quality_protocol"]["rules_hash"] == rules.config_hash
    changed = quality_rules(target_return_pct=.30)
    with pytest.raises(ValueError, match="cannot_change_existing_quality_protocol"):
        register_primary_observation(
            root, case["snapshot"], admission, PrimaryObservationRules(180),
            quality_rules=changed,
        )


def test_quality_holding_window_cannot_exceed_recorded_observation_window(tmp_path):
    case = linked_case(tmp_path / "source")
    admission = {
        **case["recorded_admission"],
        "recorded_refresh": {
            **case["refresh"],
            "snapshot_hash": case["recorded_admission"]["snapshot_hash"],
        },
    }
    with pytest.raises(ValueError, match="observation_window_shorter"):
        register_primary_observation(
            tmp_path / "watch", case["snapshot"], admission, PrimaryObservationRules(180),
            quality_rules=quality_rules(holding_seconds=181),
        )


def test_completed_broker_linked_watch_materializes_actual_entry_quality_once(tmp_path):
    root, watch_id, case, _, submission, broker, rules = prepared_watch(tmp_path)
    bind_primary_submission(root, watch_id, submission, account_scope="alpaca:paper:synthetic-account")
    fill_at = case["broker_order_observations"][0]["order"]["filled_at"]
    assert poll_primary_fills(root, broker, now_fn=lambda: aware_utc(fill_at))["filled"] == 1

    row = load(root, watch_id)
    row["case"]["outcome_snapshots"] = deepcopy(case["outcome_snapshots"])
    row["status"] = "window_closed"
    save(root, watch_id, row)

    result = evaluate_completed_primary_watches(root)
    assert result["evaluated"] == 1
    assert result["quality_statuses"] == {"pass": 1}
    assert result["broker_reads"] == result["broker_writes"] == result["journal_writes"] == 0
    row = load(root, watch_id)
    evaluation = row["entry_quality_evaluation"]
    assert evaluation["rules_hash"] == rules.config_hash
    assert evaluation["broker_order_id"] == submission.broker_order_id
    assert evaluation["quality"]["evidence_kind"] == "broker_recorded_fill"
    assert evaluation["quality"]["status"] == "pass"
    assert evaluation["edge_established"] is False

    second = evaluate_completed_primary_watches(root)
    assert second["evaluated"] == 0 and second["already_evaluated"] == 1

    row = load(root, watch_id)
    row["entry_quality_evaluation"]["quality"]["status"] = "fail"
    save(root, watch_id, row)
    tampered = evaluate_completed_primary_watches(root)
    assert tampered["errors"] and "integrity_mismatch" in tampered["errors"][0]["reason"]


def test_mixed_feed_path_remains_unscorable(tmp_path):
    root, watch_id, case, _, submission, broker, _ = prepared_watch(tmp_path)
    bind_primary_submission(root, watch_id, submission, account_scope="alpaca:paper:synthetic-account")
    fill_at = case["broker_order_observations"][0]["order"]["filled_at"]
    aware_utc = __import__("autobott_v2.bar_timing", fromlist=["aware_utc"]).aware_utc
    assert poll_primary_fills(root, broker, now_fn=lambda: aware_utc(fill_at))["filled"] == 1
    row = load(root, watch_id)
    path = deepcopy(case["outcome_snapshots"])
    path[0]["source"]["options_feed"] = "different-feed"
    row["case"]["outcome_snapshots"] = path
    row["status"] = "window_closed"
    save(root, watch_id, row)

    result = evaluate_completed_primary_watches(root)
    assert result["quality_statuses"] == {"unscorable": 1}
    quality = load(root, watch_id)["entry_quality_evaluation"]["quality"]
    assert quality["status"] == "unscorable"
    assert quality["reason"] == "mixed_or_unknown_outcome_feed"


def test_watch_without_predeclared_quality_protocol_is_never_scored(tmp_path):
    case = linked_case(tmp_path / "source")
    admission = {
        **case["recorded_admission"],
        "recorded_refresh": {
            **case["refresh"],
            "snapshot_hash": case["recorded_admission"]["snapshot_hash"],
        },
    }
    root = tmp_path / "watch"
    watch_id = register_primary_observation(
        root, case["snapshot"], admission, PrimaryObservationRules(180)
    )
    row = load(root, watch_id)
    row["status"] = "window_closed"
    row["fill_capture_status"] = "filled"
    save(root, watch_id, row)
    result = evaluate_completed_primary_watches(root)
    assert result["not_configured"] == 1 and result["evaluated"] == 0
    assert "entry_quality_evaluation" not in load(root, watch_id)
