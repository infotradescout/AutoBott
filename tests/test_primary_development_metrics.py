"""Threshold-free development-metric tests; all market/broker data is synthetic."""
from copy import deepcopy
from datetime import timedelta
import json
from types import SimpleNamespace

from autobott_v2.bar_timing import aware_utc
from autobott_v2.primary_development_metrics import materialize_primary_development_metrics
from autobott_v2.primary_entry_study import digest
from autobott_v2.primary_fill_capture import bind_primary_submission, poll_primary_fills
from autobott_v2.primary_followthrough import PrimaryObservationRules, register_primary_observation
from test_primary_fill_capture import ReadOnlyBroker
from test_primary_fill_linkage import linked_case


def prepared_development_watch(tmp_path):
    case = linked_case(tmp_path / "source")
    snapshot = deepcopy(case["snapshot"])
    start = aware_utc(case["refresh"]["received_at"])
    opening = start - timedelta(hours=2)
    closing = start + timedelta(hours=4)
    snapshot.setdefault("entry_context", {})["schedule"] = {
        "session": {
            "trading_day": True,
            "open": opening.isoformat(),
            "close": closing.isoformat(),
        }
    }
    snapshot_hash = digest(snapshot)
    case["recorded_admission"]["snapshot_hash"] = snapshot_hash
    case["primary_submission_receipts"][0]["snapshot_hash"] = snapshot_hash

    admission = {
        **case["recorded_admission"],
        "recorded_refresh": {**case["refresh"], "snapshot_hash": snapshot_hash},
    }
    root = tmp_path / "watch"
    watch_id = register_primary_observation(
        root, snapshot, admission,
        PrimaryObservationRules(23_400, end_basis="session_close"),
    )
    receipt = case["primary_submission_receipts"][0]
    intent = SimpleNamespace(
        environment="paper", side="buy_to_open", metadata={"leg_role": "primary"},
        quantity=1, decision_id=receipt["decision_id"], option_symbol=receipt["option_symbol"],
    )
    submission = SimpleNamespace(
        intent=intent, broker_order_id=receipt["broker_order_id"],
        client_order_id=receipt["client_order_id"],
        submitted_at=aware_utc(receipt["submitted_at"]),
    )
    broker = ReadOnlyBroker(case["broker_order_observations"][0]["order"])
    bind_primary_submission(root, watch_id, submission,
                            account_scope="alpaca:paper:synthetic-account")
    assert poll_primary_fills(root, broker, now_fn=lambda: start)["filled"] == 1

    path = root / (watch_id + ".json")
    row = json.loads(path.read_text())
    row["case"]["outcome_snapshots"] = deepcopy(case["outcome_snapshots"])
    row["status"] = "window_closed"
    path.write_text(json.dumps(row))
    return root, watch_id, case, start, closing


def load(root, watch_id):
    return json.loads((root / (watch_id + ".json")).read_text())


def test_development_metrics_are_raw_descriptive_and_never_pass_fail(tmp_path):
    root, watch_id, _, start, closing = prepared_development_watch(tmp_path)
    result = materialize_primary_development_metrics(root)
    assert result["materialized"] == 1
    assert result["passes"] is None and result["fails"] is None
    assert result["eligible_for_holdout"] is False
    assert result["edge_established"] is False
    assert result["broker_reads"] == result["broker_writes"] == result["journal_writes"] == 0

    metrics = load(root, watch_id)["development_metrics"]
    assert metrics["development_only"] is True
    assert metrics["eligible_for_holdout"] is False
    assert metrics["pass_fail_status"] is None
    assert metrics["edge_established"] is False
    assert metrics["session_close"] == closing.isoformat()
    assert metrics["measurement_basis"] == "broker_fill_then_sampled_option_bid_gross_before_fees"
    option = metrics["option"]
    assert option["entry_fill_price"] == 2.6
    assert option["valid_quote_count"] == 3
    assert abs(option["max_gross_bid_return_pct"] - (3.2 / 2.6 - 1)) < 1e-12
    assert abs(option["min_gross_bid_return_pct"] - (3.2 / 2.6 - 1)) < 1e-12
    assert option["seconds_to_max_gross_bid_return"] == 60
    assert option["positive_quote_fraction"] == 1
    assert option["last_quote_seconds_before_window_end"] == (closing - (start + timedelta(seconds=180))).total_seconds()


def test_development_metrics_report_path_shape_without_quality_thresholds(tmp_path):
    root, watch_id, _, _, _ = prepared_development_watch(tmp_path)
    row = load(root, watch_id)
    points = row["case"]["outcome_snapshots"]
    points[0]["option_chain"][0]["bid"] = 2.0
    points[0]["option_chain"][0]["ask"] = 2.05
    points[1]["option_chain"][0]["bid"] = 3.4
    points[1]["option_chain"][0]["ask"] = 3.45
    points[2]["option_chain"][0]["bid"] = 2.8
    points[2]["option_chain"][0]["ask"] = 2.85
    (root / (watch_id + ".json")).write_text(json.dumps(row))

    materialize_primary_development_metrics(root)
    option = load(root, watch_id)["development_metrics"]["option"]
    assert option["min_gross_bid_return_pct"] < 0
    assert option["max_gross_bid_return_pct"] > 0
    assert option["seconds_to_min_gross_bid_return"] == 60
    assert option["seconds_to_max_gross_bid_return"] == 120
    assert option["adverse_before_max_favorable_pct"] == option["min_gross_bid_return_pct"]


def test_development_metrics_are_idempotent_and_tamper_evident(tmp_path):
    root, watch_id, _, _, _ = prepared_development_watch(tmp_path)
    first = materialize_primary_development_metrics(root)
    second = materialize_primary_development_metrics(root)
    assert first["materialized"] == 1
    assert second["materialized"] == 0 and second["already_materialized"] == 1

    row = load(root, watch_id)
    row["development_metrics"]["option"]["max_gross_bid_return_pct"] = 99
    (root / (watch_id + ".json")).write_text(json.dumps(row))
    result = materialize_primary_development_metrics(root)
    assert result["errors"]
    assert "integrity_mismatch" in result["errors"][0]["reason"]


def test_fixed_duration_or_unfilled_watch_cannot_become_development_holdout(tmp_path):
    root, watch_id, _, _, _ = prepared_development_watch(tmp_path)
    row = load(root, watch_id)
    row["rules"]["end_basis"] = "fixed_duration"
    (root / (watch_id + ".json")).write_text(json.dumps(row))
    result = materialize_primary_development_metrics(root)
    assert result["not_development"] == 1
    assert result["materialized"] == 0
