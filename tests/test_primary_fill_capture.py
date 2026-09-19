"""All brokers and fills in these tests are synthetic; never use real accounts."""
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
import json
from types import SimpleNamespace
import pytest
from autobott_v2.bar_timing import aware_utc
from autobott_v2.primary_fill_capture import bind_primary_submission, paper_capture_scope, poll_primary_fills
from autobott_v2.primary_followthrough import PrimaryObservationRules, register_primary_observation, export_primary_observations
from test_primary_fill_linkage import linked_case


class ReadOnlyBroker:
    def __init__(self, order):
        self.config = SimpleNamespace(environment="paper", trading_base_url="https://paper-api.alpaca.markets")
        self.order = deepcopy(order)
        self.account = "synthetic-account"
        self.account_reads = 0
        self.order_reads = []
        self.switch_account = False
    def get_account(self):
        self.account_reads += 1
        return {"id": "changed-account" if self.switch_account and self.account_reads > 1 else self.account}
    def get_order(self, order_id):
        self.order_reads.append(order_id)
        return deepcopy(self.order)
    def submit_order(self, *args, **kwargs):
        raise AssertionError("Collector must never submit")
    def cancel_order(self, *args, **kwargs):
        raise AssertionError("Collector must never cancel")


def prepared(tmp_path, **rule_changes):
    case = linked_case(tmp_path / "source")
    admission = {**case["recorded_admission"], "recorded_refresh": {**case["refresh"], "snapshot_hash": case["recorded_admission"]["snapshot_hash"]}}
    root = tmp_path / "watch"
    watch_id = register_primary_observation(root, case["snapshot"], admission, PrimaryObservationRules(180, **rule_changes))
    receipt = case["primary_submission_receipts"][0]
    intent = SimpleNamespace(environment="paper", side="buy_to_open", metadata={"leg_role": "primary"}, quantity=1,
                             decision_id=receipt["decision_id"], option_symbol=receipt["option_symbol"])
    submission = SimpleNamespace(intent=intent, broker_order_id=receipt["broker_order_id"],
                                 client_order_id=receipt["client_order_id"], submitted_at=aware_utc(receipt["submitted_at"]))
    broker = ReadOnlyBroker(case["broker_order_observations"][0]["order"])
    now = aware_utc(case["refresh"]["received_at"]) + timedelta(seconds=1)
    return root, watch_id, submission, broker, now


def load(root, watch_id):
    return json.loads((root / (watch_id + ".json")).read_text())


def bind(root, watch_id, order):
    bind_primary_submission(root, watch_id, order, account_scope="alpaca:paper:synthetic-account")


def test_automatic_fill_uses_broker_price_and_is_idempotent(tmp_path):
    root, watch_id, order, broker, now = prepared(tmp_path)
    bind(root, watch_id, order)
    assert load(root, watch_id)["case"]["fills"] == []
    bind(root, watch_id, order)
    result = poll_primary_fills(root, broker, now_fn=lambda: now)
    assert result["filled"] == 1 and result["errors"] == []
    assert broker.order_reads == [order.broker_order_id]
    row = load(root, watch_id)
    assert row["case"]["fills"][0]["price"] == 2.6
    assert row["fill_capture_status"] == "filled"
    assert len(row["broker_order_observation_history"]) == 1
    assert poll_primary_fills(root, broker, now_fn=lambda: now + timedelta(seconds=20))["checked"] == 0
    assert export_primary_observations(root, source_kind="synthetic")["fill_capture_statuses"][watch_id] == "filled"


def test_delayed_fill_extends_window_from_actual_purchase(tmp_path):
    root, watch_id, order, broker, now = prepared(tmp_path)
    bind(root, watch_id, order)
    before = load(root, watch_id)
    delayed_fill = now + timedelta(seconds=120)
    broker.order["filled_at"] = delayed_fill.isoformat()
    result = poll_primary_fills(root, broker, now_fn=lambda: delayed_fill)
    assert result["filled"] == 1 and result["errors"] == []
    row = load(root, watch_id)
    assert aware_utc(row["pre_fill_window_end"]) == aware_utc(before["window_end"])
    assert aware_utc(row["fill_window_start"]) == delayed_fill
    assert aware_utc(row["window_end"]) == delayed_fill + timedelta(seconds=row["rules"]["window_seconds"])
    assert row["observation_window_basis"] == "broker_recorded_primary_fill"


def test_unlinked_historical_watch_never_queries_broker(tmp_path):
    root, watch_id, order, broker, now = prepared(tmp_path)
    assert poll_primary_fills(root, broker, now_fn=lambda: now)["checked"] == 0
    assert broker.account_reads == 0 and broker.order_reads == []
    assert load(root, watch_id)["case"]["fills"] == []


@pytest.mark.parametrize("url", ["https://api.alpaca.markets", "http://paper-api.alpaca.markets",
    "https://paper-api.alpaca.markets.evil.test", "https://user@paper-api.alpaca.markets", "https://paper-api.alpaca.markets/other"])
def test_nonpaper_or_ambiguous_endpoint_rejected_before_network(tmp_path, url):
    _, _, _, broker, _ = prepared(tmp_path)
    broker.config.trading_base_url = url
    with pytest.raises(ValueError, match="paper_endpoint"):
        paper_capture_scope(broker)
    assert broker.account_reads == 0


def test_live_environment_rejected_before_account_request(tmp_path):
    _, _, _, broker, _ = prepared(tmp_path)
    broker.config.environment = "live"
    with pytest.raises(ValueError): paper_capture_scope(broker)
    assert broker.account_reads == 0


def test_wrong_account_skips_order_read(tmp_path):
    root, watch_id, order, broker, now = prepared(tmp_path)
    bind(root, watch_id, order)
    broker.account = "other"
    result = poll_primary_fills(root, broker, now_fn=lambda: now)
    assert result["checked"] == 0 and result["errors"]
    assert broker.order_reads == []
    assert load(root, watch_id)["case"]["fills"] == []


def test_account_change_during_read_cannot_commit_fill(tmp_path):
    root, watch_id, order, broker, now = prepared(tmp_path)
    bind(root, watch_id, order); before = load(root, watch_id)
    broker.switch_account = True
    with pytest.raises(ValueError, match="account_changed"):
        poll_primary_fills(root, broker, now_fn=lambda: now)
    assert load(root, watch_id) == before


@pytest.mark.parametrize("field,value", [("id", "other"), ("client_order_id", "other"), ("symbol", "runner"), ("side", "sell"), ("filled_qty", "0.5"), ("filled_avg_price", "NaN")])
def test_invalid_order_observation_cannot_become_a_fill(tmp_path, field, value):
    root, watch_id, order, broker, now = prepared(tmp_path)
    bind(root, watch_id, order); broker.order[field] = value
    result = poll_primary_fills(root, broker, now_fn=lambda: now)
    assert result["filled"] == 0 and result["errors"]
    assert load(root, watch_id)["case"]["fills"] == []


def test_partial_then_complete_fill_survives_restart_and_reopens_post_fill_window(tmp_path):
    root, watch_id, order, broker, now = prepared(tmp_path)
    bind(root, watch_id, order)
    broker.order.update(status="partially_filled", filled_qty="0.5")
    assert poll_primary_fills(root, broker, now_fn=lambda: now)["pending"] == 1
    assert load(root, watch_id)["case"]["fills"] == []
    row = load(root, watch_id)
    original_end = aware_utc(row["window_end"])
    row["status"] = "window_closed"
    (root / (watch_id + ".json")).write_text(json.dumps(row))
    completed_at = now + timedelta(seconds=220)
    restarted = ReadOnlyBroker({**broker.order, "status": "filled", "filled_qty": "1",
                                "filled_at": completed_at.isoformat()})
    result = poll_primary_fills(root, restarted, now_fn=lambda: completed_at)
    assert result["filled"] == 1
    row = load(root, watch_id)
    assert len(row["broker_order_observation_history"]) == 2
    assert row["status"] == "observing"
    assert row["reopened_after_primary_fill"] is True
    assert aware_utc(row["pre_fill_window_end"]) == original_end
    assert aware_utc(row["fill_window_start"]) == completed_at
    assert aware_utc(row["window_end"]) == completed_at + timedelta(seconds=row["rules"]["window_seconds"])


@pytest.mark.parametrize("status", ["canceled", "rejected", "expired"])
def test_terminal_unfilled_order_is_not_fabricated(tmp_path, status):
    root, watch_id, order, broker, now = prepared(tmp_path)
    bind(root, watch_id, order); broker.order.update(status=status, filled_qty="0")
    result = poll_primary_fills(root, broker, now_fn=lambda: now)
    assert result["filled"] == 0
    assert load(root, watch_id)["fill_capture_status"] == "terminal_unscorable"
    assert load(root, watch_id)["case"]["fills"] == []


def test_changed_submission_cannot_overwrite_saved_identity(tmp_path):
    root, watch_id, order, broker, now = prepared(tmp_path)
    bind(root, watch_id, order); before = load(root, watch_id)
    order.broker_order_id = "other"
    with pytest.raises(ValueError, match="conflicting_primary_submission"):
        bind(root, watch_id, order)
    assert load(root, watch_id) == before


def test_bound_receipt_cannot_be_a_runner(tmp_path):
    root, watch_id, order, _, _ = prepared(tmp_path)
    order.intent.metadata["leg_role"] = "runner"
    with pytest.raises(ValueError): bind(root, watch_id, order)
    assert "primary_submission_receipts" not in load(root, watch_id)["case"]


def test_observation_history_is_bounded(tmp_path):
    root, watch_id, order, broker, now = prepared(tmp_path, max_observations=1)
    bind(root, watch_id, order); broker.order.update(status="accepted", filled_qty="0")
    poll_primary_fills(root, broker, now_fn=lambda: now)
    result = poll_primary_fills(root, broker, now_fn=lambda: now + timedelta(seconds=20))
    assert result["errors"] and load(root, watch_id)["fill_capture_status"] == "observation_limit_reached"
    assert len(load(root, watch_id)["broker_order_observation_history"]) == 1


def test_real_cycle_automatically_collects_linked_primary_fill_and_complete_path(tmp_path, monkeypatch):
    import test_entry_market_timing as timing
    from autobott_v2.primary_followthrough import poll_primary_observations
    from test_primary_followthrough import Quotes
    from test_primary_entry_study import assess, protocol, PRIMARY
    at = timing.START + timedelta(seconds=10)
    class FilledBroker(timing.FakeBroker):
        def __init__(self):
            super().__init__(); self.orders = {}; self.reads = []
        def submit_order(self, intent, **kwargs):
            returned = replace(super().submit_order(intent, **kwargs), submitted_at=at)
            self.orders[returned.broker_order_id] = returned
            return returned
        def get_order(self, order_id):
            self.reads.append(order_id)
            order = self.orders[order_id]
            return {"id": order_id, "client_order_id": order.client_order_id, "symbol": order.intent.option_symbol,
                    "side": "buy", "position_intent": "buy_to_open", "status": "filled", "qty": "1",
                    "filled_qty": "1", "filled_avg_price": "2.6", "filled_at": at.isoformat()}
    monkeypatch.setattr(timing, "FakeBroker", FilledBroker)
    monkeypatch.setenv("AUTOBOTT_PRIMARY_OBSERVATION_SECONDS", "180")
    monkeypatch.setenv("AUTOBOTT_ARTIFACTS_ROOT", str(tmp_path / "evidence"))
    result, broker, _ = timing.run_cycle(tmp_path, monkeypatch, pair=True, v2=True)
    assert len(broker.submitted) == 2
    assert any(r["disposition"] == "primary_submission_captured" for r in result.execution_outcomes)
    assert any(r["disposition"] == "primary_fill_capture_poll" and r["filled"] == 1 for r in result.execution_outcomes)
    root = tmp_path / "evidence" / "primary_followthrough"
    for seconds in (60, 120, 180):
        now = at + timedelta(seconds=seconds)
        poll_primary_observations(root, Quotes(now), now_fn=lambda: now)
    source = export_primary_observations(root, source_kind="synthetic")
    assert len(source["cases"]) == 1
    case = source["cases"][0]
    assert case["fills"][0]["option_symbol"] == PRIMARY
    assert case["fills"][0]["broker_order_id"] == "alpaca-order-1"
    assert case["primary_submission_receipts"][0]["client_order_id"] == "client-1"
    scored = assess(case, protocol(fill_basis="linked_primary_fill"))
    assert scored["status"] == "evaluated", scored
    assert scored["quality"]["status"] == "pass", scored
    assert scored["entry_price"] == 2.6
    assert source["entry_edge_established"] is False


def test_collector_write_failure_cannot_change_orders_or_exit_metadata(tmp_path, monkeypatch):
    import test_entry_market_timing as timing
    from autobott_v2 import trading_cycle
    at = timing.START + timedelta(seconds=10)
    class TimedBroker(timing.FakeBroker):
        def submit_order(self, intent, **kwargs):
            return replace(super().submit_order(intent, **kwargs), submitted_at=at)
    monkeypatch.setattr(timing, "FakeBroker", TimedBroker)
    monkeypatch.setenv("AUTOBOTT_PRIMARY_OBSERVATION_SECONDS", "180")
    monkeypatch.setenv("AUTOBOTT_ARTIFACTS_ROOT", str(tmp_path / "evidence"))
    def failed(*args, **kwargs): raise OSError("synthetic_disk_full")
    monkeypatch.setattr(trading_cycle, "bind_primary_submission", failed)
    result, broker, _ = timing.run_cycle(tmp_path, monkeypatch, pair=True, v2=True)
    assert len(broker.submitted) == 2
    assert broker.submitted[0].take_profit_price == 3.75
    assert broker.submitted[0].stop_loss_price == 1.375
    assert any(r["disposition"] == "primary_submission_capture_failed" for r in result.execution_outcomes)
    assert not any(r.get("reason") == "core_runner_paired_submission_partial_failure" for r in result.skipped)


def test_provider_failure_is_visible_without_mutating_evidence(tmp_path):
    root, watch_id, order, broker, now = prepared(tmp_path)
    bind(root, watch_id, order); before = load(root, watch_id)
    def unavailable(_): raise TimeoutError("synthetic_provider_timeout")
    broker.get_order = unavailable
    result = poll_primary_fills(root, broker, now_fn=lambda: now)
    assert result["errors"][0]["reason"] == "TimeoutError"
    assert load(root, watch_id) == before


def test_collector_retains_only_whitelisted_order_fields(tmp_path):
    root, watch_id, order, broker, now = prepared(tmp_path)
    bind(root, watch_id, order)
    broker.order["unexpected_private_field"] = "must-not-persist"
    poll_primary_fills(root, broker, now_fn=lambda: now)
    raw = (root / (watch_id + ".json")).read_text()
    assert "must-not-persist" not in raw
    assert "unexpected_private_field" not in raw


def test_poll_read_limit_is_checked_before_calls(tmp_path):
    root, watch_id, order, broker, now = prepared(tmp_path)
    bind(root, watch_id, order)
    for limit in (0, 101, True):
        with pytest.raises(ValueError):
            poll_primary_fills(root, broker, now_fn=lambda: now, max_reads=limit)
    assert broker.account_reads == 0 and broker.order_reads == []


def test_clock_regression_cannot_capture_a_fill(tmp_path):
    root, watch_id, order, broker, now = prepared(tmp_path)
    bind(root, watch_id, order); before = load(root, watch_id)
    times = iter([now, now - timedelta(seconds=1)])
    result = poll_primary_fills(root, broker, now_fn=lambda: next(times))
    assert result["errors"] and result["filled"] == 0
    assert load(root, watch_id) == before
