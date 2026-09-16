"""Synthetic regression fixtures only: these are not market-performance results."""
from __future__ import annotations

import copy
import json
import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from autobott_v2.entry_quality import EntryQualityRules, evaluate_entry_quality, summarize_entry_quality

START = datetime(2026, 9, 16, 14, 0, tzinfo=timezone.utc)


def stamp(seconds):
    return (START + timedelta(seconds=seconds)).isoformat()


def policy(**changes):
    # Explicit test parameters, NOT production defaults or calibrated thresholds.
    base = EntryQualityRules("synthetic-test-only", 180, .20, .15, 60, 10, 60, 0, 100)
    return replace(base, **changes)


def entry(**changes):
    row = dict(decision_id="synthetic-primary", ticker="TEST", timestamp=stamp(0),
               filled=True, selected_contract={"option_symbol": "TEST_CALL"},
               leg_role="tactical", entry_fill_price=1.0, entry_fill_model="synthetic_fixture")
    row.update(changes)
    return row


def snapshot(seconds, bid, **changes):
    row = {"ticker": "TEST", "timestamp": stamp(seconds), "underlying_quote": {"last": 100},
           "option_chain": [{"option_symbol": "TEST_CALL", "bid": bid, "ask": bid + .02,
                             "quote_timestamp": stamp(seconds)}]}
    row.update(changes)
    return row


def path(bids=(.99, 1.2, 1.2, 1.1)):
    return [snapshot(i * 60, bid) for i, bid in enumerate(bids)]


def assess(rows=None, rules=None, fill=None):
    return evaluate_entry_quality(entry() if fill is None else fill,
                                  path() if rows is None else rows,
                                  policy() if rules is None else rules,
                                  is_primary=True, evidence_kind="simulated_fill")


class EntryQualityTests(unittest.TestCase):
    def test_meaningful_sustained_opportunity(self):
        result = assess()
        self.assertEqual(result["status"], "pass")
        self.assertEqual(result["first_target_seconds"], 60)
        self.assertEqual(result["opportunity_confirmed_seconds"], 120)
        self.assertAlmostEqual(result["adverse_before_opportunity_pct"], -.01)
        self.assertFalse(result["edge_established"])

    def test_right_underlying_wrong_option_fails(self):
        rows = path((.99, 1.0, .99, .98))
        for row in rows:
            row["underlying_quote"]["last"] = 500
        self.assertEqual(assess(rows)["reason"], "target_not_reached")

    def test_other_option_cannot_rescue_primary(self):
        rows = path((.99, 1.0, .99, .98))
        for row in rows:
            row["option_chain"].append(dict(option_symbol="OTHER", bid=10, ask=11, quote_timestamp=row["timestamp"]))
        self.assertEqual(assess(rows)["status"], "fail")

    def test_brief_green_is_not_success(self):
        self.assertEqual(assess(path((.99, 1.01, 1.01, 1.01)))["status"], "fail")

    def test_single_target_spike_is_not_success(self):
        self.assertEqual(assess(path((.99, 1.3, 1.05, 1.05)))["reason"], "target_not_persistent")

    def test_loss_before_later_target_fails(self):
        result = assess(path((.99, .80, 1.3, 1.3)))
        self.assertEqual(result["status"], "fail")
        self.assertEqual(result["first_drawdown_breach_seconds"], 60)
        self.assertEqual(result["opportunity_confirmed_seconds"], 180)

    def test_target_touch_before_loss_does_not_count_as_persistence(self):
        self.assertEqual(assess(path((.99, 1.3, .8, 1.3)))["status"], "fail")

    def test_loss_after_confirmed_opportunity_does_not_rewrite_entry(self):
        result = assess(path((.99, 1.2, 1.2, .70)))
        self.assertEqual(result["status"], "pass")
        self.assertEqual(result["first_drawdown_breach_seconds"], 180)

    def test_exact_drawdown_limit_is_breach(self):
        self.assertEqual(assess(path((.85, 1.2, 1.2, 1.2)))["status"], "fail")

    def test_exact_target_limit_uses_decimal_comparison(self):
        self.assertEqual(assess(path((.99, 1.2, 1.2, 1.2)))["status"], "pass")

    def test_zero_bid_is_observed_loss_not_missing_data(self):
        result = assess(path((.99, 0, 1.2, 1.2)))
        self.assertEqual(result["status"], "fail")
        self.assertEqual(result["min_observed_return_pct"], -1)

    def test_fees_are_explicit_and_included(self):
        self.assertEqual(assess(path((.99, 1.2, 1.2, 1.2)), policy(round_trip_fee_per_contract=1))["status"], "fail")

    def test_actual_fill_price_not_mid_is_basis(self):
        self.assertEqual(assess(fill=entry(entry_fill_price=1.15))["status"], "fail")

    def test_later_move_outside_horizon_ignored(self):
        rows = path((.99, 1.0, 1.0, 1.0)) + [snapshot(240, 3), snapshot(300, 3)]
        self.assertEqual(assess(rows)["reason"], "target_not_reached")

    def test_pre_entry_move_ignored(self):
        self.assertEqual(assess([snapshot(-60, 3)] + path((.99, 1, 1, 1)))["status"], "fail")

    def test_incomplete_horizon_is_unscorable_even_after_target(self):
        result = assess(path()[:-1])
        self.assertEqual(result["status"], "unscorable")
        self.assertIn("holding_window_incomplete", result["data_issues"])

    def test_missing_contract_is_unscorable(self):
        rows = path()
        rows[2]["option_chain"] = []
        self.assertEqual(assess(rows)["status"], "unscorable")

    def test_wrong_ticker_does_not_complete_window(self):
        rows = path()[:-1] + [snapshot(180, 1.3, ticker="OTHER")]
        self.assertEqual(assess(rows)["status"], "unscorable")

    def test_large_sampling_gap_is_unscorable(self):
        self.assertEqual(assess([snapshot(0, .99), snapshot(180, 1.3)])["status"], "unscorable")

    def test_stale_future_crossed_and_nonfinite_quotes_rejected(self):
        for change in ({"quote_timestamp": stamp(0)}, {"quote_timestamp": stamp(61)},
                       {"ask": .5}, {"bid": float("nan")}, {"bid": float("inf")},
                       {"bid": True}, {"bid": -1}, {"bid": "1.2"}):
            with self.subTest(change=change):
                rows = path()
                rows[1]["option_chain"][0].update(change)
                self.assertEqual(assess(rows)["status"], "unscorable")

    def test_cached_quote_cannot_manufacture_persistence(self):
        rows = path((.99, 1.2, 1.2, 1.2))
        for row in rows[2:]:
            row["option_chain"][0]["quote_timestamp"] = stamp(60)
        result = assess(rows, policy(max_quote_age_seconds=180))
        self.assertEqual(result["status"], "unscorable")
        self.assertIsNone(result["opportunity_confirmed_seconds"])

    def test_variable_quote_delay_cannot_manufacture_persistence(self):
        rows = path()
        rows[1]["option_chain"][0]["quote_timestamp"] = stamp(59)
        rows[2]["option_chain"][0]["quote_timestamp"] = stamp(110)
        result = assess(rows)
        self.assertEqual(result["reason"], "target_not_persistent")
        self.assertEqual(result["longest_observed_target_run_seconds"], 51)

    def test_conflicting_same_quote_timestamp_is_unscorable(self):
        rows = path()
        rows[2]["option_chain"][0].update(quote_timestamp=stamp(60), bid=1.3, ask=1.32)
        result = assess(rows, policy(max_quote_age_seconds=60))
        self.assertEqual(result["status"], "unscorable")
        self.assertIn("conflicting_same_time_quotes", result["data_issues"])

    def test_duplicate_observations_do_not_change_score(self):
        rows = path()
        self.assertEqual(assess(rows), assess(rows + copy.deepcopy(rows)))

    def test_conflicting_duplicate_observation_is_unscorable(self):
        self.assertEqual(assess(path() + [snapshot(60, 1.4)])["status"], "unscorable")

    def test_out_of_order_snapshots_are_sorted(self):
        self.assertEqual(assess(path()), assess(list(reversed(path()))))

    def test_timezone_equivalent_points_preserve_result(self):
        rows = path()
        for row in rows:
            row["timestamp"] = datetime.fromisoformat(row["timestamp"]).astimezone(timezone(timedelta(hours=-5))).isoformat()
        self.assertEqual(assess()["status"], assess(rows)["status"])

    def test_naive_timestamp_cannot_pass(self):
        rows = path()
        rows[1]["timestamp"] = "2026-09-16T14:01:00"
        self.assertEqual(assess(rows)["status"], "unscorable")

    def test_no_protocol_means_no_success_claim(self):
        result = evaluate_entry_quality(entry(), path(), None, is_primary=True, evidence_kind="simulated_fill")
        self.assertEqual(result["reason"], "missing_predeclared_rules")
        self.assertIsNone(result["passed"])

    def test_rejected_or_invalid_fills_are_unscorable(self):
        for changes in ({"filled": False}, {"entry_fill_price": 0}, {"entry_fill_price": -1},
                        {"entry_fill_price": float("nan")}, {"entry_fill_price": True},
                        {"selected_contract": None}, {"entry_fill_model": ""}, {"decision_id": None}):
            with self.subTest(changes=changes):
                self.assertEqual(assess(fill=entry(**changes))["status"], "unscorable")

    def test_rules_validation_and_immutability(self):
        for changes in ({"holding_seconds": 0}, {"holding_seconds": True}, {"target_return_pct": float("nan")},
                        {"max_adverse_return_pct": 1.01}, {"persistence_seconds": 181},
                        {"max_observation_gap_seconds": 181}, {"round_trip_fee_per_contract": -1},
                        {"contract_multiplier": 1}, {"contract_multiplier": 100.0}, {"protocol_id": ""}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                policy(**changes)
        with self.assertRaises(AttributeError):
            policy().holding_seconds = 10

    def test_hash_records_every_threshold_change(self):
        self.assertEqual(policy().config_hash, policy().config_hash)
        self.assertNotEqual(policy().config_hash, policy(target_return_pct=.21).config_hash)

    def test_inputs_unchanged_and_output_json_safe(self):
        rows, fill = path(), entry()
        before = copy.deepcopy((rows, fill))
        json.dumps(assess(rows, fill=fill), allow_nan=False)
        self.assertEqual(before, (rows, fill))

    def test_missing_data_denominator_and_primary_are_separate(self):
        success = assess()
        failed = assess(path((.99, 1, 1, 1)))
        failed["is_primary"] = False
        unknown = assess(path()[:-1])
        summary = summarize_entry_quality([success, failed, unknown])
        self.assertEqual(summary["entries_recorded"], 3)
        self.assertEqual(summary["scorable"], 2)
        self.assertEqual(summary["unscorable"], 1)
        self.assertEqual(summary["primary"]["entries_recorded"], 2)
        self.assertEqual(summary["non_primary"]["fails"], 1)
        self.assertAlmostEqual(summary["observed_success_fraction_all_entries"], 1 / 3)
        self.assertFalse(summary["edge_established"])

    def test_empty_summary_is_not_zero_percent_measurement(self):
        self.assertIsNone(summarize_entry_quality([])["pass_rate_scorable"])

    def test_explicit_evidence_kind_required(self):
        with self.assertRaises(ValueError):
            evaluate_entry_quality(entry(), path(), policy(), is_primary=True, evidence_kind="verified_profitable")


class ReplayEntryQualityIntegrationTests(unittest.TestCase):
    def run_fixture(self, rules):
        from autobott_v2 import phase1_replay as replay
        rows = path((.99, 1, 1, 1))
        for i, row in enumerate(rows):
            row["schema_version"] = "synthetic"
            row["option_chain"].append(dict(option_symbol="OTHER", bid=.99 if i == 0 else 1.2,
                                            ask=1.22, quote_timestamp=row["timestamp"]))
        primary = entry()
        rider = entry(decision_id="synthetic-rider", leg_role="rider", selected_contract={"option_symbol": "OTHER"})
        rejected = entry(decision_id="synthetic-rejected", filled=False)
        def event(row):
            return SimpleNamespace(filled=row["filled"], decision_id=row["decision_id"],
                                   selected_contract=SimpleNamespace(**row["selected_contract"]),
                                   to_json_dict=lambda: copy.deepcopy(row))
        card = SimpleNamespace(selected_contract=SimpleNamespace(option_symbol="TEST_CALL"),
                               decision=SimpleNamespace(value="TRADE_CANDIDATE"),
                               to_json_dict=lambda: {"decision": "TRADE_CANDIDATE"})
        exit_rules = object()
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = Path(directory.name)
        manifest_at_assessment = []
        def checked_evaluator(*args, **kwargs):
            manifest_at_assessment.append(json.loads((root / "test" / "manifest.json").read_text()))
            return evaluate_entry_quality(*args, **kwargs)
        with patch.multiple(replay,
                _load_snapshot=lambda p: rows[int(p.name)],
                _decision_input_from_snapshot=lambda row: row,
                build_decision_card=lambda row: card,
                _execution_rules=lambda model: object(),
                _exit_rules=lambda model: exit_rules,
                _manifest=lambda *args: {"exit_config": {"unchanged_test_sentinel": True}},
                evaluate_decision_thesis=lambda *args: SimpleNamespace(to_json_dict=lambda: {}),
                summarize_thesis_results=lambda values: {"pass_rate": 1},
                update_phase1_gate=lambda *args: {"decision_stats": {}},
                load_phase1_gate=lambda *args: SimpleNamespace(enabled=False, reason="test_only", gate={}),
                evaluate_entry_quality=checked_evaluator), \
             patch.object(replay, "simulate_execution", side_effect=[[event(primary), event(rider), event(rejected)], [], [], []]), \
             patch.object(replay, "evaluate_exit", return_value=SimpleNamespace(exit_action="hold")) as exit_mock:
            report = replay.run_replay([str(i) for i in range(4)], artifacts_root=root,
                                       run_id="test", entry_quality_rules_by_role=rules)
        self.assertTrue(all(call.kwargs["rules"] is exit_rules for call in exit_mock.call_args_list))
        records = [json.loads(line) for line in (root / "test" / "entry_quality.jsonl").read_text().splitlines()]
        self.assertEqual(len(records), 2)  # The rejected candidate is not an entry.
        return report, records, manifest_at_assessment

    def test_primary_failure_not_hidden_by_rider_success(self):
        report, records, manifests = self.run_fixture({"tactical": policy(), "rider": policy()})
        self.assertEqual(report["entry_quality"]["primary"]["fails"], 1)
        self.assertEqual(report["entry_quality"]["non_primary"]["passes"], 1)
        self.assertEqual(records[0]["evidence_kind"], "simulated_fill")
        self.assertFalse(report["thesis_validation"]["entry_quality_evidence"])
        self.assertEqual(report["thesis_validation"]["measurement_basis"], "underlying_direction_only")
        self.assertFalse(report["entry_quality"]["edge_established"])
        self.assertTrue(all(m["entry_quality_protocols"]["tactical"]["rules_hash"] == policy().config_hash for m in manifests))
        self.assertTrue(all(m["exit_config"] == {"unchanged_test_sentinel": True} for m in manifests))

    def test_missing_rules_are_explicit_not_defaulted(self):
        report, records, manifests = self.run_fixture(None)
        self.assertEqual(report["entry_quality"]["unscorable"], 2)
        self.assertTrue(all(row["reason"] == "missing_predeclared_rules" for row in records))
        self.assertTrue(all(m["entry_quality_protocols"] == {} for m in manifests))

    def test_missing_role_does_not_borrow_other_roles_horizon(self):
        report, records, _ = self.run_fixture({"tactical": policy()})
        self.assertEqual(report["entry_quality"]["primary"]["fails"], 1)
        self.assertEqual(report["entry_quality"]["non_primary"]["unscorable"], 1)

    def test_invalid_rule_mapping_fails_before_loading_data(self):
        from autobott_v2 import phase1_replay as replay
        with patch.object(replay, "_load_snapshot") as loader:
            with self.assertRaises(ValueError):
                replay.run_replay([], entry_quality_rules_by_role={"unknown": policy()})
            loader.assert_not_called()


if __name__ == "__main__":
    unittest.main()
