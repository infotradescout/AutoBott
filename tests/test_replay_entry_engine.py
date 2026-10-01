"""Synthetic replay-wiring checks; not hosted execution or market performance."""
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

from autobott_v2.entry_quality import EntryQualityRules, evaluate_entry_quality
from autobott_v2.phase1_models import Phase1Rules

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


class ReplayEntryEngineTests(unittest.TestCase):
    def run_fixture(self, rules, *, entry_engine="legacy", decision_rules=None):
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
        def legacy_builder(row, **kwargs):
            self.assertEqual(entry_engine, "legacy")
            if decision_rules is not None:
                self.assertIs(kwargs["rules"], decision_rules)
            return card
        def v2_builder(row, **kwargs):
            self.assertEqual(entry_engine, "v2")
            if decision_rules is not None:
                self.assertIs(kwargs["rules"], decision_rules)
            return card
        with patch.multiple(replay,
                _load_snapshot=lambda p: rows[int(p.name)],
                _decision_input_from_snapshot=lambda row: row,
                build_decision_card=legacy_builder,
                _execution_rules=lambda model: object(),
                _exit_rules=lambda model: exit_rules,
                _manifest=lambda *args: {"exit_config": {"unchanged_test_sentinel": True}},
                evaluate_decision_thesis=lambda *args: SimpleNamespace(to_json_dict=lambda: {}),
                summarize_thesis_results=lambda values: {"pass_rate": 1},
                update_phase1_gate=lambda *args: {"decision_stats": {}},
                load_phase1_gate=lambda *args: SimpleNamespace(enabled=False, reason="test_only", gate={}),
                evaluate_entry_quality=checked_evaluator), \
             patch("autobott_v2.phase1_engine_v2.build_decision_card", side_effect=v2_builder) as v2_mock, \
             patch.object(replay, "simulate_execution", side_effect=[[event(primary), event(rider), event(rejected)], [], [], []]), \
             patch.object(replay, "evaluate_exit", return_value=SimpleNamespace(exit_action="hold")) as exit_mock:
            report = replay.run_replay([str(i) for i in range(4)], artifacts_root=root,
                                       run_id="test", entry_quality_rules_by_role=rules,
                                       entry_engine=entry_engine, decision_rules=decision_rules)
        self.assertEqual(v2_mock.call_count, 4 if entry_engine == "v2" else 0)
        self.assertTrue(all(call.kwargs["rules"] is exit_rules for call in exit_mock.call_args_list))
        records = [json.loads(line) for line in (root / "test" / "entry_quality.jsonl").read_text().splitlines()]
        self.assertEqual(len(records), 2)  # The rejected candidate is not an entry.
        return report, records, manifest_at_assessment

    def test_v2_builder_selection_and_honest_execution_model(self):
        report, _, manifests = self.run_fixture({"tactical": policy()}, entry_engine="v2")
        self.assertEqual(report["entry_engine"], "v2")
        self.assertFalse(report["hosted_execution_parity_verified"])
        self.assertTrue(all(m["engine_version"] == "phase1_engine_v2" for m in manifests))
        self.assertTrue(all(m["execution_model"] == "phase1_tactical_rider_simulation" for m in manifests))

    def test_explicit_decision_rules_reach_selected_v2_builder(self):
        rules = Phase1Rules(intraday_min_dte=5, intraday_max_dte=10)
        report, _, manifests = self.run_fixture({"tactical": policy()}, entry_engine="v2", decision_rules=rules)
        self.assertTrue(all(m["decision_rules_source"] == "explicit" for m in manifests))
        self.assertTrue(all(m["decision_rules"]["intraday_min_dte"] == 5 for m in manifests))

    def test_explicit_decision_rules_reach_legacy_builder(self):
        rules = Phase1Rules(min_confidence=.6)
        report, _, manifests = self.run_fixture({"tactical": policy()}, decision_rules=rules)
        self.assertEqual(report["entry_engine"], "legacy")
        self.assertTrue(all(m["decision_rules"]["min_confidence"] == .6 for m in manifests))

    def test_engine_changes_entry_hash_not_exit_policy(self):
        baseline, _, old = self.run_fixture({"tactical": policy()})
        candidate, _, new = self.run_fixture({"tactical": policy()}, entry_engine="v2")
        self.assertNotEqual(baseline["entry_config_hash"], candidate["entry_config_hash"])
        self.assertEqual(old[0]["exit_config"], new[0]["exit_config"])

    def test_decision_rule_changes_affect_entry_hash(self):
        baseline, _, old = self.run_fixture({"tactical": policy()}, entry_engine="v2")
        candidate, _, new = self.run_fixture({"tactical": policy()}, entry_engine="v2", decision_rules=Phase1Rules(min_confidence=.6))
        self.assertNotEqual(baseline["entry_config_hash"], candidate["entry_config_hash"])
        self.assertEqual(old[0]["decision_rules_source"], "Phase1Rules_defaults")

    def test_unknown_engine_rejected_before_data_loading(self):
        from autobott_v2 import phase1_replay as replay
        with patch.object(replay, "_load_snapshot") as loader:
            with self.assertRaises(ValueError):
                replay.run_replay([], entry_engine="unknown")
            loader.assert_not_called()

    def test_invalid_decision_rules_rejected_before_data_loading(self):
        from autobott_v2 import phase1_replay as replay
        with patch.object(replay, "_load_snapshot") as loader:
            with self.assertRaises(ValueError):
                replay.run_replay([], decision_rules={"min_confidence": 0})
            loader.assert_not_called()


if __name__ == "__main__":
    unittest.main()
