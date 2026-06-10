from __future__ import annotations

import sys
import unittest
from pathlib import Path

_PKG_DIR = Path(__file__).resolve().parent.parent
if str(_PKG_DIR) not in sys.path:
    sys.path.insert(0, str(_PKG_DIR))

import main  # noqa: E402


class CycleDiagnosisTests(unittest.TestCase):
    def _build(
        self,
        *,
        trade_kpi: dict,
        reject: dict[str, int] | None = None,
        contract_reject: dict[str, int] | None = None,
        order_reject: dict[str, int] | None = None,
        rate: dict | None = None,
        entered: bool = True,
    ) -> dict:
        return main._build_cycle_diagnosis(
            cycle_id="2026-05-29T15:57:22-04:00",
            trade_kpi=trade_kpi,
            reject_reasons=reject or {},
            contract_reject_reasons=contract_reject or {},
            order_reject_reasons=order_reject or {},
            rate_limit_status=rate or {},
            execution_loop_entered=entered,
        )

    def test_after_entry_window_is_rule_fault_not_strategy(self):
        diag = self._build(
            trade_kpi={
                "scanner_candidate_count": 20,
                "execution_candidate_count": 20,
                "contract_selected_count": 0,
                "order_attempted_count": 0,
                "trade_filled_count": 0,
            },
            reject={"after_entry_window": 20},
        )
        self.assertEqual(diag["pipeline_stage"], "entry_window_closed")
        self.assertTrue(diag["rule_fault"])
        self.assertFalse(diag["strategy_fault"])
        self.assertFalse(diag["learning_eligible"])
        self.assertEqual(diag["learning_skip_reason"], "rule_fault_entry_window_or_rule_gate")

    def test_blocked_unclassified_is_system_fault(self):
        diag = self._build(
            trade_kpi={
                "scanner_candidate_count": 12,
                "execution_candidate_count": 12,
                "contract_selected_count": 0,
                "order_attempted_count": 0,
                "trade_filled_count": 0,
            },
            reject={"blocked_unclassified": 7, "after_entry_window": 5},
        )
        self.assertTrue(diag["system_fault"])
        self.assertFalse(diag["learning_eligible"])
        self.assertEqual(diag["recommended_next_action"], "patch unclassified rejection path")

    def test_alpaca_429_cycle_is_data_fault(self):
        diag = self._build(
            trade_kpi={
                "scanner_candidate_count": 10,
                "execution_candidate_count": 10,
                "contract_selected_count": 0,
                "order_attempted_count": 0,
                "trade_filled_count": 0,
            },
            reject={"candidate_rejected_all": 10},
            rate={"data_source_degraded": True, "recent_429_count": 4},
        )
        self.assertTrue(diag["data_fault"])
        self.assertFalse(diag["learning_eligible"])
        self.assertEqual(diag["learning_skip_reason"], "data_fault_429_cooldown")

    def test_scanner_candidate_zero_orders_yields_concrete_primary_blocker(self):
        diag = self._build(
            trade_kpi={
                "scanner_candidate_count": 8,
                "execution_candidate_count": 8,
                "contract_selected_count": 0,
                "order_attempted_count": 0,
                "trade_filled_count": 0,
            },
            reject={"after_entry_window": 1, "blocked_entry_hour": 7},
        )
        self.assertIn(diag["primary_blocker"], {"blocked_entry_hour", "after_entry_window"})
        self.assertNotEqual(diag["primary_blocker"], "")

    def test_filled_trade_is_learning_eligible(self):
        diag = self._build(
            trade_kpi={
                "scanner_candidate_count": 3,
                "execution_candidate_count": 3,
                "contract_selected_count": 2,
                "order_attempted_count": 2,
                "trade_filled_count": 1,
            },
            reject={},
        )
        self.assertTrue(diag["learning_eligible"])
        self.assertEqual(diag["pipeline_stage"], "order_filled")

    def test_open_option_position_cap_is_risk_gate(self):
        diag = self._build(
            trade_kpi={
                "scanner_candidate_count": 5,
                "execution_candidate_count": 5,
                "contract_selected_count": 0,
                "order_attempted_count": 0,
                "trade_filled_count": 0,
            },
            reject={"open_option_position_cap": 5},
        )
        self.assertEqual(diag["pipeline_stage"], "risk_gate_failed")
        self.assertEqual(diag["primary_blocker"], "open_option_position_cap")
        self.assertFalse(diag["learning_eligible"])

    def test_tradable_pass_without_trade_is_system_fault(self):
        diag = self._build(
            trade_kpi={
                "scanner_candidate_count": 3,
                "execution_candidate_count": 2,
                "tradable_pass_count": 1,
                "contract_selected_count": 1,
                "order_attempted_count": 0,
                "trade_filled_count": 0,
                "bug_pass_without_trade_count": 1,
            },
            reject={"BUG_PASS_WITHOUT_TRADE": 1},
        )
        self.assertEqual(diag["pipeline_stage"], "pass_contract_broken")
        self.assertTrue(diag["system_fault"])
        self.assertEqual(diag["learning_skip_reason"], "system_fault_bug_pass_without_trade")

    def test_pass_means_trade_contract_marks_bug_when_tradable_has_no_trade(self):
        debug = {
            "tradable_pass_count": 1,
            "order_attempted_count": 0,
        }

        reasons = main._apply_pass_means_trade_contract(debug, {})

        self.assertEqual(debug["bug_pass_without_trade_count"], 1)
        self.assertFalse(debug["pass_means_trade_contract_ok"])
        self.assertEqual(reasons["BUG_PASS_WITHOUT_TRADE"], 1)

    def test_pass_means_trade_contract_ok_when_order_attempted(self):
        debug = {
            "tradable_pass_count": 1,
            "order_attempted_count": 1,
        }

        reasons = main._apply_pass_means_trade_contract(debug, {})

        self.assertEqual(debug["bug_pass_without_trade_count"], 0)
        self.assertTrue(debug["pass_means_trade_contract_ok"])
        self.assertEqual(reasons, {})

    def test_broker_reject_after_order_attempt_satisfies_pass_equals_trade(self):
        debug = {
            "tradable_pass_count": 1,
            "order_attempted_count": 1,
        }

        reasons = main._apply_pass_means_trade_contract(debug, {"broker_reject": 1})

        self.assertEqual(debug["bug_pass_without_trade_count"], 0)
        self.assertTrue(debug["pass_means_trade_contract_ok"])
        self.assertEqual(reasons, {"broker_reject": 1})


if __name__ == "__main__":
    unittest.main()
