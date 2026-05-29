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

    def test_scanner_pass_zero_orders_yields_concrete_primary_blocker(self):
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


if __name__ == "__main__":
    unittest.main()

