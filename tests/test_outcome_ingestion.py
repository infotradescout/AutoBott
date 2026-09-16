from copy import deepcopy
from pathlib import Path
import sys
import unittest
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from autobott_v2.outcome_ingestion import plan_outcome_append, stable_outcome_id

SCOPE = "alpaca:paper:synthetic-account"

def row(**updates):
    value = {
        "symbol": "TEST240517P00100000", "entry_broker_order_id": "entry-1",
        "exit_broker_order_id": "exit-1", "qty": 1, "entry_order_filled_qty": 1,
        "entry_price": 2.0, "exit_price": 1.5, "pnl": -50,
        "entry_time": "2024-05-15T16:40:00.111111+00:00",
        "exit_time": "2024-05-15T16:50:00.222222+00:00",
        "outcome_id": "old-timestamp-sensitive-id", "trade_group_id": "group-a",
        "leg_role": "primary", "policy_version": "synthetic-policy",
    }
    value.update(updates)
    return value

def plan(old, new):
    return plan_outcome_append(old, new, account_scope=SCOPE)

class OutcomeIngestionTests(unittest.TestCase):
    def test_first_record_has_scoped_stable_identity(self):
        p = plan([], [row()])
        self.assertTrue(p["append_safe"])
        self.assertEqual(len(p["new_rows"]), 1)
        self.assertEqual(p["new_rows"][0]["account_scope"], SCOPE)

    def test_one_microsecond_does_not_create_new_id(self):
        a, b = row(), row(exit_time="2024-05-15T16:50:00.222221Z")
        self.assertEqual(stable_outcome_id(a, account_scope=SCOPE), stable_outcome_id(b, account_scope=SCOPE))
        p = plan([a], [b])
        self.assertTrue(p["append_safe"])
        self.assertEqual(p["suppressed_replays"], 1)
        self.assertEqual(p["new_rows"], [])

    def test_restart_reload_is_idempotent(self):
        persisted = plan([], [row()])["new_rows"]
        p = plan(persisted, [row()])
        self.assertTrue(p["append_safe"])
        self.assertEqual(p["suppressed_replays"], 1)
        self.assertEqual(p["new_rows"], [])

    def test_timezone_rendering_is_equivalent(self):
        p = plan([row()], [row(exit_time="2024-05-15T11:50:00.222222-05:00")])
        self.assertTrue(p["append_safe"])
        self.assertEqual(p["suppressed_replays"], 1)

    def test_same_economics_different_orders_remain_distinct(self):
        p = plan([], [row(), row(entry_broker_order_id="entry-2", exit_broker_order_id="exit-2")])
        self.assertEqual(len(p["new_rows"]), 2)
        self.assertNotEqual(p["new_rows"][0]["outcome_id"], p["new_rows"][1]["outcome_id"])

    def test_two_partial_exits_are_not_collapsed(self):
        a = row(entry_order_filled_qty=2)
        b = row(entry_order_filled_qty=2, exit_broker_order_id="exit-2")
        self.assertEqual(len(plan([], [a, b])["new_rows"]), 2)

    def test_one_sell_consuming_two_entry_lots_is_preserved(self):
        self.assertEqual(len(plan([], [row(), row(entry_broker_order_id="entry-2")])["new_rows"]), 2)

    def test_repeat_inside_same_batch_is_not_double_appended(self):
        p = plan([], [row(), row(exit_time="2024-05-15T16:50:00.222221Z")])
        self.assertEqual(len(p["new_rows"]), 1)
        self.assertEqual(p["suppressed_replays"], 1)

    def test_changed_price_is_conflict_not_new_fill(self):
        p = plan([row()], [row(exit_price=1.6, pnl=-40)])
        self.assertFalse(p["append_safe"])
        self.assertTrue(p["conflicts"])

    def test_changed_quantity_is_conflict(self):
        p = plan([row()], [row(qty=2, entry_order_filled_qty=2, pnl=-100)])
        self.assertFalse(p["append_safe"])

    def test_changed_group_or_role_or_policy_is_conflict(self):
        for field in ("trade_group_id", "leg_role", "policy_version", "decision_id", "build_sha"):
            with self.subTest(field=field):
                p = plan([row()], [row(**{field: "different"})])
                self.assertFalse(p["append_safe"])
                self.assertTrue(p["conflicts"])

    def test_material_timestamp_change_requires_review(self):
        p = plan([row()], [row(exit_time="2024-05-15T16:50:00.222224Z")])
        self.assertFalse(p["append_safe"])

    def test_tolerance_does_not_chain(self):
        p = plan([], [row(), row(exit_time="2024-05-15T16:50:00.222221Z"),
                      row(exit_time="2024-05-15T16:50:00.222223Z")])
        self.assertFalse(p["append_safe"])

    def test_one_microsecond_across_reporting_day_is_conflict(self):
        a = row(exit_time="2024-05-16T04:59:59.999999Z")
        b = row(exit_time="2024-05-16T05:00:00.000000Z")
        self.assertFalse(plan([a], [b])["append_safe"])

    def test_existing_duplicates_preserved_and_not_declared_repaired(self):
        a, b = row(), row(exit_time="2024-05-15T16:50:00.222221Z", outcome_id="second-old-id")
        original = deepcopy([a, b])
        p = plan([a, b], [])
        self.assertFalse(p["accounting_complete"])
        self.assertEqual(len(p["historical_duplicates"]), 1)
        self.assertFalse(p["history_rewritten"])
        self.assertEqual([a, b], original)

    def test_no_source_mutation(self):
        old, new = [row()], [row(entry_broker_order_id="entry-2")]
        before = deepcopy([old, new])
        p = plan(old, new)
        p["new_rows"][0]["leg_role"] = "changed"
        self.assertEqual([old, new], before)

    def test_missing_broker_ids_is_not_guessed(self):
        for field in ("entry_broker_order_id", "exit_broker_order_id"):
            with self.subTest(field=field):
                self.assertFalse(plan([], [row(**{field: None})])["append_safe"])

    def test_empty_account_scope_rejected(self):
        for scope in ("", " ", None):
            with self.subTest(scope=scope), self.assertRaises(ValueError):
                plan_outcome_append([], [], account_scope=scope)

    def test_accounts_have_different_ids(self):
        self.assertNotEqual(stable_outcome_id(row(), account_scope=SCOPE),
                            stable_outcome_id(row(), account_scope="alpaca:paper:another-account"))

    def test_account_scope_conflict_is_blocked(self):
        p = plan([row(account_scope="alpaca:live:other-account")], [row()])
        self.assertFalse(p["append_safe"])

    def test_invalid_numeric_values_not_converted_to_zero(self):
        for value in (None, "", float("nan"), float("inf"), True, "not-a-number"):
            with self.subTest(value=value):
                self.assertFalse(plan([], [row(entry_price=value)])["append_safe"])

    def test_inconsistent_pnl_rejected(self):
        self.assertFalse(plan([], [row(pnl=-100)])["append_safe"])

    def test_overmatched_quantity_rejected(self):
        self.assertFalse(plan([], [row(qty=2, pnl=-100)])["append_safe"])

    def test_provisional_partial_fill_not_appended(self):
        self.assertFalse(plan([], [row(provisional_fill=True)])["append_safe"])

    def test_naive_timestamp_not_accepted(self):
        self.assertFalse(plan([], [row(exit_time="2024-05-15T16:51:07")])["append_safe"])

    def test_missing_history_identity_blocks_entire_batch(self):
        p = plan([row(entry_broker_order_id=None)], [row(entry_broker_order_id="entry-2")])
        self.assertFalse(p["append_safe"])
        self.assertEqual(p["new_rows"], [])

    def test_conflict_blocks_unrelated_append_in_same_transaction(self):
        p = plan([row()], [row(entry_broker_order_id="entry-2"), row(exit_price=1.6, pnl=-40)])
        self.assertEqual(p["new_rows"], [])

    def test_total_allocated_exit_qty_cannot_exceed_entry(self):
        p = plan([row()], [row(exit_broker_order_id="exit-2")])
        self.assertFalse(p["append_safe"])
        self.assertEqual(p["new_rows"], [])

    def test_entry_cumulative_quantity_cannot_disagree_across_exits(self):
        p = plan([row(entry_order_filled_qty=2)], [row(exit_broker_order_id="exit-2", entry_order_filled_qty=3)])
        self.assertFalse(p["append_safe"])

    def test_missing_entry_fill_quantity_requires_review(self):
        self.assertFalse(plan([], [row(entry_order_filled_qty=None)])["append_safe"])

    def test_identity_validation_is_not_complete_broker_accounting(self):
        p = plan([], [row()])
        self.assertTrue(p["identity_check_complete"])
        self.assertFalse(p["accounting_complete"])

    def test_partial_exits_must_keep_same_entry_provenance(self):
        p = plan([row(entry_order_filled_qty=2)], [row(exit_broker_order_id="exit-2", entry_order_filled_qty=2, trade_group_id="different")])
        self.assertFalse(p["append_safe"])

    def test_partial_exits_must_keep_same_entry_price(self):
        p = plan([row(entry_order_filled_qty=2)], [row(exit_broker_order_id="exit-2", entry_order_filled_qty=2, entry_price=3, pnl=-150)])
        self.assertFalse(p["append_safe"])

    def test_exact_zero_remains_valid(self):
        p = plan([], [row(exit_price=2, pnl=0)])
        self.assertTrue(p["append_safe"])
        self.assertEqual(p["new_rows"][0]["pnl"], 0)

    def test_missing_pnl_is_not_zero(self):
        self.assertFalse(plan([], [row(pnl=None)])["append_safe"])

    def test_negative_exit_price_invalid(self):
        self.assertFalse(plan([], [row(exit_price=-1, pnl=-300)])["append_safe"])

    def test_empty_batch_is_valid(self):
        self.assertTrue(plan([], [])["append_safe"])

    def test_string_numbers_equivalent(self):
        p = plan([row()], [row(qty="1.0", entry_price="2.000", exit_price="1.500", pnl="-50.00")])
        self.assertTrue(p["append_safe"])

    def test_adjusted_contract_not_silently_assumed(self):
        self.assertFalse(plan([], [row(contract_multiplier=10)])["append_safe"])

if __name__ == "__main__":
    unittest.main()
