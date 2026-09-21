"""Synthetic diagnostics tests. No broker, credentials, socket or subprocess."""
import ast
import copy
import importlib.util
import io
import json
from pathlib import Path
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from datetime import UTC, datetime

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('entry_runtime_status_under_test', ROOT / 'src/autobott_v2/entry_runtime_status.py')
module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(module)
AT = datetime(2026, 9, 21, 15, 40, tzinfo=UTC)


def cycle():
    return {'started_at': '2026-09-21T15:39:00+00:00', 'finished_at': AT.isoformat(),
            'decisions': [{}, {}], 'scanner_candidates_count': 2, 'trade_attempted_count': 0,
            'orders_submitted': [], 'execution_rejected_count_by_reason': {},
            'execution_outcomes': [{'disposition': 'trade_outcome_learning_summary',
                'ok': True, 'daily_realized_pnl': 0.0, 'reconciliation': {
                    'unresolved': [], 'conflicts': [], 'historical_duplicates': []}}]}


class EntryStatusTests(unittest.TestCase):
    def test_reconciliation_block_is_explicit_even_without_candidates(self):
        c = cycle(); c['scanner_candidates_count'] = 0
        c['execution_outcomes'][0].update(ok=False, error='outcome_journal_reconciliation_required')
        s = module.build_entry_status(c, observed_at=AT)
        self.assertEqual(s['status'], 'BLOCKED')
        self.assertEqual(s['reason_code'], 'outcome_journal_reconciliation_required')
        self.assertFalse(s['accounting_available'])

    def test_daily_pnl_rejection_identifies_entry_block(self):
        c = cycle(); c['execution_rejected_count_by_reason'] = {'daily_pnl_unavailable': 2}
        s = module.build_entry_status(c, observed_at=AT)
        self.assertEqual(s['status'], 'BLOCKED'); self.assertEqual(s['rejections_count'], 2)

    def test_empty_candidate_scan_does_not_deny_market_opportunities(self):
        c = cycle(); c['scanner_candidates_count'] = 0
        s = module.build_entry_status(c, observed_at=AT)
        self.assertEqual(s['status'], 'NO_CANDIDATES')
        self.assertIn('does not mean the market had no opportunities', s['reason'])

    def test_submission_is_not_labeled_fill_or_profit(self):
        c = cycle(); c['orders_submitted'] = [{'id': 'synthetic-order'}]
        s = module.build_entry_status(c, observed_at=AT)
        self.assertEqual(s['status'], 'SUBMITTED'); self.assertEqual(s['entry_submissions_count'], 1)
        self.assertIn('not confirmation of a fill or profit', s['reason'])

    def test_candidates_without_entries_are_not_green_ready(self):
        self.assertEqual(module.build_entry_status(cycle(), observed_at=AT)['status'], 'NO_ENTRIES')

    def test_missing_accounting_does_not_become_success(self):
        c = cycle(); c['execution_outcomes'] = []
        self.assertEqual(module.build_entry_status(c, observed_at=AT)['status'], 'UNKNOWN')

    def test_conflicting_accounting_rows_are_unknown(self):
        c = cycle(); c['execution_outcomes'] *= 2
        self.assertEqual(module.build_entry_status(c, observed_at=AT)['status'], 'UNKNOWN')

    def test_unknown_counts_are_not_zero(self):
        c = cycle(); del c['orders_submitted']; del c['scanner_candidates_count']
        s = module.build_entry_status(c, observed_at=AT)
        self.assertEqual(s['status'], 'UNKNOWN'); self.assertIsNone(s['entry_submissions_count'])
        self.assertIsNone(s['candidates_count'])

    def test_invalid_rejection_counts_are_not_zero_or_accepted(self):
        for value in [-1, True, '2', float('nan')]:
            with self.subTest(value=value):
                c = cycle(); c['execution_rejected_count_by_reason'] = {'other': value}
                s = module.build_entry_status(c, observed_at=AT)
                self.assertIsNone(s['rejections_count']); self.assertEqual(s['status'], 'UNKNOWN')

    def test_nonfinite_daily_results_are_unavailable(self):
        for value in [None, '', True, float('inf'), 'nan']:
            with self.subTest(value=value):
                c = cycle(); c['execution_outcomes'][0]['daily_realized_pnl'] = value
                s = module.build_entry_status(c, observed_at=AT)
                self.assertFalse(s['accounting_available']); self.assertEqual(s['status'], 'BLOCKED')

    def test_timestamps_require_explicit_timezone(self):
        c = cycle(); c['finished_at'] = '2026-09-21T15:40:00'
        self.assertEqual(module.build_entry_status(c, observed_at=AT)['status'], 'UNKNOWN')
        with self.assertRaises(ValueError): module.build_entry_status(c, observed_at=AT.replace(tzinfo=None))

    def test_provider_errors_and_account_data_are_not_logged(self):
        c = cycle(); secret = 'synthetic-private-token-not-for-log'
        c.update(account={'balance': 12345, 'key': secret}, symbols=[secret], snapshot_paths=[secret])
        c['execution_outcomes'][0].update(ok=False, error=secret, account_id=secret)
        c['execution_rejected_count_by_reason'] = {secret: 4}
        c['execution_outcomes'][0]['reconciliation']['unresolved'] = [{'id': secret}]
        output = io.StringIO()
        with patch('sys.stdout', output): module.emit_entry_status(module.build_entry_status(c, observed_at=AT))
        data = output.getvalue(); self.assertNotIn(secret, data); self.assertNotIn('12345', data)
        self.assertEqual(json.loads(data.split(' ', 1)[1])['unresolved_count'], 1)

    def test_cycle_error_is_redacted(self):
        c = cycle(); c['error'] = 'secret broker failure text'
        s = module.build_entry_status(c, observed_at=AT)
        self.assertEqual(s['status'], 'ERROR'); self.assertNotIn(c['error'], json.dumps(s))

    def test_reconciliation_conflict_overrides_contradictory_ok_flag(self):
        c = cycle(); c["execution_outcomes"][0]["reconciliation"]["conflicts"] = [{"reason": "synthetic"}]
        self.assertEqual(module.build_entry_status(c, observed_at=AT)["status"], "BLOCKED")

    def test_input_is_unchanged(self):
        c = cycle(); original = copy.deepcopy(c)
        module.build_entry_status(c, observed_at=AT); self.assertEqual(c, original)

    def test_stdout_failure_is_nonfatal(self):
        with patch('builtins.print', side_effect=OSError('log unavailable')):
            module.emit_entry_status(module.build_entry_status(cycle(), observed_at=AT))

    def test_invalid_payloads_are_unknown_without_raw_output(self):
        for value in [None, [], 'private-token', 123]:
            self.assertEqual(module.build_entry_status(value, observed_at=AT)['status'], 'UNKNOWN')


class CallbackIntegrationTests(unittest.TestCase):
    def callback(self, build=module.build_entry_status, emit=None):
        tree = ast.parse((ROOT / 'src/autobott_v2/session_supervisor.py').read_text())
        fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == '_record_cycle_result')
        state = SimpleNamespace(cycles_completed=0)
        lock = threading.Lock(); emitted = []
        def record(status):
            self.assertTrue(lock.acquire(blocking=False), 'log I/O must be outside runtime-state lock')
            lock.release(); emitted.append(status)
        scope = {'Any': object, '_SESSION_STATE': state, '_SESSION_LOCK': lock,
                 'datetime': datetime, 'UTC': UTC, 'build_entry_status': build,
                 'emit_entry_status': emit or record}
        exec(compile(ast.Module(body=[fn], type_ignores=[]), '<actual_callback>', 'exec'), scope)
        return scope['_record_cycle_result'], state, emitted

    def test_actual_callback_publishes_summary_and_preserves_cycle(self):
        call, state, output = self.callback(); c = cycle(); call(c)
        self.assertEqual(state.cycles_completed, 1)
        self.assertIs(state.last_result['cycle_results'][0], c)
        self.assertEqual(state.last_entry_status, output[0])

    def test_diagnostic_builder_failure_does_not_stop_callback(self):
        def broken(_): raise RuntimeError('synthetic')
        call, state, output = self.callback(build=broken); call(cycle())
        self.assertEqual(state.cycles_completed, 1); self.assertIsNone(state.last_entry_status); self.assertFalse(output)

    def test_diagnostic_sink_failure_does_not_stop_callback(self):
        def broken(_): raise OSError('synthetic')
        call, state, _ = self.callback(emit=broken); call(cycle())
        self.assertEqual(state.cycles_completed, 1); self.assertIsNotNone(state.last_entry_status)


if __name__ == '__main__': unittest.main()
