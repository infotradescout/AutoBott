/** Read-only runtime ingestion diagnostics; never submits a broker order. */
import assert from 'node:assert/strict';

export function reconciliationSummary(plan = {}) {
  const unresolved = plan.unresolved || [], conflicts = plan.conflicts || [], duplicates = plan.historical_duplicates || [];
  const reasons = rows => rows.reduce((all, row) => { const key = row.reason || 'unspecified'; all[key] = (all[key] || 0) + 1; return all; }, {});
  const examples = rows => rows.slice(0, 5).map(row => Object.fromEntries(Object.entries(row).filter(([key]) => ['reason', 'origin', 'index', 'existing_index', 'candidate_index'].includes(key))));
  return { appendSafe: plan.append_safe, identityCheckComplete: plan.identity_check_complete, requiresReconciliation: plan.requires_reconciliation, unresolvedCount: unresolved.length, unresolvedReasons: reasons(unresolved), unresolvedExamples: examples(unresolved), conflictCount: conflicts.length, conflictReasons: reasons(conflicts), conflictExamples: examples(conflicts), historicalDuplicateCount: duplicates.length, duplicateExamples: examples(duplicates), historyRewritten: plan.history_rewritten };
}

export function runtimeAccounting(session) {
  const cycle = (session?.state?.last_result?.cycle_results || []).at(-1) || {};
  const outcomes = cycle.execution_outcomes || [];
  const accounting = outcomes.find(row => row.disposition === 'trade_outcome_learning_summary') || {};
  const monitor = outcomes.find(row => row.disposition === 'position_monitor_summary') || {};
  return { cycleStartedAt: cycle.started_at, cycleFinishedAt: cycle.finished_at, checkedPositions: monitor.checked, monitoringOk: monitor.ok, monitorActions: (monitor.actions || []).map(row => ({ reason: row.reason, symbol: row.symbol, submitted: row.submitted, quantity: row.quantity })), accounting: { ok: accounting.ok, error: accounting.error, historyComplete: accounting.history_complete, dailyPnlComplete: accounting.daily_pnl_complete, identityCheckComplete: accounting.identity_check_complete, reconciliation: reconciliationSummary(accounting.reconciliation), snapshotPresent: Boolean(accounting.journal_snapshot), historyRewritten: accounting.journal_history_rewritten }, ordersSubmitted: (cycle.orders_submitted || []).length, rejections: cycle.execution_rejected_count_by_reason };
}

export async function verifyRuntimeAccounting({ token, expected, request = fetch }) {
  assert(token && /^[a-f0-9]{40}$/.test(expected));
  const get = async path => {
    assert(['/api/health', '/api/session/status'].includes(path));
    const response = await request('https://autobott-azl4.onrender.com' + path, { method: 'GET', redirect: 'error', headers: { Authorization: 'Bearer ' + token, Accept: 'application/json' }, signal: AbortSignal.timeout(45000) });
    assert.equal(response.status, 200, 'runtime_diagnostic_http_status');
    return response.json();
  };
  assert.equal((await get('/api/health')).version, expected);
  const report = runtimeAccounting(await get('/api/session/status'));
  assert.equal((await get('/api/health')).version, expected);
  return report;
}

if (process.env.AUTOBOTT_VERIFY_DEPLOYED === 'true' && process.env.AUTOBOTT_VERIFY_RUNTIME_ACCOUNTING === 'true' && !process.env.NODE_TEST_CONTEXT) {
  const report = await verifyRuntimeAccounting({ token: process.env.AUTOBOTT_VERIFY_TOKEN, expected: process.env.AUTOBOTT_VERIFY_EXPECTED_SHA });
  console.log('AUTOBOTT_RUNTIME_ACCOUNTING ' + JSON.stringify(report));
  delete process.env.AUTOBOTT_VERIFY_RUNTIME_ACCOUNTING;
  delete process.env.NODE_OPTIONS;
}
