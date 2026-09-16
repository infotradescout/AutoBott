/** Read-only account-scoped ingestion diagnostics. Never used for order submission. */
import assert from 'node:assert/strict';

export function runtimeAccounting(session) {
  const cycles = session?.state?.last_result?.cycle_results || [];
  const cycle = cycles.at(-1) || {};
  const outcomes = cycle.execution_outcomes || [];
  const accounting = outcomes.find(row => row.disposition === 'trade_outcome_learning_summary') || {};
  const monitor = outcomes.find(row => row.disposition === 'position_monitor_summary') || {};
  return {
    cycleStartedAt: cycle.started_at,
    cycleFinishedAt: cycle.finished_at,
    checkedPositions: monitor.checked,
    monitoringOk: monitor.ok,
    monitorActions: (monitor.actions || []).map(row => ({ reason: row.reason, symbol: row.symbol, submitted: row.submitted, quantity: row.quantity })),
    accounting: {
      ok: accounting.ok,
      error: accounting.error,
      historyComplete: accounting.history_complete,
      dailyPnlComplete: accounting.daily_pnl_complete,
      identityCheckComplete: accounting.identity_check_complete,
      reconciliation: accounting.reconciliation,
      snapshot: accounting.journal_snapshot,
      historyRewritten: accounting.journal_history_rewritten,
    },
    ordersSubmitted: (cycle.orders_submitted || []).length,
    rejections: cycle.execution_rejected_count_by_reason,
  };
}

export async function verifyRuntimeAccounting({ token, expected, request = fetch }) {
  assert(token && /^[a-f0-9]{40}$/.test(expected));
  const base = 'https://autobott-azl4.onrender.com';
  const get = async path => {
    const response = await request(base + path, { method: 'GET', redirect: 'error', headers: { Authorization: 'Bearer ' + token, Accept: 'application/json' }, signal: AbortSignal.timeout(45000) });
    assert.equal(response.status, 200, 'runtime_diagnostic_http_status');
    return response.json();
  };
  const health = await get('/api/health');
  assert.equal(health.version, expected);
  const state = await get('/api/session/status');
  const report = runtimeAccounting(state);
  assert.equal((await get('/api/health')).version, expected);
  return report;
}
