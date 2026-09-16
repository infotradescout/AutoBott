/** GET-only acceptance of the existing AutoBott deployment. No broker writes. */
import assert from 'node:assert/strict';

const ORIGIN = 'https://autobott-azl4.onrender.com';
const PATHS = new Set(['/', '/api/health', '/api/safety', '/api/v2/pairs', '/api/session/status', '/api/account/positions', '/api/positions/open', '/api/trading/timeline']);

export function inventory(rows) {
  assert(Array.isArray(rows), 'position_array_required');
  const quantities = new Map(), counts = new Map();
  for (const row of rows) {
    // Stored rows have both an underlying symbol and an option symbol.
    const symbol = String(row.option_symbol || row.symbol || '').toUpperCase();
    const quantity = Number(row.qty ?? row.quantity);
    assert(symbol && Number.isFinite(quantity), 'invalid_position_identity_or_quantity');
    quantities.set(symbol, (quantities.get(symbol) || 0) + quantity);
    counts.set(symbol, (counts.get(symbol) || 0) + 1);
  }
  return { quantities: Object.fromEntries([...quantities].sort()), duplicateSymbols: [...counts].filter(([, count]) => count > 1).map(([symbol]) => symbol).sort() };
}

function bounded(value, depth = 0) {
  if (depth > 7) return '[depth bounded]';
  if (typeof value === 'string') return value.slice(0, 400);
  if (Array.isArray(value)) return value.slice(-3).map(item => bounded(item, depth + 1));
  if (!value || typeof value !== 'object') return value;
  return Object.fromEntries(Object.entries(value).filter(([key]) => !/secret|token|password|credential|account_id|account_number|authorization|api_key|cash|equity|buying_power/i.test(key) && !['decisions', 'snapshot_paths', 'outcomes', 'broker_outcomes', 'round_trips', 'completed_groups'].includes(key)).slice(0, 45).map(([key, item]) => [key, bounded(item, depth + 1)]));
}

function positionEvidence(rows) {
  const fields = new Set(['symbol', 'option_symbol', 'qty', 'quantity', 'status', 'leg_role', 'trade_group_id', 'decision_id', 'broker_order_id', 'entry_price', 'avg_entry_price', 'opened_at', 'updated_at', 'entry_build_sha']);
  return rows.map(row => Object.fromEntries(Object.entries(row).filter(([key]) => fields.has(key))));
}

function safeFlags(value, depth = 0) {
  if (!value || typeof value !== 'object' || Array.isArray(value) || depth > 5) return {};
  const result = {};
  for (const [key, item] of Object.entries(value)) {
    if (/secret|token|password|credential|account_id|account_number/i.test(key)) continue;
    if (/^(ok|enabled|running|alive|execution_enabled|kill_switch_enabled|accounting_complete|journal_write_blocked|entry_eligible|history_complete|scope_verified|daily_pnl_available|recorded|checked|updated|missing|cycle_count|cycles_completed|last_started_at|last_finished_at|last_completed_at|last_cycle_started_at|last_cycle_finished_at)$/.test(key) && (typeof item === 'boolean' || typeof item === 'number' || typeof item === 'string' || item === null)) result[key] = item;
    else if (item && typeof item === 'object' && !Array.isArray(item)) {
      const nested = safeFlags(item, depth + 1);
      if (Object.keys(nested).length) result[key] = nested;
    }
  }
  return result;
}

export async function verify({ token, expected, request = fetch }) {
  assert(typeof token === 'string' && token.length >= 32, 'private_verification_token_required');
  assert(/^[a-f0-9]{40}$/.test(expected || ''), 'exact_release_sha_required');
  const checks = [];
  async function get(path, auth, status = 200, text = false) {
    assert(PATHS.has(path), 'unapproved_read_path');
    const headers = { Accept: text ? 'text/html' : 'application/json', 'User-Agent': 'AutoBott-ReadOnly-Release-Verification/1.0' };
    if (auth) headers.Authorization = 'Bearer ' + auth;
    const response = await request(ORIGIN + path, { method: 'GET', headers, redirect: 'error', signal: AbortSignal.timeout(45000) });
    assert.equal(response.status, status, 'http_status_' + path);
    const body = await response.text();
    assert(body.length < 20000000, 'response_too_large');
    const data = text ? body : JSON.parse(body);
    checks.push({ path, authenticated: Boolean(auth), status: response.status });
    return data;
  }
  const before = await get('/api/health');
  assert.equal(before.version, expected, 'deployed_revision_mismatch');
  assert.equal(before.ok, true, 'health_not_ok');
  assert.equal(before.policy_version, 'hosted-core-runner-v2', 'old_policy_still_running');
  const html = await get('/', null, 200, true);
  assert(html.includes('Core + Runner Trades') && html.includes('access-token'), 'new_cockpit_missing');
  for (const path of ['/api/safety', '/api/v2/pairs', '/api/session/status', '/api/trading/timeline']) {
    assert.deepEqual(await get(path, null, 401), { ok: false, error: 'unauthorized' }, 'generic_unauthorized_required');
  }
  await get('/api/v2/pairs', 'invalid-release-verification-token', 401);
  const safety = await get('/api/safety', token);
  assert.equal(safety.alpaca_env, 'paper', 'not_paper_environment');
  assert.equal(safety.paper_only, true, 'paper_only_not_enabled');
  assert.equal(safety.live_trading_enabled, false, 'live_money_not_disabled');
  const account = await get('/api/account/positions', token);
  assert.equal(account.ok, true, 'broker_account_read_failed');
  const brokerInventory = inventory(account.positions);
  const stored = await get('/api/positions/open', token);
  assert.equal(stored.ok, true, 'stored_inventory_read_failed');
  const storedInventory = inventory(stored.positions);
  const pairs = await get('/api/v2/pairs', token);
  assert.equal(pairs.ok, true, 'pair_read_failed');
  const session = await get('/api/session/status', token);
  const timeline = await get('/api/trading/timeline', token);
  const learning = timeline.outcome_learning || {};
  const accountAfter = await get('/api/account/positions', token);
  assert.equal(accountAfter.ok, true, 'broker_confirmation_read_failed');
  const stable = JSON.stringify(brokerInventory.quantities) === JSON.stringify(inventory(accountAfter.positions).quantities);
  const differences = [...new Set([...Object.keys(brokerInventory.quantities), ...Object.keys(storedInventory.quantities)])].sort().filter(symbol => brokerInventory.quantities[symbol] !== storedInventory.quantities[symbol]).map(symbol => ({ symbol, broker: brokerInventory.quantities[symbol] ?? 0, stored: storedInventory.quantities[symbol] ?? 0 }));
  const after = await get('/api/health');
  assert.equal(after.version, expected, 'revision_changed_during_verification');
  assert.equal(after.ok, true, 'health_failed_after_verification');
  return { verifiedAt: new Date().toISOString(), sourceSha: expected, releaseVerified: true, scope: 'GET-only revision, health, authentication, paper controls and account reads. Not profitability or complete historical accounting.', checks, paperOnly: true, realMoneyEnabled: false, executionEnabled: safety.execution_enabled, orderPlacementEnabled: safety.order_placement_enabled, killSwitchEnabled: safety.kill_switch_enabled, inventory: { brokerRows: account.positions.length, storedRows: stored.positions.length, snapshotStable: stable, quantityDifferences: differences, storedDuplicateSymbols: storedInventory.duplicateSymbols }, positionEvidence: { broker: positionEvidence(account.positions), stored: positionEvidence(stored.positions) }, pairs: { count: pairs.pair_count, complete: pairs.open_pair_count, retainedRunners: pairs.retained_runner_count }, session: safeFlags(session), sessionRuntime: bounded(session.state), accounting: safeFlags(learning), accountingDiagnostics: { reconciliation: bounded(learning.reconciliation), journal: bounded(learning.journal_diagnostics), snapshot: bounded(learning.journal_snapshot), persistenceEnabled: learning.persistence_enabled, journalHistoryRewritten: learning.journal_history_rewritten }, warnings: (timeline.warnings || []).map(row => typeof row === 'object' ? row.type : 'unstructured_warning').slice(0, 20), brokerWrites: 0 };
}

if (process.env.AUTOBOTT_VERIFY_DEPLOYED === 'true' && !process.env.NODE_TEST_CONTEXT) {
  const token = process.env.AUTOBOTT_VERIFY_TOKEN;
  const expected = process.env.AUTOBOTT_VERIFY_EXPECTED_SHA;
  delete process.env.AUTOBOTT_VERIFY_TOKEN;
  delete process.env.AUTOBOTT_VERIFY_DEPLOYED;
  delete process.env.NODE_OPTIONS;
  try {
    console.log('AUTOBOTT_DEPLOYED_ACCEPTANCE ' + JSON.stringify(await verify({ token, expected })));
  } catch (error) {
    console.error('AUTOBOTT_DEPLOYED_ACCEPTANCE ' + JSON.stringify({ releaseVerified: false, sourceSha: expected, error: error instanceof assert.AssertionError ? error.message : error.name, brokerWrites: 0 }));
    process.exitCode = 1;
    throw new Error('AutoBott read-only acceptance failed; see private build log');
  }
}
