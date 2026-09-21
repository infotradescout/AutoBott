/** Explicit opt-in authenticated GET-only diagnostic; never an order or cycle. */
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { verifyRuntimeAccounting } from './verify_runtime_reconciliation.mjs';
const HOST = 'https://autobott-azl4.onrender.com';
const PATHS = new Set(['/api/health', '/api/safety', '/api/account/positions', '/api/positions/open']);
const countReasons = rows => (rows || []).reduce((out, row) => {
  for (const reason of row.reasons || [row.reason || 'unspecified']) out[reason] = (out[reason] || 0) + 1;
  return out;
}, {});
const hash = value => createHash('sha256').update(String(value)).digest('hex');
export function detailedSession(session = {}) {
  const state = session.state || {}, cycle = (state.last_result?.cycle_results || []).at(-1) || {};
  const accounting = (cycle.execution_outcomes || []).find(row => row.disposition === 'trade_outcome_learning_summary') || {};
  const rows = accounting.broker_outcomes || [];
  const stamps = rows.flatMap(row => [row.entry_time, row.exit_time]).filter(v => typeof v === 'string').sort();
  return {
    running: state.running, threadAlive: session.thread_alive, cyclesCompleted: state.cycles_completed,
    lastCycleAt: state.last_cycle_at, lastErrorPresent: Boolean(state.last_error),
    scanIntervalSeconds: session.config?.interval_seconds, symbolBatchSize: session.config?.symbol_batch_size,
    configuredSymbolCount: session.config?.symbols?.length,
    currentBrokerOutcomeCount: rows.length, brokerOutcomeFirstTime: stamps[0], brokerOutcomeLastTime: stamps.at(-1),
    currentDayUnmatchedSellSymbols: accounting.current_day_unmatched_sell_symbols || [],
    historicalUnmatchedSellSymbols: accounting.historical_unmatched_sell_symbols || [],
    detailedConflictReasons: countReasons(accounting.reconciliation?.conflicts),
    incomingUnresolvedCount: (accounting.reconciliation?.unresolved || []).filter(row => row.origin === 'incoming').length,
  };
}
export function positionSummary(payload = {}, stored = false) {
  const rows = payload.positions || [];
  return { ok: payload.ok, count: rows.length, rows: rows.map(row => ({
    symbol: stored ? row.option_symbol : row.symbol,
    quantity: stored ? row.quantity : row.qty,
    status: row.status,
    ...(stored && row.broker_order_id ? { orderIdentityHash: hash(row.broker_order_id) } : {}),
  })) };
}
export async function runReadonlyPreflight({ env = process.env, request = fetch, write = console.log } = {}) {
  if (env.AUTOBOTT_VERIFY_READONLY_PREFLIGHT !== 'true') return false;
  const token = env.AUTOBOTT_VERIFY_TOKEN, expected = env.AUTOBOTT_VERIFY_EXPECTED_SHA;
  delete env.AUTOBOTT_VERIFY_TOKEN; delete env.AUTOBOTT_VERIFY_READONLY_PREFLIGHT; delete env.NODE_OPTIONS;
  let detail;
  const capture = async (url, options) => {
    const response = await request(url, options);
    if (url === HOST + '/api/session/status' && response.status === 200) {
      const session = await response.json(); detail = detailedSession(session);
      return { status: response.status, json: async () => session };
    }
    return response;
  };
  const report = await verifyRuntimeAccounting({ token, expected, request: capture });
  const get = async path => {
    assert(PATHS.has(path));
    const response = await request(HOST + path, {method:'GET',redirect:'error',headers:{Authorization:'Bearer '+token,Accept:'application/json'},signal:AbortSignal.timeout(45000)});
    assert.equal(response.status,200,'runtime_detail_http_status'); return response.json();
  };
  const safety = await get('/api/safety');
  const brokerPositions = positionSummary(await get('/api/account/positions'));
  const storedPositions = positionSummary(await get('/api/positions/open'),true);
  assert.equal((await get('/api/health')).version,expected);
  write('AUTOBOTT_RUNTIME_ACCOUNTING ' + JSON.stringify({
    observedAt:new Date().toISOString(),expectedSource:expected,...report,detail,
    safety:{paperOnly:safety.paper_only,liveTradingEnabled:safety.live_trading_enabled,orderPlacementEnabled:safety.order_placement_enabled,executionEnabled:safety.execution_enabled,killSwitchEnabled:safety.kill_switch_enabled},
    brokerPositions,storedPositions,
  }));
  return true;
}
// Explicit integration preflight before cockpit tests, not part of offline tests.
await runReadonlyPreflight();
