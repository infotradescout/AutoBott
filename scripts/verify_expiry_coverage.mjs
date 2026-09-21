/** Existing protected session reads only; no capture, cycle, or order trigger. */
import assert from 'node:assert/strict';
const HOST = 'https://autobott-azl4.onrender.com';
const PATHS = new Set(['/api/health', '/api/session/status', '/api/safety']);

export function expirySummary(session = {}) {
  const cycle = (session.state?.last_result?.cycle_results || []).at(-1) || {};
  const outcomes = cycle.execution_outcomes || [];
  const coverage = outcomes.find(row => row.disposition === 'option_expiration_coverage');
  const accounting = outcomes.find(row => row.disposition === 'trade_outcome_learning_summary');
  return {
    cycleStartedAt: cycle.started_at, cycleFinishedAt: cycle.finished_at,
    cyclesCompleted: session.state?.cycles_completed, threadAlive: session.thread_alive,
    accountingOk: accounting?.ok, dailyPnlComplete: accounting?.daily_pnl_complete,
    observedCoveragePresent: Boolean(coverage),
    coverage: (coverage?.symbols || []).slice(0, 25).map(row => {
      const expirations = Array.isArray(row.expirations) ? row.expirations : [];
      const count = (min, max) => expirations.filter(r => r.calendar_dte >= min && r.calendar_dte <= max)
        .reduce((n, r) => n + r.calls + r.puts, 0);
      return {symbol: row.symbol, status: row.status, observedAt: row.observed_at,
        referenceDate: row.reference_date, contractsReturned: row.contracts_returned,
        contractsCounted: row.contracts_counted, invalidContracts: row.invalid_contracts,
        baseline5to10: count(5, 10), extension11to13: count(11, 13),
        extensionDates: expirations.filter(r => r.calendar_dte >= 11 && r.calendar_dte <= 13)
          .map(r => ({expiration: r.expiration, calendarDte: r.calendar_dte, calls: r.calls, puts: r.puts}))};
    }),
    decisions: (cycle.decisions || []).slice(0, 25).map(row => ({symbol: row.ticker,
      status: row.decision, layer: row.execution_layer,
      selectedContract: row.selected_contract ? {symbol: row.selected_contract.option_symbol,
        expiration: row.selected_contract.expiration, bid: row.selected_contract.bid,
        ask: row.selected_contract.ask} : null})),
    rejections: cycle.execution_rejected_count_by_reason || {},
    submitted: (cycle.orders_submitted || []).map(row => ({symbol: row.option_symbol,
      underlying: row.symbol, state: row.state})),
  };
}

export async function runExpiryCoverage({env = process.env, request = fetch, write = console.log} = {}) {
  if (env.AUTOBOTT_VERIFY_EXPIRY_COVERAGE !== 'true') return false;
  const token = env.AUTOBOTT_VERIFY_TOKEN, expected = env.AUTOBOTT_VERIFY_EXPECTED_SHA;
  delete env.AUTOBOTT_VERIFY_TOKEN; delete env.AUTOBOTT_VERIFY_EXPIRY_COVERAGE;
  delete env.AUTOBOTT_VERIFY_READONLY_PREFLIGHT; delete env.NODE_OPTIONS;
  assert(typeof token === 'string' && token.length > 0 && /^[a-f0-9]{40}$/.test(expected), 'verification_configuration_required');
  const get = async path => {
    assert(PATHS.has(path));
    const response = await request(HOST + path, {method: 'GET', redirect: 'error',
      headers: {Authorization: 'Bearer ' + token, Accept: 'application/json'}, signal: AbortSignal.timeout(45000)});
    assert.equal(response.status, 200, 'expiry_verification_http_status');
    return response.json();
  };
  const health = await get('/api/health');
  assert.equal(health.version, expected, 'expected_production_source');
  const summary = expirySummary(await get('/api/session/status'));
  const safety = await get('/api/safety');
  assert.equal((await get('/api/health')).version, expected, 'production_source_changed');
  write('AUTOBOTT_EXPIRY_COVERAGE ' + JSON.stringify({observedAt: new Date().toISOString(),
    expectedSource: expected, entryDteWindows: health.entry_dte_windows, ...summary,
    safety: {paperOnly: safety.paper_only, liveTradingEnabled: safety.live_trading_enabled},
    note: 'Observed contract counts are not eligible-trade or profitability counts.'}));
  return true;
}
await runExpiryCoverage();
