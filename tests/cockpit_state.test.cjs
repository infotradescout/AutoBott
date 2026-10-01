// Exercise the actual shipped cockpit script without a browser or broker.
// Run with: node --test tests/cockpit_state.test.cjs
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { test } = require('node:test');

const source = fs.readFileSync(path.join(__dirname, '../src/autobott_v2/dashboard_app_v2.py'), 'utf8');
const script = source.split('<script>')[1].split('</script>')[0]
  .replace('controls(false);refreshAll();setInterval(refreshAll,15000);', '');
const tick = () => new Promise(resolve => setImmediate(resolve));
const deferred = () => {
  let resolve;
  const promise = new Promise(done => { resolve = done; });
  return { promise, resolve };
};

function cockpit() {
  const elements = new Map();
  const get = id => {
    if (!elements.has(id)) elements.set(id, {
      textContent: '', innerHTML: '', className: '', hidden: false, value: '',
      style: {}, classList: { add() {}, remove() {} },
    });
    return elements.get(id);
  };
  const buttons = ['Arm Paper', 'Pause', 'Kill Switch', 'Refresh']
    .map(textContent => ({ textContent, disabled: false }));
  const storage = new Map([['dashboardToken', 'synthetic-token']]);
  const context = vm.createContext({
    document: { getElementById: get, querySelectorAll: () => buttons },
    sessionStorage: {
      getItem: key => storage.get(key),
      setItem: (key, value) => storage.set(key, value),
      removeItem: key => storage.delete(key),
    },
    confirm: () => true, setInterval() {},
  });
  const state = { execution: true, orderPlacement: true, killed: false, session: { ok: true, thread_alive: true } };
  const payload = route => {
    if (route === '/api/v2/pairs') return { ok: true, account: { equity: 100 }, pairs: [], standalone_positions: [] };
    if (route === '/api/safety') return { execution_enabled: state.execution, kill_switch_enabled: state.killed, order_placement_enabled: state.orderPlacement && state.execution };
    if (route === '/api/session/status') return state.session;
    if (route === '/api/decisions/latest') return { ok: true, decisions: [] };
    return { ok: true };
  };
  const response = (data, status = 200) => ({ ok: status >= 200 && status < 300, status, json: async () => data });
  context.fetch = async route => response(payload(route));
  vm.runInContext(script, context);
  return { context, get, state, storage, payload, response,
    run: code => vm.runInContext(code, context),
    controlsDisabled: () => buttons.slice(0, 3).every(button => button.disabled),
  };
}

test('a pending command blocks refresh repaint and conflicting commands until confirmed', async () => {
  const e = cockpit();
  await e.run('refreshAll()');
  const pending = deferred();
  const posts = [];
  let reads = 0;
  e.context.fetch = async (route, options) => {
    if (options.method === 'POST') {
      posts.push(route);
      await pending.promise;
      e.state.execution = false;
      return e.response({ ok: true });
    }
    reads++;
    return e.response(e.payload(route));
  };
  const pause = e.run('pauseTrading()');
  await e.run('refreshAll()');
  assert.equal(e.controlsDisabled(), true);
  await e.run('arm()'); // The mutation guard also protects non-click callers.
  assert.deepEqual(posts, ['/api/runtime/disable-execution']);
  assert.equal(reads, 0);
  pending.resolve();
  await pause;
  assert.equal(reads, 5);
  assert.equal(e.get('runtime').textContent, 'PAUSED');
  assert.equal(e.controlsDisabled(), false);
});

test('a command invalidates an older refresh and waits for a fresh post-command snapshot', async () => {
  const e = cockpit();
  await e.run('refreshAll()');
  const stale = [];
  let reads = 0;
  e.context.fetch = (route, options) => {
    if (options.method === 'POST') {
      e.state.execution = false;
      return Promise.resolve(e.response({ ok: true }));
    }
    reads++;
    if (reads <= 5) {
      const beforePause = e.payload(route);
      return new Promise(resolve => stale.push(() => resolve(e.response(beforePause))));
    }
    return Promise.resolve(e.response(e.payload(route)));
  };
  const previousRefresh = e.run('refreshAll()');
  const pause = e.run('pauseTrading()');
  await tick();
  assert.equal(e.controlsDisabled(), true);
  stale.forEach(resolve => resolve());
  await Promise.all([previousRefresh, pause]);
  assert.equal(reads, 10);
  assert.equal(e.get('runtime').textContent, 'PAUSED');
  assert.equal(e.get('notice').textContent, 'Paper execution paused.');
});

test('locking during a refresh prevents the old response from restoring account data', async () => {
  const e = cockpit();
  await e.run('refreshAll()');
  const pending = deferred();
  e.context.fetch = async route => { await pending.promise; return e.response(e.payload(route)); };
  const refresh = e.run('refreshAll()');
  e.run('lock()');
  pending.resolve();
  await refresh;
  assert.equal(e.storage.has('dashboardToken'), false);
  assert.equal(e.get('equity').textContent, '—');
  assert.equal(e.get('auth-status').textContent, 'Locked');
  assert.equal(e.controlsDisabled(), true);
});

test('locking while a command is pending preserves the locked state and notice', async () => {
  const e = cockpit();
  await e.run('refreshAll()');
  const pending = deferred();
  e.context.fetch = async () => { await pending.promise; return e.response({ ok: true }); };
  const pause = e.run('pauseTrading()');
  e.run('lock()');
  pending.resolve();
  await pause;
  assert.equal(e.get('notice').textContent, 'Dashboard locked.');
  assert.equal(e.get('equity').textContent, '—');
  assert.equal(e.controlsDisabled(), true);
});

test('watchdog 503 preserves authenticated account data and stop controls', async () => {
  const e = cockpit();
  e.context.fetch = async route => route === '/api/health'
    ? e.response({ ok: false, session_supervisor: { stalled: true } }, 503)
    : e.response(e.payload(route));
  await e.run('refreshAll()');
  assert.equal(e.get('equity').textContent, '$100.00');
  assert.equal(e.get('session-chip').textContent, 'SESSION STALLED');
  assert.match(e.get('state').innerHTML, /Stopped unexpectedly/);
  assert.equal(e.controlsDisabled(), false);
});

for (const [route, status, data] of [
  ['/api/v2/pairs', 401, { ok: false, error: 'unauthorized' }],
  ['/api/v2/pairs', 503, { ok: false, error: 'account_unavailable' }],
  ['/api/health', 503, { ok: false, error: 'unknown_failure' }],
]) test(`${route} ${status} without a recognized watchdog failure clears stale data`, async () => {
  const e = cockpit();
  await e.run('refreshAll()');
  e.context.fetch = async target => target === route
    ? e.response(data, status) : e.response(e.payload(target));
  await e.run('refreshAll()');
  assert.equal(e.get('equity').textContent, '—');
  assert.equal(e.controlsDisabled(), true);
});

test('an execution request blocked by safety configuration is not displayed as armed', async () => {
  const e = cockpit();
  e.state.orderPlacement = false;
  await e.run('refreshAll()');
  assert.equal(e.get('runtime').textContent, 'BLOCKED');
  assert.match(e.get('state').innerHTML, /Blocked by safety configuration/);
});

test('missing broker marks stay unavailable instead of becoming a zero-dollar gain', () => {
  const e = cockpit();
  for (const value of [undefined, null, '', '   ', 'unavailable']) {
    e.context.position = {
      symbol: 'SPY261016C00600000', qty: '1',
      unrealized_pl: value, avg_entry_price: value, current_price: value,
      unrealized_plpc: value,
    };
    const html = e.run("legCard(position, 'CORE')");
    assert.doesNotMatch(html, /\$0\.00|0\.0%|goodText|badText/);
    assert.match(html, /SPY261016C00600000/);
    assert.equal((html.match(/—/g) || []).length, 4);
  }
});

test('available broker marks preserve genuine zero, profit, and loss', () => {
  const e = cockpit();
  for (const [value, expected, tone] of [
    ['0', '$0.00', 'goodText'],
    ['12.5', '$12.50', 'goodText'],
    ['-4.25', '$-4.25', 'badText'],
  ]) {
    e.context.position = {
      symbol: 'SPY261016C00600000', qty: '1', unrealized_pl: value,
      avg_entry_price: '0.25', current_price: '0', unrealized_plpc: '0',
    };
    const html = e.run("legCard(position, 'RUNNER')");
    assert.ok(html.includes(`class="${tone}">${expected}</span>`));
    assert.match(html, /\$0\.25/);
    assert.match(html, /\$0\.00/);
    assert.match(html, /0\.0%/);
  }
});

function observation(e, status, issues = [], extra = {}) {
  e.state.session = { ok: true, thread_alive: true, position_monitor_thread_alive: true, state: {
    last_monitor_at: '2026-10-01T12:00:00+00:00',
    last_monitor_result: { ok: status !== 'attention_required', exit_protection: {
      status, message: 'RAW BROKER SECRET', issues,
    } }, last_monitor_error: status === 'attention_required' ? 'Exit protection needs attention.' : null, ...extra,
  } };
}

module.exports = { cockpit };

for (const status of ['failed', 'rejected', 'canceled', 'draft', 'approved', 'uncertain', 'blocked']) {
  test(`exit ${status} stays visible with running session and readable account`, async () => {
    const e = cockpit();
    observation(e, 'attention_required', [{ symbol: 'SPY', reason: 'loss_exit', status, message: 'RAW BROKER SECRET' }]);
    await e.run('refreshAll()');
    assert.equal(e.get('equity').textContent, '$100.00');
    assert.equal(e.get('session-chip').textContent, 'SESSION RUNNING');
    assert.equal(e.get('runtime').textContent, 'ARMED');
    assert.equal(e.controlsDisabled(), false);
    assert.match(e.get('state').innerHTML, /Exit protection.*Attention required/);
    assert.match(e.get('state').innerHTML, /SPY \/ loss_exit/);
    assert.doesNotMatch(e.get('state').innerHTML, /RAW BROKER SECRET/);
  });
}

for (const status of ['pending', 'partially_filled']) {
  test(`exit ${status} awaits fill without claiming failure or closure`, async () => {
    const e = cockpit();
    observation(e, 'awaiting_fill', [{ symbol: 'SPY', reason: 'existing_profit_exit', status }]);
    await e.run('refreshAll()');
    assert.match(e.get('state').innerHTML, /Awaiting fill/);
    assert.match(e.get('state').innerHTML, /existing_profit_exit/);
    assert.doesNotMatch(e.get('state').innerHTML, /Attention required|Closed|confirmed closure|Failed/);
    assert.equal(e.get('equity').textContent, '$100.00');
  });
}

test('broker reported fill awaits position reconciliation', async () => {
  const e = cockpit();
  observation(e, 'awaiting_reconciliation', [{ symbol: 'SPY', reason: 'loss_exit', status: 'reported_fill' }]);
  await e.run('refreshAll()');
  assert.match(e.get('state').innerHTML, /Awaiting position reconciliation/);
  assert.match(e.get('state').innerHTML, /position closure is not yet confirmed/);
  assert.match(e.get('state').innerHTML, /loss_exit: Broker reported fill; reconciliation pending/);
  assert.doesNotMatch(e.get('state').innerHTML, /Closed|Exit protection<\/span><strong>Monitoring/);
});

test('missing, empty, disabled, and unknown observations cannot imply monitoring', async () => {
  const e = cockpit();
  for (const state of [{}, { last_monitor_result: {} },
    { last_monitor_at: 'invalid', last_monitor_result: { exit_protection: { status: 'monitoring' } } },
    { last_monitor_at: '2026-10-01T12:00:00Z', last_monitor_result: { exit_protection: { status: 'invented' } } },
    { last_monitor_at: '2026-10-01T12:00:00Z', last_monitor_result: { exit_protection: { status: 'disabled' } } }]) {
    e.state.session.state = state;
    await e.run('refreshAll()');
    assert.doesNotMatch(e.get('state').innerHTML, /Exit protection<\/span><strong>Monitoring/);
    assert.match(e.get('state').innerHTML, /Unavailable|Disabled/);
    assert.equal(e.get('equity').textContent, '$100.00');
  }
});

test('inactive monitor and deliberate controls remain separate from historical exit outcomes', async () => {
  const e = cockpit();
  observation(e, 'monitoring');
  e.state.session.position_monitor_thread_alive = false;
  e.state.execution = false;
  await e.run('refreshAll()');
  assert.match(e.get('state').innerHTML, /Exit protection<\/span><strong>Inactive/);
  assert.match(e.get('state').innerHTML, /recorded exit check does not establish current protection/);
  assert.equal(e.get('runtime').textContent, 'PAUSED');
  observation(e, 'attention_required', [{ symbol: 'SPY', reason: 'loss_exit', status: 'rejected' }]);
  e.state.session.position_monitor_thread_alive = false;
  await e.run('refreshAll()');
  assert.match(e.get('state').innerHTML, /Attention required/);
  assert.match(e.get('state').innerHTML, /Inactive; latest check is historical/);
  assert.equal(e.get('runtime').textContent, 'PAUSED');
  e.state.killed = true;
  await e.run('refreshAll()');
  assert.equal(e.get('runtime').textContent, 'KILLED');
  assert.match(e.get('state').innerHTML, /Kill switch<\/span><strong>Active/);
  assert.match(e.get('state').innerHTML, /SPY \/ loss_exit: Rejected/);
});

test('exit monitor liveness is independent of the entry session', async () => {
  const e = cockpit();
  observation(e, 'monitoring');
  e.state.session.thread_alive = false;
  await e.run('refreshAll()');
  assert.equal(e.get('session-chip').textContent, 'SESSION STOPPED');
  assert.match(e.get('state').innerHTML, /Exit protection<\/span><strong>Monitoring/);
  assert.match(e.get('state').innerHTML, /Exit monitor<\/span><strong>Running/);
  e.state.session.thread_alive = true;
  e.state.session.position_monitor_thread_alive = false;
  await e.run('refreshAll()');
  assert.equal(e.get('session-chip').textContent, 'SESSION RUNNING');
  assert.match(e.get('state').innerHTML, /Exit protection<\/span><strong>Inactive/);
});

test('monitor exception is unavailable and a genuine newer observation clears it', async () => {
  const e = cockpit();
  observation(e, 'monitoring', [], { last_monitor_error: 'token / broker / order-id secret' });
  await e.run('refreshAll()');
  assert.match(e.get('state').innerHTML, /Exit protection<\/span><strong>Unavailable/);
  assert.doesNotMatch(e.get('state').innerHTML, /token \/ broker|order-id secret/);
  assert.equal(e.get('session-chip').textContent, 'SESSION RUNNING');
  assert.equal(e.get('equity').textContent, '$100.00');
  observation(e, 'attention_required');
  await e.run('refreshAll()');
  assert.match(e.get('state').innerHTML, /Attention required/);
  observation(e, 'monitoring');
  await e.run('refreshAll()');
  assert.match(e.get('state').innerHTML, /Exit protection<\/span><strong>Monitoring/);
  assert.doesNotMatch(e.get('state').innerHTML, /Attention required/);
});

test('exit details are escaped, bounded, and cleared on lock or unauthorized refresh', async () => {
  const e = cockpit();
  observation(e, 'attention_required', Array.from({ length: 10 }, () => ({
    symbol: '<img src=x>', reason: '<script>&loss_exit', status: 'rejected', message: 'secret',
  })));
  await e.run('refreshAll()');
  const html = e.get('state').innerHTML;
  assert.match(html, /&lt;img src=x&gt; \/ &lt;script&gt;&amp;loss_exit/);
  assert.doesNotMatch(html, /<img|<script|secret/);
  assert.equal((html.match(/Exit detail<\/span>/g) || []).length, 8);
  assert.match(html, /Additional issues/);
  e.run('lock()');
  assert.equal(e.get('state').innerHTML, '');
  e.storage.set('dashboardToken', 'synthetic-token');
  await e.run('refreshAll()');
  e.context.fetch = async () => e.response({ ok: false }, 401);
  await e.run('refreshAll()');
  assert.equal(e.get('state').innerHTML, '');
  assert.notEqual(e.get('equity').textContent, '$100.00');
});
