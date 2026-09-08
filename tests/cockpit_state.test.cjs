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
  const state = { execution: true, orderPlacement: true };
  const payload = route => {
    if (route === '/api/v2/pairs') return { ok: true, account: { equity: 100 }, pairs: [], standalone_positions: [] };
    if (route === '/api/safety') return { execution_enabled: state.execution, kill_switch_enabled: false, order_placement_enabled: state.orderPlacement && state.execution };
    if (route === '/api/session/status') return { ok: true, thread_alive: true };
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
