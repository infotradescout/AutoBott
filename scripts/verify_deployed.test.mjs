import test from 'node:test';
import assert from 'node:assert/strict';
import { inventory, verify } from './verify_deployed.mjs';
const token = 'synthetic-token-that-is-at-least-32-characters';
const expected = 'a'.repeat(40);
function mock({ version = expected, paper = true, authorize = true, quantity = 1 } = {}) {
  const calls = [];
  const request = async (url, options) => {
    assert.equal(options.method, 'GET'); assert.equal(options.redirect, 'error');
    assert.equal(new URL(url).origin, 'https://autobott-azl4.onrender.com');
    const path = new URL(url).pathname; calls.push(path);
    const protectedPath = !['/', '/api/health'].includes(path);
    if (protectedPath && options.headers.Authorization !== 'Bearer ' + token) return { status: authorize ? 401 : 200, text: async () => JSON.stringify({ ok: false, error: 'unauthorized' }) };
    const values = {
      '/': '<html>Core + Runner Trades<input id="access-token"></html>',
      '/api/health': { ok: true, version, policy_version: 'hosted-core-runner-v2' },
      '/api/safety': { alpaca_env: paper ? 'paper' : 'live', paper_only: paper, live_trading_enabled: !paper },
      '/api/account/positions': { ok: true, positions: [{ symbol: 'TEST', qty: '1' }] },
      '/api/positions/open': { ok: true, positions: [{ option_symbol: 'TEST', quantity }] },
      '/api/v2/pairs': { ok: true, pair_count: 1, open_pair_count: 1, retained_runner_count: 0 },
      '/api/session/status': { ok: true, state: { running: true } },
      '/api/trading/timeline': { outcome_learning: { accounting_complete: false, token: 'DO-NOT-LEAK' }, warnings: [] },
    };
    return { status: 200, text: async () => typeof values[path] === 'string' ? values[path] : JSON.stringify(values[path]) };
  };
  return { request, calls };
}
test('aggregates quantities and detects duplicate stored symbols', () => assert.deepEqual(inventory([{ symbol: 'TEST', qty: 1 }, { option_symbol: 'TEST', quantity: 2 }]), { quantities: { TEST: 3 }, duplicateSymbols: ['TEST'] }));
test('stored option identity takes precedence over its underlying label', () => assert.deepEqual(inventory([{ symbol: 'NKE', option_symbol: 'NKE260925P00035500', quantity: 1 }, { symbol: 'NKE', option_symbol: 'NKE260925P00037000', quantity: 1 }]), { quantities: { NKE260925P00035500: 1, NKE260925P00037000: 1 }, duplicateSymbols: [] }));
test('rejects malformed quantities', () => assert.throws(() => inventory([{ symbol: 'TEST', qty: 'NaN' }])));
test('rejects missing private token without a request', async () => { const m = mock(); await assert.rejects(verify({ token: '', expected, request: m.request })); assert.equal(m.calls.length, 0); });
test('rejects old deployed revision before protected reads', async () => { const m = mock({ version: 'b'.repeat(40) }); await assert.rejects(verify({ token, expected, request: m.request })); assert.deepEqual(m.calls, ['/api/health']); });
test('rejects publicly accessible protected routes', async () => await assert.rejects(verify({ token, expected, request: mock({ authorize: false }).request })));
test('rejects a live-money runtime', async () => await assert.rejects(verify({ token, expected, request: mock({ paper: false }).request })));
test('keeps incomplete accounting explicit and excludes secrets', async () => { const r = await verify({ token, expected, request: mock().request }); assert.equal(r.releaseVerified, true); assert.equal(r.accounting.accounting_complete, false); assert.equal(r.brokerWrites, 0); assert(!JSON.stringify(r).includes('DO-NOT-LEAK')); });
test('reports quantity discrepancies instead of claiming reconciliation', async () => { const r = await verify({ token, expected, request: mock({ quantity: 2 }).request }); assert.deepEqual(r.inventory.quantityDifferences, [{ symbol: 'TEST', broker: 1, stored: 2 }]); });
