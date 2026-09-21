/** Explicit opt-in, authenticated GET-only preflight; not a trading cycle. */
import { verifyRuntimeAccounting } from './verify_runtime_reconciliation.mjs';

export async function runReadonlyPreflight({ env = process.env, request = fetch, write = console.log } = {}) {
  if (env.AUTOBOTT_VERIFY_READONLY_PREFLIGHT !== 'true') return false;
  const token = env.AUTOBOTT_VERIFY_TOKEN;
  const expected = env.AUTOBOTT_VERIFY_EXPECTED_SHA;
  // Never propagate the private token or this one-shot hook to child tests.
  delete env.AUTOBOTT_VERIFY_TOKEN;
  delete env.AUTOBOTT_VERIFY_READONLY_PREFLIGHT;
  delete env.NODE_OPTIONS;
  const report = await verifyRuntimeAccounting({ token, expected, request });
  write('AUTOBOTT_RUNTIME_ACCOUNTING ' + JSON.stringify({
    observedAt: new Date().toISOString(), expectedSource: expected, ...report,
  }));
  return true;
}

// This opt-in integration preflight is separate from the subsequent cockpit
// tests. The Python validator still strips credentials and denies networking.
await runReadonlyPreflight();
