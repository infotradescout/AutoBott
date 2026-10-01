/** Explicit restoration acceptance; never imported by the trading server. */
import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';

if (process.env.AUTOBOTT_VERIFY_ENTRY_RESTORE === 'true') {
  const withPublic = process.env.AUTOBOTT_ENTRY_RESTORE_PUBLIC_PROBE === 'true';
  const env = process.env;
  delete env.NODE_OPTIONS;
  delete env.AUTOBOTT_VERIFY_ENTRY_RESTORE;
  delete env.AUTOBOTT_VERIFY_TOKEN;
  const childEnv = Object.fromEntries(Object.entries(env).filter(([name]) =>
    ['PATH','HOME','SYSTEMROOT','TMPDIR','TEMP','TMP'].includes(name)));
  childEnv.PYTHONPATH = process.cwd() + '/src';
  const args = ['scripts/verify_entry_restore.py', ...(withPublic ? ['--public'] : [])];
  const result = spawnSync('python', args, {env:childEnv,encoding:'utf8',timeout:90000,maxBuffer:100000});
  if (result.stdout) process.stdout.write(result.stdout);
  assert.equal(result.status,0,'entry_restore_acceptance_failed:'+(result.stderr || '').slice(-1200));
  assert(result.stdout.includes('AUTOBOTT_ENTRY_RESTORE_ACCEPTANCE '),'entry_restore_receipt_missing');
}
