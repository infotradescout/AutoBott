/** Explicit opt-in process proof and protected GET-only rollout verification. */
import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';

if (process.env.AUTOBOTT_VERIFY_PREMIUM_ROLLOUT === 'true') {
  const env = process.env;
  const token = env.AUTOBOTT_VERIFY_TOKEN;
  const expected = env.AUTOBOTT_VERIFY_EXPECTED_SHA;
  const stage = env.AUTOBOTT_VERIFY_PREMIUM_STAGE || 'process_only';
  delete env.AUTOBOTT_VERIFY_TOKEN;
  delete env.AUTOBOTT_VERIFY_PREMIUM_ROLLOUT;
  delete env.AUTOBOTT_VERIFY_EXPIRY_COVERAGE;
  delete env.AUTOBOTT_VERIFY_READONLY_PREFLIGHT;
  delete env.NODE_OPTIONS;
  assert(['process_only', 'observe', 'active'].includes(stage));
  const childEnv = Object.fromEntries(Object.entries(env).filter(([key]) => ['PATH','HOME','SYSTEMROOT','TMPDIR','TEMP','TMP'].includes(key)));
  childEnv.PYTHONPATH = process.cwd() + '/src';
  const proof = spawnSync('python', ['scripts/validate_premium_processes.py'], {env:childEnv, encoding:'utf8', timeout:60000, maxBuffer:200000});
  assert.equal(proof.status, 0, 'premium_process_proof_failed:' + (proof.stderr || '').slice(-1000));
  const line = proof.stdout.split('\n').find(value => value.startsWith('AUTOBOTT_PREMIUM_PROCESS_PROOF '));
  assert(line, 'premium_process_receipt_missing');
  console.log(line);
  if (stage !== 'process_only') {
    assert(token && /^[a-f0-9]{40}$/.test(expected));
    const host = 'https://autobott-azl4.onrender.com';
    const paths = new Set(['/api/health','/api/session/status','/api/safety']);
    const get = async path => {
      assert(paths.has(path));
      const response = await fetch(host + path, {method:'GET',redirect:'error',
        headers:{Authorization:'Bearer '+token,Accept:'application/json'},signal:AbortSignal.timeout(45000)});
      assert.equal(response.status,200,'portfolio_rollout_http_status');
      return response.json();
    };
    assert.equal((await get('/api/health')).version,expected,'production_source_mismatch');
    const session = await get('/api/session/status');
    const safety = await get('/api/safety');
    const raw = session.portfolio_capacity;
    assert(raw?.ok === true,'verified_portfolio_capacity_unavailable');
    assert.equal(raw.allocation_enabled,stage === 'active','portfolio_activation_state_mismatch');
    assert.equal(raw.premium_budget_dollars,6000);
    assert.equal(raw.per_leg_limit_dollars,1000);
    assert.equal(raw.daily_loss_entry_guard_dollars,750);
    assert.equal(raw.new_pairs_per_cycle,3);
    assert.equal(raw.configured_position_limit,stage === 'active' ? 60 : 6);
    assert.equal(safety.paper_only,true);
    assert.equal(safety.live_trading_enabled,false);
    assert.equal((await get('/api/health')).version,expected,'production_source_changed');
    const keys = ['ok','observed_at','allocation_enabled','premium_budget_dollars','held_premium_dollars',
      'working_buy_dollars','uncertain_submission_dollars','uncertain_reservations','budget_remaining_dollars',
      'broker_available_cash_dollars','committed_option_symbols','configured_position_limit','per_leg_limit_dollars',
      'daily_loss_entry_guard_dollars','new_pairs_per_cycle','risk_basis','fees_included','future_loss_guaranteed','cached'];
    const cycle = (session.state?.last_result?.cycle_results || []).at(-1) || {};
    console.log('AUTOBOTT_PREMIUM_ROLLOUT ' + JSON.stringify({observedAt:new Date().toISOString(),
      expectedSource:expected,stage,capacity:Object.fromEntries(keys.filter(key=>key in raw).map(key=>[key,raw[key]])),
      session:{threadAlive:session.thread_alive,cyclesCompleted:session.state?.cycles_completed,
        lastCycleAt:session.state?.last_cycle_at,ordersInLastCycle:(cycle.orders_submitted || []).length},
      safety:{paperOnly:safety.paper_only,liveTradingEnabled:safety.live_trading_enabled,
        executionEnabled:safety.execution_enabled,killSwitchEnabled:safety.kill_switch_enabled},
      acceptance:'configuration_and_read_only_capacity_not_profitable_trade_evidence'}));
  }
}
