import assert from 'node:assert/strict';
import test from 'node:test';
import {runReadonlyPreflight} from './verify_runtime_preflight.mjs';
const expected = '5e0e8c3823d311fa82fed648844668c13cc54579';
const env = () => ({AUTOBOTT_VERIFY_READONLY_PREFLIGHT:'true', AUTOBOTT_VERIFY_TOKEN:'private-fixture-token', AUTOBOTT_VERIFY_EXPECTED_SHA:expected,NODE_OPTIONS:'--import=hook'});
test('inert unless explicitly opted in', async () => {
 let calls=0; assert.equal(await runReadonlyPreflight({env:{},request:()=>{calls++;},write:()=>{calls++;}}),false); assert.equal(calls,0);
});
test('only exact-host GETs; private token removed; only sanitized report printed', async () => {
 const e=env(), calls=[], logs=[];
 const request=async (url,opts)=>{
  assert.equal(opts.method,'GET');assert.equal(opts.redirect,'error');assert.equal(opts.headers.Authorization,'Bearer private-fixture-token');
  assert.equal(e.AUTOBOTT_VERIFY_TOKEN,undefined);calls.push(url);
  const payload=url.endsWith('/api/health')?{version:expected}:{state:{last_result:{cycle_results:[{started_at:'2026-09-21T16:00:00Z',execution_outcomes:[{disposition:'trade_outcome_learning_summary',ok:false,error:'outcome_journal_reconciliation_required',reconciliation:{unresolved:[{reason:'historical_account_ownership_unverified',account_id:'PRIVATE-ACCOUNT',token:'PRIVATE'}]}}],orders_submitted:[],execution_rejected_count_by_reason:{daily_pnl_unavailable:18}}]}}};
  return {status:200,json:async()=>payload};
 };
 assert.equal(await runReadonlyPreflight({env:e,request,write:s=>logs.push(s)}),true);
 assert.deepEqual(calls,['https://autobott-azl4.onrender.com/api/health','https://autobott-azl4.onrender.com/api/session/status','https://autobott-azl4.onrender.com/api/health']);
 assert.equal(logs.length,1); assert(!logs[0].includes('private-fixture-token'));assert(!logs[0].includes('PRIVATE-ACCOUNT'));assert(!logs[0].includes('"token"'));
 assert.equal(e.NODE_OPTIONS,undefined);assert.equal(e.AUTOBOTT_VERIFY_READONLY_PREFLIGHT,undefined);
});
test('wrong source fails before session read, with no report',async()=>{
 const e=env();let calls=0,logs=0;
 await assert.rejects(runReadonlyPreflight({env:e,request:async()=>{calls++;return{status:200,json:async()=>({version:'wrong'})};},write:()=>logs++}));
 assert.equal(calls,1);assert.equal(logs,0);assert.equal(e.AUTOBOTT_VERIFY_TOKEN,undefined);
});
test('unauthorized fails without report or token retention',async()=>{
 const e=env();let logs=0;
 await assert.rejects(runReadonlyPreflight({env:e,request:async()=>({status:401}),write:()=>logs++}));
 assert.equal(logs,0);assert.equal(e.AUTOBOTT_VERIFY_TOKEN,undefined);
});
test('missing token does not make a network attempt',async()=>{
 const e=env();delete e.AUTOBOTT_VERIFY_TOKEN;let calls=0;
 await assert.rejects(runReadonlyPreflight({env:e,request:async()=>{calls++;}}));assert.equal(calls,0);
});
