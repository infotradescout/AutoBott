const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const {test} = require('node:test');
const source = fs.readFileSync(path.join(__dirname,'../src/autobott_v2/dashboard_app_v2.py'),'utf8');
const script = source.split('<script>')[1].split('</script>')[0].replace('controls(false);refreshAll();setInterval(refreshAll,15000);','');
const now = Date.parse('2026-09-21T15:40:30Z');
function fixture(){
  const safety={execution_enabled:true,order_placement_enabled:true,kill_switch_enabled:false};
  const health={ok:true};
  const session={thread_alive:true,config:{interval_seconds:60},state:{last_entry_status:{
    schema:'entry_status.v1',observed_at:'2026-09-21T15:40:01Z',cycle_finished_at:'2026-09-21T15:40:00Z',
    status:'BLOCKED',reason:"New entries are blocked: AutoBott's trade records do not reconcile with the broker.",
    candidates_count:9,entry_submissions_count:0,rejections_count:9}}};
  const elements=new Map(); const get=id=>{if(!elements.has(id))elements.set(id,{textContent:'',innerHTML:'',className:'',hidden:false,style:{},classList:{add(){},remove(){}}}); return elements.get(id);};
  const context=vm.createContext({document:{getElementById:get,querySelectorAll:()=>[]},sessionStorage:{getItem:()=>''},setInterval(){},confirm:()=>false});
  vm.runInContext(script,context);
  context.safety=safety; context.session=session; context.health=health; context.now=now;
  return {safety,session,health,get,context,view:()=>vm.runInContext('entryRuntimeView(safety,session,health,now)',context)};
}
test('accounting block cannot appear as green ARMED',()=>{const f=fixture();const r=f.view();assert.equal(r.label,'BLOCKED');assert.equal(r.tone,'badText');assert.match(r.reason,/do not reconcile/);});
test('kill switch overrides old submissions',()=>{const f=fixture();f.safety.kill_switch_enabled=true;assert.equal(f.view().label,'KILLED');});
test('pause retains precedence',()=>{const f=fixture();f.safety.execution_enabled=false;assert.equal(f.view().label,'PAUSED');});
test('deployment-owned order disable retains precedence',()=>{const f=fixture();f.safety.order_placement_enabled=false;assert.match(f.view().reason,/safety configuration/);});
test('stalled supervisor is not green',()=>{const f=fixture();f.health.session_supervisor={stalled:true};assert.equal(f.view().label,'STALLED');});
test('stopped supervisor is not scanning',()=>{const f=fixture();f.session.thread_alive=false;assert.equal(f.view().label,'STOPPED');});
test('no completed scan remains waiting',()=>{const f=fixture();delete f.session.state.last_entry_status;assert.equal(f.view().label,'WAITING');});
test('stale cycle cannot impersonate current entry status',()=>{const f=fixture();f.session.state.last_entry_status.cycle_finished_at='2026-09-20T15:40:00Z';assert.equal(f.view().label,'STALE');});
test('future evidence cannot impersonate current entry status',()=>{const f=fixture();f.session.state.last_entry_status.observed_at='2026-09-22T15:40:00Z';assert.equal(f.view().label,'STALE');});
test('unparseable clock remains unknown',()=>{const f=fixture();f.session.state.last_entry_status.cycle_finished_at='bad';assert.equal(f.view().label,'UNKNOWN');});
test('no scanner candidates is not described as no market opportunities',()=>{const f=fixture();Object.assign(f.session.state.last_entry_status,{status:'NO_CANDIDATES',reason:'No candidate passed scanner rules; this does not mean there were no market opportunities.'});assert.equal(f.view().label,'NO CANDIDATES');assert.notEqual(f.view().tone,'goodText');});
test('submission requires a positive recorded submission count',()=>{const f=fixture();f.session.state.last_entry_status.status='SUBMITTED';assert.equal(f.view().label,'UNKNOWN');f.session.state.last_entry_status.entry_submissions_count=1;assert.equal(f.view().label,'ENTRY SUBMITTED');});
test('unknown schema cannot produce a green status',()=>{const f=fixture();f.session.state.last_entry_status.schema='future';assert.equal(f.view().label,'WAITING');});
test('locking removes last entry explanation',()=>{const f=fixture();f.get('entry-notice').textContent='old private status';vm.runInContext("clearAccount('locked')",f.context);assert.equal(f.get('entry-notice').textContent,'');});

test('naive timestamps cannot become current evidence',()=>{const f=fixture();f.session.state.last_entry_status.cycle_finished_at='2026-09-21T15:40:00';assert.equal(f.view().label,'UNKNOWN');});
test('reported stopped state overrides an alive thread',()=>{const f=fixture();f.session.state.running=false;assert.equal(f.view().label,'STOPPED');});
