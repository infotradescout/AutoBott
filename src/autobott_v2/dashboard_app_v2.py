from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from typing import Any, Callable
from wsgiref.simple_server import make_server

from . import dashboard_app as legacy
from .hosted_policy import HOSTED_POLICY_VERSION
from .position_monitor import _load_pair_states


JsonDict = dict[str, Any]


def app(environ: dict[str, Any], start_response: Callable[..., Any]) -> list[bytes]:
    method = str(environ.get("REQUEST_METHOD") or "GET").upper()
    path = str(environ.get("PATH_INFO") or "/")
    if path == "/":
        payload = _cockpit_html().encode("utf-8")
        start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
        return [payload]
    if path == "/api/v2/pairs" and method == "GET":
        try:
            legacy._require_auth(legacy._extract_headers(environ))
            payload = json.dumps(_pair_cockpit_payload(), allow_nan=False).encode("utf-8")
            status = "200 OK"
        except PermissionError:
            status = "401 Unauthorized"
            payload = b'{"ok":false,"error":"unauthorized"}'
        except Exception:
            status = "500 Internal Server Error"
            payload = b'{"ok":false,"error":"pair_data_unavailable"}'
        start_response(status, [("Content-Type", "application/json; charset=utf-8"), ("Cache-Control", "no-store")])
        return [payload]
    return legacy.app(environ, start_response)


def _pair_cockpit_payload() -> JsonDict:
    account_payload = legacy._account_positions_payload()
    if not account_payload.get("ok"):
        return account_payload
    positions = account_payload.get("positions") or []
    pair_states = _load_pair_states()
    groups: dict[str, list[dict[str, Any]]] = {}
    standalone: list[dict[str, Any]] = []
    for position in positions:
        group_id = str(position.get("trade_group_id") or "").strip()
        if group_id:
            groups.setdefault(group_id, []).append(position)
        else:
            standalone.append(position)

    pairs: list[JsonDict] = []
    for group_id, legs in groups.items():
        primary = next((leg for leg in legs if leg.get("leg_role") == "primary"), None)
        runner = next((leg for leg in legs if leg.get("leg_role") == "runner"), None)
        state = pair_states.get(group_id) or {}
        funding_verified = bool(state.get("funding_verified"))
        runner_funded = bool(state.get("runner_funded")) and funding_verified
        realized_pnl = _float(state.get("primary_realized_pnl")) if funding_verified else 0.0
        runner_entry_cost = _leg_entry_cost(runner)
        primary_pnl = realized_pnl + (_float(primary.get("unrealized_pl")) if primary else 0.0)
        funding_progress = 1.0 if runner_funded else (
            max(0.0, min(1.0, primary_pnl / runner_entry_cost)) if runner_entry_cost > 0 else 0.0
        )
        pair_pnl = round(sum(_float(leg.get("unrealized_pl")) for leg in legs) + realized_pnl, 2)
        status = "FUNDED RUNNER" if runner_funded else (
            "EXIT VERIFICATION PENDING" if state.get("funding_exit_blocked") else "CORE EXIT PENDING" if state.get("funding_exit_submitted") else (
                "RUNNER OPEN" if runner and not primary else "PAIR OPEN" if primary and runner else "CORE OPEN"
            )
        )
        underlying = next((str(leg.get("underlying") or "") for leg in legs if leg.get("underlying")), "")
        if not underlying:
            underlying = _underlying_from_option_symbol(str(legs[0].get("symbol") or "")) or "UNKNOWN"
        pairs.append(
            {
                "trade_group_id": group_id,
                "underlying": underlying,
                "status": status,
                "runner_funded": runner_funded,
                "funding_verified": funding_verified,
                "funding_unknown": runner is not None and primary is None and not funding_verified,
                "pnl_basis": "open_leg_marks_only" if runner and not primary and not funding_verified else "open_marks_and_verified_core_fills",
                "funding_exit_submitted": bool(state.get("funding_exit_submitted")),
                "funding_progress": round(funding_progress, 4),
                "runner_entry_cost": round(runner_entry_cost, 2),
                "primary_pnl": round(primary_pnl, 2),
                "pair_pnl": pair_pnl,
                "primary": primary,
                "runner": runner,
            }
        )
    pairs.sort(key=lambda row: (not bool(row.get("runner_funded")), -abs(_float(row.get("pair_pnl")))))
    return {
        "ok": True,
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "policy_version": HOSTED_POLICY_VERSION,
        "account": account_payload.get("account") or {},
        "pairs": pairs,
        "standalone_positions": standalone,
        "pair_count": len(pairs),
        "open_pair_count": sum(bool(pair["primary"] and pair["runner"]) for pair in pairs),
        "retained_runner_count": sum(bool(pair["runner"] and not pair["primary"]) for pair in pairs),
    }


def _leg_entry_cost(leg: dict[str, Any] | None) -> float:
    if not leg:
        return 0.0
    return _float(leg.get("avg_entry_price")) * _float(leg.get("qty")) * 100.0


def _float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _underlying_from_option_symbol(symbol: str) -> str | None:
    stripped = symbol.strip().upper()
    for index, char in enumerate(stripped):
        if char in {"C", "P"} and index >= 6:
            expiry = stripped[index - 6 : index]
            suffix = stripped[index + 1 :]
            if expiry.isdigit() and suffix.isdigit():
                return stripped[: index - 6]
    return None


def _cockpit_html() -> str:
    return r'''<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>AutoBott Paper Trading</title>
<style>
:root{color-scheme:dark;--bg:#090b0f;--panel:#11151c;--panel2:#171c25;--line:#252c38;--text:#f3f5f7;--muted:#8c97a8;--good:#43d17c;--warn:#f0b84b;--bad:#ef6262;--accent:#7aa2ff}
*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at 20% -10%,#172037 0,#090b0f 38%);font-family:Inter,system-ui,-apple-system,Segoe UI,sans-serif;color:var(--text)}button{font:inherit}.shell{max-width:1440px;margin:auto;padding:22px}.top{display:flex;align-items:center;justify-content:space-between;gap:18px;padding:16px 18px;border:1px solid var(--line);background:rgba(17,21,28,.92);border-radius:16px;position:sticky;top:12px;z-index:5;backdrop-filter:blur(16px)}.brand{display:flex;align-items:center;gap:12px}.mark{width:38px;height:38px;border-radius:11px;display:grid;place-items:center;background:linear-gradient(145deg,#253354,#141a26);font-weight:900}.brand h1{font-size:17px;margin:0}.sub{font-size:12px;color:var(--muted);margin-top:2px}.chips{display:flex;gap:8px;flex-wrap:wrap}.chip{font-size:11px;font-weight:800;letter-spacing:.04em;padding:7px 9px;border-radius:999px;border:1px solid var(--line);background:#0d1118}.chip.good{color:var(--good);border-color:#254f36}.chip.warn{color:var(--warn);border-color:#55401d}.chip.bad{color:var(--bad);border-color:#5b2929}.actions{display:flex;gap:8px;flex-wrap:wrap}.btn{border:1px solid var(--line);border-radius:10px;padding:9px 12px;background:#171d27;color:var(--text);cursor:pointer;font-weight:700}.btn:hover{background:#202836}.btn.primary{background:#e8eefc;color:#10141b;border-color:#e8eefc}.btn.danger{color:#ffc7c7;border-color:#5b2929;background:#221315}.hero{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-top:16px}.metric,.panel{border:1px solid var(--line);background:rgba(17,21,28,.94);border-radius:15px}.metric{padding:17px}.label{text-transform:uppercase;letter-spacing:.08em;font-size:10px;color:var(--muted);font-weight:800}.value{font-size:26px;font-weight:800;margin-top:7px}.delta{font-size:12px;margin-top:4px;color:var(--muted)}.section{margin-top:16px}.section-head{display:flex;justify-content:space-between;align-items:end;margin:0 2px 9px}.section-head h2{font-size:15px;margin:0}.section-head span{font-size:12px;color:var(--muted)}.pairs{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px}.pair{border:1px solid var(--line);background:linear-gradient(180deg,#131821,#0f131a);border-radius:16px;padding:16px}.pair-top{display:flex;justify-content:space-between;gap:12px}.ticker{font-size:22px;font-weight:850}.pair-id{font-size:10px;color:var(--muted);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:220px}.pnl{font-size:21px;font-weight:800;text-align:right}.goodText{color:var(--good)}.badText{color:var(--bad)}.legs{display:grid;grid-template-columns:1fr 1fr;gap:9px;margin-top:14px}.leg{background:#0b0f15;border:1px solid #202632;border-radius:12px;padding:12px}.leg-head{display:flex;justify-content:space-between;align-items:center}.role{font-size:10px;font-weight:900;letter-spacing:.08em;color:var(--muted)}.contract{font-size:12px;margin-top:7px;word-break:break-all}.leg-grid{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:10px}.small-v{font-size:14px;font-weight:750;margin-top:2px}.fund{margin-top:14px}.fund-row{display:flex;justify-content:space-between;font-size:12px;margin-bottom:6px}.bar{height:8px;background:#090c11;border:1px solid #242b36;border-radius:999px;overflow:hidden}.fill{height:100%;background:linear-gradient(90deg,#5f85e8,#43d17c);width:0}.funded{color:var(--good);font-weight:800}.empty{padding:32px;text-align:center;color:var(--muted);border:1px dashed var(--line);border-radius:15px}.lower{display:grid;grid-template-columns:1.25fr .75fr;gap:12px}.panel{padding:16px}.panel h3{font-size:14px;margin:0 0 12px}.feed{display:flex;flex-direction:column;gap:8px}.feed-row{display:grid;grid-template-columns:78px 1fr auto;gap:10px;align-items:center;padding:10px;border-radius:10px;background:#0c1016;border:1px solid #202632;font-size:12px}.feed-time{color:var(--muted)}.state-list{display:grid;gap:9px}.state-row{display:flex;justify-content:space-between;gap:16px;padding-bottom:9px;border-bottom:1px solid #222934;font-size:12px}.state-row:last-child{border-bottom:0}.muted{color:var(--muted)}.notice{display:none;margin-top:12px;padding:11px;border-radius:10px;background:#151b24;border:1px solid var(--line);font-size:12px}.notice.show{display:block}@media(max-width:900px){.top{position:static;align-items:flex-start;flex-direction:column}.hero{grid-template-columns:repeat(2,1fr)}.pairs,.lower{grid-template-columns:1fr}}@media(max-width:520px){.shell{padding:10px}.hero{grid-template-columns:1fr 1fr}.metric{padding:13px}.value{font-size:20px}.legs{grid-template-columns:1fr}.actions{width:100%}.btn{flex:1}.chips{gap:5px}}
</style>
<style>
.btn:disabled{opacity:.45;cursor:not-allowed}.access{display:flex;align-items:center;flex-wrap:wrap;gap:10px;margin-top:14px}.access label{font-size:13px}.access input{min-width:180px;flex:1;background:#090c11;border:1px solid var(--line);border-radius:9px;padding:10px;color:var(--text);font:inherit}.access-status{font-size:12px;color:var(--muted)}.pair-status{margin-top:7px;font-size:11px;color:var(--muted)}.feed-row{grid-template-columns:105px minmax(0,1fr) auto}.feed-row .chip{max-width:180px;overflow-wrap:anywhere}@media(max-width:520px){.feed-row{grid-template-columns:1fr}.access input{width:100%}.pair-id{max-width:160px}}
</style>
</head>
<body><div class="shell">
<header class="top"><div class="brand"><div class="mark">AB</div><div><h1>AutoBott</h1><div class="sub" id="policy">Paper trading</div></div></div><div class="chips"><span class="chip good">PAPER ONLY</span><span class="chip warn">REAL MONEY OFF</span><span class="chip" id="session-chip">SESSION CHECKING</span></div><div class="actions"><button class="btn primary" onclick="arm()">Arm Paper</button><button class="btn" onclick="pauseTrading()">Pause</button><button class="btn danger" onclick="kill()">Kill Switch</button><button class="btn" onclick="refreshAll()">Refresh</button></div></header>
<main>
<form class="access panel" id="access-form" onsubmit="unlock(event)"><label for="access-token">Dashboard access</label><input id="access-token" type="password" autocomplete="off" placeholder="Enter dashboard token"><button class="btn" type="submit">Unlock</button><button class="btn" type="button" onclick="lock()">Lock</button><span class="access-status" id="auth-status" role="status">Locked</span></form>
<div class="notice" id="notice" role="status"></div>
<section class="hero"><div class="metric"><div class="label">Paper Equity</div><div class="value" id="equity">—</div><div class="delta" id="cash">—</div></div><div class="metric"><div class="label">Today</div><div class="value" id="daypl">—</div><div class="delta" id="daypct">—</div></div><div class="metric"><div class="label">Open Trades</div><div class="value" id="paircount">—</div><div class="delta" id="legs">—</div></div><div class="metric"><div class="label">Runtime</div><div class="value" id="runtime">—</div><div class="delta" id="last-refresh">—</div></div></section>
<section class="section"><div class="section-head"><h2>Core + Runner Trades</h2><span id="trade-summary">Waiting for account data</span></div><div class="pairs" id="pairs"></div></section>
<section class="section" id="standalone-section" hidden><div class="section-head"><h2>Other Open Positions</h2><span>Not linked to a core + runner trade</span></div><div class="pairs" id="standalone"></div></section>
<section class="section lower"><div class="panel"><h3>Recent Decisions</h3><div class="feed" id="feed"><div class="empty">Loading current decisions…</div></div></div><div class="panel"><h3>System State</h3><div class="state-list" id="state"></div></div></section>
</main></div>
<script>

const money=n=>n!=null&&Number.isFinite(Number(n))?`$${Number(n).toFixed(2)}`:'—';
const pct=n=>n!=null&&Number.isFinite(Number(n))?`${(Number(n)*100).toFixed(1)}%`:'—';
const esc=s=>String(s??'').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;');
let generation=0, refreshing=null, authorized=false, killed=false, refreshFailed=false, commandPending=false;
const token=()=>{try{return sessionStorage.getItem('dashboardToken')||''}catch{return ''}};
function controls(enabled){document.querySelectorAll('.actions .btn').forEach(b=>{if(b.textContent!=='Refresh')b.disabled=!enabled||commandPending})}
async function api(path,opts={}){
  const t=token();
  const r=await fetch(path,{...opts,headers:{'Content-Type':'application/json',...(t?{'Authorization':`Bearer ${t}`}:{})}});
  let p;try{p=await r.json()}catch{throw new Error('The server returned an unreadable response.')}
  if(path==='/api/health'&&r.status===503&&p?.session_supervisor?.stalled===true)return p;
  if(!r.ok||p.ok===false){const e=new Error(r.status===401?'Dashboard access required.':p.detail||p.error||`Request failed (${r.status}).`);e.status=r.status;throw e}
  return p;
}
function note(text,bad=false){const n=document.getElementById('notice');n.textContent=text;n.style.borderColor=bad?'#5b2929':'#254f36';n.classList.add('show')}
function clearAccount(message){
  authorized=false;controls(false);
  ['equity','cash','daypl','daypct','paircount','legs'].forEach(id=>document.getElementById(id).textContent='—');
  document.getElementById('pairs').innerHTML=`<div class="empty">${esc(message)}</div>`;
  document.getElementById('standalone').innerHTML='';document.getElementById('standalone-section').hidden=true;
  document.getElementById('state').innerHTML='';document.getElementById('feed').innerHTML='<div class="empty">Unlock the dashboard to view decisions.</div>';
  document.getElementById('runtime').textContent='UNKNOWN';document.getElementById('runtime').className='value';
  document.getElementById('session-chip').textContent='SESSION UNKNOWN';document.getElementById('session-chip').className='chip warn';
  document.getElementById('last-refresh').textContent='Account data unavailable';
  document.getElementById('trade-summary').textContent='Waiting for account data';
}
function lock(){generation++;try{sessionStorage.removeItem('dashboardToken')}catch{}document.getElementById('access-token').value='';document.getElementById('auth-status').textContent='Locked';clearAccount('Unlock the dashboard to view your paper account.');note('Dashboard locked.')}
async function unlock(event){event.preventDefault();const value=document.getElementById('access-token').value;if(!value)return;generation++;try{sessionStorage.setItem('dashboardToken',value)}catch{note('Session storage is unavailable in this browser.',true);return}document.getElementById('access-token').value='';await refreshAll()}
function legCard(leg,role){
  if(!leg)return `<div class="leg"><div class="role">${role}</div><div class="contract muted">Closed</div></div>`;
  const pl=Number(leg.unrealized_pl||0);
  return `<div class="leg"><div class="leg-head"><span class="role">${role}</span><span class="${pl>=0?'goodText':'badText'}">${money(pl)}</span></div><div class="contract">${esc(leg.symbol)}</div><div class="leg-grid"><div><div class="label">Entry / share</div><div class="small-v">${money(leg.avg_entry_price)}</div></div><div><div class="label">Now / share</div><div class="small-v">${money(leg.current_price)}</div></div><div><div class="label">Contracts</div><div class="small-v">${esc(leg.qty??'—')}</div></div><div><div class="label">Return</div><div class="small-v">${pct(leg.unrealized_plpc)}</div></div></div></div>`;
}
function renderPairs(data){
  const pairs=data.pairs||[],other=data.standalone_positions||[];
  document.getElementById('paircount').textContent=pairs.length+other.length;
  const legs=pairs.reduce((n,p)=>n+(p.primary?1:0)+(p.runner?1:0),0)+other.length;
  document.getElementById('legs').textContent=`${legs} open positions`;
  document.getElementById('trade-summary').textContent=`${data.open_pair_count??0} complete ${data.open_pair_count===1?'pair':'pairs'} · ${data.retained_runner_count??0} retained ${data.retained_runner_count===1?'runner':'runners'}`;
  document.getElementById('pairs').innerHTML=pairs.length?pairs.map(p=>{
    const progress=Math.round(Math.max(0,Math.min(1,Number(p.funding_progress||0)))*100),pnl=Number(p.pair_pnl||0);
    const funding=p.runner_funded?'Runner funded by confirmed core fills':p.funding_unknown?'Core proceeds not verified':p.funding_exit_submitted?'Core exit awaiting confirmation':'Core paying for runner';
    return `<article class="pair"><div class="pair-top"><div><div class="ticker">${esc(p.underlying)}</div><div class="pair-id">${esc(p.trade_group_id)}</div><div class="pair-status">${esc(p.status)}</div></div><div><div class="pnl ${pnl>=0?'goodText':'badText'}">${money(pnl)}</div><div class="label" style="text-align:right">${p.funding_unknown?'OPEN RUNNER P/L':'TRADE P/L'}</div></div></div><div class="legs">${legCard(p.primary,'CORE')}${legCard(p.runner,'RUNNER')}</div><div class="fund"><div class="fund-row"><span class="${p.runner_funded?'funded':''}">${funding}</span><span>${p.funding_unknown?'—':progress+'%'}</span></div>${p.funding_unknown?'':`<div class="bar"><div class="fill" style="width:${progress}%"></div></div>`}<div class="fund-row" style="margin-top:6px;color:var(--muted)"><span>Core P/L ${p.funding_unknown?'—':money(p.primary_pnl)}</span><span>Runner cost ${money(p.runner_entry_cost)}</span></div></div></article>`;
  }).join(''):'<div class="empty">No core + runner trade is open right now.</div>';
  document.getElementById('standalone-section').hidden=!other.length;
  document.getElementById('standalone').innerHTML=other.map(leg=>legCard(leg,'OPEN POSITION')).join('');
}
function renderAccount(data){const a=data.account||{};document.getElementById('equity').textContent=money(a.equity);document.getElementById('cash').textContent=`Cash ${money(a.cash)}`;const pl=Number(a.day_pl||0),e=document.getElementById('daypl');e.textContent=money(a.day_pl);e.className=`value ${pl>=0?'goodText':'badText'}`;document.getElementById('daypct').textContent=a.day_pl_pct==null?'—':`${Number(a.day_pl_pct).toFixed(2)}% today`}
function renderState(safety,session,health){
  killed=!!safety.kill_switch_enabled;const armed=!!safety.execution_enabled&&!killed,blocked=armed&&safety.order_placement_enabled!==true;
  const runtime=killed?'KILLED':blocked?'BLOCKED':armed?'ARMED':'PAUSED';
  document.getElementById('runtime').textContent=runtime;document.getElementById('runtime').className=`value ${killed?'badText':armed&&!blocked?'goodText':''}`;
  const stalled=health.session_supervisor?.stalled===true,alive=!!session.thread_alive&&!stalled,chip=document.getElementById('session-chip');chip.textContent=stalled?'SESSION STALLED':alive?'SESSION RUNNING':'SESSION STOPPED';chip.className=`chip ${stalled?'bad':alive?'good':'warn'}`;
  document.getElementById('policy').textContent=health.policy_version||'Paper trading';
  const lastCycle=session.state?.last_cycle_at;
  document.getElementById('state').innerHTML=[['Broker','Alpaca paper'],['Real money','Locked off'],['Execution',blocked?'Blocked by safety configuration':armed?'Armed':'Paused'],['Kill switch',killed?'Active':'Off'],['Session',stalled?'Stopped unexpectedly':alive?'Running':'Stopped'],['Last cycle',lastCycle?new Date(lastCycle).toLocaleString():'No cycle recorded'],['Policy',health.policy_version||'Unknown']].map(([a,b])=>`<div class="state-row"><span class="muted">${esc(a)}</span><strong>${esc(b)}</strong></div>`).join('');
}
function renderFeed(data){
  const rows=data.decisions||[],root=document.getElementById('feed');
  if(!rows.length){root.innerHTML='<div class="empty">No recent decision cards available.</div>';return}
  root.innerHTML=rows.slice(-8).reverse().map(row=>{const d=row.decision_card||row,ts=d.timestamp||row.recorded_at||'',side=d.direction?.bias||'neutral',status=d.decision||'—',why=d.blocked_reason||d.explanation||'';return `<div class="feed-row"><span class="feed-time">${ts?esc(new Date(ts).toLocaleString()):'—'}</span><span><strong>${esc(d.ticker||row.symbol||'—')} · ${esc(side)}</strong><br><span class="muted">${esc(String(why).slice(0,120))}</span></span><span class="chip ${status==='TRADE_CANDIDATE'?'good':''}">${esc(status)}</span></div>`}).join('');
}
function refreshAll(){
  if(commandPending)return Promise.resolve();
  if(refreshing)return refreshing;
  const revision=generation;
  refreshing=refreshSnapshot(revision).finally(()=>{
    refreshing=null;
    if(revision!==generation&&!commandPending)return refreshAll();
  });
  return refreshing;
}
async function refreshSnapshot(revision){
  try{
    if(!token()){clearAccount('Unlock the dashboard to view your paper account.');return}
    const [pairs,safety,session,health,feed]=await Promise.all([api('/api/v2/pairs'),api('/api/safety'),api('/api/session/status'),api('/api/health'),api('/api/decisions/latest')]);
    if(revision!==generation)return;
    if(refreshFailed){document.getElementById('notice').classList.remove('show');refreshFailed=false}
    renderPairs(pairs);renderAccount(pairs);renderState(safety,session,health);renderFeed(feed);
    authorized=true;controls(true);document.getElementById('auth-status').textContent='Unlocked for this browser session';
    document.getElementById('last-refresh').textContent=`Updated ${new Date().toLocaleTimeString()}`;
  }catch(e){if(revision!==generation)return;refreshFailed=true;clearAccount(e.status===401?'Enter a valid dashboard token.':'Account refresh failed.');document.getElementById('auth-status').textContent=e.status===401?'Locked':'Connection unavailable';note(e.message,true)}
}
async function action(path,body,success){
  if(!authorized||commandPending)return;
  commandPending=true;const revision=++generation;controls(false);
  try{await api(path,{method:'POST',body:JSON.stringify(body)});if(revision===generation)note(success)}
  catch(e){if(revision===generation)note(e.message,true)}
  finally{commandPending=false;await refreshAll()}
}
async function arm(){if(killed&&!confirm('Clear the kill switch and resume paper trading?'))return;await action('/api/runtime/arm-paper',{reason:'v2_cockpit_arm_paper'},'Paper execution armed.')}
async function pauseTrading(){await action('/api/runtime/disable-execution',{reason:'v2_cockpit_pause'},'Paper execution paused.')}
async function kill(){if(!authorized||!confirm('Engage the paper-trading kill switch?'))return;await action('/api/runtime/kill-switch',{enabled:true,reason:'v2_cockpit_kill_switch'},'Kill switch engaged.')}
controls(false);refreshAll();setInterval(refreshAll,15000);
</script></body></html>'''


def main() -> int:
    legacy.bootstrap_env_file()
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))
    legacy.maybe_start_session_supervisor()
    with make_server(host, port, app, server_class=legacy._ThreadingWSGIServer) as httpd:
        print(f"AutoBott v2 cockpit serving on http://{host}:{port}")
        httpd.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
