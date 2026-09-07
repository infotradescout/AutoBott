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
        payload = json.dumps(_pair_cockpit_payload(), allow_nan=False).encode("utf-8")
        start_response("200 OK", [("Content-Type", "application/json; charset=utf-8")])
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
        runner_entry_cost = _leg_entry_cost(runner)
        primary_pnl = _float(primary.get("unrealized_pl")) if primary else _float(state.get("primary_realized_pnl_estimate"))
        funding_progress = 1.0 if state.get("runner_funded") else (
            max(0.0, min(1.0, primary_pnl / runner_entry_cost)) if runner_entry_cost > 0 else 0.0
        )
        pair_pnl = round(sum(_float(leg.get("unrealized_pl")) for leg in legs), 2)
        if state.get("runner_funded"):
            pair_pnl = round(pair_pnl + _float(state.get("primary_realized_pnl_estimate")), 2)
        underlying = next((str(leg.get("underlying") or "") for leg in legs if leg.get("underlying")), "")
        if not underlying:
            underlying = _underlying_from_option_symbol(str(legs[0].get("symbol") or "")) or "UNKNOWN"
        pairs.append(
            {
                "trade_group_id": group_id,
                "underlying": underlying,
                "status": "FUNDED RUNNER" if state.get("runner_funded") else "PAIR OPEN",
                "runner_funded": bool(state.get("runner_funded")),
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
</head>
<body><div class="shell">
<header class="top"><div class="brand"><div class="mark">AB</div><div><h1>AutoBott</h1><div class="sub" id="policy">Rebuilt paper trading brain</div></div></div><div class="chips"><span class="chip good">PAPER ONLY</span><span class="chip warn">REAL MONEY OFF</span><span class="chip" id="session-chip">SESSION CHECKING</span></div><div class="actions"><button class="btn primary" onclick="arm()">Arm Paper</button><button class="btn" onclick="pauseTrading()">Pause</button><button class="btn danger" onclick="kill()">Kill Switch</button><button class="btn" onclick="refreshAll()">Refresh</button></div></header>
<div class="notice" id="notice"></div>
<section class="hero"><div class="metric"><div class="label">Paper Equity</div><div class="value" id="equity">—</div><div class="delta" id="cash">—</div></div><div class="metric"><div class="label">Today</div><div class="value" id="daypl">—</div><div class="delta" id="daypct">—</div></div><div class="metric"><div class="label">Open Pairs</div><div class="value" id="paircount">—</div><div class="delta" id="legs">—</div></div><div class="metric"><div class="label">Runtime</div><div class="value" id="runtime">—</div><div class="delta" id="last-refresh">—</div></div></section>
<section class="section"><div class="section-head"><h2>Open Core + Runner Trades</h2><span>Managed as one risk unit</span></div><div class="pairs" id="pairs"></div></section>
<section class="section lower"><div class="panel"><h3>Recent Decisions</h3><div class="feed" id="feed"><div class="empty">Loading current decisions…</div></div></div><div class="panel"><h3>System State</h3><div class="state-list" id="state"></div></div></section>
</div>
<script>
const money=n=>Number.isFinite(Number(n))?`$${Number(n).toFixed(2)}`:'—';
const pct=n=>Number.isFinite(Number(n))?`${(Number(n)*100).toFixed(1)}%`:'—';
const esc=s=>String(s??'').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;');
async function api(path,opts={}){const r=await fetch(path,{...opts,headers:{'Content-Type':'application/json',...(opts.headers||{})}});let p={};try{p=await r.json()}catch{}return {ok:r.ok,p};}
function note(text,bad=false){const n=document.getElementById('notice');n.textContent=text;n.style.borderColor=bad?'#5b2929':'#254f36';n.classList.add('show');setTimeout(()=>n.classList.remove('show'),5000)}
function legCard(leg,role){if(!leg)return `<div class="leg"><div class="role">${role}</div><div class="contract muted">Closed</div></div>`;const pl=Number(leg.unrealized_pl||0);return `<div class="leg"><div class="leg-head"><span class="role">${role}</span><span class="${pl>=0?'goodText':'badText'}">${money(pl)}</span></div><div class="contract">${esc(leg.symbol)}</div><div class="leg-grid"><div><div class="label">Entry</div><div class="small-v">${money(leg.avg_entry_price)}</div></div><div><div class="label">Now</div><div class="small-v">${money(leg.current_price)}</div></div><div><div class="label">Qty</div><div class="small-v">${esc(leg.qty||'—')}</div></div><div><div class="label">Return</div><div class="small-v">${pct(leg.unrealized_plpc)}</div></div></div></div>`}
function renderPairs(data){const root=document.getElementById('pairs');document.getElementById('paircount').textContent=data.pair_count??0;const legs=(data.pairs||[]).reduce((n,p)=>n+(p.primary?1:0)+(p.runner?1:0),0);document.getElementById('legs').textContent=`${legs} open legs`;if(!(data.pairs||[]).length){root.innerHTML='<div class="empty">No core + runner pair is open right now.</div>';return}root.innerHTML=data.pairs.map(p=>{const progress=Math.round(Number(p.funding_progress||0)*100);const pnl=Number(p.pair_pnl||0);return `<article class="pair"><div class="pair-top"><div><div class="ticker">${esc(p.underlying)}</div><div class="pair-id">${esc(p.trade_group_id)}</div></div><div><div class="pnl ${pnl>=0?'goodText':'badText'}">${money(pnl)}</div><div class="label" style="text-align:right">PAIR P/L</div></div></div><div class="legs">${legCard(p.primary,'CORE')}${legCard(p.runner,'RUNNER')}</div><div class="fund"><div class="fund-row"><span>${p.runner_funded?'<span class="funded">Runner funded</span>':'Core paying for runner'}</span><span>${p.runner_funded?'100%':progress+'%'}</span></div><div class="bar"><div class="fill" style="width:${p.runner_funded?100:progress}%"></div></div><div class="fund-row" style="margin-top:6px;color:var(--muted)"><span>Core P/L ${money(p.primary_pnl)}</span><span>Runner cost ${money(p.runner_entry_cost)}</span></div></div></article>`}).join('')}
function renderAccount(data){const a=data.account||{};document.getElementById('equity').textContent=money(a.equity);document.getElementById('cash').textContent=`Cash ${money(a.cash)}`;const pl=Number(a.day_pl||0);const e=document.getElementById('daypl');e.textContent=money(pl);e.className=`value ${pl>=0?'goodText':'badText'}`;document.getElementById('daypct').textContent=`${Number(a.day_pl_pct||0).toFixed(2)}% today`;}
function renderState(safety,session,health){const armed=!!safety.execution_enabled&&!safety.kill_switch_enabled;document.getElementById('runtime').textContent=safety.kill_switch_enabled?'KILLED':armed?'ARMED':'PAUSED';document.getElementById('runtime').className=`value ${safety.kill_switch_enabled?'badText':armed?'goodText':''}`;const alive=!!session.thread_alive;const chip=document.getElementById('session-chip');chip.textContent=alive?'SESSION RUNNING':'SESSION STOPPED';chip.className=`chip ${alive?'good':'warn'}`;document.getElementById('policy').textContent=`${health.policy_version||'policy unknown'} · ${health.entry_dte_windows?.tactical?.join('–')||'5–10'} DTE core lane`;document.getElementById('state').innerHTML=[['Broker','Alpaca paper'],['Real money','Locked off'],['Execution',armed?'Armed':'Paused'],['Kill switch',safety.kill_switch_enabled?'Active':'Off'],['Session',alive?'Running':'Stopped'],['Policy',health.policy_version||'unknown']].map(([a,b])=>`<div class="state-row"><span class="muted">${esc(a)}</span><strong>${esc(b)}</strong></div>`).join('')}
function renderFeed(data){const root=document.getElementById('feed');const rows=data.decisions||data.rows||data.items||[];if(!rows.length){root.innerHTML='<div class="empty">No recent decision cards available.</div>';return}root.innerHTML=rows.slice(-8).reverse().map(row=>{const d=row.decision_card||row;const ts=d.timestamp||row.recorded_at||'';const side=d.direction?.bias||'neutral';const status=d.decision||'—';const why=d.blocked_reason||d.explanation||'';return `<div class="feed-row"><span class="feed-time">${ts?new Date(ts).toLocaleTimeString([],{hour:'2-digit',minute:'2-digit'}):'—'}</span><span><strong>${esc(d.ticker||row.symbol||'—')} · ${esc(side)}</strong><br><span class="muted">${esc(String(why).slice(0,120))}</span></span><span class="chip ${status==='TRADE_CANDIDATE'?'good':''}">${esc(status)}</span></div>`}).join('')}
async function refreshAll(){try{const [pairs,safety,session,health,feed]=await Promise.all([api('/api/v2/pairs'),api('/api/safety'),api('/api/session/status'),api('/api/health'),api('/api/decisions/feed')]);if(pairs.p.ok){renderPairs(pairs.p);renderAccount(pairs.p)}else note(pairs.p.detail||'Pair data unavailable',true);if(safety.p&&session.p&&health.p)renderState(safety.p,session.p,health.p);if(feed.p)renderFeed(feed.p);document.getElementById('last-refresh').textContent=`Updated ${new Date().toLocaleTimeString()}`;}catch(e){note(`Refresh failed: ${e.message}`,true)}}
async function arm(){const r=await api('/api/runtime/arm-paper',{method:'POST',body:JSON.stringify({reason:'v2_cockpit_arm_paper'})});note(r.p.ok?'Paper execution armed.':'Could not arm paper execution.',!r.p.ok);refreshAll()}
async function pauseTrading(){const r=await api('/api/runtime/disable-execution',{method:'POST',body:JSON.stringify({reason:'v2_cockpit_pause'})});note(r.p.ok?'Paper execution paused.':'Could not pause execution.',!r.p.ok);refreshAll()}
async function kill(){if(!confirm('Engage the paper-trading kill switch?'))return;const r=await api('/api/runtime/kill-switch',{method:'POST',body:JSON.stringify({enabled:true,reason:'v2_cockpit_kill_switch'})});note(r.p.ok?'Kill switch engaged.':'Kill switch request failed.',!r.p.ok);refreshAll()}
refreshAll();setInterval(refreshAll,15000);
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
