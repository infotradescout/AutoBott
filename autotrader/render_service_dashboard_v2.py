"""Render launcher that serves dashboard_v2 while preserving render_service behavior.

The existing render_service.py owns the trader loop, boot auto-resume,
runtime file migration, and independent stop-loss guard. This launcher swaps
only the dashboard module import so `from dashboard import app` resolves to
`dashboard_v2.app`, boots the isolated volatility proxy sidecar, starts the
persistent learning-memory worker, exposes read-only operator explanation APIs,
and registers always-available quick-link buttons.
"""

from __future__ import annotations

import csv
import runpy
import sys
import threading
from collections import deque
from pathlib import Path

from flask import jsonify, render_template_string, request

import dashboard_v2
import volatility_proxy_boot
from decision_journal import build_decision_journal
from decision_memory import build_learning_summary, run_learning_memory_forever, update_decision_memory
from decision_outcomes import build_decision_outcomes
from quick_links import register_quick_links
from state_store import load_bot_state

sys.modules["dashboard"] = dashboard_v2
volatility_proxy_boot.start()
register_quick_links(dashboard_v2.app)


def _start_learning_memory_worker() -> None:
    try:
        worker = threading.Thread(target=run_learning_memory_forever, daemon=True)
        worker.start()
        print("[decision_memory] background worker thread started")
    except Exception as exc:  # noqa: BLE001
        print(f"[decision_memory] background worker failed to start: {exc}")


_start_learning_memory_worker()


@dashboard_v2.app.get("/api/decision-journal")
def api_decision_journal():
    """Read-only explanation stream for scanner, entry, runtime, proxy, and broker decisions."""
    try:
        limit = int(str(request.args.get("limit", "100") or "100"))
    except ValueError:
        limit = 100
    limit = max(10, min(500, limit))
    return jsonify(build_decision_journal(limit=limit))


@dashboard_v2.app.get("/api/decision-outcomes")
def api_decision_outcomes():
    """Read-only after-the-fact scoring of whether decisions were good or bad."""
    try:
        limit = int(str(request.args.get("limit", "200") or "200"))
    except ValueError:
        limit = 200
    try:
        horizon = int(str(request.args.get("horizon", "15") or "15"))
    except ValueError:
        horizon = 15
    limit = max(50, min(500, limit))
    horizon = max(3, min(120, horizon))
    return jsonify(build_decision_outcomes(journal_limit=limit, horizon_minutes=horizon))


@dashboard_v2.app.get("/api/decision-learning")
def api_decision_learning():
    """Persistent learning summary built from decision memory across restarts."""
    return jsonify(build_learning_summary())


@dashboard_v2.app.get("/decision-learning")
def decision_learning_dashboard():
    """Human-readable decision-learning dashboard."""
    return render_template_string(DECISION_LEARNING_HTML)


@dashboard_v2.app.post("/api/decision-learning/update")
def api_decision_learning_update():
    """Force a read-only learning memory refresh."""
    return jsonify(update_decision_memory())


def _replay_auto_promote_events(limit: int) -> list[dict[str, str]]:
    data_dir = Path(getattr(dashboard_v2.config, "DATA_DIR"))
    path = data_dir / "replay_auto_promote_events.csv"
    if not path.exists():
        return []
    try:
        with path.open("r", newline="", encoding="utf-8") as handle:
            rows = list(deque(csv.DictReader(handle), maxlen=max(1, limit)))
    except Exception:
        return []
    return rows


@dashboard_v2.app.get("/api/replay-auto-promote")
def api_replay_auto_promote():
    """Read-only replay auto-promote status + recent audit events."""
    try:
        limit = int(str(request.args.get("limit", "50") or "50"))
    except ValueError:
        limit = 50
    limit = max(1, min(500, limit))
    state = load_bot_state()
    if not isinstance(state, dict):
        state = {}
    return jsonify(
        {
            "generated_at_et": dashboard_v2._now_et().isoformat(),
            "status": state.get("replay_auto_promote_status", {}) if isinstance(state.get("replay_auto_promote_status"), dict) else {},
            "events": _replay_auto_promote_events(limit=limit),
        }
    )


DECISION_LEARNING_HTML = r"""
<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>Decision Learning</title><style>:root{--bg:#07111f;--panel:#101c2d;--panel2:#13243a;--line:#27384f;--text:#eef5ff;--muted:#8ea1b8;--green:#21d07a;--red:#ff4d5e;--yellow:#ffd166}*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at top,#102039 0,#07111f 45%,#040913 100%);color:var(--text);font:14px/1.4 system-ui,Segoe UI,Arial,sans-serif}.wrap{max-width:1400px;margin:0 auto;padding:22px}.top{display:flex;justify-content:space-between;align-items:flex-start;gap:12px;flex-wrap:wrap}.title{font-size:30px;font-weight:900;letter-spacing:-.03em;margin:0}.sub{color:var(--muted)}.pill{border:1px solid var(--line);border-radius:999px;padding:8px 12px;background:rgba(255,255,255,.04);font-weight:700}.grid{display:grid;grid-template-columns:repeat(12,1fr);gap:14px;margin-top:14px}.card{background:linear-gradient(180deg,var(--panel2),var(--panel));border:1px solid var(--line);border-radius:16px;padding:14px}.span3{grid-column:span 3}.span4{grid-column:span 4}.span6{grid-column:span 6}.span12{grid-column:span 12}.label{color:var(--muted);font-size:12px;text-transform:uppercase;letter-spacing:.08em;font-weight:800}.big{font-size:30px;font-weight:900;margin-top:6px}.muted{color:var(--muted)}.good{color:var(--green)}.bad{color:var(--red)}.warn{color:var(--yellow)}.row{display:flex;justify-content:space-between;gap:10px;border-bottom:1px solid rgba(255,255,255,.07);padding:8px 0}.row:last-child{border-bottom:0}table{width:100%;border-collapse:collapse}th,td{text-align:left;border-bottom:1px solid rgba(255,255,255,.08);padding:8px 7px;font-size:13px}th{color:var(--muted);text-transform:uppercase;font-size:11px;letter-spacing:.07em}tr:last-child td{border-bottom:0}@media(max-width:1100px){.span3,.span4,.span6{grid-column:span 12}}</style></head><body><div class="wrap"><div class="top"><div><h1 class="title">Decision Learning</h1><div class="sub">Readable learning summary from persisted decision memory.</div></div><div style="display:flex;gap:8px;flex-wrap:wrap"><a class="pill" style="text-decoration:none;color:var(--text)" href="/">Main Dashboard</a><a class="pill" style="text-decoration:none;color:var(--text)" href="/api/decision-learning" target="_blank" rel="noopener">Raw JSON</a><span id="updated" class="pill">Loading...</span></div></div><div class="grid"><div class="card span3"><div class="label">Persisted Decisions</div><div id="count" class="big">--</div></div><div class="card span3"><div class="label">Score Total</div><div id="scoreTotal" class="big">--</div></div><div class="card span3"><div class="label">Good Rate</div><div id="goodRate" class="big">--</div></div><div class="card span3"><div class="label">Bad Rate</div><div id="badRate" class="big">--</div></div><div class="card span6"><div class="label">Persistence Status</div><div id="persistRows"></div></div><div class="card span6"><div class="label">Recommendations</div><div id="recoRows"></div></div><div class="card span6"><div class="label">Top Symbols</div><div style="overflow:auto"><table><thead><tr><th>Symbol</th><th>Count</th><th>Good</th><th>Bad</th><th>Neutral</th><th>Score</th></tr></thead><tbody id="symbolsBody"></tbody></table></div></div><div class="card span6"><div class="label">Top RVOL Buckets</div><div style="overflow:auto"><table><thead><tr><th>Bucket</th><th>Count</th><th>Good</th><th>Bad</th><th>Neutral</th><th>Score</th></tr></thead><tbody id="rvolBody"></tbody></table></div></div></div></div><script>const $=(id)=>document.getElementById(id);function n(v){return Number(v||0)}function pct(v){return `${(n(v)*100).toFixed(1)}%`}function safe(v){return (v===null||v===undefined||v==='')?'--':v}function row(k,v){return `<div class="row"><span class="muted">${k}</span><b>${v}</b></div>`}function scoreCls(v){return n(v)>=0?'good':'bad'}function render(p){const totals=p.totals||{};const pers=p.persistence||{};const recs=Array.isArray(p.recommendations)?p.recommendations:[];const bySymbol=((p.aggregates||{}).by_symbol||[]).slice(0,20);const byRvol=((p.aggregates||{}).by_rvol_bucket||[]).slice(0,20);const vc=totals.verdict_counts||{};const c=n(totals.persisted_decisions);const good=n(vc.good);const bad=n(vc.bad);$('updated').textContent=`Updated ${new Date().toLocaleTimeString()}`;$('count').textContent=c.toLocaleString();$('scoreTotal').innerHTML=`<span class="${scoreCls(totals.score_total)}">${safe(totals.score_total)}</span>`;$('goodRate').innerHTML=`<span class="good">${c>0?((good/c)*100).toFixed(1):'0.0'}%</span>`;$('badRate').innerHTML=`<span class="bad">${c>0?((bad/c)*100).toFixed(1):'0.0'}%</span>`;$('persistRows').innerHTML=row('Data dir',safe(pers.data_dir))+row('Memory CSV',safe(pers.memory_csv))+row('Summary JSON',safe(pers.summary_json))+row('Persistent disk check',pers.data_dir_is_persistent_candidate?'<span class="good">OK (/data)</span>':'<span class="bad">NOT PERSISTENT</span>')+row('Note',safe(pers.note));$('recoRows').innerHTML=(recs.map(r=>`<div class="row"><span class="${r.priority==='high'?'bad':(r.priority==='medium'?'warn':'muted')}">${safe(r.priority).toUpperCase()}</span><div style="max-width:78%"><div><b>${safe(r.type)}</b></div><div class="muted">${safe(r.message)}</div><div>${safe(r.action)}</div></div></div>`).join(''))||'<div class="muted">No recommendations yet.</div>';$('symbolsBody').innerHTML=bySymbol.map(x=>`<tr><td>${safe(x.key)}</td><td>${safe(x.count)}</td><td>${safe(x.good_rate)}</td><td>${safe(x.bad_rate)}</td><td>${safe(x.neutral_rate)}</td><td class="${scoreCls(x.score)}"><b>${safe(x.score)}</b></td></tr>`).join('')||'<tr><td colspan="6" class="muted">No data</td></tr>';$('rvolBody').innerHTML=byRvol.map(x=>`<tr><td>${safe(x.key)}</td><td>${safe(x.count)}</td><td>${safe(x.good_rate)}</td><td>${safe(x.bad_rate)}</td><td>${safe(x.neutral_rate)}</td><td class="${scoreCls(x.score)}"><b>${safe(x.score)}</b></td></tr>`).join('')||'<tr><td colspan="6" class="muted">No data</td></tr>'}async function load(){try{const res=await fetch('/api/decision-learning',{cache:'no-store'});render(await res.json())}catch(e){$('updated').textContent='Load failed'}}load();setInterval(load,15000);</script></body></html>
"""


runpy.run_module("render_service", run_name="__main__")
