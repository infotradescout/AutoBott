from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any, Callable
from wsgiref.simple_server import make_server

from .phase1_alpaca_capture_now import capture_now
from .phase1_alpaca_client import AlpacaPaperClient
from .phase1_alpaca_config import load_alpaca_paper_config
from .phase1_campaign_runner import run_phase1_campaign
from .runtime_paths import gate_path as default_gate_path
from .runtime_paths import phase1_replay_campaign_root, phase1_snapshots_root


JsonDict = dict[str, Any]


def app(environ: dict[str, Any], start_response: Callable[..., Any]) -> list[bytes]:
    method = environ.get("REQUEST_METHOD", "GET").upper()
    path = environ.get("PATH_INFO", "/")
    headers = _extract_headers(environ)
    body = _read_body(environ)

    try:
        status_code, content_type, payload = handle_request(method, path, headers, body)
    except PermissionError as exc:
        status_code = 401
        content_type = "application/json; charset=utf-8"
        payload = {"ok": False, "error": "unauthorized", "detail": str(exc)}
    except Exception as exc:  # pragma: no cover
        status_code = 500
        content_type = "application/json; charset=utf-8"
        payload = {"ok": False, "error": type(exc).__name__, "detail": str(exc)}

    start_response(f"{status_code} {_reason(status_code)}", [("Content-Type", content_type)])
    if isinstance(payload, str):
        return [payload.encode("utf-8")]
    return [json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")]


def handle_request(method: str, path: str, headers: dict[str, str], body: bytes) -> tuple[int, str, JsonDict | str]:
    if path == "/":
        return 200, "text/html; charset=utf-8", _dashboard_html()
    if path == "/api/health":
        return 200, "application/json; charset=utf-8", _health_payload()
    if path.startswith("/api/"):
        _require_auth(headers)
        if path == "/api/safety" and method == "GET":
            return 200, "application/json; charset=utf-8", _safety_payload()
        if path == "/api/alpaca/status" and method == "GET":
            return 200, "application/json; charset=utf-8", _alpaca_status_payload()
        if path == "/api/corpus/latest" and method == "GET":
            return 200, "application/json; charset=utf-8", _latest_corpus_payload()
        if path == "/api/campaign/latest" and method == "GET":
            return 200, "application/json; charset=utf-8", _latest_campaign_payload()
        if path == "/api/reports/bucket-edge/latest" and method == "GET":
            return 200, "application/json; charset=utf-8", _latest_bucket_edge_payload()
        if path == "/api/reports/gate-candidate/latest" and method == "GET":
            return 200, "application/json; charset=utf-8", _latest_gate_candidate_payload()
        if path == "/api/capture/start" and method == "POST":
            return 200, "application/json; charset=utf-8", _capture_start_payload(_json_body(body))
        if path == "/api/campaign/run" and method == "POST":
            return 200, "application/json; charset=utf-8", _campaign_run_payload(_json_body(body))
    return 404, "application/json; charset=utf-8", {"ok": False, "error": "not_found"}


def _health_payload() -> JsonDict:
    return {
        "ok": True,
        "app": "autobott-phase1-dashboard",
        "timestamp": datetime.now(UTC).isoformat(),
        "version": os.getenv("RENDER_GIT_COMMIT") or "dev",
    }


def _safety_payload() -> JsonDict:
    config = load_alpaca_paper_config()
    gate_path = _gate_path()
    return {
        "alpaca_env": config.env,
        "paper_only": True,
        "live_trading_enabled": False,
        "order_placement_enabled": False,
        "active_gate_mutation_allowed": False,
        "active_gate_hash": _file_hash(gate_path),
        "active_gate_path": str(gate_path),
        "order_methods_present": _order_methods_present(),
        "mode_banner": "PAPER ONLY | LIVE TRADING LOCKED | ORDERS DISABLED",
    }


def _alpaca_status_payload() -> JsonDict:
    config = load_alpaca_paper_config()
    response: JsonDict = {
        "config": config.redacted_dict(),
        "paper_only": True,
        "live_trading_enabled": False,
        "order_placement_enabled": False,
        "credentials_present": bool(config.api_key and config.secret_key),
    }
    try:
        config.validate()
    except Exception as exc:
        response.update({"ok": False, "status": "config_invalid", "detail": str(exc)})
        return response

    client = AlpacaPaperClient(config)
    try:
        account = client.get_account()
        quotes = client.get_latest_stock_quotes(["SPY", "QQQ"])
        response.update(
            {
                "ok": True,
                "status": "paper_connected",
                "account_status": str(account.get("status", "unknown")),
                "quote_symbols": sorted(quotes.keys()),
                "quote_checks": {"SPY": "PASS" if "SPY" in quotes else "FAIL", "QQQ": "PASS" if "QQQ" in quotes else "FAIL"},
            }
        )
    except Exception as exc:
        response.update({"ok": False, "status": "paper_connection_failed", "detail": str(exc)})
    return response


def _latest_corpus_payload() -> JsonDict:
    manifest_path = _latest_manifest(_corpus_root())
    if manifest_path is None:
        return {"ok": False, "status": "no_corpus_found"}
    manifest = _read_json(manifest_path)
    symbol_dir = manifest_path.parent
    return {
        "ok": True,
        "manifest_path": str(manifest_path),
        "source": manifest.get("source"),
        "corpus_type": manifest.get("corpus_type"),
        "symbol": manifest.get("symbol"),
        "trading_date": manifest.get("trading_date"),
        "snapshots_captured": manifest.get("snapshots_captured"),
        "option_quotes_captured": manifest.get("option_quotes_captured"),
        "skipped_option_quote_reason": manifest.get("skipped_option_quote_reason"),
        "data_quality_flags": manifest.get("data_quality_flags", []),
        "raw_payload_count": len(list((symbol_dir / "raw").glob("*.json"))),
    }


def _latest_campaign_payload() -> JsonDict:
    campaign_dir = _latest_campaign_dir()
    if campaign_dir is None:
        return {"ok": False, "status": "no_campaign_found"}
    manifest = _read_json(campaign_dir / "manifest.json")
    return {
        "ok": True,
        "artifact_dir": str(campaign_dir),
        "campaign_run_id": manifest.get("campaign_run_id", campaign_dir.name),
        "corpus_type": manifest.get("corpus_type"),
        "symbols": manifest.get("symbols", []),
        "campaign_quality": manifest.get("campaign_quality", {}),
        "corpus_quality": manifest.get("corpus_quality", {}),
    }


def _latest_bucket_edge_payload() -> JsonDict:
    campaign_dir = _latest_campaign_dir()
    if campaign_dir is None:
        return {"ok": False, "status": "no_campaign_found"}
    report = _read_json(campaign_dir / "bucket_edge_report.json")
    summary = []
    for bucket_key, bucket in report.get("buckets", {}).items():
        metrics = bucket.get("metrics_by_fill_model", {})
        summary.append(
            {
                "bucket": bucket_key,
                "fill_models": {
                    fill_model: {
                        "closed_trades": values.get("closed_trades"),
                        "profit_factor": values.get("profit_factor"),
                        "expectancy": values.get("expectancy"),
                        "unresolved_position_rate": values.get("unresolved_position_rate"),
                    }
                    for fill_model, values in metrics.items()
                },
            }
        )
    return {"ok": True, "artifact_dir": str(campaign_dir), "bucket_count": len(summary), "buckets": summary}


def _latest_gate_candidate_payload() -> JsonDict:
    campaign_dir = _latest_campaign_dir()
    if campaign_dir is None:
        return {"ok": False, "status": "no_campaign_found"}
    report = _read_json(campaign_dir / "gate_candidate_report.json")
    for candidate in report.get("bucket_candidates", {}).values():
        candidate["live_enabled"] = False
        candidate["manual_approval_required"] = True
    return {
        "ok": True,
        "artifact_dir": str(campaign_dir),
        "live_enabled": False,
        "manual_approval_required": True,
        "bucket_candidates": report.get("bucket_candidates", {}),
    }


def _capture_start_payload(payload: JsonDict) -> JsonDict:
    symbols = [str(symbol).upper() for symbol in payload.get("symbols", ["SPY", "QQQ"])]
    minutes = int(payload.get("minutes", 5))
    interval_seconds = int(payload.get("interval_seconds", 60))
    if minutes > 180:
        raise ValueError("capture_minutes_exceed_v1_limit")
    gate_before = _file_hash(_gate_path())
    result = capture_now(
        symbols=symbols,
        minutes=minutes,
        interval_seconds=interval_seconds,
        corpus_root=_corpus_root(),
        active_gate_path=_gate_path(),
    )
    gate_after = _file_hash(_gate_path())
    result["active_gate_changed"] = gate_before != gate_after
    return result


def _campaign_run_payload(payload: JsonDict) -> JsonDict:
    corpus_root = Path(str(payload.get("corpus_root", _corpus_root())))
    run_id = str(payload.get("campaign_run_id", datetime.now(UTC).strftime("dashboard-%Y%m%d-%H%M%S")))
    gate_before = _file_hash(_gate_path())
    result = run_phase1_campaign(
        corpus_root,
        artifacts_root=_artifacts_root(),
        campaign_run_id=run_id,
        active_gate_path=_gate_path(),
    )
    gate_after = _file_hash(_gate_path())
    result["active_gate_changed"] = gate_before != gate_after
    return result


def _dashboard_html() -> str:
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>AutoBott Phase 1 Operator Console</title>
  <style>
    :root { --bg:#f5efe1; --ink:#132a13; --card:#fffdf6; --accent:#2d6a4f; --warn:#bc4749; --line:#d8cdb8; }
    body { margin:0; font-family: Georgia, 'Times New Roman', serif; background: linear-gradient(180deg, #efe7d6, #f8f4ea); color:var(--ink); }
    header { padding:24px; background: radial-gradient(circle at top left, #fefae0, #dde5b6); border-bottom:1px solid var(--line); }
    .banner { font-weight:bold; letter-spacing:0.04em; color:#fff; background:linear-gradient(90deg, var(--accent), #40916c); padding:12px 16px; display:inline-block; border-radius:999px; }
    main { padding:24px; display:grid; gap:18px; }
    .grid { display:grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap:16px; }
    .card { background:var(--card); border:1px solid var(--line); border-radius:18px; padding:16px; box-shadow:0 12px 30px rgba(19,42,19,0.08); }
    button { border:0; border-radius:999px; padding:12px 16px; background:var(--accent); color:#fff; cursor:pointer; margin-right:8px; margin-bottom:8px; }
    button.secondary { background:#6c757d; }
    pre { white-space:pre-wrap; word-break:break-word; background:#fbf8f1; padding:12px; border-radius:12px; border:1px solid var(--line); }
    table { width:100%; border-collapse:collapse; font-size:14px; }
    th, td { text-align:left; padding:8px; border-bottom:1px solid var(--line); }
  </style>
</head>
<body>
  <header>
    <h1>AutoBott Phase 1 Operator Console</h1>
    <div class="banner">PAPER ONLY | LIVE TRADING LOCKED | ORDERS DISABLED</div>
    <p>Safe operator console for paper capture, advisory campaigns, and report inspection.</p>
  </header>
  <main>
    <section class="grid">
      <div class="card"><h2>Alpaca Paper Config</h2><pre id="alpaca-status">Loading...</pre></div>
      <div class="card"><h2>Latest Capture</h2><pre id="corpus-status">Loading...</pre></div>
      <div class="card"><h2>Latest Campaign</h2><pre id="campaign-status">Loading...</pre></div>
      <div class="card"><h2>Active Gate Safety</h2><pre id="safety-status">Loading...</pre></div>
    </section>
    <section class="card">
      <h2>Actions</h2>
      <button onclick="setToken()">Set Dashboard Token</button>
      <button onclick="refreshAll()" class="secondary">Refresh</button>
      <button onclick="startCapture(5)">Run 5-minute capture</button>
      <button onclick="startCapture(30)">Run 30-minute capture</button>
      <button onclick="runCampaign()">Run campaign from latest corpus</button>
      <pre id="action-log">Idle.</pre>
    </section>
    <section class="card">
      <h2>Bucket Edge Summary</h2>
      <pre id="bucket-report">Loading...</pre>
    </section>
    <section class="card">
      <h2>Gate Candidate Summary</h2>
      <pre id="gate-report">Loading...</pre>
    </section>
    <section class="card">
      <h2>Operator Notes</h2>
      <p>Trading controls are intentionally omitted. This console is limited to paper capture, advisory replay, and report inspection.</p>
    </section>
  </main>
  <script>
    const apiHeaders = () => {
      const token = sessionStorage.getItem('dashboardToken') || '';
      return token ? { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' } : { 'Content-Type': 'application/json' };
    };
    async function callApi(path, options = {}) {
      const response = await fetch(path, { ...options, headers: { ...apiHeaders(), ...(options.headers || {}) } });
      return response.json();
    }
    function setToken() {
      const value = window.prompt('Enter dashboard auth token');
      if (value) sessionStorage.setItem('dashboardToken', value);
    }
    async function refreshAll() {
      document.getElementById('safety-status').textContent = JSON.stringify(await callApi('/api/safety'), null, 2);
      document.getElementById('alpaca-status').textContent = JSON.stringify(await callApi('/api/alpaca/status'), null, 2);
      document.getElementById('corpus-status').textContent = JSON.stringify(await callApi('/api/corpus/latest'), null, 2);
      document.getElementById('campaign-status').textContent = JSON.stringify(await callApi('/api/campaign/latest'), null, 2);
      document.getElementById('bucket-report').textContent = JSON.stringify(await callApi('/api/reports/bucket-edge/latest'), null, 2);
      document.getElementById('gate-report').textContent = JSON.stringify(await callApi('/api/reports/gate-candidate/latest'), null, 2);
    }
    async function startCapture(minutes) {
      const payload = await callApi('/api/capture/start', { method:'POST', body: JSON.stringify({ symbols:['SPY','QQQ'], minutes, interval_seconds:60 }) });
      document.getElementById('action-log').textContent = JSON.stringify(payload, null, 2);
      refreshAll();
    }
    async function runCampaign() {
      const payload = await callApi('/api/campaign/run', { method:'POST', body: JSON.stringify({}) });
      document.getElementById('action-log').textContent = JSON.stringify(payload, null, 2);
      refreshAll();
    }
    refreshAll();
  </script>
</body>
</html>
"""


def _require_auth(headers: dict[str, str]) -> None:
    expected = os.getenv("AUTOBOTT_DASHBOARD_AUTH_TOKEN", "")
    if not expected:
        raise PermissionError("dashboard_auth_token_not_configured")
    provided = headers.get("authorization", "")
    if provided != f"Bearer {expected}":
        raise PermissionError("dashboard_auth_required")


def _extract_headers(environ: dict[str, Any]) -> dict[str, str]:
    headers: dict[str, str] = {}
    for key, value in environ.items():
        if key.startswith("HTTP_"):
            header = key[5:].replace("_", "-").lower()
            headers[header] = str(value)
    if "CONTENT_TYPE" in environ:
        headers["content-type"] = str(environ["CONTENT_TYPE"])
    return headers


def _read_body(environ: dict[str, Any]) -> bytes:
    try:
        length = int(environ.get("CONTENT_LENGTH") or "0")
    except ValueError:
        length = 0
    stream = environ.get("wsgi.input")
    return stream.read(length) if stream is not None else b""


def _json_body(body: bytes) -> JsonDict:
    return json.loads(body.decode("utf-8")) if body else {}


def _reason(status_code: int) -> str:
    return {200: "OK", 401: "Unauthorized", 404: "Not Found", 500: "Internal Server Error"}.get(status_code, "OK")


def _corpus_root() -> Path:
    return phase1_snapshots_root()


def _artifacts_root() -> Path:
    return phase1_replay_campaign_root()


def _gate_path() -> Path:
    return default_gate_path()


def _latest_manifest(root: Path) -> Path | None:
    manifests = sorted(root.rglob("manifest.json"), key=lambda path: path.stat().st_mtime)
    return manifests[-1] if manifests else None


def _latest_campaign_dir() -> Path | None:
    manifests = sorted(_artifacts_root().rglob("manifest.json"), key=lambda path: path.stat().st_mtime)
    return manifests[-1].parent if manifests else None


def _read_json(path: Path) -> JsonDict:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def _file_hash(path: Path) -> str | None:
    if not path.exists():
        return None
    return sha256(path.read_bytes()).hexdigest()


def _order_methods_present() -> bool:
    forbidden = ("submit_order", "replace_order", "cancel_order", "buy", "sell", "close", "liquidate")
    return any(hasattr(AlpacaPaperClient, method_name) for method_name in forbidden)


def main() -> int:
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))
    with make_server(host, port, app) as httpd:
        print(f"AutoBott dashboard serving on http://{host}:{port}")
        httpd.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
