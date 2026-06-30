import io
import json
from pathlib import Path

import pytest

from autobott_v2 import dashboard_app


def _invoke_app(method: str, path: str, *, token: str | None = None, payload: dict | None = None):
    body = json.dumps(payload).encode("utf-8") if payload is not None else b""
    environ = {
        "REQUEST_METHOD": method,
        "PATH_INFO": path,
        "CONTENT_LENGTH": str(len(body)),
        "CONTENT_TYPE": "application/json",
        "wsgi.input": io.BytesIO(body),
    }
    if token is not None:
        environ["HTTP_AUTHORIZATION"] = f"Bearer {token}"

    captured: dict[str, object] = {}

    def _start_response(status, headers):
        captured["status"] = status
        captured["headers"] = headers

    chunks = dashboard_app.app(environ, _start_response)
    raw = b"".join(chunks).decode("utf-8")
    return str(captured["status"]), raw


def _auth_env(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("AUTOBOTT_DASHBOARD_AUTH_TOKEN", "dashboard-token")
    monkeypatch.setenv("AUTOBOTT_DATA_ROOT", str(tmp_path / "data"))
    monkeypatch.setenv("AUTOBOTT_ARTIFACTS_ROOT", str(tmp_path / "artifacts"))
    monkeypatch.setenv("AUTOBOTT_GATE_PATH", str(tmp_path / "data" / "PHASE1_CYCLE_GATE.json"))
    gate_path = tmp_path / "data" / "PHASE1_CYCLE_GATE.json"
    gate_path.parent.mkdir(parents=True, exist_ok=True)
    gate_path.write_text(json.dumps({"sentinel": True}, sort_keys=True), encoding="utf-8")


def _write_corpus_manifest(tmp_path: Path) -> None:
    manifest_path = tmp_path / "data" / "phase1_snapshots" / "2026-06-30" / "SPY" / "manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(
            {
                "source": "alpaca",
                "corpus_type": "paper_capture",
                "symbol": "SPY",
                "trading_date": "2026-06-30",
                "snapshots_captured": 10,
                "option_quotes_captured": 8,
                "skipped_option_quote_reason": None,
                "data_quality_flags": ["option_quotes_skipped"],
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def _write_campaign_artifacts(tmp_path: Path) -> None:
    campaign_dir = tmp_path / "artifacts" / "phase1_replay_campaign" / "campaign1"
    campaign_dir.mkdir(parents=True, exist_ok=True)
    (campaign_dir / "manifest.json").write_text(
        json.dumps(
            {
                "campaign_run_id": "campaign1",
                "corpus_type": "paper_capture",
                "symbols": ["SPY", "QQQ"],
                "campaign_quality": {"campaign_valid": True},
                "corpus_quality": {"trading_days": 1},
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    (campaign_dir / "bucket_edge_report.json").write_text(
        json.dumps(
            {
                "buckets": {
                    "spy_call": {
                        "metrics_by_fill_model": {
                            "realistic_mid_penalty": {
                                "closed_trades": 3,
                                "profit_factor": 1.2,
                                "expectancy": 0.1,
                                "unresolved_position_rate": 0.0,
                            }
                        }
                    }
                }
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    (campaign_dir / "gate_candidate_report.json").write_text(
        json.dumps(
            {
                "bucket_candidates": {
                    "spy_call": {
                        "eligible_for_paper_forward": True,
                        "eligible_for_live_review": False,
                    }
                }
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def test_dashboard_health_returns_ok() -> None:
    status, body = _invoke_app("GET", "/api/health")
    payload = json.loads(body)
    assert status.startswith("200")
    assert payload["ok"] is True


def test_dashboard_safety_reports_live_locked(monkeypatch, tmp_path) -> None:
    _auth_env(monkeypatch, tmp_path)
    monkeypatch.setenv("ALPACA_ENV", "paper")
    status, body = _invoke_app("GET", "/api/safety", token="dashboard-token")
    payload = json.loads(body)
    assert status.startswith("200")
    assert payload["live_trading_enabled"] is False
    assert payload["order_placement_enabled"] is False


def test_dashboard_rejects_missing_auth_for_capture(monkeypatch, tmp_path) -> None:
    _auth_env(monkeypatch, tmp_path)
    status, body = _invoke_app("POST", "/api/capture/start", payload={})
    payload = json.loads(body)
    assert status.startswith("401")
    assert payload["error"] == "unauthorized"


def test_dashboard_rejects_missing_auth_for_campaign(monkeypatch, tmp_path) -> None:
    _auth_env(monkeypatch, tmp_path)
    status, body = _invoke_app("POST", "/api/campaign/run", payload={})
    payload = json.loads(body)
    assert status.startswith("401")
    assert payload["error"] == "unauthorized"


def test_dashboard_does_not_expose_alpaca_secrets(monkeypatch, tmp_path) -> None:
    _auth_env(monkeypatch, tmp_path)
    monkeypatch.setenv("ALPACA_ENV", "paper")
    monkeypatch.setenv("ALPACA_API_KEY_ID", "paper-key-123")
    monkeypatch.setenv("ALPACA_API_SECRET_KEY", "paper-secret-456")
    monkeypatch.setenv("ALPACA_TRADING_BASE_URL", "https://paper-api.alpaca.markets")
    monkeypatch.setenv("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets")

    class FakeClient:
        def __init__(self, *_args, **_kwargs):
            pass

        def get_account(self):
            return {"status": "ACTIVE"}

        def get_latest_stock_quotes(self, _symbols):
            return {"SPY": {}, "QQQ": {}}

    monkeypatch.setattr(dashboard_app, "AlpacaPaperClient", FakeClient)
    status, body = _invoke_app("GET", "/api/alpaca/status", token="dashboard-token")
    payload = json.loads(body)
    assert status.startswith("200")
    assert "paper-secret-456" not in body
    assert "paper-key-123" not in body
    assert payload["config"]["secret_key"] != "paper-secret-456"


def test_dashboard_has_no_order_endpoints() -> None:
    assert dashboard_app._order_methods_present() is False
    routes_source = Path("src/autobott_v2/dashboard_app.py").read_text(encoding="utf-8").lower()
    assert "/api/order" not in routes_source


def test_dashboard_capture_preserves_active_gate(monkeypatch, tmp_path) -> None:
    _auth_env(monkeypatch, tmp_path)

    def _fake_capture_now(**_kwargs):
        return {"ok": True, "corpus_root": "data/phase1_snapshots"}

    monkeypatch.setattr(dashboard_app, "capture_now", _fake_capture_now)
    gate_before = Path(tmp_path / "data" / "PHASE1_CYCLE_GATE.json").read_text(encoding="utf-8")
    status, body = _invoke_app("POST", "/api/capture/start", token="dashboard-token", payload={})
    gate_after = Path(tmp_path / "data" / "PHASE1_CYCLE_GATE.json").read_text(encoding="utf-8")
    payload = json.loads(body)
    assert status.startswith("200")
    assert gate_before == gate_after
    assert payload["active_gate_changed"] is False


def test_dashboard_campaign_preserves_active_gate(monkeypatch, tmp_path) -> None:
    _auth_env(monkeypatch, tmp_path)
    _write_corpus_manifest(tmp_path)

    def _fake_run_phase1_campaign(*_args, **_kwargs):
        return {"artifact_dir": "artifacts/phase1_replay_campaign/campaign1"}

    monkeypatch.setattr(dashboard_app, "run_phase1_campaign", _fake_run_phase1_campaign)
    gate_before = Path(tmp_path / "data" / "PHASE1_CYCLE_GATE.json").read_text(encoding="utf-8")
    status, body = _invoke_app("POST", "/api/campaign/run", token="dashboard-token", payload={})
    gate_after = Path(tmp_path / "data" / "PHASE1_CYCLE_GATE.json").read_text(encoding="utf-8")
    payload = json.loads(body)
    assert status.startswith("200")
    assert gate_before == gate_after
    assert payload["active_gate_changed"] is False


def test_latest_bucket_edge_report_loads_without_mutating_gate(monkeypatch, tmp_path) -> None:
    _auth_env(monkeypatch, tmp_path)
    _write_campaign_artifacts(tmp_path)
    gate_before = Path(tmp_path / "data" / "PHASE1_CYCLE_GATE.json").read_text(encoding="utf-8")
    status, body = _invoke_app("GET", "/api/reports/bucket-edge/latest", token="dashboard-token")
    gate_after = Path(tmp_path / "data" / "PHASE1_CYCLE_GATE.json").read_text(encoding="utf-8")
    payload = json.loads(body)
    assert status.startswith("200")
    assert gate_before == gate_after
    assert payload["bucket_count"] == 1


def test_render_config_has_health_check() -> None:
    render_config = Path("render.yaml").read_text(encoding="utf-8")
    assert "healthCheckPath: /api/health" in render_config


def test_frontend_contains_paper_only_live_locked_orders_disabled() -> None:
    status, body = _invoke_app("GET", "/")
    assert status.startswith("200")
    assert "PAPER ONLY | LIVE TRADING LOCKED | ORDERS DISABLED" in body


def test_frontend_contains_no_buy_sell_submit_order_controls() -> None:
    _status, body = _invoke_app("GET", "/")
    lowered = body.lower()
    assert "buy button" not in lowered
    assert "sell button" not in lowered
    assert "submit order" not in lowered
