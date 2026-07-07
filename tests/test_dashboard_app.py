import io
import json
from pathlib import Path

import pytest

from autobott_v2 import dashboard_app
from autobott_v2.runtime_control import default_runtime_state, save_runtime_state
from autobott_v2.runtime_control import set_kill_switch


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
    save_runtime_state(default_runtime_state())


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
    primary_dir = campaign_dir / "fill_model_results" / "realistic_mid_penalty"
    primary_dir.mkdir(parents=True, exist_ok=True)
    (campaign_dir / "manifest.json").write_text(
        json.dumps(
            {
                "campaign_run_id": "campaign1",
                "corpus_type": "paper_capture",
                "symbols": ["SPY", "QQQ"],
                "campaign_quality": {"campaign_valid": True},
                "corpus_quality": {"trading_days": 1},
                "thesis_validation_by_fill_model": {
                    "realistic_mid_penalty": {
                        "pass_rate": 0.72,
                        "tactical_2dte_pass_rate": 0.68,
                        "reversal_pass_rate": 0.5,
                    }
                },
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    (primary_dir / "thesis_validation.jsonl").write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "decision_id": "bad-2dte",
                        "ticker": "AAPL",
                        "trade_setup": "bullish_continuation",
                        "option_type": "call",
                        "contract_dte_days": 2,
                        "reason": "directional_followthrough_failed",
                        "passed": False,
                        "followthrough_rate": 0.0,
                        "adverse_move_pct": -0.03,
                        "first_move_match": False,
                        "reversal_confirmed": False,
                    },
                    sort_keys=True,
                ),
                json.dumps(
                    {
                        "decision_id": "bad-reversal",
                        "ticker": "SPY",
                        "trade_setup": "late_cycle_bearish_reversal",
                        "option_type": "put",
                        "contract_dte_days": 6,
                        "reason": "reversal_not_confirmed",
                        "passed": False,
                        "followthrough_rate": 0.25,
                        "adverse_move_pct": -0.01,
                        "first_move_match": False,
                        "reversal_confirmed": False,
                    },
                    sort_keys=True,
                ),
            ]
        ) + "\n",
        encoding="utf-8",
    )
    call_contract = {
        "option_symbol": "AAPL260717C00100000",
        "option_type": "call",
        "expiration": "2026-07-17",
        "mid": 2.5,
        "spread_pct": 0.08,
        "delta": 0.52,
        "theta": -0.04,
        "implied_volatility": 0.35,
    }
    put_contract = {
        "option_symbol": "SPY260717P00100000",
        "option_type": "put",
        "expiration": "2026-07-17",
        "mid": 5.5,
        "spread_pct": 0.14,
        "delta": -0.55,
        "theta": -0.18,
        "implied_volatility": 0.62,
    }
    (primary_dir / "decisions.jsonl").write_text(
        "\n".join(
            [
                json.dumps({"decision_id": "bad-2dte", "ticker": "AAPL", "timestamp": "2026-07-06T15:00:00+00:00", "decision": "TRADE_CANDIDATE", "confidence_score": 0.71, "reason_codes": ["selected_tactical_priority"], "selected_contract": call_contract}, sort_keys=True),
                json.dumps({"decision_id": "bad-reversal", "ticker": "SPY", "timestamp": "2026-07-06T15:00:00+00:00", "decision": "TRADE_CANDIDATE", "confidence_score": 0.66, "reason_codes": ["reversal_confirmation_present"], "selected_contract": put_contract}, sort_keys=True),
            ]
        ) + "\n",
        encoding="utf-8",
    )
    outcomes = [
        {"decision_id": "bad-2dte", "ticker": "AAPL", "trade_setup": "bullish_continuation", "execution_layer": "tactical", "selected_contract": call_contract, "lifecycle_status": "closed", "entry_fill_price": 2.5, "exit_fill_price": 3.0, "pnl": 50.0, "exit_reason": "profit_target"},
        {"decision_id": "bad-reversal", "ticker": "SPY", "trade_setup": "late_cycle_bearish_reversal", "execution_layer": "tactical", "selected_contract": put_contract, "lifecycle_status": "closed", "entry_fill_price": 5.5, "exit_fill_price": 4.0, "pnl": -150.0, "exit_reason": "stop_loss"},
    ]
    (primary_dir / "orders.jsonl").write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in outcomes), encoding="utf-8")
    (primary_dir / "outcomes.jsonl").write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in outcomes), encoding="utf-8")
    (primary_dir / "scorecard.json").write_text(
        json.dumps(
            {
                "win_rate": 0.5,
                "profit_factor": 0.3333,
                "expectancy_per_trade": -50.0,
                "max_drawdown_pct_observed": 1.0,
                "thesis_validation": {"pass_rate": 0.0},
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
                                "thesis_pass_rate": 0.67,
                                "tactical_2dte_pass_rate": 0.67,
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


def test_dashboard_health_fails_when_session_loop_has_died(monkeypatch) -> None:
    monkeypatch.setattr(
        dashboard_app,
        "session_supervisor_status",
        lambda: {
            "config": {"enabled": True},
            "state": {"started_at": "2026-07-07T14:00:00+00:00", "running": False, "last_error": "boom"},
            "thread_alive": False,
        },
    )
    status, body = _invoke_app("GET", "/api/health")
    payload = json.loads(body)
    assert status.startswith("503")
    assert payload["ok"] is False
    assert payload["session_supervisor"]["stalled"] is True
    assert payload["session_supervisor"]["last_error"] == "boom"


def test_dashboard_safety_reports_live_locked(monkeypatch, tmp_path) -> None:
    _auth_env(monkeypatch, tmp_path)
    monkeypatch.setenv("ALPACA_ENV", "paper")
    monkeypatch.setenv("AUTOBOTT_ALLOW_ORDER_PLACEMENT", "true")
    status, body = _invoke_app("GET", "/api/safety", token="dashboard-token")
    payload = json.loads(body)
    assert status.startswith("200")
    assert payload["live_trading_enabled"] is False
    assert payload["order_placement_enabled"] is True
    assert payload["kill_switch_enabled"] is False
    assert payload["mode_banner"].endswith("PAPER EXECUTION ARMED")


def test_dashboard_execution_state_reflects_kill_switch(monkeypatch, tmp_path) -> None:
    _auth_env(monkeypatch, tmp_path)
    set_kill_switch(True, reason="manual_stop")
    status, body = _invoke_app("GET", "/api/execution/state", token="dashboard-token")
    payload = json.loads(body)
    assert status.startswith("200")
    assert payload["kill_switch_enabled"] is True
    assert payload["execution_enabled"] is False


def test_dashboard_latest_decisions_endpoint_returns_rows(monkeypatch, tmp_path) -> None:
    _auth_env(monkeypatch, tmp_path)
    monkeypatch.setattr(dashboard_app, "load_decision_cards", lambda limit=10: [{"decision_card": {"ticker": "AAPL"}}])
    status, body = _invoke_app("GET", "/api/decisions/latest", token="dashboard-token")
    payload = json.loads(body)
    assert status.startswith("200")
    assert payload["count"] == 1
    assert payload["decisions"][0]["decision_card"]["ticker"] == "AAPL"


def test_dashboard_session_status_endpoint_returns_supervisor_state(monkeypatch, tmp_path) -> None:
    _auth_env(monkeypatch, tmp_path)
    monkeypatch.setattr(
        dashboard_app,
        "session_supervisor_status",
        lambda: {
            "config": {"enabled": True, "symbols": ["SPY"], "interval_seconds": 300, "max_cycles": 2, "quantity": 1, "position_count": 0, "daily_pnl": 0.0},
            "state": {"running": False, "started_at": None, "finished_at": None, "last_result": {"cycles_completed": 2}, "last_error": None},
            "thread_alive": False,
        },
    )
    status, body = _invoke_app("GET", "/api/session/status", token="dashboard-token")
    payload = json.loads(body)
    assert status.startswith("200")
    assert payload["ok"] is True
    assert payload["config"]["enabled"] is True
    assert payload["state"]["last_result"]["cycles_completed"] == 2


def test_dashboard_runtime_arm_paper_endpoint_enables_execution(monkeypatch, tmp_path) -> None:
    _auth_env(monkeypatch, tmp_path)
    status, body = _invoke_app("POST", "/api/runtime/arm-paper", token="dashboard-token", payload={"reason": "test_arm"})
    payload = json.loads(body)
    assert status.startswith("200")
    assert payload["runtime_state"]["kill_switch_enabled"] is False
    assert payload["runtime_state"]["execution_enabled"] is True
    assert payload["runtime_state"]["live_mode_enabled"] is False


def test_dashboard_runtime_disable_and_kill_switch_endpoints(monkeypatch, tmp_path) -> None:
    _auth_env(monkeypatch, tmp_path)
    disable_status, disable_body = _invoke_app("POST", "/api/runtime/disable-execution", token="dashboard-token", payload={"reason": "pause"})
    kill_status, kill_body = _invoke_app("POST", "/api/runtime/kill-switch", token="dashboard-token", payload={"enabled": True, "reason": "panic"})
    disable_payload = json.loads(disable_body)
    kill_payload = json.loads(kill_body)
    assert disable_status.startswith("200")
    assert disable_payload["runtime_state"]["execution_enabled"] is False
    assert kill_status.startswith("200")
    assert kill_payload["runtime_state"]["kill_switch_enabled"] is True


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


def test_dashboard_rejects_missing_auth_for_trading_cycle(monkeypatch, tmp_path) -> None:
    _auth_env(monkeypatch, tmp_path)
    status, body = _invoke_app("POST", "/api/trading-cycle/run", payload={})
    payload = json.loads(body)
    assert status.startswith("401")
    assert payload["error"] == "unauthorized"


def test_dashboard_rejects_missing_auth_for_runtime_controls(monkeypatch, tmp_path) -> None:
    _auth_env(monkeypatch, tmp_path)
    status, body = _invoke_app("POST", "/api/runtime/arm-paper", payload={})
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


def test_dashboard_paper_readiness_endpoint_returns_probe(monkeypatch, tmp_path) -> None:
    _auth_env(monkeypatch, tmp_path)
    monkeypatch.setattr(
        dashboard_app,
        "run_paper_readiness_probe",
        lambda: {
            "ok": True,
            "status": "paper_trading_ready",
            "paper_execution_ready": True,
            "paper_config_valid": True,
            "paper_execution_config_valid": True,
            "option_chain_count": 8,
            "decision_status": "TRADE_CANDIDATE",
            "selected_contract": "SPY260703C00600000",
        },
    )
    status, body = _invoke_app("GET", "/api/paper/readiness", token="dashboard-token")
    payload = json.loads(body)
    assert status.startswith("200")
    assert payload["status"] == "paper_trading_ready"
    assert payload["option_chain_count"] == 8


def test_dashboard_has_no_order_endpoints() -> None:
    assert dashboard_app._order_methods_present() is False
    routes_source = Path("src/autobott_v2/dashboard_app.py").read_text(encoding="utf-8").lower()
    assert "/api/order" not in routes_source


def test_dashboard_trading_cycle_endpoint_returns_result(monkeypatch, tmp_path) -> None:
    _auth_env(monkeypatch, tmp_path)

    class FakeCycleResult:
        def to_json_dict(self):
            return {
                "symbols": ["SPY"],
                "orders_submitted": [],
                "skipped": [],
                "snapshot_paths": [],
                "decisions": [],
                "runtime_state": {},
                "started_at": "2026-07-01T15:35:00+00:00",
                "finished_at": "2026-07-01T15:36:00+00:00",
            }

    monkeypatch.setattr(dashboard_app, "run_trading_cycle", lambda **_kwargs: FakeCycleResult())
    status, body = _invoke_app("POST", "/api/trading-cycle/run", token="dashboard-token", payload={"symbols": ["SPY"]})
    payload = json.loads(body)
    assert status.startswith("200")
    assert payload["ok"] is True
    assert payload["symbols"] == ["SPY"]


def test_dashboard_trading_session_endpoint_returns_result(monkeypatch, tmp_path) -> None:
    _auth_env(monkeypatch, tmp_path)

    class FakeSessionResult:
        def to_json_dict(self):
            return {
                "symbols": ["SPY"],
                "cycles_completed": 2,
                "cycle_results": [],
                "started_at": "2026-07-01T15:35:00+00:00",
                "finished_at": "2026-07-01T15:40:00+00:00",
            }

    monkeypatch.setattr(dashboard_app, "run_trading_session", lambda **_kwargs: FakeSessionResult())
    status, body = _invoke_app("POST", "/api/trading-session/run", token="dashboard-token", payload={"symbols": ["SPY"], "max_cycles": 2})
    payload = json.loads(body)
    assert status.startswith("200")
    assert payload["cycles_completed"] == 2


def test_dashboard_session_start_endpoint_returns_started(monkeypatch, tmp_path) -> None:
    _auth_env(monkeypatch, tmp_path)
    monkeypatch.setattr(dashboard_app, "start_session_supervisor", lambda config: True)
    monkeypatch.setattr(
        dashboard_app,
        "session_supervisor_status",
        lambda: {
            "config": {"enabled": True, "symbols": ["SPY"], "interval_seconds": 300, "max_cycles": 1, "quantity": 1, "position_count": 0, "daily_pnl": 0.0},
            "state": {"running": True, "started_at": None, "finished_at": None, "last_result": None, "last_error": None},
            "thread_alive": True,
        },
    )
    status, body = _invoke_app("POST", "/api/session/start", token="dashboard-token", payload={"symbols": ["SPY"], "interval_seconds": 300})
    payload = json.loads(body)
    assert status.startswith("200")
    assert payload["started"] is True
    assert payload["thread_alive"] is True


def test_dashboard_open_positions_endpoint_returns_positions(monkeypatch, tmp_path) -> None:
    _auth_env(monkeypatch, tmp_path)

    class FakePosition:
        def to_json_dict(self):
            return {"broker_order_id": "alpaca-order-1", "symbol": "AAPL", "status": "submitted"}

    monkeypatch.setattr(dashboard_app, "load_open_positions", lambda: [FakePosition()])
    status, body = _invoke_app("GET", "/api/positions/open", token="dashboard-token")
    payload = json.loads(body)
    assert status.startswith("200")
    assert payload["count"] == 1
    assert payload["positions"][0]["symbol"] == "AAPL"


def test_dashboard_account_positions_endpoint_returns_pl(monkeypatch, tmp_path) -> None:
    _auth_env(monkeypatch, tmp_path)

    class FakeConfig:
        def validate(self):
            return self

    class FakeClient:
        def __init__(self, config):
            self.config = config

        def get_account(self):
            return {
                "equity": "10500.00",
                "last_equity": "10000.00",
                "cash": "5000.00",
                "buying_power": "20000.00",
                "portfolio_value": "10500.00",
            }

        def get_positions(self):
            return [
                {
                    "symbol": "SPY260703C00600000",
                    "side": "long",
                    "qty": "1",
                    "avg_entry_price": "2.50",
                    "current_price": "3.00",
                    "market_value": "300.00",
                    "unrealized_pl": "50.00",
                    "unrealized_plpc": "0.2",
                }
            ]

    monkeypatch.setattr(dashboard_app, "load_alpaca_paper_config", lambda: FakeConfig())
    monkeypatch.setattr(dashboard_app, "AlpacaPaperClient", FakeClient)

    status, body = _invoke_app("GET", "/api/account/positions", token="dashboard-token")
    payload = json.loads(body)

    assert status.startswith("200")
    assert payload["ok"] is True
    assert payload["account"]["day_pl"] == 500.0
    assert payload["positions"][0]["symbol"] == "SPY260703C00600000"
    assert payload["positions"][0]["unrealized_pl"] == "50.00"


def test_dashboard_account_orders_endpoint_returns_history(monkeypatch, tmp_path) -> None:
    _auth_env(monkeypatch, tmp_path)

    class FakeConfig:
        def validate(self):
            return self

    class FakeClient:
        def __init__(self, config):
            self.config = config

        def get_orders(self, *, status, limit, direction="desc"):
            return [
                {
                    "symbol": "AAPL",
                    "side": "buy",
                    "qty": "1",
                    "filled_qty": "1",
                    "filled_avg_price": "150.00",
                    "status": "filled",
                    "submitted_at": "2026-07-01T15:35:00Z",
                    "filled_at": "2026-07-01T15:35:05Z",
                }
            ]

    monkeypatch.setattr(dashboard_app, "load_alpaca_paper_config", lambda: FakeConfig())
    monkeypatch.setattr(dashboard_app, "AlpacaPaperClient", FakeClient)

    status, body = _invoke_app("GET", "/api/account/orders", token="dashboard-token")
    payload = json.loads(body)

    assert status.startswith("200")
    assert payload["ok"] is True
    assert payload["orders"][0]["symbol"] == "AAPL"
    assert payload["orders"][0]["status"] == "filled"


def test_dashboard_account_orders_decode_option_type(monkeypatch, tmp_path) -> None:
    _auth_env(monkeypatch, tmp_path)

    class FakeConfig:
        def validate(self):
            return self

    class FakeClient:
        def __init__(self, config):
            self.config = config

        def get_orders(self, *, status, limit, direction="desc"):
            return [
                {
                    "symbol": "QQQ260708P00726000",
                    "side": "buy",
                    "qty": "1",
                    "filled_qty": "1",
                    "filled_avg_price": "5.20",
                    "status": "filled",
                    "submitted_at": "2026-07-06T15:35:00Z",
                    "filled_at": "2026-07-06T15:35:05Z",
                }
            ]

    monkeypatch.setattr(dashboard_app, "load_alpaca_paper_config", lambda: FakeConfig())
    monkeypatch.setattr(dashboard_app, "AlpacaPaperClient", FakeClient)

    status, body = _invoke_app("GET", "/api/account/orders", token="dashboard-token")
    payload = json.loads(body)

    assert status.startswith("200")
    assert payload["orders"][0]["underlying"] == "QQQ"
    assert payload["orders"][0]["option_type"] == "PUT"
    assert payload["orders"][0]["expiration"] == "2026-07-08"
    assert payload["orders"][0]["strike"] == 726.0


def test_dashboard_account_endpoints_require_auth(monkeypatch, tmp_path) -> None:
    _auth_env(monkeypatch, tmp_path)
    status, _ = _invoke_app("GET", "/api/account/positions")
    assert status.startswith("401")
    status, _ = _invoke_app("GET", "/api/account/orders")
    assert status.startswith("401")


def test_dashboard_execution_exit_endpoint_returns_order(monkeypatch, tmp_path) -> None:
    _auth_env(monkeypatch, tmp_path)

    class FakePosition:
        broker_order_id = "alpaca-entry-1"

    class FakeOrder:
        broker_order_id = "alpaca-exit-1"
        state = type("State", (), {"value": "submitted"})()

    monkeypatch.setattr(dashboard_app, "load_open_positions", lambda: [FakePosition()])
    monkeypatch.setattr(dashboard_app, "AlpacaExecutionBroker", lambda: object())
    monkeypatch.setattr(dashboard_app, "submit_exit_for_position", lambda position, broker, limit_price: FakeOrder())
    status, body = _invoke_app("POST", "/api/execution/exit", token="dashboard-token", payload={"broker_order_id": "alpaca-entry-1", "limit_price": 3.1})
    payload = json.loads(body)
    assert status.startswith("200")
    assert payload["broker_order_id"] == "alpaca-exit-1"


def test_dashboard_execution_cancel_and_replace_endpoints(monkeypatch, tmp_path) -> None:
    _auth_env(monkeypatch, tmp_path)
    monkeypatch.setattr(dashboard_app, "AlpacaExecutionBroker", lambda: object())
    monkeypatch.setattr(dashboard_app, "cancel_open_order", lambda broker_order_id, broker: {"id": broker_order_id, "status": "canceled"})
    monkeypatch.setattr(dashboard_app, "replace_open_order", lambda broker_order_id, broker, limit_price: {"id": broker_order_id, "limit_price": f"{limit_price:.2f}"})

    cancel_status, cancel_body = _invoke_app("POST", "/api/execution/cancel", token="dashboard-token", payload={"broker_order_id": "alpaca-entry-1"})
    replace_status, replace_body = _invoke_app("POST", "/api/execution/replace", token="dashboard-token", payload={"broker_order_id": "alpaca-entry-1", "limit_price": 2.75})
    cancel_payload = json.loads(cancel_body)
    replace_payload = json.loads(replace_body)

    assert cancel_status.startswith("200")
    assert cancel_payload["result"]["status"] == "canceled"
    assert replace_status.startswith("200")
    assert replace_payload["result"]["limit_price"] == "2.75"


def test_dashboard_execution_reconcile_endpoint_returns_summary(monkeypatch, tmp_path) -> None:
    _auth_env(monkeypatch, tmp_path)
    monkeypatch.setattr(dashboard_app, "AlpacaExecutionBroker", lambda: object())
    monkeypatch.setattr(
        dashboard_app,
        "reconcile_open_positions",
        lambda broker, journal_path=None: type("Summary", (), {"checked": 2, "updated": 1, "unchanged": 1, "missing": 0})(),
    )
    monkeypatch.setattr(dashboard_app, "load_open_positions", lambda: [object(), object()])
    status, body = _invoke_app("POST", "/api/execution/reconcile", token="dashboard-token", payload={})
    payload = json.loads(body)
    assert status.startswith("200")
    assert payload["checked"] == 2
    assert payload["updated"] == 1
    assert payload["open_position_count"] == 2


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
    assert payload["buckets"][0]["fill_models"]["realistic_mid_penalty"]["tactical_2dte_pass_rate"] == 0.67


def test_latest_decision_lab_payload_scores_campaign(monkeypatch, tmp_path) -> None:
    _auth_env(monkeypatch, tmp_path)
    _write_campaign_artifacts(tmp_path)

    status, body = _invoke_app("GET", "/api/reports/decision-lab/latest", token="dashboard-token")
    payload = json.loads(body)

    assert status.startswith("200")
    assert payload["ok"] is True
    assert payload["summary"]["closed_trades"] == 2
    assert payload["baselines"]["actual_vs_no_trade"] == -100.0
    assert any(row["action"] == "do_not_scale" for row in payload["recommendations"])


def test_decision_lab_backfill_run_endpoint_returns_report(monkeypatch, tmp_path) -> None:
    _auth_env(monkeypatch, tmp_path)
    _write_campaign_artifacts(tmp_path)
    calls = {}

    def _fake_backfill(**kwargs):
        calls["backfill"] = kwargs
        return {"schema_version": "phase1_historical_backfill.v1", "corpus_root": str(kwargs["corpus_root"]), "symbols": kwargs["symbols"]}

    def _fake_campaign(corpus_root, **kwargs):
        calls["campaign"] = {"corpus_root": corpus_root, **kwargs}
        return {"artifact_dir": str(tmp_path / "artifacts" / "phase1_replay_campaign" / "campaign1")}

    monkeypatch.setattr(dashboard_app, "run_historical_backfill", _fake_backfill)
    monkeypatch.setattr(dashboard_app, "run_phase1_campaign", _fake_campaign)
    monkeypatch.setattr(dashboard_app, "_artifacts_root", lambda: tmp_path / "artifacts" / "phase1_replay_campaign")

    status, body = _invoke_app(
        "POST",
        "/api/reports/decision-lab/backfill-run",
        token="dashboard-token",
        payload={"symbols": ["AAPL", "SPY", "QQQ"], "start_date": "2026-06-01", "end_date": "2026-06-10", "interval_minutes": 15, "campaign_run_id": "campaign1"},
    )
    payload = json.loads(body)

    assert status.startswith("200")
    assert payload["ok"] is True
    assert calls["backfill"]["symbols"] == ["AAPL", "SPY"]
    assert calls["backfill"]["start_date"].isoformat() == "2026-06-08"
    assert calls["backfill"]["interval_minutes"] == 30
    assert calls["campaign"]["campaign_run_id"] == "campaign1"
    assert payload["operational_limits"]["requested_symbols"] == ["AAPL", "SPY", "QQQ"]
    assert payload["operational_limits"]["used_symbols"] == ["AAPL", "SPY"]
    assert payload["decision_lab"]["summary"]["closed_trades"] == 2


def test_latest_campaign_payload_includes_primary_thesis_metrics(monkeypatch, tmp_path) -> None:
    _auth_env(monkeypatch, tmp_path)
    _write_campaign_artifacts(tmp_path)

    status, body = _invoke_app("GET", "/api/campaign/latest", token="dashboard-token")
    payload = json.loads(body)

    assert status.startswith("200")
    assert payload["primary_thesis_validation"]["pass_rate"] == 0.72
    assert payload["primary_thesis_validation"]["tactical_2dte_pass_rate"] == 0.68


def test_latest_thesis_failures_payload_ranks_and_returns_failures(monkeypatch, tmp_path) -> None:
    _auth_env(monkeypatch, tmp_path)
    _write_campaign_artifacts(tmp_path)

    status, body = _invoke_app("GET", "/api/reports/thesis-failures/latest", token="dashboard-token")
    payload = json.loads(body)

    assert status.startswith("200")
    assert payload["count"] == 2
    assert payload["failures"][0]["decision_id"] == "bad-2dte"
    assert payload["failures"][0]["contract_dte_days"] == 2
    assert payload["failures"][0]["reason"] == "directional_followthrough_failed"


def test_render_config_has_health_check() -> None:
    render_config = Path("render.yaml").read_text(encoding="utf-8")
    assert "healthCheckPath: /api/health" in render_config
    assert "mountPath: /var/data/autobott" in render_config
    assert "value: paper" in render_config
    assert "value: https://paper-api.alpaca.markets" in render_config
    assert 'value: "true"' in render_config
    assert 'value: "false"' in render_config
    assert "key: AUTOBOTT_DATA_ROOT" in render_config
    assert "key: AUTOBOTT_ARTIFACTS_ROOT" in render_config
    assert "key: AUTOBOTT_GATE_PATH" in render_config
    assert "key: AUTOBOTT_SESSION_AUTOSTART" in render_config
    assert "key: AUTOBOTT_SESSION_SYMBOLS" in render_config
    assert "key: AUTOBOTT_SESSION_START_TIME" in render_config
    assert "key: AUTOBOTT_SESSION_END_TIME" in render_config
    assert "key: AUTOBOTT_SESSION_MARKET_TIMEZONE" in render_config
    assert "key: AUTOBOTT_SESSION_ARM_PAPER_EXECUTION" in render_config
    assert "key: AUTOBOTT_PAPER_TRADE_ALL_PASSED_SIGNALS" in render_config
    assert "key: AUTOBOTT_PAPER_MAX_NEW_ENTRY_ATTEMPTS_PER_LOOP" in render_config
    assert "key: AUTOBOTT_PAPER_MAX_OPEN_ENTRY_BUY_ORDERS" in render_config


def test_frontend_contains_paper_only_live_locked_orders_disabled() -> None:
    status, body = _invoke_app("GET", "/")
    assert status.startswith("200")
    assert "PAPER ONLY | LIVE TRADING LOCKED | EXECUTION CHECKING" in body
    assert "AutoBott Phase 1 Operator Console" in body
    assert "LOCKED" in body
    assert "Session Supervisor" in body
    assert "Arm paper execution" in body
    assert "Paper Readiness" in body


def test_frontend_contains_no_buy_sell_submit_order_controls() -> None:
    _status, body = _invoke_app("GET", "/")
    lowered = body.lower()
    assert "buy button" not in lowered
    assert "sell button" not in lowered
    assert "submit order" not in lowered
    assert "run protected trading cycle" in lowered
    assert "start paper session" in lowered


def test_frontend_contains_clean_locked_state_copy() -> None:
    status, body = _invoke_app("GET", "/")
    assert status.startswith("200")
    assert "Dashboard token required" in body
    assert "Set token to view this panel." in body
    assert "Theory pass" in body
    assert "2DTE pass" in body
    assert "Worst Thesis Failures" in body
