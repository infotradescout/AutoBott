"""Authenticated dashboard routing must match scheduled entry engine bindings."""
import inspect
import io
import json
from types import SimpleNamespace
import pytest
from autobott_v2 import dashboard_app, dashboard_app_v2, session_runner
from autobott_v2 import trading_cycle as legacy, trading_cycle_v2 as adapter


def test_dashboard_and_scheduled_sessions_use_identical_cycle_entrypoint():
    assert dashboard_app.run_trading_cycle is adapter.run_trading_cycle
    assert inspect.signature(session_runner.run_trading_session).parameters["cycle_runner"].default is adapter.run_trading_cycle


@pytest.mark.parametrize("application", [dashboard_app.app, dashboard_app_v2.app])
def test_authenticated_dashboard_action_gets_private_v2_bindings(application, monkeypatch):
    # Fail here on the baseline: never fall through to a credentialed real cycle.
    assert dashboard_app.run_trading_cycle is adapter.run_trading_cycle
    monkeypatch.setenv("AUTOBOTT_DASHBOARD_AUTH_TOKEN", "synthetic-route-token")
    old_builder, old_monitor = legacy.build_decision_card, legacy.run_position_monitor
    seen = []
    def shell(*, symbols, **kwargs):
        assert globals()["build_decision_card"] is adapter.build_decision_card_v2
        assert globals()["run_position_monitor"] is adapter.run_position_monitor_v2
        assert legacy.build_decision_card is old_builder
        assert legacy.run_position_monitor is old_monitor
        seen.append((symbols, kwargs))
        return SimpleNamespace(to_json_dict=lambda: {"entry_engine": "v2", "orders_submitted": []})
    monkeypatch.setattr(legacy, "run_trading_cycle", shell)
    raw = json.dumps({"symbols": ["aapl"], "quantity": 1}).encode()
    def request(authorized):
        status = []
        env = {"REQUEST_METHOD": "POST", "PATH_INFO": "/api/trading-cycle/run",
               "CONTENT_LENGTH": str(len(raw)), "CONTENT_TYPE": "application/json",
               "wsgi.input": io.BytesIO(raw)}
        if authorized: env["HTTP_AUTHORIZATION"] = "Bearer synthetic-route-token"
        body = b"".join(application(env, lambda value, headers: status.append(value)))
        return status[0], json.loads(body)
    status, _ = request(False)
    assert status.startswith("401")
    assert seen == []
    status, body = request(True)
    assert status.startswith("200"), body
    assert body["entry_engine"] == "v2"
    assert seen == [(["AAPL"], {"quantity": 1, "position_count": 0, "current_daily_realized_pnl": 0.0})]
    assert legacy.build_decision_card is old_builder
    assert legacy.run_position_monitor is old_monitor
