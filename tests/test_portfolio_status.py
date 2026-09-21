import json

from autobott_v2 import portfolio_status as status
from autobott_v2 import session_supervisor
from autobott_v2 import dashboard_app
from test_budgeted_broker_integration import setup
from test_portfolio_budget import intent, reserve
from test_dashboard_app import _invoke_app


def test_status_does_not_create_ledger_or_submit_orders(monkeypatch, tmp_path):
    broker, ledger, transport = setup(monkeypatch, tmp_path)
    before = set(tmp_path.iterdir())
    result = status.capacity_status(broker=broker, ledger_path=ledger.path)
    assert result["ok"]
    assert result["allocation_enabled"]
    assert result["premium_budget_dollars"] == 1000
    assert result["budget_remaining_dollars"] == 1000
    assert not transport.posts
    assert not ledger.path.exists()
    assert set(tmp_path.iterdir()) == before
    assert "synthetic-secret" not in json.dumps(result)


def test_status_keeps_unknown_reservation_without_rewriting_sqlite(monkeypatch, tmp_path):
    broker, ledger, transport = setup(monkeypatch, tmp_path)
    reserve(ledger, [intent(1, 200)])
    before = ledger.path.read_bytes()
    result = status.capacity_status(broker=broker, ledger_path=ledger.path)
    assert result["uncertain_submission_dollars"] == 200
    assert result["budget_remaining_dollars"] == 800
    assert ledger.path.read_bytes() == before
    assert not transport.posts


def test_optional_status_redacts_dependency_errors(monkeypatch):
    monkeypatch.setattr(status, "_CACHE", None)
    monkeypatch.setattr(status, "portfolio_mode_enabled", lambda: False)
    monkeypatch.setenv("AUTOBOTT_PORTFOLIO_BUDGET_OBSERVE", "true")
    def failed():
        raise RuntimeError("PRIVATE-CREDENTIAL-FIXTURE")
    monkeypatch.setattr(status, "capacity_status", failed)
    result = status.optional_capacity_status()
    assert result["ok"] is False
    assert "premium_budget_dollars" not in result
    assert "PRIVATE" not in json.dumps(result)


def test_session_status_never_holds_publication_lock_while_reading_capacity(monkeypatch):
    def diagnostic():
        assert session_supervisor._SESSION_LOCK.acquire(blocking=False)
        session_supervisor._SESSION_LOCK.release()
        return {"ok": True, "allocation_enabled": False}
    monkeypatch.setattr(status, "optional_capacity_status", diagnostic)
    result = session_supervisor.session_supervisor_status()
    assert result["portfolio_capacity"]["ok"]


def test_unauthenticated_session_request_cannot_invoke_capacity_read(monkeypatch):
    monkeypatch.setenv("AUTOBOTT_DASHBOARD_AUTH_TOKEN", "fixture-private-token")
    def forbidden():
        raise AssertionError("unauthorized request invoked capacity provider")
    monkeypatch.setattr(status, "optional_capacity_status", forbidden)
    code, body = _invoke_app("GET", "/api/session/status")
    assert code.startswith("401")
    assert "portfolio_capacity" not in body
