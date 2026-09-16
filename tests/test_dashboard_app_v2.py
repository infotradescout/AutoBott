from __future__ import annotations

import io
import json

import pytest

from autobott_v2 import dashboard_app_v2


def test_pair_cockpit_aggregates_core_runner_and_funding_progress(monkeypatch) -> None:
    monkeypatch.setattr(
        dashboard_app_v2.legacy,
        "_account_positions_payload",
        lambda: {
            "ok": True,
            "account": {"equity": 10000.0, "cash": 5000.0, "day_pl": 125.0, "day_pl_pct": 1.25},
            "positions": [
                {
                    "symbol": "VIX261016C00017000",
                    "leg_role": "primary",
                    "trade_group_id": "group-1",
                    "qty": "1",
                    "avg_entry_price": "0.70",
                    "current_price": "0.90",
                    "unrealized_pl": "20.0",
                    "unrealized_plpc": "0.2857",
                },
                {
                    "symbol": "VIX261016C00020000",
                    "leg_role": "runner",
                    "trade_group_id": "group-1",
                    "qty": "1",
                    "avg_entry_price": "0.25",
                    "current_price": "0.30",
                    "unrealized_pl": "5.0",
                    "unrealized_plpc": "0.20",
                },
            ],
        },
    )
    monkeypatch.setattr(dashboard_app_v2, "_load_pair_states", lambda: {})

    payload = dashboard_app_v2._pair_cockpit_payload()

    assert payload["ok"] is True
    assert payload["pair_count"] == 1
    pair = payload["pairs"][0]
    assert pair["underlying"] == "VIX"
    assert pair["pair_pnl"] == 25.0
    assert pair["runner_entry_cost"] == 25.0
    assert pair["primary_pnl"] == 20.0
    assert pair["funding_progress"] == 0.8
    assert pair["runner_funded"] is False


def test_pair_cockpit_includes_realized_core_after_runner_is_funded(monkeypatch) -> None:
    monkeypatch.setattr(
        dashboard_app_v2.legacy,
        "_account_positions_payload",
        lambda: {
            "ok": True,
            "account": {},
            "positions": [
                {
                    "symbol": "VIX261016C00020000",
                    "leg_role": "runner",
                    "trade_group_id": "group-1",
                    "qty": "1",
                    "avg_entry_price": "0.25",
                    "current_price": "0.40",
                    "unrealized_pl": "15.0",
                    "unrealized_plpc": "0.60",
                }
            ],
        },
    )
    monkeypatch.setattr(
        dashboard_app_v2,
        "_load_pair_states",
        lambda: {"group-1": {"runner_funded": True, "funding_verified": True, "primary_realized_pnl": 30.0}},
    )

    payload = dashboard_app_v2._pair_cockpit_payload()

    pair = payload["pairs"][0]
    assert pair["status"] == "FUNDED RUNNER"
    assert pair["funding_progress"] == 1.0
    assert pair["pair_pnl"] == 45.0


def _request(path, token=None, method="GET"):
    environ = {"REQUEST_METHOD": method, "PATH_INFO": path, "CONTENT_LENGTH": "0", "wsgi.input": io.BytesIO(b"")}
    if token is not None:
        environ["HTTP_AUTHORIZATION"] = f"Bearer {token}"
    response = {}
    body = b"".join(dashboard_app_v2.app(environ, lambda status, headers: response.update(status=status, headers=headers)))
    return response["status"], body


@pytest.mark.parametrize("configured", [None, "test-token"])
@pytest.mark.parametrize("provided", [None, "wrong-token", "非ASCII-token"])
def test_pairs_route_requires_auth_before_reading_account(monkeypatch, configured, provided):
    monkeypatch.delenv("AUTOBOTT_DASHBOARD_AUTH_TOKEN", raising=False)
    if configured:
        monkeypatch.setenv("AUTOBOTT_DASHBOARD_AUTH_TOKEN", configured)
    monkeypatch.setattr(dashboard_app_v2, "_pair_cockpit_payload", lambda: pytest.fail("Unauthorized request read account data"))
    status, body = _request("/api/v2/pairs", provided)
    assert status.startswith("401")
    assert json.loads(body) == {"ok": False, "error": "unauthorized"}


def test_pairs_route_accepts_exact_token_and_bounds_failures(monkeypatch):
    monkeypatch.setenv("AUTOBOTT_DASHBOARD_AUTH_TOKEN", "test-token")
    monkeypatch.setattr(dashboard_app_v2, "_pair_cockpit_payload", lambda: {"ok": True, "pairs": []})
    status, body = _request("/api/v2/pairs", "test-token")
    assert status.startswith("200")
    assert json.loads(body)["pairs"] == []
    def broken():
        raise RuntimeError("internal-data-must-not-leak")
    monkeypatch.setattr(dashboard_app_v2, "_pair_cockpit_payload", broken)
    status, body = _request("/api/v2/pairs", "test-token")
    assert status.startswith("500")
    assert json.loads(body) == {"ok": False, "error": "pair_data_unavailable"}


@pytest.mark.parametrize("method,path", [("GET", "/api/account/positions"), ("POST", "/api/runtime/arm-paper"), ("POST", "/api/runtime/kill-switch")])
def test_cockpit_cannot_bypass_shared_auth_for_legacy_routes(monkeypatch, method, path):
    monkeypatch.setenv("AUTOBOTT_DASHBOARD_AUTH_TOKEN", "test-token")
    status, body = _request(path, method=method)
    assert status.startswith("401")
    assert json.loads(body) == {"ok": False, "error": "unauthorized"}


@pytest.mark.parametrize("state,expected_pnl,unknown", [
    ({}, 15.0, True),
    ({"runner_funded": True, "primary_realized_pnl_estimate": 30.0}, 15.0, True),
    ({"runner_funded": False, "funding_verified": True, "primary_realized_pnl": 23.0}, 38.0, False),
])
def test_cockpit_uses_only_verified_core_fills(monkeypatch, state, expected_pnl, unknown):
    monkeypatch.setattr(dashboard_app_v2.legacy, "_account_positions_payload", lambda: {
        "ok": True, "account": {}, "positions": [{"symbol": "VXX261016P00017000", "leg_role": "runner", "trade_group_id": "g", "qty": "1", "avg_entry_price": "0.25", "unrealized_pl": "15.0"}]
    })
    monkeypatch.setattr(dashboard_app_v2, "_load_pair_states", lambda: {"g": state})
    data = dashboard_app_v2._pair_cockpit_payload()
    assert data["open_pair_count"] == 0
    assert data["retained_runner_count"] == 1
    pair = data["pairs"][0]
    assert pair["runner_funded"] is False
    assert pair["funding_unknown"] is unknown
    assert pair["pair_pnl"] == expected_pnl
