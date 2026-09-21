"""Collect every preserved dashboard regression plus the current health contract.

The case module is the byte-preserved previous dashboard test file. Re-export
its fixtures/helpers/tests so existing imports and collection stay intact; only
the named expiration-window health expectation is superseded below.
"""
import dashboard_regression_cases as _dashboard_cases

for _name, _value in vars(_dashboard_cases).items():
    if not _name.startswith("__"):
        globals()[_name] = _value


def test_dashboard_health_returns_ok() -> None:
    status, body = _invoke_app("GET", "/api/health")
    payload = json.loads(body)
    assert status.startswith("200")
    assert payload["ok"] is True
    assert payload["policy_version"] == HOSTED_POLICY_VERSION
    assert payload["volatility_lane"] == ["VIX", "VXX", "UVXY"]
    assert payload["vix_execution_contracts"] == ["VIX", "VIXW"]
    assert payload["vix_signal_proxy"] == "VIXY"
    assert payload["entry_dte_windows"] == {"tactical": [5, 13], "rider": [14, 45]}


def test_dashboard_regression_collection_is_preserved() -> None:
    cases = {name: value for name, value in vars(_dashboard_cases).items()
             if name.startswith("test_") and name != "test_dashboard_health_returns_ok"}
    assert cases
    assert all(globals().get(name) is value for name, value in cases.items())
