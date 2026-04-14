"""Lightweight regression smoke checks for the autotrader app.

Usage:
  python smoke_check.py
"""

from __future__ import annotations

import json
import os
import py_compile
import sys
from pathlib import Path


def _ensure_env() -> None:
    # For smoke checks we only need the app to bootstrap.
    os.environ.setdefault("ALPACA_API_KEY", "smoke_key")
    os.environ.setdefault("ALPACA_SECRET_KEY", "smoke_secret")


def _compile_core_files() -> list[str]:
    root = Path(__file__).resolve().parent
    targets = [
        root / "config.py",
        root / "feature_flags.py",
        root / "dashboard.py",
        root / "main.py",
        root / "scanner.py",
        root / "data.py",
    ]
    failures: list[str] = []
    for path in targets:
        try:
            py_compile.compile(str(path), doraise=True)
        except Exception as exc:  # noqa: BLE001
            failures.append(f"compile failed: {path.name}: {exc}")
    return failures


def _smoke_dashboard_routes() -> list[str]:
    failures: list[str] = []
    try:
        import dashboard
    except Exception as exc:  # noqa: BLE001
        return [f"dashboard import failed: {exc}"]

    app = dashboard.app
    client = app.test_client()
    checks = [
        ("GET", "/healthz", {200}),
        ("GET", "/", {200}),
        ("GET", "/watch", {200}),
        ("GET", "/api/status", {200}),
        ("GET", "/api/account", {200}),
        ("GET", "/api/positions", {200}),
        ("GET", "/api/trading-control", {200}),
        ("GET", "/api/watchlist-control", {200}),
        ("GET", "/api/trade-replay", {200}),
        ("GET", "/api/premarket-plan", {200}),
        ("GET", "/api/exit-reliability", {200}),
        ("GET", "/api/ticker-scorecards", {200}),
        ("GET", "/api/weekly-review", {200}),
    ]

    for method, route, ok_statuses in checks:
        try:
            response = client.get(route) if method == "GET" else client.post(route, json={})
            if response.status_code not in ok_statuses:
                failures.append(f"{method} {route} unexpected status={response.status_code}")
        except Exception as exc:  # noqa: BLE001
            failures.append(f"{method} {route} exception: {exc}")

    # Protected routes should reject unauthenticated requests (not 500).
    protected = [
        ("POST", "/api/trading-control/stop"),
        ("POST", "/api/trading-control/start"),
        ("POST", "/api/watchlist-control"),
    ]
    for method, route in protected:
        try:
            response = client.post(route, json={"reason": "smoke_check"})
            if response.status_code not in {401, 503}:
                failures.append(f"{method} {route} expected 401/503 got {response.status_code}")
        except Exception as exc:  # noqa: BLE001
            failures.append(f"{method} {route} exception: {exc}")

    return failures


def main() -> int:
    _ensure_env()
    failures: list[str] = []
    failures.extend(_compile_core_files())
    failures.extend(_smoke_dashboard_routes())

    if failures:
        print("SMOKE_CHECK_FAIL")
        print(json.dumps({"failures": failures}, indent=2))
        return 1

    print("SMOKE_CHECK_OK")
    print(json.dumps({"checked": ["compile", "dashboard_routes"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
