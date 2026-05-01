"""Render launcher that serves dashboard_v2 while preserving render_service behavior.

The existing render_service.py owns the trader loop, boot auto-resume,
runtime file migration, and independent stop-loss guard. This launcher swaps
only the dashboard module import so `from dashboard import app` resolves to
`dashboard_v2.app`, boots the isolated volatility proxy sidecar, and exposes
read-only operator explanation APIs.
"""

from __future__ import annotations

import runpy
import sys

from flask import jsonify, request

import dashboard_v2
import volatility_proxy_boot
from decision_journal import build_decision_journal
from decision_outcomes import build_decision_outcomes

sys.modules["dashboard"] = dashboard_v2
volatility_proxy_boot.start()


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


runpy.run_module("render_service", run_name="__main__")
