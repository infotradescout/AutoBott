"""Render launcher that serves dashboard_v2 while preserving render_service behavior.

The existing render_service.py owns the trader loop, boot auto-resume,
runtime file migration, and independent stop-loss guard. This launcher swaps
only the dashboard module import so `from dashboard import app` resolves to
`dashboard_v2.app`.
"""

from __future__ import annotations

import runpy
import sys

import dashboard_v2

sys.modules["dashboard"] = dashboard_v2

runpy.run_module("render_service", run_name="__main__")
