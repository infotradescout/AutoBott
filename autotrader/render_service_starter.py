"""Lean Render Starter launcher for AutoBott.

Maps `dashboard` to dashboard_starter, then delegates process ownership to
render_service.py. This preserves the trader, watchdog, and stop-loss guard
while avoiding dashboard_v2's heavier operator UI import graph on Starter.
"""

from __future__ import annotations

import os
import runpy
import sys


os.environ.setdefault("PAPER_TRADE_THROUGH_MODE", "true")
os.environ.setdefault("RENDER_STARTER_SAFE_MODE", "true")
os.environ.setdefault("DASHBOARD_TRUTH_CACHE_SECONDS", "30")
os.environ.setdefault("ENABLE_YFINANCE_FALLBACK", "false")
os.environ.setdefault("VIXW_HEAVY_MODE", "false")
os.environ.setdefault("ENABLE_REPLAY_AUTO_PROMOTE", "false")
os.environ.setdefault("REPLAY_AUTO_PROMOTE_ENABLED", "false")
os.environ.setdefault("ENABLE_HISTORICAL_REPLAY_LEARNING", "false")
os.environ.setdefault("ENABLE_DECISION_MEMORY_WORKER", "false")
os.environ.setdefault("ENABLE_MARKET_CONTEXT_WORKER", "false")

import dashboard_starter

sys.modules["dashboard"] = dashboard_starter

print(
    "[render_starter_launcher] lean_dashboard=true "
    f"starter_safe={os.getenv('RENDER_STARTER_SAFE_MODE', 'true')} "
    f"yfinance_fallback={os.getenv('ENABLE_YFINANCE_FALLBACK', 'false')} "
    f"historical_replay={os.getenv('ENABLE_HISTORICAL_REPLAY_LEARNING', 'false')} "
    f"decision_memory_worker={os.getenv('ENABLE_DECISION_MEMORY_WORKER', 'false')} "
    f"market_context_worker={os.getenv('ENABLE_MARKET_CONTEXT_WORKER', 'false')}"
)

runpy.run_module("render_service", run_name="__main__")
