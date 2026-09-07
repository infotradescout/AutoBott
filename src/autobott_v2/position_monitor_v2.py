from __future__ import annotations

from typing import Any

from . import position_monitor as base_monitor


PositionMonitorRules = base_monitor.PositionMonitorRules
load_position_monitor_rules = base_monitor.load_position_monitor_rules


def run_position_monitor(**kwargs: Any) -> dict[str, Any]:
    """Use the canonical monitor's persisted, fill-reconciled pair lifecycle."""
    return base_monitor.run_position_monitor(**kwargs)
