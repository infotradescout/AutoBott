from __future__ import annotations

from typing import Any

from . import position_monitor as base_monitor


PositionMonitorRules = base_monitor.PositionMonitorRules
load_position_monitor_rules = base_monitor.load_position_monitor_rules


def run_position_monitor(**kwargs: Any) -> dict[str, Any]:
    """Run pair-aware monitoring without repeating a pending core harvest.

    The base pair monitor persists ``funding_exit_submitted`` as soon as the
    core sell is accepted. While that core remains visible at the broker, later
    monitor cycles must keep the pair managed but suppress another identical
    core sell request.
    """

    original_builder = base_monitor._build_pair_actions

    def _safe_builder(**builder_kwargs: Any):
        actions, managed = original_builder(**builder_kwargs)
        pair_states = builder_kwargs.get("pair_states") or {}
        stored_positions = builder_kwargs.get("stored_positions") or []
        submitted_groups = {
            str(group_id)
            for group_id, payload in pair_states.items()
            if isinstance(payload, dict) and payload.get("funding_exit_submitted")
        }
        if not submitted_groups:
            return actions, managed
        for stored in stored_positions:
            if (
                stored.trade_group_id in submitted_groups
                and stored.leg_role == "primary"
                and stored.option_symbol.upper() in actions
                and actions[stored.option_symbol.upper()].get("reason") == "primary_profit_funds_runner"
            ):
                actions.pop(stored.option_symbol.upper(), None)
        return actions, managed

    base_monitor._build_pair_actions = _safe_builder
    try:
        return base_monitor.run_position_monitor(**kwargs)
    finally:
        base_monitor._build_pair_actions = original_builder
