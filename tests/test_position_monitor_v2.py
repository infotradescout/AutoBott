from __future__ import annotations

from types import SimpleNamespace

from autobott_v2 import position_monitor_v2


def test_v2_monitor_suppresses_repeated_primary_funding_exit(monkeypatch) -> None:
    primary = SimpleNamespace(
        trade_group_id="group-1",
        leg_role="primary",
        option_symbol="VIX261016C00017000",
    )

    def original_builder(**kwargs):
        return (
            {
                "VIX261016C00017000": {
                    "reason": "primary_profit_funds_runner",
                    "symbol": "VIX261016C00017000",
                }
            },
            {"VIX261016C00017000", "VIX261016C00020000"},
        )

    monkeypatch.setattr(position_monitor_v2.base_monitor, "_build_pair_actions", original_builder)

    def fake_monitor(**kwargs):
        actions, managed = position_monitor_v2.base_monitor._build_pair_actions(
            pair_states={"group-1": {"funding_exit_submitted": True}},
            stored_positions=[primary],
        )
        return {"actions": actions, "managed": managed}

    monkeypatch.setattr(position_monitor_v2.base_monitor, "run_position_monitor", fake_monitor)

    result = position_monitor_v2.run_position_monitor()

    assert result["actions"] == {}
    assert result["managed"] == {"VIX261016C00017000", "VIX261016C00020000"}


def test_v2_monitor_allows_first_primary_funding_exit(monkeypatch) -> None:
    primary = SimpleNamespace(
        trade_group_id="group-1",
        leg_role="primary",
        option_symbol="VIX261016C00017000",
    )

    def original_builder(**kwargs):
        return (
            {
                "VIX261016C00017000": {
                    "reason": "primary_profit_funds_runner",
                    "symbol": "VIX261016C00017000",
                }
            },
            {"VIX261016C00017000", "VIX261016C00020000"},
        )

    monkeypatch.setattr(position_monitor_v2.base_monitor, "_build_pair_actions", original_builder)

    def fake_monitor(**kwargs):
        actions, managed = position_monitor_v2.base_monitor._build_pair_actions(
            pair_states={},
            stored_positions=[primary],
        )
        return {"actions": actions, "managed": managed}

    monkeypatch.setattr(position_monitor_v2.base_monitor, "run_position_monitor", fake_monitor)

    result = position_monitor_v2.run_position_monitor()

    assert result["actions"]["VIX261016C00017000"]["reason"] == "primary_profit_funds_runner"
