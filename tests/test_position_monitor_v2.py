from __future__ import annotations

import pytest

from autobott_v2 import position_monitor_v2


@pytest.mark.parametrize("fails", [False, True])
def test_v2_delegates_without_mutating_canonical_builder(monkeypatch, fails) -> None:
    original_builder = position_monitor_v2.base_monitor._build_pair_actions
    broker = object()

    def canonical_monitor(**kwargs):
        assert kwargs == {"broker": broker}
        assert position_monitor_v2.base_monitor._build_pair_actions is original_builder
        if fails:
            raise RuntimeError("broker unavailable")
        return {"actions": []}

    monkeypatch.setattr(position_monitor_v2.base_monitor, "run_position_monitor", canonical_monitor)
    if fails:
        with pytest.raises(RuntimeError, match="broker unavailable"):
            position_monitor_v2.run_position_monitor(broker=broker)
    else:
        assert position_monitor_v2.run_position_monitor(broker=broker) == {"actions": []}
    assert position_monitor_v2.base_monitor._build_pair_actions is original_builder
