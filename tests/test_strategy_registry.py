from __future__ import annotations

import pytest

from autobott_v2.strategy_registry import StrategyDefinition, StrategyRegistry
from autobott_v2.vix_trader import VIX_STRATEGY_ID


def test_vix_strategy_is_registered_additively() -> None:
    from autobott_v2.strategy_registry import registry

    definition = registry.get(VIX_STRATEGY_ID)
    assert definition.name == "VIX Trader"
    assert definition.supported_underlying_types == ("VIX", "VIXW")
    assert definition.simulation_supported is True
    assert definition.broker_execution_supported is False


def test_registry_rejects_duplicate_strategy_ids() -> None:
    registry = StrategyRegistry()
    definition = StrategyDefinition("one", "One", "test", True, (), {}, lambda *_args: None)
    registry.register(definition)
    with pytest.raises(ValueError, match="strategy_already_registered"):
        registry.register(definition)
