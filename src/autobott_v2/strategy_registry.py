from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Callable


@dataclass(frozen=True)
class StrategyDefinition:
    strategy_id: str
    name: str
    category: str
    enabled: bool
    supported_underlying_types: tuple[str, ...]
    configuration_schema: dict[str, Any]
    preflight_validator: Callable[..., Any]
    lifecycle_handler: Callable[..., Any] | None = None
    risk_policy_extensions: tuple[str, ...] = ()
    analytics_definitions: tuple[str, ...] = ()
    strategy_screens: tuple[str, ...] = ()
    simulation_supported: bool = True
    broker_execution_supported: bool = False

    def to_json_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload.pop("preflight_validator", None)
        payload.pop("lifecycle_handler", None)
        return payload


class StrategyRegistry:
    def __init__(self) -> None:
        self._definitions: dict[str, StrategyDefinition] = {}

    def register(self, definition: StrategyDefinition) -> StrategyDefinition:
        if not definition.strategy_id.strip():
            raise ValueError("strategy_id_required")
        if definition.strategy_id in self._definitions:
            raise ValueError(f"strategy_already_registered:{definition.strategy_id}")
        self._definitions[definition.strategy_id] = definition
        return definition

    def get(self, strategy_id: str) -> StrategyDefinition:
        try:
            return self._definitions[strategy_id]
        except KeyError as exc:
            raise KeyError(f"strategy_not_registered:{strategy_id}") from exc

    def list(self) -> list[StrategyDefinition]:
        return [self._definitions[key] for key in sorted(self._definitions)]


registry = StrategyRegistry()


def register_strategy(definition: StrategyDefinition) -> StrategyDefinition:
    return registry.register(definition)


def strategy_registry_payload() -> dict[str, Any]:
    definitions = registry.list()
    return {
        "ok": True,
        "strategy_count": len(definitions),
        "strategies": [definition.to_json_dict() for definition in definitions],
    }
