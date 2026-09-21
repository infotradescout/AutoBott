from __future__ import annotations

from dataclasses import replace
from types import FunctionType
from typing import Any

from . import trading_cycle as legacy_cycle
from .expiry_coverage import ExpiryCoverageClient
from .phase1_engine_v2 import build_decision_card as build_decision_card_v2
from .position_monitor_v2 import run_position_monitor as run_position_monitor_v2


TradingCycleResult = legacy_cycle.TradingCycleResult
load_decision_cards = legacy_cycle.load_decision_cards


def run_trading_cycle(*, symbols: list[str], **kwargs: Any) -> TradingCycleResult:
    """Run the execution shell with private, per-call engine bindings.

    Journal helpers, locks, broker safety gates and durable stores retain their
    original identities. No shared module attribute is replaced, including
    while an overlapping call is running or an exception is being handled.
    Expiration telemetry observes existing data reads, not additional requests.
    """
    shell = legacy_cycle.run_trading_cycle
    if not isinstance(shell, FunctionType):
        raise TypeError("cycle_shell_must_be_python_function")
    namespace = {
        **shell.__globals__,
        "build_decision_card": build_decision_card_v2,
        "run_position_monitor": run_position_monitor_v2,
    }
    coverage: list[dict[str, Any]] = []
    if kwargs.get("data_client") is not None:
        kwargs = {**kwargs, "data_client": ExpiryCoverageClient(kwargs["data_client"], coverage)}
    elif "AlpacaPaperClient" in namespace:
        client_factory = namespace["AlpacaPaperClient"]
        namespace["AlpacaPaperClient"] = lambda: ExpiryCoverageClient(client_factory(), coverage)
    isolated_shell = FunctionType(
        shell.__code__, namespace, shell.__name__, shell.__defaults__, shell.__closure__
    )
    isolated_shell.__kwdefaults__ = shell.__kwdefaults__
    result = isolated_shell(symbols=symbols, **kwargs)
    if coverage and isinstance(result, TradingCycleResult):
        result = replace(result, execution_outcomes=[*result.execution_outcomes, {
            "disposition": "option_expiration_coverage", "version": "observed_expirations.v1",
            "extra_provider_requests": 0, "symbols": coverage,
        }])
    return result
