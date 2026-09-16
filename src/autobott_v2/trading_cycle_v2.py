from __future__ import annotations

from types import FunctionType
from typing import Any

from . import trading_cycle as legacy_cycle
from .phase1_engine_v2 import build_decision_card as build_decision_card_v2
from .position_monitor_v2 import run_position_monitor as run_position_monitor_v2


TradingCycleResult = legacy_cycle.TradingCycleResult
load_decision_cards = legacy_cycle.load_decision_cards


def run_trading_cycle(*, symbols: list[str], **kwargs: Any) -> TradingCycleResult:
    """Run the execution shell with private, per-call engine bindings.

    The shell predates dependency injection. Copy its function namespace, not
    its source or runtime state: only the builder and monitor bindings differ.
    Journal helpers, locks, broker safety gates and durable stores retain their
    original identities. No shared module attribute is replaced, including
    while an overlapping call is running or an exception is being handled.
    """
    shell = legacy_cycle.run_trading_cycle
    if not isinstance(shell, FunctionType):
        raise TypeError("cycle_shell_must_be_python_function")
    namespace = {
        **shell.__globals__,
        "build_decision_card": build_decision_card_v2,
        "run_position_monitor": run_position_monitor_v2,
    }
    isolated_shell = FunctionType(
        shell.__code__, namespace, shell.__name__, shell.__defaults__, shell.__closure__
    )
    isolated_shell.__kwdefaults__ = shell.__kwdefaults__
    return isolated_shell(symbols=symbols, **kwargs)
