from __future__ import annotations

from typing import Any

from . import trading_cycle as legacy_cycle
from .phase1_engine_v2 import build_decision_card as build_decision_card_v2
from .position_monitor_v2 import run_position_monitor as run_position_monitor_v2


TradingCycleResult = legacy_cycle.TradingCycleResult
load_decision_cards = legacy_cycle.load_decision_cards


def run_trading_cycle(*, symbols: list[str], **kwargs: Any) -> TradingCycleResult:
    """Run the stable execution shell with the rebuilt trading brain.

    Capture, broker safety gates, journaling, and order submission remain in the
    proven cycle shell. This adapter swaps in the continuous-evidence decision
    engine and the duplicate-safe coordinated pair monitor for each run.
    """

    previous_builder = legacy_cycle.build_decision_card
    previous_monitor = legacy_cycle.run_position_monitor
    legacy_cycle.build_decision_card = build_decision_card_v2
    legacy_cycle.run_position_monitor = run_position_monitor_v2
    try:
        return legacy_cycle.run_trading_cycle(symbols=symbols, **kwargs)
    finally:
        legacy_cycle.build_decision_card = previous_builder
        legacy_cycle.run_position_monitor = previous_monitor
