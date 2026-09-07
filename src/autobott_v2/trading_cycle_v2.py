from __future__ import annotations

from typing import Any

from . import trading_cycle as legacy_cycle
from .phase1_engine_v2 import build_decision_card as build_decision_card_v2


TradingCycleResult = legacy_cycle.TradingCycleResult
load_decision_cards = legacy_cycle.load_decision_cards


def run_trading_cycle(*, symbols: list[str], **kwargs: Any) -> TradingCycleResult:
    """Run the stable execution shell with the rebuilt direction brain.

    The legacy cycle module still owns capture, risk gates, journaling, pair
    selection, and paper order submission. This adapter replaces only the
    decision-card callable for the duration of the cycle, making the migration
    explicit without copying or forking the large execution shell.
    """

    previous_builder = legacy_cycle.build_decision_card
    legacy_cycle.build_decision_card = build_decision_card_v2
    try:
        return legacy_cycle.run_trading_cycle(symbols=symbols, **kwargs)
    finally:
        legacy_cycle.build_decision_card = previous_builder
