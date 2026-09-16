"""Diagnostic capture for the existing synthetic trading-cycle regressions."""
from dataclasses import asdict
import json
import pytest


@pytest.fixture(autouse=True)
def synthetic_cycle_failure_evidence(request, monkeypatch):
    if request.node.path.name != "test_trading_cycle.py":
        return
    import autobott_v2.trading_cycle as cycle
    original = cycle.build_decision_card

    def capture(decision_input, rules=None):
        result = original(decision_input, rules)
        print("SYNTHETIC_CYCLE_DECISION " + json.dumps({
            "timestamp": decision_input.timestamp.isoformat(),
            "market_bars": len(decision_input.market_bars),
            "cycle_profile": asdict(decision_input.cycle_profile),
            "rules": asdict(rules) if rules is not None else None,
            "decision": asdict(result),
        }, default=str, sort_keys=True))
        return result

    monkeypatch.setattr(cycle, "build_decision_card", capture)
