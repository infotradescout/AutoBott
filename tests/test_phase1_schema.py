from __future__ import annotations

import json
from pathlib import Path


def test_phase1_decision_card_schema_exists_and_names_required_statuses() -> None:
    schema = json.loads(Path("schemas/phase1_decision_card.schema.json").read_text(encoding="utf-8"))

    statuses = schema["properties"]["decision"]["enum"]
    assert "TRADE_CANDIDATE" in statuses
    assert "NO_TRADE" in statuses
    assert "BLOCKED_BY_REGIME" in statuses
    assert "BLOCKED_BY_VOLATILITY" in statuses
    assert "BLOCKED_BY_SPREAD" in statuses
    assert "BLOCKED_BY_RISK" in statuses


def test_phase1_market_snapshot_schema_exists() -> None:
    schema = json.loads(Path("schemas/phase1_market_snapshot.schema.json").read_text(encoding="utf-8"))

    assert "underlying_quote" in schema["required"]
    assert "option_chain" in schema["required"]
    assert "iv_history" in schema["required"]
    assert schema["properties"]["market_bars"]["minItems"] == 30
