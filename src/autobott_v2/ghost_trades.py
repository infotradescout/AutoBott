from __future__ import annotations

import json
from dataclasses import asdict
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from .phase1_models import DecisionCard, DecisionInput
from .runtime_paths import data_root


def ghost_trade_journal_path() -> Path:
    return data_root() / "execution" / "ghost_trades.jsonl"


def append_ghost_trade(
    decision: DecisionCard,
    *,
    reason: str,
    max_real_cost: float,
    journal_path: str | Path | None = None,
) -> dict[str, Any]:
    if decision.selected_contract is None:
        raise ValueError("ghost_trade_requires_selected_contract")
    contract = decision.selected_contract
    row = {
        "schema_version": "ghost_trade.v1",
        "event_type": "ghost_entry",
        "recorded_at": datetime.now(tz=UTC).isoformat(),
        "decision_id": decision.decision_id,
        "ticker": decision.ticker,
        "trade_setup": decision.trade_setup.value,
        "execution_layer": decision.execution_layer.value,
        "option_symbol": contract.option_symbol,
        "option_type": contract.option_type.value,
        "entry_mid": contract.mid,
        "notional": round(contract.mid * 100, 2),
        "max_real_cost": max_real_cost,
        "reason": reason,
        "reason_codes": list(decision.reason_codes),
        "selected_contract": _json_safe(asdict(contract)),
    }
    _append_row(row, journal_path=journal_path)
    return row


def observe_ghost_trades(
    decision_input: DecisionInput,
    *,
    journal_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    rows = load_ghost_trades(journal_path=journal_path)
    open_entries = [
        row
        for row in rows
        if row.get("event_type") == "ghost_entry"
        and row.get("ticker") == decision_input.ticker
        and not _has_observation(rows, row.get("decision_id"), decision_input.timestamp.isoformat())
    ]
    quotes = {contract.option_symbol: contract.mid for contract in decision_input.option_chain}
    observations: list[dict[str, Any]] = []
    for entry in open_entries:
        current_mid = quotes.get(str(entry.get("option_symbol")))
        entry_mid = _float_or_none(entry.get("entry_mid"))
        if current_mid is None or entry_mid is None or entry_mid <= 0:
            continue
        pnl = round((current_mid - entry_mid) * 100, 2)
        return_pct = round((current_mid - entry_mid) / entry_mid, 4)
        observation = {
            "schema_version": "ghost_trade.v1",
            "event_type": "ghost_observation",
            "recorded_at": datetime.now(tz=UTC).isoformat(),
            "decision_id": entry.get("decision_id"),
            "ticker": entry.get("ticker"),
            "option_symbol": entry.get("option_symbol"),
            "entry_mid": entry_mid,
            "current_mid": round(current_mid, 4),
            "pnl": pnl,
            "return_pct": return_pct,
            "result": "winner" if pnl > 0 else "loser" if pnl < 0 else "flat",
            "observed_at": decision_input.timestamp.isoformat(),
        }
        _append_row(observation, journal_path=journal_path)
        observations.append(observation)
    return observations


def load_ghost_trades(*, journal_path: str | Path | None = None) -> list[dict[str, Any]]:
    path = Path(journal_path) if journal_path is not None else ghost_trade_journal_path()
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _append_row(row: dict[str, Any], *, journal_path: str | Path | None = None) -> Path:
    path = Path(journal_path) if journal_path is not None else ghost_trade_journal_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, sort_keys=True))
        handle.write("\n")
    return path


def _has_observation(rows: list[dict[str, Any]], decision_id: Any, observed_at: str) -> bool:
    return any(
        row.get("event_type") == "ghost_observation"
        and row.get("decision_id") == decision_id
        and row.get("observed_at") == observed_at
        for row in rows
    )


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if hasattr(value, "value"):
        return value.value
    return value
