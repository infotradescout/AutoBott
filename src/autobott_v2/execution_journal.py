from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .execution_models import ExecutionOrder, RiskCheckResult, TradeIntent
from .runtime_paths import data_root


def execution_journal_path() -> Path:
    return data_root() / "execution" / "execution_orders.jsonl"


@dataclass(frozen=True)
class ExecutionJournalRecord:
    recorded_at: datetime
    event_type: str
    decision_id: str | None
    thesis_id: str | None
    payload: dict[str, Any]

    def to_json_dict(self) -> dict[str, Any]:
        return _json_safe(asdict(self))


def append_risk_check(intent: TradeIntent, risk_check: RiskCheckResult, *, journal_path: str | Path | None = None) -> Path:
    record = ExecutionJournalRecord(
        recorded_at=datetime.now(tz=UTC),
        event_type="risk_check",
        decision_id=intent.decision_id,
        thesis_id=intent.thesis_id,
        payload={
            "intent": _json_safe(asdict(intent)),
            "risk_check": _json_safe(asdict(risk_check)),
        },
    )
    return _append_record(record, journal_path=journal_path)


def append_order_submission(order: ExecutionOrder, *, journal_path: str | Path | None = None) -> Path:
    record = ExecutionJournalRecord(
        recorded_at=datetime.now(tz=UTC),
        event_type="order_submission",
        decision_id=order.intent.decision_id,
        thesis_id=order.intent.thesis_id,
        payload=_json_safe(asdict(order)),
    )
    return _append_record(record, journal_path=journal_path)


def load_execution_journal(*, journal_path: str | Path | None = None) -> list[dict[str, Any]]:
    path = Path(journal_path) if journal_path is not None else execution_journal_path()
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def _append_record(record: ExecutionJournalRecord, *, journal_path: str | Path | None = None) -> Path:
    path = Path(journal_path) if journal_path is not None else execution_journal_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record.to_json_dict(), sort_keys=True))
        handle.write("\n")
    return path


def _json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if hasattr(value, "value"):
        return value.value
    return value
