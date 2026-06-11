from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from .models import TradeDecision


def _json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    if isinstance(value, tuple):
        return [_json_safe(v) for v in value]
    return value


def serialize_decision(decision: TradeDecision) -> str:
    payload = _json_safe(decision.to_json_dict())
    return json.dumps(payload, sort_keys=True)
