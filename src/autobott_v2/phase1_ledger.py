from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .jsonl_retention import compact_jsonl_tail


class LearningLedger:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def append(self, record: Any) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as handle:
            if hasattr(record, "to_json_dict"):
                payload = record.to_json_dict()
            else:
                payload = record
            handle.write(json.dumps(payload, sort_keys=True) + "\n")
        compact_jsonl_tail(self.path)
