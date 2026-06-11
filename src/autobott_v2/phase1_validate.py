from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .phase1_ledger import LearningLedger
from .phase1_snapshot_contract import SnapshotValidationError, validate_market_snapshot


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate one read-only Phase 1 options decision snapshot.")
    parser.add_argument("--snapshot", required=True, help="Path to a real market/options snapshot JSON file.")
    parser.add_argument("--ledger", help="Optional JSONL ledger path for the produced decision card.")
    args = parser.parse_args(argv)

    try:
        snapshot = _load_snapshot(Path(args.snapshot))
    except SnapshotValidationError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    record = _validation_record(snapshot, Path(args.snapshot))
    if args.ledger:
        LearningLedger(args.ledger).append(record)
    print(json.dumps(record, indent=2, sort_keys=True))
    return 0


def _load_snapshot(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    validate_market_snapshot(data)
    return data


def _validation_record(snapshot: dict[str, Any], snapshot_path: Path) -> dict[str, Any]:
    source = snapshot["source"]
    return {
        "schema_version": snapshot["schema_version"],
        "ticker": snapshot["ticker"],
        "timestamp": snapshot["timestamp"],
        "captured_at": snapshot["captured_at"],
        "snapshot_path": str(snapshot_path),
        "source": {
            "name": source["name"],
            "environment": source["environment"],
            "latency_assumption": source["latency_assumption"],
        },
        "validation_status": "SNAPSHOT_VALID",
        "forward_outcomes": {
            "after_5m": None,
            "after_15m": None,
            "after_30m": None,
            "after_1h": None,
        },
    }


if __name__ == "__main__":
    raise SystemExit(main())
