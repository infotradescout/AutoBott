from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

from .phase1_engine import build_decision_card
from .phase1_ledger import LearningLedger
from .phase1_models import CycleProfile, CycleStatus, DecisionInput, MarketBar, MarketContext, OptionContractSnapshot, OptionType
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
    decision_card = build_decision_card(_decision_input_from_snapshot(snapshot))
    record = _evaluation_record(snapshot, Path(args.snapshot), decision_card.to_json_dict())
    if args.ledger:
        LearningLedger(args.ledger).append(record)
    print(json.dumps(record, indent=2, sort_keys=True))
    return 0


def _load_snapshot(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    validate_market_snapshot(data)
    return data


def _evaluation_record(snapshot: dict[str, Any], snapshot_path: Path, decision_card: dict[str, Any]) -> dict[str, Any]:
    source = snapshot["source"]
    return {
        "snapshot_schema_version": snapshot["schema_version"],
        "captured_at": snapshot["captured_at"],
        "snapshot_path": str(snapshot_path),
        "source": {
            "name": source["name"],
            "environment": source["environment"],
            "latency_assumption": source["latency_assumption"],
        },
        "validation_status": "SNAPSHOT_VALID",
        **decision_card,
    }


def _decision_input_from_snapshot(snapshot: dict[str, Any]) -> DecisionInput:
    return DecisionInput(
        ticker=snapshot["ticker"],
        timestamp=_parse_datetime(snapshot["timestamp"]),
        market_bars=[_market_bar_from_payload(bar) for bar in snapshot["market_bars"]],
        option_chain=[_contract_from_payload(contract) for contract in snapshot["option_chain"]],
        context=MarketContext(
            spy_bars=[_market_bar_from_payload(bar) for bar in snapshot["context"]["spy_bars"]],
            qqq_bars=[_market_bar_from_payload(bar) for bar in snapshot["context"]["qqq_bars"]],
            vix_bars=[_market_bar_from_payload(bar) for bar in snapshot["context"]["vix_bars"]],
            blackout_event=snapshot["context"]["blackout_event"],
            event_labels=list(snapshot["context"]["event_labels"]),
        ),
        iv_history=list(snapshot["iv_history"]),
        cycle_profile=_cycle_profile_from_snapshot(snapshot.get("cycle_profile", {})),
    )


def _market_bar_from_payload(payload: dict[str, Any]) -> MarketBar:
    return MarketBar(
        timestamp=_parse_datetime(payload["timestamp"]),
        open=payload["open"],
        high=payload["high"],
        low=payload["low"],
        close=payload["close"],
        volume=payload["volume"],
    )


def _contract_from_payload(payload: dict[str, Any]) -> OptionContractSnapshot:
    return OptionContractSnapshot(
        option_symbol=payload["option_symbol"],
        underlying=payload["underlying"],
        expiration=_parse_date(payload["expiration"]),
        strike=payload["strike"],
        option_type=OptionType(payload["option_type"]),
        bid=payload["bid"],
        ask=payload["ask"],
        last=payload["last"],
        volume=payload["volume"],
        open_interest=payload["open_interest"],
        delta=payload["delta"],
        theta=payload["theta"],
        vega=payload["vega"],
        implied_volatility=payload["implied_volatility"],
    )


def _parse_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _cycle_profile_from_snapshot(payload: dict[str, Any]) -> CycleProfile:
    return CycleProfile(
        median_valley_to_peak_bars=payload.get("median_valley_to_peak_bars"),
        median_peak_to_valley_bars=payload.get("median_peak_to_valley_bars"),
        bars_since_last_valley=payload.get("bars_since_last_valley"),
        bars_since_last_peak=payload.get("bars_since_last_peak"),
        expected_holding_days=payload.get("expected_holding_days"),
        cycle_confidence=CycleStatus(payload.get("cycle_confidence", CycleStatus.UNKNOWN.value)),
        last_pivot_type=payload.get("last_pivot_type", "unknown"),
    )


if __name__ == "__main__":
    raise SystemExit(main())
