from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from .phase1_snapshot_contract import validate_market_snapshot


@dataclass(frozen=True)
class SnapshotCorpusQualityRules:
    min_snapshot_coverage: float = 0.90
    min_option_quote_coverage: float = 0.80
    max_stale_quote_rate: float = 0.10
    stale_quote_age_seconds: int = 120
    max_major_missing_time_blocks: int = 0
    major_missing_block_threshold_intervals: int = 5


def load_snapshot_corpus(
    corpus_root: str | Path,
    *,
    symbols: list[str] | None = None,
    start_date: str | date | None = None,
    end_date: str | date | None = None,
    quality_rules: SnapshotCorpusQualityRules | None = None,
) -> dict[str, Any]:
    rules = quality_rules or SnapshotCorpusQualityRules()
    root = Path(corpus_root)
    selected_symbols = {symbol.upper() for symbol in symbols} if symbols else None
    start = _coerce_date(start_date)
    end = _coerce_date(end_date)
    selected_days: list[dict[str, Any]] = []
    aggregate_flags: set[str] = set()
    aggregate_missing_intervals: list[str] = []
    snapshot_schema_versions: set[str] = set()
    decision_schema_versions: set[str] = set()
    corpus_types: set[str] = set()
    snapshot_paths: list[str] = []
    total_snapshots = 0
    total_option_quote_files = 0
    total_contract_quotes = 0
    total_stale_quotes = 0
    major_missing_time_blocks = 0
    total_expected_intervals = 0

    for manifest_path in sorted(root.rglob("manifest.json")):
        manifest = _read_json(manifest_path)
        if manifest.get("schema_version") != "phase1_snapshot_day_manifest.v1":
            continue
        trading_date = date.fromisoformat(manifest["trading_date"])
        symbol = str(manifest["symbol"]).upper()
        if selected_symbols is not None and symbol not in selected_symbols:
            continue
        if start is not None and trading_date < start:
            continue
        if end is not None and trading_date > end:
            continue

        symbol_dir = manifest_path.parent
        snapshots_dir = symbol_dir / "snapshots"
        option_quotes_dir = symbol_dir / "option_quotes"
        day_snapshot_paths = sorted(snapshots_dir.glob("*.json"))
        day_option_quote_paths = sorted(option_quotes_dir.glob("*.json"))
        day_snapshots = [_read_snapshot(path) for path in day_snapshot_paths]
        day_timestamps = sorted(_parse_datetime(snapshot["timestamp"]) for snapshot in day_snapshots)
        detected_missing_intervals, detected_major_blocks = _detect_missing_intervals(
            day_timestamps,
            int(manifest.get("capture_interval_seconds", 60)),
            rules.major_missing_block_threshold_intervals,
        )
        expected_intervals = _expected_intervals(manifest)
        manifest_missing_intervals = [str(item) for item in manifest.get("missing_intervals", [])]
        day_flags = set(str(item) for item in manifest.get("data_quality_flags", []))
        if len(day_snapshot_paths) != int(manifest.get("snapshots_captured", 0)):
            day_flags.add("snapshot_count_mismatch")
        if len(day_option_quote_paths) != int(manifest.get("option_quotes_captured", 0)):
            day_flags.add("option_quote_count_mismatch")
        if manifest_missing_intervals != [item.split("/")[-1] for item in detected_missing_intervals]:
            day_flags.add("manifest_missing_interval_mismatch")
        if detected_missing_intervals:
            day_flags.add("missing_intervals_detected")

        snapshot_schema_versions.add(str(manifest.get("snapshot_schema_version")))
        decision_schema_versions.add(str(manifest.get("decision_schema_version")))
        corpus_types.add(str(manifest.get("corpus_type", "historical_replay")))
        aggregate_missing_intervals.extend(detected_missing_intervals)
        snapshot_paths.extend(str(path) for path in day_snapshot_paths)
        total_snapshots += len(day_snapshot_paths)
        total_option_quote_files += len(day_option_quote_paths)
        major_missing_time_blocks += detected_major_blocks
        total_expected_intervals += expected_intervals

        stale_quotes = 0
        contract_quotes = 0
        actual_snapshot_schema_versions = {snapshot["schema_version"] for snapshot in day_snapshots}
        if len(actual_snapshot_schema_versions) > 1:
            day_flags.add("mixed_snapshot_schema_versions_within_day")
        for snapshot in day_snapshots:
            snapshot_time = _parse_datetime(snapshot["timestamp"])
            for contract in snapshot.get("option_chain", []):
                contract_quotes += 1
                quote_time = _parse_datetime(contract["quote_timestamp"])
                age_seconds = max(0, int((snapshot_time - quote_time).total_seconds()))
                if age_seconds > rules.stale_quote_age_seconds:
                    stale_quotes += 1

        total_contract_quotes += contract_quotes
        total_stale_quotes += stale_quotes
        aggregate_flags.update(day_flags)
        selected_days.append(
            {
                "symbol": symbol,
                "trading_date": trading_date.isoformat(),
                "manifest_path": str(manifest_path),
                "snapshot_paths": [str(path) for path in day_snapshot_paths],
                "option_quote_paths": [str(path) for path in day_option_quote_paths],
                "capture_interval_seconds": int(manifest.get("capture_interval_seconds", 60)),
                "expected_intervals": expected_intervals,
                "captured_intervals": len(day_snapshot_paths),
                "snapshot_coverage_pct": round(len(day_snapshot_paths) / expected_intervals, 4) if expected_intervals else 0.0,
                "data_quality_flags": sorted(day_flags),
                "missing_intervals": detected_missing_intervals,
                "stale_quote_rate": round(stale_quotes / contract_quotes, 4) if contract_quotes else 0.0,
            }
        )

    if not selected_days:
        raise ValueError("snapshot_corpus_empty")

    if len(snapshot_schema_versions) != 1 or len(decision_schema_versions) != 1:
        raise ValueError("mixed_schema_versions")
    if len(corpus_types) != 1:
        raise ValueError("mixed_corpus_types")

    blocking_reasons: list[str] = []
    snapshot_coverage_pct = round(total_snapshots / total_expected_intervals, 4) if total_expected_intervals else 0.0
    option_quote_coverage = round(total_option_quote_files / total_snapshots, 4) if total_snapshots else 0.0
    stale_quote_rate = round(total_stale_quotes / total_contract_quotes, 4) if total_contract_quotes else 0.0
    if snapshot_coverage_pct < rules.min_snapshot_coverage:
        blocking_reasons.append("snapshot_coverage_below_threshold")
    if major_missing_time_blocks > rules.max_major_missing_time_blocks:
        blocking_reasons.append("major_missing_time_blocks_detected")
    if option_quote_coverage < rules.min_option_quote_coverage:
        blocking_reasons.append("option_quote_coverage_below_threshold")
    if stale_quote_rate > rules.max_stale_quote_rate:
        blocking_reasons.append("stale_quote_rate_above_threshold")

    return {
        "schema_version": "phase1_snapshot_corpus.v1",
        "corpus_root": str(root),
        "symbols": sorted({day["symbol"] for day in selected_days}),
        "start_date": min(day["trading_date"] for day in selected_days),
        "end_date": max(day["trading_date"] for day in selected_days),
        "snapshot_schema_version": next(iter(snapshot_schema_versions)),
        "decision_schema_version": next(iter(decision_schema_versions)),
        "corpus_type": next(iter(corpus_types)),
        "days": selected_days,
        "snapshot_paths": snapshot_paths,
        "quality": {
            "campaign_ready": not blocking_reasons,
            "blocking_reasons": blocking_reasons,
            "corpus_type": next(iter(corpus_types)),
            "symbols": sorted({day["symbol"] for day in selected_days}),
            "trading_days": len(selected_days),
            "expected_intervals": total_expected_intervals,
            "captured_intervals": total_snapshots,
            "missing_interval_count": len(aggregate_missing_intervals),
            "snapshot_coverage_pct": snapshot_coverage_pct,
            "option_quote_coverage_pct": option_quote_coverage,
            "stale_quote_rate": stale_quote_rate,
            "schema_versions": {
                "snapshot": sorted(snapshot_schema_versions),
                "decision": sorted(decision_schema_versions),
            },
            "quality_flags": sorted(aggregate_flags),
            "data_quality_flags": sorted(aggregate_flags),
            "missing_intervals": aggregate_missing_intervals,
            "major_missing_time_blocks": major_missing_time_blocks,
            "option_quote_coverage": option_quote_coverage,
            "stale_quote_rate": stale_quote_rate,
            "total_snapshots": total_snapshots,
            "total_option_quote_files": total_option_quote_files,
            "total_contract_quotes": total_contract_quotes,
        },
    }


def _coerce_date(value: str | date | None) -> date | None:
    if value is None or isinstance(value, date):
        return value
    return date.fromisoformat(value)


def _expected_intervals(manifest: dict[str, Any]) -> int:
    start = datetime.strptime(str(manifest["capture_start"]), "%H:%M:%S")
    end = datetime.strptime(str(manifest["capture_end"]), "%H:%M:%S")
    interval_seconds = int(manifest.get("capture_interval_seconds", 60))
    if interval_seconds <= 0:
        return 0
    span_seconds = int((end - start).total_seconds())
    return max(1, (span_seconds // interval_seconds) + 1)


def _detect_missing_intervals(
    timestamps: list[datetime],
    interval_seconds: int,
    major_block_threshold_intervals: int,
) -> tuple[list[str], int]:
    if len(timestamps) < 2:
        return [], 0
    missing: list[str] = []
    major_blocks = 0
    unique_timestamps = sorted(set(timestamps))
    expected_delta = timedelta(seconds=interval_seconds)
    for previous, current in zip(unique_timestamps, unique_timestamps[1:]):
        current_block = 0
        cursor = previous + expected_delta
        while cursor < current:
            current_block += 1
            missing.append(f"{cursor.date().isoformat()}/{cursor.time().replace(tzinfo=None).isoformat()}")
            cursor += expected_delta
        if current_block >= major_block_threshold_intervals:
            major_blocks += 1
    return missing, major_blocks


def _read_snapshot(path: Path) -> dict[str, Any]:
    payload = _read_json(path)
    validate_market_snapshot(payload)
    return payload


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _parse_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))
