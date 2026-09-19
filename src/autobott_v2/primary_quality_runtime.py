"""Materialize actual primary-entry quality from completed paper watches.

This module never places, cancels, or changes an order. Quality rules are bound
before outcome collection and later read from the watch itself, so changing an
environment variable after the trade cannot rewrite the protocol used to score
that trade.
"""
from __future__ import annotations

from collections import Counter
from copy import deepcopy
from dataclasses import fields
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Mapping

from .bar_timing import aware_utc
from .entry_quality import EntryQualityRules, evaluate_entry_quality
from .primary_fill_linkage import linked_primary_fill
from .primary_followthrough import PrimaryObservationRules, _locked, _read, _write

_ENV = "AUTOBOTT_ENTRY_QUALITY_RULES_JSON"


def _digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, allow_nan=False,
                                     separators=(",", ":")).encode()).hexdigest()


def configured_entry_quality_rules() -> EntryQualityRules | None:
    """Load an explicit runtime protocol. There are intentionally no defaults."""
    raw = os.getenv(_ENV)
    if raw is None or not raw.strip():
        return None
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("invalid_entry_quality_rules_json") from exc
    if not isinstance(payload, dict):
        raise ValueError("entry_quality_rules_object_required")
    expected = {field.name for field in fields(EntryQualityRules)}
    if set(payload) != expected:
        raise ValueError("exact_entry_quality_rule_fields_required")
    return EntryQualityRules(**payload)


def _protocol_from_watch(row: Mapping[str, Any]) -> EntryQualityRules | None:
    protocol = row.get("quality_protocol")
    if protocol is None:
        return None
    if not isinstance(protocol, Mapping) or protocol.get("schema_version") != "entry_quality_rules.v1":
        raise ValueError("invalid_bound_entry_quality_protocol")
    rules_payload = protocol.get("rules")
    if not isinstance(rules_payload, Mapping):
        raise ValueError("bound_entry_quality_rules_required")
    rules = EntryQualityRules(**dict(rules_payload))
    if protocol.get("rules_hash") != rules.config_hash:
        raise ValueError("bound_entry_quality_rules_hash_mismatch")
    observation = PrimaryObservationRules(**row["rules"])
    if rules.holding_seconds > observation.window_seconds:
        raise ValueError("bound_quality_window_exceeds_observation_window")
    return rules


def _feed_issue(case: Mapping[str, Any], *, start, end) -> str | None:
    refresh = case.get("refresh")
    if not isinstance(refresh, Mapping):
        return "recorded_refresh_required"
    expected = refresh.get("options_feed")
    if not isinstance(expected, str) or not expected.strip():
        return "recorded_options_feed_required"
    observations = case.get("outcome_snapshots")
    if not isinstance(observations, list):
        return "option_path_not_recorded"
    for observation in observations:
        if not isinstance(observation, Mapping):
            return "invalid_outcome_observation"
        try:
            stamp = aware_utc(observation.get("timestamp"))
        except (ValueError, TypeError):
            return "invalid_outcome_observation_timestamp"
        if start <= stamp <= end:
            source = observation.get("source")
            if not isinstance(source, Mapping) or source.get("options_feed") != expected:
                return "mixed_or_unknown_outcome_feed"
    return None


def evaluate_completed_primary_watches(root: str | Path) -> dict[str, Any]:
    """Score complete broker-linked watches exactly once using their bound rules."""
    root = Path(root)
    summary: dict[str, Any] = {
        "checked": 0, "evaluated": 0, "already_evaluated": 0,
        "not_configured": 0, "not_ready": 0, "errors": [],
        "quality_statuses": {}, "broker_reads": 0, "broker_writes": 0, "journal_writes": 0,
        "edge_established": False,
    }
    if not root.exists():
        return summary

    statuses: Counter[str] = Counter()
    with _locked(root):
        for path in sorted(root.glob("*.json")):
            try:
                row = _read(path)
                summary["checked"] += 1
                if row.get("entry_quality_evaluation") is not None:
                    summary["already_evaluated"] += 1
                    quality = row["entry_quality_evaluation"].get("quality", {})
                    if isinstance(quality, Mapping) and isinstance(quality.get("status"), str):
                        statuses[quality["status"]] += 1
                    continue

                rules = _protocol_from_watch(row)
                if rules is None:
                    summary["not_configured"] += 1
                    continue
                if row.get("fill_capture_status") != "filled" or row.get("status") != "window_closed":
                    summary["not_ready"] += 1
                    continue
                if row.get("observation_window_basis") != "broker_recorded_primary_fill":
                    raise ValueError("filled_watch_not_anchored_to_primary_fill")

                case = deepcopy(row.get("case"))
                if not isinstance(case, dict):
                    raise ValueError("primary_watch_case_required")
                symbol = row.get("primary_option_symbol")
                linked = linked_primary_fill(case, symbol)
                fill = linked["fill"]
                start = aware_utc(fill["timestamp"])
                if aware_utc(row.get("fill_window_start")) != start:
                    raise ValueError("primary_fill_window_start_mismatch")
                end = start + __import__("datetime").timedelta(seconds=rules.holding_seconds)

                entry = {
                    "decision_id": row.get("decision_id"),
                    "ticker": case.get("snapshot", {}).get("ticker"),
                    "timestamp": start.isoformat(),
                    "selected_contract": {"option_symbol": symbol},
                    "leg_role": "primary",
                    "filled": True,
                    "entry_fill_price": fill["price"],
                    "entry_fill_model": "account_scoped_submission_and_broker_order",
                }
                quality = evaluate_entry_quality(
                    entry, deepcopy(case.get("outcome_snapshots", [])), rules,
                    is_primary=True, evidence_kind="broker_recorded_fill",
                )
                issue = _feed_issue(case, start=start, end=end)
                if issue is not None:
                    quality["status"] = "unscorable"
                    quality["passed"] = None
                    quality["reason"] = issue
                    quality["data_issues"] = sorted(set([*quality.get("data_issues", []), issue]))

                evaluation = {
                    "schema_version": "primary_runtime_entry_quality.v1",
                    "watch_id": row["watch_id"],
                    "decision_id": row.get("decision_id"),
                    "rules_hash": rules.config_hash,
                    "account_scope": linked["account_scope"],
                    "broker_order_id": fill["broker_order_id"],
                    "fill_timestamp": fill["timestamp"],
                    "fill_price": fill["price"],
                    "quality": quality,
                    "measurement_basis": "broker_fill_then_sampled_option_bid_net_of_declared_fees",
                    "exit_policy": "not_executed_primary_opportunity_only",
                    "source_authenticity_verified": False,
                    "edge_established": False,
                }
                evaluation["evaluation_hash"] = _digest(evaluation)
                row["entry_quality_evaluation"] = evaluation
                _write(path, row, PrimaryObservationRules(**row["rules"]))
                summary["evaluated"] += 1
                statuses[quality["status"]] += 1
            except Exception as exc:
                summary["errors"].append({
                    "watch_id": path.stem,
                    "reason": f"{type(exc).__name__}:{exc}",
                })
    summary["quality_statuses"] = dict(statuses)
    return summary
