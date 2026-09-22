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
from datetime import timedelta
import hashlib
import json
import math
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
    if observation.end_basis != "fixed_duration":
        raise ValueError("bound_quality_protocol_requires_fixed_observation_duration")
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


def _underlying_response(row: Mapping[str, Any], case: Mapping[str, Any], *,
                         start, end, rules: EntryQualityRules,
                         quality: Mapping[str, Any]) -> dict[str, Any]:
    """Describe direct-underlying movement after fill without changing entry policy."""
    config = row.get("underlying_followthrough")
    base = {
        "schema_version": "primary_underlying_response.v1",
        "status": "not_recorded", "reason": None, "diagnostic": "not_available",
        "symbol": None, "direction": None, "valid_quote_count": 0,
        "baseline_basis": "first_valid_post_fill_underlying_midpoint",
        "baseline_mid": None, "final_directional_return_pct": None,
        "max_directional_return_pct": None, "min_directional_return_pct": None,
        "first_favorable_seconds": None, "observed_span_seconds": None,
        "causal_conclusion": False,
        "note": ("Descriptive paired evidence only; no minimum underlying-move threshold "
                 "was preregistered, so this does not prove signal or contract causality."),
    }
    if not isinstance(config, Mapping):
        return {**base, "reason": "underlying_followthrough_not_recorded"}
    if config.get("status") != "configured":
        return {**base, "status": config.get("status", "not_recorded"),
                "reason": config.get("reason", "underlying_followthrough_not_configured"),
                "symbol": config.get("signal_symbol"), "direction": config.get("direction")}
    symbol, direction = config.get("signal_symbol"), config.get("direction")
    if not isinstance(symbol, str) or direction not in {"bullish", "bearish"}:
        return {**base, "reason": "invalid_underlying_followthrough_identity"}

    observations = case.get("outcome_snapshots")
    if not isinstance(observations, list):
        return {**base, "status": "insufficient", "reason": "underlying_path_not_recorded",
                "symbol": symbol, "direction": direction}
    points: list[tuple[Any, Any, float]] = []
    issues: list[str] = []
    seen: dict[str, tuple[float, float]] = {}
    for observation in observations:
        if not isinstance(observation, Mapping):
            continue
        try:
            receipt = aware_utc(observation.get("timestamp"))
        except (ValueError, TypeError):
            issues.append("invalid_underlying_observation_timestamp")
            continue
        if receipt < start or receipt > end:
            continue
        chain = observation.get("underlying_chain")
        if not isinstance(chain, list):
            issues.append("underlying_chain_not_recorded")
            continue
        matches = [quote for quote in chain if isinstance(quote, Mapping)
                   and quote.get("symbol") == symbol]
        if not matches:
            if observation.get("underlying_data_issue"):
                issues.append(str(observation["underlying_data_issue"]))
            continue
        if len(matches) != 1:
            issues.append("ambiguous_underlying_quote")
            continue
        quote = matches[0]
        try:
            bid, ask = quote.get("bid"), quote.get("ask")
            if (isinstance(bid, bool) or isinstance(ask, bool)
                    or not isinstance(bid, (int, float)) or not isinstance(ask, (int, float))
                    or not math.isfinite(bid) or not math.isfinite(ask)
                    or bid <= 0 or ask <= 0 or ask < bid):
                raise ValueError("invalid_underlying_bid_ask")
            quote_time = aware_utc(quote.get("quote_timestamp"))
            age = (receipt - quote_time).total_seconds()
            if quote_time < start:
                raise ValueError("pre_fill_underlying_quote")
            if age < 0 or age > rules.max_quote_age_seconds:
                raise ValueError("future_or_stale_underlying_quote")
            identity = quote_time.isoformat()
            prices = (float(bid), float(ask))
            if identity in seen:
                if seen[identity] != prices:
                    raise ValueError("conflicting_same_time_underlying_quotes")
                continue
            seen[identity] = prices
            points.append((receipt, quote_time, (float(bid) + float(ask)) / 2.0))
        except (ValueError, TypeError, OverflowError) as exc:
            issues.append(str(exc))
    points.sort(key=lambda item: (item[1], item[0]))
    base.update(symbol=symbol, direction=direction, valid_quote_count=len(points),
                data_issues=sorted(set(issues)))
    if len(points) < 2:
        return {**base, "status": "insufficient", "reason": "fewer_than_two_valid_post_fill_underlying_quotes"}

    baseline = points[0][2]
    directional = []
    first_favorable = None
    for receipt, quote_time, midpoint in points:
        move = midpoint / baseline - 1.0
        value = move if direction == "bullish" else -move
        directional.append((receipt, quote_time, value))
        if first_favorable is None and value > 0:
            first_favorable = receipt
    values = [value for _, _, value in directional]
    max_move, min_move, final_move = max(values), min(values), values[-1]
    quality_status = quality.get("status")
    if quality_status == "pass":
        diagnostic = "option_opportunity_observed"
    elif quality_status == "fail" and max_move > 0:
        diagnostic = "underlying_moved_with_direction_option_opportunity_failed"
    elif quality_status == "fail":
        diagnostic = "underlying_never_moved_with_direction"
    else:
        diagnostic = "option_quality_unscorable"
    return {
        **base, "status": "observed", "reason": "paired_direct_underlying_quotes",
        "diagnostic": diagnostic, "baseline_mid": baseline,
        "final_directional_return_pct": final_move,
        "max_directional_return_pct": max_move,
        "min_directional_return_pct": min_move,
        "first_favorable_seconds": ((first_favorable - start).total_seconds()
                                    if first_favorable is not None else None),
        "observed_span_seconds": (points[-1][1] - points[0][1]).total_seconds(),
    }


def evaluate_completed_primary_watches(root: str | Path) -> dict[str, Any]:
    """Score complete broker-linked watches exactly once using their bound rules."""
    root = Path(root)
    summary: dict[str, Any] = {
        "checked": 0, "evaluated": 0, "already_evaluated": 0,
        "not_configured": 0, "not_ready": 0, "errors": [],
        "quality_statuses": {}, "underlying_diagnostics": {},
        "broker_reads": 0, "broker_writes": 0, "journal_writes": 0,
        "edge_established": False,
    }
    if not root.exists():
        return summary

    statuses: Counter[str] = Counter()
    diagnostics: Counter[str] = Counter()
    with _locked(root):
        for path in sorted(root.glob("*.json")):
            try:
                row = _read(path)
                summary["checked"] += 1
                if row.get("entry_quality_evaluation") is not None:
                    existing = row["entry_quality_evaluation"]
                    if (not isinstance(existing, Mapping)
                            or existing.get("schema_version") != "primary_runtime_entry_quality.v1"
                            or existing.get("evaluation_hash") != _digest(
                                {key: value for key, value in existing.items() if key != "evaluation_hash"})):
                        raise ValueError("primary_entry_quality_evaluation_integrity_mismatch")
                    summary["already_evaluated"] += 1
                    quality = existing.get("quality", {})
                    if isinstance(quality, Mapping) and isinstance(quality.get("status"), str):
                        statuses[quality["status"]] += 1
                    underlying = existing.get("underlying_response", {})
                    if isinstance(underlying, Mapping) and isinstance(underlying.get("diagnostic"), str):
                        diagnostics[underlying["diagnostic"]] += 1
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
                end = start + timedelta(seconds=rules.holding_seconds)

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
                underlying_response = _underlying_response(
                    row, case, start=start, end=end, rules=rules, quality=quality)

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
                    "underlying_response": underlying_response,
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
                diagnostics[underlying_response["diagnostic"]] += 1
            except Exception as exc:
                summary["errors"].append({
                    "watch_id": path.stem,
                    "reason": f"{type(exc).__name__}:{exc}",
                })
    summary["quality_statuses"] = dict(statuses)
    summary["underlying_diagnostics"] = dict(diagnostics)
    return summary
