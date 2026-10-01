"""Threshold-free metrics for session-close primary-entry development watches.

Development rows are intentionally descriptive. They may be used to choose a
future protocol, so they must never be represented as holdout results, passes,
fails, realized exits, or evidence that an entry edge exists.
"""
from __future__ import annotations

from copy import deepcopy
import hashlib
import json
import math
from pathlib import Path
from statistics import median
from typing import Any, Mapping

from .bar_timing import aware_utc
from .primary_fill_linkage import linked_primary_fill
from .primary_followthrough import PrimaryObservationRules, _locked, _read, _write


def _digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, allow_nan=False, separators=(",", ":")).encode()
    ).hexdigest()


def _finite_number(value: Any, *, positive: bool = False, nonnegative: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError("finite_number_required")
    result = float(value)
    if positive and result <= 0:
        raise ValueError("positive_number_required")
    if nonnegative and result < 0:
        raise ValueError("nonnegative_number_required")
    return result


def _option_points(row: Mapping[str, Any], case: Mapping[str, Any], *, start, end, fill_price: float):
    symbol = row["primary_option_symbol"]
    points = []
    issues: list[str] = []
    feeds: set[str] = set()
    seen_quotes: dict[str, tuple[float, float]] = {}

    for observation in case.get("outcome_snapshots", []):
        if not isinstance(observation, Mapping):
            issues.append("invalid_outcome_observation")
            continue
        try:
            receipt = aware_utc(observation.get("timestamp"))
        except (ValueError, TypeError):
            issues.append("invalid_outcome_observation_timestamp")
            continue
        if receipt < start or receipt > end:
            continue
        source = observation.get("source")
        if isinstance(source, Mapping) and isinstance(source.get("options_feed"), str):
            feeds.add(source["options_feed"])
        else:
            issues.append("options_feed_not_recorded")
        chain = observation.get("option_chain")
        if not isinstance(chain, list):
            issues.append("option_chain_not_recorded")
            continue
        matches = [q for q in chain if isinstance(q, Mapping) and q.get("option_symbol") == symbol]
        if len(matches) != 1:
            issues.append("missing_or_ambiguous_primary_quote")
            continue
        quote = matches[0]
        try:
            bid = _finite_number(quote.get("bid"), nonnegative=True)
            ask = _finite_number(quote.get("ask"), positive=True)
            if ask < bid:
                raise ValueError("crossed_primary_quote")
            quote_time = aware_utc(quote.get("quote_timestamp"))
            if quote_time < start:
                raise ValueError("pre_fill_primary_quote")
            if quote_time > receipt:
                raise ValueError("future_primary_quote")
            identity = quote_time.isoformat()
            prices = (bid, ask)
            if identity in seen_quotes:
                if seen_quotes[identity] != prices:
                    raise ValueError("conflicting_same_time_primary_quotes")
                continue
            seen_quotes[identity] = prices
            midpoint = (bid + ask) / 2.0
            spread_pct = (ask - bid) / midpoint if midpoint > 0 else None
            points.append({
                "receipt": receipt,
                "quote_time": quote_time,
                "bid": bid,
                "ask": ask,
                "gross_bid_return_pct": bid / fill_price - 1.0,
                "quote_age_seconds": (receipt - quote_time).total_seconds(),
                "spread_pct_of_mid": spread_pct,
            })
        except (ValueError, TypeError, OverflowError) as exc:
            issues.append(str(exc))
    points.sort(key=lambda point: (point["quote_time"], point["receipt"]))
    return points, sorted(set(issues)), sorted(feeds)


def _option_metrics(points, *, start, end, fill_price: float) -> dict[str, Any]:
    base = {
        "entry_fill_price": fill_price,
        "valid_quote_count": len(points),
        "session_seconds_after_fill": (end - start).total_seconds(),
        "first_quote_seconds": None,
        "last_quote_seconds": None,
        "last_quote_seconds_before_window_end": None,
        "max_observation_gap_seconds": None,
        "max_quote_age_seconds": None,
        "median_quote_age_seconds": None,
        "median_spread_pct_of_mid": None,
        "max_gross_bid_return_pct": None,
        "min_gross_bid_return_pct": None,
        "final_observed_gross_bid_return_pct": None,
        "seconds_to_max_gross_bid_return": None,
        "seconds_to_min_gross_bid_return": None,
        "adverse_before_max_favorable_pct": None,
        "positive_quote_fraction": None,
    }
    if not points:
        return base

    receipts = [point["receipt"] for point in points]
    quote_ages = [point["quote_age_seconds"] for point in points]
    spreads = [point["spread_pct_of_mid"] for point in points
               if point["spread_pct_of_mid"] is not None]
    returns = [point["gross_bid_return_pct"] for point in points]
    peak_index = max(range(len(points)), key=lambda i: returns[i])
    trough_index = min(range(len(points)), key=lambda i: returns[i])
    boundaries = [start, *receipts, end]
    gaps = [(b - a).total_seconds() for a, b in zip(boundaries, boundaries[1:])]
    positive = sum(value > 0 for value in returns)

    return {
        **base,
        "first_quote_seconds": (receipts[0] - start).total_seconds(),
        "last_quote_seconds": (receipts[-1] - start).total_seconds(),
        "last_quote_seconds_before_window_end": (end - receipts[-1]).total_seconds(),
        "max_observation_gap_seconds": max(gaps) if gaps else None,
        "max_quote_age_seconds": max(quote_ages),
        "median_quote_age_seconds": median(quote_ages),
        "median_spread_pct_of_mid": median(spreads) if spreads else None,
        "max_gross_bid_return_pct": returns[peak_index],
        "min_gross_bid_return_pct": returns[trough_index],
        "final_observed_gross_bid_return_pct": returns[-1],
        "seconds_to_max_gross_bid_return": (receipts[peak_index] - start).total_seconds(),
        "seconds_to_min_gross_bid_return": (receipts[trough_index] - start).total_seconds(),
        "adverse_before_max_favorable_pct": min([0.0, *returns[:peak_index + 1]]),
        "positive_quote_fraction": positive / len(returns),
    }


def _underlying_metrics(row: Mapping[str, Any], case: Mapping[str, Any], *, start, end) -> dict[str, Any]:
    config = row.get("underlying_followthrough")
    base = {
        "status": "not_recorded",
        "reason": None,
        "symbol": None,
        "direction": None,
        "valid_quote_count": 0,
        "baseline_mid": None,
        "max_directional_return_pct": None,
        "min_directional_return_pct": None,
        "final_directional_return_pct": None,
        "seconds_to_max_directional_return": None,
        "seconds_to_min_directional_return": None,
        "data_issues": [],
    }
    if not isinstance(config, Mapping):
        return {**base, "reason": "underlying_followthrough_not_recorded"}
    if config.get("status") != "configured":
        return {**base, "status": config.get("status", "not_recorded"),
                "reason": config.get("reason"), "symbol": config.get("signal_symbol"),
                "direction": config.get("direction")}

    symbol = config.get("signal_symbol")
    direction = config.get("direction")
    if not isinstance(symbol, str) or direction not in {"bullish", "bearish"}:
        return {**base, "reason": "invalid_underlying_identity"}

    points = []
    issues: list[str] = []
    seen: dict[str, tuple[float, float]] = {}
    for observation in case.get("outcome_snapshots", []):
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
        matches = [q for q in chain if isinstance(q, Mapping) and q.get("symbol") == symbol]
        if len(matches) != 1:
            if observation.get("underlying_data_issue"):
                issues.append(str(observation["underlying_data_issue"]))
            else:
                issues.append("missing_or_ambiguous_underlying_quote")
            continue
        quote = matches[0]
        try:
            bid = _finite_number(quote.get("bid"), positive=True)
            ask = _finite_number(quote.get("ask"), positive=True)
            if ask < bid:
                raise ValueError("crossed_underlying_quote")
            quote_time = aware_utc(quote.get("quote_timestamp"))
            if quote_time < start:
                raise ValueError("pre_fill_underlying_quote")
            if quote_time > receipt:
                raise ValueError("future_underlying_quote")
            identity = quote_time.isoformat()
            prices = (bid, ask)
            if identity in seen:
                if seen[identity] != prices:
                    raise ValueError("conflicting_same_time_underlying_quotes")
                continue
            seen[identity] = prices
            points.append((receipt, quote_time, (bid + ask) / 2.0))
        except (ValueError, TypeError, OverflowError) as exc:
            issues.append(str(exc))
    points.sort(key=lambda item: (item[1], item[0]))
    if not points:
        return {**base, "status": "insufficient", "reason": "no_valid_post_fill_underlying_quotes",
                "symbol": symbol, "direction": direction, "data_issues": sorted(set(issues))}

    baseline = points[0][2]
    values = []
    for receipt, quote_time, midpoint in points:
        raw = midpoint / baseline - 1.0
        values.append(raw if direction == "bullish" else -raw)
    peak = max(range(len(values)), key=lambda i: values[i])
    trough = min(range(len(values)), key=lambda i: values[i])
    return {
        **base,
        "status": "observed",
        "reason": "direct_underlying_post_fill_midpoints",
        "symbol": symbol,
        "direction": direction,
        "valid_quote_count": len(points),
        "baseline_mid": baseline,
        "max_directional_return_pct": values[peak],
        "min_directional_return_pct": values[trough],
        "final_directional_return_pct": values[-1],
        "seconds_to_max_directional_return": (points[peak][0] - start).total_seconds(),
        "seconds_to_min_directional_return": (points[trough][0] - start).total_seconds(),
        "data_issues": sorted(set(issues)),
    }


def materialize_primary_development_metrics(root: str | Path) -> dict[str, Any]:
    root = Path(root)
    summary = {
        "checked": 0, "materialized": 0, "already_materialized": 0,
        "not_development": 0, "not_ready": 0, "errors": [],
        "passes": None, "fails": None, "edge_established": False,
        "eligible_for_holdout": False, "broker_reads": 0, "broker_writes": 0,
        "journal_writes": 0,
    }
    if not root.exists():
        return summary

    with _locked(root):
        for path in sorted(root.glob("*.json")):
            try:
                row = _read(path)
                summary["checked"] += 1
                rules = PrimaryObservationRules(**row["rules"])
                if rules.end_basis != "session_close":
                    summary["not_development"] += 1
                    continue

                existing = row.get("development_metrics")
                if existing is not None:
                    if (not isinstance(existing, Mapping)
                            or existing.get("schema_version") != "primary_development_metrics.v1"
                            or existing.get("metrics_hash") != _digest(
                                {key: value for key, value in existing.items() if key != "metrics_hash"})):
                        raise ValueError("primary_development_metrics_integrity_mismatch")
                    summary["already_materialized"] += 1
                    continue

                if row.get("fill_capture_status") != "filled" or row.get("status") != "window_closed":
                    summary["not_ready"] += 1
                    continue
                if row.get("quality_protocol") is not None:
                    raise ValueError("development_watch_must_not_have_quality_protocol")
                if row.get("observation_window_basis") != "broker_recorded_primary_fill_to_session_close":
                    raise ValueError("development_watch_fill_anchor_required")

                case = deepcopy(row.get("case"))
                if not isinstance(case, dict):
                    raise ValueError("development_case_required")
                linked = linked_primary_fill(case, row["primary_option_symbol"])
                fill = linked["fill"]
                start = aware_utc(fill["timestamp"])
                end = aware_utc(row["window_end"])
                if not start < end:
                    raise ValueError("development_fill_must_precede_session_close")
                fill_price = _finite_number(fill["price"], positive=True)

                points, issues, feeds = _option_points(
                    row, case, start=start, end=end, fill_price=fill_price)
                option = _option_metrics(points, start=start, end=end, fill_price=fill_price)
                expected_feed = case.get("refresh", {}).get("options_feed")
                if not isinstance(expected_feed, str) or not expected_feed:
                    issues.append("recorded_refresh_options_feed_required")
                elif any(feed != expected_feed for feed in feeds):
                    issues.append("mixed_options_feed")
                underlying = _underlying_metrics(row, case, start=start, end=end)

                metrics = {
                    "schema_version": "primary_development_metrics.v1",
                    "watch_id": row["watch_id"],
                    "decision_id": row.get("decision_id"),
                    "account_scope": linked["account_scope"],
                    "broker_order_id": fill["broker_order_id"],
                    "option_symbol": row["primary_option_symbol"],
                    "fill_timestamp": fill["timestamp"],
                    "session_close": end.isoformat(),
                    "measurement_basis": "broker_fill_then_sampled_option_bid_gross_before_fees",
                    "options_feeds_observed": feeds,
                    "data_issues": sorted(set(issues)),
                    "option": option,
                    "underlying": underlying,
                    "development_only": True,
                    "eligible_for_holdout": False,
                    "pass_fail_status": None,
                    "edge_established": False,
                    "note": ("Threshold-free development evidence only. No exit is selected, no pass/fail "
                             "is assigned, and these rows must not be reused as prospective holdout evidence."),
                }
                metrics["metrics_hash"] = _digest(metrics)
                row["development_metrics"] = metrics
                _write(path, row, rules)
                summary["materialized"] += 1
            except Exception as exc:
                summary["errors"].append({
                    "watch_id": path.stem,
                    "reason": f"{type(exc).__name__}:{exc}",
                })
    return summary
