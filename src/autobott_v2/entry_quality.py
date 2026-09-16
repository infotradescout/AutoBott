"""Read-only, sampled-quote opportunity assessment. Never places or changes orders.

Rules must be supplied before evaluation; there are no fitted/default success
thresholds. A config hash identifies a protocol, not proof of preregistration.
Bid observations are not guaranteed fills. MFE is diagnostic, never a hindsight
exit. Keep separate holdout/comparator evidence before claiming an entry edge.
"""
from __future__ import annotations

import hashlib
import json
import math
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Iterable, Mapping


@dataclass(frozen=True)
class EntryQualityRules:
    protocol_id: str
    holding_seconds: float
    target_return_pct: float
    max_adverse_return_pct: float
    persistence_seconds: float
    max_quote_age_seconds: float
    max_observation_gap_seconds: float
    round_trip_fee_per_contract: float
    contract_multiplier: int

    def __post_init__(self) -> None:
        if not isinstance(self.protocol_id, str) or not self.protocol_id.strip():
            raise ValueError("protocol_id is required")
        names = ("holding_seconds", "target_return_pct", "max_adverse_return_pct",
                 "persistence_seconds", "max_quote_age_seconds",
                 "max_observation_gap_seconds", "round_trip_fee_per_contract")
        for name in names:
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
                raise ValueError(f"{name} must be a finite number")
        for name in ("holding_seconds", "target_return_pct", "max_adverse_return_pct",
                     "persistence_seconds", "max_observation_gap_seconds"):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")
        if self.max_adverse_return_pct > 1:
            raise ValueError("max_adverse_return_pct must be at most 1")
        if self.persistence_seconds > self.holding_seconds:
            raise ValueError("persistence exceeds holding window")
        if self.max_observation_gap_seconds > self.holding_seconds:
            raise ValueError("observation gap exceeds holding window")
        if self.max_quote_age_seconds < 0 or self.round_trip_fee_per_contract < 0:
            raise ValueError("quote age and fees cannot be negative")
        # Phase1 option replay currently models standard, unadjusted contracts.
        if type(self.contract_multiplier) is not int or self.contract_multiplier != 100:
            raise ValueError("only an explicit standard contract multiplier of 100 is supported")

    def to_json_dict(self) -> dict[str, Any]:
        return asdict(self)

    @property
    def config_hash(self) -> str:
        return _hash(self.to_json_dict())


def _hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, allow_nan=False).encode()).hexdigest()


def _time(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError("timestamp must be an ISO string")
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError("timestamp must have a timezone")
    return result.astimezone(timezone.utc)


def _number(value: Any) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError("price must be a finite number")
    return Decimal(str(value))


def evaluate_entry_quality(
    entry: Mapping[str, Any],
    snapshots: Iterable[Mapping[str, Any]],
    rules: EntryQualityRules | None,
    *,
    is_primary: bool,
    evidence_kind: str,
) -> dict[str, Any]:
    """Assess ONE filled contract, independently of the actual/simulated exit.

    Input is a Phase1LedgerEvent JSON row plus timestamped option snapshots.
    Include the entry snapshot. Evidence_kind must distinguish simulated fills
    from broker-recorded fills; it is a supplied label, not broker verification.
    Both point sampling and its coverage limits remain explicit in the result.
    Missing/invalid data is never silently dropped from a success denominator.
    """
    if evidence_kind not in {"simulated_fill", "broker_recorded_fill"}:
        raise ValueError("explicit supported evidence_kind is required")
    if type(is_primary) is not bool:
        raise ValueError("is_primary must be a bool")
    contract = entry.get("selected_contract")
    contract = contract if isinstance(contract, Mapping) else {}
    result: dict[str, Any] = {
        "schema_version": "entry_quality.v1", "decision_id": entry.get("decision_id"),
        "ticker": entry.get("ticker"), "option_symbol": contract.get("option_symbol"),
        "leg_role": entry.get("leg_role"), "is_primary": is_primary,
        "evidence_kind": evidence_kind, "measurement_basis": "sampled_option_bid_net_of_declared_fees",
        "status": "unscorable", "passed": None, "reason": None,
        "entry_fill_model": entry.get("entry_fill_model"),
        "protocol_id": rules.protocol_id if rules else None,
        "rules_hash": rules.config_hash if rules else None,
        "window_end": None, "valid_quote_count": 0, "data_issues": [],
        "max_observed_return_pct": None, "min_observed_return_pct": None,
        "adverse_before_opportunity_pct": None, "first_target_seconds": None,
        "opportunity_confirmed_seconds": None, "first_drawdown_breach_seconds": None,
        "longest_observed_target_run_seconds": 0.0,
        "edge_established": False,
    }
    if rules is None:
        result["reason"] = "missing_predeclared_rules"
        return result
    try:
        if entry.get("filled") is not True:
            raise ValueError("entry_not_filled")
        if not all(isinstance(entry.get(key), str) and entry[key].strip()
                   for key in ("decision_id", "ticker", "entry_fill_model")):
            raise ValueError("missing_entry_identity")
        if not isinstance(contract.get("option_symbol"), str) or not contract["option_symbol"].strip():
            raise ValueError("missing_selected_contract")
        start = _time(entry.get("timestamp"))
        price = _number(entry.get("entry_fill_price"))
        if price <= 0:
            raise ValueError("entry_fill_price_must_be_positive")
        end = start + timedelta(seconds=rules.holding_seconds)
        multiplier = Decimal(rules.contract_multiplier)
        cost = price * multiplier
        fees = Decimal(str(rules.round_trip_fee_per_contract))
    except (ValueError, TypeError, OverflowError) as exc:
        result["reason"] = f"invalid_entry:{exc}"
        return result
    result["window_end"] = end.isoformat()
    result["entry_fill_price"] = float(price)
    issues: list[str] = []
    observed_through = start
    candidates: list[tuple[datetime, Mapping[str, Any]]] = []
    for snapshot in snapshots:
        if not isinstance(snapshot, Mapping):
            issues.append("invalid_snapshot")
            continue
        if snapshot.get("ticker") != entry["ticker"]:
            continue
        try:
            timestamp = _time(snapshot.get("timestamp"))
        except (ValueError, TypeError, OverflowError):
            issues.append("invalid_snapshot_timestamp")
            continue
        observed_through = max(observed_through, timestamp)
        if start <= timestamp <= end:
            candidates.append((timestamp, snapshot))
    candidates.sort(key=lambda item: item[0])
    points: list[tuple[datetime, datetime, Decimal]] = []
    seen_quotes: dict[datetime, tuple[Decimal, Decimal]] = {}
    seen_observations: dict[datetime, tuple[datetime, Decimal, Decimal]] = {}
    previous_quote_time: datetime | None = None
    for timestamp, snapshot in candidates:
        try:
            chain = snapshot.get("option_chain")
            if not isinstance(chain, list):
                raise ValueError("missing_option_chain")
            matches = [q for q in chain if isinstance(q, Mapping)
                       and q.get("option_symbol") == contract["option_symbol"]]
            if len(matches) != 1:
                raise ValueError("missing_or_ambiguous_selected_contract_quote")
            quote = matches[0]
            bid, ask = _number(quote.get("bid")), _number(quote.get("ask"))
            if bid < 0 or ask <= 0 or ask < bid:
                raise ValueError("invalid_or_crossed_quote")
            quote_time = _time(quote.get("quote_timestamp"))
            age = (timestamp - quote_time).total_seconds()
            if age < 0 or age > rules.max_quote_age_seconds:
                raise ValueError("future_or_stale_quote")
            observed = (quote_time, bid, ask)
            if timestamp in seen_observations:
                if seen_observations[timestamp] != observed:
                    raise ValueError("conflicting_same_time_observations")
                continue
            seen_observations[timestamp] = observed
            if quote_time in seen_quotes:
                if seen_quotes[quote_time] != (bid, ask):
                    raise ValueError("conflicting_same_time_quotes")
                # A cached quote is not new evidence of profit persistence.
                continue
            if previous_quote_time is not None and quote_time < previous_quote_time:
                raise ValueError("nonmonotonic_quote_timestamps")
            previous_quote_time = quote_time
            seen_quotes[quote_time] = (bid, ask)
            value = (bid * multiplier - cost - fees) / cost
            # A quote published before entry cannot count as future profit.
            # At t=0 it still measures the entry's initial bid-side drawdown.
            if timestamp > start and quote_time <= start:
                raise ValueError("no_post_entry_quote")
            points.append((timestamp, quote_time, value))
        except (ValueError, TypeError, OverflowError) as exc:
            issues.append(str(exc))
    result["valid_quote_count"] = len(points)
    if observed_through < end:
        issues.append("holding_window_incomplete")
    if not points:
        issues.append("no_valid_selected_contract_quotes")
    else:
        boundaries = [start, *(time for time, _, _ in points), end]
        if any((b - a).total_seconds() > rules.max_observation_gap_seconds
               for a, b in zip(boundaries, boundaries[1:])):
            issues.append("observation_gap_exceeds_protocol")
        values = [value for _, _, value in points]
        result["max_observed_return_pct"] = float(max(values))
        result["min_observed_return_pct"] = float(min(values))
        target = Decimal(str(rules.target_return_pct))
        loss = -Decimal(str(rules.max_adverse_return_pct))
        run_start: datetime | None = None
        previous: datetime | None = None
        run_quote_start: datetime | None = None
        qualified: datetime | None = None
        breach: datetime | None = None
        first_target: datetime | None = None
        pre_opportunity_min = Decimal(0)
        longest = 0.0
        for timestamp, quote_time, value in points:
            if qualified is None:
                pre_opportunity_min = min(pre_opportunity_min, value)
            if breach is None and value <= loss:
                breach = timestamp
            if timestamp > start and value >= target:
                if first_target is None:
                    first_target = timestamp
                if run_start is None or previous is None or (timestamp - previous).total_seconds() > rules.max_observation_gap_seconds:
                    run_start = timestamp
                    run_quote_start = quote_time
                # Fresh quote timestamps must also span the required interval;
                # variable delivery delay cannot manufacture persistence.
                duration = min((timestamp - run_start).total_seconds(),
                               (quote_time - run_quote_start).total_seconds())
                longest = max(longest, duration)
                if qualified is None and duration >= rules.persistence_seconds:
                    qualified = timestamp
            else:
                run_start = None
                run_quote_start = None
            previous = timestamp
        seconds = lambda value: (value - start).total_seconds() if value is not None else None
        result.update({
            "first_target_seconds": seconds(first_target),
            "opportunity_confirmed_seconds": seconds(qualified),
            "first_drawdown_breach_seconds": seconds(breach),
            "adverse_before_opportunity_pct": float(pre_opportunity_min),
            "longest_observed_target_run_seconds": longest,
        })
        if not issues:
            passed = qualified is not None and (breach is None or qualified < breach)
            reason = ("sustained_opportunity_before_drawdown" if passed else
                      "drawdown_before_sustained_opportunity" if breach is not None else
                      "target_not_reached" if first_target is None else "target_not_persistent")
            result.update(status="pass" if passed else "fail", passed=passed, reason=reason)
    if issues:
        result["data_issues"] = sorted(set(issues))
        result["reason"] = "insufficient_or_invalid_option_path"
    return result


def summarize_entry_quality(results: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    rows = list(results)
    def counts(items: list[Mapping[str, Any]]) -> dict[str, Any]:
        statuses = Counter(row["status"] for row in items)
        scored = statuses["pass"] + statuses["fail"]
        return {
            "entries_recorded": len(items), "scorable": scored,
            "passes": statuses["pass"], "fails": statuses["fail"],
            "unscorable": statuses["unscorable"],
            "pass_rate_scorable": statuses["pass"] / scored if scored else None,
            "observed_success_fraction_all_entries": statuses["pass"] / len(items) if items else None,
        }
    return {
        **counts(rows), "primary": counts([row for row in rows if row["is_primary"]]),
        "non_primary": counts([row for row in rows if not row["is_primary"]]),
        "rules_hashes": sorted({row["rules_hash"] for row in rows if row.get("rules_hash")}),
        "measurement_basis": "sampled_option_bid_net_of_declared_fees",
        "edge_established": False,
        "note": "Opportunity diagnostics only; not realized P/L, entry-method lift, or a guaranteed executable fill.",
    }
