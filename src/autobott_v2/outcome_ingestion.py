"""Conservative, pure append planning for one account-owned outcome journal.

Broker entry/exit IDs identify an order-level matched lot. Timestamps are checked
for conflicts, not used to mint a second economic event. This module NEVER
rewrites history, chooses a tax lot, contacts a broker, or changes an order.
The caller must supply a verified broker/environment/account namespace; a path,
API key, inferred account or missing account must not stand in for that scope.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from hashlib import sha256
import json
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

_JSON = dict[str, Any]
_ATTRIBUTION = (
    "trade_group_id", "leg_role", "policy_version", "policy_attribution_source",
    "decision_id", "thesis_id", "strategy_version", "entry_client_order_id",
    "exit_client_order_id", "build_sha",
)


def _scope(value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("verified_account_scope_required")
    return value.strip()


def _number(value: Any) -> Decimal:
    if value is None or isinstance(value, bool):
        raise ValueError("missing_or_invalid_financial_field")
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise ValueError("invalid_financial_field") from None
    if not number.is_finite():
        raise ValueError("nonfinite_financial_field")
    return number


def _stamp(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError("unusable_timestamp")
    try:
        stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise ValueError("unusable_timestamp") from None
    if stamp.tzinfo is None:
        raise ValueError("timezone_required")
    return stamp


def _identity(row: Mapping[str, Any], scope: str) -> tuple[str, str, str, str]:
    fields = [row.get(k) for k in ("symbol", "entry_broker_order_id", "exit_broker_order_id")]
    if any(not isinstance(v, str) or not v.strip() for v in fields):
        raise ValueError("stable_broker_order_ids_required")
    symbol, entry, exit_order = (v.strip() for v in fields)
    # Historical rows without a label are only evaluated in the explicitly
    # verified namespace supplied by the caller. A conflicting label blocks.
    if row.get("account_scope") not in (None, "", scope):
        raise ValueError("account_scope_conflict")
    return scope, symbol.upper(), entry, exit_order


def stable_outcome_id(row: Mapping[str, Any], *, account_scope: str) -> str:
    """Stable ID for ONE order-pair allocation in a verified account scope."""
    identity = _identity(row, _scope(account_scope))
    raw = json.dumps(["trade_outcome.identity.v2", *identity], separators=(",", ":"))
    return sha256(raw.encode("utf-8")).hexdigest()[:24]


def _validate(row: Mapping[str, Any]) -> None:
    qty, entry, exit_price, pnl = (_number(row.get(k)) for k in ("qty", "entry_price", "exit_price", "pnl"))
    if qty <= 0 or entry <= 0 or exit_price < 0:
        raise ValueError("invalid_quantity_or_price")
    required = row.get("entry_order_filled_qty")
    if required is not None and (qty > _number(required) or _number(required) <= 0):
        raise ValueError("entry_quantity_overmatched")
    # This journal stores gross premium P/L for ordinary 100-multiplier options.
    # Adjusted contracts and fees must be handled by an explicitly different
    # schema; never silently force them into this reconciliation.
    if row.get("contract_multiplier", 100) not in (100, 100.0, "100"):
        raise ValueError("unsupported_contract_multiplier")
    expected = ((exit_price - entry) * qty * 100).quantize(Decimal("0.01"))
    if pnl != expected:
        raise ValueError("pnl_economics_conflict")
    if row.get("provisional_fill"):
        raise ValueError("provisional_fill_not_appendable")
    if _stamp(row.get("exit_time")) < _stamp(row.get("entry_time")):
        raise ValueError("exit_precedes_entry")


def _differences(a: Mapping[str, Any], b: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    for key in ("qty", "entry_price", "exit_price", "pnl", "entry_order_filled_qty"):
        av, bv = a.get(key), b.get(key)
        if av is None and bv is None:
            continue
        if av is None or bv is None or _number(av) != _number(bv):
            reasons.append("financial_payload_changed:" + key)
    for key in _ATTRIBUTION:
        if a.get(key) != b.get(key):
            reasons.append("attribution_changed:" + key)
    for key in ("entry_time", "exit_time"):
        av, bv = _stamp(a.get(key)), _stamp(b.get(key))
        if abs(av - bv) > timedelta(microseconds=1):
            reasons.append("material_timestamp_change:" + key)
        for zone in ("America/Chicago", "America/New_York"):
            if av.astimezone(ZoneInfo(zone)).date() != bv.astimezone(ZoneInfo(zone)).date():
                reasons.append("reporting_day_changed:" + zone + ":" + key)
    return sorted(set(reasons))


def plan_outcome_append(
    existing_rows: Sequence[Mapping[str, Any]],
    incoming_rows: Sequence[Mapping[str, Any]],
    *,
    account_scope: str,
) -> _JSON:
    """Plan an all-or-nothing append; leave all input/history rows unchanged.

    A terminal order pair can allocate to multiple entry lots, distinguished by
    entry ID. A later restatement of the SAME entry/exit pair is a conflict, not
    another event. Existing duplicates remain visible and prevent a complete-
    accounting claim. Unknown identity cannot be fixed by prices or timestamps.
    """
    scope = _scope(account_scope)
    indexed: dict[tuple[str, str, str, str], list[tuple[str, int, Mapping[str, Any]]]] = {}
    conflicts: list[_JSON] = []
    unresolved: list[_JSON] = []
    historical_duplicates: list[_JSON] = []
    candidates: list[_JSON] = []
    suppressed = 0
    for origin, rows in (("existing", existing_rows), ("incoming", incoming_rows)):
        for index, row in enumerate(rows):
            try:
                if not isinstance(row, Mapping):
                    raise ValueError("invalid_outcome_record")
                identity = _identity(row, scope)
                _validate(row)
            except (ValueError, TypeError, InvalidOperation) as exc:
                unresolved.append({"origin": origin, "index": index, "reason": str(exc)})
                continue
            prior = indexed.get(identity, [])
            reasons: set[str] = set()
            # Compare all prior rows, not just the first: +1/-1 microsecond
            # records must not chain into an unbounded timestamp tolerance.
            for _, _, previous in prior:
                reasons.update(_differences(previous, row))
            if reasons:
                conflicts.append({"origin": origin, "index": index,
                                  "identity": list(identity), "reasons": sorted(reasons)})
            elif prior and origin == "existing":
                historical_duplicates.append({"index": index, "first_index": prior[0][1],
                                              "identity": list(identity)})
            elif prior:
                suppressed += 1
            elif origin == "incoming":
                candidate = deepcopy(dict(row))
                candidate["account_scope"] = scope
                candidate["identity_version"] = "trade_outcome.identity.v2"
                candidate["outcome_id"] = stable_outcome_id(row, account_scope=scope)
                candidates.append(candidate)
            indexed.setdefault(identity, []).append((origin, index, row))
    # Different exit IDs may represent valid partial closes, but their total
    # cannot exceed the original entry's cumulative filled quantity.
    entry_allocations: dict[tuple[str, str, str], tuple[Decimal, set[Decimal]]] = {}
    entry_reference: dict[tuple[str, str, str], Mapping[str, Any]] = {}
    for identity, observations in indexed.items():
        current = observations[0][2]
        cap_value = current.get("entry_order_filled_qty")
        if cap_value is None:
            unresolved.append({"origin": observations[0][0], "index": observations[0][1],
                               "reason": "entry_fill_quantity_required"})
            continue
        entry_key = (identity[0], identity[1], identity[2])
        reference = entry_reference.setdefault(entry_key, current)
        entry_fields = ("trade_group_id", "leg_role", "policy_version", "decision_id", "thesis_id", "build_sha")
        if any(reference.get(k) != current.get(k) for k in entry_fields) or _number(reference["entry_price"]) != _number(current["entry_price"]):
            conflicts.append({"identity": list(entry_key), "reasons": ["entry_lot_provenance_conflict"]})
        allocated, caps = entry_allocations.setdefault(entry_key, (Decimal(0), set()))
        caps.add(_number(cap_value))
        entry_allocations[entry_key] = (allocated + _number(current["qty"]), caps)
    for identity, (allocated, caps) in entry_allocations.items():
        if len(caps) != 1 or allocated > next(iter(caps)):
            conflicts.append({"identity": list(identity), "reasons": ["entry_lot_allocation_conflict"]})
    append_safe = not conflicts and not unresolved and not historical_duplicates
    return {
        "append_safe": append_safe,
        "identity_check_complete": append_safe,
        "accounting_complete": False,  # Broker coverage/fees are not verified here.
        "new_rows": candidates if append_safe else [],
        "suppressed_replays": suppressed,
        "historical_duplicates": historical_duplicates,
        "conflicts": conflicts,
        "unresolved": unresolved,
        "history_rewritten": False,
        "requires_reconciliation": not append_safe,
    }
