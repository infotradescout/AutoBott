"""Pure paper-fill linkage. Never queries a broker or mutates a journal.

Matching retained records proves referential consistency, not authenticity of
caller-supplied files. Missing account or order identities are never inferred.
"""
from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal, InvalidOperation
import hashlib
import json
import math
from typing import Any

from .bar_timing import aware_utc


def _digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, allow_nan=False,
                                     separators=(",", ":")).encode()).hexdigest()


def _identity(value: Any) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError("explicit_fill_identity_required")
    return value


def _number(value: Any) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise ValueError("invalid_recorded_fill_number")
    try:
        number = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError("invalid_recorded_fill_number") from exc
    if not number.is_finite() or number <= 0:
        raise ValueError("invalid_recorded_fill_number")
    return number


def linked_primary_fill(case: Mapping[str, Any], primary_symbol: str) -> dict[str, Any]:
    """Join exact admission -> scoped submission -> full broker order fill.

    Evidence arrays retain their original records; conflicting/duplicate matches
    are errors. Historical unscoped rows cannot be repaired by symbol or time.
    """
    admission = case.get("recorded_admission")
    if not isinstance(admission, Mapping):
        raise ValueError("recorded_admission_binding_required")
    decision_id = _identity(admission.get("decision_id"))
    if admission.get("primary_option_symbol") != primary_symbol:
        raise ValueError("recorded_admission_primary_mismatch")
    snapshot_hash = _digest(case["snapshot"])
    if admission.get("snapshot_hash") != snapshot_hash:
        raise ValueError("recorded_admission_snapshot_mismatch")
    checked_at = aware_utc(admission.get("checked_at"))
    if checked_at != aware_utc(case["refresh"]["received_at"]):
        raise ValueError("recorded_admission_refresh_mismatch")
    receipts = case.get("primary_submission_receipts")
    observations = case.get("broker_order_observations")
    for rows in (receipts, observations):
        if not isinstance(rows, list) or len(rows) > 1000 or any(not isinstance(r, Mapping) for r in rows):
            raise ValueError("bounded_fill_linkage_records_required")
    matches = [r for r in receipts if r.get("decision_id") == decision_id
               and r.get("option_symbol") == primary_symbol]
    if len(matches) != 1:
        raise ValueError("exactly_one_primary_submission_receipt_required")
    receipt = matches[0]
    if receipt.get("schema_version") != "primary_submission_receipt.v1":
        raise ValueError("primary_submission_receipt_schema_required")
    if receipt.get("snapshot_hash") != snapshot_hash or aware_utc(receipt.get("admission_checked_at")) != checked_at:
        raise ValueError("submission_admission_binding_mismatch")
    if (receipt.get("leg_role") != "primary" or receipt.get("side") != "buy_to_open"
            or type(receipt.get("quantity")) is not int or receipt["quantity"] != 1):
        raise ValueError("single_primary_buy_submission_required")
    scope = _identity(receipt.get("account_scope"))
    if not scope.startswith("alpaca:paper:") or not scope.removeprefix("alpaca:paper:"):
        raise ValueError("explicit_paper_account_scope_required")
    broker_id = _identity(receipt.get("broker_order_id"))
    client_id = _identity(receipt.get("client_order_id"))
    submitted_at = aware_utc(receipt.get("submitted_at"))
    if submitted_at < checked_at:
        raise ValueError("submission_precedes_admission")
    if any(not isinstance(r.get("order"), Mapping) for r in observations):
        raise ValueError("broker_order_observation_payload_required")
    matches = [r for r in observations if r["order"].get("id") == broker_id]
    if len(matches) != 1:
        raise ValueError("exactly_one_broker_order_observation_required")
    observation = matches[0]
    if observation.get("schema_version") != "broker_order_observation.v1":
        raise ValueError("broker_order_observation_schema_required")
    if observation.get("account_scope") != scope:
        raise ValueError("broker_order_account_scope_mismatch")
    order = observation["order"]
    if order.get("client_order_id") != client_id or order.get("symbol") != primary_symbol:
        raise ValueError("broker_order_contract_or_client_identity_mismatch")
    if order.get("side") != "buy" or order.get("position_intent", "buy_to_open") != "buy_to_open":
        raise ValueError("broker_order_not_primary_buy")
    if order.get("status") != "filled" or _number(order.get("qty")) != 1 or _number(order.get("filled_qty")) != 1:
        raise ValueError("complete_single_contract_fill_required")
    filled_at = aware_utc(order.get("filled_at"))
    observed_at = aware_utc(observation.get("received_at"))
    if not submitted_at <= filled_at <= observed_at:
        raise ValueError("broker_fill_receipt_clock_mismatch")
    price = float(_number(order.get("filled_avg_price")))
    if not math.isfinite(price) or price <= 0:
        raise ValueError("invalid_recorded_fill_number")
    fill = {"option_symbol": primary_symbol, "price": price, "timestamp": filled_at.isoformat(),
            "side": "buy_to_open", "quantity": 1, "broker_order_id": broker_id}
    supplied = case.get("fills", [])
    if not isinstance(supplied, list) or any(not isinstance(f, Mapping) for f in supplied):
        raise ValueError("invalid_supplied_fill_records")
    selected = [f for f in supplied if f.get("option_symbol") == primary_symbol]
    if selected:
        if len(selected) != 1:
            raise ValueError("ambiguous_supplied_primary_fill")
        prior = selected[0]
        if (prior.get("broker_order_id") != broker_id or prior.get("side") != "buy_to_open"
                or type(prior.get("quantity")) is not int or prior["quantity"] != 1
                or _number(prior.get("price")) != _number(order["filled_avg_price"])
                or aware_utc(prior.get("timestamp")) != filled_at):
            raise ValueError("supplied_fill_conflicts_with_linked_broker_order")
    return {"schema_version": "primary_fill_linkage.v1", "fill": fill,
            "account_scope": scope, "decision_id": decision_id,
            "client_order_id": client_id, "broker_order_observed_at": observed_at.isoformat(),
            "admission_hash": _digest(admission), "submission_receipt_hash": _digest(receipt),
            "broker_observation_hash": _digest(observation), "snapshot_hash": snapshot_hash,
            "identity_links_consistent": True, "source_authenticity_verified": False,
            "broker_writes": 0, "journal_writes": 0}
