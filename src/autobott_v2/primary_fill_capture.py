"""Opt-in paper-fill collection: no order mutations or historical ownership guesses."""
from __future__ import annotations
from collections.abc import Callable, Mapping
from copy import deepcopy
from datetime import datetime
from pathlib import Path
import re
from typing import Any
from urllib.parse import urlsplit
from .bar_timing import aware_utc
from .primary_entry_study import digest
from .primary_fill_linkage import linked_primary_fill
from .primary_followthrough import PrimaryObservationRules, _locked, _read, _write
from .trade_outcomes import verified_broker_account_scope

_ORDER_FIELDS = ("id", "client_order_id", "symbol", "side", "position_intent", "status",
                 "qty", "filled_qty", "filled_avg_price", "filled_at", "submitted_at")
_TERMINAL = {"filled", "terminal_unscorable", "observation_limit_reached"}


def paper_capture_scope(broker: Any) -> str:
    config = broker.config
    environment = getattr(config.environment, "value", config.environment)
    url = urlsplit(config.trading_base_url)
    if (environment != "paper" or url.scheme != "https" or url.hostname != "paper-api.alpaca.markets"
            or url.port not in {None, 443} or url.username or url.password or url.path not in {"", "/"}
            or url.query or url.fragment):
        raise ValueError("primary_fill_capture_requires_paper_endpoint")
    return verified_broker_account_scope(broker)


def _watch_path(root: Path, watch_id: str) -> Path:
    if not isinstance(watch_id, str) or not re.fullmatch(r"[0-9a-f]{64}", watch_id):
        raise ValueError("invalid_primary_watch_id")
    return root / (watch_id + ".json")


def bind_primary_submission(root: str | Path, watch_id: str, order: Any, *, account_scope: str) -> None:
    """Persist the actual submission receipt; never claim it is filled."""
    if (not isinstance(account_scope, str) or not account_scope.startswith("alpaca:paper:")
            or not account_scope.removeprefix("alpaca:paper:") or account_scope != account_scope.strip()):
        raise ValueError("verified_paper_capture_scope_required")
    intent = order.intent
    if (getattr(intent.environment, "value", intent.environment) != "paper"
            or getattr(intent.side, "value", intent.side) != "buy_to_open"
            or intent.metadata.get("leg_role", "primary") != "primary"
            or type(intent.quantity) is not int or intent.quantity != 1):
        raise ValueError("single_primary_paper_submission_required")
    for value in (order.broker_order_id, order.client_order_id, intent.decision_id):
        if not isinstance(value, str) or not value.strip() or value != value.strip():
            raise ValueError("primary_submission_identity_required")
    submitted_at = aware_utc(order.submitted_at)
    root = Path(root)
    with _locked(root):
        path = _watch_path(root, watch_id)
        row = _read(path)
        case = row["case"]
        admission = case.get("recorded_admission", {})
        if (admission.get("decision_id") != intent.decision_id
                or admission.get("primary_option_symbol") != intent.option_symbol
                or admission.get("snapshot_hash") != digest(case["snapshot"])
                or row["primary_option_symbol"] != intent.option_symbol):
            raise ValueError("submission_does_not_match_recorded_admission")
        checked = aware_utc(admission["checked_at"])
        if submitted_at < checked:
            raise ValueError("submission_precedes_recorded_admission")
        receipt = {"schema_version": "primary_submission_receipt.v1",
                   "decision_id": intent.decision_id, "option_symbol": intent.option_symbol,
                   "snapshot_hash": admission["snapshot_hash"], "admission_checked_at": checked.isoformat(),
                   "leg_role": "primary", "side": "buy_to_open", "quantity": 1,
                   "account_scope": account_scope, "submitted_at": submitted_at.isoformat(),
                   "broker_order_id": order.broker_order_id, "client_order_id": order.client_order_id}
        existing = case.get("primary_submission_receipts", [])
        if existing:
            if existing != [receipt]:
                raise ValueError("conflicting_primary_submission_receipt")
            return
        case["primary_submission_receipts"] = [receipt]
        row["fill_capture_status"] = "awaiting_broker_observation"
        row["fill_provenance"] = "submission_recorded_fill_not_yet_observed"
        _write(path, row, PrimaryObservationRules(**row["rules"]))


def poll_primary_fills(root: str | Path, broker: Any, *, now_fn: Callable[[], datetime],
                       max_reads: int = 64) -> dict[str, Any]:
    if type(max_reads) is not int or not 1 <= max_reads <= 100:
        raise ValueError("bounded_primary_fill_reads_required")
    summary = {"checked": 0, "filled": 0, "pending": 0, "errors": [], "broker_writes": 0, "journal_writes": 0}
    root = Path(root)
    if not root.exists():
        return summary
    before = aware_utc(now_fn())
    due = []
    with _locked(root):
        for path in sorted(root.glob("*.json")):
            row = _read(path)
            receipts = row["case"].get("primary_submission_receipts", [])
            if not receipts or row.get("fill_capture_status") in _TERMINAL:
                continue
            if len(receipts) != 1:
                summary["errors"].append({"watch_id": row["watch_id"], "reason": "ambiguous_submission"})
                continue
            last = row.get("last_fill_observed_at")
            if last and (before - aware_utc(last)).total_seconds() < row["rules"]["minimum_poll_seconds"]:
                continue
            due.append((row["watch_id"], deepcopy(receipts[0])))
    if not due:
        return summary
    scope = paper_capture_scope(broker)
    observations = []
    for watch_id, receipt in due:
        if receipt.get("account_scope") != scope:
            summary["errors"].append({"watch_id": watch_id, "reason": "capture_account_scope_mismatch"})
            continue
        if summary["checked"] >= max_reads:
            break
        summary["checked"] += 1
        try:
            order = broker.get_order(receipt["broker_order_id"])
            received = aware_utc(now_fn())
            if received < before:
                raise ValueError("primary_fill_capture_clock_regressed")
            if (not isinstance(order, Mapping) or order.get("id") != receipt["broker_order_id"]
                    or order.get("client_order_id") != receipt["client_order_id"]
                    or order.get("symbol") != receipt["option_symbol"] or order.get("side") != "buy"):
                raise ValueError("primary_broker_observation_identity_mismatch")
            observation = {"schema_version": "broker_order_observation.v1", "account_scope": scope,
                           "received_at": received.isoformat(),
                           "order": {key: deepcopy(order[key]) for key in _ORDER_FIELDS if key in order}}
            observations.append((watch_id, receipt, observation))
        except Exception as exc:
            summary["errors"].append({"watch_id": watch_id, "reason": type(exc).__name__})
    # Never commit observations if the authenticated account changed mid-read.
    if paper_capture_scope(broker) != scope:
        raise ValueError("primary_fill_capture_account_changed")
    with _locked(root):
        for watch_id, receipt, observation in observations:
            try:
                path = _watch_path(root, watch_id)
                row = _read(path)
                if row["case"].get("primary_submission_receipts") != [receipt]:
                    raise ValueError("primary_submission_changed_during_capture")
                if row.get("fill_capture_status") in _TERMINAL:
                    continue
                if row.get("last_fill_observed_at") and aware_utc(observation["received_at"]) <= aware_utc(row["last_fill_observed_at"]):
                    continue
                rules = PrimaryObservationRules(**row["rules"])
                history = row.setdefault("broker_order_observation_history", [])
                if len(history) >= rules.max_observations:
                    row["fill_capture_status"] = "observation_limit_reached"
                    _write(path, row, rules)
                    summary["errors"].append({"watch_id": watch_id, "reason": "observation_limit_reached"})
                    continue
                history.append(observation)
                row["last_fill_observed_at"] = observation["received_at"]
                status = observation["order"].get("status")
                if status == "filled":
                    candidate = deepcopy(row["case"])
                    candidate["broker_order_observations"] = [observation]
                    try:
                        linked = linked_primary_fill(candidate, row["primary_option_symbol"])
                    except ValueError as exc:
                        row["fill_capture_status"] = "invalid_broker_fill"
                        summary["errors"].append({"watch_id": watch_id, "reason": str(exc)})
                    else:
                        candidate["fills"] = [linked["fill"]]
                        row["case"] = candidate
                        row["fill_capture_status"] = "filled"
                        row["fill_provenance"] = "account_scoped_submission_and_broker_read"
                        summary["filled"] += 1
                elif status in {"canceled", "rejected", "expired"}:
                    row["case"]["broker_order_observations"] = [observation]
                    row["fill_capture_status"] = "terminal_unscorable"
                    row["fill_provenance"] = "terminal_order_not_a_complete_primary_fill"
                else:
                    row["fill_capture_status"] = "awaiting_complete_fill"
                    summary["pending"] += 1
                _write(path, row, rules)
            except Exception as exc:
                summary["errors"].append({"watch_id": watch_id, "reason": type(exc).__name__})
    return summary
