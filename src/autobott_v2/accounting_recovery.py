"""Explicit paper-only journal recovery with preserved, content-addressed history.

The original bytes are archived, never silently called reconciled. The new active
view comes exclusively from authenticated broker fills and the existing matcher.
No order, position, strategy, or execution-control write is performed here.
"""
from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from hashlib import sha256
import json
import math
import os
from pathlib import Path
import tempfile
from typing import Any
from zoneinfo import ZoneInfo


class RecoveryBlocked(ValueError):
    """A fixed reason code, safe to include in the operator diagnostic."""


def _digest(data: bytes) -> str:
    return sha256(data).hexdigest()


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError("unsupported_recovery_evidence_type")


def _encode(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, allow_nan=False, default=_json_default) + "\n").encode()


def _read(path: Path, limit: int = 64 * 1024 * 1024) -> bytes:
    if path.is_symlink():
        raise RecoveryBlocked("recovery_symlink_refused")
    if not path.exists():
        return b""
    if not path.is_file() or path.stat().st_size > limit:
        raise RecoveryBlocked("recovery_file_unavailable_or_oversized")
    return path.read_bytes()


def _quantity(value: Any) -> Decimal:
    try:
        number = Decimal(str(value))
    except InvalidOperation:
        raise RecoveryBlocked("invalid_position_quantity") from None
    if isinstance(value, bool) or not number.is_finite() or number <= 0 or number != number.to_integral_value():
        raise RecoveryBlocked("invalid_position_quantity")
    return number


def _broker_positions(rows: list[dict]) -> dict[str, Decimal]:
    if not isinstance(rows, list):
        raise RecoveryBlocked("broker_positions_not_a_list")
    result = {}
    for row in rows:
        symbol = row.get("symbol")
        if not isinstance(symbol, str) or not symbol or symbol in result:
            raise RecoveryBlocked("broker_position_identity_ambiguous")
        if row.get("asset_class") not in (None, "", "us_option"):
            raise RecoveryBlocked("non_option_position_requires_separate_reconciliation")
        result[symbol] = _quantity(row.get("qty"))
    return result


def verify_open_lots(pending: list[dict], broker_positions: list[dict], stored_positions: list[dict]) -> None:
    """Require the same current quantities AND entry-order identities; no relinking."""
    actual = _broker_positions(broker_positions)
    if any(row.get("pending_kind") == "open_order" for row in pending):
        raise RecoveryBlocked("open_broker_order_prevents_recovery")
    lots = {}
    for row in pending:
        if row.get("pending_kind") != "open_filled_buy" or row.get("symbol") not in actual:
            continue
        key = (row.get("symbol"), row.get("broker_order_id"))
        if not isinstance(key[1], str) or not key[1] or key in lots:
            raise RecoveryBlocked("broker_open_lot_identity_ambiguous")
        lots[key] = _quantity(row.get("remaining_filled_qty"))
    stored = {}
    for row in stored_positions:
        key = (row.get("option_symbol"), row.get("broker_order_id"))
        if not all(isinstance(v, str) and v for v in key) or key in stored:
            raise RecoveryBlocked("stored_open_lot_identity_ambiguous")
        stored[key] = _quantity(row.get("quantity"))
    if lots != stored:
        raise RecoveryBlocked("stored_and_broker_open_lots_disagree")
    totals: dict[str, Decimal] = {}
    for (symbol, _), quantity in lots.items():
        totals[symbol] = totals.get(symbol, Decimal(0)) + quantity
    if totals != actual:
        raise RecoveryBlocked("broker_fill_and_position_quantities_disagree")


def _paper_scope(broker: Any) -> str:
    from .trade_outcomes import verified_broker_account_scope
    config = broker.config
    environment = getattr(config.environment, "value", config.environment)
    if environment != "paper" or config.allow_live_trading:
        raise RecoveryBlocked("paper_only_recovery_required")
    if config.trading_base_url.rstrip("/") != "https://paper-api.alpaca.markets":
        raise RecoveryBlocked("exact_paper_broker_endpoint_required")
    scope = verified_broker_account_scope(broker)
    if not scope.startswith("alpaca:paper:"):
        raise RecoveryBlocked("verified_paper_account_required")
    return scope


def _order_signature(orders: list[dict]) -> str:
    if not isinstance(orders, list):
        raise RecoveryBlocked("broker_order_history_not_a_list")
    # Canonicalize ordering, but retain every field and every duplicate for the
    # native matcher to validate. Different snapshots must not be averaged.
    return _digest(_encode(sorted(orders, key=lambda row: str(row.get("id") or ""))))


def _legacy_map(original: bytes, canonical: list[dict], scope: str) -> list[dict]:
    by_pair = {(r["symbol"], r["entry_broker_order_id"], r["exit_broker_order_id"]): r for r in canonical}
    mapped = []
    for line_number, line in enumerate(original.splitlines(), 1):
        if not line.strip():
            continue
        item = {"line": line_number, "original_line_sha256": _digest(line)}
        try:
            row = json.loads(line)
        except (ValueError, UnicodeError):
            row = None
        if not isinstance(row, dict):
            item["disposition"] = "unreadable_legacy_record_preserved"
        elif row.get("account_scope") not in (None, "", scope):
            item["disposition"] = "different_or_unverified_account_record_preserved"
        else:
            pair = tuple(row.get(k) for k in ("symbol", "entry_broker_order_id", "exit_broker_order_id"))
            verified = by_pair.get(pair) if all(isinstance(v, str) and v for v in pair) else None
            if verified is None:
                item["disposition"] = "unverified_or_outside_window_record_preserved"
            else:
                item["disposition"] = "broker_verified_active_allocation"
                item["active_outcome_id"] = verified["outcome_id"]
                item["changed_fields"] = sorted(k for k in set(row) | set(verified) if row.get(k) != verified.get(k))
        mapped.append(item)
    return mapped


def prepare_recovery(*, broker: Any, journal_path: Path, store_path: Path,
                     execution_rows: list[dict], now: datetime | None = None) -> dict:
    from .outcome_ingestion import plan_outcome_append
    from .trade_outcomes import daily_realized_pnl, match_broker_order_lots
    now = now or datetime.now(UTC)
    if now.tzinfo is None:
        raise RecoveryBlocked("aware_recovery_clock_required")
    trading_day = now.astimezone(ZoneInfo("America/New_York")).date()
    scope = _paper_scope(broker)
    original, stored_bytes = _read(journal_path), _read(store_path)
    stored = json.loads(stored_bytes) if stored_bytes else []
    if not isinstance(stored, list):
        raise RecoveryBlocked("stored_positions_not_a_list")
    before_positions = broker.list_open_positions()
    orders = broker.list_order_history(status="all")
    order_signature = _order_signature(orders)
    matched = match_broker_order_lots(orders, execution_journal_rows=execution_rows, include_nonterminal_fills=True)
    for row in matched["unmatched_sells"]:
        stamp = row.get("event_time")
        if not isinstance(stamp, datetime) or stamp.tzinfo is None or stamp.astimezone(ZoneInfo("America/New_York")).date() >= trading_day:
            raise RecoveryBlocked("current_day_or_undated_unmatched_sell")
    verify_open_lots(matched["pending"], before_positions, stored)
    permanent = [row for row in matched["outcomes"] if not row.get("provisional_fill")]
    canonical_plan = plan_outcome_append([], permanent, account_scope=scope)
    if not canonical_plan["append_safe"]:
        raise RecoveryBlocked("broker_derived_active_journal_invalid")
    pnl = daily_realized_pnl(matched["outcomes"], trading_day=trading_day)
    if not isinstance(pnl, (int, float)) or not math.isfinite(pnl):
        raise RecoveryBlocked("current_day_broker_pnl_unavailable")
    if _paper_scope(broker) != scope:
        raise RecoveryBlocked("broker_account_changed_during_recovery")
    if _order_signature(broker.list_order_history(status="all")) != order_signature:
        raise RecoveryBlocked("broker_orders_changed_during_recovery")
    if _broker_positions(broker.list_open_positions()) != _broker_positions(before_positions):
        raise RecoveryBlocked("broker_positions_changed_during_recovery")
    if _read(journal_path) != original or _read(store_path) != stored_bytes:
        raise RecoveryBlocked("local_records_changed_during_recovery")
    rows = canonical_plan["new_rows"]
    candidate = b"".join(_encode(row) for row in rows)
    mapping = _legacy_map(original, rows, scope)
    return {
        "version": "paper_accounting_recovery.v1", "status": "prepared",
        "observed_at": now.isoformat(), "trading_day": trading_day.isoformat(),
        "source_sha": os.getenv("RENDER_GIT_COMMIT", "local"),
        "original_sha256": _digest(original), "candidate_sha256": _digest(candidate),
        "account_scope_sha256": _digest(scope.encode()), "orders_sha256": order_signature,
        "stored_positions_sha256": _digest(stored_bytes), "active_row_count": len(rows),
        "legacy_record_count": len(mapping), "legacy_dispositions": dict(Counter(r["disposition"] for r in mapping)),
        "historical_unmatched_sell_count": len(matched["unmatched_sells"]),
        "legacy_history_resolved": False, "full_account_lifetime_accounting_complete": False,
        "current_day_pnl_verified": True, "current_day_realized_pnl": pnl,
        "open_lot_identity_and_quantity_verified": True, "open_position_count": len(before_positions),
        "paper_only": True, "broker_writes": 0,
        "_original": original, "_candidate": candidate, "_orders": orders,
        "_mapping": mapping, "_stored_bytes": stored_bytes, "_positions": before_positions,
        "_scope": scope,
    }


def public_receipt(plan: dict) -> dict:
    return {key: value for key, value in plan.items() if not key.startswith("_")}


def _sync_directory(path: Path) -> None:
    if os.name == "posix":
        fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def _validate_plan_bytes(plan: dict) -> None:
    if (_digest(plan["_original"]) != plan["original_sha256"]
            or _digest(plan["_candidate"]) != plan["candidate_sha256"]
            or _digest(plan["_scope"].encode()) != plan["account_scope_sha256"]
            or _order_signature(plan["_orders"]) != plan["orders_sha256"]
            or plan.get("paper_only") is not True
            or plan.get("current_day_pnl_verified") is not True
            or plan.get("open_lot_identity_and_quantity_verified") is not True):
        raise RecoveryBlocked("recovery_plan_integrity_failed")


def _exclusive(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        if path.is_symlink() or path.read_bytes() != content:
            raise RecoveryBlocked("recovery_archive_content_conflict")
        return
    with os.fdopen(fd, "wb") as stream:
        stream.write(content); stream.flush(); os.fsync(stream.fileno())
    _sync_directory(path.parent)


def preserve_recovery(plan: dict, journal_path: Path) -> Path:
    _validate_plan_bytes(plan)
    directory = journal_path.parent / "accounting_recovery" / plan["original_sha256"] / plan["account_scope_sha256"] / (plan["candidate_sha256"] + "-" + plan["orders_sha256"])
    _exclusive(directory / "original.jsonl", plan["_original"])
    _exclusive(directory / "candidate.jsonl", plan["_candidate"])
    _exclusive(directory / "broker_orders.json", _encode(plan["_orders"]))
    _exclusive(directory / "legacy_record_map.json", _encode(plan["_mapping"]))
    # The prepared receipt is append-only by observation identity; a repeated
    # proposal cannot overwrite an earlier receipt or its supporting records.
    receipt_bytes = _encode(public_receipt(plan))
    _exclusive(directory / ("prepared-" + _digest(receipt_bytes) + ".json"), receipt_bytes)
    return directory


def apply_recovery(plan: dict, *, broker: Any, journal_path: Path, store_path: Path,
                   expected_original: str, expected_scope: str) -> dict:
    _validate_plan_bytes(plan)
    if not expected_original or expected_original != plan["original_sha256"]:
        raise RecoveryBlocked("explicit_original_digest_confirmation_required")
    if not expected_scope or expected_scope != plan["account_scope_sha256"]:
        raise RecoveryBlocked("explicit_account_digest_confirmation_required")
    if datetime.now(UTC).astimezone(ZoneInfo("America/New_York")).date().isoformat() != plan["trading_day"]:
        raise RecoveryBlocked("trading_day_changed_before_recovery")
    if _paper_scope(broker) != plan["_scope"]:
        raise RecoveryBlocked("broker_account_changed_before_apply")
    if _order_signature(broker.list_order_history(status="all")) != plan["orders_sha256"]:
        raise RecoveryBlocked("broker_orders_changed_before_apply")
    if _broker_positions(broker.list_open_positions()) != _broker_positions(plan["_positions"]):
        raise RecoveryBlocked("broker_positions_changed_before_apply")
    if _read(journal_path) != plan["_original"] or _read(store_path) != plan["_stored_bytes"]:
        raise RecoveryBlocked("local_records_changed_before_apply")
    directory = preserve_recovery(plan, journal_path)
    # Recheck the durable archive before the active-path atomic replacement.
    if _digest((directory / "original.jsonl").read_bytes()) != expected_original:
        raise RecoveryBlocked("recovery_archive_verification_failed")
    temp_path = None
    committed = False
    commit_confirmed = True
    try:
        with tempfile.NamedTemporaryFile(dir=journal_path.parent, prefix=".accounting-recovery-", delete=False) as stream:
            temp_path = Path(stream.name)
            stream.write(plan["_candidate"]); stream.flush(); os.fsync(stream.fileno())
        if _read(journal_path) != plan["_original"] or _read(store_path) != plan["_stored_bytes"]:
            raise RecoveryBlocked("local_records_changed_before_commit")
        os.replace(temp_path, journal_path)
        temp_path = None
        committed = True
        _sync_directory(journal_path.parent)
    except OSError:
        if not committed:
            raise
        commit_confirmed = False
    finally:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)
    receipt = {**public_receipt(plan), "status": "applied", "archive_directory": str(directory)}
    try:
        if not commit_confirmed or _digest(_read(journal_path)) != plan["candidate_sha256"]:
            raise RecoveryBlocked("active_recovery_verification_failed")
        _exclusive(directory / "applied.json", _encode(receipt))
    except (OSError, RecoveryBlocked):
        # The rename already happened. Never claim rollback or no side effects.
        receipt.update(status="applied_unconfirmed", reason="post_commit_receipt_or_durability_unconfirmed")
    return receipt


_ATTEMPTED = False


def maybe_recover_blocked_cycle(cycle: dict) -> dict | None:
    """Opt-in, once per process; the normal entry gate is never overridden."""
    global _ATTEMPTED
    mode = os.getenv("AUTOBOTT_ACCOUNTING_RECOVERY_MODE", "").strip().lower()
    if mode not in {"plan", "apply"} or _ATTEMPTED:
        return None
    accounting = next((r for r in cycle.get("execution_outcomes", []) if r.get("disposition") == "trade_outcome_learning_summary"), {})
    if accounting.get("error") != "outcome_journal_reconciliation_required":
        return None
    _ATTEMPTED = True
    try:
        from .execution_broker import AlpacaExecutionBroker
        from .execution_journal import load_execution_journal
        from .position_store import position_store_path
        from .trade_outcomes import _TRADE_OUTCOME_LOCK, trade_outcome_journal_path
        broker = AlpacaExecutionBroker()
        path, store = trade_outcome_journal_path(), position_store_path()
        with _TRADE_OUTCOME_LOCK:
            plan = prepare_recovery(broker=broker, journal_path=path, store_path=store,
                                    execution_rows=load_execution_journal())
            directory = preserve_recovery(plan, path)
            if mode == "apply":
                return apply_recovery(plan, broker=broker, journal_path=path, store_path=store,
                                      expected_original=os.getenv("AUTOBOTT_ACCOUNTING_RECOVERY_ORIGINAL_SHA256", ""),
                                      expected_scope=os.getenv("AUTOBOTT_ACCOUNTING_RECOVERY_ACCOUNT_SHA256", ""))
            return {**public_receipt(plan), "archive_directory": str(directory)}
    except RecoveryBlocked as exc:
        return {"status": "blocked", "reason": str(exc), "broker_writes": 0}
    except Exception as exc:
        # Broker exception text can contain private request data; never log it.
        return {"status": "blocked", "reason": "recovery_dependency_failed", "error_type": type(exc).__name__, "broker_writes": 0}
