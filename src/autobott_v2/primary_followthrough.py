"""Persist primary-option quote observations independently of exits and positions.

This module has no broker/order API. Admission is not a fill. Missing quotes,
missed intervals, unknown fills and feed changes remain visible to the study.
"""
from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
import json
import math
import os
from pathlib import Path
import tempfile
from typing import Any, Callable, Mapping

from .bar_timing import aware_utc
from .entry_quality import EntryQualityRules
from .observation_lock import observation_store_lock as _locked
from .primary_entry_study import case_from_runtime_evidence, digest
from .quote_observation import observed_quote_fields


@dataclass(frozen=True)
class PrimaryObservationRules:
    window_seconds: int
    minimum_poll_seconds: int = 15
    max_active: int = 64
    max_observations: int = 512
    max_case_bytes: int = 2_000_000
    max_store_bytes: int = 64_000_000
    end_basis: str = "fixed_duration"

    def __post_init__(self) -> None:
        for name in ("window_seconds", "minimum_poll_seconds", "max_active",
                     "max_observations", "max_case_bytes", "max_store_bytes"):
            value = getattr(self, name)
            if type(value) is not int or value <= 0:
                raise ValueError("positive_integer_observation_rules_required")
        if self.end_basis not in {"fixed_duration", "session_close"}:
            raise ValueError("invalid_primary_observation_end_basis")
        if self.minimum_poll_seconds > self.window_seconds or self.max_active > 100:
            raise ValueError("invalid_primary_observation_bounds")
        if self.window_seconds > 86400:
            raise ValueError("explicit_session_calendar_required_for_longer_observation")


def configured_observation_rules() -> PrimaryObservationRules | None:
    value = os.getenv("AUTOBOTT_PRIMARY_OBSERVATION_SECONDS")
    development = os.getenv("AUTOBOTT_PRIMARY_DEVELOPMENT_CAPTURE")
    if value is not None and value.strip() and development is not None and development.strip():
        raise ValueError("conflicting_primary_observation_modes")
    if value is not None and value.strip():
        return PrimaryObservationRules(window_seconds=int(value))
    if development is None or not development.strip():
        return None  # Collection window must be selected, not fitted from outcomes.
    if development.strip().lower() != "session":
        raise ValueError("unsupported_primary_development_capture_mode")
    # 6.5 regular-session hours is a storage/capacity bound, not a quality horizon.
    return PrimaryObservationRules(window_seconds=23_400, end_basis="session_close")


def _observation_window_end(snapshot: Mapping[str, Any], start: datetime,
                            rules: PrimaryObservationRules) -> datetime:
    if rules.end_basis == "fixed_duration":
        return start + timedelta(seconds=rules.window_seconds)
    schedule = snapshot.get("entry_context", {}).get("schedule", {})
    session = schedule.get("session", {}) if isinstance(schedule, Mapping) else {}
    if not isinstance(session, Mapping) or session.get("trading_day") is not True:
        raise ValueError("development_capture_session_required")
    opening, closing = aware_utc(session.get("open")), aware_utc(session.get("close"))
    if not opening <= start < closing:
        raise ValueError("development_capture_start_outside_session")
    remaining = (closing - start).total_seconds()
    if remaining <= 0 or remaining > rules.window_seconds:
        raise ValueError("development_capture_session_bound_invalid")
    return closing


def _write(path: Path, row: dict, rules: PrimaryObservationRules) -> None:
    raw = (json.dumps(row, sort_keys=True, allow_nan=False) + "\n").encode()
    if len(raw) > rules.max_case_bytes:
        raise ValueError("primary_observation_case_size_limit")
    existing = path.stat().st_size if path.exists() else 0
    total = sum(p.stat().st_size for p in path.parent.glob("*.json"))
    if total-existing+len(raw) > rules.max_store_bytes:
        raise ValueError("primary_observation_store_size_limit")
    name = None
    try:
        with tempfile.NamedTemporaryFile(dir=path.parent, prefix=".watch-", delete=False) as handle:
            name = handle.name
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(name, path)
    finally:
        if name and os.path.exists(name):
            os.unlink(name)


def _read(path: Path, maximum: int = 2_000_000) -> dict:
    if path.is_symlink() or path.stat().st_size > maximum:
        raise ValueError("invalid_primary_observation_file")
    row = json.loads(path.read_text(encoding="utf-8"))
    if row.get("schema_version") != "primary_observation.v1" or path.stem != row.get("watch_id"):
        raise ValueError("primary_observation_identity_mismatch")
    return row


def register_primary_observation(root: str | Path, snapshot: Mapping[str, Any],
                                 admission: Mapping[str, Any], rules: PrimaryObservationRules,
                                 *, quality_rules: EntryQualityRules | None = None) -> str:
    root = Path(root)
    symbol = admission.get("primary_option_symbol")
    if not isinstance(symbol, str) or not symbol.strip():
        raise ValueError("primary_observation_symbol_required")
    start = aware_utc(admission["checked_at"])
    window_end = _observation_window_end(snapshot, start, rules)
    capsule = admission.get("recorded_refresh", {})
    if aware_utc(capsule.get("received_at")) != start:
        raise ValueError("primary_observation_receipt_identity_mismatch")
    if symbol not in capsule.get("option_quotes", {}):
        raise ValueError("primary_observation_quote_missing")
    watch_id = digest({"snapshot": digest(snapshot), "primary": symbol, "received_at": start.isoformat()})
    case = case_from_runtime_evidence(snapshot=snapshot, admission_event=admission, sample_id=watch_id,
                                     outcome_snapshots=[], fills=[])
    quality_protocol = None
    if quality_rules is not None:
        if rules.end_basis != "fixed_duration":
            raise ValueError("quality_protocol_requires_fixed_observation_duration")
        if quality_rules.holding_seconds > rules.window_seconds:
            raise ValueError("observation_window_shorter_than_quality_holding_period")
        quality_protocol = {"schema_version": "entry_quality_rules.v1",
                            "rules": quality_rules.to_json_dict(),
                            "rules_hash": quality_rules.config_hash}
    ticker = snapshot.get("ticker")
    thesis = admission.get("live_signal_thesis", {}).get("at_refresh", {})
    signal_symbol = admission.get("signal_symbol", thesis.get("signal_symbol"))
    direction = thesis.get("direction")
    reference_basis = admission.get("underlying_reference_basis")
    if not all(isinstance(value, str) and value.strip()
               for value in (ticker, signal_symbol, direction, reference_basis)):
        underlying_followthrough = {"status": "not_recorded", "reason": "direct_signal_identity_not_recorded"}
    elif signal_symbol.upper() != ticker.upper() or reference_basis != "fresh_equity_mid":
        underlying_followthrough = {
            "status": "not_applicable", "reason": "proxy_or_non_equity_reference_basis",
            "ticker": ticker.upper(), "signal_symbol": signal_symbol.upper(),
            "direction": direction, "reference_basis": reference_basis,
        }
    elif direction not in {"bullish", "bearish"}:
        underlying_followthrough = {"status": "not_recorded", "reason": "direction_not_recorded"}
    else:
        underlying_followthrough = {
            "status": "configured", "ticker": ticker.upper(), "signal_symbol": signal_symbol.upper(),
            "direction": direction, "reference_basis": reference_basis,
        }
    row = {"schema_version": "primary_observation.v1", "watch_id": watch_id,
           "decision_id": admission.get("decision_id"), "primary_option_symbol": symbol,
           "start": start.isoformat(), "window_end": window_end.isoformat(),
           "rules": asdict(rules), "status": "observing", "last_observed_at": None,
           "observation_window_basis": ("admission_pending_fill_session_close"
                                         if rules.end_basis == "session_close"
                                         else "admission_pending_fill"),
           "quality_protocol": quality_protocol,
           "underlying_followthrough": underlying_followthrough,
           "fill_provenance": "not_collected_admission_is_not_a_fill", "case": case}
    with _locked(root):
        path = root / (watch_id + ".json")
        if path.exists():
            existing = _read(path, rules.max_case_bytes)
            if existing["rules"] != row["rules"]:
                raise ValueError("cannot_change_existing_observation_window")
            if existing.get("quality_protocol") != row["quality_protocol"]:
                raise ValueError("cannot_change_existing_quality_protocol")
            if ("underlying_followthrough" in existing
                    and existing["underlying_followthrough"] != row["underlying_followthrough"]):
                raise ValueError("cannot_change_existing_underlying_followthrough")
            return watch_id
        active = sum(_read(p)["status"] == "observing" for p in root.glob("*.json"))
        if active >= rules.max_active:
            raise ValueError("primary_observation_capacity_reached")
        _write(path, row, rules)
    return watch_id


def poll_primary_observations(root: str | Path, data_client: Any, *,
                              now_fn: Callable[[], datetime]) -> dict[str, Any]:
    root = Path(root)
    if not root.exists():
        return {"checked": 0, "observed": 0, "window_closed": 0, "errors": [],
                "underlying_observed": 0, "underlying_errors": [], "broker_writes": 0}
    before = aware_utc(now_fn())
    expired = 0
    with _locked(root):
        due = []
        # Do not retain every historical snapshot/path in runtime memory.
        for path in sorted(root.glob("*.json")):
            row = _read(path)
            if row["status"] != "observing":
                continue
            rules = PrimaryObservationRules(**row["rules"])
            # Never let a restart or next-session cycle attach a post-window quote
            # to yesterday's entry. Closing an expired watch is local-only and
            # preserves any missing tail as missing evidence.
            if before > aware_utc(row["window_end"]):
                row["status"] = "window_closed"
                row["closed_without_post_window_quote"] = True
                _write(path, row, rules)
                expired += 1
                continue
            if (before >= aware_utc(row["start"])
                    and (row["last_observed_at"] is None or
                         (before-aware_utc(row["last_observed_at"])).total_seconds() >= row["rules"]["minimum_poll_seconds"])):
                underlying = row.get("underlying_followthrough", {})
                due.append({"watch_id": row["watch_id"], "primary_option_symbol": row["primary_option_symbol"],
                            "underlying_symbol": (underlying.get("signal_symbol")
                                if underlying.get("status") == "configured" else None)})
    if not due:
        return {"checked": 0, "observed": 0, "window_closed": expired, "errors": [],
                "underlying_observed": 0, "underlying_errors": [], "broker_writes": 0}
    symbols = sorted({r["primary_option_symbol"] for r in due})
    stock_symbols = sorted({r["underlying_symbol"] for r in due if r["underlying_symbol"]})
    if len(symbols) > 100 or len(stock_symbols) > 100:
        raise ValueError("primary_observation_provider_batch_limit")
    failure = None
    try:
        quotes = data_client.get_latest_option_quotes(symbols)
        if not isinstance(quotes, Mapping):
            raise ValueError("invalid_quote_response")
    except Exception as exc:
        failure, quotes = type(exc).__name__, {}
    stock_failure, stock_quotes = None, {}
    if stock_symbols:
        try:
            stock_quotes = data_client.get_latest_stock_quotes(stock_symbols)
            if not isinstance(stock_quotes, Mapping):
                raise ValueError("invalid_stock_quote_response")
        except Exception as exc:
            stock_failure, stock_quotes = type(exc).__name__, {}
    after = aware_utc(now_fn())
    if after < before:
        raise ValueError("primary_observation_clock_regressed")
    feed = getattr(data_client, "option_feed", getattr(data_client, "feed", "unreported"))
    stock_feed = getattr(data_client, "stock_feed", getattr(data_client, "feed", "unreported"))
    summary = {"checked": len(due), "observed": 0, "window_closed": expired, "errors": [],
               "underlying_observed": 0, "underlying_errors": [], "broker_writes": 0}
    with _locked(root):
        for initial in due:
            path = root / (initial["watch_id"] + ".json")
            row = _read(path)
            if row["status"] != "observing":
                continue
            if row["last_observed_at"] is not None and after <= aware_utc(row["last_observed_at"]):
                continue
            rules = PrimaryObservationRules(**row["rules"])
            observations = row["case"]["outcome_snapshots"]
            if len(observations) >= rules.max_observations:
                row["status"] = "observation_limit_reached"
                _write(path, row, rules)
                summary["errors"].append({"watch_id": row["watch_id"], "reason": row["status"]})
                continue
            symbol = row["primary_option_symbol"]
            point = {"ticker": row["case"]["snapshot"]["ticker"], "timestamp": after.isoformat(),
                     "source": {"options_feed": feed, "stock_feed": stock_feed,
                                "name": "primary_followthrough_recorder"},
                     "option_chain": [], "underlying_chain": []}
            try:
                if failure:
                    raise ValueError("quote_request_failed:"+failure)
                bid, ask, stamp = observed_quote_fields(quotes.get(symbol), allow_zero_bid=True)
                point["option_chain"].append({"option_symbol": symbol, "bid": bid, "ask": ask, "quote_timestamp": stamp})
            except ValueError as exc:
                point["data_issue"] = str(exc)
                summary["errors"].append({"watch_id": row["watch_id"], "reason": str(exc)})
            underlying = row.get("underlying_followthrough", {})
            if underlying.get("status") == "configured":
                stock_symbol = underlying["signal_symbol"]
                try:
                    if stock_failure:
                        raise ValueError("stock_quote_request_failed:"+stock_failure)
                    ubid, uask, ustamp = observed_quote_fields(stock_quotes.get(stock_symbol), allow_zero_bid=False)
                    point["underlying_chain"].append({
                        "symbol": stock_symbol, "bid": ubid, "ask": uask, "quote_timestamp": ustamp})
                    summary["underlying_observed"] += 1
                except ValueError as exc:
                    point["underlying_data_issue"] = str(exc)
                    summary["underlying_errors"].append({"watch_id": row["watch_id"], "reason": str(exc)})
            observations.append(point)
            row["last_observed_at"] = after.isoformat()
            # A quote requested no later than the boundary may close the watch.
            # Later cycles close locally before any provider request.
            if after >= aware_utc(row["window_end"]):
                row["status"] = "window_closed"
                summary["window_closed"] += 1
            _write(path, row, rules)
            summary["observed"] += 1
    return summary


def export_primary_observations(root: str | Path, *, source_kind: str) -> dict[str, Any]:
    if source_kind not in {"synthetic", "recorded_market"}:
        raise ValueError("explicit_observation_source_kind_required")
    root = Path(root)
    with _locked(root):
        rows = [_read(p) for p in sorted(root.glob("*.json"))]
    return {"schema_version": "primary_entry_tape.v1", "source_kind": source_kind,
            "cohort_scope": "admitted_entries_only", "cases": [deepcopy(r["case"]) for r in rows],
            "capture_statuses": {r["watch_id"]: r["status"] for r in rows},
            "fill_provenance": ("per_watch_account_scoped_capture" if any(r["case"].get("primary_submission_receipts") for r in rows)
                                else "not_collected_admission_is_not_a_fill"),
            "fill_capture_statuses": {r["watch_id"]: r.get("fill_capture_status", "not_collected") for r in rows},
            "entry_edge_established": False}
