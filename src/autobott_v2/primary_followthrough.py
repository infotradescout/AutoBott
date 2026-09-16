"""Persist primary-option quote observations independently of exits and positions.

This module has no broker/order API. Admission is not a fill. Missing quotes,
missed intervals, unknown fills and feed changes remain visible to the study.
"""
from __future__ import annotations

from contextlib import contextmanager
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

    def __post_init__(self) -> None:
        for value in asdict(self).values():
            if type(value) is not int or value <= 0:
                raise ValueError("positive_integer_observation_rules_required")
        if self.minimum_poll_seconds > self.window_seconds or self.max_active > 100:
            raise ValueError("invalid_primary_observation_bounds")
        if self.window_seconds > 86400:
            raise ValueError("explicit_session_calendar_required_for_longer_observation")


def configured_observation_rules() -> PrimaryObservationRules | None:
    value = os.getenv("AUTOBOTT_PRIMARY_OBSERVATION_SECONDS")
    if value is None or not value.strip():
        return None  # Collection window must be selected, not fitted from outcomes.
    return PrimaryObservationRules(window_seconds=int(value))


@contextmanager
def _locked(root: Path):
    root.mkdir(parents=True, exist_ok=True)
    lock = root / ".primary-observation.lock"
    fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        os.close(fd)
        yield
    finally:
        lock.unlink()


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
                                 admission: Mapping[str, Any], rules: PrimaryObservationRules) -> str:
    root = Path(root)
    symbol = admission.get("primary_option_symbol")
    if not isinstance(symbol, str) or not symbol.strip():
        raise ValueError("primary_observation_symbol_required")
    start = aware_utc(admission["checked_at"])
    capsule = admission.get("recorded_refresh", {})
    if aware_utc(capsule.get("received_at")) != start:
        raise ValueError("primary_observation_receipt_identity_mismatch")
    if symbol not in capsule.get("option_quotes", {}):
        raise ValueError("primary_observation_quote_missing")
    watch_id = digest({"snapshot": digest(snapshot), "primary": symbol, "received_at": start.isoformat()})
    case = case_from_runtime_evidence(snapshot=snapshot, admission_event=admission, sample_id=watch_id,
                                     outcome_snapshots=[], fills=[])
    row = {"schema_version": "primary_observation.v1", "watch_id": watch_id,
           "decision_id": admission.get("decision_id"), "primary_option_symbol": symbol,
           "start": start.isoformat(), "window_end": (start+timedelta(seconds=rules.window_seconds)).isoformat(),
           "rules": asdict(rules), "status": "observing", "last_observed_at": None,
           "fill_provenance": "not_collected_admission_is_not_a_fill", "case": case}
    with _locked(root):
        path = root / (watch_id + ".json")
        if path.exists():
            existing = _read(path, rules.max_case_bytes)
            if existing["rules"] != row["rules"]:
                raise ValueError("cannot_change_existing_observation_window")
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
        return {"checked": 0, "observed": 0, "window_closed": 0, "errors": [], "broker_writes": 0}
    before = aware_utc(now_fn())
    with _locked(root):
        due = []
        # Do not retain every historical snapshot/path in runtime memory.
        for path in sorted(root.glob("*.json")):
            row = _read(path)
            if (row["status"] == "observing" and before >= aware_utc(row["start"])
                    and (row["last_observed_at"] is None or
                         (before-aware_utc(row["last_observed_at"])).total_seconds() >= row["rules"]["minimum_poll_seconds"])):
                due.append({"watch_id": row["watch_id"], "primary_option_symbol": row["primary_option_symbol"]})
    if not due:
        return {"checked": 0, "observed": 0, "window_closed": 0, "errors": [], "broker_writes": 0}
    symbols = sorted({r["primary_option_symbol"] for r in due})
    if len(symbols) > 100:
        raise ValueError("primary_observation_provider_batch_limit")
    failure = None
    try:
        quotes = data_client.get_latest_option_quotes(symbols)
        if not isinstance(quotes, Mapping):
            raise ValueError("invalid_quote_response")
    except Exception as exc:
        failure, quotes = type(exc).__name__, {}
    after = aware_utc(now_fn())
    if after < before:
        raise ValueError("primary_observation_clock_regressed")
    feed = getattr(data_client, "option_feed", getattr(data_client, "feed", "unreported"))
    summary = {"checked": len(due), "observed": 0, "window_closed": 0, "errors": [], "broker_writes": 0}
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
                     "source": {"options_feed": feed, "name": "primary_followthrough_recorder"}, "option_chain": []}
            try:
                if failure:
                    raise ValueError("quote_request_failed:"+failure)
                bid, ask, stamp = observed_quote_fields(quotes.get(symbol), allow_zero_bid=True)
                point["option_chain"].append({"option_symbol": symbol, "bid": bid, "ask": ask, "quote_timestamp": stamp})
            except ValueError as exc:
                point["data_issue"] = str(exc)
                summary["errors"].append({"watch_id": row["watch_id"], "reason": str(exc)})
            observations.append(point)
            row["last_observed_at"] = after.isoformat()
            # A receipt after the window closes the watch but does not fill in
            # missed earlier quotes. The evaluator still enforces coverage gaps.
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
            "fill_provenance": "not_collected_admission_is_not_a_fill",
            "entry_edge_established": False}
