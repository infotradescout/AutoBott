"""Offline primary-option study using production selection and admission functions.

No broker, runtime activation, account writes, guessed fills, or automatic tuning.
Every input case remains visible. Opportunity assessment never executes an exit.
"""
from __future__ import annotations

import argparse
from collections import Counter
from copy import deepcopy
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timedelta
import hashlib
import json
import math
from pathlib import Path
from statistics import median
from types import SimpleNamespace
from typing import Any, Mapping

from .bar_timing import aware_utc
from .core_runner import CoreRunnerRules, select_core_runner_pair
from .entry_admission import (EntryMarketRejected, EntryMarketRules, _completed_evidence,
                              filter_entry_quote_candidates, refresh_entry_admission)
from .entry_quality import EntryQualityRules, evaluate_entry_quality, summarize_entry_quality
from .phase1_engine import build_decision_card as legacy_builder
from .phase1_engine_v2 import build_decision_card as v2_builder
from .phase1_models import DecisionStatus, Phase1Rules
from .phase1_snapshot_contract import validate_market_snapshot
from .phase1_validate import _decision_input_from_snapshot


def digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, allow_nan=False,
                                     separators=(",", ":")).encode()).hexdigest()


@dataclass(frozen=True)
class PrimaryStudyProtocol:
    study_id: str
    development_last_entry_at: str
    evaluation_start: str
    evaluation_end: str
    fill_basis: str
    max_fill_delay_seconds: float
    minimum_paired_scorable: int
    quality: EntryQualityRules
    decision: Phase1Rules
    pair: CoreRunnerRules
    admission: EntryMarketRules

    def __post_init__(self) -> None:
        if not isinstance(self.study_id, str) or not self.study_id.strip():
            raise ValueError("study_id_required")
        if not isinstance(self.quality, EntryQualityRules) or not isinstance(self.decision, Phase1Rules):
            raise ValueError("explicit_typed_study_rules_required")
        if not isinstance(self.pair, CoreRunnerRules) or not isinstance(self.admission, EntryMarketRules):
            raise ValueError("explicit_pair_and_admission_rules_required")
        self.pair.validate()
        start, end = aware_utc(self.evaluation_start), aware_utc(self.evaluation_end)
        development = aware_utc(self.development_last_entry_at)
        if not development + timedelta(seconds=self.quality.holding_seconds) <= start < end:
            raise ValueError("development_windows_overlap_evaluation_or_invalid_window")
        if self.fill_basis not in {"recorded_primary_fill", "refresh_ask_shadow"}:
            raise ValueError("explicit_supported_fill_basis_required")
        if type(self.minimum_paired_scorable) is not int or self.minimum_paired_scorable <= 0:
            raise ValueError("positive_minimum_paired_scorable_required")
        v = self.max_fill_delay_seconds
        if isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v) or v < 0:
            raise ValueError("invalid_fill_delay_bound")
        digest(asdict(self))  # Reject non-JSON/NaN protocol values before any outcomes.

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "PrimaryStudyProtocol":
        values = dict(payload)
        # Do not silently fill missing persisted settings from evolving defaults.
        for name, kind in (("quality", EntryQualityRules), ("decision", Phase1Rules),
                           ("pair", CoreRunnerRules), ("admission", EntryMarketRules)):
            raw = values[name]
            if set(raw) != set(kind.__dataclass_fields__):
                raise ValueError(f"incomplete_or_unknown_{name}_protocol_fields")
            values[name] = kind(**raw)
        return cls(**values)

    @property
    def config_hash(self) -> str:
        return digest(asdict(self))


class RecordedRefresh:
    """Only recorded entry-time responses; never holds outcome observations."""
    def __init__(self, refresh: Mapping[str, Any]) -> None:
        self.option_feed = refresh["options_feed"]
        self._options = deepcopy(refresh["option_quotes"])
        self._stocks = deepcopy(refresh["stock_quotes"])
        if not isinstance(self._options, dict) or not isinstance(self._stocks, dict):
            raise ValueError("recorded_quote_maps_required")

    def get_latest_option_quotes(self, symbols: list[str]) -> dict:
        return {s: deepcopy(self._options[s]) for s in symbols if s in self._options}

    def get_latest_stock_quotes(self, symbols: list[str]) -> dict:
        return {s: deepcopy(self._stocks[s]) for s in symbols if s in self._stocks}


def evaluate_primary_case(case: Mapping[str, Any], protocol: PrimaryStudyProtocol,
                          *, engine: str) -> dict[str, Any]:
    if engine not in {"legacy", "v2"}:
        raise ValueError("unknown_entry_engine")
    result: dict[str, Any] = {"sample_id": case["sample_id"], "status": "invalid_evidence",
                             "reason": None, "primary_option_symbol": None, "quality": None}
    try:
        snapshot = deepcopy(case["snapshot"])
        validate_market_snapshot(snapshot)
        timestamp = aware_utc(snapshot["timestamp"])
        result.update(ticker=snapshot["ticker"], decision_timestamp=timestamp.isoformat())
        if not aware_utc(protocol.evaluation_start) <= timestamp < aware_utc(protocol.evaluation_end):
            return {**result, "status": "outside_evaluation_window", "reason": "entry_time_outside_fixed_window"}
        parsed = _decision_input_from_snapshot(snapshot)
        _completed_evidence(snapshot, SimpleNamespace(timestamp=parsed.timestamp))
        parsed, exclusions = filter_entry_quote_candidates(parsed, snapshot, rules=protocol.admission)
        result["candidate_quote_filter"] = exclusions
        decision = (v2_builder if engine == "v2" else legacy_builder)(parsed, protocol.decision)
        result["decision"] = decision.to_json_dict()
        if decision.decision is not DecisionStatus.TRADE_CANDIDATE:
            return {**result, "status": "no_candidate", "reason": decision.blocked_reason}
        pair = select_core_runner_pair(decision.selected_contract, parsed.option_chain, rules=protocol.pair)
        if pair is None:
            return {**result, "status": "no_pair", "reason": "core_runner_pair_not_found"}
        result.update(primary_option_symbol=pair.primary.option_symbol, runner_option_symbol=pair.runner.option_symbol)
        decision = replace(decision, selected_contract=pair.primary)
        refresh = case.get("refresh")
        if not isinstance(refresh, Mapping):
            return {**result, "status": "missing_refresh_evidence", "reason": "no_recorded_refresh"}
        if refresh.get("options_feed") not in {"opra", "indicative"}:
            return {**result, "status": "missing_refresh_evidence", "reason": "known_options_feed_required"}
        if snapshot.get("source", {}).get("options_feed") != refresh["options_feed"]:
            return {**result, "reason": "entry_snapshot_and_refresh_feed_mismatch"}
        requested, received = aware_utc(refresh["requested_at"]), aware_utc(refresh["received_at"])
        if requested < timestamp or received < requested:
            raise ValueError("refresh_clock_precedes_decision_or_regresses")
        # The recorded authorization must exist for both EXACT selected contracts.
        # Missing comparator quotes are unknown, not a fabricated failed entry.
        symbols = (pair.primary.option_symbol, pair.runner.option_symbol)
        if any(s not in refresh["option_quotes"] or s not in refresh["authorized_prices"] for s in symbols):
            return {**result, "status": "missing_refresh_evidence", "reason": "selected_contract_refresh_not_recorded"}
        clock = iter((requested, received))
        admission = refresh_entry_admission(decision, pair, snapshot, RecordedRefresh(refresh),
            decision_rules=protocol.decision, pair_rules=protocol.pair,
            authorized_prices=refresh["authorized_prices"], now_fn=lambda: next(clock), rules=protocol.admission)
        result["admission"] = admission
        primary_quote = next(q for q in admission["quotes"] if q["option_symbol"] == pair.primary.option_symbol)
        if protocol.fill_basis == "recorded_primary_fill":
            fills = [f for f in case.get("fills", []) if f.get("option_symbol") == pair.primary.option_symbol]
            if len(fills) != 1:
                return {**result, "status": "missing_or_ambiguous_fill", "reason": "exactly_one_primary_fill_required"}
            fill = fills[0]
            if fill.get("side") != "buy_to_open" or type(fill.get("quantity")) is not int or fill["quantity"] != 1:
                raise ValueError("single_standard_primary_buy_fill_required")
            if not isinstance(fill.get("broker_order_id"), str) or not fill["broker_order_id"].strip():
                raise ValueError("recorded_fill_order_identity_required")
            start = aware_utc(fill["timestamp"])
            if not 0 <= (start - received).total_seconds() <= protocol.max_fill_delay_seconds:
                raise ValueError("recorded_fill_time_outside_declared_admission_window")
            price, evidence_kind, fill_model = fill["price"], "broker_recorded_fill", "supplied_recorded_primary_fill"
        else:
            start, price = received, primary_quote["ask"]
            evidence_kind, fill_model = "simulated_fill", "recorded_refresh_ask_assumption_no_order"
        if isinstance(price, bool) or not isinstance(price, (int, float)) or not math.isfinite(price) or price <= 0:
            raise ValueError("invalid_primary_entry_price")
        result.update(entry_timestamp=start.isoformat(), entry_price=price, fill_model=fill_model)
        if start + timedelta(seconds=protocol.quality.holding_seconds) > aware_utc(protocol.evaluation_end):
            return {**result, "status": "boundary_censored", "reason": "full_opportunity_window_exceeds_evaluation_end"}
        observations = case.get("outcome_snapshots")
        if not isinstance(observations, list):
            return {**result, "status": "missing_outcome_evidence", "reason": "option_path_not_recorded"}
        for observation in observations:
            if not isinstance(observation, dict):
                raise ValueError("invalid_outcome_observation")
            if observation.get("ticker") != decision.ticker:
                continue
            if start <= aware_utc(observation["timestamp"]) <= start + timedelta(seconds=protocol.quality.holding_seconds):
                if observation.get("source", {}).get("options_feed") != refresh["options_feed"]:
                    return {**result, "status": "missing_outcome_evidence", "reason": "mixed_or_unknown_outcome_feed"}
        # Include the ACTUAL refresh observation at its recorded receipt time.
        # A later fill does not move this quote forward to manufacture a t=0 quote.
        anchor = {"ticker": snapshot["ticker"], "timestamp": received.isoformat(),
                  "option_chain": admission["quotes"]}
        entry = {"decision_id": decision.decision_id, "ticker": decision.ticker,
                 "timestamp": start.isoformat(), "selected_contract": {"option_symbol": pair.primary.option_symbol},
                 "leg_role": "primary", "filled": True, "entry_fill_price": price,
                 "entry_fill_model": fill_model}
        quality = evaluate_entry_quality(entry, [anchor, *deepcopy(observations)], protocol.quality,
                                         is_primary=True, evidence_kind=evidence_kind)
        result.update(status="evaluated", reason=quality["reason"], quality=quality)
        return result
    except EntryMarketRejected as exc:
        return {**result, "status": "admission_rejected", "reason": exc.reason, "detail": exc.detail}
    except (ValueError, TypeError, KeyError, AttributeError, OverflowError, StopIteration) as exc:
        return {**result, "reason": f"{type(exc).__name__}:{exc}"}


def case_from_runtime_evidence(*, sample_id: str, snapshot: Mapping[str, Any],
                               admission_event: Mapping[str, Any], outcome_snapshots: list[dict],
                               fills: list[dict]) -> dict[str, Any]:
    """Export one recorded admission; caller must disclose admitted-only cohorts.

    This verifies snapshot binding, not broker provenance, full scan coverage,
    authenticity of supplied fills, or observation continuity after manual exits.
    """
    capsule = admission_event.get("recorded_refresh")
    if not isinstance(capsule, Mapping) or capsule.get("snapshot_hash") != digest(snapshot):
        raise ValueError("runtime_refresh_missing_or_bound_to_another_snapshot")
    return {"sample_id": sample_id, "snapshot": deepcopy(snapshot),
            "refresh": deepcopy(capsule), "outcome_snapshots": deepcopy(outcome_snapshots),
            "fills": deepcopy(fills)}


def source_fingerprint() -> str:
    root = Path(__file__).parent
    return digest({p.name: hashlib.sha256(p.read_bytes().replace(b"\r\n", b"\n")).hexdigest()
                   for p in sorted(root.glob("*.py"))})


def run_primary_study(tape: Mapping[str, Any], protocol: PrimaryStudyProtocol,
                      *, engine: str, output_dir: str | Path | None = None) -> dict[str, Any]:
    if engine not in {"legacy", "v2"}:
        raise ValueError("unknown_entry_engine")
    if tape.get("schema_version") != "primary_entry_tape.v1" or tape.get("source_kind") not in {"synthetic", "recorded_market"}:
        raise ValueError("explicit_tape_schema_and_source_required")
    if tape.get("cohort_scope") not in {"all_recorded_scans", "admitted_entries_only"}:
        raise ValueError("explicit_cohort_scope_required")
    cases = deepcopy(tape["cases"])
    if not isinstance(cases, list) or any(not isinstance(c, dict) or not isinstance(c.get("sample_id"), str) or not c["sample_id"].strip() for c in cases):
        raise ValueError("identified_cases_required")
    ids = [c["sample_id"] for c in cases]
    if len(ids) != len(set(ids)):
        raise ValueError("duplicate_sample_id")
    # Same snapshot may not be reweighted under a different friendly ID.
    identities = []
    for case in cases:
        snap = case.get("snapshot")
        if not isinstance(snap, dict):
            raise ValueError("snapshot_object_required")
        identities.append((snap.get("ticker"), aware_utc(snap["timestamp"]).isoformat()))
    if len(identities) != len(set(identities)):
        raise ValueError("duplicate_snapshot_identity")
    order_ids = [f.get("broker_order_id") for case in cases for f in case.get("fills", [])
                 if isinstance(f, dict) and f.get("broker_order_id")]
    if len(order_ids) != len(set(order_ids)):
        raise ValueError("duplicate_recorded_fill_order_identity")
    manifest = {"schema_version": "primary_entry_study.v1", "engine": engine,
                "protocol": asdict(protocol), "protocol_hash": protocol.config_hash,
                "tape_hash": digest(tape), "entry_source_hash": source_fingerprint(),
                "source_kind": tape["source_kind"], "cohort_scope": tape["cohort_scope"],
                "cohort_completeness_verified": False, "source_provenance_verified": False,
                "preregistration_verified": False,
                "selection_and_admission": "production_functions",
                "full_account_broker_replay": False, "independent_samples_assumed": False,
                "exit_policy": "not_executed_primary_opportunity_only"}
    output = Path(output_dir) if output_dir is not None else None
    if output is not None:
        output.mkdir(parents=True, exist_ok=False)  # Never overwrite a prior study.
        (output / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    # Decision functions only receive entry-time fields; future paths are used
    # solely after exact primary selection and recorded admission.
    results = [evaluate_primary_case(c, protocol, engine=engine) for c in sorted(cases, key=lambda c: c["sample_id"])]
    quality = [r["quality"] for r in results if r["quality"] is not None]
    statuses = dict(Counter(r["status"] for r in results))
    def middle(key: str) -> float | None:
        values = [q[key] for q in quality if q["status"] in {"pass", "fail"} and q.get(key) is not None]
        return median(values) if values else None
    summary = {"samples_recorded": len(cases), "sample_statuses": statuses,
               "primary_quality": summarize_entry_quality(quality)["primary"],
               "confirmed_opportunities_per_recorded_sample": sum(q["status"] == "pass" for q in quality) / len(cases) if cases else None,
               "median_time_to_first_target_seconds": middle("first_target_seconds"),
               "median_pre_opportunity_adverse_return": middle("adverse_before_opportunity_pct"),
               "edge_established": False, "realized_pnl_computed": False}
    report = {"manifest": manifest, "summary": summary, "results": results}
    report["report_hash"] = digest(report)
    if output is not None:
        (output / "results.jsonl").write_text("".join(json.dumps(r, sort_keys=True, allow_nan=False) + "\n" for r in results), encoding="utf-8")
        (output / "report.json").write_text(json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")
    return report


def compare_primary_studies(baseline: Mapping[str, Any], candidate: Mapping[str, Any]) -> dict[str, Any]:
    for report in (baseline, candidate):
        if report.get("report_hash") != digest({k: v for k, v in report.items() if k != "report_hash"}):
            raise ValueError("study_report_integrity_mismatch")
        if report["manifest"]["protocol_hash"] != digest(report["manifest"]["protocol"]):
            raise ValueError("study_protocol_integrity_mismatch")
    for key in ("protocol_hash", "tape_hash", "source_kind", "cohort_scope", "exit_policy"):
        if baseline["manifest"][key] != candidate["manifest"][key]:
            raise ValueError(f"incomparable_{key}")
    left, right = ({r["sample_id"]: r for r in report["results"]} for report in (baseline, candidate))
    if left.keys() != right.keys() or len(left) != len(baseline["results"]) or len(right) != len(candidate["results"]):
        raise ValueError("incomparable_case_cohorts")
    paired = [(left[k]["quality"], right[k]["quality"]) for k in sorted(left)
              if left[k]["quality"] and right[k]["quality"]
              and left[k]["quality"]["status"] in {"pass", "fail"}
              and right[k]["quality"]["status"] in {"pass", "fail"}]
    delta = sum(int(b["passed"]) - int(a["passed"]) for a, b in paired) / len(paired) if paired else None
    enough = len(paired) >= candidate["manifest"]["protocol"]["minimum_paired_scorable"]
    return {"baseline_engine": baseline["manifest"]["engine"], "candidate_engine": candidate["manifest"]["engine"],
            "baseline_source_hash": baseline["manifest"]["entry_source_hash"],
            "candidate_source_hash": candidate["manifest"]["entry_source_hash"],
            "paired_scorable_primary_entries": len(paired), "paired_pass_fraction_delta": delta,
            "baseline_all_samples": baseline["summary"], "candidate_all_samples": candidate["summary"],
            "conclusion": "insufficient_paired_evidence" if not enough else "descriptive_comparison_only",
            "edge_established": False, "note": "Named-engine/source comparison, not automatically a prior-release comparison or statistical proof."}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Offline primary-option selection/admission/opportunity study; no orders.")
    parser.add_argument("--tape", type=Path)
    parser.add_argument("--protocol", type=Path)
    parser.add_argument("--engine", choices=["legacy", "v2"])
    parser.add_argument("--compare", nargs=2, type=Path, metavar=("BASELINE_REPORT", "CANDIDATE_REPORT"))
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.compare:
        if any(value is not None for value in (args.tape, args.protocol, args.engine)):
            parser.error("--compare cannot be mixed with study-generation arguments")
        reports = [json.loads(path.read_text(encoding="utf-8")) for path in args.compare]
        comparison = compare_primary_studies(*reports)
        args.output.mkdir(parents=True, exist_ok=False)
        (args.output / "comparison.json").write_text(json.dumps(comparison, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(json.dumps(comparison, indent=2, sort_keys=True))
        return 0
    if any(value is None for value in (args.tape, args.protocol, args.engine)):
        parser.error("--tape, --protocol and --engine are required to generate a study")
    protocol = PrimaryStudyProtocol.from_dict(json.loads(args.protocol.read_text(encoding="utf-8")))
    report = run_primary_study(json.loads(args.tape.read_text(encoding="utf-8")), protocol,
                               engine=args.engine, output_dir=args.output)
    print(json.dumps(report["summary"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
