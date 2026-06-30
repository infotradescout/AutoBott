from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .phase1_bucket_eligibility import (
    EXIT_POLICY_VERSION,
    FILL_MODEL_ORDER,
    PRIMARY_FILL_MODEL,
    BucketEligibilityRules,
    build_bucket_edge_report,
    build_gate_candidate_report,
)
from .phase1_replay import run_replay


def run_replay_campaign(
    snapshots: str | Path | list[str | Path],
    *,
    artifacts_root: str | Path | None = None,
    campaign_run_id: str = "default",
    active_gate_path: str | Path | None = None,
    snapshot_source_label: str | None = None,
    campaign_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    snapshots_label = snapshot_source_label or _snapshot_source_label(snapshots)
    campaign_dir = (Path(artifacts_root) if artifacts_root is not None else Path("artifacts") / "phase1_replay_campaign") / campaign_run_id
    fill_model_results_dir = campaign_dir / "fill_model_results"
    fill_model_results_dir.mkdir(parents=True, exist_ok=True)

    gate_target = Path(active_gate_path) if active_gate_path is not None else Path(__file__).resolve().parents[2] / "data" / "PHASE1_CYCLE_GATE.json"
    gate_before = _file_hash(gate_target)

    replay_runs: dict[str, Any] = {}
    fill_model_payloads: dict[str, dict[str, Any]] = {}
    for fill_model in FILL_MODEL_ORDER:
        replay_runs[fill_model] = run_replay(
            snapshots,
            artifacts_root=fill_model_results_dir,
            run_id=fill_model,
            fill_model=fill_model,
            promote_gate=False,
        )
        fill_model_payloads[fill_model] = _load_fill_model_payload(fill_model_results_dir / fill_model)

    rules = BucketEligibilityRules()
    edge_report = build_bucket_edge_report(
        campaign_run_id=campaign_run_id,
        fill_model_payloads=fill_model_payloads,
        rules=rules,
    )
    gate_candidate_report = build_gate_candidate_report(
        campaign_run_id=campaign_run_id,
        edge_report=edge_report,
        rules=rules,
    )
    _apply_campaign_safety_overrides(gate_candidate_report, campaign_context)
    gate_candidate_report["created_at"] = _created_at()

    manifest = _campaign_manifest(campaign_run_id, snapshots_label, fill_model_payloads, replay_runs, edge_report, rules, campaign_context)
    gate_after = _file_hash(gate_target)
    gate_mutation_status = "not_mutated" if gate_before == gate_after else "mutated"

    campaign_dir.mkdir(parents=True, exist_ok=True)
    (campaign_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    (campaign_dir / "bucket_edge_report.json").write_text(json.dumps(edge_report, indent=2, sort_keys=True), encoding="utf-8")
    (campaign_dir / "gate_candidate_report.json").write_text(json.dumps(gate_candidate_report, indent=2, sort_keys=True), encoding="utf-8")
    (campaign_dir / "replay_campaign_summary.md").write_text(
        _summary(campaign_run_id, manifest, replay_runs, edge_report, gate_candidate_report, gate_mutation_status),
        encoding="utf-8",
    )

    return {
        "campaign_run_id": campaign_run_id,
        "artifact_dir": str(campaign_dir),
        "gate_mutation_status": gate_mutation_status,
        "bucket_count": len(edge_report["buckets"]),
        "eligible_for_paper_forward": len([bucket for bucket in gate_candidate_report["bucket_candidates"].values() if bucket["eligible_for_paper_forward"]]),
        "eligible_for_live_review": len([bucket for bucket in gate_candidate_report["bucket_candidates"].values() if bucket["eligible_for_live_review"]]),
    }


def _load_fill_model_payload(path: Path) -> dict[str, Any]:
    return {
        "decisions": _read_jsonl(path / "decisions.jsonl"),
        "orders": _read_jsonl(path / "orders.jsonl"),
        "positions": _read_jsonl(path / "positions.jsonl"),
        "outcomes": _read_jsonl(path / "outcomes.jsonl"),
        "scorecard": _read_json(path / "scorecard.json"),
        "gate_result": _read_json(path / "gate_result.json"),
        "manifest": _read_json(path / "manifest.json"),
    }


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def _campaign_manifest(
    campaign_run_id: str,
    snapshots_label: str,
    fill_model_payloads: dict[str, dict[str, Any]],
    replay_runs: dict[str, Any],
    edge_report: dict[str, Any],
    rules: BucketEligibilityRules,
    campaign_context: dict[str, Any] | None,
) -> dict[str, Any]:
    manifests = [payload["manifest"] for payload in fill_model_payloads.values() if payload.get("manifest")]
    input_snapshot_hash = manifests[0].get("input_snapshot_hash") if manifests else None
    corpus_summary = (campaign_context or {}).get("corpus_summary", {})
    corpus_quality = corpus_summary.get("quality", {})
    config = {
        "fill_models": list(FILL_MODEL_ORDER),
        "primary_fill_model": PRIMARY_FILL_MODEL,
        "exit_policy_version": rules.exit_policy_version,
        "paper_thresholds": {
            "closed_trades": rules.paper_min_closed_trades,
            "profit_factor": rules.paper_min_profit_factor,
            "expectancy": rules.paper_min_expectancy,
            "min_fill_rate": rules.min_fill_rate,
            "max_unresolved_position_rate": rules.max_unresolved_position_rate,
        },
        "live_review_thresholds": {
            "closed_trades": rules.live_min_closed_trades,
            "profit_factor": rules.live_min_profit_factor,
            "expectancy": rules.live_min_expectancy,
            "min_trading_days": rules.min_live_trading_days,
        },
    }
    config_hash = hashlib.sha256(json.dumps(config, sort_keys=True).encode("utf-8")).hexdigest()
    fill_models_completed = all(replay_runs.get(fill_model, {}).get("artifact_dir") for fill_model in FILL_MODEL_ORDER)
    buckets = edge_report.get("buckets", {})
    unresolved_reported = bool(buckets) and all(
        "unresolved_position_rate" in bucket["metrics_by_fill_model"][PRIMARY_FILL_MODEL] for bucket in buckets.values()
    )
    corpus_quality_summary = {
        "corpus_type": corpus_summary.get("corpus_type", "historical_replay"),
        "symbols": corpus_summary.get("symbols", []),
        "trading_days": corpus_quality.get("trading_days"),
        "expected_intervals": corpus_quality.get("expected_intervals"),
        "captured_intervals": corpus_quality.get("captured_intervals"),
        "missing_interval_count": corpus_quality.get("missing_interval_count"),
        "snapshot_coverage_pct": corpus_quality.get("snapshot_coverage_pct"),
        "option_quote_coverage_pct": corpus_quality.get("option_quote_coverage_pct", corpus_quality.get("option_quote_coverage")),
        "stale_quote_rate": corpus_quality.get("stale_quote_rate"),
        "schema_versions": corpus_quality.get("schema_versions", {}),
        "quality_flags": corpus_quality.get("quality_flags", corpus_quality.get("data_quality_flags", [])),
    }
    return {
        "schema_version": "phase1_replay_campaign_manifest.v1",
        "campaign_run_id": campaign_run_id,
        "created_at": _created_at(),
        "engine_version": "phase1_engine.v1",
        "decision_schema_version": "phase1_decision_card.v1",
        "snapshot_schema_version": manifests[0].get("snapshot_schema_version") if manifests else None,
        "replay_campaign_config_hash": config_hash,
        "input_snapshot_hash": input_snapshot_hash,
        "snapshots_path": snapshots_label,
        "fill_model_roles": {
            "optimistic_mid": "diagnostic only",
            "realistic_mid_penalty": "primary eligibility model",
            "conservative": "robustness check",
            "stress": "adverse robustness check",
        },
        "exit_policy_version": EXIT_POLICY_VERSION,
        "exit_policy": {
            "tactical_profit_target_pct": 35,
            "tactical_stop_loss_pct": 35,
            "tactical_eod_flatten": True,
            "rider_profit_target_pct": 60,
            "rider_stop_loss_pct": 40,
            "rider_dte_floor": 5,
        },
        "corpus_type": corpus_summary.get("corpus_type", "historical_replay"),
        "corpus_quality": corpus_quality_summary,
        "campaign_quality": {
            "campaign_valid": bool(corpus_quality.get("campaign_ready", True)) and fill_models_completed and unresolved_reported,
            "corpus_ready": bool(corpus_quality.get("campaign_ready", True)),
            "fill_models_completed": fill_models_completed,
            "unresolved_position_rate_reported": unresolved_reported,
            "blocking_reasons": corpus_quality.get("blocking_reasons", []),
            "data_quality_flags": corpus_quality.get("data_quality_flags", []),
            "option_quote_coverage": corpus_quality.get("option_quote_coverage"),
            "stale_quote_rate": corpus_quality.get("stale_quote_rate"),
            "major_missing_time_blocks": corpus_quality.get("major_missing_time_blocks"),
        },
        "symbols": sorted({symbol for payload in fill_model_payloads.values() for symbol in payload.get("manifest", {}).get("symbols", [])}),
        "replay_runs": replay_runs,
    }


def _summary(
    campaign_run_id: str,
    manifest: dict[str, Any],
    replay_runs: dict[str, Any],
    edge_report: dict[str, Any],
    gate_candidate_report: dict[str, Any],
    gate_mutation_status: str,
) -> str:
    best_buckets = _best_buckets(edge_report)
    blocked_buckets = _blocked_buckets(edge_report)
    eligible_buckets = [
        key for key, candidate in gate_candidate_report["bucket_candidates"].items() if candidate["eligible_for_paper_forward"]
    ]
    unresolved_notes = _unresolved_notes(edge_report)
    outlier_notes = _outlier_notes(edge_report)
    lines = [
        "Campaign Manifest",
        f"Campaign Run ID: {campaign_run_id}",
        f"Primary fill model: {PRIMARY_FILL_MODEL}",
        f"Exit policy version: {manifest['exit_policy_version']}",
        "",
        "Snapshot Coverage",
        f"Symbols covered: {', '.join(manifest.get('symbols', []))}",
        f"Snapshot source: {manifest.get('snapshots_path')}",
        f"Corpus type: {manifest.get('corpus_type', 'historical_replay')}",
        "",
        "Data Quality",
        f"Campaign valid: {manifest.get('campaign_quality', {}).get('campaign_valid')}",
        f"Corpus ready: {manifest.get('campaign_quality', {}).get('corpus_ready')}",
        f"Trading days: {manifest.get('corpus_quality', {}).get('trading_days')}",
        f"Expected intervals: {manifest.get('corpus_quality', {}).get('expected_intervals')}",
        f"Captured intervals: {manifest.get('corpus_quality', {}).get('captured_intervals')}",
        f"Snapshot coverage: {manifest.get('corpus_quality', {}).get('snapshot_coverage_pct')}",
        f"Option quote coverage: {manifest.get('corpus_quality', {}).get('option_quote_coverage_pct')}",
        f"Stale quote rate: {manifest.get('campaign_quality', {}).get('stale_quote_rate')}",
        f"Major missing time blocks: {manifest.get('campaign_quality', {}).get('major_missing_time_blocks')}",
        f"Data quality flags: {', '.join(manifest.get('corpus_quality', {}).get('quality_flags', [])) or 'None'}",
        f"Campaign blocking reasons: {', '.join(manifest.get('campaign_quality', {}).get('blocking_reasons', [])) or 'None'}",
        "",
        "Fill Model Comparison",
        "optimistic_mid              diagnostic only",
        "realistic_mid_penalty       primary eligibility model",
        "conservative                robustness check",
        "stress                      adverse robustness check",
        "",
        "Overall Replay Results",
    ]
    for fill_model in FILL_MODEL_ORDER:
        run = replay_runs[fill_model]
        lines.append(
            f"{fill_model}: decisions={run['decisions_generated']}, orders={run['orders_attempted']}, fills={run['orders_filled']}, closed_trades={run['closed_trades']}, gate_reason={run['gate_reason']}"
        )
    lines.extend(
        [
            "",
            "Best Buckets",
            *(best_buckets or ["None"]),
            "",
            "Blocked Buckets",
            *(blocked_buckets or ["None"]),
            "",
            "Eligibility Candidates",
            *(eligible_buckets or ["None"]),
            "",
            "Rejection/Unresolved Diagnostics",
            *(unresolved_notes or ["None"]),
            "",
            "Outlier Dependency Notes",
            *(outlier_notes or ["None"]),
            "",
            "Gate Mutation Status",
            gate_mutation_status,
        ]
    )
    return "\n".join(lines)


def _best_buckets(edge_report: dict[str, Any]) -> list[str]:
    ranked = []
    for bucket_key, bucket in edge_report["buckets"].items():
        metrics = bucket["metrics_by_fill_model"][PRIMARY_FILL_MODEL]
        if metrics["closed_trades"] > 0:
            ranked.append((metrics["expectancy"], bucket_key))
    return [item[1] for item in sorted(ranked, reverse=True)[:3]]


def _blocked_buckets(edge_report: dict[str, Any]) -> list[str]:
    blocked = []
    for bucket_key, bucket in edge_report["buckets"].items():
        reasons = bucket["eligibility"]["blocking_reasons"]
        if reasons:
            blocked.append(f"{bucket_key}: {', '.join(reasons)}")
    return blocked[:5]


def _unresolved_notes(edge_report: dict[str, Any]) -> list[str]:
    notes = []
    for bucket_key, bucket in edge_report["buckets"].items():
        primary = bucket["metrics_by_fill_model"][PRIMARY_FILL_MODEL]
        if primary["unresolved_position_rate"] > 0:
            notes.append(f"{bucket_key}: unresolved_rate={primary['unresolved_position_rate']}")
    return notes


def _outlier_notes(edge_report: dict[str, Any]) -> list[str]:
    notes = []
    for bucket_key, bucket in edge_report["buckets"].items():
        primary = bucket["metrics_by_fill_model"][PRIMARY_FILL_MODEL]
        if primary["largest_win_pct_of_total_net_profit"] >= 0.50 and primary["net_profit"] > 0:
            notes.append(f"{bucket_key}: largest_win_pct_of_total_net_profit={primary['largest_win_pct_of_total_net_profit']}")
    return notes


def _file_hash(path: Path) -> str | None:
    if not path.exists():
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _created_at() -> str:
    return datetime.now(timezone.utc).isoformat()


def _snapshot_source_label(snapshots: str | Path | list[str | Path]) -> str:
    if isinstance(snapshots, list):
        if not snapshots:
            return "<empty>"
        return f"<explicit-snapshot-list:{len(snapshots)}>"
    return str(snapshots)


def _apply_campaign_safety_overrides(gate_candidate_report: dict[str, Any], campaign_context: dict[str, Any] | None) -> None:
    corpus_summary = (campaign_context or {}).get("corpus_summary", {})
    corpus_type = corpus_summary.get("corpus_type", "historical_replay")
    gate_candidate_report["corpus_type"] = corpus_type
    if corpus_type != "test_fixture":
        return
    for candidate in gate_candidate_report["bucket_candidates"].values():
        candidate["eligible_for_live_review"] = False
        reasons = list(candidate.get("reasons", []))
        if "test_fixture_corpus_blocks_live_review" not in reasons:
            reasons.append("test_fixture_corpus_blocks_live_review")
        candidate["reasons"] = sorted(set(reasons))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run a low-level Phase 1 replay campaign from raw replayable snapshots.")
    parser.add_argument("--snapshots", required=True, help="Path to a directory of replayable snapshot JSON files.")
    parser.add_argument("--out", help="Artifacts root directory. Defaults to artifacts/phase1_replay_campaign.")
    parser.add_argument("--campaign-run-id", default="default", help="Stable campaign identifier for artifact output.")
    args = parser.parse_args(argv)
    result = run_replay_campaign(
        args.snapshots,
        artifacts_root=args.out,
        campaign_run_id=args.campaign_run_id,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
