from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, replace
from pathlib import Path
from typing import Any

from .entry_quality import EntryQualityRules, evaluate_entry_quality, summarize_entry_quality
from .phase1_engine import build_decision_card
from .phase1_execution_sim import ExecutionSimRules, simulate_execution
from .phase1_exit_engine import ExitRules, evaluate_exit
from .phase1_models import LifecycleStatus, Phase1Rules
from .phase1_scorecard import load_phase1_gate, update_phase1_gate
from .phase1_validate import _decision_input_from_snapshot, _load_snapshot, _parse_datetime
from .runtime_paths import artifacts_root as default_artifacts_root
from .thesis_validation import evaluate_decision_thesis, summarize_thesis_results


def run_replay(
    snapshots: str | Path | list[str | Path],
    *,
    artifacts_root: str | Path | None = None,
    run_id: str = "default",
    fill_model: str = "realistic_mid_penalty",
    promote_gate: bool = False,
    active_gate_path: str | Path | None = None,
    entry_quality_rules_by_role: dict[str, EntryQualityRules] | None = None,
    entry_engine: str = "legacy",
    decision_rules: Phase1Rules | None = None,
) -> dict[str, Any]:
    if entry_engine not in {"legacy", "v2"}:
        raise ValueError("entry_engine must be legacy or v2")
    if decision_rules is not None and not isinstance(decision_rules, Phase1Rules):
        raise ValueError("decision_rules must be Phase1Rules")
    resolved_decision_rules = decision_rules if decision_rules is not None else Phase1Rules()
    decision_builder = build_decision_card
    if entry_engine == "v2":
        from .phase1_engine_v2 import build_decision_card as decision_builder
    # Explicit per-role horizons/thresholds: never infer them from future prices.
    entry_quality_rules_by_role = dict(entry_quality_rules_by_role or {})
    if any(role not in {"tactical", "rider"} or not isinstance(rules, EntryQualityRules)
           for role, rules in entry_quality_rules_by_role.items()):
        raise ValueError("entry quality rules must map tactical/rider roles to EntryQualityRules")
    snapshot_paths = _snapshot_paths(snapshots)
    artifact_dir = (Path(artifacts_root) if artifacts_root is not None else default_artifacts_root() / "phase1_replay") / run_id
    artifact_dir.mkdir(parents=True, exist_ok=True)

    decisions_path = artifact_dir / "decisions.jsonl"
    orders_path = artifact_dir / "orders.jsonl"
    fills_path = artifact_dir / "fills.jsonl"
    positions_path = artifact_dir / "positions.jsonl"
    outcomes_path = artifact_dir / "outcomes.jsonl"
    thesis_path = artifact_dir / "thesis_validation.jsonl"
    entry_quality_path = artifact_dir / "entry_quality.jsonl"
    manifest_path = artifact_dir / "manifest.json"

    decisions: list[dict[str, Any]] = []
    orders: list[dict[str, Any]] = []
    fills: list[dict[str, Any]] = []
    positions: list[dict[str, Any]] = []
    outcomes: list[dict[str, Any]] = []
    thesis_results = []
    entry_quality_results = []
    terminal_events = []
    open_positions = []
    execution_rules = _execution_rules(fill_model)
    exit_rules = _exit_rules(fill_model)
    snapshots_payload = [_load_snapshot(snapshot_path) for snapshot_path in snapshot_paths]
    manifest = _manifest(run_id, snapshot_paths, snapshots_payload, fill_model, execution_rules, exit_rules)
    manifest["engine_version"] = "phase1_engine.v1" if entry_engine == "legacy" else "phase1_engine_v2"
    manifest["entry_engine"] = entry_engine
    manifest["decision_rules"] = asdict(resolved_decision_rules)
    manifest["decision_rules_source"] = "explicit" if decision_rules is not None else "Phase1Rules_defaults"
    manifest["entry_config_hash"] = hashlib.sha256(json.dumps({
        "entry_engine": entry_engine, "decision_rules": manifest["decision_rules"],
    }, sort_keys=True, allow_nan=False).encode("utf-8")).hexdigest()
    # This replay still simulates tactical/rider legs, not the hosted core/runner
    # broker lifecycle. Selecting v2 does not make it full production parity.
    manifest["execution_model"] = "phase1_tactical_rider_simulation"
    manifest["hosted_execution_parity_verified"] = False
    # Freeze the assessment protocol alongside unchanged exit/fill settings.
    # This records configuration, not proof of preregistration or a held-out test.
    manifest["entry_quality_protocols"] = {
        role: {"rules": rules.to_json_dict(), "rules_hash": rules.config_hash}
        for role, rules in sorted(entry_quality_rules_by_role.items())
    }
    manifest["entry_quality_preregistration_verified"] = False
    manifest["entry_quality_evidence_kind"] = "simulated_fill"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")

    for snapshot_path, snapshot in zip(snapshot_paths, snapshots_payload):
        timestamp = _parse_datetime(snapshot["timestamp"])

        closed_this_bar = []
        for open_position in open_positions:
            quote_age_seconds = _quote_age_seconds(snapshot, open_position.selected_contract.option_symbol) if open_position.selected_contract else 0
            exit_decision = evaluate_exit(open_position, snapshot, quote_age_seconds=quote_age_seconds, rules=exit_rules)
            if exit_decision.exit_action == "close":
                outcome = replace(
                    open_position,
                    lifecycle_status=LifecycleStatus.CLOSED,
                    exit_option_bid=exit_decision.exit_option_bid,
                    exit_option_ask=exit_decision.exit_option_ask,
                    exit_option_mid=exit_decision.exit_option_mid,
                    exit_spread_pct=exit_decision.exit_spread_pct,
                    exit_fill_model=exit_decision.exit_fill_model,
                    exit_fill_price=exit_decision.exit_fill_price,
                    exit_reason=exit_decision.exit_reason,
                    option_return_pct=exit_decision.option_return_pct,
                    pnl=exit_decision.pnl_dollars,
                    hold_minutes=exit_decision.hold_minutes,
                    underlying_price_at_exit=exit_decision.exit_underlying_price,
                    timestamp=timestamp,
                )
                outcomes.append(outcome.to_json_dict())
                terminal_events.append(outcome)
                closed_this_bar.append(open_position.decision_id)
            elif exit_decision.exit_action == "unresolved":
                unresolved = replace(
                    open_position,
                    lifecycle_status=LifecycleStatus.UNRESOLVED,
                    exit_reason=exit_decision.exit_reason,
                    timestamp=timestamp,
                )
                positions.append(unresolved.to_json_dict())
                terminal_events.append(unresolved)
                closed_this_bar.append(open_position.decision_id)
        open_positions = [event for event in open_positions if event.decision_id not in closed_this_bar]

        decision_input = _decision_input_from_snapshot(snapshot)
        decision_card = (decision_builder(decision_input, rules=resolved_decision_rules)
                         if decision_rules is not None else decision_builder(decision_input))
        decision_record = {
            "snapshot_path": str(snapshot_path),
            **decision_card.to_json_dict(),
        }
        decisions.append(decision_record)
        if decision_card.selected_contract is not None and decision_card.decision.value == "TRADE_CANDIDATE":
            future_snapshots = [item for item in snapshots_payload if _parse_datetime(item["timestamp"]) > timestamp]
            thesis_results.append(evaluate_decision_thesis(decision_card, snapshot, future_snapshots))

        quote_age_seconds = _quote_age_seconds(snapshot, decision_card.selected_contract.option_symbol) if decision_card.selected_contract else 0
        execution_events = simulate_execution(
            decision_card,
            quote_age_seconds=quote_age_seconds,
            underlying_price_at_entry=snapshot["underlying_quote"]["last"],
            timestamp=timestamp,
            rules=execution_rules,
        )
        for event in execution_events:
            orders.append(event.to_json_dict())
            if event.filled:
                # Assess the filled contract and fill basis, never an unfilled
                # candidate or another leg's option. This has no order/gate effect.
                event_row = event.to_json_dict()
                primary_symbol = decision_card.selected_contract.option_symbol if decision_card.selected_contract else None
                entry_quality_results.append(evaluate_entry_quality(
                    event_row,
                    snapshots_payload,
                    entry_quality_rules_by_role.get(event_row.get("leg_role")),
                    is_primary=bool(event.selected_contract and event.selected_contract.option_symbol == primary_symbol),
                    evidence_kind="simulated_fill",
                ))
                fills.append(event_row)
                positions.append(event.to_json_dict())
                open_positions.append(event)
            else:
                terminal_events.append(event)

    _write_jsonl(decisions_path, decisions)
    _write_jsonl(orders_path, orders)
    _write_jsonl(fills_path, fills)
    _write_jsonl(positions_path, positions)
    _write_jsonl(outcomes_path, outcomes)
    _write_jsonl(thesis_path, [result.to_json_dict() for result in thesis_results])
    _write_jsonl(entry_quality_path, entry_quality_results)

    replay_gate_path = artifact_dir / "gate.json"
    scorecard = update_phase1_gate(terminal_events, replay_gate_path)
    scorecard["decision_stats"]["snapshots_processed"] = len(snapshot_paths)
    scorecard["decision_stats"]["decisions_generated"] = len(decisions)
    scorecard["decision_stats"]["no_trade_decisions"] = len([decision for decision in decisions if decision.get("decision") == "NO_TRADE"])
    scorecard["fill_model"] = fill_model
    scorecard["entry_engine"] = entry_engine
    scorecard["entry_config_hash"] = manifest["entry_config_hash"]
    scorecard["execution_model"] = manifest["execution_model"]
    scorecard["hosted_execution_parity_verified"] = False
    thesis_summary = {
        **summarize_thesis_results(thesis_results),
        "measurement_basis": "underlying_direction_only",
        "entry_quality_evidence": False,
    }
    scorecard["thesis_validation"] = thesis_summary
    scorecard["entry_quality"] = summarize_entry_quality(entry_quality_results)
    replay_gate_path.write_text(json.dumps(scorecard, indent=2, sort_keys=True), encoding="utf-8")
    gate_result = load_phase1_gate(replay_gate_path)
    if promote_gate:
        update_phase1_gate(terminal_events, active_gate_path)

    scorecard_path = artifact_dir / "scorecard.json"
    scorecard_path.write_text(json.dumps(scorecard, indent=2, sort_keys=True), encoding="utf-8")
    gate_result_path = artifact_dir / "gate_result.json"
    gate_result_path.write_text(json.dumps({"enabled": gate_result.enabled, "reason": gate_result.reason, "gate": gate_result.gate}, indent=2, sort_keys=True), encoding="utf-8")
    summary_path = artifact_dir / "summary.md"
    summary_path.write_text(_summary(run_id, snapshot_paths, decisions, orders, fills, outcomes, scorecard, gate_result.reason), encoding="utf-8")

    return {
        "run_id": run_id,
        "entry_engine": entry_engine,
        "entry_config_hash": manifest["entry_config_hash"],
        "hosted_execution_parity_verified": False,
        "artifact_dir": str(artifact_dir),
        "fill_model": fill_model,
        "snapshots_processed": len(snapshot_paths),
        "decisions_generated": len(decisions),
        "orders_attempted": len(orders),
        "orders_filled": len(fills),
        "closed_trades": len(outcomes),
        "gate_reason": gate_result.reason,
        "thesis_validation": thesis_summary,
        "entry_quality": scorecard["entry_quality"],
    }


def _snapshot_paths(path_or_paths: str | Path | list[str | Path]) -> list[Path]:
    if isinstance(path_or_paths, list):
        return [Path(item) for item in path_or_paths]
    path = Path(path_or_paths)
    if path.is_dir():
        candidates = sorted(path.rglob("*.json"))
        snapshot_candidates = [candidate for candidate in candidates if "option_quotes" not in candidate.parts and candidate.name != "manifest.json"]
        nested_snapshot_dirs = [candidate for candidate in snapshot_candidates if "snapshots" in candidate.parts]
        return nested_snapshot_dirs or snapshot_candidates
    return [path]


def _quote_age_seconds(snapshot: dict[str, Any], option_symbol: str) -> int:
    snapshot_time = _parse_datetime(snapshot["timestamp"])
    for contract in snapshot["option_chain"]:
        if contract["option_symbol"] == option_symbol:
            quote_time = _parse_datetime(contract["quote_timestamp"])
            return max(0, int((snapshot_time - quote_time).total_seconds()))
    return 0


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    payload = "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows)
    path.write_text(payload, encoding="utf-8")


def _summary(
    run_id: str,
    snapshot_paths: list[Path],
    decisions: list[dict[str, Any]],
    orders: list[dict[str, Any]],
    fills: list[dict[str, Any]],
    outcomes: list[dict[str, Any]],
    scorecard: dict[str, Any],
    gate_reason: str,
) -> str:
    return "\n".join(
        [
            f"Run ID: {run_id}",
            f"Entry engine: {scorecard.get('entry_engine', 'legacy')}",
            "Hosted execution parity: not verified (tactical/rider simulation).",
            f"Fill model: {scorecard.get('fill_model', 'unknown')}",
            f"Snapshots processed: {len(snapshot_paths)}",
            f"Decisions generated: {len(decisions)}",
            f"Orders attempted: {len(orders)}",
            f"Orders filled: {len(fills)}",
            f"Closed trades: {len(outcomes)}",
            f"Open trades remaining: {max(0, len(fills) - len(outcomes))}",
            f"Unresolved positions: {scorecard.get('position_stats', {}).get('positions_unresolved', 0)}",
            f"P/L expectancy: {scorecard.get('expectancy_per_trade', 0.0)}",
            f"Win rate: {scorecard.get('win_rate', 0.0)}",
            f"Profit factor: {scorecard.get('profit_factor', 0.0)}",
            f"Max drawdown: {scorecard.get('max_drawdown_pct_observed', 0.0)}",
            f"Underlying-direction diagnostic (not entry quality): {scorecard.get('thesis_validation', {}).get('pass_rate', 0.0)}",
            f"2DTE thesis pass rate: {scorecard.get('thesis_validation', {}).get('tactical_2dte_pass_rate', 0.0)}",
            f"Reversal thesis pass rate: {scorecard.get('thesis_validation', {}).get('reversal_pass_rate', 0.0)}",
            f"Primary entry opportunity assessment: {scorecard.get('entry_quality', {}).get('primary', {})}",
            "Entry-method edge: not established by opportunity diagnostics alone.",
            f"Gate eligibility result: {gate_reason}",
        ]
    )


def _execution_rules(fill_model: str) -> ExecutionSimRules:
    if fill_model == "optimistic_mid":
        return ExecutionSimRules(entry_slippage_pct=0.0, fill_model=fill_model)
    if fill_model == "conservative":
        return ExecutionSimRules(entry_slippage_pct=1.0, fill_model=fill_model)
    if fill_model == "stress":
        return ExecutionSimRules(entry_slippage_pct=1.0, fill_model=fill_model, max_spread_pct=0.12, min_contract_volume=25, min_open_interest=250)
    return ExecutionSimRules(entry_slippage_pct=0.10, fill_model=fill_model)


def _exit_rules(fill_model: str) -> ExitRules:
    return ExitRules(fill_model=fill_model)


def _manifest(
    run_id: str,
    snapshot_paths: list[Path],
    snapshots_payload: list[dict[str, Any]],
    fill_model: str,
    execution_rules: ExecutionSimRules,
    exit_rules: ExitRules,
) -> dict[str, Any]:
    config = {
        "fill_model": fill_model,
        "execution_rules": execution_rules.__dict__,
        "exit_rules": {
            "tactical_profit_target_pct": exit_rules.tactical_profit_target_pct,
            "tactical_stop_loss_pct": exit_rules.tactical_stop_loss_pct,
            "tactical_eod_flatten_time": exit_rules.tactical_eod_flatten_time.isoformat(),
            "rider_profit_target_pct": exit_rules.rider_profit_target_pct,
            "rider_stop_loss_pct": exit_rules.rider_stop_loss_pct,
            "rider_min_dte": exit_rules.rider_min_dte,
            "max_exit_quote_age_seconds": exit_rules.max_exit_quote_age_seconds,
            "fill_model": exit_rules.fill_model,
        },
    }
    config_hash = hashlib.sha256(json.dumps(config, sort_keys=True).encode("utf-8")).hexdigest()
    snapshot_hash = hashlib.sha256(json.dumps(snapshots_payload, sort_keys=True).encode("utf-8")).hexdigest()
    timestamps = [_parse_datetime(payload["timestamp"]) for payload in snapshots_payload]
    return {
        "run_id": run_id,
        "created_at": timestamps[0].isoformat() if timestamps else None,
        "engine_version": "phase1_engine.v1",
        "decision_schema_version": "phase1_decision_card.v1",
        "snapshot_schema_version": snapshots_payload[0]["schema_version"] if snapshots_payload else None,
        "replay_config_hash": config_hash,
        "input_snapshot_hash": snapshot_hash,
        "fill_model": fill_model,
        "exit_config": config["exit_rules"],
        "symbols": sorted({payload["ticker"] for payload in snapshots_payload}),
        "start_time": min(timestamps).isoformat() if timestamps else None,
        "end_time": max(timestamps).isoformat() if timestamps else None,
        "snapshot_paths": [str(path) for path in snapshot_paths],
    }
