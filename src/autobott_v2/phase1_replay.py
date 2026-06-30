from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from pathlib import Path
from typing import Any

from .phase1_engine import build_decision_card
from .phase1_execution_sim import ExecutionSimRules, simulate_execution
from .phase1_exit_engine import ExitRules, evaluate_exit
from .phase1_models import LifecycleStatus
from .phase1_scorecard import load_phase1_gate, update_phase1_gate
from .phase1_validate import _decision_input_from_snapshot, _load_snapshot, _parse_datetime


def run_replay(
    snapshots: str | Path | list[str | Path],
    *,
    artifacts_root: str | Path | None = None,
    run_id: str = "default",
    fill_model: str = "realistic_mid_penalty",
    promote_gate: bool = False,
    active_gate_path: str | Path | None = None,
) -> dict[str, Any]:
    snapshot_paths = _snapshot_paths(snapshots)
    artifact_dir = (Path(artifacts_root) if artifacts_root is not None else Path("artifacts") / "phase1_replay") / run_id
    artifact_dir.mkdir(parents=True, exist_ok=True)

    decisions_path = artifact_dir / "decisions.jsonl"
    orders_path = artifact_dir / "orders.jsonl"
    fills_path = artifact_dir / "fills.jsonl"
    positions_path = artifact_dir / "positions.jsonl"
    outcomes_path = artifact_dir / "outcomes.jsonl"
    manifest_path = artifact_dir / "manifest.json"

    decisions: list[dict[str, Any]] = []
    orders: list[dict[str, Any]] = []
    fills: list[dict[str, Any]] = []
    positions: list[dict[str, Any]] = []
    outcomes: list[dict[str, Any]] = []
    terminal_events = []
    open_positions = []
    execution_rules = _execution_rules(fill_model)
    exit_rules = _exit_rules(fill_model)
    snapshots_payload = [_load_snapshot(snapshot_path) for snapshot_path in snapshot_paths]
    manifest = _manifest(run_id, snapshot_paths, snapshots_payload, fill_model, execution_rules, exit_rules)
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

        decision_card = build_decision_card(_decision_input_from_snapshot(snapshot))
        decision_record = {
            "snapshot_path": str(snapshot_path),
            **decision_card.to_json_dict(),
        }
        decisions.append(decision_record)

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
                fills.append(event.to_json_dict())
                positions.append(event.to_json_dict())
                open_positions.append(event)
            else:
                terminal_events.append(event)

    _write_jsonl(decisions_path, decisions)
    _write_jsonl(orders_path, orders)
    _write_jsonl(fills_path, fills)
    _write_jsonl(positions_path, positions)
    _write_jsonl(outcomes_path, outcomes)

    replay_gate_path = artifact_dir / "gate.json"
    scorecard = update_phase1_gate(terminal_events, replay_gate_path)
    scorecard["decision_stats"]["snapshots_processed"] = len(snapshot_paths)
    scorecard["decision_stats"]["decisions_generated"] = len(decisions)
    scorecard["decision_stats"]["no_trade_decisions"] = len([decision for decision in decisions if decision.get("decision") == "NO_TRADE"])
    scorecard["fill_model"] = fill_model
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
        "artifact_dir": str(artifact_dir),
        "fill_model": fill_model,
        "snapshots_processed": len(snapshot_paths),
        "decisions_generated": len(decisions),
        "orders_attempted": len(orders),
        "orders_filled": len(fills),
        "closed_trades": len(outcomes),
        "gate_reason": gate_result.reason,
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
