from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .core_runner import load_core_runner_rules, select_core_runner_pair
from .execution_broker import AlpacaExecutionBroker
from .defined_risk_spreads import append_defined_risk_spread_candidate, select_defined_risk_spread
from .execution_models import BrokerEnvironment
from .execution_journal import append_execution_outcome, execution_journal_path
from .execution_reconciler import reconcile_open_positions
from .execution_orchestrator import ExecutionRejectedError, submit_core_runner_to_broker, submit_decision_to_broker
from .ghost_trades import append_ghost_trade, observe_ghost_trades
from .hosted_policy import (
    HOSTED_BAR_TIMEFRAME,
    HOSTED_LOOKBACK_BARS,
    HOSTED_LOOKBACK_CALENDAR_DAYS,
    HOSTED_MIN_OPEN_INTEREST,
    HOSTED_OPEN_DRAWDOWN_LOSS_RATE,
    HOSTED_OPEN_DRAWDOWN_MAX_LOSS,
    HOSTED_OPEN_DRAWDOWN_MIN_LOSERS,
    HOSTED_POLICY_VERSION,
    HOSTED_RIDER_MAX_DTE,
    HOSTED_RIDER_MIN_DTE,
    HOSTED_TACTICAL_MAX_DTE,
    HOSTED_TACTICAL_MIN_DTE,
    is_hosted_paper_runtime,
    is_volatility_symbol,
)
from .jsonl_retention import compact_jsonl_tail, read_jsonl_tail
from .phase1_alpaca_client import AlpacaPaperClient
from .phase1_engine import build_decision_card
from .phase1_models import (
    DecisionCard,
    DecisionStatus,
    DirectionBias,
    ExecutionLayer,
    Phase1Rules,
    TradeSetup,
)
from .phase1_snapshot_capture import CaptureRules, capture_symbol_snapshot
from .phase1_validate import _decision_input_from_snapshot, _load_snapshot
from .position_store import load_open_positions
from .position_monitor import run_position_monitor
from .runtime_control import load_runtime_state
from .runtime_paths import data_root, phase1_snapshots_root
from .storage_retention import prune_snapshot_storage
from .trade_outcomes import recent_loss_guard, recent_winner_bias, sync_trade_outcomes_from_broker


def decision_journal_path() -> Path:
    return data_root() / "execution" / "decision_cards.jsonl"


@dataclass(frozen=True)
class TradingCycleResult:
    started_at: datetime
    finished_at: datetime
    symbols: list[str]
    snapshot_paths: list[str]
    decisions: list[dict[str, Any]]
    orders_submitted: list[dict[str, Any]]
    skipped: list[dict[str, Any]]
    runtime_state: dict[str, Any]
    execution_outcomes: list[dict[str, Any]] = field(default_factory=list)
    scanner_candidates_count: int = 0
    execution_rejected_count_by_reason: dict[str, int] = field(default_factory=dict)
    trade_attempted_count: int = 0
    zero_trade_cycle: bool = False

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "started_at": self.started_at.astimezone(UTC).isoformat(),
            "finished_at": self.finished_at.astimezone(UTC).isoformat(),
            "symbols": self.symbols,
            "snapshot_paths": self.snapshot_paths,
            "decisions": self.decisions,
            "orders_submitted": self.orders_submitted,
            "skipped": self.skipped,
            "runtime_state": self.runtime_state,
            "execution_outcomes": self.execution_outcomes,
            "scanner_candidates_count": self.scanner_candidates_count,
            "execution_rejected_count_by_reason": self.execution_rejected_count_by_reason,
            "trade_attempted_count": self.trade_attempted_count,
            "zero_trade_cycle": self.zero_trade_cycle,
        }


def run_trading_cycle(
    *,
    symbols: list[str],
    broker: AlpacaExecutionBroker | None = None,
    data_client: Any | None = None,
    scheduled_market_time: datetime | None = None,
    captured_at_utc: datetime | None = None,
    corpus_root: str | Path | None = None,
    decision_log_path: str | Path | None = None,
    execution_log_path: str | None = None,
    position_count: int | None = None,
    current_daily_realized_pnl: float = 0.0,
    quantity: int = 1,
    rules: CaptureRules | None = None,
) -> TradingCycleResult:
    started_at = datetime.now(tz=UTC)
    runtime_state = load_runtime_state()
    resolved_broker = broker or AlpacaExecutionBroker()
    resolved_data_client = data_client or AlpacaPaperClient()
    resolved_corpus_root = Path(corpus_root) if corpus_root is not None else phase1_snapshots_root()
    try:
        storage_retention = prune_snapshot_storage(resolved_corpus_root)
    except Exception as exc:
        storage_retention = {
            "ok": False,
            "enabled": True,
            "root": str(resolved_corpus_root),
            "error": f"{type(exc).__name__}: {exc}",
        }
    snapshot_time = scheduled_market_time or datetime.now(tz=UTC)
    captured_at = captured_at_utc or datetime.now(tz=UTC)
    resolved_rules = rules or _hosted_capture_rules()
    if hasattr(resolved_broker, "get_order"):
        try:
            reconcile_open_positions(
                resolved_broker,
                journal_path=execution_log_path,
            )
        except Exception:
            pass
    if hasattr(resolved_broker, "list_open_positions"):
        try:
            monitor_summary = run_position_monitor(
                broker=resolved_broker,
                journal_path=execution_log_path,
            )
        except Exception as exc:
            monitor_summary = {"ok": False, "error": str(exc)}
    else:
        monitor_summary = {"ok": True, "enabled": True, "checked": 0, "actions": []}
    outcome_journal_path = _trade_outcome_journal_for_execution_log(execution_log_path)
    try:
        outcome_learning_summary = sync_trade_outcomes_from_broker(
            resolved_broker,
            journal_path=outcome_journal_path,
            execution_journal_path=execution_log_path,
            limit=500,
        )
    except Exception as exc:
        outcome_learning_summary = {
            "ok": False,
            "recorded": 0,
            "outcomes": [],
            "error": f"{type(exc).__name__}: {exc}",
        }
    hosted_policy_version = HOSTED_POLICY_VERSION if is_hosted_paper_runtime() else None
    loss_guard = recent_loss_guard(
        journal_path=outcome_journal_path,
        policy_version=hosted_policy_version,
    )
    winner_bias = recent_winner_bias(
        journal_path=outcome_journal_path,
        policy_version=hosted_policy_version,
    )
    hosted_paper = is_hosted_paper_runtime()
    broker_daily_pnl_available = (
        bool(outcome_learning_summary.get("ok"))
        and outcome_learning_summary.get("daily_realized_pnl") is not None
    )
    daily_pnl_available = not hosted_paper or broker_daily_pnl_available
    effective_daily_realized_pnl = (
        float(outcome_learning_summary["daily_realized_pnl"])
        if broker_daily_pnl_available
        else (current_daily_realized_pnl if not hosted_paper else None)
    )
    cycle_symbols = _prioritize_symbols_by_winners(symbols, winner_bias)

    snapshot_paths: list[str] = []
    decisions: list[dict[str, Any]] = []
    orders_submitted: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    execution_outcomes: list[dict[str, Any]] = []
    execution_rejected_count_by_reason: dict[str, int] = {}
    scanner_candidates_count = 0
    trade_attempted_count = 0
    open_positions = max(position_count or 0, _active_open_position_count(resolved_broker))
    active_underlyings = _active_underlying_symbols(resolved_broker)
    open_drawdown_guard = _open_drawdown_guard(resolved_broker)
    max_new_entry_attempts_per_loop = resolved_broker.config.effective_max_new_entry_attempts_per_loop()
    core_runner_enabled = True if is_hosted_paper_runtime() else _env_bool("AUTOBOTT_CORE_RUNNER_ENABLED", default=True)
    core_runner_rules = load_core_runner_rules() if core_runner_enabled else None
    recent_setup_events = _recent_entry_setup_event_ids(execution_log_path)

    for symbol in cycle_symbols:
        try:
            snapshot_path = capture_symbol_snapshot(
                symbol=symbol,
                corpus_root=resolved_corpus_root,
                scheduled_market_time=snapshot_time,
                captured_at_utc=captured_at,
                corpus_type="production_capture" if resolved_broker.config.environment.value == "live" else "paper_capture",
                market_timezone="America/New_York",
                volatility_proxy_symbol="VIXY",
                data_client=resolved_data_client,
                rules=resolved_rules,
            )
            snapshot = _load_snapshot(Path(snapshot_path))
            decision_input = _decision_input_from_snapshot(snapshot)
            decision = build_decision_card(decision_input, _hosted_execution_rules())
        except Exception as exc:
            _append_skip(
                skipped,
                symbol=symbol.upper(),
                reason=(
                    "vix_index_lane_unavailable_using_proxy_fallback"
                    if symbol.upper() in {"VIX", "VIXW"}
                    else "snapshot_or_decision_failed"
                ),
                detail=str(exc),
            )
            continue
        try:
            ghost_journal_path = _ghost_trade_journal_for_execution_log(execution_log_path)
            ghost_observations = observe_ghost_trades(decision_input, journal_path=ghost_journal_path)
            if ghost_observations:
                _record_execution_outcome(
                    execution_outcomes,
                    ticker=symbol.upper(),
                    decision_id=f"ghost-observe-{symbol.upper()}",
                    thesis_id=f"{symbol.upper()}:ghost_observation",
                    disposition="ghost_trade_observed",
                    detail=f"observations={len(ghost_observations)}",
                    journal_path=execution_log_path,
                    payload={"observations": ghost_observations},
                )
        except Exception as exc:
            execution_outcomes.append(
                {
                    "symbol": symbol.upper(),
                    "disposition": "ghost_trade_telemetry_failed",
                    "detail": f"{type(exc).__name__}: {exc}",
                }
            )
        try:
            spread_candidate = select_defined_risk_spread(decision_input, decision.direction.bias)
            if spread_candidate is not None:
                spread_journal_path = _spread_journal_for_execution_log(execution_log_path)
                append_defined_risk_spread_candidate(
                    spread_candidate,
                    decision_id=decision.decision_id,
                    journal_path=spread_journal_path,
                )
                _record_execution_outcome(
                    execution_outcomes,
                    ticker=symbol.upper(),
                    decision_id=decision.decision_id,
                    thesis_id=f"{symbol.upper()}:defined_risk_spread:{spread_candidate.strategy}",
                    disposition="defined_risk_spread_backtest_candidate",
                    detail=f"{spread_candidate.strategy}:max_risk={spread_candidate.max_risk}:credit={spread_candidate.net_credit}",
                    journal_path=execution_log_path,
                    payload=spread_candidate.to_json_dict(),
                )
        except Exception as exc:
            execution_outcomes.append(
                {
                    "symbol": symbol.upper(),
                    "disposition": "defined_risk_spread_telemetry_failed",
                    "detail": f"{type(exc).__name__}: {exc}",
                }
            )
        snapshot_paths.append(snapshot_path)
        decision_payload = decision.to_json_dict()
        decisions.append(decision_payload)
        try:
            append_decision_card(decision_payload, snapshot_path=snapshot_path, log_path=decision_log_path)
        except Exception as exc:
            execution_outcomes.append(
                {
                    "symbol": symbol.upper(),
                    "decision_id": decision.decision_id,
                    "disposition": "decision_journal_failed",
                    "detail": f"{type(exc).__name__}: {exc}",
                }
            )
        is_candidate = decision.decision is DecisionStatus.TRADE_CANDIDATE
        thesis_id = _decision_thesis_id(decision)

        if is_candidate:
            scanner_candidates_count += 1
            _record_execution_outcome(
                execution_outcomes,
                ticker=symbol.upper(),
                decision_id=decision.decision_id,
                thesis_id=thesis_id,
                disposition="scanner_candidate",
                detail=f"{decision.trade_setup.value}:{decision.execution_layer.value}",
                journal_path=execution_log_path,
                payload={
                    "reason_codes": list(decision.reason_codes),
                    "selected_contract": decision.selected_contract.option_symbol if decision.selected_contract else None,
                    "recent_winner_bias": (winner_bias.get("reasons") or {}).get(symbol.upper()),
                },
            )

        if runtime_state.kill_switch_enabled:
            _append_skip(skipped, symbol=symbol.upper(), reason="kill_switch_enabled")
            if is_candidate:
                _record_execution_rejection(
                    execution_outcomes,
                    execution_rejected_count_by_reason,
                    ticker=symbol.upper(),
                    decision_id=decision.decision_id,
                    thesis_id=thesis_id,
                    reason="kill_switch_enabled",
                    detail="kill_switch_enabled",
                    journal_path=execution_log_path,
                )
            continue
        if not runtime_state.execution_enabled:
            _append_skip(skipped, symbol=symbol.upper(), reason="execution_disabled")
            if is_candidate:
                _record_execution_rejection(
                    execution_outcomes,
                    execution_rejected_count_by_reason,
                    ticker=symbol.upper(),
                    decision_id=decision.decision_id,
                    thesis_id=thesis_id,
                    reason="execution_disabled",
                    detail="execution_disabled",
                    journal_path=execution_log_path,
                )
            continue
        if not is_candidate:
            _append_skip(
                skipped,
                symbol=symbol.upper(),
                reason=decision_payload["decision"],
                detail=decision.blocked_reason,
                reasons=list(decision.reason_codes),
            )
            continue
        if not daily_pnl_available:
            _append_skip(
                skipped,
                symbol=symbol.upper(),
                reason="daily_pnl_unavailable",
                detail=str(outcome_learning_summary.get("error") or "broker fill history unavailable"),
            )
            _record_execution_rejection(
                execution_outcomes,
                execution_rejected_count_by_reason,
                ticker=symbol.upper(),
                decision_id=decision.decision_id,
                thesis_id=thesis_id,
                reason="daily_pnl_unavailable",
                detail="new entries fail closed until account-wide realized P/L is available",
                journal_path=execution_log_path,
                payload={"outcome_sync": outcome_learning_summary},
            )
            continue
        if _env_bool("AUTOBOTT_SINGLE_LEG_REAL_ENTRIES_DISABLED", default=False) and not core_runner_enabled:
            if decision.selected_contract is not None:
                ghost = append_ghost_trade(
                    decision,
                    reason="single_leg_real_entries_disabled",
                    max_real_cost=resolved_broker.config.max_position_cost,
                    journal_path=_ghost_trade_journal_for_execution_log(execution_log_path),
                )
            else:
                ghost = {}
            _append_skip(
                skipped,
                symbol=symbol.upper(),
                reason="single_leg_real_entries_disabled",
                detail="single-leg real entries are ghost-only while defined-risk spread lane is evaluated",
            )
            _record_execution_rejection(
                execution_outcomes,
                execution_rejected_count_by_reason,
                ticker=symbol.upper(),
                decision_id=decision.decision_id,
                thesis_id=thesis_id,
                reason="single_leg_real_entries_disabled",
                detail="single-leg real entries are ghost-only while defined-risk spread lane is evaluated",
                journal_path=execution_log_path,
                payload={"ghost": ghost},
            )
            continue
        blocked_underlyings = set(loss_guard.get("blocked_underlyings") or [])
        learning_underlying = "VOLATILITY" if is_volatility_symbol(symbol) else symbol.upper()
        if symbol.upper() in blocked_underlyings or learning_underlying in blocked_underlyings:
            _append_skip(
                skipped,
                symbol=symbol.upper(),
                reason="recent_loss_guard",
                detail=json.dumps((loss_guard.get("reasons") or {}).get(learning_underlying, {}), sort_keys=True),
            )
            _record_execution_rejection(
                execution_outcomes,
                execution_rejected_count_by_reason,
                ticker=symbol.upper(),
                decision_id=decision.decision_id,
                thesis_id=thesis_id,
                reason="recent_loss_guard",
                detail="recent outcomes for this underlying are underperforming",
                journal_path=execution_log_path,
                payload=(loss_guard.get("reasons") or {}).get(learning_underlying, {}),
            )
            continue
        setup_event_id = _decision_setup_event_id(decision)
        if _SETUP_REGISTRY_UNAVAILABLE in recent_setup_events or setup_event_id in recent_setup_events:
            cooldown_unavailable = _SETUP_REGISTRY_UNAVAILABLE in recent_setup_events
            cooldown_reason = "setup_event_registry_unavailable" if cooldown_unavailable else "setup_event_already_traded"
            _append_skip(
                skipped,
                symbol=symbol.upper(),
                reason=cooldown_reason,
                detail=setup_event_id,
            )
            _record_execution_rejection(
                execution_outcomes,
                execution_rejected_count_by_reason,
                ticker=symbol.upper(),
                decision_id=decision.decision_id,
                thesis_id=thesis_id,
                reason=cooldown_reason,
                detail=(
                    "setup registry unavailable; new entries fail closed"
                    if cooldown_unavailable
                    else "the same setup and hourly evidence already produced an entry"
                ),
                journal_path=execution_log_path,
                payload={"setup_event_id": setup_event_id},
            )
            continue
        volatility_exposure_open = is_volatility_symbol(symbol) and any(
            is_volatility_symbol(active_symbol) for active_symbol in active_underlyings
        )
        if symbol.upper() in active_underlyings or volatility_exposure_open:
            rejection_reason = (
                "volatility_exposure_already_open"
                if volatility_exposure_open
                else "underlying_exposure_already_open"
            )
            _append_skip(skipped, symbol=symbol.upper(), reason=rejection_reason)
            _record_execution_rejection(
                execution_outcomes,
                execution_rejected_count_by_reason,
                ticker=symbol.upper(),
                decision_id=decision.decision_id,
                thesis_id=thesis_id,
                reason=rejection_reason,
                detail=(
                    "one VIX/VXX/UVXY exposure group is already active"
                    if volatility_exposure_open
                    else f"active_underlying={symbol.upper()}"
                ),
                journal_path=execution_log_path,
                payload={"active_underlyings": sorted(active_underlyings)},
            )
            continue
        if open_drawdown_guard.get("blocked"):
            if decision.selected_contract is not None:
                ghost = append_ghost_trade(
                    decision,
                    reason="open_drawdown_guard_real_entry_blocked",
                    max_real_cost=resolved_broker.config.max_position_cost,
                    journal_path=_ghost_trade_journal_for_execution_log(execution_log_path),
                )
            else:
                ghost = {}
            _append_skip(
                skipped,
                symbol=symbol.upper(),
                reason="open_drawdown_guard",
                detail=json.dumps(open_drawdown_guard, sort_keys=True),
            )
            _record_execution_rejection(
                execution_outcomes,
                execution_rejected_count_by_reason,
                ticker=symbol.upper(),
                decision_id=decision.decision_id,
                thesis_id=thesis_id,
                reason="open_drawdown_guard",
                detail="current open basket drawdown blocks fresh real entries; signal routed to ghost lane",
                journal_path=execution_log_path,
                payload={"guard": open_drawdown_guard, "ghost": ghost},
            )
            continue
        core_runner_pair = None
        if core_runner_enabled:
            if decision.selected_contract is not None:
                core_runner_pair = select_core_runner_pair(
                    decision.selected_contract,
                    decision_input.option_chain,
                    rules=core_runner_rules,
                )
            if core_runner_pair is None:
                ghost = (
                    append_ghost_trade(
                        decision,
                        reason="core_runner_pair_not_found",
                        max_real_cost=_selected_contract_real_cost(decision),
                        journal_path=_ghost_trade_journal_for_execution_log(execution_log_path),
                    )
                    if decision.selected_contract is not None
                    else {}
                )
                _append_skip(
                    skipped,
                    symbol=symbol.upper(),
                    reason="core_runner_pair_not_found",
                    detail="one primary plus one distinct cheaper liquid runner could not be selected",
                )
                _record_execution_rejection(
                    execution_outcomes,
                    execution_rejected_count_by_reason,
                    ticker=symbol.upper(),
                    decision_id=decision.decision_id,
                    thesis_id=thesis_id,
                    reason="core_runner_pair_not_found",
                    detail="neither leg submitted because the required primary/runner structure was unavailable",
                    journal_path=execution_log_path,
                    payload={"ghost": ghost},
                )
                continue
            decision = replace(
                decision,
                selected_contract=core_runner_pair.primary,
                reason_codes=[*decision.reason_codes, "core_runner_pair_selected"],
                explanation=f"{decision.explanation}; primary plus convex runner selected for paper execution",
            )
            thesis_id = _decision_thesis_id(decision)
            _record_execution_outcome(
                execution_outcomes,
                ticker=symbol.upper(),
                decision_id=decision.decision_id,
                thesis_id=thesis_id,
                disposition="core_runner_pair_selected",
                detail=f"combined_debit={core_runner_pair.estimated_group_cost}",
                journal_path=execution_log_path,
                payload={
                    "primary_option_symbol": core_runner_pair.primary.option_symbol,
                    "runner_option_symbol": core_runner_pair.runner.option_symbol,
                    "estimated_group_cost": core_runner_pair.estimated_group_cost,
                },
            )
        if max_new_entry_attempts_per_loop is not None and trade_attempted_count >= max_new_entry_attempts_per_loop:
            _append_skip(skipped, symbol=symbol.upper(), reason="max_new_entry_attempts_per_loop_reached")
            _record_execution_rejection(
                execution_outcomes,
                execution_rejected_count_by_reason,
                ticker=symbol.upper(),
                decision_id=decision.decision_id,
                thesis_id=thesis_id,
                reason="max_new_entry_attempts_per_loop_reached",
                detail=f"max_new_entry_attempts_per_loop={max_new_entry_attempts_per_loop}",
                journal_path=execution_log_path,
            )
            continue

        submission_attempted = False

        def _mark_submission_attempt(intent: Any) -> None:
            nonlocal trade_attempted_count, submission_attempted
            _remember_setup_event(setup_event_id, execution_log_path, recent_setup_events)
            submission_attempted = True
            trade_attempted_count += 1
            _record_execution_outcome(
                execution_outcomes,
                ticker=symbol.upper(),
                decision_id=decision.decision_id,
                thesis_id=thesis_id,
                disposition="pass_trade_attempted",
                detail="submission_requested",
                journal_path=execution_log_path,
                payload={
                    "option_symbol": intent.option_symbol,
                    "quantity": intent.quantity,
                    "limit_price": intent.limit_price,
                },
            )

        try:
            if core_runner_pair is not None:
                submitted_orders = submit_core_runner_to_broker(
                    decision,
                    core_runner_pair,
                    broker=resolved_broker,
                    current_daily_realized_pnl=float(effective_daily_realized_pnl),
                    open_positions=open_positions,
                    journal_path=execution_log_path,
                    on_submission_attempt=_mark_submission_attempt,
                )
            else:
                submitted_orders = (
                    submit_decision_to_broker(
                        decision,
                        broker=resolved_broker,
                        quantity=quantity,
                        current_daily_realized_pnl=float(effective_daily_realized_pnl),
                        open_positions=open_positions,
                        journal_path=execution_log_path,
                        on_submission_attempt=_mark_submission_attempt,
                    ),
                )
            open_positions += len(submitted_orders)
            active_underlyings.add(symbol.upper())
            for order in submitted_orders:
                orders_submitted.append(
                    {
                        "symbol": symbol.upper(),
                        "option_symbol": order.intent.option_symbol,
                        "leg_role": order.intent.metadata.get("leg_role", "primary"),
                        "trade_group_id": order.intent.metadata.get("trade_group_id"),
                        "broker_order_id": order.broker_order_id,
                        "state": order.state.value,
                        "client_order_id": order.client_order_id,
                    }
                )
        except ExecutionRejectedError as exc:
            if exc.reason == "core_runner_paired_submission_partial_failure":
                # The first ordinary paper leg may already be accepted or
                # filled when the second leg fails. Reserve this underlying
                # (and therefore the shared volatility group) immediately so
                # the same scan cannot stack a VXX/UVXY fallback on the orphan.
                active_underlyings.add(symbol.upper())
                open_positions += 1
            _append_skip(
                skipped,
                symbol=symbol.upper(),
                reason=exc.reason,
                detail=exc.detail,
                reasons=list(exc.reasons),
            )
            _record_execution_rejection(
                execution_outcomes,
                execution_rejected_count_by_reason,
                ticker=symbol.upper(),
                decision_id=decision.decision_id,
                thesis_id=thesis_id,
                reason=exc.reason,
                detail=exc.detail,
                journal_path=execution_log_path,
                payload={"reasons": list(exc.reasons)},
            )
        except Exception as exc:
            if submission_attempted:
                _append_skip(skipped, symbol=symbol.upper(), reason="trade_attempt_failed", detail=str(exc))
                _record_execution_outcome(
                    execution_outcomes,
                    ticker=symbol.upper(),
                    decision_id=decision.decision_id,
                    thesis_id=thesis_id,
                    disposition="trade_attempt_failed",
                    detail=str(exc),
                    journal_path=execution_log_path,
                    payload={"exception_type": type(exc).__name__},
                )
            else:
                _append_skip(
                    skipped,
                    symbol=symbol.upper(),
                    reason="unexpected_exception_before_submission",
                    detail=str(exc),
                )
                _record_execution_rejection(
                    execution_outcomes,
                    execution_rejected_count_by_reason,
                    ticker=symbol.upper(),
                    decision_id=decision.decision_id,
                    thesis_id=thesis_id,
                    reason="unexpected_exception_before_submission",
                    detail=str(exc),
                    journal_path=execution_log_path,
                    payload={"exception_type": type(exc).__name__},
                )

    finished_at = datetime.now(tz=UTC)
    return TradingCycleResult(
        started_at=started_at,
        finished_at=finished_at,
        symbols=cycle_symbols,
        snapshot_paths=snapshot_paths,
        decisions=decisions,
        orders_submitted=orders_submitted,
        skipped=skipped,
        runtime_state=runtime_state.to_json_dict(),
        execution_outcomes=[
            {"disposition": "snapshot_storage_retention_summary", **storage_retention},
            {"disposition": "position_monitor_summary", **monitor_summary},
            {"disposition": "trade_outcome_learning_summary", **outcome_learning_summary, "winner_bias": winner_bias},
            {
                "disposition": "daily_realized_pnl_summary",
                "daily_realized_pnl": effective_daily_realized_pnl,
                "source": (
                    "broker_fill_outcomes"
                    if broker_daily_pnl_available
                    else ("unavailable" if hosted_paper else "cycle_argument")
                ),
            },
            {"disposition": "open_drawdown_guard_summary", **open_drawdown_guard},
            *execution_outcomes,
        ],
        scanner_candidates_count=scanner_candidates_count,
        execution_rejected_count_by_reason=execution_rejected_count_by_reason,
        trade_attempted_count=trade_attempted_count,
        zero_trade_cycle=(
            scanner_candidates_count > 0
            and trade_attempted_count == 0
            and runtime_state.execution_enabled
            and not runtime_state.kill_switch_enabled
        ),
    )


def append_decision_card(payload: dict[str, Any], *, snapshot_path: str, log_path: str | Path | None = None) -> Path:
    path = Path(log_path) if log_path is not None else decision_journal_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    record = {
        "recorded_at": datetime.now(tz=UTC).isoformat(),
        "snapshot_path": snapshot_path,
        "decision_card": payload,
    }
    needs_separator = False
    if path.exists() and path.stat().st_size:
        with path.open("rb") as existing:
            existing.seek(-1, 2)
            needs_separator = existing.read(1) != b"\n"
    with path.open("a", encoding="utf-8") as handle:
        if needs_separator:
            handle.write("\n")
        handle.write(json.dumps(record, sort_keys=True))
        handle.write("\n")
    compact_jsonl_tail(path)
    return path


def load_decision_cards(*, log_path: str | Path | None = None, limit: int | None = None) -> list[dict[str, Any]]:
    path = Path(log_path) if log_path is not None else decision_journal_path()
    if not path.exists():
        return []
    max_tail_bytes = 16 * 1024 * 1024 if limit is not None else None
    rows: list[dict[str, Any]] = []
    for raw_line in read_jsonl_tail(path, max_tail_bytes=max_tail_bytes):
        if not raw_line.strip():
            continue
        try:
            decoded = json.loads(raw_line)
        except (json.JSONDecodeError, UnicodeDecodeError, TypeError):
            continue
        if isinstance(decoded, dict):
            rows.append(decoded)
    return rows[-limit:] if limit is not None else rows


def _active_open_position_count(broker: Any | None = None) -> int:
    # The local open_positions.json store only ever grows: entries are added
    # on entry but nothing removes them when position_monitor closes a
    # position, so it drifts further from reality the longer the bot runs.
    # Prefer the broker's live position list, same as _active_underlying_symbols.
    if broker is not None and hasattr(broker, "list_open_positions"):
        try:
            return sum(1 for position in broker.list_open_positions() if _broker_position_is_active(position))
        except Exception:
            pass
    positions = load_open_positions()
    return len(
        [
            position
            for position in positions
            if not position.status.startswith("canceled")
            and not position.status.startswith("rejected")
            and not position.status.startswith("failed")
        ]
    )


def _trade_outcome_journal_for_execution_log(execution_log_path: str | Path | None) -> Path | None:
    if execution_log_path is None:
        return None
    return Path(execution_log_path).parent / "trade_outcomes.jsonl"


def _ghost_trade_journal_for_execution_log(execution_log_path: str | Path | None) -> Path | None:
    if execution_log_path is None:
        return None
    return Path(execution_log_path).parent / "ghost_trades.jsonl"


def _spread_journal_for_execution_log(execution_log_path: str | Path | None) -> Path | None:
    if execution_log_path is None:
        return None
    return Path(execution_log_path).parent / "defined_risk_spreads.jsonl"


def _selected_contract_real_cost(decision: DecisionCard) -> float:
    if decision.selected_contract is None:
        return 0.0
    return round(float(decision.selected_contract.mid) * 100, 2)


def _open_drawdown_guard(broker: Any) -> dict[str, Any]:
    hosted_paper = is_hosted_paper_runtime()
    if not hosted_paper and not _env_bool("AUTOBOTT_OPEN_DRAWDOWN_GUARD_ENABLED", default=True):
        return {"enabled": False, "blocked": False}
    if not hasattr(broker, "list_open_positions"):
        return {"enabled": True, "blocked": False, "reason": "broker_positions_unavailable"}
    try:
        positions = [position for position in broker.list_open_positions() if _broker_position_is_active(position)]
    except Exception as exc:
        return {"enabled": True, "blocked": False, "reason": "position_read_failed", "error": str(exc)}
    total = len(positions)
    if total == 0:
        return {"enabled": True, "blocked": False, "open_positions": 0, "unrealized_pl": 0.0, "losers": 0, "loss_rate": 0.0}
    unrealized = round(sum(_float_value(position.get("unrealized_pl")) for position in positions), 2)
    losers = sum(1 for position in positions if _float_value(position.get("unrealized_pl")) < 0)
    loss_rate = round(losers / total, 4)
    max_unrealized_loss = (
        HOSTED_OPEN_DRAWDOWN_MAX_LOSS
        if hosted_paper
        else abs(float(os.getenv("AUTOBOTT_OPEN_DRAWDOWN_GUARD_MAX_UNREALIZED_LOSS", "20")))
    )
    min_losers = (
        HOSTED_OPEN_DRAWDOWN_MIN_LOSERS
        if hosted_paper
        else int(os.getenv("AUTOBOTT_OPEN_DRAWDOWN_GUARD_MIN_LOSERS", "3"))
    )
    min_loss_rate = (
        HOSTED_OPEN_DRAWDOWN_LOSS_RATE
        if hosted_paper
        else float(os.getenv("AUTOBOTT_OPEN_DRAWDOWN_GUARD_LOSS_RATE", "0.60"))
    )
    blocked = unrealized <= -max_unrealized_loss and losers >= min_losers and loss_rate >= min_loss_rate
    return {
        "enabled": True,
        "blocked": blocked,
        "open_positions": total,
        "unrealized_pl": unrealized,
        "losers": losers,
        "loss_rate": loss_rate,
        "max_unrealized_loss": max_unrealized_loss,
        "min_losers": min_losers,
        "min_loss_rate": min_loss_rate,
    }


def _env_bool(name: str, *, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _float_value(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _prioritize_symbols_by_winners(symbols: list[str], winner_bias: dict[str, Any]) -> list[str]:
    preferred = [str(symbol).upper() for symbol in winner_bias.get("preferred_underlyings") or []]
    normalized = list(dict.fromkeys(symbol.upper() for symbol in symbols))
    # Keep the direct VIX lane and its two explicit fallbacks ahead of equity
    # winner reordering. With a three-pair cycle cap, historical equity winners
    # must not silently starve volatility discovery.
    volatility_priority = [symbol for symbol in ("VIX", "VXX", "UVXY") if symbol in normalized]
    non_volatility = [symbol for symbol in normalized if symbol not in set(volatility_priority)]
    preferred_in_cycle = [symbol for symbol in preferred if symbol in non_volatility]
    remaining = [symbol for symbol in non_volatility if symbol not in set(preferred_in_cycle)]
    return volatility_priority + preferred_in_cycle + remaining


def _active_underlying_symbols(broker: Any | None = None) -> set[str]:
    if broker is not None and hasattr(broker, "list_open_positions"):
        positions_read = False
        symbols: set[str] = set()
        try:
            symbols.update(
                {
                _underlying_from_option_symbol(str(position.get("symbol") or "")) or str(position.get("symbol") or "").upper()
                for position in broker.list_open_positions()
                if _broker_position_is_active(position)
                }
            )
            positions_read = True
        except Exception:
            pass
        try:
            symbols.update(_pending_entry_underlying_symbols(broker))
        except Exception:
            # A transient order-list failure must not discard live positions
            # that were already read successfully.
            pass
        if positions_read:
            return {symbol for symbol in symbols if symbol}
    return {
        position.symbol.upper()
        for position in load_open_positions()
        if _position_is_active(position.status)
    }


def _broker_position_is_active(position: dict[str, Any]) -> bool:
    side = str(position.get("side") or "long").lower()
    qty = float(position.get("qty") or 0)
    return side == "long" and qty > 0


def _pending_entry_underlying_symbols(broker: Any) -> set[str]:
    if not hasattr(broker, "list_orders"):
        return set()
    orders = broker.list_orders(status="open", limit=100, direction="desc")
    return {
        _underlying_from_option_symbol(str(order.get("symbol") or "")) or str(order.get("symbol") or "").upper()
        for order in orders
        if _broker_order_is_pending_entry(order)
    }


def _broker_order_is_pending_entry(order: dict[str, Any]) -> bool:
    side = str(order.get("side") or "").lower()
    status = str(order.get("status") or "").lower()
    filled_qty = float(order.get("filled_qty") or 0)
    qty = float(order.get("qty") or 0)
    return side == "buy" and status in {"new", "accepted", "partially_filled", "pending_new"} and filled_qty < qty


def _position_is_active(status: str) -> bool:
    normalized = status.strip().lower()
    return not (
        normalized.startswith("canceled")
        or normalized.startswith("cancelled")
        or normalized.startswith("rejected")
        or normalized.startswith("failed")
        or normalized.startswith("expired")
        or normalized.startswith("closed")
    )


def _underlying_from_option_symbol(symbol: str) -> str | None:
    stripped = symbol.strip().upper()
    for index, char in enumerate(stripped):
        if char in {"C", "P"} and index >= 6:
            expiry = stripped[index - 6 : index]
            suffix = stripped[index + 1 :]
            if expiry.isdigit() and suffix.isdigit():
                return stripped[: index - 6]
    return None


def _decision_thesis_id(decision: DecisionCard) -> str:
    return f"{decision.ticker}:{decision.trade_setup.value}:{decision.execution_layer.value}"


def _decision_setup_event_id(decision: DecisionCard) -> str:
    completed_bar_bucket = decision.timestamp.astimezone(UTC).replace(minute=0, second=0, microsecond=0)
    learning_symbol = "VOLATILITY" if is_volatility_symbol(decision.ticker) else decision.ticker.upper()
    return ":".join(
        (
            learning_symbol,
            completed_bar_bucket.isoformat(),
            decision.trade_setup.value,
            decision.direction.bias.value,
        )
    )


_SETUP_REGISTRY_UNAVAILABLE = "__SETUP_REGISTRY_UNAVAILABLE__"


def _recent_entry_setup_event_ids(
    execution_log_path: str | Path | None,
) -> set[str]:
    """Load the bounded setup registry, bootstrapping it from the journal."""

    path = Path(execution_log_path) if execution_log_path is not None else execution_journal_path()
    registry_path = path.with_name("setup_events.json")
    registry = _load_setup_event_registry(registry_path)
    if registry is not None:
        return set(registry)
    if not path.exists():
        return set()
    try:
        raw_lines = read_jsonl_tail(path, max_tail_bytes=64 * 1024 * 1024)
    except OSError:
        return {_SETUP_REGISTRY_UNAVAILABLE}
    setup_events: list[str] = []
    for raw_line in raw_lines:
        try:
            row = json.loads(raw_line)
        except (json.JSONDecodeError, UnicodeDecodeError, TypeError):
            continue
        if row.get("event_type") != "order_submission":
            continue
        intent = (row.get("payload") or {}).get("intent") or {}
        if str(intent.get("side") or "").lower() != "buy_to_open":
            continue
        setup_event_id = str((intent.get("metadata") or {}).get("setup_event_id") or "").strip()
        if setup_event_id and setup_event_id not in setup_events:
            setup_events.append(setup_event_id)
    retained = setup_events[-500:]
    try:
        _write_setup_event_registry(registry_path, retained)
    except OSError:
        pass
    return set(retained)


def _remember_setup_event(
    setup_event_id: str,
    execution_log_path: str | Path | None,
    in_memory: set[str],
) -> None:
    path = Path(execution_log_path) if execution_log_path is not None else execution_journal_path()
    registry_path = path.with_name("setup_events.json")
    existing = _load_setup_event_registry(registry_path) or list(in_memory)
    retained = [value for value in existing if value != setup_event_id]
    retained.append(setup_event_id)
    try:
        _write_setup_event_registry(registry_path, retained[-500:])
    except OSError as exc:
        # This write happens immediately before the broker POST.  If the
        # durable reservation cannot be made, do not submit: an ambiguous POST
        # followed by a process restart could otherwise repeat the same setup.
        in_memory.add(_SETUP_REGISTRY_UNAVAILABLE)
        raise ExecutionRejectedError(
            "setup_event_registry_unavailable",
            detail=f"setup event could not be reserved before submission: {exc}",
            reasons=("setup_event_registry_unavailable",),
        ) from exc
    in_memory.add(setup_event_id)


def _load_setup_event_registry(path: Path) -> list[str] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except OSError:
        return [_SETUP_REGISTRY_UNAVAILABLE]
    except (UnicodeError, json.JSONDecodeError):
        return None
    if not isinstance(payload, list):
        return None
    return [str(value) for value in payload if str(value).strip()][-500:]


def _write_setup_event_registry(path: Path, setup_events: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(json.dumps(setup_events[-500:], indent=2), encoding="utf-8")
    temporary.replace(path)


def _hosted_capture_rules() -> CaptureRules:
    if not is_hosted_paper_runtime():
        return CaptureRules()
    return CaptureRules(
        lookback_bars=HOSTED_LOOKBACK_BARS,
        bar_timeframe=HOSTED_BAR_TIMEFRAME,
        lookback_calendar_days=HOSTED_LOOKBACK_CALENDAR_DAYS,
        option_chain_min_dte=HOSTED_TACTICAL_MIN_DTE,
        option_chain_max_dte=HOSTED_RIDER_MAX_DTE,
        tactical_min_dte=HOSTED_TACTICAL_MIN_DTE,
        tactical_max_dte=HOSTED_TACTICAL_MAX_DTE,
        rider_min_dte=HOSTED_RIDER_MIN_DTE,
        rider_max_dte=HOSTED_RIDER_MAX_DTE,
    )


def _hosted_execution_rules() -> Phase1Rules:
    """Apply deployment contract horizons without changing replay defaults."""

    rules = Phase1Rules()
    if is_hosted_paper_runtime():
        return replace(
            rules,
            intraday_min_dte=HOSTED_TACTICAL_MIN_DTE,
            intraday_max_dte=HOSTED_TACTICAL_MAX_DTE,
            rider_min_dte=HOSTED_RIDER_MIN_DTE,
            rider_max_dte=HOSTED_RIDER_MAX_DTE,
            min_open_interest=HOSTED_MIN_OPEN_INTEREST,
            risk_off_bullish_exempt_symbols=("VIX", "VIXW", "VXX", "UVXY"),
        )
    min_dte = int(os.getenv("AUTOBOTT_ENTRY_MIN_DTE", str(rules.intraday_min_dte)))
    tactical_max_dte = int(os.getenv("AUTOBOTT_ENTRY_TACTICAL_MAX_DTE", str(rules.intraday_max_dte)))
    rider_min_dte = int(os.getenv("AUTOBOTT_ENTRY_RIDER_MIN_DTE", str(rules.rider_min_dte)))
    rider_max_dte = int(os.getenv("AUTOBOTT_ENTRY_RIDER_MAX_DTE", str(rules.rider_max_dte)))
    if not 1 <= min_dte <= tactical_max_dte < rider_min_dte <= rider_max_dte <= rules.max_dte:
        raise ValueError("invalid_hosted_entry_dte_windows")
    return replace(
        rules,
        intraday_min_dte=min_dte,
        intraday_max_dte=tactical_max_dte,
        rider_min_dte=rider_min_dte,
        rider_max_dte=rider_max_dte,
    )


def _append_skip(
    skipped: list[dict[str, Any]],
    *,
    symbol: str,
    reason: str,
    detail: str | None = None,
    reasons: list[str] | None = None,
) -> None:
    payload: dict[str, Any] = {"symbol": symbol, "reason": reason}
    if detail is not None:
        payload["detail"] = detail
    if reasons:
        payload["reasons"] = reasons
    skipped.append(payload)


def _record_execution_rejection(
    execution_outcomes: list[dict[str, Any]],
    execution_rejected_count_by_reason: dict[str, int],
    *,
    ticker: str,
    decision_id: str,
    thesis_id: str,
    reason: str,
    detail: str,
    journal_path: str | None,
    payload: dict[str, Any] | None = None,
) -> None:
    execution_rejected_count_by_reason[reason] = execution_rejected_count_by_reason.get(reason, 0) + 1
    rejection_payload = {"reason": reason}
    if payload:
        rejection_payload.update(payload)
    _record_execution_outcome(
        execution_outcomes,
        ticker=ticker,
        decision_id=decision_id,
        thesis_id=thesis_id,
        disposition="execution_rejected",
        detail=detail,
        journal_path=journal_path,
        payload=rejection_payload,
    )


def _record_execution_outcome(
    execution_outcomes: list[dict[str, Any]],
    *,
    ticker: str,
    decision_id: str,
    thesis_id: str,
    disposition: str,
    detail: str,
    journal_path: str | None,
    payload: dict[str, Any] | None = None,
) -> None:
    outcome = {
        "symbol": ticker,
        "decision_id": decision_id,
        "thesis_id": thesis_id,
        "disposition": disposition,
        "detail": detail,
    }
    if payload:
        outcome.update(payload)
    execution_outcomes.append(outcome)
    try:
        append_execution_outcome(
            decision_id=decision_id,
            thesis_id=thesis_id,
            symbol=ticker,
            disposition=disposition,
            detail=detail,
            payload=payload,
            journal_path=journal_path,
        )
    except Exception as exc:
        # Telemetry is valuable but not part of entry eligibility. A journal
        # write/compaction failure must remain visible without suppressing a
        # qualified broker submission.
        outcome["journal_error"] = f"{type(exc).__name__}: {exc}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run one automated AutoBott trading cycle.")
    parser.add_argument("--symbols", nargs="+", required=True, help="Ticker list, for example: AAPL MSFT NVDA")
    parser.add_argument("--quantity", type=int, default=1, help="Contracts per eligible decision.")
    parser.add_argument("--position-count", type=int, default=0, help="Current open-position count for risk checks.")
    parser.add_argument("--daily-pnl", type=float, default=0.0, help="Current realized PnL for daily loss checks.")
    parser.add_argument("--corpus-root", help="Optional snapshot output root.")
    args = parser.parse_args(argv)

    result = run_trading_cycle(
        symbols=args.symbols,
        quantity=args.quantity,
        position_count=args.position_count,
        current_daily_realized_pnl=args.daily_pnl,
        corpus_root=args.corpus_root,
    )
    print(json.dumps(result.to_json_dict(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
