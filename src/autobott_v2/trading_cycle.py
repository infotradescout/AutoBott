from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .execution_broker import AlpacaExecutionBroker
from .execution_models import BrokerEnvironment
from .execution_journal import append_execution_outcome
from .execution_reconciler import reconcile_open_positions
from .execution_orchestrator import ExecutionRejectedError, submit_decision_to_broker
from .phase1_alpaca_client import AlpacaPaperClient
from .phase1_engine import build_decision_card
from .phase1_models import (
    ContractScore,
    DecisionCard,
    DecisionStatus,
    DirectionBias,
    ExecutionLayer,
    OptionContractSnapshot,
    OptionType,
    Phase1Rules,
    SelectedContract,
)
from .phase1_snapshot_capture import CaptureRules, capture_symbol_snapshot
from .phase1_validate import _decision_input_from_snapshot, _load_snapshot
from .position_store import load_open_positions
from .position_monitor import run_position_monitor
from .runtime_control import load_runtime_state
from .runtime_paths import data_root, phase1_snapshots_root
from .trade_outcomes import recent_loss_guard, sync_trade_outcomes_from_broker


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
    snapshot_time = scheduled_market_time or datetime.now(tz=UTC)
    captured_at = captured_at_utc or datetime.now(tz=UTC)
    resolved_rules = rules or CaptureRules()
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
    outcome_learning_summary = sync_trade_outcomes_from_broker(resolved_broker, journal_path=outcome_journal_path)
    loss_guard = recent_loss_guard(journal_path=outcome_journal_path)

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
    max_new_entry_attempts_per_loop = resolved_broker.config.effective_max_new_entry_attempts_per_loop()

    for symbol in symbols:
        try:
            snapshot_path = capture_symbol_snapshot(
                symbol=symbol,
                corpus_root=resolved_corpus_root,
                scheduled_market_time=snapshot_time,
                captured_at_utc=captured_at,
                corpus_type="production_capture" if resolved_broker.config.environment.value == "live" else "paper_capture",
                market_timezone="America/New_York",
                volatility_proxy_symbol="UVXY",
                data_client=resolved_data_client,
                rules=resolved_rules,
            )
            snapshot = _load_snapshot(Path(snapshot_path))
            decision_input = _decision_input_from_snapshot(snapshot)
            decision = build_decision_card(decision_input)
        except Exception as exc:
            _append_skip(
                skipped,
                symbol=symbol.upper(),
                reason="snapshot_or_decision_failed",
                detail=str(exc),
            )
            continue
        snapshot_paths.append(snapshot_path)
        decision_payload = decision.to_json_dict()
        decisions.append(decision_payload)
        append_decision_card(decision_payload, snapshot_path=snapshot_path, log_path=decision_log_path)
        strict_decision = decision
        opportunistic_decision = _paper_opportunistic_decision(
            strict_decision,
            decision_input=decision_input,
            broker=resolved_broker,
        )
        if opportunistic_decision is not None:
            decision = opportunistic_decision
            decision_payload = decision.to_json_dict()
            decisions.append(decision_payload)
            append_decision_card(decision_payload, snapshot_path=snapshot_path, log_path=decision_log_path)
            _record_execution_outcome(
                execution_outcomes,
                ticker=symbol.upper(),
                decision_id=decision.decision_id,
                thesis_id=_decision_thesis_id(decision),
                disposition="paper_opportunistic_override",
                detail=str(strict_decision.blocked_reason or strict_decision.decision.value),
                journal_path=execution_log_path,
                payload={
                    "strict_decision": strict_decision.decision.value,
                    "strict_blocked_reason": strict_decision.blocked_reason,
                    "override_reason_codes": list(decision.reason_codes),
                    "selected_contract": decision.selected_contract.option_symbol if decision.selected_contract else None,
                },
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
            _append_skip(skipped, symbol=symbol.upper(), reason=decision_payload["decision"])
            continue
        if symbol.upper() in set(loss_guard.get("blocked_underlyings") or []):
            _append_skip(
                skipped,
                symbol=symbol.upper(),
                reason="recent_loss_guard",
                detail=json.dumps((loss_guard.get("reasons") or {}).get(symbol.upper(), {}), sort_keys=True),
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
                payload=(loss_guard.get("reasons") or {}).get(symbol.upper(), {}),
            )
            continue
        if symbol.upper() in active_underlyings:
            _append_skip(skipped, symbol=symbol.upper(), reason="underlying_exposure_already_open")
            _record_execution_rejection(
                execution_outcomes,
                execution_rejected_count_by_reason,
                ticker=symbol.upper(),
                decision_id=decision.decision_id,
                thesis_id=thesis_id,
                reason="underlying_exposure_already_open",
                detail=f"active_underlying={symbol.upper()}",
                journal_path=execution_log_path,
                payload={"active_underlyings": sorted(active_underlyings)},
            )
            continue
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
            order = submit_decision_to_broker(
                decision,
                broker=resolved_broker,
                quantity=quantity,
                current_daily_realized_pnl=current_daily_realized_pnl,
                open_positions=open_positions,
                journal_path=execution_log_path,
                on_submission_attempt=_mark_submission_attempt,
            )
            open_positions += 1
            active_underlyings.add(symbol.upper())
            orders_submitted.append(
                {
                    "symbol": symbol.upper(),
                    "broker_order_id": order.broker_order_id,
                    "state": order.state.value,
                    "client_order_id": order.client_order_id,
                }
            )
        except ExecutionRejectedError as exc:
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
        symbols=[symbol.upper() for symbol in symbols],
        snapshot_paths=snapshot_paths,
        decisions=decisions,
        orders_submitted=orders_submitted,
        skipped=skipped,
        runtime_state=runtime_state.to_json_dict(),
        execution_outcomes=[
            {"disposition": "position_monitor_summary", **monitor_summary},
            {"disposition": "trade_outcome_learning_summary", **outcome_learning_summary},
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
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True))
        handle.write("\n")
    return path


def load_decision_cards(*, log_path: str | Path | None = None, limit: int | None = None) -> list[dict[str, Any]]:
    path = Path(log_path) if log_path is not None else decision_journal_path()
    if not path.exists():
        return []
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
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


def _active_underlying_symbols(broker: Any | None = None) -> set[str]:
    if broker is not None and hasattr(broker, "list_open_positions"):
        try:
            symbols = {
                _underlying_from_option_symbol(str(position.get("symbol") or "")) or str(position.get("symbol") or "").upper()
                for position in broker.list_open_positions()
                if _broker_position_is_active(position)
            }
            symbols.update(_pending_entry_underlying_symbols(broker))
            return {symbol for symbol in symbols if symbol}
        except Exception:
            pass
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


def _paper_opportunistic_decision(
    strict_decision: DecisionCard,
    *,
    decision_input: Any,
    broker: AlpacaExecutionBroker,
) -> DecisionCard | None:
    if broker.config.environment is not BrokerEnvironment.PAPER:
        return None
    if not _paper_opportunistic_mode_enabled():
        return None
    if strict_decision.decision is DecisionStatus.TRADE_CANDIDATE:
        return None
    if strict_decision.decision not in {
        DecisionStatus.BLOCKED_BY_VOLATILITY,
        DecisionStatus.BLOCKED_BY_SPREAD,
        DecisionStatus.NO_TRADE,
    }:
        return None
    if strict_decision.decision is DecisionStatus.NO_TRADE and strict_decision.blocked_reason != "confidence_below_threshold":
        return None

    rules = _paper_opportunistic_rules()
    relaxed = build_decision_card(decision_input, rules)
    if relaxed.decision is not DecisionStatus.TRADE_CANDIDATE or relaxed.selected_contract is None:
        fallback = _paper_discovery_contract(strict_decision, decision_input=decision_input, rules=rules)
        if fallback is None:
            return None
        relaxed = fallback
    reason_codes = list(relaxed.reason_codes)
    reason_codes.extend(
        [
            "paper_opportunistic_discovery",
            f"strict_decision_{strict_decision.decision.value.lower()}",
        ]
    )
    if strict_decision.blocked_reason:
        reason_codes.append(f"strict_blocked_{strict_decision.blocked_reason}")
    return replace(
        relaxed,
        reason_codes=reason_codes,
        explanation=(
            f"{relaxed.explanation}; paper_opportunistic_discovery override from "
            f"{strict_decision.decision.value}:{strict_decision.blocked_reason or 'none'}"
        ),
    )


def _paper_opportunistic_mode_enabled() -> bool:
    value = os.getenv("AUTOBOTT_PAPER_OPPORTUNISTIC_ENTRIES")
    if value is None:
        return True
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _paper_opportunistic_rules() -> Phase1Rules:
    # Discovery mode relaxes signal-strength gates (confidence, direction score,
    # delta range) to generate more paper reps -- but it must never relax
    # liquidity below the strict engine's floor. A thin, wide-spread contract
    # bleeds the same real bid/ask cost in paper as it would live; the strict
    # defaults (spread<=18%, OI>=100, volume>=10) are the actual floor, not a
    # discovery-mode toggle.
    strict_defaults = Phase1Rules()
    return Phase1Rules(
        min_direction_score=0.20,
        min_volatility_score=-1.0,
        min_confidence=0.12,
        max_spread_pct=strict_defaults.max_spread_pct,
        min_open_interest=strict_defaults.min_open_interest,
        min_contract_volume=strict_defaults.min_contract_volume,
        min_abs_delta=0.15,
        max_abs_delta=0.85,
        intraday_min_abs_delta=0.20,
        intraday_max_abs_delta=0.85,
        min_vega=0.001,
        max_theta_abs=1.0,
        min_reward_risk_ratio=0.05,
    )


def _paper_discovery_contract(
    strict_decision: DecisionCard,
    *,
    decision_input: Any,
    rules: Phase1Rules,
) -> DecisionCard | None:
    if strict_decision.direction.bias is DirectionBias.NEUTRAL:
        return None
    option_type = OptionType.CALL if strict_decision.direction.bias is DirectionBias.BULLISH else OptionType.PUT
    candidates = [
        contract
        for contract in decision_input.option_chain
        if contract.option_type is option_type
        and contract.bid > 0
        and contract.ask > 0
        and contract.ask >= contract.bid
        and contract.mid <= _paper_discovery_max_contract_price()
        and contract.spread_pct <= rules.max_spread_pct
    ]
    if not candidates:
        return None
    selected_contract = _best_paper_discovery_contract(candidates, decision_input.timestamp.date())
    spread_penalty = min(1.0, selected_contract.spread_pct / max(rules.max_spread_pct, 0.01))
    delta_fit = max(0.0, 1 - abs(abs(selected_contract.delta) - 0.50) / 0.50)
    score = round(max(0.05, delta_fit * 0.55 + (1 - spread_penalty) * 0.30 + min(1.0, selected_contract.volume / 200) * 0.15), 4)
    contract_score = ContractScore(
        contract=selected_contract,
        score=score,
        reward_risk_ratio=0.0,
        reasons=[
            "paper_discovery_contract_selected",
            "soft_contract_filters_overridden",
            f"spread_pct={round(selected_contract.spread_pct, 4)}",
        ],
    )
    selected = SelectedContract.from_score(contract_score, rules)
    return replace(
        strict_decision,
        selected_contract=selected,
        tactical_contract=selected,
        rider_contract=None,
        execution_layer=ExecutionLayer.TACTICAL,
        decision=DecisionStatus.TRADE_CANDIDATE,
        blocked_reason=None,
        confidence_score=max(0.12, strict_decision.confidence_score),
    )


def _paper_discovery_max_contract_price() -> float:
    value = os.getenv("AUTOBOTT_PAPER_DISCOVERY_MAX_CONTRACT_PRICE")
    if value is None or not value.strip():
        return 10.0
    return max(0.01, float(value))


def _best_paper_discovery_contract(contracts: list[OptionContractSnapshot], as_of: Any) -> OptionContractSnapshot:
    return sorted(
        contracts,
        key=lambda contract: (
            abs((contract.expiration - as_of).days - 2),
            contract.mid,
            contract.spread_pct,
            abs(abs(contract.delta) - 0.50),
            -contract.volume,
            -contract.open_interest,
        ),
    )[0]


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
    append_execution_outcome(
        decision_id=decision_id,
        thesis_id=thesis_id,
        symbol=ticker,
        disposition=disposition,
        detail=detail,
        payload=payload,
        journal_path=journal_path,
    )


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
