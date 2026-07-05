from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .execution_broker import AlpacaExecutionBroker
from .execution_journal import append_execution_outcome
from .execution_reconciler import reconcile_open_positions
from .execution_orchestrator import ExecutionRejectedError, submit_decision_to_broker
from .phase1_engine import build_decision_card
from .phase1_models import DecisionCard, DecisionStatus
from .phase1_snapshot_capture import CaptureRules, capture_symbol_snapshot
from .phase1_validate import _decision_input_from_snapshot, _load_snapshot
from .position_store import load_open_positions
from .runtime_control import load_runtime_state
from .runtime_paths import data_root, phase1_snapshots_root


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

    snapshot_paths: list[str] = []
    decisions: list[dict[str, Any]] = []
    orders_submitted: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    execution_outcomes: list[dict[str, Any]] = []
    execution_rejected_count_by_reason: dict[str, int] = {}
    scanner_candidates_count = 0
    trade_attempted_count = 0
    open_positions = max(position_count or 0, _active_open_position_count())
    max_new_entry_attempts_per_loop = resolved_broker.config.effective_max_new_entry_attempts_per_loop()

    for symbol in symbols:
        snapshot_path = capture_symbol_snapshot(
            symbol=symbol,
            corpus_root=resolved_corpus_root,
            scheduled_market_time=snapshot_time,
            captured_at_utc=captured_at,
            corpus_type="production_capture" if resolved_broker.config.environment.value == "live" else "paper_capture",
            market_timezone="America/New_York",
            volatility_proxy_symbol="VIXY",
            data_client=data_client,
            rules=resolved_rules,
        )
        snapshot_paths.append(snapshot_path)
        snapshot = _load_snapshot(Path(snapshot_path))
        decision = build_decision_card(_decision_input_from_snapshot(snapshot))
        decision_payload = decision.to_json_dict()
        decisions.append(decision_payload)
        append_decision_card(decision_payload, snapshot_path=snapshot_path, log_path=decision_log_path)
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
        execution_outcomes=execution_outcomes,
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


def _active_open_position_count() -> int:
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


def _decision_thesis_id(decision: DecisionCard) -> str:
    return f"{decision.ticker}:{decision.trade_setup.value}:{decision.execution_layer.value}"


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
