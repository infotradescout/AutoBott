from __future__ import annotations

import argparse
import json
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .execution_config import AlpacaExecutionConfig
from .execution_journal import append_order_submission, append_risk_check
from .execution_models import (
    BrokerEnvironment,
    ExecutionOrder,
    ExecutionState,
    OrderSide,
    RiskCheckResult,
    TradeIntent,
    build_execution_order,
    validate_trade_intent,
)
from .exit_orchestrator import build_exit_intent_from_position
from .phase1_engine import build_decision_card
from .phase1_exit_engine import ExitRules, evaluate_exit
from .phase1_models import LifecycleStatus, Phase1LedgerEvent
from .phase1_scorecard import create_ledger_event, load_phase1_gate, update_phase1_gate
from .phase1_snapshot_corpus import load_snapshot_corpus
from .thesis_validation import evaluate_decision_thesis, summarize_thesis_results
from .phase1_validate import _decision_input_from_snapshot, _load_snapshot, _parse_datetime
from .position_store import OpenPosition
from .runtime_paths import artifacts_root as default_artifacts_root
from .trading_cycle import append_decision_card


class HistoricalSimBroker:
    def __init__(self, config: AlpacaExecutionConfig, *, fill_model: str = "inside_spread") -> None:
        self.config = config.validate()
        self.fill_model = fill_model
        self._current_snapshot: dict[str, Any] | None = None
        self._current_timestamp: datetime | None = None
        self._orders: dict[str, dict[str, Any]] = {}
        self._fills: dict[str, float] = {}
        self._counter = 0

    def set_snapshot(self, snapshot: dict[str, Any]) -> None:
        self._current_snapshot = snapshot
        self._current_timestamp = _parse_datetime(snapshot["timestamp"])

    def submit_order(
        self,
        intent: TradeIntent,
        *,
        current_daily_realized_pnl: float = 0.0,
        open_positions: int = 0,
    ) -> ExecutionOrder:
        risk_check = validate_trade_intent(
            intent,
            self.config.risk_controls(),
            current_daily_realized_pnl=current_daily_realized_pnl,
            open_positions=open_positions,
        )
        order = build_execution_order(intent, risk_check)
        self._counter += 1
        broker_order_id = f"historical-order-{self._counter}"
        fill_price = self._fill_price(intent)
        state = ExecutionState.FILLED if fill_price is not None else ExecutionState.REJECTED
        submitted_at = self._current_timestamp or datetime.now(tz=UTC)
        resolved = ExecutionOrder(
            order_id=order.order_id,
            client_order_id=order.client_order_id,
            intent=order.intent,
            state=state,
            submitted_at=submitted_at,
            broker_order_id=broker_order_id,
        )
        self._orders[broker_order_id] = {
            "id": broker_order_id,
            "client_order_id": order.client_order_id,
            "status": state.value,
            "submitted_at": submitted_at.isoformat(),
            "symbol": intent.option_symbol,
            "qty": str(intent.quantity),
            "limit_price": f"{intent.limit_price:.2f}",
        }
        if fill_price is not None:
            self._fills[broker_order_id] = fill_price
        return resolved

    def get_order(self, broker_order_id: str) -> dict[str, Any]:
        return dict(self._orders.get(broker_order_id, {}))

    def filled_price(self, broker_order_id: str) -> float | None:
        return self._fills.get(broker_order_id)

    def _fill_price(self, intent: TradeIntent) -> float | None:
        contract = _option_quote(self._current_snapshot, intent.option_symbol)
        if contract is None:
            return None
        bid = float(contract["bid"])
        ask = float(contract["ask"])
        if bid <= 0 or ask <= 0 or ask < bid:
            return None
        if intent.side is OrderSide.BUY_TO_OPEN:
            if intent.limit_price < bid:
                return None
            return round(min(max(intent.limit_price, bid), ask), 4)
        if intent.limit_price > ask:
            return None
        return round(max(min(intent.limit_price, ask), bid), 4)


def run_historical_live_simulation(
    snapshot_corpus: str | Path,
    *,
    symbols: list[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    artifacts_root: str | Path | None = None,
    run_id: str = "historical-live-sim",
    quantity: int = 1,
    config: AlpacaExecutionConfig | None = None,
) -> dict[str, Any]:
    corpus = load_snapshot_corpus(snapshot_corpus, symbols=symbols, start_date=start_date, end_date=end_date)
    snapshot_paths = [Path(path) for path in corpus["snapshot_paths"]]
    snapshots = sorted((_load_snapshot(path) for path in snapshot_paths), key=lambda payload: _parse_datetime(payload["timestamp"]))
    artifact_dir = (Path(artifacts_root) if artifacts_root is not None else default_artifacts_root() / "historical_live_sim") / run_id
    artifact_dir.mkdir(parents=True, exist_ok=True)

    resolved_config = (config or AlpacaExecutionConfig(
        environment=BrokerEnvironment.PAPER,
        api_key="historical-sim",
        secret_key="historical-sim",
        trading_base_url="https://paper-api.alpaca.markets",
        data_base_url="https://data.alpaca.markets",
        allow_live_trading=False,
        allow_order_placement=True,
        max_position_cost=1000.0,
        max_daily_loss=500.0,
        max_open_positions=1,
    )).validate()
    broker = HistoricalSimBroker(resolved_config)

    decisions: list[dict[str, Any]] = []
    orders: list[dict[str, Any]] = []
    outcomes: list[dict[str, Any]] = []
    thesis_results = []
    open_events: list[Phase1LedgerEvent] = []
    terminal_events: list[Phase1LedgerEvent] = []
    realized_pnl = 0.0

    decision_log_path = artifact_dir / "decisions_runtime.jsonl"
    execution_log_path = artifact_dir / "execution_orders.jsonl"
    open_positions_path = artifact_dir / "open_positions.jsonl"
    outcomes_path = artifact_dir / "outcomes.jsonl"

    for snapshot in snapshots:
        broker.set_snapshot(snapshot)
        timestamp = _parse_datetime(snapshot["timestamp"])

        still_open: list[Phase1LedgerEvent] = []
        for open_event in open_events:
            quote_age_seconds = _quote_age_seconds(snapshot, open_event.selected_contract.option_symbol) if open_event.selected_contract else 0
            exit_decision = evaluate_exit(open_event, snapshot, quote_age_seconds=quote_age_seconds, rules=ExitRules())
            if exit_decision.exit_action == "close":
                exit_position = _position_from_event(open_event)
                exit_intent = build_exit_intent_from_position(
                    exit_position,
                    limit_price=exit_decision.exit_fill_price or exit_position.entry_limit_price,
                    exit_reason=exit_decision.exit_reason or "automated_exit",
                    environment=resolved_config.environment,
                )
                exit_risk = RiskCheckResult(
                    approved=True,
                    reasons=(),
                    estimated_notional=round(exit_intent.quantity * exit_intent.limit_price * 100, 2),
                    normalized_limit_price=round(exit_intent.limit_price, 2),
                )
                append_risk_check(exit_intent, exit_risk, journal_path=execution_log_path)
                exit_order = broker.submit_order(exit_intent, current_daily_realized_pnl=realized_pnl, open_positions=max(0, len(open_events) - 1))
                append_order_submission(exit_order, journal_path=execution_log_path)
                closed = replace(
                    open_event,
                    lifecycle_status=LifecycleStatus.CLOSED,
                    timestamp=timestamp,
                    exit_option_bid=exit_decision.exit_option_bid,
                    exit_option_ask=exit_decision.exit_option_ask,
                    exit_option_mid=exit_decision.exit_option_mid,
                    exit_spread_pct=exit_decision.exit_spread_pct,
                    exit_fill_model=exit_decision.exit_fill_model,
                    exit_fill_price=broker.filled_price(exit_order.broker_order_id or "") or exit_decision.exit_fill_price,
                    exit_reason=exit_decision.exit_reason,
                    option_return_pct=exit_decision.option_return_pct,
                    pnl=exit_decision.pnl_dollars,
                    hold_minutes=exit_decision.hold_minutes,
                    underlying_price_at_exit=exit_decision.exit_underlying_price,
                )
                realized_pnl += float(closed.pnl or 0.0)
                outcomes.append(closed.to_json_dict())
                terminal_events.append(closed)
            elif exit_decision.exit_action == "unresolved":
                unresolved = replace(
                    open_event,
                    lifecycle_status=LifecycleStatus.UNRESOLVED,
                    timestamp=timestamp,
                    exit_reason=exit_decision.exit_reason,
                )
                outcomes.append(unresolved.to_json_dict())
                terminal_events.append(unresolved)
            else:
                still_open.append(open_event)
        open_events = still_open

        decision = build_decision_card(_decision_input_from_snapshot(snapshot))
        decision_payload = decision.to_json_dict()
        decisions.append(decision_payload)
        append_decision_card(decision_payload, snapshot_path=snapshot["timestamp"], log_path=decision_log_path)
        if decision.selected_contract is not None and decision.decision.value == "TRADE_CANDIDATE":
            future_snapshots = [item for item in snapshots if _parse_datetime(item["timestamp"]) > timestamp]
            thesis_results.append(evaluate_decision_thesis(decision, snapshot, future_snapshots))

        if decision.decision.value != "TRADE_CANDIDATE" or len(open_events) >= resolved_config.max_open_positions:
            continue

        try:
            intent = _trade_intent_from_decision(decision, quantity=quantity, environment=resolved_config.environment)
        except ValueError:
            continue
        risk_check = validate_trade_intent(
            intent,
            resolved_config.risk_controls(),
            current_daily_realized_pnl=realized_pnl,
            open_positions=len(open_events),
        )
        append_risk_check(intent, risk_check, journal_path=execution_log_path)
        if not risk_check.approved:
            continue
        order = broker.submit_order(intent, current_daily_realized_pnl=realized_pnl, open_positions=len(open_events))
        append_order_submission(order, journal_path=execution_log_path)
        orders.append(
            {
                "timestamp": timestamp.isoformat(),
                "decision_id": decision.decision_id,
                "broker_order_id": order.broker_order_id,
                "state": order.state.value,
                "option_symbol": intent.option_symbol,
            }
        )
        if order.state is not ExecutionState.FILLED:
            continue
        contract = decision.selected_contract
        fill_price = broker.filled_price(order.broker_order_id or "")
        open_event = create_ledger_event(
            decision_id=decision.decision_id,
            ticker=decision.ticker,
            timestamp=timestamp,
            trade_setup=decision.trade_setup,
            execution_layer=decision.execution_layer,
            cycle_confidence=decision.cycle.status,
            selected_contract=contract,
            filled=True,
            lifecycle_status=LifecycleStatus.OPEN,
            entry_fill_model=broker.fill_model,
            entry_underlying_price=snapshot["underlying_quote"]["last"],
            entry_option_bid=contract.bid if contract else None,
            entry_option_ask=contract.ask if contract else None,
            entry_option_mid=contract.mid if contract else None,
            entry_spread_pct=round(contract.spread_pct, 4) if contract else None,
            entry_fill_price=fill_price,
            contract_volume=contract.volume if contract else None,
            contract_open_interest=contract.open_interest if contract else None,
            quote_age_seconds=0,
        )
        open_events.append(open_event)

    final_timestamp = _parse_datetime(snapshots[-1]["timestamp"]) if snapshots else datetime.now(tz=UTC)
    for open_event in open_events:
        unresolved = replace(
            open_event,
            lifecycle_status=LifecycleStatus.UNRESOLVED,
            timestamp=final_timestamp,
            exit_reason="end_of_corpus_open_position",
        )
        outcomes.append(unresolved.to_json_dict())
        terminal_events.append(unresolved)

    _write_jsonl(open_positions_path, [event.to_json_dict() for event in open_events])
    _write_jsonl(outcomes_path, outcomes)
    replay_gate_path = artifact_dir / "gate.json"
    scorecard = update_phase1_gate(terminal_events, replay_gate_path)
    scorecard["simulation_type"] = "historical_live_sim"
    scorecard["thesis_validation"] = summarize_thesis_results(thesis_results)
    scorecard["decision_stats"]["snapshots_processed"] = len(snapshots)
    scorecard["decision_stats"]["decisions_generated"] = len(decisions)
    scorecard["decision_stats"]["orders_attempted"] = len(orders)
    scorecard["decision_stats"]["orders_filled"] = len([order for order in orders if order["state"] == "filled"])
    replay_gate_path.write_text(json.dumps(scorecard, indent=2, sort_keys=True), encoding="utf-8")
    gate = load_phase1_gate(replay_gate_path)
    summary = {
        "run_id": run_id,
        "artifact_dir": str(artifact_dir),
        "symbols": corpus["symbols"],
        "snapshots_processed": len(snapshots),
        "decisions_generated": len(decisions),
        "orders_attempted": len(orders),
        "closed_trades": len([event for event in terminal_events if event.lifecycle_status is LifecycleStatus.CLOSED]),
        "unresolved_positions": len([event for event in terminal_events if event.lifecycle_status is LifecycleStatus.UNRESOLVED]),
        "gate_reason": gate.reason,
        "thesis_validation": summarize_thesis_results(thesis_results),
        "scorecard_path": str(replay_gate_path),
    }
    (artifact_dir / "simulation_summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run historical snapshots through the live execution stack with a simulated broker.")
    parser.add_argument("--snapshot-corpus", required=True, help="Root directory containing manifest-backed historical snapshots.")
    parser.add_argument("--symbols", nargs="+", help="Optional ticker filter.")
    parser.add_argument("--start-date", help="Optional inclusive YYYY-MM-DD start date.")
    parser.add_argument("--end-date", help="Optional inclusive YYYY-MM-DD end date.")
    parser.add_argument("--run-id", default="historical-live-sim", help="Artifact run id.")
    args = parser.parse_args(argv)

    result = run_historical_live_simulation(
        args.snapshot_corpus,
        symbols=args.symbols,
        start_date=args.start_date,
        end_date=args.end_date,
        run_id=args.run_id,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def _trade_intent_from_decision(decision: Any, *, quantity: int, environment: BrokerEnvironment) -> TradeIntent:
    from .execution_orchestrator import build_trade_intent_from_decision

    return build_trade_intent_from_decision(decision, quantity=quantity, environment=environment)


def _position_from_event(event: Phase1LedgerEvent) -> OpenPosition:
    return OpenPosition(
        broker_order_id=event.decision_id,
        decision_id=event.decision_id,
        symbol=event.ticker,
        option_symbol=event.selected_contract.option_symbol if event.selected_contract else event.ticker,
        quantity=1,
        entry_limit_price=float(event.entry_fill_price or 0.0),
        entry_submitted_at=event.timestamp,
        take_profit_price=event.selected_contract.target_exit_mid if event.selected_contract else None,
        stop_loss_price=event.selected_contract.stop_exit_mid if event.selected_contract else None,
        status=event.lifecycle_status.value,
    )


def _option_quote(snapshot: dict[str, Any] | None, option_symbol: str) -> dict[str, Any] | None:
    if snapshot is None:
        return None
    for contract in snapshot.get("option_chain", []):
        if contract["option_symbol"] == option_symbol:
            return contract
    return None


def _quote_age_seconds(snapshot: dict[str, Any], option_symbol: str) -> int:
    snapshot_time = _parse_datetime(snapshot["timestamp"])
    contract = _option_quote(snapshot, option_symbol)
    if contract is None:
        return 999999
    quote_time = _parse_datetime(contract["quote_timestamp"])
    return max(0, int((snapshot_time - quote_time).total_seconds()))


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    payload = "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows)
    path.write_text(payload, encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
