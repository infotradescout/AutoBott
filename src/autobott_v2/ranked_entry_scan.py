"""Batch ranking with native decisions and fresh pre-entry reconsideration.

Ranking uses the existing model's scores, not a fabricated probability of profit.
Provider errors are still handled by the native cycle; no orders occur here.
"""
from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
import math
from pathlib import Path
from typing import Any, Callable

from .phase1_models import DecisionStatus


def _finite(value: Any) -> float:
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("ranking_score_not_finite")
    return number


class RankedCapturePlan:
    def __init__(self, *, capture: Callable, load: Callable, make_input: Callable,
                 build: Callable, execution_rules: Callable, capture_args: Callable,
                 original_priority: Callable):
        self.capture = capture
        self.load = load
        self.make_input = make_input
        self.build = build
        self.execution_rules = execution_rules
        self.capture_args = capture_args
        self.original_priority = original_priority
        self.rows: dict[str, dict] = {}
        self.summary: list[dict] = []

    def rank_symbols(self, symbols: list[str], winner_bias: dict) -> list[str]:
        ordered = self.original_priority(symbols, winner_bias)
        sortable = []
        for index, raw_symbol in enumerate(ordered):
            symbol = raw_symbol.upper()
            if symbol in self.rows:
                continue
            try:
                path = self.capture(symbol=symbol, **self.capture_args())
                decision = self.build(self.make_input(self.load(Path(path))), self.execution_rules())
                contract = decision.selected_contract
                candidate = decision.decision is DecisionStatus.TRADE_CANDIDATE and contract is not None
                scores = (0.0, 0.0, 0.0, 0.0)
                if candidate:
                    scores = (_finite(decision.confidence_score), _finite(contract.reward_risk_ratio),
                              _finite(contract.contract_score), _finite(contract.spread_pct))
                self.rows[symbol] = {"path": path, "decision": decision, "candidate": candidate}
                # Preserve prior ordering among non-candidates and exact ties.
                key = (not candidate, -scores[0], -scores[1], -scores[2], scores[3], index)
                sortable.append((key, symbol))
            except Exception as exc:
                self.rows[symbol] = {"error_type": type(exc).__name__, "candidate": False}
                sortable.append(((True, 0.0, 0.0, 0.0, 0.0, index), symbol))
        result = [symbol for _, symbol in sorted(sortable)]
        for position, symbol in enumerate(result, 1):
            row = self.rows[symbol]
            decision = row.get("decision")
            self.summary.append({"symbol": symbol, "scan_rank": position,
                "candidate": row["candidate"], "confidence": decision.confidence_score if decision else None,
                "selected_contract": decision.selected_contract.option_symbol if decision and decision.selected_contract else None,
                "error_type": row.get("error_type")})
        return result

    def capture_for_execution(self, *, symbol: str, **kwargs: Any) -> Any:
        row = self.rows.get(symbol.upper(), {})
        if row.get("path") and not row.get("candidate"):
            return row["path"]
        # Candidates are refreshed rather than spending against a quote from
        # the beginning of a complete batch. Failed pre-scans get normal retry.
        observed = datetime.now(UTC)
        return self.capture(symbol=symbol, **{**kwargs,
            "scheduled_market_time": observed, "captured_at_utc": observed})

    def build_for_execution(self, input_: Any, rules: Any) -> Any:
        decision = self.build(input_, rules)
        prior = self.rows.get(input_.ticker.upper(), {}).get("decision")
        if (prior is None or prior.decision is not DecisionStatus.TRADE_CANDIDATE
                or decision.decision is not DecisionStatus.TRADE_CANDIDATE):
            return decision
        before, after = prior.selected_contract, decision.selected_contract
        reason = None
        if before is None or after is None or before.option_symbol != after.option_symbol:
            reason = "ranked_candidate_contract_changed"
        elif _finite(after.ask) > _finite(before.ask):
            reason = "ranked_candidate_original_price_allowance_exceeded"
        if reason:
            return replace(decision, decision=DecisionStatus.NO_TRADE, blocked_reason=reason,
                           reason_codes=[*decision.reason_codes, reason])
        return decision
