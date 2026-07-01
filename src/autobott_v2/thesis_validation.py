from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime
from typing import Any

from .phase1_models import DecisionCard, OptionType, TradeSetup


@dataclass(frozen=True)
class ThesisValidationResult:
    decision_id: str
    ticker: str
    trade_setup: str
    option_type: str
    contract_dte_days: int | None
    horizon_end: str | None
    entry_underlying_price: float
    end_underlying_price: float | None
    net_move_pct: float | None
    prior_move_pct: float | None
    first_move_pct: float | None
    favorable_move_pct: float | None
    adverse_move_pct: float | None
    followthrough_rate: float | None
    directional_match: bool
    first_move_match: bool
    reversal_confirmed: bool
    passed: bool
    reason: str
    snapshots_seen: int

    def to_json_dict(self) -> dict[str, Any]:
        return asdict(self)


def evaluate_decision_thesis(
    decision: DecisionCard,
    entry_snapshot: dict[str, Any],
    future_snapshots: list[dict[str, Any]],
) -> ThesisValidationResult:
    if decision.selected_contract is None:
        return ThesisValidationResult(
            decision_id=decision.decision_id,
            ticker=decision.ticker,
            trade_setup=decision.trade_setup.value,
            option_type="unknown",
            contract_dte_days=None,
            horizon_end=None,
            entry_underlying_price=float(entry_snapshot["underlying_quote"]["last"]),
            end_underlying_price=None,
            net_move_pct=None,
            prior_move_pct=None,
            first_move_pct=None,
            favorable_move_pct=None,
            adverse_move_pct=None,
            followthrough_rate=None,
            directional_match=False,
            first_move_match=False,
            reversal_confirmed=False,
            passed=False,
            reason="missing_selected_contract",
            snapshots_seen=0,
        )

    horizon_date = decision.selected_contract.expiration
    contract_dte_days = max(0, (horizon_date - _parse_datetime(entry_snapshot["timestamp"]).date()).days)
    horizon_snapshots = [
        snapshot
        for snapshot in future_snapshots
        if snapshot.get("ticker") == decision.ticker and _parse_datetime(snapshot["timestamp"]).date() <= horizon_date
    ]
    entry_price = float(entry_snapshot["underlying_quote"]["last"])
    prior_move_pct = _prior_move_pct(entry_snapshot)
    option_type = decision.selected_contract.option_type

    if not horizon_snapshots:
        return ThesisValidationResult(
            decision_id=decision.decision_id,
            ticker=decision.ticker,
            trade_setup=decision.trade_setup.value,
            option_type=option_type.value,
            contract_dte_days=contract_dte_days,
            horizon_end=None,
            entry_underlying_price=entry_price,
            end_underlying_price=None,
            net_move_pct=None,
            prior_move_pct=prior_move_pct,
            first_move_pct=None,
            favorable_move_pct=None,
            adverse_move_pct=None,
            followthrough_rate=None,
            directional_match=False,
            first_move_match=False,
            reversal_confirmed=False,
            passed=False,
            reason="no_future_snapshots_before_expiration",
            snapshots_seen=0,
        )

    end_snapshot = horizon_snapshots[-1]
    end_price = float(end_snapshot["underlying_quote"]["last"])
    net_move_pct = (end_price - entry_price) / entry_price if entry_price else 0.0
    direction_sign = 1 if option_type is OptionType.CALL else -1
    signed_moves = [
        direction_sign * ((float(snapshot["underlying_quote"]["last"]) - entry_price) / entry_price if entry_price else 0.0)
        for snapshot in horizon_snapshots
    ]
    first_move_pct = signed_moves[0] if signed_moves else None
    favorable_move_pct = max(signed_moves) if signed_moves else None
    adverse_move_pct = min(signed_moves) if signed_moves else None
    followthrough_rate = sum(1 for move in signed_moves if move > 0) / len(signed_moves)
    directional_match = signed_moves[-1] > 0
    first_move_match = bool(first_move_pct is not None and first_move_pct > 0)
    tactical_2dte_followthrough = contract_dte_days <= 2
    followthrough_confirmed = directional_match and first_move_match and followthrough_rate >= 0.60

    reversal_setup = decision.trade_setup in {
        TradeSetup.LATE_CYCLE_BULLISH_REVERSAL,
        TradeSetup.LATE_CYCLE_BEARISH_REVERSAL,
    }
    if reversal_setup:
        reversal_confirmed = (
            decision.trade_setup is TradeSetup.LATE_CYCLE_BULLISH_REVERSAL
            and prior_move_pct is not None
            and prior_move_pct < 0
            and followthrough_confirmed
        ) or (
            decision.trade_setup is TradeSetup.LATE_CYCLE_BEARISH_REVERSAL
            and prior_move_pct is not None
            and prior_move_pct > 0
            and followthrough_confirmed
        )
        passed = directional_match and reversal_confirmed
        reason = "reversal_confirmed" if passed else "reversal_not_confirmed"
    else:
        reversal_confirmed = False
        passed = followthrough_confirmed if tactical_2dte_followthrough else directional_match
        reason = "directional_followthrough" if passed else "directional_followthrough_failed"

    return ThesisValidationResult(
        decision_id=decision.decision_id,
        ticker=decision.ticker,
        trade_setup=decision.trade_setup.value,
        option_type=option_type.value,
        contract_dte_days=contract_dte_days,
        horizon_end=end_snapshot["timestamp"],
        entry_underlying_price=entry_price,
        end_underlying_price=end_price,
        net_move_pct=round(net_move_pct, 4),
        prior_move_pct=round(prior_move_pct, 4) if prior_move_pct is not None else None,
        first_move_pct=round(first_move_pct, 4) if first_move_pct is not None else None,
        favorable_move_pct=round(favorable_move_pct, 4) if favorable_move_pct is not None else None,
        adverse_move_pct=round(adverse_move_pct, 4) if adverse_move_pct is not None else None,
        followthrough_rate=round(followthrough_rate, 4) if followthrough_rate is not None else None,
        directional_match=directional_match,
        first_move_match=first_move_match,
        reversal_confirmed=reversal_confirmed,
        passed=passed,
        reason=reason,
        snapshots_seen=len(horizon_snapshots),
    )


def summarize_thesis_results(results: list[ThesisValidationResult]) -> dict[str, Any]:
    if not results:
        return {
            "decisions_evaluated": 0,
            "passes": 0,
            "fails": 0,
            "pass_rate": 0.0,
            "continuation_pass_rate": 0.0,
            "reversal_pass_rate": 0.0,
            "tactical_2dte_pass_rate": 0.0,
        }
    passes = [result for result in results if result.passed]
    continuations = [result for result in results if "continuation" in result.trade_setup]
    reversals = [result for result in results if "reversal" in result.trade_setup]
    tactical_2dte = [result for result in results if result.contract_dte_days is not None and result.contract_dte_days <= 2]
    return {
        "decisions_evaluated": len(results),
        "passes": len(passes),
        "fails": len(results) - len(passes),
        "pass_rate": round(len(passes) / len(results), 4),
        "continuation_pass_rate": round(sum(1 for item in continuations if item.passed) / len(continuations), 4) if continuations else 0.0,
        "reversal_pass_rate": round(sum(1 for item in reversals if item.passed) / len(reversals), 4) if reversals else 0.0,
        "tactical_2dte_pass_rate": round(sum(1 for item in tactical_2dte if item.passed) / len(tactical_2dte), 4) if tactical_2dte else 0.0,
    }


def _prior_move_pct(snapshot: dict[str, Any], *, lookback_bars: int = 5) -> float | None:
    market_bars = snapshot.get("market_bars", [])
    if len(market_bars) < lookback_bars:
        return None
    closes = [float(item["close"]) for item in market_bars[-lookback_bars:]]
    if not closes or closes[0] == 0:
        return None
    return (closes[-1] - closes[0]) / closes[0]


def _parse_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))
