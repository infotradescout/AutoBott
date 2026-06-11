from __future__ import annotations

import hashlib
from dataclasses import asdict
from datetime import datetime

from .models import (
    AccountState,
    DecisionReasonCode,
    MarketState,
    PaperOrder,
    RiskRules,
    TradeDecision,
    TradingSignal,
)


def _decision_id(seed: str) -> str:
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()[:16]


def _reject(
    *,
    reason_code: DecisionReasonCode,
    reason_detail: str,
    market_state: MarketState | None,
    account_state: AccountState | None,
    risk_rules: RiskRules,
    signal: TradingSignal,
) -> TradeDecision:
    seed = "|".join(
        [
            str(signal.signal_id),
            str(signal.symbol),
            reason_code.value,
            str(signal.timestamp.isoformat() if signal.timestamp else "missing_ts"),
        ]
    )
    return TradeDecision(
        decision_id=_decision_id(seed),
        accepted=False,
        reason_code=reason_code,
        reason_detail=reason_detail,
        paper_order=None,
        replay_payload={
            "market_state": asdict(market_state) if market_state is not None else None,
            "account_state": asdict(account_state) if account_state is not None else None,
            "risk_rules": asdict(risk_rules),
            "signal": asdict(signal),
            "decision_type": "reject",
        },
    )


def evaluate_trade(
    *,
    market_state: MarketState | None,
    account_state: AccountState | None,
    risk_rules: RiskRules,
    signal: TradingSignal,
) -> TradeDecision:
    if account_state is None:
        return _reject(
            reason_code=DecisionReasonCode.REJECT_MISSING_ACCOUNT_STATE,
            reason_detail="Account state is required.",
            market_state=market_state,
            account_state=account_state,
            risk_rules=risk_rules,
            signal=signal,
        )

    if market_state is None:
        return _reject(
            reason_code=DecisionReasonCode.REJECT_MISSING_MARKET_STATE,
            reason_detail="Market state is required.",
            market_state=market_state,
            account_state=account_state,
            risk_rules=risk_rules,
            signal=signal,
        )

    if signal.timestamp is None or market_state.timestamp is None:
        return _reject(
            reason_code=DecisionReasonCode.REJECT_MISSING_TIMESTAMP,
            reason_detail="Signal and market timestamps are required.",
            market_state=market_state,
            account_state=account_state,
            risk_rules=risk_rules,
            signal=signal,
        )

    if not str(signal.strategy_id or "").strip():
        return _reject(
            reason_code=DecisionReasonCode.REJECT_MISSING_STRATEGY_IDENTITY,
            reason_detail="Strategy identity is required.",
            market_state=market_state,
            account_state=account_state,
            risk_rules=risk_rules,
            signal=signal,
        )

    if account_state.realized_pnl <= -abs(risk_rules.max_daily_loss):
        return _reject(
            reason_code=DecisionReasonCode.REJECT_MAX_LOSS_EXCEEDED,
            reason_detail="Daily max loss reached.",
            market_state=market_state,
            account_state=account_state,
            risk_rules=risk_rules,
            signal=signal,
        )

    if signal.expected_entry_price <= 0:
        return _reject(
            reason_code=DecisionReasonCode.REJECT_POSITION_SIZE_TOO_LARGE,
            reason_detail="Expected entry price must be positive.",
            market_state=market_state,
            account_state=account_state,
            risk_rules=risk_rules,
            signal=signal,
        )

    max_notional = account_state.equity * risk_rules.max_position_fraction
    affordable_notional = min(max_notional, account_state.buying_power)
    units = int(affordable_notional // signal.expected_entry_price)

    if units <= 0:
        return _reject(
            reason_code=DecisionReasonCode.REJECT_INSUFFICIENT_BUYING_POWER,
            reason_detail="Insufficient buying power for one unit.",
            market_state=market_state,
            account_state=account_state,
            risk_rules=risk_rules,
            signal=signal,
        )

    if units > risk_rules.max_units_per_trade:
        return _reject(
            reason_code=DecisionReasonCode.REJECT_POSITION_SIZE_TOO_LARGE,
            reason_detail="Calculated size exceeds max units per trade.",
            market_state=market_state,
            account_state=account_state,
            risk_rules=risk_rules,
            signal=signal,
        )

    notional = units * signal.expected_entry_price
    timestamp = signal.timestamp or datetime.utcnow()
    order_seed = "|".join([signal.signal_id, signal.strategy_id, signal.symbol, timestamp.isoformat()])
    order = PaperOrder(
        order_id=f"paper-{_decision_id(order_seed)}",
        symbol=signal.symbol,
        side=signal.side,
        units=units,
        expected_fill_price=signal.expected_entry_price,
        notional=notional,
        created_at=timestamp,
    )

    decision_seed = "|".join([order.order_id, DecisionReasonCode.APPROVED_PAPER_ORDER.value])
    return TradeDecision(
        decision_id=_decision_id(decision_seed),
        accepted=True,
        reason_code=DecisionReasonCode.APPROVED_PAPER_ORDER,
        reason_detail="Paper order approved.",
        paper_order=order,
        replay_payload={
            "market_state": asdict(market_state),
            "account_state": asdict(account_state),
            "risk_rules": asdict(risk_rules),
            "signal": asdict(signal),
            "decision_type": "paper_order",
            "execution_mode": "paper_only",
        },
    )
