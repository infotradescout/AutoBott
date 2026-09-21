from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from .phase1_models import DecisionCard, ExecutionLayer, LegRole, LifecycleStatus, Phase1LedgerEvent, SelectedContract
from .phase1_scorecard import create_ledger_event


@dataclass(frozen=True)
class ExecutionSimRules:
    max_spread_pct: float = 0.18
    max_quote_age_seconds: int = 30
    min_contract_volume: int = 10
    min_open_interest: int = 100
    min_option_mid: float = 0.50
    tactical_min_abs_delta: float = 0.45
    tactical_max_abs_delta: float = 0.65
    rider_min_abs_delta: float = 0.25
    rider_max_abs_delta: float = 0.70
    entry_slippage_pct: float = 0.05
    fill_model: str = "realistic_mid_penalty"


def simulate_execution(
    decision_card: DecisionCard,
    *,
    quote_age_seconds: int = 0,
    underlying_price_at_entry: float | None = None,
    underlying_price_at_exit: float | None = None,
    exit_option_bid: float | None = None,
    exit_option_ask: float | None = None,
    exit_reason: str | None = None,
    timestamp: datetime | None = None,
    rules: ExecutionSimRules | None = None,
) -> list[Phase1LedgerEvent]:
    rules = rules or ExecutionSimRules()
    if decision_card.execution_layer == ExecutionLayer.NONE or decision_card.selected_contract is None:
        return [
            _rejected_event(
                decision_card,
                decision_card.selected_contract,
                LegRole.TACTICAL if decision_card.execution_layer == ExecutionLayer.TACTICAL else None,
                quote_age_seconds,
                "rejected",
                underlying_price_at_entry,
                timestamp,
            )
        ]

    events: list[Phase1LedgerEvent] = []
    if decision_card.execution_layer in {ExecutionLayer.TACTICAL, ExecutionLayer.BOTH} and decision_card.tactical_contract is not None:
        events.append(
            _simulate_leg(
                decision_card,
                decision_card.tactical_contract,
                LegRole.TACTICAL,
                quote_age_seconds,
                underlying_price_at_entry,
                underlying_price_at_exit,
                exit_option_bid,
                exit_option_ask,
                exit_reason,
                timestamp,
                rules,
            )
        )
    if decision_card.execution_layer in {ExecutionLayer.RIDER, ExecutionLayer.BOTH} and decision_card.rider_contract is not None:
        events.append(
            _simulate_leg(
                decision_card,
                decision_card.rider_contract,
                LegRole.RIDER,
                quote_age_seconds,
                underlying_price_at_entry,
                underlying_price_at_exit,
                exit_option_bid,
                exit_option_ask,
                exit_reason,
                timestamp,
                rules,
            )
        )
    return events


def _simulate_leg(
    decision_card: DecisionCard,
    contract: SelectedContract,
    leg_role: LegRole,
    quote_age_seconds: int,
    underlying_price_at_entry: float | None,
    underlying_price_at_exit: float | None,
    exit_option_bid: float | None,
    exit_option_ask: float | None,
    exit_reason: str | None,
    timestamp: datetime | None,
    rules: ExecutionSimRules,
) -> Phase1LedgerEvent:
    rejection_reason = _tradability_rejection_reason(contract, leg_role, quote_age_seconds, rules)
    if rejection_reason is not None:
        return _rejected_event(
            decision_card,
            contract,
            leg_role,
            quote_age_seconds,
            rejection_reason,
            underlying_price_at_entry,
            timestamp,
        )

    entry_fill_price, entry_fill_model = _entry_fill(contract, rules)
    exit_mid = None
    exit_spread_pct = None
    exit_fill_price = None
    pnl = None
    if exit_option_bid is not None and exit_option_ask is not None and exit_option_bid > 0 and exit_option_ask >= exit_option_bid:
        exit_mid = round((exit_option_bid + exit_option_ask) / 2, 4)
        exit_spread_pct = round((exit_option_ask - exit_option_bid) / exit_mid, 4) if exit_mid > 0 else None
        exit_fill_price = round(exit_mid * (1 - rules.entry_slippage_pct), 4)
        pnl = round(exit_fill_price - entry_fill_price, 4)

    return create_ledger_event(
        decision_id=decision_card.decision_id if leg_role == LegRole.TACTICAL else f"{decision_card.decision_id}:{leg_role.value}",
        parent_decision_id=decision_card.decision_id if leg_role == LegRole.RIDER else None,
        leg_role=leg_role,
        ticker=decision_card.ticker,
        timestamp=timestamp or decision_card.timestamp,
        trade_setup=decision_card.trade_setup,
        execution_layer=ExecutionLayer.TACTICAL if leg_role == LegRole.TACTICAL else ExecutionLayer.RIDER,
        cycle_confidence=decision_card.cycle.status,
        selected_contract=contract,
        filled=True,
        lifecycle_status=LifecycleStatus.OPEN,
        entry_fill_model=entry_fill_model,
        entry_underlying_price=underlying_price_at_entry,
        entry_option_bid=contract.bid,
        entry_option_ask=contract.ask,
        entry_option_mid=contract.mid,
        entry_spread_pct=round(contract.spread_pct, 4),
        entry_fill_price=entry_fill_price,
        exit_option_bid=exit_option_bid,
        exit_option_ask=exit_option_ask,
        exit_option_mid=exit_mid,
        exit_spread_pct=exit_spread_pct,
        exit_fill_model="mid_minus_slippage" if exit_fill_price is not None else None,
        exit_fill_price=exit_fill_price,
        exit_reason=exit_reason,
        option_return_pct=None,
        pnl=pnl,
        max_favorable_excursion=None,
        max_adverse_excursion=None,
        hold_minutes=None,
        contract_volume=contract.volume,
        contract_open_interest=contract.open_interest,
        quote_age_seconds=quote_age_seconds,
        underlying_price_at_exit=underlying_price_at_exit,
    )


def _rejected_event(
    decision_card: DecisionCard,
    contract: SelectedContract | None,
    leg_role: LegRole | None,
    quote_age_seconds: int,
    rejection_reason: str,
    underlying_price_at_entry: float | None,
    timestamp: datetime | None,
) -> Phase1LedgerEvent:
    execution_layer = ExecutionLayer.NONE if leg_role is None else ExecutionLayer.TACTICAL if leg_role == LegRole.TACTICAL else ExecutionLayer.RIDER
    return create_ledger_event(
        decision_id=decision_card.decision_id if leg_role != LegRole.RIDER else f"{decision_card.decision_id}:{leg_role.value}",
        parent_decision_id=decision_card.decision_id if leg_role == LegRole.RIDER else None,
        leg_role=leg_role,
        ticker=decision_card.ticker,
        timestamp=timestamp or decision_card.timestamp,
        trade_setup=decision_card.trade_setup,
        execution_layer=execution_layer,
        cycle_confidence=decision_card.cycle.status,
        selected_contract=contract,
        filled=False,
        lifecycle_status=LifecycleStatus.REJECTED,
        entry_fill_model="rejected",
        entry_underlying_price=underlying_price_at_entry,
        entry_option_bid=contract.bid if contract else None,
        entry_option_ask=contract.ask if contract else None,
        entry_option_mid=contract.mid if contract else None,
        entry_spread_pct=round(contract.spread_pct, 4) if contract else None,
        entry_fill_price=None,
        exit_option_bid=None,
        exit_option_ask=None,
        exit_option_mid=None,
        exit_spread_pct=None,
        exit_fill_model=None,
        exit_fill_price=None,
        exit_reason=rejection_reason,
        option_return_pct=None,
        pnl=None,
        max_favorable_excursion=None,
        max_adverse_excursion=None,
        hold_minutes=None,
        contract_volume=contract.volume if contract else None,
        contract_open_interest=contract.open_interest if contract else None,
        quote_age_seconds=quote_age_seconds,
        underlying_price_at_exit=None,
    )


def _tradability_rejection_reason(
    contract: SelectedContract,
    leg_role: LegRole,
    quote_age_seconds: int,
    rules: ExecutionSimRules,
) -> str | None:
    if contract.bid <= 0:
        return "zero_bid_rejects_contract"
    if contract.ask <= 0:
        return "zero_ask_rejects_contract"
    if contract.spread_pct > rules.max_spread_pct:
        return "wide_bid_ask_rejects_contract"
    if quote_age_seconds > rules.max_quote_age_seconds:
        return "stale_quote_rejects_contract"
    if contract.volume < rules.min_contract_volume or contract.open_interest < rules.min_open_interest:
        return "low_volume_contract_rejected"
    if contract.mid < rules.min_option_mid:
        return "min_premium_contract_rejected"
    abs_delta = abs(contract.delta)
    if leg_role == LegRole.TACTICAL and not (rules.tactical_min_abs_delta <= abs_delta <= rules.tactical_max_abs_delta):
        return "delta_outside_tactical_band"
    if leg_role == LegRole.RIDER and not (rules.rider_min_abs_delta <= abs_delta <= rules.rider_max_abs_delta):
        return "delta_outside_rider_band"
    return None


def _entry_fill(contract: SelectedContract, rules: ExecutionSimRules) -> tuple[float, str]:
    if rules.fill_model == "optimistic_mid":
        return round(contract.mid, 4), "mid"
    if rules.fill_model == "conservative":
        return round(contract.ask, 4), "ask"
    if rules.fill_model == "stress":
        return round(contract.ask, 4), "ask"
    penalty = rules.entry_slippage_pct
    return round(contract.mid + ((contract.ask - contract.bid) * penalty), 4), "mid_plus_slippage"
