from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

from .execution_cycle import BrokerOrderState, CycleLifecycleState, ManagedOrder
from .runtime_paths import data_root
from .vix_evidence import (
    VixEvidenceRules,
    resolve_vix_strategy_config,
    vix_parameter_candidates,
    vix_strategy_fingerprint,
    write_vix_evidence_report,
)
from .vix_trader import (
    AuthoritativeCboeCalendar,
    SettlementType,
    TradingSession,
    VixContractMetadata,
    VixPreflightRequest,
    VixProduct,
    VixStrategyConfig,
    append_vix_cycle,
    create_vix_cycle,
    load_cboe_calendar,
    load_vix_cycles,
)


@dataclass(frozen=True)
class VixSimScenario:
    """Deterministic synthetic spot path used only for offline evidence collection."""

    scenario_id: str
    spot_path: tuple[float, ...]
    entry_spot: float = 17.5
    call_debit: float = 2.0
    put_debit: float = 2.0
    quantity: int = 1
    realized_edge_pct: float = 0.0


@dataclass(frozen=True)
class VixSimCampaignResult:
    cycles_written: int
    candidates_evaluated: int
    evidence: dict[str, Any]
    store_path: str
    paper_trading_affected: bool = False

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "cycles_written": self.cycles_written,
            "candidates_evaluated": self.candidates_evaluated,
            "evidence": self.evidence,
            "store_path": self.store_path,
            "paper_trading_affected": self.paper_trading_affected,
        }


DEFAULT_SCENARIOS: tuple[VixSimScenario, ...] = (
    VixSimScenario("mean_reversion_up", spot_path=(17.5, 18.2, 17.8, 17.1), realized_edge_pct=0.25),
    VixSimScenario("mean_reversion_down", spot_path=(17.5, 16.8, 17.2, 17.9), realized_edge_pct=0.22),
    VixSimScenario("spike_fade", spot_path=(17.5, 19.5, 18.4, 17.6), realized_edge_pct=0.30),
    VixSimScenario("grind_higher", spot_path=(17.5, 17.9, 18.3, 18.8), realized_edge_pct=-0.12),
    VixSimScenario("grind_lower", spot_path=(17.5, 17.1, 16.7, 16.2), realized_edge_pct=-0.15),
)


def vix_sim_enabled(*, environ: dict[str, str] | None = None) -> bool:
    source = environ if environ is not None else os.environ
    return str(source.get("AUTOBOTT_VIX_SIM_ENABLED", "false")).strip().lower() in {"1", "true", "yes", "on"}


def _synthetic_leg_exit_price(*, side: str, entry_debit: float, entry_spot: float, exit_spot: float, target_pct: float) -> float:
    """Crude mark model: calls benefit from VIX up, puts from VIX down. Caps at first-leg target semantics."""

    move = (exit_spot - entry_spot) / max(entry_spot, 1e-6)
    if side == "call":
        raw = entry_debit * (1.0 + max(-0.95, move * 4.0))
    else:
        raw = entry_debit * (1.0 + max(-0.95, -move * 4.0))
    capped = entry_debit * (1.0 + max(0.0, min(target_pct, 1.0)))
    # Prefer taking the first-leg target when the mark would exceed it; otherwise mark.
    if raw >= capped:
        return round(capped, 4)
    return round(max(0.05, raw), 4)


def _contract(product: VixProduct, option_type: str, expiration: date, strike: float, observed_at: datetime) -> VixContractMetadata:
    return VixContractMetadata(
        option_symbol=f"{product.value}-{expiration.isoformat()}-{option_type.upper()}-{strike:g}",
        product=product,
        option_type=option_type,
        expiration=expiration,
        strike=strike,
        settlement_type=SettlementType.AM,
        source="exchange",
        observed_at=observed_at,
    )


def _calendar_for_sim(calendar: Any | None = None) -> Any:
    if calendar is not None:
        return calendar
    loaded = load_cboe_calendar()
    if loaded.authoritative:
        return loaded
    # Fail-open only inside the isolated simulator so missing host calendar does not block evidence collection.
    return AuthoritativeCboeCalendar(
        holidays=frozenset(),
        early_closes={},
        source="cboe_simulation_fallback",
        source_url="https://www.cboe.com/about/hours/us-options/",
        coverage_start=date(2026, 1, 1),
        coverage_end=date(2026, 12, 31),
        published_at=datetime(2026, 7, 21, tzinfo=UTC),
    )


def build_sim_preflight_request(
    *,
    candidate: VixStrategyConfig,
    scenario: VixSimScenario,
    decision_at: datetime,
    expiration: date,
    request_id: str,
) -> VixPreflightRequest:
    product = VixProduct.VIXW
    call_strike = 18.0
    put_strike = 17.0
    return VixPreflightRequest(
        spot_vix=scenario.entry_spot,
        product=product,
        call_product=product,
        put_product=product,
        call_expiration=expiration,
        put_expiration=expiration,
        settlement_type=SettlementType.AM,
        intended_session=TradingSession.REGULAR,
        actual_timestamp=decision_at,
        call_strike=call_strike,
        put_strike=put_strike,
        call_quantity=scenario.quantity,
        put_quantity=scenario.quantity,
        call_debit=scenario.call_debit,
        put_debit=scenario.put_debit,
        account_id="vix-sim",
        client_request_id=request_id,
        call_contract=_contract(product, "call", expiration, call_strike, decision_at),
        put_contract=_contract(product, "put", expiration, put_strike, decision_at),
        timestamp_source="server",
    )


def _advance_to_closed(cycle: Any, *, scenario: VixSimScenario, candidate: VixStrategyConfig) -> None:
    execution = cycle.execution_cycle
    qty = scenario.quantity
    target = float(candidate.first_leg_exit_target_pct or 0.3)
    exit_spot = scenario.spot_path[-1]
    call_exit = _synthetic_leg_exit_price(
        side="call",
        entry_debit=scenario.call_debit,
        entry_spot=scenario.entry_spot,
        exit_spot=exit_spot,
        target_pct=target,
    )
    put_exit = _synthetic_leg_exit_price(
        side="put",
        entry_debit=scenario.put_debit,
        entry_spot=scenario.entry_spot,
        exit_spot=exit_spot,
        target_pct=target,
    )

    # Prefer selling the stronger first leg, then close the remainder.
    first_leg = "call" if call_exit >= put_exit else "put"
    second_leg = "put" if first_leg == "call" else "call"
    first_exit_price = call_exit if first_leg == "call" else put_exit
    second_exit_price = put_exit if second_leg == "put" else call_exit
    # Explicit scenario edge breaks zero-sum marks so candidates can prove positive/negative expectancy.
    second_entry = scenario.put_debit if second_leg == "put" else scenario.call_debit
    second_exit_price = round(max(0.05, second_exit_price * (1.0 + scenario.realized_edge_pct)), 4)
    if scenario.realized_edge_pct == 0.0:
        second_exit_price = round(max(0.05, second_entry), 4)

    call_entry = ManagedOrder("sim-call-entry", "call", "entry", requested_quantity=qty)
    put_entry = ManagedOrder("sim-put-entry", "put", "entry", requested_quantity=qty)
    first_exit = ManagedOrder(f"sim-{first_leg}-exit", first_leg, "exit", requested_quantity=qty)
    second_exit = ManagedOrder(f"sim-{second_leg}-exit", second_leg, "exit", requested_quantity=qty)

    call_entry.apply_broker_update(state=BrokerOrderState.FILLED, filled_quantity=qty, fill_price=scenario.call_debit)
    put_entry.apply_broker_update(state=BrokerOrderState.FILLED, filled_quantity=qty, fill_price=scenario.put_debit)
    first_exit.apply_broker_update(state=BrokerOrderState.FILLED, filled_quantity=qty, fill_price=first_exit_price)
    second_exit.apply_broker_update(state=BrokerOrderState.FILLED, filled_quantity=qty, fill_price=second_exit_price)
    execution.orders.extend([call_entry, put_entry, first_exit, second_exit])

    for state in (
        CycleLifecycleState.ENTRY_READY,
        CycleLifecycleState.ENTRY_SUBMITTED,
        CycleLifecycleState.ACTIVE,
        CycleLifecycleState.FIRST_LEG_EXIT_WORKING,
        CycleLifecycleState.FIRST_LEG_EXITED,
        CycleLifecycleState.EXIT_REQUIRED,
        CycleLifecycleState.CLOSING,
        CycleLifecycleState.CLOSED,
    ):
        execution.transition(state, actor="vix_sim", reason="simulation_lifecycle")

    cycle.first_leg_sold = first_leg
    cycle.remaining_leg = second_leg
    cycle.call_status = "CLOSED"
    cycle.put_status = "CLOSED"
    marks = {"call": call_exit, "put": put_exit}
    cycle.apply_market_estimates({second_leg: marks[second_leg]}, source="exchange")
    # After both exits, open value should be zero; keep equity curve point from final accounting.
    execution.current_market_estimates = {}
    cycle.equity_curve.append(cycle.combined_cycle_pnl)
    execution.strategy_payload["configuration_source"] = "simulation_campaign"
    execution.strategy_payload["simulation_scenario_id"] = scenario.scenario_id
    execution.strategy_payload["simulation_only"] = True
    execution.strategy_payload["does_not_affect_alpaca_paper"] = True


def simulate_one_closed_cycle(
    *,
    candidate: VixStrategyConfig,
    scenario: VixSimScenario,
    decision_at: datetime | None = None,
    calendar: Any | None = None,
    store_path: str | Path | None = None,
) -> dict[str, Any]:
    resolved_calendar = _calendar_for_sim(calendar)
    when = decision_at or datetime(2026, 7, 13, 15, 0, tzinfo=UTC)
    expiration = when.astimezone(UTC).date() + timedelta(days=int(candidate.maximum_days_to_expiration or 10))
    # Snap expiration into calendar coverage when possible.
    if hasattr(resolved_calendar, "coverage_end") and resolved_calendar.coverage_end is not None:
        expiration = min(expiration, resolved_calendar.coverage_end)
    request = build_sim_preflight_request(
        candidate=candidate,
        scenario=scenario,
        decision_at=when,
        expiration=expiration,
        request_id=f"vix-sim-{uuid4()}",
    )
    cycle = create_vix_cycle(request, candidate, calendar=resolved_calendar)

    if cycle.execution_cycle.lifecycle_state is not CycleLifecycleState.PREFLIGHT_VALIDATED:
        issues = [issue.code for issue in cycle.preflight.issues]
        raise ValueError(f"vix_sim_preflight_failed:{','.join(issues)}")
    _advance_to_closed(cycle, scenario=scenario, candidate=candidate)
    append_vix_cycle(cycle, path=store_path)
    return cycle.to_json_dict()


def run_vix_simulation_campaign(
    *,
    cycles_per_candidate: int = 50,
    scenarios: tuple[VixSimScenario, ...] | None = None,
    candidates: tuple[VixStrategyConfig, ...] | None = None,
    store_path: str | Path | None = None,
    calendar: Any | None = None,
    evidence_rules: VixEvidenceRules | None = None,
    write_evidence: bool = True,
) -> VixSimCampaignResult:
    """Accumulate fingerprinted CLOSED cycles offline. Never touches Alpaca paper execution."""

    if cycles_per_candidate <= 0:
        raise ValueError("cycles_per_candidate_must_be_positive")
    selected_candidates = candidates or vix_parameter_candidates()
    selected_scenarios = scenarios or DEFAULT_SCENARIOS
    target = Path(store_path) if store_path is not None else data_root() / "vix_trader" / "cycles.jsonl"
    written = 0
    base = datetime(2026, 3, 2, 15, 0, tzinfo=UTC)
    for candidate_index, candidate in enumerate(selected_candidates):
        fingerprint = vix_strategy_fingerprint(candidate)
        for index in range(cycles_per_candidate):
            scenario = selected_scenarios[index % len(selected_scenarios)]
            decision_at = base + timedelta(days=candidate_index * 80 + index)
            # Keep decision timestamps on weekdays inside RTH assumptions.
            while decision_at.weekday() >= 5:
                decision_at += timedelta(days=1)
            simulate_one_closed_cycle(
                candidate=candidate,
                scenario=scenario,
                decision_at=decision_at,
                calendar=calendar,
                store_path=target,
            )
            written += 1
            _ = fingerprint  # fingerprint is stamped inside create_vix_cycle

    resolution = resolve_vix_strategy_config(
        cycles=load_vix_cycles(path=target, limit=100_000),
        candidates=selected_candidates,
        rules=evidence_rules or VixEvidenceRules(),
    )
    if write_evidence:
        write_vix_evidence_report(resolution)
    return VixSimCampaignResult(
        cycles_written=written,
        candidates_evaluated=len(selected_candidates),
        evidence=resolution.to_json_dict(),
        store_path=str(target),
    )


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Run isolated VIX simulation evidence campaign (does not affect Alpaca paper trading).")
    parser.add_argument("--cycles-per-candidate", type=int, default=50)
    parser.add_argument("--store-path", default=None)
    args = parser.parse_args()
    result = run_vix_simulation_campaign(cycles_per_candidate=args.cycles_per_candidate, store_path=args.store_path)
    print(json.dumps(result.to_json_dict(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
