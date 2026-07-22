from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from .vix_evidence import resolve_vix_strategy_config, vix_parameter_candidates
from .vix_trader import ACTIVE_EXPOSURE_STATES, load_vix_cycles, load_vix_strategy_config


CLOSED_STATES = frozenset({"CLOSED", "RECONCILED"})
MIRROR_ACTIONABLE_STATES = frozenset(
    {
        "PREFLIGHT_VALIDATED",
        "ENTRY_READY",
        "ENTRY_SUBMITTED",
        "ENTRY_PARTIALLY_FILLED",
        "ACTIVE",
        "FIRST_LEG_EXIT_WORKING",
        "FIRST_LEG_EXITED",
        "REBALANCE_ELIGIBLE",
        "REBALANCE_SUBMITTED",
        "REBALANCED",
        "EXIT_REQUIRED",
        "CLOSING",
    }
)


def paper_vix_operating_config() -> dict[str, Any]:
    """Config used for VIX paper/Robinhood-mirror flow.

    Evidence-selected params win when available. Otherwise the first predeclared
    candidate is used for paper recommendations only — never claimed as proven live edge.
    """

    ceilings = load_vix_strategy_config()
    resolution = resolve_vix_strategy_config(ceilings=ceilings)
    if resolution.config is not None:
        return {
            "config": resolution.config.to_json_dict(),
            "source": resolution.source,
            "fingerprint": resolution.fingerprint,
            "profitability_status": resolution.profitability_status,
            "paper_only": True,
            "robinhood_mirror": True,
            "proven": True,
        }
    candidate = vix_parameter_candidates()[0]
    return {
        "config": candidate.to_json_dict(),
        "source": "paper_candidate",
        "fingerprint": None,
        "profitability_status": "paper_candidate_unproven",
        "paper_only": True,
        "robinhood_mirror": True,
        "proven": False,
        "note": "Using paper candidate until closed paper cycles promote an evidence-selected set.",
    }


def _leg_card(
    *,
    action: str,
    product: str,
    option_type: str,
    strike: float | None,
    expiration: str | None,
    quantity: int | None,
    limit_debit: float | None,
    reason: str,
) -> dict[str, Any]:
    right = "Call" if str(option_type).lower() == "call" else "Put"
    return {
        "action": action,
        "broker": "robinhood",
        "product": product,
        "option_type": option_type,
        "right": right,
        "strike": strike,
        "expiration": expiration,
        "quantity": quantity,
        "limit_price": limit_debit,
        "order_type": "limit",
        "time_in_force": "day",
        "reason": reason,
        "robinhood_search_hint": f"{product} {expiration} {strike:g} {right}" if strike is not None and expiration else product,
        "copy_line": (
            f"{action} {quantity} {product} {right} {strike:g} exp {expiration}"
            + (f" @ {limit_debit:.2f} debit" if limit_debit is not None else "")
            if strike is not None and expiration and quantity is not None
            else action
        ),
    }


def _cycle_mirror_actions(row: dict[str, Any]) -> list[dict[str, Any]]:
    payload = row.get("strategy_payload") or {}
    product = str(payload.get("product") or "VIXW")
    expiration = payload.get("expiration")
    call_strike = payload.get("call_strike")
    put_strike = payload.get("put_strike")
    call_qty = payload.get("call_quantity")
    put_qty = payload.get("put_quantity")
    call_debit = payload.get("call_debit")
    put_debit = payload.get("put_debit")
    state = str(row.get("lifecycle_state") or "")
    first_leg = row.get("first_leg_sold")
    actions: list[dict[str, Any]] = []

    if state in {"PREFLIGHT_VALIDATED", "ENTRY_READY", "ENTRY_SUBMITTED", "ENTRY_PARTIALLY_FILLED"}:
        actions.append(
            _leg_card(
                action="BUY_TO_OPEN",
                product=product,
                option_type="call",
                strike=float(call_strike) if call_strike is not None else None,
                expiration=str(expiration) if expiration else None,
                quantity=int(call_qty) if call_qty is not None else None,
                limit_debit=float(call_debit) if call_debit is not None else None,
                reason="paper_paired_entry_call",
            )
        )
        actions.append(
            _leg_card(
                action="BUY_TO_OPEN",
                product=product,
                option_type="put",
                strike=float(put_strike) if put_strike is not None else None,
                expiration=str(expiration) if expiration else None,
                quantity=int(put_qty) if put_qty is not None else None,
                limit_debit=float(put_debit) if put_debit is not None else None,
                reason="paper_paired_entry_put",
            )
        )
        return actions

    if state in ACTIVE_EXPOSURE_STATES or state in MIRROR_ACTIONABLE_STATES:
        if state in {"ACTIVE", "FIRST_LEG_EXIT_WORKING"} and not first_leg:
            target = payload.get("first_leg_profit_target_pct")
            actions.append(
                {
                    "action": "MONITOR",
                    "broker": "robinhood",
                    "product": product,
                    "reason": "hold_both_legs_until_first_leg_target",
                    "first_leg_target_pct": target,
                    "copy_line": (
                        f"HOLD both {product} legs exp {expiration}. "
                        f"Sell first leg near {float(target) * 100:.0f}% gain when one side works."
                        if target is not None and expiration
                        else f"HOLD both {product} legs and sell the working side first."
                    ),
                }
            )
        if first_leg in {"call", "put"}:
            remaining = "put" if first_leg == "call" else "call"
            strike = put_strike if remaining == "put" else call_strike
            qty = put_qty if remaining == "put" else call_qty
            actions.append(
                _leg_card(
                    action="SELL_TO_CLOSE",
                    product=product,
                    option_type=str(first_leg),
                    strike=float(payload.get(f"{first_leg}_strike")) if payload.get(f"{first_leg}_strike") is not None else None,
                    expiration=str(expiration) if expiration else None,
                    quantity=int(payload.get(f"{first_leg}_quantity")) if payload.get(f"{first_leg}_quantity") is not None else None,
                    limit_debit=None,
                    reason="take_first_leg_profit_on_robinhood",
                )
            )
            actions.append(
                _leg_card(
                    action="HOLD",
                    product=product,
                    option_type=remaining,
                    strike=float(strike) if strike is not None else None,
                    expiration=str(expiration) if expiration else None,
                    quantity=int(qty) if qty is not None else None,
                    limit_debit=None,
                    reason="manage_remaining_leg_per_paper_rule",
                )
            )
        if state in {"EXIT_REQUIRED", "CLOSING"}:
            for leg, strike, qty in (
                ("call", call_strike, call_qty),
                ("put", put_strike, put_qty),
            ):
                if first_leg == leg:
                    continue
                actions.append(
                    _leg_card(
                        action="SELL_TO_CLOSE",
                        product=product,
                        option_type=leg,
                        strike=float(strike) if strike is not None else None,
                        expiration=str(expiration) if expiration else None,
                        quantity=int(qty) if qty is not None else None,
                        limit_debit=None,
                        reason="paper_exit_deadline_or_close",
                    )
                )
    return actions


def _open_position_summary(row: dict[str, Any]) -> dict[str, Any]:
    payload = row.get("strategy_payload") or {}
    return {
        "cycle_id": row.get("cycle_id"),
        "lifecycle_state": row.get("lifecycle_state"),
        "product": payload.get("product"),
        "spot_vix_at_decision": payload.get("spot_vix_at_decision"),
        "expiration": payload.get("expiration"),
        "call": {
            "strike": payload.get("call_strike"),
            "quantity": payload.get("call_quantity"),
            "entry_debit": payload.get("call_debit"),
            "status": row.get("call_status"),
        },
        "put": {
            "strike": payload.get("put_strike"),
            "quantity": payload.get("put_quantity"),
            "entry_debit": payload.get("put_debit"),
            "status": row.get("put_status"),
        },
        "combined_debit": payload.get("combined_debit"),
        "combined_cycle_pnl": row.get("combined_cycle_pnl"),
        "first_leg_sold": row.get("first_leg_sold"),
        "robinhood_actions": _cycle_mirror_actions(row),
        "paper_only": True,
    }


def build_robinhood_mirror_report(*, limit: int = 100) -> dict[str, Any]:
    """Operator report: paper VIX cycles translated into Robinhood copy actions."""

    cycles = load_vix_cycles(limit=max(limit, 1))
    operating = paper_vix_operating_config()
    open_rows = [row for row in cycles if str(row.get("lifecycle_state") or "") in MIRROR_ACTIONABLE_STATES | ACTIVE_EXPOSURE_STATES]
    closed_rows = [row for row in cycles if str(row.get("lifecycle_state") or "") in CLOSED_STATES]
    open_summaries = [_open_position_summary(row) for row in reversed(open_rows)]
    flat_actions = [action for summary in open_summaries for action in summary.get("robinhood_actions") or []]
    closed_pnls = [float(row.get("combined_cycle_pnl") or 0.0) for row in closed_rows]
    wins = sum(1 for value in closed_pnls if value > 0)
    losses = sum(1 for value in closed_pnls if value < 0)
    net = round(sum(closed_pnls), 2)
    return {
        "ok": True,
        "product": "vix_paper_robinhood_mirror",
        "mode": "paper_trading_with_robinhood_reporting",
        "real_money_venue": "robinhood_manual",
        "autobott_submits_live_orders": False,
        "alpaca_paper_isolated": True,
        "generated_at": datetime.now(UTC).isoformat(),
        "operating_config": operating,
        "open_count": len(open_summaries),
        "closed_count": len(closed_rows),
        "open_positions": open_summaries,
        "robinhood_action_queue": flat_actions,
        "performance_report": {
            "closed_cycles": len(closed_rows),
            "wins": wins,
            "losses": losses,
            "win_rate": round(wins / len(closed_rows), 4) if closed_rows else 0.0,
            "net_paper_pnl": net,
            "avg_paper_pnl": round(net / len(closed_rows), 4) if closed_rows else 0.0,
            "claim": "paper_ledger_only_not_robinhood_fills",
        },
        "how_to_use": [
            "AutoBott papers the VIX paired trade and keeps the ledger.",
            "Read robinhood_action_queue top-down and place the same orders on Robinhood.",
            "When paper says SELL_TO_CLOSE or HOLD, do that on Robinhood for the matching contract.",
            "Closed-cycle report shows what worked in paper so future entries can follow winners.",
        ],
    }
