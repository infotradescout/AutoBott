from __future__ import annotations

import json
import os
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from .execution_journal import load_execution_journal
from .jsonl_retention import compact_jsonl_tail
from .hosted_policy import (
    HOSTED_LOSS_GUARD_CONSECUTIVE_LOSSES,
    HOSTED_LOSS_GUARD_LOOKBACK,
    HOSTED_LOSS_GUARD_LOSS_RATE,
    HOSTED_LOSS_GUARD_MIN_SAMPLE,
    HOSTED_POLICY_VERSION,
    HOSTED_VOLATILITY_SYMBOLS,
    HOSTED_WINNER_BIAS_CONSECUTIVE_WINS,
    HOSTED_WINNER_BIAS_LOOKBACK,
    HOSTED_WINNER_BIAS_MIN_SAMPLE,
    HOSTED_WINNER_BIAS_WIN_RATE,
    is_hosted_paper_runtime,
)
from .runtime_paths import data_root


_TERMINAL_FILL_STATUSES = {
    "filled",
    "canceled",
    "cancelled",
    "expired",
    "replaced",
    "rejected",
    "done_for_day",
}


def _order_has_realized_fill(order: dict[str, Any]) -> bool:
    """Return whether the broker row represents cash that has actually traded.

    Alpaca reports ``filled_qty`` cumulatively while an order is still
    ``partially_filled``.  Those fills already affect the account's intraday
    realized P/L and therefore the daily-loss guard, even though we deliberately
    wait for a terminal order state before appending a permanent outcome row.
    """

    return float(order.get("filled_qty") or 0.0) > 0 and order.get("filled_avg_price") is not None


def trade_outcome_journal_path() -> Path:
    return data_root() / "execution" / "trade_outcomes.jsonl"


def sync_trade_outcomes_from_broker(
    broker: Any,
    *,
    journal_path: str | Path | None = None,
    execution_journal_path: str | Path | None = None,
    execution_journal_rows: list[dict[str, Any]] | None = None,
    limit: int = 200,
    trading_day: date | datetime | str | None = None,
) -> dict[str, Any]:
    if not hasattr(broker, "list_orders"):
        return {"ok": True, "recorded": 0, "outcomes": [], "blocked_underlyings": []}
    try:
        if hasattr(broker, "list_order_history"):
            orders = broker.list_order_history(status="all")
            history_complete = True
        else:
            orders = broker.list_orders(status="all", limit=limit, direction="desc")
            history_complete = len(orders) < limit
    except Exception as exc:
        return {"ok": False, "recorded": 0, "error": str(exc), "outcomes": [], "blocked_underlyings": []}
    summary = record_trade_outcomes_from_orders(
        orders,
        journal_path=journal_path,
        execution_journal_path=execution_journal_path,
        execution_journal_rows=execution_journal_rows,
        trading_day=trading_day,
    )
    resolved_trading_day = _resolve_trading_day(trading_day)
    unmatched_sell_events = _unmatched_realized_sell_events(orders)
    current_day_unmatched = [
        event
        for event in unmatched_sell_events
        if event["event_time"] is None
        or event["event_time"].astimezone(ZoneInfo("America/New_York")).date() == resolved_trading_day
    ]
    historical_unmatched = [event for event in unmatched_sell_events if event not in current_day_unmatched]
    current_day_symbols = sorted({str(event["symbol"]) for event in current_day_unmatched})
    historical_symbols = sorted({str(event["symbol"]) for event in historical_unmatched})

    # A truncated broker result can hide any fill, including a loss today, so
    # it remains fatal. Historical unmatched exits make long-range statistics
    # incomplete but cannot make today's realized P/L incomplete. Only an
    # unmatched sell from the current New York trading day blocks new entries.
    if not history_complete:
        summary["ok"] = False
        summary["error"] = "broker_order_history_truncated"
        summary["history_complete"] = False
        summary["daily_pnl_complete"] = False
    elif current_day_symbols:
        summary["ok"] = False
        summary["error"] = f"broker_order_history_unmatched_sells:{','.join(current_day_symbols)}"
        summary["history_complete"] = False
        summary["daily_pnl_complete"] = False
    else:
        summary["history_complete"] = not historical_symbols
        summary["daily_pnl_complete"] = True
    if historical_symbols:
        summary["history_warning"] = (
            f"broker_order_history_historical_unmatched_sells:{','.join(historical_symbols)}"
        )
        summary["historical_unmatched_sell_symbols"] = historical_symbols
    if current_day_symbols:
        summary["current_day_unmatched_sell_symbols"] = current_day_symbols
    return summary


def record_trade_outcomes_from_orders(
    orders: list[dict[str, Any]],
    *,
    journal_path: str | Path | None = None,
    execution_journal_path: str | Path | None = None,
    execution_journal_rows: list[dict[str, Any]] | None = None,
    trading_day: date | datetime | str | None = None,
) -> dict[str, Any]:
    existing = {str(row.get("outcome_id")) for row in load_trade_outcomes(journal_path=journal_path)}
    resolved_execution_rows = _resolve_execution_journal_rows(
        execution_journal_rows,
        execution_journal_path=execution_journal_path,
        outcome_journal_path=journal_path,
    )
    outcomes = build_trade_outcomes_from_orders(orders, execution_journal_rows=resolved_execution_rows)
    live_fill_outcomes = build_trade_outcomes_from_orders(
        orders,
        execution_journal_rows=resolved_execution_rows,
        include_nonterminal_fills=True,
    )
    new_rows = [row for row in outcomes if row["outcome_id"] not in existing]
    path = Path(journal_path) if journal_path is not None else trade_outcome_journal_path()
    if new_rows:
        _append_trade_outcomes(path, new_rows)
    all_rows = load_trade_outcomes(journal_path=journal_path)
    summary_policy_version = HOSTED_POLICY_VERSION if is_hosted_paper_runtime() else None
    current_policy_rows = _filter_rows_for_policy(all_rows, summary_policy_version)
    completed_groups = build_completed_trade_groups(current_policy_rows)
    provisional_outcomes = [row for row in live_fill_outcomes if row.get("provisional_fill")]
    return {
        "ok": True,
        "recorded": len(new_rows),
        "outcomes": new_rows,
        "summary_policy_version": summary_policy_version,
        "summary": summarize_trade_outcomes(current_policy_rows),
        "completed_groups": completed_groups,
        "group_summary": summarize_completed_trade_groups(current_policy_rows),
        # Daily loss protection is account-wide. A legacy-policy loss still
        # reduced today's paper account equity and must not disappear merely
        # because the strategy version changed.
        # Permanent rows deliberately wait for a terminal broker state so a
        # cumulative partial fill cannot be appended and counted twice when it
        # later completes.  Intraday protection still includes the currently
        # realized, nonterminal quantity from the fresh broker snapshot.
        "daily_realized_pnl": daily_realized_pnl(
            [*all_rows, *provisional_outcomes],
            trading_day=trading_day,
        ),
        "blocked_underlyings": recent_loss_guard(
            all_rows,
            policy_version=summary_policy_version,
        )["blocked_underlyings"],
    }


def build_trade_outcomes_from_orders(
    orders: list[dict[str, Any]],
    *,
    execution_journal_rows: list[dict[str, Any]] | None = None,
    include_nonterminal_fills: bool = False,
) -> list[dict[str, Any]]:
    journal_index = _execution_journal_index(execution_journal_rows or [])
    normalized = sorted(
        (_enrich_order_from_execution_journal(_normalize_order(order), journal_index) for order in orders),
        key=lambda row: row["event_time"] or datetime.min.replace(tzinfo=UTC),
    )
    open_buys: dict[str, list[dict[str, Any]]] = {}
    outcomes: list[dict[str, Any]] = []
    for order in normalized:
        # Alpaca reports filled_qty cumulatively. Recording a partially-filled
        # order and then its final filled state would double-count the first
        # fill in this append-only ledger. Wait for the terminal fill.
        if order["status"] not in _TERMINAL_FILL_STATUSES and not (
            include_nonterminal_fills and _order_has_realized_fill(order)
        ):
            continue
        if order["filled_qty"] <= 0 or order["filled_avg_price"] is None:
            continue
        if order["side"] == "buy":
            open_buys.setdefault(order["symbol"], []).append({"order": order, "remaining_qty": order["filled_qty"]})
            continue
        if order["side"] != "sell":
            continue
        remaining_sell_qty = order["filled_qty"]
        queue = open_buys.get(order["symbol"], [])
        while remaining_sell_qty > 0 and queue:
            lot = queue[0]
            matched_qty = min(float(lot["remaining_qty"]), remaining_sell_qty)
            outcome = _outcome_from_pair(lot["order"], order, qty=matched_qty)
            if order["status"] not in _TERMINAL_FILL_STATUSES:
                outcome["provisional_fill"] = True
            outcomes.append(outcome)
            lot["remaining_qty"] = round(float(lot["remaining_qty"]) - matched_qty, 10)
            remaining_sell_qty = round(remaining_sell_qty - matched_qty, 10)
            if lot["remaining_qty"] <= 0:
                queue.pop(0)
    return outcomes


def _unmatched_realized_sell_events(orders: list[dict[str, Any]]) -> list[dict[str, Any]]:
    inventory: dict[str, float] = {}
    unmatched: list[dict[str, Any]] = []
    normalized = sorted(
        (_normalize_order(order) for order in orders),
        key=lambda row: row["event_time"] or datetime.min.replace(tzinfo=UTC),
    )
    for order in normalized:
        if not _order_has_realized_fill(order):
            continue
        quantity = float(order["filled_qty"] or 0.0)
        if quantity <= 0:
            continue
        symbol = str(order["symbol"] or "").upper()
        if order["side"] == "buy":
            inventory[symbol] = inventory.get(symbol, 0.0) + quantity
        elif order["side"] == "sell":
            available = inventory.get(symbol, 0.0)
            if quantity > available + 1e-9:
                unmatched.append(
                    {
                        "symbol": symbol,
                        "event_time": order["event_time"],
                        "unmatched_qty": round(quantity - available, 10),
                    }
                )
                inventory[symbol] = 0.0
            else:
                inventory[symbol] = available - quantity
    return unmatched


def load_trade_outcomes(*, journal_path: str | Path | None = None, limit: int | None = None) -> list[dict[str, Any]]:
    path = Path(journal_path) if journal_path is not None else trade_outcome_journal_path()
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                try:
                    decoded = json.loads(line)
                except (json.JSONDecodeError, TypeError):
                    # A process can be interrupted between writing the JSON
                    # body and its newline. Preserve every complete outcome
                    # around that malformed/truncated record.
                    continue
                if isinstance(decoded, dict):
                    rows.append(decoded)
    except (OSError, UnicodeError):
        return rows
    return rows[-limit:] if limit is not None else rows


def _append_trade_outcomes(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    needs_separator = False
    if path.exists() and path.stat().st_size:
        with path.open("rb") as existing:
            existing.seek(-1, 2)
            needs_separator = existing.read(1) != b"\n"
    with path.open("a", encoding="utf-8") as handle:
        if needs_separator:
            handle.write("\n")
        for row in rows:
            handle.write(f"{json.dumps(row, sort_keys=True)}\n")
    compact_jsonl_tail(path)


def summarize_trade_outcomes(rows: list[dict[str, Any]]) -> dict[str, Any]:
    completed_groups = build_completed_trade_groups(rows)
    independent = _independent_trade_outcomes(rows, completed_groups=completed_groups)
    pnls = [float(row.get("pnl") or 0.0) for row in rows]
    wins = [pnl for pnl in pnls if pnl > 0]
    losses = [pnl for pnl in pnls if pnl < 0]
    net = round(sum(pnls), 2)
    independent_pnls = [float(row.get("pnl") or 0.0) for row in independent]
    independent_net = round(sum(independent_pnls), 2)
    grouped_leg_count = sum(1 for row in rows if row.get("trade_group_id"))
    completed_group_leg_count = sum(int(group.get("closed_legs") or 0) for group in completed_groups)
    return {
        # Preserve the original leg-level headline for dashboard/API callers.
        # The independent_* fields and group_summary carry pair-level metrics.
        "closed_trades": len(rows),
        "closed_legs": len(rows),
        "completed_groups": len(completed_groups),
        "standalone_trades": sum(1 for row in rows if not row.get("trade_group_id")),
        "incomplete_group_legs": max(0, grouped_leg_count - completed_group_leg_count),
        "independent_trades": len(independent),
        "independent_net_pnl": independent_net,
        "independent_expectancy": round(independent_net / len(independent), 2) if independent else 0.0,
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": round(len(wins) / len(rows), 4) if rows else 0.0,
        "net_pnl": net,
        "expectancy": round(net / len(rows), 2) if rows else 0.0,
        "profit_factor": round(sum(wins) / abs(sum(losses)), 4) if losses else (float("inf") if wins else 0.0),
    }


def build_completed_trade_groups(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Aggregate only fully closed primary+runner pairs into independent outcomes."""

    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        group_id = str(row.get("trade_group_id") or "").strip()
        if group_id:
            grouped.setdefault(group_id, []).append(row)

    completed: list[dict[str, Any]] = []
    for group_id, legs in grouped.items():
        primary = [row for row in legs if _canonical_leg_role(row.get("leg_role")) == "primary"]
        runner = [row for row in legs if _canonical_leg_role(row.get("leg_role")) == "runner"]
        if not primary or not runner or not _all_entry_lots_closed(primary) or not _all_entry_lots_closed(runner):
            continue

        ordered_legs = sorted(legs, key=_outcome_sort_key)
        pnl = round(sum(float(row.get("pnl") or 0.0) for row in ordered_legs), 2)
        entry_debit = round(
            sum(float(row.get("entry_price") or 0.0) * float(row.get("qty") or 0.0) * 100.0 for row in ordered_legs),
            2,
        )
        return_pct = pnl / entry_debit if entry_debit else 0.0
        primary_pnl = round(sum(float(row.get("pnl") or 0.0) for row in primary), 2)
        runner_pnl = round(sum(float(row.get("pnl") or 0.0) for row in runner), 2)
        primary_entry_debit = round(
            sum(float(row.get("entry_price") or 0.0) * float(row.get("qty") or 0.0) * 100.0 for row in primary),
            2,
        )
        runner_entry_debit = round(
            sum(float(row.get("entry_price") or 0.0) * float(row.get("qty") or 0.0) * 100.0 for row in runner),
            2,
        )
        exit_times = [row.get("exit_time") for row in ordered_legs if row.get("exit_time")]
        entry_times = [row.get("entry_time") for row in ordered_legs if row.get("entry_time")]
        payload = {
            "schema_version": "trade_group_outcome.v1",
            "trade_group_id": group_id,
            "underlying": _consistent_value(ordered_legs, "underlying"),
            "decision_id": _consistent_value(ordered_legs, "decision_id"),
            "thesis_id": _consistent_value(ordered_legs, "thesis_id"),
            "trade_setup": _consistent_value(ordered_legs, "trade_setup"),
            "execution_layer": _consistent_value(ordered_legs, "execution_layer"),
            "strategy_version": _consistent_value(ordered_legs, "strategy_version"),
            "policy_version": _consistent_value(ordered_legs, "policy_version"),
            "build_sha": _consistent_value(ordered_legs, "build_sha"),
            "entry_time": min(entry_times, key=_datetime_sort_value) if entry_times else None,
            "exit_time": max(exit_times, key=_datetime_sort_value) if exit_times else None,
            "closed_legs": len(ordered_legs),
            "entry_debit": entry_debit,
            "pnl": pnl,
            "return_pct": round(return_pct, 4),
            "result": "winner" if pnl > 0 else "loser" if pnl < 0 else "flat",
            "primary_pnl": primary_pnl,
            "runner_pnl": runner_pnl,
            "primary_entry_debit": primary_entry_debit,
            "runner_entry_debit": runner_entry_debit,
            "runner_funded": runner_entry_debit > 0 and primary_pnl >= runner_entry_debit,
            "leg_outcome_ids": [str(row.get("outcome_id") or "") for row in ordered_legs],
        }
        payload["outcome_id"] = _group_outcome_id(payload)
        completed.append(payload)
    return sorted(completed, key=_outcome_sort_key)


def summarize_completed_trade_groups(rows: list[dict[str, Any]]) -> dict[str, Any]:
    groups = build_completed_trade_groups(rows)
    pnls = [float(group.get("pnl") or 0.0) for group in groups]
    wins = [pnl for pnl in pnls if pnl > 0]
    losses = [pnl for pnl in pnls if pnl < 0]
    net = round(sum(pnls), 2)
    return {
        "completed_groups": len(groups),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": round(len(wins) / len(groups), 4) if groups else 0.0,
        "net_pnl": net,
        "expectancy": round(net / len(groups), 2) if groups else 0.0,
        "profit_factor": round(sum(wins) / abs(sum(losses)), 4) if losses else (float("inf") if wins else 0.0),
        "average_return_pct": round(
            sum(float(group.get("return_pct") or 0.0) for group in groups) / len(groups),
            4,
        )
        if groups
        else 0.0,
        "runner_contribution": round(sum(float(group.get("runner_pnl") or 0.0) for group in groups), 2),
        "runner_funded_groups": sum(1 for group in groups if group.get("runner_funded")),
    }


def daily_realized_pnl(
    rows: list[dict[str, Any]],
    *,
    trading_day: date | datetime | str | None = None,
    timezone_name: str = "America/New_York",
) -> float:
    """Return cash P/L realized by leg exits on one broker trading day."""

    timezone = ZoneInfo(timezone_name)
    if isinstance(trading_day, str):
        resolved_day = date.fromisoformat(trading_day)
    elif isinstance(trading_day, datetime):
        resolved_day = (
            trading_day.astimezone(timezone).date()
            if trading_day.tzinfo is not None
            else trading_day.date()
        )
    elif isinstance(trading_day, date):
        resolved_day = trading_day
    else:
        resolved_day = datetime.now(tz=timezone).date()

    realized = 0.0
    for row in rows:
        exit_time = _parse_datetime(row.get("exit_time"))
        if exit_time is not None and exit_time.astimezone(timezone).date() == resolved_day:
            realized += float(row.get("pnl") or 0.0)
    return round(realized, 2)


def recent_loss_guard(
    rows: list[dict[str, Any]] | None = None,
    *,
    journal_path: str | Path | None = None,
    policy_version: str | None = None,
) -> dict[str, Any]:
    if not _loss_guard_enabled():
        return {"enabled": False, "blocked_underlyings": [], "reasons": {}}
    source = rows if rows is not None else load_trade_outcomes(journal_path=journal_path)
    if policy_version is not None:
        source = _filter_rows_for_policy(source, policy_version)
    independent = _independent_trade_outcomes(source)
    recent = independent[-_loss_guard_lookback() :]
    by_underlying: dict[str, list[dict[str, Any]]] = {}
    for row in recent:
        learning_bucket = _learning_bucket(row)
        if learning_bucket:
            by_underlying.setdefault(learning_bucket, []).append(row)
    blocked: list[str] = []
    reasons: dict[str, dict[str, Any]] = {}
    for learning_bucket, outcomes in by_underlying.items():
        sample_ready = len(outcomes) >= _loss_guard_min_sample()
        # Preserve the legacy/local two-strike behavior for existing callers.
        # Hosted paper policy deliberately waits for its code-owned minimum
        # sample before treating a streak as evidence.
        streak_sample_ready = sample_ready or not is_hosted_paper_runtime()
        tail = outcomes[-_loss_guard_consecutive_losses() :]
        consecutive_losses = (
            streak_sample_ready
            and len(tail) >= _loss_guard_consecutive_losses()
            and all(float(row.get("pnl") or 0.0) < 0 for row in tail)
        )
        loss_rate = sum(1 for row in outcomes if float(row.get("pnl") or 0.0) < 0) / len(outcomes)
        loss_rate_block = sample_ready and loss_rate >= _loss_guard_loss_rate()
        if consecutive_losses or loss_rate_block:
            reason = {
                "learning_bucket": learning_bucket,
                "recent_trades": len(outcomes),
                "loss_rate": round(loss_rate, 4),
                "consecutive_losses": consecutive_losses,
                "loss_rate_block": loss_rate_block,
                "last_outcome_id": outcomes[-1].get("outcome_id"),
            }
            for underlying in _bucket_members(learning_bucket):
                blocked.append(underlying)
                reasons[underlying] = dict(reason)
    return {"enabled": True, "blocked_underlyings": sorted(blocked), "reasons": reasons}


def recent_winner_bias(
    rows: list[dict[str, Any]] | None = None,
    *,
    journal_path: str | Path | None = None,
    policy_version: str | None = None,
) -> dict[str, Any]:
    if not _winner_bias_enabled():
        return {"enabled": False, "preferred_underlyings": [], "reasons": {}}
    source = rows if rows is not None else load_trade_outcomes(journal_path=journal_path)
    if policy_version is not None:
        source = _filter_rows_for_policy(source, policy_version)
    independent = _independent_trade_outcomes(source)
    recent = independent[-_winner_bias_lookback() :]
    by_underlying: dict[str, list[dict[str, Any]]] = {}
    for row in recent:
        learning_bucket = _learning_bucket(row)
        if learning_bucket:
            by_underlying.setdefault(learning_bucket, []).append(row)
    preferred: list[str] = []
    reasons: dict[str, dict[str, Any]] = {}
    for learning_bucket, outcomes in by_underlying.items():
        wins = [row for row in outcomes if float(row.get("pnl") or 0.0) > 0]
        pnls = [float(row.get("pnl") or 0.0) for row in outcomes]
        win_rate = len(wins) / len(outcomes)
        net_pnl = round(sum(pnls), 2)
        tail = outcomes[-_winner_bias_consecutive_wins() :]
        consecutive_wins = len(tail) >= _winner_bias_consecutive_wins() and all(float(row.get("pnl") or 0.0) > 0 for row in tail)
        if len(outcomes) >= _winner_bias_min_sample() and net_pnl > 0 and (win_rate >= _winner_bias_win_rate() or consecutive_wins):
            reason = {
                "learning_bucket": learning_bucket,
                "recent_trades": len(outcomes),
                "wins": len(wins),
                "win_rate": round(win_rate, 4),
                "net_pnl": net_pnl,
                "consecutive_wins": consecutive_wins,
                "last_outcome_id": outcomes[-1].get("outcome_id"),
            }
            for underlying in _bucket_members(learning_bucket):
                preferred.append(underlying)
                reasons[underlying] = dict(reason)
    ranked = sorted(preferred, key=lambda symbol: (-float(reasons[symbol]["net_pnl"]), symbol))
    return {"enabled": True, "preferred_underlyings": ranked, "reasons": reasons}


def _outcome_from_pair(entry: dict[str, Any], exit_order: dict[str, Any], *, qty: float | None = None) -> dict[str, Any]:
    entry_price = float(entry["filled_avg_price"])
    exit_price = float(exit_order["filled_avg_price"])
    matched_qty = qty if qty is not None else min(float(entry["filled_qty"]), float(exit_order["filled_qty"]))
    pnl = round((exit_price - entry_price) * matched_qty * 100.0, 2)
    return_pct = ((exit_price - entry_price) / entry_price) if entry_price else 0.0
    symbol = str(entry["symbol"])
    parts = _option_symbol_parts(symbol)
    entry_metadata = entry.get("execution_metadata") or {}
    exit_metadata = exit_order.get("execution_metadata") or {}
    entry_policy_version = _string_or_none(entry_metadata.get("policy_version"))
    persisted_entry_policy_version = _string_or_none(exit_metadata.get("entry_policy_version"))
    policy_version = entry_policy_version or persisted_entry_policy_version
    policy_attribution_source = (
        "entry_execution_metadata"
        if entry_policy_version
        else ("persisted_entry_policy" if persisted_entry_policy_version else None)
    )
    leg_role = _canonical_leg_role(entry_metadata.get("leg_role") or exit_metadata.get("leg_role"))
    classification, reason = _classify_outcome(return_pct, pnl, leg_role=leg_role)
    payload = {
        "schema_version": "trade_outcome.v1",
        "recorded_at": datetime.now(tz=UTC).isoformat(),
        "symbol": symbol,
        **parts,
        "entry_time": entry.get("filled_at") or entry.get("submitted_at"),
        "exit_time": exit_order.get("filled_at") or exit_order.get("submitted_at"),
        "entry_price": entry_price,
        "exit_price": exit_price,
        "qty": matched_qty,
        "entry_order_filled_qty": float(entry["filled_qty"]),
        "pnl": pnl,
        "return_pct": round(return_pct, 4),
        "result": "winner" if pnl > 0 else "loser" if pnl < 0 else "flat",
        "classification": classification,
        "reason_inferred": reason,
        "why": _why_tags(parts, return_pct, pnl, leg_role=leg_role),
        "entry_broker_order_id": entry.get("broker_order_id"),
        "entry_client_order_id": entry.get("client_order_id"),
        "exit_broker_order_id": exit_order.get("broker_order_id"),
        "exit_client_order_id": exit_order.get("client_order_id"),
        "decision_id": entry_metadata.get("decision_id") or exit_metadata.get("entry_decision_id"),
        "thesis_id": entry_metadata.get("thesis_id") or exit_metadata.get("thesis_id"),
        "trade_group_id": entry_metadata.get("trade_group_id") or exit_metadata.get("trade_group_id"),
        "leg_role": leg_role or entry_metadata.get("leg_role") or exit_metadata.get("leg_role"),
        "trade_setup": entry_metadata.get("trade_setup") or exit_metadata.get("trade_setup"),
        "execution_layer": entry_metadata.get("execution_layer") or exit_metadata.get("execution_layer"),
        "confidence_score": entry_metadata.get("confidence_score") or exit_metadata.get("confidence_score"),
        "strategy_version": entry_metadata.get("strategy_version") or exit_metadata.get("strategy_version"),
        # Exit policy describes the rules that closed a position, not the
        # strategy that opened it. Attribute performance only to entry policy.
        "policy_version": policy_version,
        "policy_attribution_source": policy_attribution_source,
        "build_sha": entry_metadata.get("build_sha") or exit_metadata.get("entry_build_sha"),
        "exit_reason": exit_metadata.get("exit_reason"),
        "match_source": (
            "execution_journal"
            if entry_metadata
            else ("exit_journal_recovery" if exit_metadata else "symbol_fifo_legacy")
        ),
    }
    payload["outcome_id"] = _outcome_id(payload)
    return payload


def _classify_outcome(return_pct: float, pnl: float, *, leg_role: str | None = None) -> tuple[str, str]:
    profit_target_pct, stop_loss_pct = _role_exit_thresholds(leg_role)
    if return_pct >= 1.20:
        return "huge_winner", "large_option_return"
    if return_pct >= profit_target_pct:
        return "winner", "profit_target_region"
    if pnl > 0:
        return "small_winner", "profitable_exit"
    if return_pct <= -max(0.50, stop_loss_pct):
        return "large_loss", "deep_loss_beyond_stop_threshold"
    if return_pct <= -stop_loss_pct:
        return "loss_cut", "stop_loss_threshold_breached"
    if pnl < 0:
        return "small_loss", "loss_before_stop_threshold"
    return "flat", "flat_exit"


def _why_tags(
    parts: dict[str, Any],
    return_pct: float,
    pnl: float,
    *,
    leg_role: str | None = None,
) -> list[str]:
    profit_target_pct, stop_loss_pct = _role_exit_thresholds(leg_role)
    tags = [
        f"option_return_pct={round(return_pct, 4)}",
        f"leg_role={leg_role or 'standalone'}",
        f"profit_target_pct={profit_target_pct}",
        f"stop_loss_pct={stop_loss_pct}",
    ]
    if pnl < 0 and return_pct <= -stop_loss_pct:
        tags.append("loss_exceeded_configured_stop_zone")
    if pnl < 0 and abs(return_pct) >= max(0.50, stop_loss_pct):
        tags.append("severe_option_decay_or_wrong_direction")
    if pnl > 0 and return_pct >= profit_target_pct:
        tags.append("profit_move_captured")
    if parts.get("option_type"):
        tags.append(f"option_type={str(parts['option_type']).lower()}")
    return tags


def _role_exit_thresholds(leg_role: str | None) -> tuple[float, float]:
    if _canonical_leg_role(leg_role) == "runner":
        return 1.00, 0.70
    return 0.30, 0.22


def _normalize_order(order: dict[str, Any]) -> dict[str, Any]:
    submitted_at = _parse_datetime(order.get("submitted_at"))
    filled_at = _parse_datetime(order.get("filled_at"))
    event_time = filled_at or submitted_at
    return {
        "symbol": str(order.get("symbol") or "").upper(),
        "side": str(order.get("side") or "").lower(),
        "status": str(order.get("status") or "").lower(),
        "filled_qty": _float_or_zero(order.get("filled_qty")),
        "filled_avg_price": _float_or_none(order.get("filled_avg_price")),
        "submitted_at": submitted_at.isoformat() if submitted_at else None,
        "filled_at": filled_at.isoformat() if filled_at else None,
        "event_time": event_time,
        "broker_order_id": _string_or_none(order.get("id") or order.get("broker_order_id")),
        "client_order_id": _string_or_none(order.get("client_order_id")),
    }


def _resolve_execution_journal_rows(
    rows: list[dict[str, Any]] | None,
    *,
    execution_journal_path: str | Path | None,
    outcome_journal_path: str | Path | None,
) -> list[dict[str, Any]]:
    if rows is not None:
        return rows
    resolved_path = execution_journal_path
    if resolved_path is None and outcome_journal_path is not None:
        resolved_path = Path(outcome_journal_path).with_name("execution_orders.jsonl")
    try:
        return load_execution_journal(journal_path=resolved_path, max_tail_bytes=16 * 1024 * 1024)
    except (OSError, TypeError, ValueError):
        # Outcome recording must remain available for legacy/manual broker
        # orders even if an execution journal is absent or malformed.
        return []


def _execution_journal_index(rows: list[dict[str, Any]]) -> dict[str, dict[tuple[str, str], dict[str, Any]]]:
    index: dict[str, dict[tuple[str, str], dict[str, Any]]] = {
        "broker": {},
        "client": {},
    }
    for row in rows:
        if str(row.get("event_type") or "") != "order_submission":
            continue
        payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
        intent = payload.get("intent") if isinstance(payload.get("intent"), dict) else {}
        metadata = intent.get("metadata") if isinstance(intent.get("metadata"), dict) else {}
        option_symbol = str(intent.get("option_symbol") or payload.get("symbol") or "").upper()
        if not option_symbol:
            continue
        enriched = {
            "decision_id": row.get("decision_id") or intent.get("decision_id"),
            "thesis_id": row.get("thesis_id") or intent.get("thesis_id"),
            "trade_group_id": metadata.get("trade_group_id"),
            "leg_role": metadata.get("leg_role"),
            "trade_setup": metadata.get("trade_setup"),
            "execution_layer": metadata.get("execution_layer"),
            "confidence_score": metadata.get("confidence_score"),
            "strategy_version": metadata.get("strategy_version"),
            "policy_version": metadata.get("policy_version"),
            "entry_policy_version": metadata.get("entry_policy_version"),
            "exit_policy_version": metadata.get("exit_policy_version"),
            "entry_build_sha": metadata.get("entry_build_sha"),
            "build_sha": metadata.get("build_sha"),
            "exit_reason": metadata.get("exit_reason"),
            "entry_decision_id": metadata.get("entry_decision_id"),
        }
        broker_order_id = _string_or_none(payload.get("broker_order_id"))
        client_order_id = _string_or_none(payload.get("client_order_id"))
        if broker_order_id:
            index["broker"][(broker_order_id, option_symbol)] = enriched
        if client_order_id:
            index["client"][(client_order_id, option_symbol)] = enriched
    return index


def _enrich_order_from_execution_journal(
    order: dict[str, Any],
    index: dict[str, dict[tuple[str, str], dict[str, Any]]],
) -> dict[str, Any]:
    symbol = str(order.get("symbol") or "").upper()
    metadata = None
    broker_order_id = _string_or_none(order.get("broker_order_id"))
    client_order_id = _string_or_none(order.get("client_order_id"))
    if broker_order_id:
        metadata = index.get("broker", {}).get((broker_order_id, symbol))
    if metadata is None and client_order_id:
        metadata = index.get("client", {}).get((client_order_id, symbol))
    if metadata is None:
        return order
    return {**order, "execution_metadata": metadata}


def _independent_trade_outcomes(
    rows: list[dict[str, Any]],
    *,
    completed_groups: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    groups = completed_groups if completed_groups is not None else build_completed_trade_groups(rows)
    standalone = [row for row in rows if not row.get("trade_group_id")]
    return sorted([*standalone, *groups], key=_outcome_sort_key)


def _filter_rows_for_policy(
    rows: list[dict[str, Any]],
    policy_version: str | None,
) -> list[dict[str, Any]]:
    if policy_version is None:
        return rows
    return [
        row
        for row in rows
        if row.get("policy_version") == policy_version
        and _has_trustworthy_policy_attribution(row, policy_version=policy_version)
    ]


def _has_trustworthy_policy_attribution(
    row: dict[str, Any],
    *,
    policy_version: str,
) -> bool:
    # PR32's first rollout recovered legacy entries from their exit journal
    # rows and copied the then-current exit policy onto them. Those persisted
    # outcomes have no entry-policy provenance and must stay out of the new
    # policy's expectancy, guard, and winner cohorts. Rows made after this fix
    # explicitly identify persisted entry-policy provenance.
    attribution_source = row.get("policy_attribution_source")
    if policy_version == HOSTED_POLICY_VERSION:
        return attribution_source in {"entry_execution_metadata", "persisted_entry_policy"}
    if row.get("match_source") == "exit_journal_recovery":
        return attribution_source == "persisted_entry_policy"
    return True


def _learning_bucket(row: dict[str, Any]) -> str:
    underlying = str(row.get("underlying") or "").strip().upper()
    root_symbol = str(row.get("root_symbol") or "").strip().upper()
    if underlying in HOSTED_VOLATILITY_SYMBOLS or root_symbol in HOSTED_VOLATILITY_SYMBOLS:
        return "VOLATILITY"
    return underlying


def _bucket_members(learning_bucket: str) -> tuple[str, ...]:
    if learning_bucket == "VOLATILITY":
        return HOSTED_VOLATILITY_SYMBOLS
    return (learning_bucket,)


def _all_entry_lots_closed(rows: list[dict[str, Any]]) -> bool:
    lots: dict[str, dict[str, float]] = {}
    for row in rows:
        lot_id = str(
            row.get("entry_broker_order_id")
            or row.get("entry_client_order_id")
            or f"{row.get('symbol')}|{row.get('entry_time')}"
        )
        matched_qty = float(row.get("qty") or 0.0)
        required_qty = float(row.get("entry_order_filled_qty") or matched_qty)
        lot = lots.setdefault(lot_id, {"matched": 0.0, "required": 0.0})
        lot["matched"] += matched_qty
        lot["required"] = max(lot["required"], required_qty)
    return bool(lots) and all(lot["matched"] + 1e-9 >= lot["required"] for lot in lots.values())


def _canonical_leg_role(value: Any) -> str | None:
    normalized = str(value or "").strip().lower()
    if normalized in {"primary", "core", "tactical"}:
        return "primary"
    if normalized in {"runner", "rider"}:
        return "runner"
    return None


def _consistent_value(rows: list[dict[str, Any]], key: str) -> Any:
    values = [row.get(key) for row in rows if row.get(key) not in {None, ""}]
    if not values:
        return None
    first = values[0]
    return first if all(value == first for value in values) else None


def _outcome_sort_key(row: dict[str, Any]) -> datetime:
    return _datetime_sort_value(row.get("exit_time") or row.get("recorded_at"))


def _datetime_sort_value(value: Any) -> datetime:
    return _parse_datetime(value) or datetime.min.replace(tzinfo=UTC)


def _outcome_id(payload: dict[str, Any]) -> str:
    raw = "|".join(
        str(payload.get(key) or "")
        for key in ("symbol", "entry_time", "exit_time", "entry_price", "exit_price", "qty")
    )
    if float(payload.get("qty") or 0.0) < float(payload.get("entry_order_filled_qty") or payload.get("qty") or 0.0):
        raw += f"|{payload.get('entry_broker_order_id') or ''}|{payload.get('exit_broker_order_id') or ''}"
    return sha256(raw.encode("utf-8")).hexdigest()[:24]


def _group_outcome_id(payload: dict[str, Any]) -> str:
    raw = "|".join(
        [
            str(payload.get("trade_group_id") or ""),
            *sorted(str(value) for value in payload.get("leg_outcome_ids") or []),
        ]
    )
    return sha256(raw.encode("utf-8")).hexdigest()[:24]


def _option_symbol_parts(symbol: str) -> dict[str, Any]:
    stripped = symbol.strip().upper()
    for index, char in enumerate(stripped):
        if char in {"C", "P"} and index >= 6:
            expiry = stripped[index - 6 : index]
            suffix = stripped[index + 1 :]
            if expiry.isdigit() and suffix.isdigit():
                root_symbol = stripped[: index - 6]
                return {
                    "underlying": "VIX" if root_symbol == "VIXW" else root_symbol,
                    "root_symbol": root_symbol,
                    "option_type": "CALL" if char == "C" else "PUT",
                    "expiration": f"20{expiry[:2]}-{expiry[2:4]}-{expiry[4:6]}",
                    "strike": int(suffix) / 1000.0,
                }
    return {"underlying": stripped, "root_symbol": stripped, "option_type": None, "expiration": None, "strike": None}


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


def _resolve_trading_day(
    trading_day: date | datetime | str | None,
    *,
    timezone_name: str = "America/New_York",
) -> date:
    timezone = ZoneInfo(timezone_name)
    if isinstance(trading_day, str):
        return date.fromisoformat(trading_day)
    if isinstance(trading_day, datetime):
        return trading_day.astimezone(timezone).date() if trading_day.tzinfo else trading_day.date()
    if isinstance(trading_day, date):
        return trading_day
    return datetime.now(tz=timezone).date()


def _float_or_zero(value: Any) -> float:
    parsed = _float_or_none(value)
    return parsed if parsed is not None else 0.0


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _string_or_none(value: Any) -> str | None:
    normalized = str(value or "").strip()
    return normalized or None


def _loss_guard_enabled() -> bool:
    if is_hosted_paper_runtime():
        return True
    return (os.getenv("AUTOBOTT_RECENT_LOSS_GUARD_ENABLED") or "true").strip().lower() in {"1", "true", "yes", "on"}


def _loss_guard_lookback() -> int:
    if is_hosted_paper_runtime():
        return HOSTED_LOSS_GUARD_LOOKBACK
    return max(1, int(os.getenv("AUTOBOTT_RECENT_LOSS_GUARD_LOOKBACK", "20")))


def _loss_guard_consecutive_losses() -> int:
    if is_hosted_paper_runtime():
        return HOSTED_LOSS_GUARD_CONSECUTIVE_LOSSES
    return max(1, int(os.getenv("AUTOBOTT_RECENT_LOSS_GUARD_CONSECUTIVE_LOSSES", "2")))


def _loss_guard_min_sample() -> int:
    if is_hosted_paper_runtime():
        return HOSTED_LOSS_GUARD_MIN_SAMPLE
    return max(1, int(os.getenv("AUTOBOTT_RECENT_LOSS_GUARD_MIN_SAMPLE", "3")))


def _loss_guard_loss_rate() -> float:
    if is_hosted_paper_runtime():
        return HOSTED_LOSS_GUARD_LOSS_RATE
    return min(1.0, max(0.0, float(os.getenv("AUTOBOTT_RECENT_LOSS_GUARD_LOSS_RATE", "0.67"))))


def _winner_bias_enabled() -> bool:
    if is_hosted_paper_runtime():
        return True
    return (os.getenv("AUTOBOTT_RECENT_WINNER_BIAS_ENABLED") or "true").strip().lower() in {"1", "true", "yes", "on"}


def _winner_bias_lookback() -> int:
    if is_hosted_paper_runtime():
        return HOSTED_WINNER_BIAS_LOOKBACK
    return max(1, int(os.getenv("AUTOBOTT_RECENT_WINNER_BIAS_LOOKBACK", "20")))


def _winner_bias_min_sample() -> int:
    if is_hosted_paper_runtime():
        return HOSTED_WINNER_BIAS_MIN_SAMPLE
    return max(1, int(os.getenv("AUTOBOTT_RECENT_WINNER_BIAS_MIN_SAMPLE", "2")))


def _winner_bias_win_rate() -> float:
    if is_hosted_paper_runtime():
        return HOSTED_WINNER_BIAS_WIN_RATE
    return min(1.0, max(0.0, float(os.getenv("AUTOBOTT_RECENT_WINNER_BIAS_WIN_RATE", "0.67"))))


def _winner_bias_consecutive_wins() -> int:
    if is_hosted_paper_runtime():
        return HOSTED_WINNER_BIAS_CONSECUTIVE_WINS
    return max(1, int(os.getenv("AUTOBOTT_RECENT_WINNER_BIAS_CONSECUTIVE_WINS", "2")))
