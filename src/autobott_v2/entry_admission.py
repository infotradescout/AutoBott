"""Read-only entry revalidation; no broker, exit, or accounting writes."""
from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime
import math
import hashlib
import json
from typing import Any

from .bar_timing import aware_utc, bar_duration
from .entry_market_context import assess_entry_context
from .core_runner import CoreRunnerPair, CoreRunnerRules, select_core_runner_pair
from .hosted_policy import signal_proxy_for
from .live_entry_thesis import assess_live_entry_thesis
from .phase1_engine import _contract_rejection_reasons
from .phase1_models import DecisionCard, DecisionInput, DecisionStatus, ExecutionLayer, Phase1Rules
from .phase1_validate import _contract_from_payload, _cycle_profile_from_snapshot
from .quote_observation import observed_quote_fields


@dataclass(frozen=True)
class EntryMarketRules:
    # Operational freshness bounds, not fitted success/holding-period rules.
    # 30 seconds matches the existing execution simulator's quote-age ceiling.
    max_quote_age_seconds: float = 30.0
    max_decision_age_seconds: float = 30.0
    # One completed interval plus bounded provider publication delay.
    # No overnight/weekend exemption: new entries wait for fresh signal bars.
    max_bar_publication_delay_seconds: float = 30.0

    def __post_init__(self) -> None:
        for value in (self.max_quote_age_seconds, self.max_decision_age_seconds,
                      self.max_bar_publication_delay_seconds):
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
                raise ValueError("invalid_entry_market_age_limit")


class EntryMarketRejected(ValueError):
    def __init__(self, reason: str, detail: str | None = None):
        self.reason = reason
        self.detail = detail or reason
        super().__init__(self.detail)


def _age(timestamp: datetime, now: datetime, maximum: float, label: str) -> float:
    age = (now - timestamp).total_seconds()
    if age < 0:
        raise EntryMarketRejected("entry_future_timestamp", label)
    if age > maximum:
        raise EntryMarketRejected("entry_stale_market_evidence", label)
    return age


def _quote(raw: Any, now: datetime, maximum: float, symbol: str):
    try:
        bid, ask, timestamp = observed_quote_fields(raw, allow_zero_bid=False)
    except ValueError as exc:
        raise EntryMarketRejected("entry_quote_invalid", f"{symbol}:{exc}") from exc
    stamp = aware_utc(timestamp)
    return bid, ask, stamp, _age(stamp, now, maximum, symbol)


def _live_thesis(snapshot: Mapping[str, Any], decision: DecisionCard, *,
                 signal_symbol: str, bid: float, ask: float, stage: str) -> dict[str, Any]:
    evidence = assess_live_entry_thesis(snapshot, direction=decision.direction.bias.value,
                                        signal_symbol=signal_symbol, bid=bid, ask=ask)
    if evidence["status"] == "invalidated":
        raise EntryMarketRejected("entry_signal_price_invalidated",
                                  f"{stage}:{decision.direction.bias.value}:{evidence['boundary']}")
    return {"stage": stage, **evidence}


def _entry_context(snapshot, decision, data_client, *, checked_at, bid, ask):
    evidence = assess_entry_context(snapshot,direction=decision.direction.bias.value,
                                    checked_at=checked_at,bid=bid,ask=ask)
    legacy = evidence["status"] == "not_recorded"
    if evidence["status"] != "confirmed" and not (legacy and not getattr(data_client,"requires_entry_context",False)):
        raise EntryMarketRejected("entry_context_not_confirmed", evidence["reason"])
    return evidence


def _completed_evidence(snapshot: Mapping[str, Any], decision: DecisionCard, *,
                        checked_at: datetime, rules: EntryMarketRules) -> dict[str, Any]:
    proof = snapshot.get("bar_evidence", {})
    if proof.get("timestamp_semantics") != "interval_start" or proof.get("completed_bars_only") is not True:
        raise EntryMarketRejected("entry_completed_bar_evidence_missing")
    cutoff = aware_utc(proof.get("cutoff"))
    if cutoff > aware_utc(decision.timestamp):
        raise EntryMarketRejected("entry_bar_cutoff_after_decision")
    duration = bar_duration(proof.get("timeframe"))
    checked_at = aware_utc(checked_at)
    maximum_age = duration.total_seconds() + rules.max_bar_publication_delay_seconds
    last_closed, ages = {}, {}
    groups = {"underlying": snapshot.get("market_bars", [])}
    context = snapshot.get("context", {})
    groups.update({key: context.get(key, []) for key in ("spy_bars", "qqq_bars", "vix_bars")})
    for label, rows in groups.items():
        if not rows:
            raise EntryMarketRejected("entry_completed_bar_evidence_missing", label)
        times = [aware_utc(row.get("timestamp")) for row in rows]
        if any(a >= b for a, b in zip(times, times[1:])):
            raise EntryMarketRejected("entry_bar_order_invalid", label)
        if any(t + duration > cutoff for t in times):
            raise EntryMarketRejected("entry_unfinished_bar", label)
        close = times[-1] + duration
        age = (checked_at - close).total_seconds()
        if age < 0:
            raise EntryMarketRejected("entry_future_timestamp", label)
        if age > maximum_age:
            raise EntryMarketRejected("entry_stale_completed_bar", label)
        last_closed[label], ages[label] = close.isoformat(), age
    return {"timeframe": proof["timeframe"], "cutoff": cutoff.isoformat(),
            "last_closed": last_closed, "checked_at": checked_at.isoformat(),
            "max_completed_bar_age_seconds": maximum_age,
            "completed_bar_ages_seconds": ages}


def filter_entry_quote_candidates(
    decision_input: DecisionInput, snapshot: Mapping[str, Any], *, rules: EntryMarketRules | None = None,
) -> tuple[DecisionInput, dict[str, Any]]:
    """Keep stale/invalid quotes out of the ranking, not just out of submission.

    The source snapshot stays unchanged, including rejected observations.
    This is runtime input admission, not a retrospective rewrite of replay data.
    """
    rules = rules or EntryMarketRules()
    now = aware_utc(decision_input.timestamp)
    if snapshot.get("ticker") != decision_input.ticker or aware_utc(snapshot.get("timestamp")) != now:
        raise EntryMarketRejected("entry_snapshot_identity_mismatch")
    stock = snapshot.get("underlying_quote", {})
    _quote({"bp": stock.get("bid"), "ap": stock.get("ask"), "t": stock.get("quote_timestamp")},
           now, rules.max_quote_age_seconds, decision_input.ticker)
    raw_by_symbol: dict[str, list[Mapping[str, Any]]] = {}
    for row in snapshot.get("option_chain", []):
        raw_by_symbol.setdefault(row.get("option_symbol"), []).append(row)
    allowed, rejected = [], []
    for contract in decision_input.option_chain:
        matches = raw_by_symbol.get(contract.option_symbol, [])
        try:
            if len(matches) != 1:
                raise EntryMarketRejected("entry_contract_identity_missing_or_ambiguous")
            row = matches[0]
            _quote({"bp": row.get("bid"), "ap": row.get("ask"), "t": row.get("quote_timestamp")},
                   now, rules.max_quote_age_seconds, contract.option_symbol)
        except EntryMarketRejected as exc:
            rejected.append({"option_symbol": contract.option_symbol, "reason": exc.reason})
        else:
            allowed.append(contract)
    return replace(decision_input, option_chain=allowed), {
        "checked_at": now.isoformat(), "max_quote_age_seconds": rules.max_quote_age_seconds,
        "eligible_contracts": len(allowed), "rejected": rejected,
    }


def refresh_entry_admission(
    decision: DecisionCard,
    pair: CoreRunnerPair | None,
    snapshot: Mapping[str, Any],
    data_client: Any,
    *,
    decision_rules: Phase1Rules,
    pair_rules: CoreRunnerRules | None,
    authorized_prices: Mapping[str, float],
    now_fn: Callable[[], datetime],
    rules: EntryMarketRules | None = None,
) -> dict[str, Any]:
    """Recheck EXACT approved contracts just before the submission callback.

    Only reads market quotes. Returned evidence does not change order prices,
    exits, or selection. The candidate entry policy rejects a captured or
    refreshed quote wholly beyond the completed signal bar against direction.
    Indicative-feed quotes are not represented as executable prices. Greeks/OI
    remain captured metadata, bounded by the decision-age ceiling, not newly
    broker-verified or recalculated Greeks.
    """
    try:
        rules = rules or EntryMarketRules()
        if decision.decision is not DecisionStatus.TRADE_CANDIDATE or decision.selected_contract is None:
            raise EntryMarketRejected("entry_not_approved")
        if snapshot.get("ticker") != decision.ticker or aware_utc(snapshot.get("timestamp")) != aware_utc(decision.timestamp):
            raise EntryMarketRejected("entry_snapshot_identity_mismatch")
        before = aware_utc(now_fn())
        _age(aware_utc(decision.timestamp), before, rules.max_decision_age_seconds, "decision")
        bars = _completed_evidence(snapshot, decision, checked_at=before, rules=rules)
        signal_symbol = signal_proxy_for(decision.ticker)
        captured_stock = snapshot.get("underlying_quote", {})
        cbid, cask, _, _ = _quote({"bp": captured_stock.get("bid"), "ap": captured_stock.get("ask"),
                                  "t": captured_stock.get("quote_timestamp")},
                                 aware_utc(decision.timestamp), rules.max_quote_age_seconds, decision.ticker)
        captured_thesis = _live_thesis(snapshot, decision, signal_symbol=signal_symbol,
                                      bid=cbid, ask=cask, stage="capture")
        captured_context = _entry_context(snapshot,decision,data_client,checked_at=before,bid=cbid,ask=cask)
        primary = decision.selected_contract
        if pair is not None and pair.primary.option_symbol != primary.option_symbol:
            raise EntryMarketRejected("entry_primary_identity_changed")
        contracts = [primary] if pair is None else [pair.primary, pair.runner]
        symbols = [contract.option_symbol for contract in contracts]
        if len(set(symbols)) != len(symbols):
            raise EntryMarketRejected("entry_duplicate_contract_identity")
        originals = {}
        for contract in contracts:
            matches = [row for row in snapshot.get("option_chain", []) if row.get("option_symbol") == contract.option_symbol]
            if len(matches) != 1:
                raise EntryMarketRejected("entry_contract_identity_missing_or_ambiguous", contract.option_symbol)
            row = matches[0]
            if (row.get("underlying", "").upper() != decision.ticker.upper()
                    or row.get("expiration") != contract.expiration.isoformat()
                    or row.get("strike") != contract.strike
                    or row.get("option_type") != contract.option_type.value):
                raise EntryMarketRejected("entry_contract_identity_changed", contract.option_symbol)
            originals[contract.option_symbol] = row
        option_getter = getattr(data_client, "get_latest_option_quotes", None)
        stock_getter = getattr(data_client, "get_latest_stock_quotes", None)
        if not callable(option_getter) or not callable(stock_getter):
            raise EntryMarketRejected("entry_quote_refresh_unavailable")
        try:
            option_quotes = option_getter(symbols)
            stock_quotes = stock_getter([signal_symbol])
        except Exception as exc:
            raise EntryMarketRejected("entry_quote_refresh_failed", type(exc).__name__) from exc
        after = aware_utc(now_fn())
        if after < before:
            raise EntryMarketRejected("entry_clock_regressed")
        decision_age = _age(aware_utc(decision.timestamp), after, rules.max_decision_age_seconds, "decision")
        # Provider latency counts against the signal deadline as well.
        bars = _completed_evidence(snapshot, decision, checked_at=after, rules=rules)
        if not isinstance(option_quotes, Mapping) or not isinstance(stock_quotes, Mapping):
            raise EntryMarketRejected("entry_quote_response_invalid")
        sbid, sask, stime, sage = _quote(stock_quotes.get(signal_symbol), after, rules.max_quote_age_seconds, signal_symbol)
        source_stock_time = aware_utc(snapshot["underlying_quote"]["quote_timestamp"])
        _age(source_stock_time, aware_utc(decision.timestamp), rules.max_quote_age_seconds, "captured_underlying")
        if stime < source_stock_time:
            raise EntryMarketRejected("entry_quote_timestamp_regressed", signal_symbol)
        refreshed_thesis = _live_thesis(snapshot, decision, signal_symbol=signal_symbol,
                                       bid=sbid, ask=sask, stage="refresh")
        refreshed_context = _entry_context(snapshot,decision,data_client,checked_at=after,bid=sbid,ask=sask)
        fresh = []
        quote_evidence = []
        for approved in contracts:
            symbol = approved.option_symbol
            bid, ask, timestamp, age = _quote(option_quotes.get(symbol), after, rules.max_quote_age_seconds, symbol)
            source_time = aware_utc(originals[symbol]["quote_timestamp"])
            _age(source_time, aware_utc(decision.timestamp), rules.max_quote_age_seconds, f"captured:{symbol}")
            if timestamp < source_time:
                raise EntryMarketRejected("entry_quote_timestamp_regressed", symbol)
            authorized = authorized_prices.get(symbol)
            if isinstance(authorized, bool) or not isinstance(authorized, (int, float)) or not math.isfinite(authorized) or authorized <= 0:
                raise EntryMarketRejected("entry_authorized_price_missing", symbol)
            # Do not chase a higher ask beyond the ORIGINAL allowance. A passive
            # limit can remain passive; this does not promise a fill at the ask.
            ceiling = max(approved.ask, authorized)
            if ask > ceiling:
                raise EntryMarketRejected("entry_quote_above_original_allowance", symbol)
            fresh.append(replace(_contract_from_payload(originals[symbol]), bid=bid, ask=ask))
            quote_evidence.append({"option_symbol": symbol, "bid": bid, "ask": ask,
                "quote_timestamp": timestamp.isoformat(), "quote_age_seconds": age,
                "original_ask": approved.ask, "original_price_allowance": ceiling})
        layer = ExecutionLayer.TACTICAL if decision.execution_layer in {ExecutionLayer.TACTICAL, ExecutionLayer.BOTH} else ExecutionLayer.RIDER
        underlying = (sbid + sask) / 2 if signal_symbol == decision.ticker.upper() else float(snapshot["underlying_quote"]["last"])
        rejections = _contract_rejection_reasons(fresh[0], underlying, after.date(), decision_rules, layer,
                                                _cycle_profile_from_snapshot(snapshot.get("cycle_profile", {})))
        if rejections:
            raise EntryMarketRejected("entry_primary_contract_no_longer_eligible", ",".join(rejections))
        if pair is not None:
            if pair_rules is None:
                raise EntryMarketRejected("entry_pair_rules_missing")
            checked = select_core_runner_pair(pair.primary, fresh, rules=pair_rules)
            if checked is None or checked.runner.option_symbol != pair.runner.option_symbol:
                raise EntryMarketRejected("entry_pair_no_longer_eligible")
        feed = getattr(data_client, "option_feed", getattr(data_client, "feed", "unreported"))
        return {
            "schema_version": "entry_admission.v1", "decision_id": decision.decision_id,
            "checked_at": after.isoformat(), "decision_age_seconds": decision_age,
            "max_quote_age_seconds": rules.max_quote_age_seconds,
            "max_decision_age_seconds": rules.max_decision_age_seconds,
            "primary_option_symbol": primary.option_symbol, "quotes": quote_evidence,
            "signal_symbol": signal_symbol, "signal_quote_timestamp": stime.isoformat(),
            "signal_quote_age_seconds": sage, "completed_bars": bars,
            "live_signal_thesis": {"at_capture": captured_thesis, "at_refresh": refreshed_thesis},
            "entry_context_evidence": {"at_capture":captured_context,"at_refresh":refreshed_context},
            "underlying_reference_basis": "fresh_equity_mid" if signal_symbol == decision.ticker.upper() else "captured_index_estimate_with_fresh_proxy",
            "options_feed": feed, "executable_fill_verified": False,
            "entry_edge_established": False,
            # Retain normalized entry-time observations for an offline study.
            # No credentials, future path, order submission, or new data read.
            "recorded_refresh": {
                "snapshot_hash": hashlib.sha256(json.dumps(snapshot, sort_keys=True,
                    allow_nan=False, separators=(",", ":")).encode()).hexdigest(),
                "requested_at": before.isoformat(), "received_at": after.isoformat(),
                "options_feed": feed,
                "authorized_prices": {c.option_symbol: authorized_prices[c.option_symbol] for c in contracts},
                "option_quotes": {q["option_symbol"]: {"bp": q["bid"], "ap": q["ask"],
                    "t": q["quote_timestamp"]} for q in quote_evidence},
                "stock_quotes": {signal_symbol: {"bp": sbid, "ap": sask, "t": stime.isoformat()}},
            },
        }
    except EntryMarketRejected:
        raise
    except (ValueError, TypeError, KeyError, AttributeError, OverflowError) as exc:
        raise EntryMarketRejected("entry_market_evidence_invalid", type(exc).__name__) from exc
