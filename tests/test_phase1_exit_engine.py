from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from autobott_v2.phase1_engine import build_decision_card
from autobott_v2.phase1_execution_sim import simulate_execution
from autobott_v2.phase1_exit_engine import evaluate_exit
from autobott_v2.phase1_models import (
    CycleProfile,
    CycleStatus,
    DecisionInput,
    MarketBar,
    MarketContext,
    OptionContractSnapshot,
    OptionType,
)


BASE_TIME = datetime(2026, 6, 1, 15, 30, tzinfo=timezone.utc)


def _bars(start: float, step: float) -> list[MarketBar]:
    bars: list[MarketBar] = []
    for index in range(35):
        close = start + index * step
        bars.append(
            MarketBar(
                timestamp=BASE_TIME - timedelta(minutes=34 - index),
                open=close - 0.05,
                high=close + 0.25,
                low=close - 0.25,
                close=close,
                volume=1000 + index * 20,
            )
        )
    return bars


def _contract(symbol: str, expiration: date, option_type: OptionType = OptionType.CALL, delta: float = 0.56) -> OptionContractSnapshot:
    return OptionContractSnapshot(
        option_symbol=symbol,
        underlying="AAPL",
        expiration=expiration,
        strike=215.0,
        option_type=option_type,
        bid=4.9,
        ask=5.1,
        last=5.0,
        volume=200,
        open_interest=900,
        delta=delta,
        theta=-0.04,
        vega=0.08,
        implied_volatility=0.01,
    )


def _entry_event(expiration: date, option_type: OptionType = OptionType.CALL):
    decision_input = DecisionInput(
        ticker="AAPL",
        timestamp=BASE_TIME,
        market_bars=_bars(200.0, 0.45),
        option_chain=[
            _contract("AAPL260602C00215000", date(2026, 6, 2), OptionType.CALL, delta=0.56),
            _contract("AAPL260619C00215000", date(2026, 6, 19), OptionType.CALL, delta=0.48),
        ] if expiration == date(2026, 6, 19) else [_contract("AAPL260602C00215000", expiration, option_type)],
        context=MarketContext(
            spy_bars=_bars(500.0, 0.2),
            qqq_bars=_bars(430.0, 0.15),
            vix_bars=_bars(16.0, -0.02),
        ),
        iv_history=[0.01, 0.02, 0.03, 0.04],
        cycle_profile=CycleProfile(expected_holding_days=6, cycle_confidence=CycleStatus.HIGH),
    )
    card = build_decision_card(decision_input)
    events = simulate_execution(card, underlying_price_at_entry=215.0)
    return events[-1] if expiration == date(2026, 6, 19) and len(events) > 1 else events[0]


def _snapshot(timestamp: datetime, option_symbol: str, expiration: date, bid: float, ask: float) -> dict[str, object]:
    return {
        "schema_version": "phase1.snapshot.v1",
        "source": {
            "name": "deterministic_fixture",
            "environment": "test",
            "latency_assumption": "retail_api_latency",
        },
        "captured_at": timestamp.isoformat(),
        "ticker": "AAPL",
        "timestamp": timestamp.isoformat(),
        "underlying_quote": {
            "symbol": "AAPL",
            "bid": 214.9,
            "ask": 215.1,
            "last": 215.0,
            "spread": 0.2,
            "spread_pct": 0.0009,
            "quote_timestamp": timestamp.isoformat(),
        },
        "market_bars": [],
        "option_chain": [
            {
                "option_symbol": option_symbol,
                "underlying": "AAPL",
                "expiration": expiration.isoformat(),
                "strike": 215.0,
                "option_type": "call",
                "bid": bid,
                "ask": ask,
                "last": round((bid + ask) / 2, 4),
                "spread": round(ask - bid, 4),
                "spread_pct": round((ask - bid) / ((bid + ask) / 2), 4),
                "quote_timestamp": timestamp.isoformat(),
                "volume": 200,
                "open_interest": 900,
                "delta": 0.56,
                "theta": -0.04,
                "vega": 0.08,
                "implied_volatility": 0.01,
                "iv_percentile": 0.25,
                "realized_volatility": 0.01,
            }
        ],
        "context": {
            "spy_bars": [],
            "qqq_bars": [],
            "vix_bars": [],
            "blackout_event": False,
            "event_labels": [],
        },
        "iv_history": [0.01],
    }


def test_open_position_closes_on_tactical_profit_target() -> None:
    entry = _entry_event(date(2026, 6, 2))
    snapshot = _snapshot(BASE_TIME + timedelta(minutes=20), entry.selected_contract.option_symbol, entry.selected_contract.expiration, 7.2, 7.4)

    decision = evaluate_exit(entry, snapshot)

    assert decision.exit_action == "close"
    assert decision.exit_reason == "profit_target"


def test_open_position_closes_on_tactical_stop_loss() -> None:
    entry = _entry_event(date(2026, 6, 2))
    snapshot = _snapshot(BASE_TIME + timedelta(minutes=10), entry.selected_contract.option_symbol, entry.selected_contract.expiration, 3.1, 3.3)

    decision = evaluate_exit(entry, snapshot)

    assert decision.exit_action == "close"
    assert decision.exit_reason == "stop_loss"


def test_rider_closes_when_dte_floor_breached() -> None:
    entry = _entry_event(date(2026, 6, 19))
    snapshot_time = datetime(2026, 6, 15, 15, 30, tzinfo=timezone.utc)
    snapshot = _snapshot(snapshot_time, entry.selected_contract.option_symbol, entry.selected_contract.expiration, 5.0, 5.2)

    decision = evaluate_exit(entry, snapshot)

    assert decision.exit_action == "close"
    assert decision.exit_reason == "dte_floor"


def test_stale_exit_quote_does_not_close_at_mid() -> None:
    entry = _entry_event(date(2026, 6, 2))
    snapshot = _snapshot(BASE_TIME + timedelta(minutes=10), entry.selected_contract.option_symbol, entry.selected_contract.expiration, 7.2, 7.4)

    decision = evaluate_exit(entry, snapshot, quote_age_seconds=90)

    assert decision.exit_action == "unresolved"
    assert decision.exit_reason == "exit_rejected_stale_quote"


def test_missing_exit_quote_creates_unresolved_position() -> None:
    entry = _entry_event(date(2026, 6, 2))
    snapshot = _snapshot(BASE_TIME + timedelta(minutes=20), "OTHER", entry.selected_contract.expiration, 7.2, 7.4)

    decision = evaluate_exit(entry, snapshot)

    assert decision.exit_action == "unresolved"
    assert decision.exit_reason == "missing_exit_quote"
