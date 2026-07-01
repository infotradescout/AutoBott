from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from autobott_v2.phase1_engine import build_decision_card
from autobott_v2.phase1_models import OptionType, TradeSetup
from autobott_v2.phase1_validate import _decision_input_from_snapshot
from autobott_v2.thesis_validation import evaluate_decision_thesis, summarize_thesis_results


BASE_TIME = datetime(2026, 6, 1, 15, 30, tzinfo=UTC)


def _bar(index: int, ts: datetime, start: float = 200.0, step: float = 0.45) -> dict[str, object]:
    close = start + index * step
    return {
        "timestamp": (ts - timedelta(minutes=34 - index)).isoformat(),
        "open": close - 0.05,
        "high": close + 0.25,
        "low": close - 0.25,
        "close": close,
        "volume": 1000 + index * 10,
    }


def _bars(ts: datetime, start: float = 200.0, step: float = 0.45) -> list[dict[str, object]]:
    return [_bar(index, ts, start, step) for index in range(35)]


def _snapshot(ts: datetime, *, option_type: str = "call", underlying_last: float = 215.0, market_step: float = 0.45, prior_step: float | None = None) -> dict[str, object]:
    bars = _bars(ts, 200.0, prior_step if prior_step is not None else market_step)
    delta = 0.56 if option_type == "call" else -0.56
    return {
        "schema_version": "phase1.snapshot.v1",
        "source": {"name": "fixture", "environment": "test", "latency_assumption": "retail_api_latency"},
        "captured_at": ts.isoformat(),
        "ticker": "AAPL",
        "timestamp": ts.isoformat(),
        "underlying_quote": {
            "symbol": "AAPL",
            "bid": underlying_last - 0.1,
            "ask": underlying_last + 0.1,
            "last": underlying_last,
            "spread": 0.2,
            "spread_pct": 0.001,
            "quote_timestamp": ts.isoformat(),
        },
        "market_bars": bars,
        "option_chain": [
            {
                "option_symbol": f"AAPL260603{'C' if option_type == 'call' else 'P'}00215000",
                "underlying": "AAPL",
                "expiration": "2026-06-03",
                "strike": 215.0,
                "option_type": option_type,
                "bid": 4.9,
                "ask": 5.1,
                "last": 5.0,
                "spread": 0.2,
                "spread_pct": 0.04,
                "quote_timestamp": ts.isoformat(),
                "volume": 250,
                "open_interest": 900,
                "delta": delta,
                "theta": -0.04,
                "vega": 0.08,
                "implied_volatility": 0.01,
                "iv_percentile": 0.25,
                "realized_volatility": 0.01,
            }
        ],
        "context": {
            "spy_bars": _bars(ts, 500.0, 0.2),
            "qqq_bars": _bars(ts, 430.0, 0.15),
            "vix_bars": _bars(ts, 16.0, -0.02),
            "blackout_event": False,
            "event_labels": [],
        },
        "iv_history": [0.01, 0.02, 0.03, 0.04],
        "cycle_profile": {"expected_holding_days": 2, "cycle_confidence": "high", "last_pivot_type": "unknown"},
    }


def test_continuation_call_requires_up_followthrough() -> None:
    entry = _snapshot(BASE_TIME, option_type="call", underlying_last=215.0, market_step=0.45)
    future_1 = _snapshot(BASE_TIME + timedelta(hours=4), option_type="call", underlying_last=217.0, market_step=0.45)
    future_2 = _snapshot(BASE_TIME + timedelta(days=1), option_type="call", underlying_last=220.0, market_step=0.45)
    decision = build_decision_card(_decision_input_from_snapshot(entry))
    result = evaluate_decision_thesis(decision, entry, [future_1, future_2])
    assert result.passed is True
    assert result.directional_match is True
    assert result.first_move_match is True
    assert result.followthrough_rate == 1.0


def test_tactical_2dte_continuation_fails_when_price_first_moves_against_pick() -> None:
    entry = _snapshot(BASE_TIME, option_type="call", underlying_last=215.0, market_step=0.45)
    future_1 = _snapshot(BASE_TIME + timedelta(hours=4), option_type="call", underlying_last=214.0, market_step=-0.20)
    future_2 = _snapshot(BASE_TIME + timedelta(days=1), option_type="call", underlying_last=220.0, market_step=0.45)
    decision = build_decision_card(_decision_input_from_snapshot(entry))
    result = evaluate_decision_thesis(decision, entry, [future_1, future_2])
    assert result.directional_match is True
    assert result.first_move_match is False
    assert result.passed is False


def test_reversal_requires_prior_move_opposite_and_future_move_back() -> None:
    entry = _snapshot(BASE_TIME, option_type="put", underlying_last=215.0, prior_step=0.8)
    decision = SimpleNamespace(
        decision_id="reversal-1",
        ticker="AAPL",
        trade_setup=TradeSetup.LATE_CYCLE_BEARISH_REVERSAL,
        selected_contract=SimpleNamespace(option_type=OptionType.PUT, expiration=(BASE_TIME + timedelta(days=2)).date()),
    )
    future_1 = _snapshot(BASE_TIME + timedelta(hours=4), option_type="put", underlying_last=213.0, market_step=-0.25)
    future_2 = _snapshot(BASE_TIME + timedelta(days=1), option_type="put", underlying_last=210.0, market_step=-0.45)
    result = evaluate_decision_thesis(decision, entry, [future_1, future_2])
    assert result.passed is True
    assert result.reversal_confirmed is True
    assert result.first_move_match is True


def test_reversal_fails_without_actual_turn_back() -> None:
    entry = _snapshot(BASE_TIME, option_type="put", underlying_last=215.0, prior_step=0.8)
    decision = SimpleNamespace(
        decision_id="reversal-2",
        ticker="AAPL",
        trade_setup=TradeSetup.LATE_CYCLE_BEARISH_REVERSAL,
        selected_contract=SimpleNamespace(option_type=OptionType.PUT, expiration=(BASE_TIME + timedelta(days=2)).date()),
    )
    future_1 = _snapshot(BASE_TIME + timedelta(hours=4), option_type="put", underlying_last=216.0, market_step=0.15)
    future_2 = _snapshot(BASE_TIME + timedelta(days=1), option_type="put", underlying_last=214.0, market_step=-0.10)
    result = evaluate_decision_thesis(decision, entry, [future_1, future_2])
    assert result.directional_match is True
    assert result.first_move_match is False
    assert result.reversal_confirmed is False
    assert result.passed is False


def test_summary_counts_passes_and_fails() -> None:
    entry = _snapshot(BASE_TIME)
    decision = build_decision_card(_decision_input_from_snapshot(entry))
    good = evaluate_decision_thesis(
        decision,
        entry,
        [
            _snapshot(BASE_TIME + timedelta(hours=4), underlying_last=217.0),
            _snapshot(BASE_TIME + timedelta(days=1), underlying_last=220.0),
        ],
    )
    bad = evaluate_decision_thesis(
        decision,
        entry,
        [
            _snapshot(BASE_TIME + timedelta(hours=4), underlying_last=214.0, market_step=-0.20),
            _snapshot(BASE_TIME + timedelta(days=1), underlying_last=220.0),
        ],
    )
    summary = summarize_thesis_results([good, bad])
    assert summary["decisions_evaluated"] == 2
    assert summary["passes"] == 1
    assert summary["tactical_2dte_pass_rate"] == 0.5
