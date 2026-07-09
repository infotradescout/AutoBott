from __future__ import annotations

from autobott_v2.trade_outcomes import record_trade_outcomes_from_orders, recent_loss_guard, recent_winner_bias


def test_trade_outcomes_persist_winner_loser_and_reason(tmp_path) -> None:
    orders = [
        {
            "symbol": "AAPL260710C00105000",
            "side": "buy",
            "qty": "1",
            "filled_qty": "1",
            "filled_avg_price": "2.00",
            "status": "filled",
            "submitted_at": "2026-07-09T14:00:00Z",
            "filled_at": "2026-07-09T14:00:00Z",
        },
        {
            "symbol": "AAPL260710C00105000",
            "side": "sell",
            "qty": "1",
            "filled_qty": "1",
            "filled_avg_price": "1.00",
            "status": "filled",
            "submitted_at": "2026-07-09T14:15:00Z",
            "filled_at": "2026-07-09T14:15:00Z",
        },
        {
            "symbol": "MSFT260710P00200000",
            "side": "buy",
            "qty": "1",
            "filled_qty": "1",
            "filled_avg_price": "2.00",
            "status": "filled",
            "submitted_at": "2026-07-09T15:00:00Z",
            "filled_at": "2026-07-09T15:00:00Z",
        },
        {
            "symbol": "MSFT260710P00200000",
            "side": "sell",
            "qty": "1",
            "filled_qty": "1",
            "filled_avg_price": "3.00",
            "status": "filled",
            "submitted_at": "2026-07-09T15:30:00Z",
            "filled_at": "2026-07-09T15:30:00Z",
        },
    ]

    result = record_trade_outcomes_from_orders(orders, journal_path=tmp_path / "trade_outcomes.jsonl")
    repeat = record_trade_outcomes_from_orders(orders, journal_path=tmp_path / "trade_outcomes.jsonl")

    assert result["recorded"] == 2
    assert repeat["recorded"] == 0
    assert result["summary"]["wins"] == 1
    assert result["summary"]["losses"] == 1
    loser = [row for row in result["outcomes"] if row["result"] == "loser"][0]
    assert loser["underlying"] == "AAPL"
    assert loser["classification"] == "large_loss"
    assert "loss_exceeded_configured_stop_zone" in loser["why"]


def test_recent_loss_guard_blocks_repeated_underlying_losses() -> None:
    rows = [
        {"underlying": "AAPL", "pnl": -50.0, "outcome_id": "loss-1"},
        {"underlying": "MSFT", "pnl": 30.0, "outcome_id": "win-1"},
        {"underlying": "AAPL", "pnl": -75.0, "outcome_id": "loss-2"},
    ]

    guard = recent_loss_guard(rows)

    assert guard["enabled"] is True
    assert guard["blocked_underlyings"] == ["AAPL"]
    assert guard["reasons"]["AAPL"]["consecutive_losses"] is True


def test_recent_winner_bias_prefers_repeated_winners() -> None:
    rows = [
        {"underlying": "AAPL", "pnl": 50.0, "outcome_id": "win-1"},
        {"underlying": "MSFT", "pnl": -30.0, "outcome_id": "loss-1"},
        {"underlying": "AAPL", "pnl": 75.0, "outcome_id": "win-2"},
    ]

    bias = recent_winner_bias(rows)

    assert bias["enabled"] is True
    assert bias["preferred_underlyings"] == ["AAPL"]
    assert bias["reasons"]["AAPL"]["consecutive_wins"] is True
