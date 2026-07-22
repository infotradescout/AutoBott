from __future__ import annotations

import json
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

from autobott_v2.trade_outcomes import (
    build_trade_outcomes_from_orders,
    daily_realized_pnl,
    load_trade_outcomes,
    record_trade_outcomes_from_orders,
    recent_loss_guard,
    recent_winner_bias,
    summarize_trade_outcomes,
    sync_trade_outcomes_from_broker,
)


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
        {"underlying": "AAPL", "pnl": -25.0, "outcome_id": "loss-3"},
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


def test_execution_journal_enriches_legs_and_builds_one_completed_pair(tmp_path) -> None:
    primary = "AAPL260710C00105000"
    runner = "AAPL260710C00110000"
    orders = [
        _broker_order(primary, "buy", "entry-primary", "2.00", "2026-07-09T14:00:00Z"),
        _broker_order(runner, "buy", "entry-runner", "1.00", "2026-07-09T14:00:01Z"),
        _broker_order(primary, "sell", "exit-primary", "3.00", "2026-07-09T14:30:00Z"),
        _broker_order(runner, "sell", "exit-runner", "0.50", "2026-07-09T14:31:00Z"),
    ]
    execution_rows = [
        _execution_order_row(primary, "entry-primary", leg_role="primary"),
        _execution_order_row(runner, "entry-runner", leg_role="runner"),
        _execution_order_row(primary, "exit-primary", exit_reason="take_profit"),
        _execution_order_row(runner, "exit-runner", exit_reason="stop_loss"),
    ]

    result = record_trade_outcomes_from_orders(
        orders,
        journal_path=tmp_path / "trade_outcomes.jsonl",
        execution_journal_rows=execution_rows,
    )

    assert result["recorded"] == 2
    assert result["summary"]["closed_legs"] == 2
    assert result["summary"]["closed_trades"] == 2
    assert result["summary"]["independent_trades"] == 1
    assert result["summary"]["completed_groups"] == 1
    assert result["summary"]["net_pnl"] == 50.0
    by_role = {row["leg_role"]: row for row in result["outcomes"]}
    assert by_role["primary"]["decision_id"] == "decision-123"
    assert by_role["primary"]["trade_group_id"] == "core-runner:decision-123"
    assert by_role["primary"]["exit_reason"] == "take_profit"
    assert by_role["runner"]["exit_reason"] == "stop_loss"
    assert all(row["match_source"] == "execution_journal" for row in result["outcomes"])
    group = result["completed_groups"][0]
    assert group["primary_pnl"] == 100.0
    assert group["runner_pnl"] == -50.0
    assert group["pnl"] == 50.0
    assert group["entry_debit"] == 300.0
    assert group["return_pct"] == 0.1667
    assert group["runner_funded"] is True
    assert result["group_summary"]["expectancy"] == 50.0


def test_partial_exit_fills_preserve_remaining_entry_quantity() -> None:
    symbol = "AAPL260710C00105000"
    orders = [
        _broker_order(symbol, "buy", "entry", "2.00", "2026-07-09T14:00:00Z", qty="2"),
        _broker_order(symbol, "sell", "exit-1", "3.00", "2026-07-09T14:30:00Z"),
        _broker_order(symbol, "sell", "exit-2", "1.00", "2026-07-09T15:00:00Z"),
    ]

    outcomes = build_trade_outcomes_from_orders(orders)

    assert len(outcomes) == 2
    assert [row["qty"] for row in outcomes] == [1.0, 1.0]
    assert [row["pnl"] for row in outcomes] == [100.0, -100.0]
    assert len({row["outcome_id"] for row in outcomes}) == 2


def test_cumulative_partial_fill_is_recorded_once_only_after_terminal_fill(tmp_path) -> None:
    symbol = "AAPL260710C00105000"
    journal_path = tmp_path / "trade_outcomes.jsonl"
    entry = _broker_order(symbol, "buy", "entry", "2.00", "2026-07-09T14:00:00Z", qty="2")
    partial_exit = _broker_order(
        symbol,
        "sell",
        "exit",
        "3.00",
        "2026-07-09T14:30:00Z",
        qty="2",
        filled_qty="1",
        status="partially_filled",
    )

    partial = record_trade_outcomes_from_orders([entry, partial_exit], journal_path=journal_path)
    final_exit = {**partial_exit, "status": "filled", "filled_qty": "2", "filled_at": "2026-07-09T14:31:00Z"}
    final = record_trade_outcomes_from_orders([entry, final_exit], journal_path=journal_path)
    repeat = record_trade_outcomes_from_orders([entry, final_exit], journal_path=journal_path)

    assert partial["recorded"] == 0
    assert final["recorded"] == 1
    assert repeat["recorded"] == 0
    assert final["outcomes"][0]["qty"] == 2.0
    assert final["outcomes"][0]["pnl"] == 200.0
    assert len(load_trade_outcomes(journal_path=journal_path)) == 1


def test_live_partial_loss_is_included_in_daily_guard_without_persisting(tmp_path) -> None:
    symbol = "AAPL260710C00105000"
    journal_path = tmp_path / "trade_outcomes.jsonl"
    trading_day = datetime.now(tz=ZoneInfo("America/New_York")).replace(
        hour=11,
        minute=0,
        second=0,
        microsecond=0,
    )
    entry = _broker_order(symbol, "buy", "entry", "10.00", trading_day.astimezone(UTC).isoformat())
    partial_exit = _broker_order(
        symbol,
        "sell",
        "partial-exit",
        "1.00",
        trading_day.astimezone(UTC).isoformat(),
        qty="2",
        filled_qty="1",
        status="partially_filled",
    )

    result = record_trade_outcomes_from_orders([entry, partial_exit], journal_path=journal_path)

    assert result["recorded"] == 0
    assert result["daily_realized_pnl"] == -900.0
    assert load_trade_outcomes(journal_path=journal_path) == []


def test_loss_guard_counts_completed_pairs_instead_of_correlated_legs() -> None:
    first_pair = [
        _group_leg("group-1", "primary", -60.0, "primary-1", "2026-07-09T14:30:00Z"),
        _group_leg("group-1", "runner", -20.0, "runner-1", "2026-07-09T14:31:00Z"),
    ]

    assert recent_loss_guard(first_pair)["blocked_underlyings"] == []

    second_pair = [
        _group_leg("group-2", "primary", -40.0, "primary-2", "2026-07-10T14:30:00Z"),
        _group_leg("group-2", "runner", -10.0, "runner-2", "2026-07-10T14:31:00Z"),
    ]
    guard = recent_loss_guard([*first_pair, *second_pair])

    assert guard["blocked_underlyings"] == ["AAPL"]
    assert guard["reasons"]["AAPL"]["recent_trades"] == 2


def test_winner_bias_counts_completed_pairs_instead_of_correlated_legs() -> None:
    first_pair = [
        _group_leg("group-1", "primary", 60.0, "primary-1", "2026-07-09T14:30:00Z"),
        _group_leg("group-1", "runner", 20.0, "runner-1", "2026-07-09T14:31:00Z"),
    ]

    assert recent_winner_bias(first_pair)["preferred_underlyings"] == []

    second_pair = [
        _group_leg("group-2", "primary", 40.0, "primary-2", "2026-07-10T14:30:00Z"),
        _group_leg("group-2", "runner", 10.0, "runner-2", "2026-07-10T14:31:00Z"),
    ]
    bias = recent_winner_bias([*first_pair, *second_pair])

    assert bias["preferred_underlyings"] == ["AAPL"]
    assert bias["reasons"]["AAPL"]["recent_trades"] == 2


def test_incomplete_group_is_excluded_from_expectancy() -> None:
    rows = [_group_leg("group-1", "primary", 100.0, "primary-1", "2026-07-09T14:30:00Z")]

    summary = summarize_trade_outcomes(rows)

    assert summary["closed_legs"] == 1
    assert summary["closed_trades"] == 1
    assert summary["independent_trades"] == 0
    assert summary["incomplete_group_legs"] == 1
    assert summary["net_pnl"] == 100.0
    assert summary["independent_net_pnl"] == 0.0


def test_hosted_loss_guard_ignores_legacy_cohort_and_cannot_be_disabled(monkeypatch) -> None:
    monkeypatch.setenv("RENDER", "true")
    monkeypatch.setenv("ALPACA_ENV", "paper")
    monkeypatch.setenv("AUTOBOTT_RECENT_LOSS_GUARD_ENABLED", "false")
    legacy = [
        {"underlying": "AAPL", "pnl": -100.0, "outcome_id": "legacy-1", "exit_time": "2026-07-01T14:00:00Z"},
        {"underlying": "AAPL", "pnl": -100.0, "outcome_id": "legacy-2", "exit_time": "2026-07-01T15:00:00Z"},
    ]
    current = [
        _group_leg("current-1", "primary", 40.0, "current-primary", "2026-07-22T14:30:00Z", policy_version="hosted-vix-profit-v1"),
        _group_leg("current-1", "runner", -10.0, "current-runner", "2026-07-22T14:31:00Z", policy_version="hosted-vix-profit-v1"),
    ]

    guard = recent_loss_guard(
        [*legacy, *current],
        policy_version="hosted-vix-profit-v1",
    )

    assert guard["enabled"] is True
    assert guard["blocked_underlyings"] == []


def test_hosted_learning_thresholds_are_code_owned_and_require_minimum_sample(monkeypatch) -> None:
    monkeypatch.setenv("RENDER", "true")
    monkeypatch.setenv("ALPACA_ENV", "paper")
    monkeypatch.setenv("AUTOBOTT_RECENT_LOSS_GUARD_MIN_SAMPLE", "1")
    monkeypatch.setenv("AUTOBOTT_RECENT_LOSS_GUARD_CONSECUTIVE_LOSSES", "1")
    monkeypatch.setenv("AUTOBOTT_RECENT_WINNER_BIAS_MIN_SAMPLE", "1")
    monkeypatch.setenv("AUTOBOTT_RECENT_WINNER_BIAS_CONSECUTIVE_WINS", "1")
    losses = [
        {
            "underlying": "AAPL",
            "pnl": -10.0,
            "outcome_id": f"loss-{index}",
            "policy_version": "hosted-vix-profit-v1",
        }
        for index in range(5)
    ]
    wins = [
        {
            "underlying": "MSFT",
            "pnl": 10.0,
            "outcome_id": f"win-{index}",
            "policy_version": "hosted-vix-profit-v1",
        }
        for index in range(5)
    ]

    assert recent_loss_guard(losses[:4], policy_version="hosted-vix-profit-v1")["blocked_underlyings"] == []
    assert recent_loss_guard(losses, policy_version="hosted-vix-profit-v1")["blocked_underlyings"] == ["AAPL"]
    assert recent_winner_bias(wins[:4], policy_version="hosted-vix-profit-v1")["preferred_underlyings"] == []
    assert recent_winner_bias(wins, policy_version="hosted-vix-profit-v1")["preferred_underlyings"] == ["MSFT"]


def test_volatility_proxies_share_one_loss_and_winner_learning_bucket(monkeypatch) -> None:
    monkeypatch.setenv("AUTOBOTT_RECENT_LOSS_GUARD_MIN_SAMPLE", "3")
    monkeypatch.setenv("AUTOBOTT_RECENT_LOSS_GUARD_CONSECUTIVE_LOSSES", "3")
    monkeypatch.setenv("AUTOBOTT_RECENT_WINNER_BIAS_MIN_SAMPLE", "3")
    loss_rows = [
        {"underlying": symbol, "pnl": -10.0, "outcome_id": f"loss-{symbol}"}
        for symbol in ("VIX", "VXX", "UVXY")
    ]
    win_rows = [
        {"underlying": symbol, "pnl": 10.0, "outcome_id": f"win-{symbol}"}
        for symbol in ("VIX", "VXX", "UVXY")
    ]

    guard = recent_loss_guard(loss_rows)
    bias = recent_winner_bias(win_rows)

    assert guard["blocked_underlyings"] == ["UVXY", "VIX", "VIXW", "VXX"]
    assert guard["reasons"]["VXX"]["learning_bucket"] == "VOLATILITY"
    assert guard["reasons"]["VXX"]["recent_trades"] == 3
    assert bias["preferred_underlyings"] == ["UVXY", "VIX", "VIXW", "VXX"]
    assert bias["reasons"]["UVXY"]["learning_bucket"] == "VOLATILITY"


def test_role_aware_outcomes_do_not_call_runner_move_a_primary_target() -> None:
    primary = "AAPL260814C00105000"
    runner = "AAPL260814C00110000"
    runner_loss = "AAPL260814C00115000"
    orders = [
        _broker_order(primary, "buy", "entry-primary", "2.00", "2026-07-22T14:00:00Z"),
        _broker_order(primary, "sell", "exit-primary", "3.00", "2026-07-22T15:00:00Z"),
        _broker_order(runner, "buy", "entry-runner", "1.00", "2026-07-22T14:00:01Z"),
        _broker_order(runner, "sell", "exit-runner", "1.50", "2026-07-22T15:00:01Z"),
        _broker_order(runner_loss, "buy", "entry-runner-loss", "1.00", "2026-07-22T14:00:02Z"),
        _broker_order(runner_loss, "sell", "exit-runner-loss", "0.70", "2026-07-22T15:00:02Z"),
    ]
    execution_rows = [
        _execution_order_row(primary, "entry-primary", leg_role="primary"),
        _execution_order_row(runner, "entry-runner", leg_role="runner"),
        _execution_order_row(runner_loss, "entry-runner-loss", leg_role="runner"),
    ]

    outcomes = build_trade_outcomes_from_orders(orders, execution_journal_rows=execution_rows)
    by_symbol = {row["symbol"]: row for row in outcomes}

    assert by_symbol[primary]["classification"] == "winner"
    assert "profit_move_captured" in by_symbol[primary]["why"]
    assert by_symbol[runner]["classification"] == "small_winner"
    assert "profit_move_captured" not in by_symbol[runner]["why"]
    assert "profit_target_pct=1.0" in by_symbol[runner]["why"]
    assert by_symbol[runner_loss]["classification"] == "small_loss"
    assert "loss_exceeded_configured_stop_zone" not in by_symbol[runner_loss]["why"]
    assert "stop_loss_pct=0.7" in by_symbol[runner_loss]["why"]


def test_trade_outcome_reader_skips_malformed_jsonl_records(tmp_path) -> None:
    journal_path = tmp_path / "trade_outcomes.jsonl"
    journal_path.write_text(
        "\n".join(
            [
                json.dumps({"outcome_id": "good-1", "pnl": 10.0}),
                '{"outcome_id":"truncated"',
                "[]",
                json.dumps({"outcome_id": "good-2", "pnl": -5.0}),
            ]
        ),
        encoding="utf-8",
    )

    rows = load_trade_outcomes(journal_path=journal_path)

    assert [row["outcome_id"] for row in rows] == ["good-1", "good-2"]


def test_trade_outcome_append_recovers_after_truncated_final_record(tmp_path) -> None:
    journal_path = tmp_path / "trade_outcomes.jsonl"
    journal_path.write_text('{"outcome_id":"truncated"', encoding="utf-8")
    symbol = "AAPL260814C00105000"

    result = record_trade_outcomes_from_orders(
        [
            _broker_order(symbol, "buy", "entry", "2.00", "2026-07-22T14:00:00Z"),
            _broker_order(symbol, "sell", "exit", "3.00", "2026-07-22T15:00:00Z"),
        ],
        journal_path=journal_path,
    )

    assert result["recorded"] == 1
    assert result["summary"]["closed_trades"] == 1
    assert len(load_trade_outcomes(journal_path=journal_path)) == 1


def test_hosted_summary_is_current_policy_but_daily_pnl_is_account_wide(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("RENDER", "true")
    monkeypatch.setenv("ALPACA_ENV", "paper")
    journal_path = tmp_path / "trade_outcomes.jsonl"
    today = datetime.now(tz=ZoneInfo("America/New_York")).replace(hour=14, minute=0, second=0, microsecond=0)
    exit_time = today.astimezone(UTC).isoformat()
    rows = [
        {
            "outcome_id": "legacy-loss",
            "underlying": "AAPL",
            "pnl": -100.0,
            "exit_time": exit_time,
            "policy_version": "legacy-policy",
        },
        {
            "outcome_id": "current-win",
            "underlying": "MSFT",
            "pnl": 40.0,
            "exit_time": exit_time,
            "policy_version": "hosted-vix-profit-v1",
        },
    ]
    journal_path.write_text("".join(f"{json.dumps(row)}\n" for row in rows), encoding="utf-8")

    result = record_trade_outcomes_from_orders([], journal_path=journal_path)

    assert result["summary_policy_version"] == "hosted-vix-profit-v1"
    assert result["summary"]["closed_trades"] == 1
    assert result["summary"]["net_pnl"] == 40.0
    assert result["daily_realized_pnl"] == -60.0


def test_daily_realized_pnl_uses_new_york_trading_date() -> None:
    rows = [
        {"exit_time": "2026-07-23T01:30:00Z", "pnl": 25.0},
        {"exit_time": "2026-07-23T14:00:00Z", "pnl": -10.0},
        {"exit_time": None, "pnl": 500.0},
    ]

    assert daily_realized_pnl(rows, trading_day="2026-07-22") == 25.0
    assert daily_realized_pnl(rows, trading_day="2026-07-23") == -10.0


def test_vix_weekly_outcome_uses_vix_as_underlying() -> None:
    outcomes = build_trade_outcomes_from_orders(
        [
            _broker_order("VIXW260814C00024000", "buy", "entry", "2.00", "2026-07-22T14:00:00Z"),
            _broker_order("VIXW260814C00024000", "sell", "exit", "2.50", "2026-07-22T15:00:00Z"),
        ]
    )

    assert outcomes[0]["underlying"] == "VIX"
    assert outcomes[0]["root_symbol"] == "VIXW"


def test_canceled_partial_fill_is_terminal_and_counted_once() -> None:
    symbol = "AAPL260814C00105000"

    outcomes = build_trade_outcomes_from_orders(
        [
            _broker_order(symbol, "buy", "entry", "2.00", "2026-07-22T14:00:00Z"),
            _broker_order(
                symbol,
                "sell",
                "partial-exit",
                "2.50",
                "2026-07-22T15:00:00Z",
                qty="2",
                filled_qty="1",
                status="canceled",
            ),
        ]
    )

    assert len(outcomes) == 1
    assert outcomes[0]["qty"] == 1.0
    assert outcomes[0]["pnl"] == 50.0


def test_replaced_partial_exit_fill_is_terminal_and_counted() -> None:
    symbol = "AAPL260814C00105000"

    outcomes = build_trade_outcomes_from_orders(
        [
            _broker_order(symbol, "buy", "entry", "2.00", "2026-07-22T14:00:00Z"),
            _broker_order(
                symbol,
                "sell",
                "replaced-exit",
                "2.40",
                "2026-07-22T15:00:00Z",
                qty="2",
                filled_qty="1",
                status="replaced",
            ),
        ]
    )

    assert len(outcomes) == 1
    assert outcomes[0]["pnl"] == 40.0


def test_done_for_day_partial_exit_fill_is_terminal_and_counted() -> None:
    symbol = "AAPL260814C00105000"

    outcomes = build_trade_outcomes_from_orders(
        [
            _broker_order(symbol, "buy", "entry", "2.00", "2026-07-22T14:00:00Z"),
            _broker_order(
                symbol,
                "sell",
                "day-end-exit",
                "1.25",
                "2026-07-22T20:00:00Z",
                qty="2",
                filled_qty="1",
                status="done_for_day",
            ),
        ]
    )

    assert len(outcomes) == 1
    assert outcomes[0]["pnl"] == -75.0


def test_sync_fails_closed_when_nonpaginated_history_hits_limit(tmp_path) -> None:
    class TruncatedBroker:
        def list_orders(self, *, status="all", limit=200, direction="desc"):
            return [
                {
                    "id": f"order-{index}",
                    "symbol": "AAPL260814C00105000",
                    "side": "buy",
                    "status": "filled",
                    "filled_qty": "1",
                    "filled_avg_price": "1.00",
                    "filled_at": f"2026-07-22T14:{index % 60:02d}:00Z",
                }
                for index in range(limit)
            ]

    result = sync_trade_outcomes_from_broker(
        TruncatedBroker(),
        journal_path=tmp_path / "trade_outcomes.jsonl",
        limit=200,
    )

    assert result["ok"] is False
    assert result["history_complete"] is False
    assert result["error"] == "broker_order_history_truncated"


def test_sync_fails_closed_on_unmatched_terminal_sell(tmp_path) -> None:
    class UnmatchedSellBroker:
        def list_orders(self, *, status="all", limit=200, direction="desc"):
            return [_broker_order("AAPL260814C00105000", "sell", "exit", "2.00", "2026-07-22T15:00:00Z")]

    result = sync_trade_outcomes_from_broker(
        UnmatchedSellBroker(),
        journal_path=tmp_path / "trade_outcomes.jsonl",
    )

    assert result["ok"] is False
    assert result["history_complete"] is False
    assert "broker_order_history_unmatched_sells:AAPL260814C00105000" == result["error"]


def _broker_order(
    symbol: str,
    side: str,
    order_id: str,
    price: str,
    filled_at: str,
    *,
    qty: str = "1",
    filled_qty: str | None = None,
    status: str = "filled",
) -> dict:
    return {
        "id": order_id,
        "client_order_id": f"client-{order_id}",
        "symbol": symbol,
        "side": side,
        "qty": qty,
        "filled_qty": filled_qty if filled_qty is not None else qty,
        "filled_avg_price": price,
        "status": status,
        "submitted_at": filled_at,
        "filled_at": filled_at,
    }


def _execution_order_row(
    symbol: str,
    order_id: str,
    *,
    leg_role: str | None = None,
    exit_reason: str | None = None,
) -> dict:
    metadata = {
        "trade_group_id": "core-runner:decision-123" if leg_role else None,
        "leg_role": leg_role,
        "trade_setup": "bullish_continuation",
        "execution_layer": "tactical",
        "confidence_score": 0.81,
        "policy_version": "policy-v1",
        "build_sha": "abc123",
        "exit_reason": exit_reason,
    }
    return {
        "event_type": "order_submission",
        "decision_id": "decision-123" if leg_role else f"monitor-{symbol}",
        "thesis_id": "AAPL:bullish_continuation:tactical",
        "payload": {
            "broker_order_id": order_id,
            "client_order_id": f"client-{order_id}",
            "intent": {
                "option_symbol": symbol,
                "decision_id": "decision-123" if leg_role else f"monitor-{symbol}",
                "thesis_id": "AAPL:bullish_continuation:tactical",
                "metadata": metadata,
            },
        },
    }


def _group_leg(
    group_id: str,
    role: str,
    pnl: float,
    outcome_id: str,
    exit_time: str,
    *,
    policy_version: str | None = None,
) -> dict:
    return {
        "outcome_id": outcome_id,
        "underlying": "AAPL",
        "symbol": f"AAPL-{role}",
        "trade_group_id": group_id,
        "leg_role": role,
        "entry_broker_order_id": f"entry-{outcome_id}",
        "entry_order_filled_qty": 1.0,
        "entry_price": 1.0,
        "entry_time": "2026-07-09T14:00:00Z",
        "exit_time": exit_time,
        "qty": 1.0,
        "pnl": pnl,
        "policy_version": policy_version,
    }
