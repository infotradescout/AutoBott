"""Synthetic broker-to-journal/timeline regressions; never contact an account."""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import UTC, datetime, timedelta
import json
from types import SimpleNamespace

import pytest

from autobott_v2.dashboard_app import _timeline_round_trips
from autobott_v2.trade_outcomes import (
    build_trade_outcomes_from_orders,
    load_trade_outcomes,
    match_broker_order_lots,
    record_trade_outcomes_from_orders,
    sync_trade_outcomes_from_broker,
)


SYMBOL = "TEST260918C00100000"
SCOPE = "alpaca:paper:synthetic-account"


def order(identifier, side, price, *, qty=1, filled_qty=None, minute=0, status="filled"):
    stamp = (datetime(2026, 9, 11, 14, tzinfo=UTC) + timedelta(minutes=minute)).isoformat()
    return {
        "id": identifier, "symbol": SYMBOL, "side": side,
        "qty": str(qty), "filled_qty": str(qty if filled_qty is None else filled_qty),
        "filled_avg_price": str(price), "status": status,
        "submitted_at": stamp, "filled_at": stamp,
    }


class Broker:
    config = SimpleNamespace(environment="paper")

    def __init__(self, orders, account_id="synthetic-account"):
        self.orders = orders
        self.account_id = account_id
        self.reads = []

    def get_account(self):
        self.reads.append("account")
        return {"id": self.account_id}

    def list_orders(self, **kwargs):
        self.reads.append("orders")
        return deepcopy(self.orders)

    def submit_order(self, *args, **kwargs):
        raise AssertionError("accounting_must_never_submit_an_order")


def sync(broker, path):
    return sync_trade_outcomes_from_broker(broker, journal_path=path, trading_day="2026-09-11")


@pytest.mark.parametrize("orders,expected_pnls,expected_pending", [
    ([order("buy", "buy", 2, qty=2), order("sell-1", "sell", 2.5, minute=1),
      order("sell-2", "sell", 1.5, minute=2)], [50.0, -50.0], 0),
    ([order("buy-1", "buy", 2), order("buy-2", "buy", 1, minute=1),
      order("sell", "sell", 3, qty=2, minute=2)], [100.0, 200.0], 0),
    ([order("buy", "buy", 2), order("sell", "sell", 1.5, qty=2, filled_qty=1,
                                     minute=1, status="canceled")], [-50.0], 0),
    ([order("buy", "buy", 2, qty=3), order("sell", "sell", 2.5, minute=1)], [50.0], 1),
])
def test_actual_quantities_own_journal_and_timeline(tmp_path, orders, expected_pnls, expected_pending):
    path = tmp_path / "outcomes.jsonl"
    broker = Broker(orders)
    result = sync(broker, path)
    trips, pending = _timeline_round_trips(orders)
    assert result["ok"] is True
    assert [row["pnl"] for row in trips] == expected_pnls
    assert [row["pnl"] for row in load_trade_outcomes(journal_path=path)] == expected_pnls
    assert result["daily_realized_pnl"] == sum(expected_pnls)
    assert len(pending) == expected_pending
    if expected_pending:
        assert pending[0]["remaining_filled_qty"] == 2
    assert broker.reads == ["orders", "account"]
    assert result["accounting_complete"] is False


def test_unmatched_exit_is_unavailable_and_prevents_current_day_append(tmp_path):
    orders = [order("buy", "buy", 2), order("sell", "sell", 3, qty=2, minute=1)]
    path = tmp_path / "outcomes.jsonl"
    result = sync(Broker(orders), path)
    trips, _ = _timeline_round_trips(orders)
    assert result["ok"] is False
    assert result["daily_pnl_complete"] is False
    assert result["recorded"] == 0
    assert not path.exists()
    assert trips[-1]["classification"] == "unmatched_sell"
    assert trips[-1]["qty"] == 1
    assert trips[-1]["pnl"] is None


def test_restart_microsecond_replay_preserves_bytes(tmp_path):
    orders = [order("buy", "buy", 2), order("sell", "sell", 3, minute=1)]
    path = tmp_path / "outcomes.jsonl"
    assert sync(Broker(orders), path)["recorded"] == 1
    before = path.read_bytes()
    replay = deepcopy(orders)
    replay[1]["filled_at"] = "2026-09-11T14:01:00.000001+00:00"
    result = sync(Broker(replay), path)
    assert result["ok"] is True
    assert result["recorded"] == 0
    assert result["daily_realized_pnl"] == 100
    assert path.read_bytes() == before


def test_legacy_duplicate_is_preserved_but_cannot_inflate_broker_pnl(tmp_path):
    orders = [order("buy", "buy", 2), order("sell", "sell", 3, minute=1)]
    legacy = build_trade_outcomes_from_orders(orders)[0]
    path = tmp_path / "outcomes.jsonl"
    path.write_text(json.dumps(legacy) + "\n" + json.dumps(dict(legacy, outcome_id="old-duplicate")) + "\n")
    before = path.read_bytes()
    result = sync(Broker(orders), path)
    assert result["ok"] is False
    assert result["recorded"] == 0
    assert result["daily_realized_pnl"] == 100
    assert result["journal_diagnostics"]["daily_realized_pnl"] == 200
    assert len(result["reconciliation"]["historical_duplicates"]) == 1
    assert path.read_bytes() == before


def test_verified_legacy_migration_snapshots_and_preserves_prefix(tmp_path):
    first = [order("buy-1", "buy", 2), order("sell-1", "sell", 3, minute=1)]
    legacy = build_trade_outcomes_from_orders(first)[0]
    path = tmp_path / "outcomes.jsonl"
    path.write_text(json.dumps(legacy) + "\n")
    before = path.read_bytes()
    orders = [*first, order("buy-2", "buy", 2, minute=2), order("sell-2", "sell", 1, minute=3)]
    result = sync(Broker(orders), path)
    assert result["ok"] is True
    assert result["recorded"] == 1
    assert path.read_bytes().startswith(before)
    snapshot = next(path.parent.glob(path.name + ".pre-identity-v2-*.jsonl"))
    assert snapshot.read_bytes() == before
    assert len(load_trade_outcomes(journal_path=path)) == 2
    assert result["journal_history_rewritten"] is False


def test_snapshot_failure_cannot_append_new_financial_history(tmp_path, monkeypatch):
    from autobott_v2 import trade_outcomes

    first = [order("buy-1", "buy", 2), order("sell-1", "sell", 3, minute=1)]
    path = tmp_path / "outcomes.jsonl"
    path.write_text(json.dumps(build_trade_outcomes_from_orders(first)[0]) + "\n")
    before = path.read_bytes()
    orders = [*first, order("buy-2", "buy", 2, minute=2), order("sell-2", "sell", 1, minute=3)]

    def unavailable(*args):
        raise OSError("synthetic_storage_failure")

    monkeypatch.setattr(trade_outcomes.os, "link", unavailable)
    with pytest.raises(OSError, match="synthetic_storage_failure"):
        sync(Broker(orders), path)
    assert path.read_bytes() == before
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.parametrize("scope", [None, "alpaca:paper:another-account"])
def test_unowned_historical_rows_block_without_changing_them(tmp_path, scope):
    past = [order("old-buy", "buy", 2), order("old-sell", "sell", 3, minute=1)]
    legacy = dict(build_trade_outcomes_from_orders(past)[0], account_scope=scope)
    path = tmp_path / "outcomes.jsonl"
    path.write_text(json.dumps(legacy) + "\n")
    before = path.read_bytes()
    orders = [order("buy", "buy", 2), order("sell", "sell", 3, minute=1)]
    result = sync(Broker(orders), path)
    assert result["ok"] is False
    assert result["recorded"] == 0
    assert result["reconciliation"]["unresolved"]
    assert path.read_bytes() == before


def test_missing_authenticated_account_cannot_create_journal(tmp_path):
    path = tmp_path / "outcomes.jsonl"
    orders = [order("buy", "buy", 2), order("sell", "sell", 3, minute=1)]
    result = sync(Broker(orders, account_id=""), path)
    assert result["ok"] is False
    assert result["error"] == "verified_broker_account_required"
    assert not path.exists()


def test_read_only_view_does_not_create_or_repair_any_journal(tmp_path):
    path = tmp_path / "outcomes.jsonl"
    orders = [order("buy", "buy", 2), order("sell", "sell", 3, minute=1)]
    result = record_trade_outcomes_from_orders(orders, journal_path=path, persist=False, trading_day="2026-09-11")
    assert result["daily_realized_pnl"] == 100
    assert result["recorded"] == 0
    assert list(tmp_path.iterdir()) == []


def test_concurrent_runtime_replays_append_exactly_once(tmp_path):
    path = tmp_path / "outcomes.jsonl"
    orders = [order("buy", "buy", 2), order("sell", "sell", 3, minute=1)]
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(lambda _: sync(Broker(orders), path), range(12)))
    assert all(result["ok"] for result in results)
    assert sum(result["recorded"] for result in results) == 1
    assert len(load_trade_outcomes(journal_path=path)) == 1


def test_duplicate_broker_snapshot_is_not_a_second_lot():
    buy = order("buy", "buy", 2)
    sell = order("sell", "sell", 3, minute=1)
    matched = match_broker_order_lots([buy, buy, sell, sell])
    assert [row["pnl"] for row in matched["outcomes"]] == [100]
    assert matched["pending"] == []
    with pytest.raises(ValueError, match="conflicting_broker_order_snapshot"):
        match_broker_order_lots([buy, dict(buy, filled_qty="2"), sell])


def test_nonterminal_entry_never_creates_permanent_outcome(tmp_path):
    orders = [order("buy", "buy", 2, qty=2, filled_qty=1, status="partially_filled"),
              order("sell", "sell", 1.5, minute=1)]
    path = tmp_path / "outcomes.jsonl"
    result = sync(Broker(orders), path)
    assert result["recorded"] == 0
    assert result["daily_realized_pnl"] == -50
    assert result["broker_outcomes"][0]["provisional_fill"] is True
    assert not path.exists()


def test_partial_entry_is_allocated_before_later_terminal_entry(tmp_path):
    orders = [
        order("buy-a", "buy", 2, qty=2, filled_qty=1, status="partially_filled"),
        order("buy-b", "buy", 5, minute=1),
        order("sell", "sell", 3, minute=2),
    ]
    path = tmp_path / "outcomes.jsonl"
    result = sync(Broker(orders), path)
    assert result["daily_realized_pnl"] == 100
    assert result["recorded"] == 0
    assert result["broker_outcomes"][0]["entry_broker_order_id"] == "buy-a"
    assert result["broker_outcomes"][0]["provisional_fill"] is True
    assert not path.exists()
    orders[0]["status"] = "canceled"
    final = sync(Broker(orders), path)
    assert final["recorded"] == 1
    assert final["outcomes"][0]["entry_broker_order_id"] == "buy-a"
    assert final["outcomes"][0]["pnl"] == 100


def test_partial_exit_consumes_first_lot_before_later_terminal_exit(tmp_path):
    orders = [
        order("buy-a", "buy", 2), order("buy-b", "buy", 5, minute=1),
        order("sell-a", "sell", 3, qty=2, filled_qty=1, minute=2, status="partially_filled"),
        order("sell-b", "sell", 4, minute=3),
    ]
    path = tmp_path / "outcomes.jsonl"
    result = sync(Broker(orders), path)
    assert result["daily_realized_pnl"] == 0
    assert result["recorded"] == 1
    assert result["outcomes"][0]["entry_broker_order_id"] == "buy-b"
    assert result["outcomes"][0]["exit_broker_order_id"] == "sell-b"
    assert result["outcomes"][0]["pnl"] == -100
    orders[2]["status"] = "canceled"
    final = sync(Broker(orders), path)
    assert final["ok"] is True
    assert final["recorded"] == 1
    assert sum(row["pnl"] for row in load_trade_outcomes(journal_path=path)) == 0


@pytest.mark.parametrize("invalid", [
    {"filled_qty": "nan"}, {"filled_qty": "invalid"}, {"filled_qty": "-1"},
    {"filled_avg_price": "inf"}, {"filled_avg_price": None},
    {"contract_multiplier": 10}, {"asset_class": "us_equity"},
])
def test_invalid_or_unsupported_fills_cannot_be_silently_recorded(tmp_path, invalid):
    path = tmp_path / "outcomes.jsonl"
    orders = [dict(order("buy", "buy", 2), **invalid), order("sell", "sell", 3, minute=1)]
    with pytest.raises(ValueError):
        sync(Broker(orders), path)
    assert not path.exists()
