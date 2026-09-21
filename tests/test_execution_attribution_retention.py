"""Regression for recovery/full-history vs runtime/tail attribution drift.

Synthetic fills only. These tests never call a broker or change controls.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from hashlib import sha256
import json

import pytest

from autobott_v2 import execution_journal as journal
from autobott_v2.hosted_policy import HOSTED_POLICY_VERSION
from autobott_v2.jsonl_retention import compact_jsonl_tail, read_jsonl_tail
from autobott_v2.outcome_ingestion import plan_outcome_append
from autobott_v2.trade_outcomes import match_broker_order_lots, record_trade_outcomes_from_orders


PROTECTED = frozenset({"order_submission"})
SYMBOL = "SPY260925C00600000"
SCOPE = "alpaca:paper:synthetic-retention-account"


def receipt(order_id="fixture-entry", build_sha="a" * 40):
    return {
        "event_type": "order_submission",
        "decision_id": "fixture-decision",
        "thesis_id": "fixture-thesis",
        "payload": {
            "broker_order_id": order_id,
            "intent": {
                "option_symbol": SYMBOL,
                "metadata": {
                    "policy_version": HOSTED_POLICY_VERSION,
                    "build_sha": build_sha,
                    "leg_role": "primary",
                    "trade_group_id": "fixture-group",
                },
            },
        },
    }


def fills():
    return [
        {"id": "fixture-entry", "symbol": SYMBOL, "side": "buy", "status": "filled",
         "qty": "1", "filled_qty": "1", "filled_avg_price": "1.00",
         "filled_at": "2026-09-16T13:52:31Z"},
        {"id": "fixture-exit", "symbol": SYMBOL, "side": "sell", "status": "filled",
         "qty": "1", "filled_qty": "1", "filled_avg_price": "0.86",
         "filled_at": "2026-09-21T13:35:47Z"},
    ]


def encoded(row):
    return (json.dumps(row, sort_keys=True) + "\n").encode()


def seeded_journals(tmp_path):
    execution_path = tmp_path / "execution_orders.jsonl"
    outcome_path = tmp_path / "trade_outcomes.jsonl"
    # Deliberately exceed the runtime's existing 16 MiB activity window while
    # staying below the generic journal's 64 MiB compaction threshold.
    with execution_path.open("wb") as stream:
        stream.write(encoded(receipt()))
        noise = encoded({"event_type": "execution_outcome", "detail": "x" * 65536})
        for _ in range(270):
            stream.write(noise)
        stream.write(encoded({"event_type": "execution_outcome", "detail": "latest"}))
    complete = journal.load_execution_journal(journal_path=execution_path)
    matched = match_broker_order_lots(fills(), execution_journal_rows=complete)
    canonical = plan_outcome_append([], matched["outcomes"], account_scope=SCOPE)
    assert canonical["append_safe"]
    assert canonical["new_rows"][0]["decision_id"] == "fixture-decision"
    outcome_path.write_bytes(b"".join(encoded(row) for row in canonical["new_rows"]))
    return execution_path, outcome_path


def test_recovery_attribution_survives_actual_runtime_16mib_read(tmp_path):
    execution_path, outcome_path = seeded_journals(tmp_path)
    before = sha256(outcome_path.read_bytes()).hexdigest()
    execution_before = sha256(execution_path.read_bytes()).hexdigest()
    result = record_trade_outcomes_from_orders(
        fills(), journal_path=outcome_path, execution_journal_path=execution_path,
        account_scope=SCOPE, trading_day="2026-09-21",
    )
    assert result["ok"], result["reconciliation"]
    assert result["identity_check_complete"]
    assert result["recorded"] == 0
    assert result["daily_realized_pnl"] == -14.0
    assert result["broker_outcomes"][0]["decision_id"] == "fixture-decision"
    assert result["broker_outcomes"][0]["build_sha"] == "a" * 40
    assert sha256(outcome_path.read_bytes()).hexdigest() == before
    assert sha256(execution_path.read_bytes()).hexdigest() == execution_before


@pytest.mark.parametrize("conflict", ["economics", "attribution"])
def test_real_conflicts_still_block_and_preserve_active_history(tmp_path, conflict):
    execution_path, outcome_path = seeded_journals(tmp_path)
    before = outcome_path.read_bytes()
    orders = fills()
    if conflict == "economics":
        orders[-1]["filled_avg_price"] = "0.90"
    else:
        with execution_path.open("ab") as stream:
            stream.write(encoded(receipt(build_sha="b" * 40)))
    result = record_trade_outcomes_from_orders(
        orders, journal_path=outcome_path, execution_journal_path=execution_path,
        account_scope=SCOPE, trading_day="2026-09-21",
    )
    assert not result["ok"]
    assert result["error"] == "outcome_journal_reconciliation_required"
    assert result["reconciliation"]["conflicts"]
    assert result["recorded"] == 0
    assert outcome_path.read_bytes() == before


def test_bounded_read_retains_receipts_without_old_activity_or_writes(tmp_path):
    path = tmp_path / "events.jsonl"
    original = encoded(receipt()) + b"".join(
        encoded({"event_type": "execution_outcome", "index": i, "padding": "x" * 100})
        for i in range(20)
    )
    path.write_bytes(original)
    rows = journal.load_execution_journal(journal_path=path, max_tail_bytes=400)
    assert rows[0] == receipt()
    assert sum(r.get("event_type") == "order_submission" for r in rows) == 1
    assert rows[-1]["index"] == 19
    assert not any(r.get("index") == 0 for r in rows)
    assert path.read_bytes() == original


@pytest.mark.parametrize("cut", [0, 1, -1])
def test_receipt_on_or_crossing_tail_boundary_is_retained_once(tmp_path, cut):
    path = tmp_path / "events.jsonl"
    prefix = encoded({"event_type": "noise", "padding": "z" * 100})
    body = encoded(receipt())
    suffix = encoded({"event_type": "noise", "padding": "y" * 100})
    path.write_bytes(prefix + body + suffix)
    start = len(prefix) + cut
    rows = [json.loads(r) for r in read_jsonl_tail(
        path, max_tail_bytes=path.stat().st_size - start, preserve_event_types=PROTECTED,
    )]
    assert sum(r.get("event_type") == "order_submission" for r in rows) == 1
    assert next(r for r in rows if r.get("event_type") == "order_submission") == receipt()


def test_compaction_retains_receipt_bytes_and_order_not_old_activity(tmp_path):
    path = tmp_path / "events.jsonl"
    first = encoded(receipt("first"))
    second = encoded(receipt("second"))
    noise = b"".join(encoded({"event_type": "noise", "index": i, "padding": "z" * 100}) for i in range(30))
    path.write_bytes(first + noise + second)
    assert compact_jsonl_tail(path, max_bytes=1000, retain_bytes=500, preserve_event_types=PROTECTED)
    data = path.read_bytes()
    assert first in data and second in data
    rows = journal.load_execution_journal(journal_path=path, max_tail_bytes=100)
    assert [r["payload"]["broker_order_id"] for r in rows if r.get("event_type") == "order_submission"] == ["first", "second"]
    assert not any(r.get("index") == 0 for r in rows)


def test_malformed_prefix_does_not_hide_a_valid_receipt(tmp_path):
    path = tmp_path / "events.jsonl"
    path.write_bytes(b'not-json\n[]\n{"event_type":[]}\n\xff\n' + encoded(receipt()) + encoded({"event_type": "noise", "padding": "z" * 1000}))
    rows = journal.load_execution_journal(journal_path=path, max_tail_bytes=100)
    assert rows == [receipt()]


def test_append_uses_receipt_preservation_under_concurrent_compaction(tmp_path, monkeypatch):
    path = tmp_path / "events.jsonl"
    original_compact = journal.compact_jsonl_tail
    def small_compact(target, **kwargs):
        assert kwargs["preserve_event_types"] == PROTECTED
        return original_compact(target, max_bytes=1000, retain_bytes=500, **kwargs)
    monkeypatch.setattr(journal, "compact_jsonl_tail", small_compact)
    def append(index):
        record = journal.ExecutionJournalRecord(
            datetime.now(UTC), "order_submission", str(index), None,
            {"broker_order_id": str(index), "padding": "x" * 100},
        )
        journal._append_record(record, journal_path=path)
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(append, range(32)))
    rows = journal.load_execution_journal(journal_path=path, max_tail_bytes=100)
    assert len(rows) == 32
    assert {r["payload"]["broker_order_id"] for r in rows} == {str(i) for i in range(32)}
