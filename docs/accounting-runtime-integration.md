# Accounting integration

The canonical owner is `trade_outcomes.py`. The trading loop and authenticated
timeline routes now use its quantity-aware broker-order matcher. Partial exits
retain the remaining entry quantity, one sale can consume several entry lots,
and canceled orders retain their actual filled quantity. Unmatched exits have
unavailable P/L. The timeline GET routes do not append or repair journal data.

Runtime ingestion obtains account identity from the same authenticated broker's
account endpoint. Within the journal transaction, the existing append planner
checks stable entry/exit order identity, quantity caps, replay, economics and
attribution. An unscoped historical row must also have its entry/exit pair in
the account's current broker history. Unresolved ownership, duplicate history,
changed economics or malformed JSONL blocks the append; history is never
rewritten or retention-compacted. A complete, content-verified snapshot is
published before appending to an existing journal. Snapshot failure prevents
the append.

Broker-derived current P/L and learning summaries are computed separately from
historical journal diagnostics. Historical duplicates cannot inflate current
P/L. Incomplete current-day broker history, failed account identity, or a
blocked journal prevents new entries. Position monitoring runs before this
entry gate, and risk-reducing exits remain available. No code path in this
change submits an order as part of accounting or toggles a trading control.

`accounting_complete` remains false. Order-level gross premium P/L does not
establish fee reconciliation, activity-level intraday allocation, or coverage
of the complete account lifetime. The authenticated timeline uses its existing
bounded order window. Legacy rows that cannot be matched to the current broker
window still require reconciliation before runtime entries can resume. This
change provides a separate broker-derived view and conflict map; it does not
rewrite those rows or approve their correction.

The journal transaction is protected against concurrent callers in the hosted
process. Run one ingestion process per journal; independent operating-system
processes sharing a journal are not supported by the existing runtime model.

## Verification

Run the complete combined branch with broker credentials removed and external
socket/process operations denied:

```sh
python scripts/validate_offline.py
node --test tests/cockpit_state.test.cjs
```

`tests/test_outcome_runtime_integration.py` exercises broker-to-journal and
timeline results using synthetic fills, including partial-close P/L of $0,
multiple-lot P/L of $300, canceled-fill P/L of -$50, replay, account separation,
legacy duplication, atomic snapshot failure and concurrent runtime calls.
`tests/test_trading_cycle.py` proves an actual guarded legacy-journal failure
blocks synthetic new entries while a synthetic risk-reducing exit still runs.
`tests/test_dashboard_app.py` checks both authenticated timeline aliases with
the real matcher and proves every existing runtime file remains unchanged.

These checks do not establish production-account reconciliation or hosted
operation of this revision. The existing stop-switch, authentication, command
ordering and cockpit tests remain part of the combined release validation.
