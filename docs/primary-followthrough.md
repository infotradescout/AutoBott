# Persistent primary-option follow-through

## Why this exists

A study cannot infer a bid-price path from order fills, trade-price bars, an underlying chart, or an exit. `primary_followthrough.py` records the exact admitted primary's future bid/ask observations without consulting open positions. Manual exit therefore does not remove an observation watch. Persisted watches survive a process restart; absent observations during downtime remain absent.

## Registration, collection, and export

Set `AUTOBOTT_PRIMARY_OBSERVATION_SECONDS` to an explicit collection window (positive integer, at most 86400). Unset means disabled. The setting is a retention/collection horizon, not a profit target or a strategy setting. For actual-fill studies, the window must cover the desired holding horizon **plus** possible fill delay after admission.

Successful final entry admission registers a hash-bound snapshot and its already fetched `recorded_refresh` capsule before the first broker submission. That is **not a broker fill**. Orders can fail or remain pending afterward. Stored cases have an empty fills array and explicit `not_collected_admission_is_not_a_fill` provenance until genuine matching fill records are supplied to the study. No fill time or fill price is fabricated.

At cycle completion, active primary watches are queried in a bounded batch through the existing read-only option-quote client. The actual request completion time and unchanged source quote timestamps are persisted. Missing/invalid responses are recorded as data issues with an empty quote observation, not silently discarded or converted into a favorable price. Feed labels remain explicit. Only the primary is followed; a runner cannot mask its failure.

The recorder is cycle-driven: its 15-second minimum request spacing is a lower bound, **not a promise of 15-second sampling**. Actual observations depend on session cadence, data latency and process availability. No background assistant task or new autonomous trading session is created. An observation at or after the window end closes the watch, but cannot manufacture earlier coverage. The quality evaluator still enforces its own quote freshness, missing-data, drawdown and persistence rules.

The pre-submission registration point means this is an admitted-only cohort. Candidates stopped by earlier accounting/risk controls are not included. This recorder does not repair historical account scope, disable `daily_pnl_unavailable`, or convert blocked candidates into actual trades. A separate explicitly labeled shadow-scan study is needed to evaluate that broader cohort.

Call `export_primary_observations(root, source_kind="recorded_market")` from an authorized local/server process to construct an admitted-only study tape. The export includes open, closed and limited watches; none are silently filtered to manufacture success. Synthetic tests must use `source_kind="synthetic"`. No new public or unauthenticated data endpoint is added.

## Data and safety bounds

Files live under the existing artifacts root in a separate `primary_followthrough` directory. Hash-based file names prevent sample IDs from becoming paths. Atomic replacement, fsync, and an exclusive lock avoid partial file writes. The recorder never edits the position store, account journals, trading controls, source snapshots, exits or broker orders. Caps apply to active watches, observations per case, bytes per case and total store bytes. Exceeding a limit is an explicit telemetry error; there is no silent historical deletion. A stale lock after an interrupted process requires operator inspection, not automatic unverified removal.

Collection failures are visible in cycle telemetry but do not weaken entry admission, disable protective exits or rewrite broker permissions. Changing an existing watch's observation rules is rejected. Completed watches are not queried again. No successful return from the recorder asserts a profitable entry or an executable fill.

## Validation scope

Synthetic integration tests use the actual v2 entry path and study evaluator. They cover persistence across reconstructed process state, continuation independent of position status, missing quotes, bounded storage, no fabricated fill, primary-only requests, expired watches, retry idempotence, and retention of existing entry rejection behavior. Software test success is not a current-market profitability result.

## Release status at implementation

This addition is in the tested candidate branch, not an assertion that the production paper service already runs it. The production service still requires independent review and a verified main release. The separate no-order hosted validation service can exercise startup and the full guarded suite without enabling its session or broker permissions.
