# Entry runtime routing and automatic primary fill capture - 2026-09-17

Baseline: `0effe38d16dd8fddd50eef59191a67a5eef05ced`, existing PR #44 lane.

## Entry routing repaired

The dashboard single-cycle action imported the legacy engine while scheduled
sessions used v2. Three guarded regressions failed on that baseline. Both
dashboard interfaces now dispatch through the same v2 cycle adapter as sessions.
The adapter retains private per-call engine bindings; no shared globals are
replaced. Authentication still runs before a cycle. The canonical monitor and
exit implementations remain unchanged. Historical replay's explicit legacy
engine choice is not removed.

## Automatic admitted primary-fill evidence

When the existing `AUTOBOTT_PRIMARY_OBSERVATION_SECONDS` opt-in is configured,
the cycle verifies the paper account before admitting new watches. Actual returned
primary submissions are attached to the corresponding watch with exact decision,
snapshot, account, contract, broker-order and client-order identities. The
submission receipt is not a fill. Single-leg quantities other than one remain
unsupported for this study rather than being silently resized.

A bounded cycle-completion collector reads only each recorded order ID. It
requires the exact HTTPS paper endpoint and verifies account identity before
and after the read batch. Wrong-account watches are not queried. Matching complete
one-contract fills flow through the existing strict linker, using the broker
observation's actual fill price/time. Other metadata is whitelisted. Pending
observations remain pending; partial/canceled/conflicting data is not invented
into a valid fill. Observation history is retained within the existing per-watch
and store bounds. Terminal fills are not repeatedly queried.

This replaces the earlier "collector not implemented" checkpoint: runtime
receipt capture and read-only order capture are now implemented and integrated.
It does NOT establish that collection has run on the production paper account.
Existing unlinked history is not backfilled by guessing symbol/time ownership.
Only evidence-store files are changed by the collector; execution/accounting
journals are not rewritten. Source-file consistency is not a broker signature.

## Exits, interruption and failure isolation

Optional receipt callbacks cannot turn accepted orders into submission failures.
The primary's receipt is retained on a known partial-pair submission failure;
the existing cancellation/flattening compensation remains responsible for risk
reduction. Its source body is unchanged. Collector failures are explicit in
cycle outcomes and cannot block monitoring or alter entry permissions.

Fill polling and quote follow-through do not depend on an open position, so
manual exits and restarting with the same evidence store do not erase the path.
Polling still uses cycle completion, NOT a guaranteed 15-second scheduler.
No observation window is extended to manufacture missing quotes. Sparse paths,
late fills, source/proxy limitations, and unlinked cohorts stay unscorable.

## Verification and release

Tests use synthetic brokers/quotes only. Coverage includes the actual cycle ->
admission -> primary submission -> broker fill -> complete quote-path -> linked
study chain, routing/authentication, scope changes, delayed/partial fills,
restart/closed-window behavior, exact IDs, disk/provider failures, bounded
history, and unchanged compensating exits. The complete-chain targeted run
passed 104 tests before final defensive cases were added. Exact full-suite
counts and committed-source identity are recorded in the PR checkpoint.

No production order permissions, account journals, risk limits, profit targets,
holding/drawdown rules, or observation settings were changed. No real broker
orders were submitted for validation. Required Gemini review, protected runtime
acceptance and accepted paper release remain outstanding. Then collect genuine
linked primary-fill paths under the fixed study protocol. Software correctness
is not a demonstrated profitable entry advantage: `entry_advantage_established=false`.
