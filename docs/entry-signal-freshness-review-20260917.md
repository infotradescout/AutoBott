# Entry signal freshness review - 2026-09-17

## Reproduced release defect

Reviewed candidate: `d27996cc302cd2abc845e1cf6b7e169bb7033352`, PR #44.
The admission path proved bars were completed, ordered and not from the future,
but did not bound the age of the latest completed signal bar.
Fresh stock/option quotes and a fresh decision timestamp could therefore admit
an old underlying, SPY, QQQ or volatility-proxy signal.

Ten guarded synthetic integration cases shifted all bars, or each series alone,
back seven days. Both legacy and v2 cycle shells still submitted two fake orders
per case. All ten rejection assertions failed on the original candidate. No real
broker, account or network was involved. This establishes a logic defect, not
that this defect caused any particular historical trade or loss.

## Repair and fixed operational rule

For every required bar series, latest interval close must be no older than one
configured fixed bar interval plus `max_bar_publication_delay_seconds` (default
30 seconds). This is an operational freshness bound, not an optimized profit,
drawdown or holding-period setting. A 1Min stream permits at most 90 seconds;
a 1Hour stream permits at most 3,630 seconds since the latest interval close.
The exact boundary is inclusive. Invalid publication-delay values are rejected.

The check runs before quote refresh and again using the actual refresh receipt
time. Provider latency cannot carry an expiring signal over the deadline.
Rejections use `entry_stale_completed_bar` and identify the affected series.

Older lookback bars remain valid history when the latest bar is fresh. No source
snapshot or journal is rewritten. No overnight/weekend/session-open exemption
is invented: new entries wait for fresh required bars. This is conservative and
may suppress entries when the provider has not supplied recent observations;
it is not an exchange-calendar or complete missing-bar detector.

The study caller passes its recorded timestamp and fixed admission protocol to
the same helper. It does not consult current wall time or future quote paths.
Admission evidence includes each series' age and the actual applied age limit.

## Validation and boundaries

The targeted guarded suite passes 144 tests, including 27 new freshness tests.
Coverage includes each stale series in both cycle shells, exact minute/hour
boundaries, quote-refresh latency, invalid delay settings, replay parity,
timezone equivalence at DST transitions, and unchanged historical input.
Existing valid-entry, primary identity, exit metadata, study and follow-through
regressions pass. Full-suite and exact committed-source results belong in the
PR verification checkpoint; do not infer them from this targeted result.

Only `entry_admission.py` behavior and its `primary_entry_study.py` call site
change in this slice, with tests and this document. Direction weights, primary/
runner selection, broker submission, accounting, runtime controls, all exits,
profit/drawdown rules and order-price allowances are unchanged.

This is an assistant adversarial review and repair, not the required Gemini
review. PR #44 must remain unmerged until that review and release acceptance
are complete. No production deployment, orders or runtime-setting changes were
performed. `entry_advantage_established=false` remains the honest state.
