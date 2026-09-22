# Primary-entry market admission checkpoint

Continuation of PR #44 from `3a007dcc82cd725b9cedd1446e5d453aa26d9419`.

## Objective

Primary entries must create meaningful favorable opportunity in the purchased option before unacceptable drawdown. Manual exits remain available. This checkpoint repairs entry evidence and admission, not exit management and not a demonstrated predictive advantage.

## Runtime behavior implemented

1. Completed fixed-interval bars only: Alpaca timestamps identify interval starts, so a bar is eligible only when its start plus its declared duration is at or before the query cutoff. Capture applies that rule to the underlying and all three context series. Chronological ordering, duplicate identity, numeric/range validation, and the minimum sample count apply after incomplete bars are removed. Identical duplicates do not add weight; conflicting duplicates fail closed. Extra requested history and retry checks use completed-bar availability. Unsupported calendar timeframes require their own calendar contract rather than guessed durations.
2. Current stock quotes: `AlpacaPaperClient.get_latest_stock_quotes` re-requests the entire symbol set on each call. A later missing response cannot reuse an earlier cached observation. This removes the previous lifetime-of-client latest-quote cache without removing the historical context-bar cache.
3. Freshness before ranking: stale, future-dated, invalid, or ambiguous candidate option quotes are excluded from the runtime decision input before contract scoring. The original snapshot remains unchanged, including rejected rows. A stale quote cannot win the ranking merely to be rejected at submission; still-eligible fresh alternatives retain the existing scoring rules. Filtering details are recorded with the decision.
4. Final refresh before the first submission: the existing submission callback reads new quotes for the exact approved primary and, when present, exact runner, plus the underlying equity or declared signal proxy. Missing refresh capability or any bad quote blocks submission before setup-registration or attempt-count changes. Both refreshed quotes must be valid before either pair leg is submitted. The check runs in the legacy shell and its actual v2 isolated bindings.
5. Revalidation: the captured decision and source quote ages have explicit 30-second ceilings, matching the existing simulator's quote-age ceiling. This is an operational data-quality rule, not a fitted profit/holding threshold. Refresh latency counts against the ceiling. Future timestamps, regressing clocks/quote times, identity mismatches, and loss of the existing primary/pair contract eligibility all reject the entry. New quotes cannot increase the original price allowance or substitute a different primary/runner. A passive original limit remains passive; a market order is not transformed into a guaranteed capped fill.
6. Evidence: recorded admission metadata includes checked time, source quote ages, exact symbols, original price allowance, completed-bar cutoff, and feed. Variable receipt delays no longer create false missing-schedule ticks: day manifests use the preserved scheduled timestamp for cadence and retain receipt times for observation coverage.

## Verification actually performed

The first targeted run reproduced 15 failing regression cases against the prior source. The same cases pass after the changes. Additional tests cover exact freshness boundaries, refresh-time expiry, clock regression, stale candidate exclusion without snapshot rewriting, duplicate bars, invalid OHLC/volume, unsupported timeframes, v2 pair rejection, successful exact-primary admission, unchanged exit metadata, and retry eligibility after a rejected quote.

The complete source-owned guarded suite passed on Windows Python 3.11.9 / pytest 9.0.3: **741 top-level tests plus 68 subtests**, zero failures/errors/skips, validation exit zero, and zero prohibited network/process attempts during tests. The 809 JUnit records include the subtests; they are not 809 independent top-level tests. Broker/market providers in the tests are synthetic doubles; the actual capture, v2 engine, pair selection, submission orchestration, and admission integration run as source code. No missing-module import stubs are used.

Older fixtures were corrected rather than production checks weakened: July trading fixtures now use an explicit July admission clock and a refreshed-quote method; readiness fixtures contain distinct completed timestamps instead of 35 copies of one forming timestamp; the hosted hourly fixture supplies hourly completed bars; capture fixtures honor the requested row limit. No confidence or profit threshold was reduced to make tests pass.

An invariant check verifies no changes in 18 protected files, including exits/monitors, pair lifecycle, broker submission/orchestrator, execution risk configuration, outcome accounting, strategy/hosted policy, both scoring engines, directional evidence, core/runner selection, and render.yaml. All changes occur in market-data capture/client code, entry admission, its trading-cycle integration, tests, and documentation. No broker order, runtime switch, journal correction, main-branch merge, or deployment is performed by this checkpoint.

## Boundaries and next evidence

A passing admission means fresh, complete, structurally eligible evidence under this protocol; it is not proof of a profitable entry. The existing indicative options feed is unchanged. Alpaca documents indicative quotes as modified and its trades as delayed; these observations are not represented as executable OPRA prices. Real fill price/capacity, metadata/Greek refresh, session-calendar bar-gap policy, and timing between sequential pair legs remain distinct concerns. Bar completion does not claim a full session-calendar freshness model. VIX/VIXW use a fresh signal-proxy quote with the captured index estimate; a proxy is not relabeled a direct current index quote.

The original entry/exit order types and exit settings stay unchanged. Revalidation occurs before the first broker submission, not as a guarantee of the eventual fill or the second leg's later execution time. Current market-data execution was not performed in this offline validation. The legacy tactical/rider replay still does not reproduce the full hosted core/runner admission path; do not claim a matched production backtest from it.

Next: evaluate and compare primary-option outcomes under genuinely predeclared holding, target, drawdown, and persistence rules using the same entry admission and unchanged exits, with exact contract/quote provenance and chronological holdout coverage. Do not infer an entry advantage from test counts or retimestamp old captures into clean evidence.

## Provider contracts checked

- Alpaca Market Data FAQ, bar aggregation and interval-start timestamps: https://docs.alpaca.markets/us/docs/market-data-faq
- Alpaca latest option quotes, contract-symbol request and OPRA/indicative feed semantics: https://docs.alpaca.markets/us/reference/optionlatestquotes
