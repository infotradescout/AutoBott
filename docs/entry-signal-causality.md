# Entry signal causality repair

Base candidate: `c220e62728d333cfa25bb03d58e0e412577f8c6f`. This is a direct entry-scoring change, not an exit change or another outcome recorder.

## Defects corrected

**Current-date reference contamination.** Both the legacy regime/cycle code and the v2 direction scorer called helpers named `_session_vwap`, but those helpers included every supplied historical bar. A high-volume previous day could dominate the current reference. Both now share `current_market_date_reference`, restricted to the latest observed New York calendar date and no later than the latest input bar. Daylight-saving and UTC-midnight tests are included. If that date has no volume, the distance contribution is neutral rather than borrowing an earlier date.

This remains a volume-weighted HLC3 **bar proxy over supplied observations**. It is not exact trade-level VWAP, guaranteed full-session coverage, a regular-hours-only filter, or an exchange calendar. History-based momentum/EMA calculations remain history-based; this repair does not remove their intended lookback. Source bars remain unchanged.

**False relative strength.** The v2 scorer previously compared the instrument's final 20 bars against whichever final 20 benchmark bars were supplied, without matching their timestamps. It also used zero market return when benchmarks were absent, turning the instrument's own momentum into a second directional contribution. Benchmark returns now require the exact same UTC-normalized start/end observations. Future or earlier windows cannot replace those endpoints; invalid or conflicting endpoint prices make that benchmark unavailable. Identical duplicate endpoints do not reweight returns. With one valid benchmark, only that benchmark is used. With none, relative-strength contribution is zero and its unavailable state is exposed through `benchmark_series_used=0`.

The capture contract supplies a common fixed bar timeframe. Endpoint matching does not independently establish identical vendor aggregation intervals, a full missing-bar calendar, or complete provider provenance.

**Cycle age creating direction.** Before this change, flat synthetic prices with no reversal confirmation produced a bearish score of -0.35 when the up-cycle was merely labelled late, or bullish +0.35 when the down-cycle was labelled late. Without confirmed reversal evidence, cycle age now only reduces an existing directional score toward zero. It cannot create, amplify, or reverse a direction. Existing confirmed-reversal magnitudes, trend weights, neutral threshold and confidence threshold remain unchanged. This invariant is a design correction, not an empirically optimized timing strategy.

## Actual behavior checks

`tests/test_entry_signal_causality.py` checks old-date exclusion, timezone equivalence/DST, absent and mismatched benchmarks, future benchmark isolation, exact return endpoints, duplicate/conflicting/invalid endpoints, flat and weak-trend late-cycle cases, unchanged strong-trend behavior, input immutability, and the actual v2 builder returning no selected option for age-only directional cases. Existing bullish/bearish trend and confirmed-reversal regressions remain part of validation.

A pinned-source synthetic demonstration compares the old scorer loaded from the exact base Git object with the corrected scorer. Flat + late-up changes from bearish -0.35 to neutral 0; flat + late-down changes from bullish +0.35 to neutral 0. A strong matched uptrend retains its original score. No account, broker or market-performance result is involved. Evidence is retained under `artifacts/signal-causality` in the working environment.

## Boundaries

This repair changes `signal_evidence.py` and only the shared price-reference helper in `phase1_engine.py`, plus a new pure reference module and tests. Contract selection, order handling, entry admission, exits, risk controls, accounting, capture and runtime settings are unchanged in this slice. No profit target, drawdown limit, stop or order permission is loosened. It does not establish a profitable primary-entry advantage.

The repository's Gemini-before-merge requirement remains in force; a pending or failed external-review attempt is not approval. The tested candidate can be deployed to the existing isolated no-order validation service without representing it as the production paper release. Actual market comparison, independent review and production running-revision verification remain separate acceptance evidence.

## Follow-up review - 2026-09-17

A subsequent adversarial review reproduced stale completed bars passing entry
admission with fresh quotes. The bounded signal-age repair, failing-before
cases and remaining release gates are recorded in
`docs/entry-signal-freshness-review-20260917.md`.

## Fill evidence continuation - 2026-09-17

The next slice preserves admission identity in runtime exports and requires
account-scoped submission/broker-order linkage for actual-fill market studies.
Continue from `docs/primary-fill-linkage-20260917.md`; neither matching symbols
nor synthetic test passes establish a profitable entry advantage.

## Runtime continuation - 2026-09-17

Dashboard/session routing and automatic account-scoped primary receipt/fill
capture are implemented in `docs/entry-runtime-capture-20260917.md`. This
supersedes the earlier missing-collector/routing status, not the remaining
review, production acceptance or market-profitability requirements.

## News and minute context continuation - 2026-09-17

Continue from `docs/entry-news-minute-context-20260917.md` for native news and
minute-trigger admission, actual provider connectivity and remaining limits.
