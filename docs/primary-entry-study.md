# Primary-entry study runner

Continuation of PR #44 from `2122bf5fd7636e59b696765aef97d57925e0c234`.

## What is shared with runtime

`primary_entry_study.py` calls the real snapshot validator/parser, completed-bar evidence check, fresh-candidate filter, selected legacy/v2 decision builder, exact-primary core/runner selector, and final quote-admission function. There is no alternate tactical/rider fill simulator in this path. The outcome stage follows only the chosen primary option through a fixed observation window, independently of manual exits. A runner's profitable path is never a primary success.

This is **entry-function parity**, not complete account/broker replay. It does not simulate account balances, exposure capacity, cooldown scheduling, order rejection or sequential leg execution, or actual exits/P&L. Repeated scanner observations are not assumed independent. A comparison of the named legacy and v2 engines is not automatically a comparison of an old deployed release and the current release; source fingerprints identify the executed code and must support the stated comparison.

## Explicit protocol

`PrimaryStudyProtocol` requires a study ID, last development-entry timestamp, evaluation start/end, fill basis, maximum fill delay, minimum paired scorable count, and complete persisted quality/decision/pair/admission settings. Loading JSON requires every nested dataclass field; missing settings cannot silently inherit changed defaults. Development-entry outcome windows must end before evaluation starts. Entries whose full primary observation window exceeds the evaluation boundary remain visibly censored.

No production gain, drawdown, persistence, or holding-period parameters are supplied by this change. Test numbers are synthetic fixtures, not calibrated defaults. A saved protocol hash is not proof that nobody inspected outcomes first; reports explicitly leave preregistration verification false.

## Input contract

The JSON tape uses `schema_version: primary_entry_tape.v1`, `source_kind: synthetic | recorded_market`, `cohort_scope: all_recorded_scans | admitted_entries_only`, and a `cases` array. Each case requires a unique `sample_id`, the original `snapshot`, the recorded `refresh`, optional supplied `fills`, and `outcome_snapshots`.

The refresh includes `requested_at`, `received_at`, `options_feed`, normalized raw `option_quotes` and `stock_quotes`, and the original `authorized_prices` keyed by exact option symbol. It is a recorded market response, not a query against future observations. No missing refresh quote or authorization for a newly selected comparator contract is fabricated. An admitted-only tape remains labeled as such; it cannot establish scanner-wide selection performance. Cohort completeness and source authenticity are not verified by a caller-supplied label.

Every outcome observation uses its actual timestamp, ticker, quote chain, and `source.options_feed`. Mixed or unknown outcome feeds cannot produce a measured primary return. Missing path coverage remains unscorable. Each broker fill order ID may appear only once across the study, preventing the same purchase from being counted repeatedly across scanner cases. Duplicate sample IDs and equivalent snapshot identities are rejected.

## Filled versus hypothetical entry price

- `recorded_primary_fill` requires exactly one supplied matching primary buy-to-open record: `option_symbol`, `timestamp`, finite positive `price`, `quantity: 1`, and `broker_order_id`. The timestamp must follow admission and fit the declared maximum delay. This record is not silently replaced with a displayed quote. Partial-fill aggregation and multi-contract fills require a separately verified input adapter; they are not guessed here. The record label itself does not prove broker authenticity.
- `refresh_ask_shadow` explicitly uses the recorded refreshed ask as a hypothetical entry price and labels it as an assumption with no order. It is not a broker fill or realized return. This permits shadow comparisons on the same recorded data when actual fills are absent, without mislabeling the evidence.

The refresh is included in the observed path at its real receipt time. A later fill does not retimestamp that quote. Manual-exit metadata does not truncate the observation window: the supplied option path must continue through its declared end. This runner does not itself subscribe to quotes or create missing post-exit observations.

## Runtime evidence export

Successful runtime admission now adds a `recorded_refresh` capsule to its existing returned/journaled telemetry. It contains only the already fetched selected option/stock observations, original authorization, request/receipt times, feed, and a hash binding the capsule to the original snapshot. No extra market request, credential, broker action, or eligibility change is introduced. `case_from_runtime_evidence` verifies that binding and copies the records without rewriting the source.

An older journal without that capsule cannot be converted by inventing it. The capsule covers admitted cases and exact requested contracts only; complete comparisons may require separately recorded scanner cases and alternative-contract refresh observations. Actual fill records and post-entry/post-manual-exit quote paths must still be supplied from retained evidence.

## Commands

Run from a checkout with `src` on Python's module path (or an installed package):

```text
python -m autobott_v2.primary_entry_study --tape tape.json --protocol protocol.json --engine legacy --output artifacts/primary-baseline
python -m autobott_v2.primary_entry_study --tape tape.json --protocol protocol.json --engine v2 --output artifacts/primary-candidate
python -m autobott_v2.primary_entry_study --compare artifacts/primary-baseline/report.json artifacts/primary-candidate/report.json --output artifacts/primary-comparison
```

Each output directory must be new. The runner writes its manifest before evaluating cases, then per-case JSONL and a report with a content hash. Comparisons verify report integrity and require the same tape, protocol, source kind, cohort scope, and exit-assessment scope. No outcome-selected thresholds, code tuning, or order placement are performed.

## What the report means

Sample counts include no-candidate, no-pair, rejected admission, missing refresh, missing/ambiguous fill, absent outcome path, boundary censoring, and evaluated results. Scorable primary passes/failures and unscorable paths are reported separately. The report also includes confirmed opportunities per recorded sample, observed time to first target, and adverse movement before opportunity. Eliminating all entries therefore cannot be represented as a 100% winning strategy. Empty/scorable-zero rates are null, not a measured zero-percent rate.

A paired comparison includes only sample IDs scorable for both methods but retains each full cohort summary so missing coverage and selection changes remain visible. Below the predeclared minimum it reports insufficient paired evidence; above it the comparison is descriptive, not a statistical finding or a profitable-edge claim. `edge_established` remains false, and no realized P/L is calculated.

## Acceptance status

Synthetic regression coverage includes actual v2 shell-to-offline primary/runner/admission equality, wrong-option and runner masking, supplied-fill versus assumed-ask distinctions, continued observation after manual exit, future-outcome independence of entry selection, drawdown-before-recovery, persistence, window censoring, identical tape/protocol comparison, duplicate fill protection, feed provenance, output immutability, and independence from poisoned deployment environment settings. These demonstrate implementation behavior, not actual market performance.

Still required for a market conclusion: genuinely fixed study settings, a traceable current market dataset and cohort, retained post-exit observations, the correct release/engine comparator, adequate scorable chronological evaluation coverage, and appropriate uncertainty analysis for correlated samples. Target-runtime validation, independent review, main merge and deployment are separate acceptance items. No live or paper order is placed by this runner.

Full guarded source suite on Windows Python 3.11.9 / pytest 9.0.3: 783 top-level tests and 68 subtests passed with zero failures/errors/skips and zero prohibited network/process attempts. The 851 JUnit records include the 68 subtests. Runtime admission is AST-identical after removing the added recorded-refresh telemetry key; entry rules, both decision engines, pair selection, exits, broker submission and risk/accounting policies remain unchanged in this checkpoint.

## Recorded-market fill linkage update - 2026-09-17

Actual-fill recorded-market studies now require `fill_basis=linked_primary_fill`
and exact admission/submission/account/broker-order records. The legacy
`recorded_primary_fill` input mode is restricted to synthetic diagnostics.
This supersedes any earlier description treating supplied fill fields as the
complete actual-fill input contract. Shadow-ask evaluation remains hypothetical.
See `docs/primary-fill-linkage-20260917.md` for schemas, compatibility and limits.
