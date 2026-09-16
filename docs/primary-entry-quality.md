# Primary-entry opportunity assessment

## Purpose and boundary

A primary entry must create a meaningful favorable move in the selected option within its intended holding window, before an unacceptable initial drawdown. Exit management is evaluated separately against an unchanged exit policy. A favorable underlying move, a fleeting green mark, or a winning secondary leg is not a substitute for primary-contract opportunity.

This change adds a read-only assessment to the existing replay path. It does not alter primary-entry selection, broker execution, live/paper settings, exit rules, outcome accounting, or safety gates. It is measurement infrastructure, not a demonstrated strategy improvement.

## Existing mismatch found

At baseline `5e0e8c3823d311fa82fed648844668c13cc54579`, `thesis_validation.py` measures signed underlying-price movement through the selected contract's expiration date. `phase1_replay.py` invokes it on selected candidates before simulated fills. Its pass rate therefore is not a filled-option entry-quality rate. That diagnostic remains available, now explicitly labeled `underlying_direction_only` / `entry_quality_evidence: false` in replay summary output.

## Contract and output

`entry_quality.py` evaluates one filled `Phase1LedgerEvent` JSON record at a time, using the exact selected option symbol, fill price, timestamp, and subsequent timestamped bid/ask observations. The replay adapter calls it only for filled events. Rejected/unfilled candidates do not become entries. Primary and non-primary legs are reported separately; a secondary winner cannot improve the primary's count.

Inputs must explicitly provide a frozen `EntryQualityRules` for each relevant leg role. Fields are `protocol_id`, `holding_seconds`, `target_return_pct`, `max_adverse_return_pct`, `persistence_seconds`, `max_quote_age_seconds`, `max_observation_gap_seconds`, `round_trip_fee_per_contract`, and `contract_multiplier`. Percentage fields are fractions (for example, 0.20 means 20%). No success thresholds or holding windows are inferred from future prices. No production parameter set is supplied by this patch. The numeric parameters in tests are synthetic test fixtures only.

For one standard 100-share option contract, sampled net return is:

```text
(bid * 100 - entry_fill_price * 100 - declared_round_trip_fee)
/ (entry_fill_price * 100)
```

A pass requires consecutive favorable fresh observations spanning the declared persistence period, with both quote-source and observation timestamps spanning that period. The target must be confirmed before the first drawdown-limit breach. Exact target equality qualifies; exact drawdown-limit equality breaches the limit. A zero bid is an observed loss, not missing data. Later loss does not erase an already confirmed entry opportunity, but loss before confirmation prevents a pass even if the contract later recovers.

The assessment records the largest/smallest observed returns, initial adverse movement before confirmation, first target time, confirmation time, first drawdown-breach time, and longest observed target run. Maximum favorable movement is a diagnostic; it is not an assumed exit or the sole success criterion.

A missing protocol, invalid fill basis, missing selected-contract quotes, crossed/nonfinite/stale/future-dated quotes, ambiguous duplicate quotes, incomplete holding window, or excessive observation gaps produces `unscorable`, not success. Identical duplicate/cached quotes do not extend persistence. Windows and quote times are timezone-aware and observations are sorted. Unknown cases remain visible in the total denominator and separately from scorable cases. A run with no scorable entries reports no measured pass rate, not 0%.

## Replay use and reproducibility

Call the existing `run_replay` with `entry_quality_rules_by_role={"tactical": tactical_rules, "rider": rider_rules}` after fixing the study protocol. Omit a role and its fills remain unscorable; one role never inherits another role's horizon. The assessment writes `entry_quality.jsonl` and adds `entry_quality` to `scorecard.json` and the return object. Its protocol values and SHA-256 hashes are written to the manifest before entry assessment begins. Existing exit/fill configuration and input snapshot hashes remain intact.

The manifest explicitly reports `entry_quality_preregistration_verified: false`: a saved hash is an identity check, not evidence that the protocol was committed before anyone examined the sample. Independently retain the protocol commit, data cutoff, entry/exit code revisions, and holdout assignment. Do not relabel a retrospective exploratory run as preregistered.

## Limits and next experiment

These are sampled displayed-bid opportunities, not continuous-price-path proof, guaranteed fill capacity, or realized P/L. The configured maximum observation gap applies to the start/end boundaries as well as interior samples. No interpolation or continuous holding above target is asserted between observations. Wall-clock horizons that cross unobserved overnight/session periods will be unscorable when their gaps exceed the protocol; no market-calendar gap exemption is silently invented. Only explicitly declared standard 100-share contracts are supported.

Replay labels its fill evidence `simulated_fill`. The module can label supplied broker rows `broker_recorded_fill`, but that label itself does not verify broker provenance, reconciliation, or actual execution. Existing reconciliation protections remain in place.

To establish an entry-method advantage, freeze a development/chronological-holdout split, prospective success thresholds, contract-availability rules, entry-method versions, and the same exit/fill settings for both methods. Keep overlapping observation windows out of opposite sides of the split. Compare primary opportunity rate, unscorable coverage, time to target, pre-opportunity drawdown, and unchanged-exit net expectancy against a predeclared relevant baseline. Preserve ticker/session/leg attribution rather than pooling secondary winners into the primary. Account for clustered trades when estimating uncertainty. Do not tune on the holdout or select hindsight option contracts or selling points. The report deliberately leaves `edge_established` false.

## Validation performed

40 synthetic unittest methods passed under Python 3.13.5 in an isolated sandbox with socket/DNS connection attempts denied (zero attempts). This includes 36 evaluator tests and four replay-orchestration tests. The replay tests exercise the candidate `run_replay` with mocked existing engine, simulation, exit, and gate dependencies; they are not a complete-repository or broker integration run. The source-owned test file can run with normal repository imports in a full checkout.

The baseline replay source was reconstructed and verified against Git blob `74f43a7a3493a2c9da20fcf9ebd89e77fc5978d7` before patching. The entire existing replay exit-processing block remains byte-for-byte unchanged; `_execution_rules`, `_exit_rules`, and `_manifest` remain AST-identical. No real trade dataset was evaluated, no empirical entry advantage was established, and no runtime deployment was performed by this work.
