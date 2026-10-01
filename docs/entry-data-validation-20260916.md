# Entry-data integrity and full-source validation checkpoint

Continue PR #44 from `c8d05502b98f47a9a4818fa5e5198ff229b9d18b`. The target remains a meaningful primary-option opportunity before unacceptable initial drawdown. Manual exit capability is not evidence that the entry worked; exit policy stays fixed.

## Repairs in this checkpoint

- **Observed quotes only.** `quote_observation.py` requires finite observed bid/ask prices and a timezone-aware source quote timestamp. Missing quotes are not reconstructed from an earlier stock bar, and missing quote timestamps cannot borrow a latest-trade timestamp or the current clock. A real zero option bid stays zero; it is not replaced with a last-trade price. Invalid option quotes are excluded before contract selection and can produce machine-readable rejection reasons. Valid source quote timestamps retain their original age.
- **Receipt time, not scheduled scan time.** Capture now records the observation time after market-data retrieval, using elapsed monotonic time anchored to the supplied capture-start clock. The original schedule is retained separately. Runtime scans obtain a new capture-start time per symbol instead of reusing the first symbol's clock across the batch. Raw quote timestamps are never advanced to hide a delay. Deterministic capture tests inject the elapsed-time clock, then exercise the real snapshot validator, parser, and v2 decision builder.
- **No unapproved simulated entries.** A `NO_TRADE` or blocked card may retain a selected option for diagnostics. That metadata is not permission to trade. The execution simulator now rejects non-candidate cards before leg simulation. The approved-candidate fill model and all exit calculations remain unchanged.
- **Campaign test precondition repaired.** The previous small-sample campaign fixture relied on those unauthorized simulated fills. It now uses an explicitly stronger synthetic history and asserts that the real entry engine approves it before testing the unchanged minimum-sample gate. A separate regression proves that the original low-confidence fixture produces no candidate outcome buckets. No production confidence threshold was lowered.
- **Complete-suite isolation.** Supervisor tests now stop and join every worker while mocked dependencies are still installed. Previously an escaped test worker could call real runtime code after mocks were restored. This fixture changes tests only, not production workers. The validation runner caches local platform metadata before installing the existing audit guard, preventing Python 3.11's Windows hostname lookup from invoking subprocesses during JUnit reporting. Network/process deny rules are unchanged.

## Validation and provenance

Run `python scripts/validate_offline.py` from a short checkout path on Windows. Long checkout paths can exceed Windows path limits in nested campaign and immutable-journal snapshot tests; this is not corrected by lowering safety checks. The complete repository is present in this checkout: no missing-module stubs are needed. Normal test doubles for brokers and market-data providers remain synthetic, not evidence of market performance.

The first new quote/permission regression run reproduced 19 failures before the source repair. Targeted quote, capture, simulator, campaign, and v2-cycle-isolation runs then passed. Use the retained full-suite summary and source manifest for final counts; a test definition or workflow file alone is not a passing run.

The changed runtime areas are quote capture and its capture-clock argument. Broker order-submission implementations, exit modules, account/risk policy, outcome accounting, and live/paper configuration are not changed by this checkpoint. No historical owner journal or capture file is rewritten. No broker order or new session is started by validation.

## Limits that must remain explicit

A valid source timestamp is not a complete freshness or entry-timing approval. Maximum quote age, quote refresh immediately before submission, coherent stock/option timestamps, completed-bar semantics, and full hosted core/runner replay parity still need end-to-end verification. Receipt timestamps improve causal recording but do not establish perfect provider-clock synchronization. The old snapshot archive must not be silently retimestamped and scored as clean evidence.

Do not treat a historical local corpus as a current September holdout. Fix the primary opportunity protocol and actual entry/contract versions before reading its outcomes; preserve same-exit comparison, unknown coverage, and source identities. No profitable primary-entry advantage is established by this checkpoint. Deployment remains a separate acceptance step.
