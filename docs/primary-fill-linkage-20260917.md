# Account-scoped primary fill linkage - 2026-09-17

## Scope and reproduced gap

Baseline: `609a5cb475c8f582d97a06eccfeb871748d8a904`, PR #44.
The older `recorded_primary_fill` mode accepted a supplied matching symbol,
quantity, price, timestamp and nonempty order ID without binding that ID to the
recorded admission, submission or broker account. The manifest already warned
that source provenance was unverified; nevertheless those fields alone were
insufficient for the required admission -> actual primary fill evidence chain.
A guarded synthetic case labeled `recorded_market` reproduced that acceptance.

## Formal study contract

`linked_primary_fill` is the required actual-fill mode for `recorded_market`
tapes. The old supplied-fill mode remains available for synthetic diagnostics;
using it for recorded-market studies raises
`linked_primary_fill_required_for_recorded_market`. `refresh_ask_shadow`
remains explicitly hypothetical, not an actual fill.

The linker joins the retained admission's decision ID, primary option,
checked-at timestamp and exact snapshot hash to one primary submission receipt.
The receipt must specify the same admission, a paper account scope, broker and
client order IDs, a primary buy-to-open intent and exactly one contract.
A retained broker observation must match both order IDs, account and option,
and report a complete one-contract buy fill. Its actual average fill price and
fill timestamp supply the entry basis. Partial/canceled orders, conflicting
quantities, invalid prices, duplicate links and clock contradictions fail.

Inputs are `recorded_admission`, `primary_submission_receipts` and
`broker_order_observations` on each case. The two receipt schemas are
`primary_submission_receipt.v1` and `broker_order_observation.v1`; executable
synthetic examples are in `tests/test_primary_fill_linkage.py`. Existing
supplied fills cannot override or contradict the matched broker observation.
A reused account/order pair cannot reweight the study under another sample ID.
The same textual order ID in two distinct scoped accounts is not conflated.
Missing or invalid links retain their case in the denominator without scoring.

Runtime export now preserves the actual admission identity, rather than just
the refresh capsule. **This slice does not automatically collect authenticated
submission receipts or broker orders.** Existing observation files without
those records remain unlinked. Historical ownership is not inferred or fixed.
The collector and accepted production release remain separate work.

The pure linker proves referential consistency of retained records, NOT their
authenticity. Source-file hashes are not broker signatures. The report keeps
`source_provenance_verified=false`, `source_authenticity_verified=false`, and
`edge_established=false`. Synthetic fixtures do not become market evidence by
changing a label. A new fill-basis protocol hash is not retrospectively treated
as an earlier preregistered experiment or a measured before/after improvement.

No broker calls, journal writes, runtime configuration or exit changes occur in
this module. The fixed opportunity, persistence, drawdown and fill-delay rules
are unchanged. Gemini review remains required before merge. Exact committed
validation and hosting results are recorded in the PR checkpoint, separately
from these implementation claims. No profitable entry advantage is established.
