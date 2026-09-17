# Live entry thesis candidate - 2026-09-17

Baseline: `308d3cb1b951d156ce8ee8804ce49e25bba82906`, PR #44.
This slice changes actual entry admission rather than only adding study records.
The existing freshness/identity checks do not by themselves establish that the
observed underlying price still supports the earlier completed-bar direction.

## Candidate policy, not a proved edge

For direct same-symbol equity/ETF observations, reject a bullish admission when
the whole observed quote is below the last completed signal bar's low (ask < low).
Reject a bearish admission when the whole quote is above that bar's high
(bid > high). Exact touches and spread straddles alone are not confirmed breaks.
Check the captured quote and the refreshed quote against the same original bar.
Do not move the boundary after seeing the refresh or consult future outcomes.

This is a new conservative entry-policy hypothesis. It is not a demonstrated
cause of historical losses and is not proof that excluding these entries improves
returns. No favorable-move, persistence, holding-window or drawdown threshold has
been fitted or changed. It can suppress a later recovery or profitable retest;
that opportunity cost must remain in the fixed-cohort market comparison.

Proxy/index units are explicitly `not_evaluated` by this rule. VIXY quotes are
not compared numerically against rescaled VIX bars. Other existing admission
checks remain active, but this rule does not certify those proxy theses.

## Implementation and verification scope

Only entry_admission.py and the new pure live_entry_thesis.py change application
behavior. Capture and refresh evidence is returned in live_signal_thesis, or
entry_signal_price_invalidated identifies the rejection stage and boundary.
Both cycle paths and the study already share the admission function. No exits,
monitors, order price allowances, accounting journals or trading permissions
are modified. The pure helper passed 40 isolated Python tests in the current
execution environment, with its Git blob verified against the tested bytes.
Full-repository and hosted results must be recorded separately after execution.
Three added cycle tests cover capture/refresh invalidation and an intact entry
retaining its selected primary and unchanged exit metadata.

The desktop connection became unresponsive during this slice. An attempted
write of tests/test_entry_live_thesis.py there timed out, so its presence is
unknown and must be checked before reusing that checkout. The current candidate
uses differently named test files to avoid silently overwriting that possible
untracked work. GitHub is the publication authority until local alignment is
verified. Do not describe the desktop checkout as clean or aligned.

## Other observed issue and release boundary

The dashboard single-cycle action imports the legacy cycle while automatic
sessions import v2. The monitor-v2 wrapper delegates to the canonical monitor.
That entry-routing inconsistency was identified, not fixed in this slice; do not
claim the dashboard path has been migrated merely because this shared gate runs.

Gemini review and accepted paper-production release remain required. No review
request is an approval. Authenticated receipt/order collection, historical
account reconciliation, protected runtime access and complete primary quote paths
remain outstanding. `entry_advantage_established=false` remains the status.
