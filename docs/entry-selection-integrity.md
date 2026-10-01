# Primary-entry selection integrity checkpoint

## Objective

The primary option must create a usable favorable opportunity before unacceptable initial drawdown. Manual exits remain available; none of these changes remove automated exits, emergency controls, or accounting protections. This checkpoint fixes reproducible entry-path defects. It does not establish that the entry method has a profitable advantage.

Base: PR #44 head `5e2734c49ec55faf7e71ef3e15a92796d93fd2d0`, on main `5e0e8c3823d311fa82fed648844668c13cc54579`.

## Corrected defects

1. **Directional volume inversion.** The old formula multiplies candle direction by a signed below/above-baseline volume factor. At half normal volume, a red candle supplies approximately +0.5828 bullish evidence and a green candle approximately -0.5828 bearish evidence. The new formula treats below-baseline volume as absent confirmation, not opposite-side pressure. High-volume directional confirmation is unchanged; a doji contributes no directional volume evidence. Existing score weights and thresholds are unchanged.

2. **The runner could override primary approval.** The old pair selector prefers the engine-selected primary but searches other primaries when that contract has no valid runner. The repaired selector requires exactly one matching primary symbol and validates its type, expiry, strike, and existing core liquidity gates. Missing, ambiguous, mismatched, or ineligible primary means no pair, not a substitute contract. A runner must share the primary underlying, expiry, and option type. A deterministic symbol tie-breaker removes input-order dependence. These identity checks do not replace full engine revalidation when an upstream quote changes.

3. **Human explanations could reclassify setups.** The old `"reversal" in explanation` test can label a bullish continuation as a bullish reversal because its explanation mentions *opposing* `reversal_adjustment:-0.55`. Classification now uses the structured same-side reversal adjustment, with the scorer's existing +/-0.25 boundaries. Two-argument compatibility callers need the corresponding cycle confirmation. This does not retune the reversal score itself.

4. **Replay engine identity was implicit.** The hosted v2 trading-cycle shell binds `phase1_engine_v2`, while replay previously imported the legacy engine only. `run_replay` now accepts `entry_engine="legacy"|"v2"` and optional explicit `decision_rules: Phase1Rules`. The backward-compatible default remains `legacy`. The selected engine, resolved decision rules, their source, and an entry-configuration hash are persisted before assessment. Unknown engine/rule types fail before data loading. This permits an explicit v2 comparison instead of silently evaluating another entry method.

## Important fidelity limit

Selecting `entry_engine="v2"` does NOT reproduce hosted trading end to end. Replay still uses its existing tactical/rider execution simulation, not the hosted core/runner lifecycle, and no source-owned prospective primary-opportunity protocol or genuine holdout result is supplied here. Reports explicitly mark `execution_model=phase1_tactical_rider_simulation` and `hosted_execution_parity_verified=false`. Hosted entry rules also differ from default `Phase1Rules`; supply and retain the actual resolved entry rules for a comparison. Do not relabel the default replay as a production-performance backtest.

## Local validation actually performed

82 synthetic unittest methods pass on Python 3.13.5, comprising the previous 40 tests plus 35 primary-integrity tests and seven new replay-configuration tests. Core/runner selection, direction evidence, model dataclasses, and hosted-policy modules execute their real source. The unavailable legacy engine helpers and replay dependencies are import stubs in the local subset; replay orchestration uses explicit mocks in the tests. This is NOT a full-repository or full v2 decision-engine integration run.

A separate pinned-baseline comparison reproduces six failures: red/green low-volume inversion, high-volume doji bias, primary substitution to obtain a runner, a cross-underlying runner, and text-driven setup misclassification. All six produce the corrected results on the candidate. A valid pair's full data, including exit metadata, matches baseline. These are synthetic regression results, not real trade outcomes.

Source reconstructions were verified against Git blob identities before edits. Replay's exit-processing block remains byte-identical. `_execution_rules`, `_exit_rules`, `_manifest`, core `load_core_runner_rules`, `runner_is_funded`, and `_selected_contract` remain AST-identical. Local test socket/DNS connection attempts: zero. No broker calls, account-data edits, order submissions, or deployed settings were made.

The added read-only GitHub workflow requests the complete repository Python suite on Python 3.12 through the existing `scripts/validate_offline.py`. It checks out the exact PR merge revision, injects no account secrets, strips inherited settings before tests, and applies the existing network/process guard. A workflow file is not a passing run; inspect the resulting run and source identity before merge.

## Remaining acceptance

Run full-repository validation on the combined revision. Then establish full primary/core contract replay parity and evaluate fixed prospective holding/target/drawdown/persistence rules on actual selected-option quote paths, with missing coverage visible. Compare against the prior entry method under the same exit policy and retain chronological holdout evidence. No threshold tuning on that holdout, no runner masking, no hindsight contract choice, and no best-selling-point P/L claims. Deployment and an entry advantage remain unverified at this local checkpoint.
