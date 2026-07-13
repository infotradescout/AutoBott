# Core + Convex Runner Entry Contract

Every enabled AutoBott paper buy-in is one linked two-leg setup:

- `primary`: exactly one useful directional contract
- `runner`: exactly one different, cheaper, farther-out-of-the-money contract
- combined maximum debit: `$100` by default, including both contracts

The runner is not an extra quantity of the primary. Both legs share a `trade_group_id`, identify their `leg_role`, and
record the other leg's option symbol.

## Pair selection

The pair selector prefers the contract chosen by the decision engine when that contract and a valid runner fit the
combined budget. If it does not fit, the selector may choose a cheaper primary from the same direction and expiration.

The runner must:

- use the same call/put direction and expiration as the primary
- use a higher strike for calls or lower strike for puts
- cost no more than 40% of the primary ask by default
- have lower absolute delta than the primary
- pass configurable spread, volume, and open-interest minimums

If no qualifying pair fits under the total debit cap, AutoBott submits neither leg and records
`core_runner_pair_not_found_under_budget`. It never duplicates the primary and never exceeds the group budget to force
an entry.

## Exit behavior

The primary keeps the normal harvest rules. The runner is independently monitored with wider defaults: a 100% profit
target, trailing activation at 50%, a 25-point trailing drawdown, and a 70% stop. Closing the primary does not close the
runner.

The runner is considered funded only when realized primary profit covers the runner's entry debit and fees. An
unrealized recovery in the runner's own premium does not satisfy that accounting condition.

## Safety posture

Core + runner entry is enabled on the Render paper service. Live paired submission is explicitly rejected with
`core_runner_live_not_validated` until atomic/multi-leg live execution and recovery behavior are validated.
