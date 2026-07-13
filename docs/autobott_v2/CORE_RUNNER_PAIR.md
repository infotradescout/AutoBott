# Core + Convex Runner Entry Contract

Every enabled AutoBott paper buy-in is one linked two-leg setup:

- `primary`: exactly one useful directional contract
- `runner`: exactly one different, cheaper, farther-out-of-the-money contract
- real-money affordability marker: `$100` per leg by default

The runner is not an extra quantity of the primary. Both legs share a `trade_group_id`, identify their `leg_role`, and
record the other leg's option symbol. The affordability marker is reporting metadata, not a paper-trade eligibility cap.

## Pair selection

The pair selector prefers the contract chosen by the decision engine and then finds a valid runner from the same
direction and expiration. Contract cost does not make an otherwise valid pair ineligible for paper trading.

The runner must:

- use the same call/put direction and expiration as the primary
- use a higher strike for calls or lower strike for puts
- cost no more than 40% of the primary ask by default
- have lower absolute delta than the primary
- pass configurable spread, volume, and open-interest minimums

If no structurally valid, liquid pair exists, AutoBott submits neither leg and records `core_runner_pair_not_found`.
It never duplicates the primary to force an entry.

Pairs with both legs at or below `$100` are labeled `real_money_affordable=true` for the operator to identify trades
that fit the user's real-money testing budget. Any pair with either leg above that marker remains eligible for paper
execution and is labeled paper-only. This does not authorize automatic live execution.

## Exit behavior

The primary keeps the normal harvest rules. The runner is independently monitored with wider defaults: a 100% profit
target, trailing activation at 50%, a 25-point trailing drawdown, and a 70% stop. Closing the primary does not close the
runner.

The runner is considered funded only when realized primary profit covers the runner's entry debit and fees. An
unrealized recovery in the runner's own premium does not satisfy that accounting condition.

## Safety posture

Core + runner entry is enabled on the Render paper service. The paper service keeps a separate `$5,000` per-leg
operational ceiling so the `$100` affordability marker does not suppress larger paper tests. Live paired submission is explicitly rejected with
`core_runner_live_not_validated` until atomic/multi-leg live execution and recovery behavior are validated.
