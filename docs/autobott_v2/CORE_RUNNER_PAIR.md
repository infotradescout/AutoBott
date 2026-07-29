# Core + Convex Runner Entry Contract

Every enabled AutoBott paper buy-in is one linked two-leg setup:

- `primary`: exactly one useful directional contract
- `runner`: exactly one different, cheaper, farther-out-of-the-money contract

Paper pair selection has no affordability ceiling. The hosted service uses Alpaca's fake buying power for live-market
paper testing; this does not enable Alpaca's real-money environment.

The runner is not an extra quantity of the primary. Both legs share a `trade_group_id`, identify their `leg_role`, and
record the other leg's option symbol.

## Pair selection

The pair selector prefers the contract chosen by the decision engine and finds a valid runner from the same direction
and expiration. Contract price does not make an otherwise valid pair ineligible for paper trading.

The runner must:

- use the same call/put direction and expiration as the primary
- use a higher strike for calls or lower strike for puts
- cost no more than 40% of the primary ask by default
- have lower absolute delta than the primary
- pass configurable spread, volume, and open-interest minimums

If no structurally valid, liquid pair exists, AutoBott submits neither leg and records `core_runner_pair_not_found`.
It never duplicates the primary to force an entry.

The `$100` default exists only in the dashboard's **Decision Feed / Manual Mirror** window and is configurable with
`AUTOBOTT_MANUAL_MIRROR_MAX_CONTRACT_COST`. That window keeps a separate affordable candidate set, requires the same
expiration and minimum liquidity, and refreshes the quote before display. It does not alter scanner output, the
paper-selected primary, runner selection, or broker submission.

## Pair submission

The hosted paper account sends the primary and runner as two linked ordinary `buy_to_open` orders because Alpaca MLeg
approval is not required for that lane. Both orders retain the same `trade_group_id`. If the second submission fails,
AutoBott cancels or flattens the first accepted leg instead of intentionally keeping an accidental single-leg entry.

When atomic MLeg mode is enabled, the pair is sent as one two-leg limit order and fails closed if atomic submission is
unavailable, rejected, or malformed.

## Exit behavior

The primary keeps the normal harvest rules. The runner is independently monitored with wider defaults: a 100% profit
target, trailing activation at 50%, a 25-point trailing drawdown, and a 70% stop. Closing the primary does not close the
runner. A retained runner continues to count toward account position and drawdown limits, but it does not occupy the
same-underlying or shared-volatility entry slot by itself; a later qualified setup can open a new primary/runner pair.

The runner is considered funded only when realized primary profit covers the runner's entry debit and fees. An
unrealized recovery in the runner's own premium does not satisfy that accounting condition.

## Safety posture

Core + runner entry is enabled on the Render paper service. Live paired submission is explicitly rejected with
`core_runner_live_not_validated`; atomic multi-leg execution is currently paper-only.
