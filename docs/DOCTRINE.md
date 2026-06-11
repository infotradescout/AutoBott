# AutoBott v2 Doctrine (P0 Lock)

AutoBott v2 is built as a risk-control system that happens to trade.

## Non-Negotiables

- Paper-first.
- Risk-gated.
- Deterministic decisions.
- Replayable records.
- Fail-closed on missing or unsafe inputs.
- No live execution before approval gate.

## Forbidden in P0/P1

- Live trading.
- Live broker execution.
- Real orders.
- Broker credential requirements.
- External connector dependencies.

## Initial KPIs

- 100% of trades have a decision record.
- 100% of rejected trades have reason codes.
- 0 trades bypass risk gate.
- 0 live orders before approval gate.
