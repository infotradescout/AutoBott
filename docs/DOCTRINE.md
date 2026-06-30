# AutoBott v2 Doctrine (P0 Lock)

AutoBott v2 is built as a regime-first options decision system that refuses to trade until the decision stack can explain itself.

## Non-Negotiables

- Paper-first.
- Read-only before paper execution.
- Risk-gated.
- Deterministic decisions.
- Replayable records.
- Fail-closed on missing or unsafe inputs.
- No live execution before approval gate.
- Paper-only broker connectivity is allowed only for status, market-data capture, and advisory operator workflows.

## Forbidden in P0/P1

- Live trading.
- Live broker execution.
- Real orders.
- Market-making logic.
- HFT assumptions.
- Broker credential requirements for local validation.
- External connector dependencies for live or order-writing behavior.
- Broker connector integration for order placement.
- Secret exposure in frontend/operator surfaces.

## Allowed in Phase 1F/1G

- Paper-only Alpaca status checks.
- Paper-only Alpaca market and option snapshot capture.
- Safe operator-console actions for capture, replay, and report refresh.
- Render-hosted monitoring surfaces that keep secrets server-side and preserve `PAPER ONLY` / `LIVE LOCKED` / `ORDERS DISABLED` status.

## Initial KPIs

- 100% of trade candidates have a decision card.
- 100% of rejected candidates have blocked reasons.
- 0 trades bypass risk gate.
- 0 live orders before approval gate.
