# AutoBott v2 Purpose Lock

## One-Sentence Purpose

AutoBott exists to automatically trade long-call and long-put options with deterministic signals, explicit risk
controls, auditable execution, and operator oversight, while using research, replay, and historical analysis as
supporting systems rather than the end product.

## 1. Primary Purpose

Provide a disciplined automated options trading system that can generate, submit, manage, and exit trades through
controlled broker integrations without losing explainability, risk discipline, or auditability.

## 2. Primary User

Thomas as the owner-operator who configures, supervises, reviews, and can interrupt the bot.

## 3. Market Scope

US listed equities and listed equity options during regular market sessions, with optional premarket or overnight
research inputs only where explicitly supported.

## 4. Instrument Scope

Primary instrument: long calls and long puts.

Out of scope in this lock phase: market making, naked option selling, crypto, futures, sports, and unrelated
multi-asset routing.

## 5. Time Horizon

Intraday decisioning and trade management, with replay and historical analysis used to improve the production
system.

## 6. Automation Level

Automated signal generation, risk validation, trade execution, position management, and operator-visible safety
controls. The current repository is still pre-execution in committed code, but the product target is automated
trading rather than a read-only advisory tool.

## 7. Decision Authority Model

Engine authority: generate and route eligible trades only when risk, market-state, and execution preconditions
pass.

Human authority: define limits, operate kill switches, review outcomes, and approve major production transitions.

## 8. Signal/Edge Source

Structured regime, direction, volatility, and option-contract signals with risk asymmetry filters, explicit reject
reasons, and post-trade evidence loops.

Signal complexity is secondary to durable edge, risk discipline, and operational clarity.

## 9. Profit/Risk Objective

Primary objective: produce positive risk-adjusted trading outcomes through repeatable automation.

Risk objective: constrain loss through fail-closed rejection, position-size limits, exposure caps, stop logic, and
operator-visible shutdown controls.

## 10. KPIs

Control KPIs:

- 100% of trade submissions have decision and risk records.
- 100% of orders, fills, cancels, and exits have immutable audit trails.
- 0 trades bypass risk, exposure, or operator safety controls.
- 0 secrets reach browser clients.

Performance KPIs:

- Win rate.
- Average win / average loss.
- Max drawdown.
- Profit factor.
- Trade frequency.
- Slippage versus expected fill.
- Strategy decay indicators over time slices.

## 11. Forbidden Scope

- Naked option selling.
- Emotional or undocumented manual overrides.
- Production execution without operator-visible risk status.
- Secret exposure in frontend/operator surfaces.
- Credential-dependent local test requirements where mocks or fixtures should suffice.
- Strategy changes that bypass replay, audit, or risk instrumentation.

## 11A. Supporting Systems

- Paper-market capture, replay campaigns, scorecards, and historical backfills are required support systems.
- These systems are evidence and iteration tools for the trading bot.
- They must not redefine the top-level identity of the repository.

## 12. Build Implications for P2/P3/P4

- P2 (Execution Domain): add order-intent models, broker adapters, and execution-state tracking.
- P3 (Position Domain): add live position lifecycle management, exits, and exposure controls.
- P4 (Research Domain): preserve replay and historical tooling as decision-improvement infrastructure.

No subsystem expansion is valid if it recenters the repository away from the automated trading product.
