# AutoBott v2 Purpose Lock

## One-Sentence Purpose

AutoBott exists to evaluate regime-first, read-only intraday options trade candidates for Thomas using deterministic decision cards and replayable ledgers while avoiding live execution, connector lock-in, and undocumented discretionary overrides.

## 1. Primary Purpose

Provide a disciplined, fail-closed options decision system that can prove whether a repeatable regime, direction, volatility, and contract-selection process has edge before any broker order path exists.

## 2. Primary User

Thomas as the owner-operator and reviewer of decision records, replay outputs, and risk outcomes.

## 3. Market Scope

US listed equities and listed equity options during regular market sessions, with optional premarket underlying context in replay only.

## 4. Instrument Scope

Primary instrument: long calls and long puts for read-only decision cards in Phase 1.

Out of scope in this lock phase: market making, naked option selling, crypto, futures, sports, multi-asset routing, and live broker orders.

## 5. Time Horizon

Intraday decisioning with replay windows used for historical evaluation.

## 6. Automation Level

Automated read-only decision engine with deterministic rules. No live order routing.

## 7. Decision Authority Model

Engine authority: may classify decision cards as trade candidates or rejected/no-trade records.

Human authority: final gate for any future live enablement. Live authority remains disabled until explicit approval gate criteria are met.

## 8. Signal/Edge Source

Structured regime, direction, volatility, and option-contract signals with risk asymmetry filters and strict reject reasons.

Signal complexity is secondary to decision quality and replay transparency.

## 9. Profit/Risk Objective

Primary objective: survive long enough to prove edge quality, not maximize short-term PnL.

Risk objective: preserve paper capital assumptions through fail-closed rejection, position-size limits, and max-loss controls.

## 10. KPIs

Initial control KPIs (must hold before expansion):

- 100% of trade candidates have decision cards.
- 100% of rejected candidates have blocked reasons.
- 0 trades bypass risk gate.
- 0 live orders before approval gate.

Evaluation KPIs (after control KPIs are stable):

- Win rate.
- Average win / average loss.
- Max drawdown.
- Profit factor.
- Trade frequency.
- Slippage assumptions in replay.
- Strategy decay indicators over time slices.

## 11. Forbidden Scope

- Live trading execution.
- Broker connector integration for live order placement.
- Credential-gated runtime requirements for local validation.
- External monetization/integration surface (SaaS/Discord/Stripe) in foundation phases.
- Emotional or undocumented manual overrides.

## 12. Build Implications for P2/P3/P4

- P2 (Paper Execution): only after decision cards are stable, add paper-only limit-order lifecycle simulation.
- P3 (Learning): persist accepted and rejected candidates with future-window outcomes.
- P4 (Replay Harness): replay output must support KPI auditing and blocked-reason attribution for every decision.

No P2 expansion is valid if it conflicts with this purpose lock.
