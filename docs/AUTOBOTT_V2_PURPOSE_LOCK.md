# AutoBott v2 Purpose Lock

## One-Sentence Purpose

AutoBott exists to evaluate and paper-execute risk-gated intraday stock signals for Thomas using deterministic decision records and replayable scorecards while avoiding live execution, connector lock-in, and undocumented discretionary overrides.

## 1. Primary Purpose

Provide a disciplined, fail-closed paper trading engine that can prove whether a repeatable signal process has edge under controlled risk constraints.

## 2. Primary User

Thomas as the owner-operator and reviewer of decision records, replay outputs, and risk outcomes.

## 3. Market Scope

US listed equities during regular market sessions, with optional premarket data in replay only.

## 4. Instrument Scope

Primary instrument: stocks (shares) for paper execution in v2 foundation phases.

Out of scope in this lock phase: options, crypto, futures, sports, and multi-asset routing.

## 5. Time Horizon

Intraday decisioning and same-session paper position lifecycle, with replay windows used for historical evaluation.

## 6. Automation Level

Automated paper decision engine with deterministic rules. No live order routing.

## 7. Decision Authority Model

Engine authority: may approve or reject paper trades.

Human authority: final gate for any future live enablement. Live authority remains disabled until explicit approval gate criteria are met.

## 8. Signal/Edge Source

Structured technical/state-based signals with risk asymmetry filters and strict reject reasons.

Signal complexity is secondary to decision quality and replay transparency.

## 9. Profit/Risk Objective

Primary objective: survive long enough to prove edge quality, not maximize short-term PnL.

Risk objective: preserve paper capital assumptions through fail-closed rejection, position-size limits, and max-loss controls.

## 10. KPIs

Initial control KPIs (must hold before expansion):

- 100% of approved paper trades have decision records.
- 100% of rejected trades have reason codes.
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
- Credential-gated runtime requirements.
- External monetization/integration surface (SaaS/Discord/Stripe) in foundation phases.
- Emotional or undocumented manual overrides.

## 12. Build Implications for P2/P3/P4

- P2 (Risk Gate): implement only stock-focused, intraday, fail-closed risk rules aligned to this purpose lock.
- P3 (Signal Intake): intake contracts must preserve deterministic, replayable signal context and strategy identity.
- P4 (Replay Harness): replay output must support KPI auditing and reason-code attribution for every decision.

No P2 expansion is valid if it conflicts with this purpose lock.
