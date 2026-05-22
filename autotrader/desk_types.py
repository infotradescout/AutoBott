"""Typed evidence contracts for the AutoBott options desk.

These objects are intentionally small and serializable.  Each analysis lane
(direction, volatility, Greeks, liquidity, risk) produces evidence only.  The
coordinator is the only layer that can turn evidence into a trade decision.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Literal

LaneName = Literal["direction", "volatility", "greeks", "liquidity", "risk"]
Verdict = Literal["pass", "fail", "warn", "unknown"]
TradeAction = Literal["buy_call", "buy_put", "no_trade"]


@dataclass(frozen=True)
class EvidenceCard:
    """A single lane's typed opinion about a possible options trade."""

    lane: LaneName
    symbol: str
    verdict: Verdict
    confidence: float
    reason: str
    metrics: dict[str, Any] = field(default_factory=dict)
    produced_at: str = ""

    def __post_init__(self) -> None:
        normalized_symbol = str(self.symbol or "").upper().strip()
        object.__setattr__(self, "symbol", normalized_symbol)
        confidence = max(0.0, min(1.0, float(self.confidence or 0.0)))
        object.__setattr__(self, "confidence", confidence)
        if not self.produced_at:
            object.__setattr__(self, "produced_at", datetime.utcnow().isoformat(timespec="seconds") + "Z")

    @property
    def passed(self) -> bool:
        return self.verdict == "pass"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class DirectionOpinion:
    """The direction lane's final call/put opinion for an underlying."""

    symbol: str
    direction: Literal["call", "put", "none"]
    confidence: float
    reason: str
    metrics: dict[str, Any] = field(default_factory=dict)

    def to_evidence(self) -> EvidenceCard:
        verdict: Verdict = "pass" if self.direction in {"call", "put"} and self.confidence > 0 else "fail"
        return EvidenceCard(
            lane="direction",
            symbol=self.symbol,
            verdict=verdict,
            confidence=self.confidence,
            reason=self.reason,
            metrics={"direction": self.direction, **self.metrics},
        )


@dataclass(frozen=True)
class TradeDecisionCard:
    """Coordinator output.  This is the only object that authorizes execution."""

    symbol: str
    action: TradeAction
    authorized: bool
    confidence: float
    reason: str
    evidence: list[EvidenceCard] = field(default_factory=list)
    produced_at: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", str(self.symbol or "").upper().strip())
        confidence = max(0.0, min(1.0, float(self.confidence or 0.0)))
        object.__setattr__(self, "confidence", confidence)
        if not self.produced_at:
            object.__setattr__(self, "produced_at", datetime.utcnow().isoformat(timespec="seconds") + "Z")

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["evidence"] = [card.to_dict() for card in self.evidence]
        return payload
