from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .jsonl_retention import compact_jsonl_tail
from .hosted_policy import is_hosted_paper_runtime
from .phase1_models import DecisionInput, DirectionBias, OptionContractSnapshot, OptionType
from .runtime_paths import data_root


@dataclass(frozen=True)
class DefinedRiskSpreadRules:
    enabled: bool = True
    min_dte: int = 0
    max_dte: int = 3
    min_width: float = 1.0
    max_width: float = 4.0
    min_credit: float = 0.05
    max_risk: float = 100.0
    profit_target_pct: float = 0.50
    short_delta_min: float = 0.20
    short_delta_max: float = 0.60
    long_delta_min: float = 0.05
    long_delta_max: float = 0.40


@dataclass(frozen=True)
class SpreadLeg:
    option_symbol: str
    side: str
    option_type: str
    expiration: str
    strike: float
    bid: float
    ask: float
    delta: float
    volume: int
    open_interest: int


@dataclass(frozen=True)
class DefinedRiskSpreadCandidate:
    strategy: str
    underlying: str
    direction: str
    expiration: str
    short_leg: SpreadLeg
    long_leg: SpreadLeg
    width: float
    net_credit: float
    max_profit: float
    max_risk: float
    profit_target_debit: float
    risk_reward_ratio: float
    score: float
    reasons: list[str]

    def to_json_dict(self) -> dict[str, Any]:
        return asdict(self)


def defined_risk_spread_journal_path() -> Path:
    return data_root() / "execution" / "defined_risk_spreads.jsonl"


def load_defined_risk_spread_rules() -> DefinedRiskSpreadRules:
    if is_hosted_paper_runtime():
        # This 0-3 DTE research lane is outside the hosted 5-45 DTE policy and
        # must never parse retained env values on the real entry path.
        return DefinedRiskSpreadRules(enabled=False)
    return DefinedRiskSpreadRules(
        enabled=_env_bool("AUTOBOTT_DEFINED_RISK_SPREADS_ENABLED", default=True),
        min_dte=int(os.getenv("AUTOBOTT_SPREAD_MIN_DTE", "0")),
        max_dte=int(os.getenv("AUTOBOTT_SPREAD_MAX_DTE", "3")),
        min_width=float(os.getenv("AUTOBOTT_SPREAD_MIN_WIDTH", "1")),
        max_width=float(os.getenv("AUTOBOTT_SPREAD_MAX_WIDTH", "4")),
        min_credit=float(os.getenv("AUTOBOTT_SPREAD_MIN_CREDIT", "0.05")),
        max_risk=float(os.getenv("AUTOBOTT_SPREAD_MAX_RISK", "100")),
        profit_target_pct=float(os.getenv("AUTOBOTT_SPREAD_PROFIT_TARGET_PCT", "0.50")),
        short_delta_min=float(os.getenv("AUTOBOTT_SPREAD_SHORT_DELTA_MIN", "0.20")),
        short_delta_max=float(os.getenv("AUTOBOTT_SPREAD_SHORT_DELTA_MAX", "0.60")),
        long_delta_min=float(os.getenv("AUTOBOTT_SPREAD_LONG_DELTA_MIN", "0.05")),
        long_delta_max=float(os.getenv("AUTOBOTT_SPREAD_LONG_DELTA_MAX", "0.40")),
    )


def select_defined_risk_spread(
    decision_input: DecisionInput,
    direction: DirectionBias,
    *,
    rules: DefinedRiskSpreadRules | None = None,
) -> DefinedRiskSpreadCandidate | None:
    resolved_rules = rules or load_defined_risk_spread_rules()
    if not resolved_rules.enabled:
        return None
    if direction is DirectionBias.BULLISH:
        return _select_credit_spread(
            decision_input,
            direction=direction,
            option_type=OptionType.PUT,
            strategy="bull_put_spread",
            rules=resolved_rules,
        )
    if direction is DirectionBias.BEARISH:
        return _select_credit_spread(
            decision_input,
            direction=direction,
            option_type=OptionType.CALL,
            strategy="bear_call_spread",
            rules=resolved_rules,
        )
    return None


def append_defined_risk_spread_candidate(
    candidate: DefinedRiskSpreadCandidate,
    *,
    decision_id: str,
    journal_path: str | Path | None = None,
) -> Path:
    path = Path(journal_path) if journal_path is not None else defined_risk_spread_journal_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    record = {
        "schema_version": "defined_risk_spread.v1",
        "recorded_at": datetime.now(tz=UTC).isoformat(),
        "decision_id": decision_id,
        "candidate": candidate.to_json_dict(),
    }
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True))
        handle.write("\n")
    compact_jsonl_tail(path)
    return path


def _select_credit_spread(
    decision_input: DecisionInput,
    *,
    direction: DirectionBias,
    option_type: OptionType,
    strategy: str,
    rules: DefinedRiskSpreadRules,
) -> DefinedRiskSpreadCandidate | None:
    contracts = [
        contract
        for contract in decision_input.option_chain
        if contract.option_type is option_type and _contract_is_eligible(contract, decision_input=decision_input, rules=rules)
    ]
    candidates: list[DefinedRiskSpreadCandidate] = []
    for short_leg in contracts:
        for long_leg in contracts:
            if short_leg.expiration != long_leg.expiration:
                continue
            if strategy == "bull_put_spread" and not short_leg.strike > long_leg.strike:
                continue
            if strategy == "bear_call_spread" and not short_leg.strike < long_leg.strike:
                continue
            width = round(abs(short_leg.strike - long_leg.strike), 2)
            if not rules.min_width <= width <= rules.max_width:
                continue
            short_delta = abs(short_leg.delta)
            long_delta = abs(long_leg.delta)
            if not rules.short_delta_min <= short_delta <= rules.short_delta_max:
                continue
            if not rules.long_delta_min <= long_delta <= rules.long_delta_max:
                continue
            net_credit = round(short_leg.bid - long_leg.ask, 2)
            if net_credit < rules.min_credit:
                continue
            max_profit = round(net_credit * 100, 2)
            max_risk = round((width - net_credit) * 100, 2)
            if max_risk <= 0 or max_risk > rules.max_risk:
                continue
            risk_reward_ratio = round(max_profit / max_risk, 4)
            score = _spread_score(short_delta, long_delta, risk_reward_ratio, width, rules)
            candidates.append(
                DefinedRiskSpreadCandidate(
                    strategy=strategy,
                    underlying=decision_input.ticker.upper(),
                    direction=direction.value,
                    expiration=short_leg.expiration.isoformat(),
                    short_leg=_leg(short_leg, side="sell"),
                    long_leg=_leg(long_leg, side="buy"),
                    width=width,
                    net_credit=net_credit,
                    max_profit=max_profit,
                    max_risk=max_risk,
                    profit_target_debit=round(net_credit * (1 - rules.profit_target_pct), 2),
                    risk_reward_ratio=risk_reward_ratio,
                    score=score,
                    reasons=[
                        "alpaca_style_defined_risk_credit_spread",
                        "paired_short_long_option_legs",
                        "max_risk_bounded",
                        "profit_target_debit_defined",
                    ],
                )
            )
    return max(candidates, key=lambda candidate: candidate.score) if candidates else None


def _contract_is_eligible(
    contract: OptionContractSnapshot,
    *,
    decision_input: DecisionInput,
    rules: DefinedRiskSpreadRules,
) -> bool:
    dte = (contract.expiration - decision_input.timestamp.date()).days
    return (
        rules.min_dte <= dte <= rules.max_dte
        and contract.bid > 0
        and contract.ask > 0
        and contract.ask >= contract.bid
        and contract.volume > 0
        and contract.open_interest > 0
    )


def _spread_score(
    short_delta: float,
    long_delta: float,
    risk_reward_ratio: float,
    width: float,
    rules: DefinedRiskSpreadRules,
) -> float:
    short_fit = max(0.0, 1 - abs(short_delta - 0.35) / 0.35)
    long_fit = max(0.0, 1 - abs(long_delta - 0.20) / 0.30)
    risk_fit = min(1.0, risk_reward_ratio / 0.50)
    width_fit = max(0.0, 1 - (width - rules.min_width) / max(rules.max_width - rules.min_width, 1.0))
    return round(short_fit * 0.35 + long_fit * 0.25 + risk_fit * 0.25 + width_fit * 0.15, 4)


def _leg(contract: OptionContractSnapshot, *, side: str) -> SpreadLeg:
    return SpreadLeg(
        option_symbol=contract.option_symbol,
        side=side,
        option_type=contract.option_type.value,
        expiration=contract.expiration.isoformat(),
        strike=contract.strike,
        bid=contract.bid,
        ask=contract.ask,
        delta=contract.delta,
        volume=contract.volume,
        open_interest=contract.open_interest,
    )


def _env_bool(name: str, *, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}
