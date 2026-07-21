from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .runtime_paths import data_root

# Late imports from vix_trader happen inside functions to avoid an import cycle.


CLOSED_LIFECYCLE_STATES = frozenset({"CLOSED", "RECONCILED"})


@dataclass(frozen=True)
class VixEvidenceRules:
    """Mirror Phase 1 bucket gates: promote only what the sample actually supports."""

    min_closed_cycles: int = 50
    min_profit_factor: float = 1.25
    min_expectancy: float = 0.0
    max_drawdown_pct_of_capital: float = 5.0
    min_win_rate: float = 0.0


@dataclass(frozen=True)
class VixEvidenceResolution:
    config: Any
    source: str
    fingerprint: str | None
    closed_cycles: int
    blocking_reasons: tuple[str, ...]
    candidate_summaries: tuple[dict[str, Any], ...]
    evidence_artifact_id: str | None
    profitability_status: str
    selected_metrics: dict[str, Any] = field(default_factory=dict)

    def to_json_dict(self) -> dict[str, Any]:
        config_payload = self.config.to_json_dict() if self.config is not None else None
        missing = self.config.missing_required_fields() if self.config is not None else []
        return {
            "config": config_payload,
            "source": self.source,
            "fingerprint": self.fingerprint,
            "closed_cycles": self.closed_cycles,
            "blocking_reasons": list(self.blocking_reasons),
            "candidate_summaries": list(self.candidate_summaries),
            "evidence_artifact_id": self.evidence_artifact_id,
            "profitability_status": self.profitability_status,
            "selected_metrics": self.selected_metrics,
            "executable": self.config is not None and not missing,
        }


STRATEGY_PARAM_KEYS = (
    "minimum_full_trading_sessions_remaining",
    "maximum_days_to_expiration",
    "maximum_combined_debit",
    "maximum_cycle_allocation",
    "first_leg_exit_target_pct",
    "second_leg_management_rule",
    "maximum_additions",
    "maximum_additional_capital",
    "addition_sizing",
    "addition_trigger",
)


def vix_evidence_path() -> Path:
    return data_root() / "vix_trader" / "evidence.json"


def vix_strategy_fingerprint(config: Any) -> str:
    payload = {key: getattr(config, key) for key in STRATEGY_PARAM_KEYS}
    payload["preferred_entry_min"] = config.preferred_entry_min
    payload["preferred_entry_max"] = config.preferred_entry_max
    payload["enabled_entry_min"] = config.enabled_entry_min
    payload["enabled_entry_max"] = config.enabled_entry_max
    payload["accepted_products"] = [item.value for item in config.accepted_products]
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:16]


def vix_parameter_candidates() -> tuple[Any, ...]:
    """Predeclared search grid only. None of these are executable until evidence promotes one."""

    from .vix_trader import VixProduct, VixStrategyConfig

    shared = {
        "preferred_entry_min": 17.0,
        "preferred_entry_max": 17.99,
        "enabled_entry_min": 16.0,
        "enabled_entry_max": 19.99,
        "accepted_products": (VixProduct.VIX, VixProduct.VIXW),
        "second_leg_management_rule": "hold_remaining_leg_until_reversal_or_deadline",
        "addition_trigger": "confirmed_opposite_move",
        "addition_sizing": 1,
    }
    return (
        VixStrategyConfig(
            minimum_full_trading_sessions_remaining=3,
            maximum_days_to_expiration=10,
            maximum_combined_debit=8.0,
            maximum_cycle_allocation=1_500.0,
            first_leg_exit_target_pct=0.30,
            maximum_additions=1,
            maximum_additional_capital=400.0,
            **shared,
        ),
        VixStrategyConfig(
            minimum_full_trading_sessions_remaining=4,
            maximum_days_to_expiration=7,
            maximum_combined_debit=6.0,
            maximum_cycle_allocation=1_000.0,
            first_leg_exit_target_pct=0.50,
            maximum_additions=1,
            maximum_additional_capital=300.0,
            **shared,
        ),
        VixStrategyConfig(
            minimum_full_trading_sessions_remaining=3,
            maximum_days_to_expiration=14,
            maximum_combined_debit=10.0,
            maximum_cycle_allocation=2_000.0,
            first_leg_exit_target_pct=0.25,
            maximum_additions=0,
            maximum_additional_capital=1.0,
            addition_trigger="disabled",
            preferred_entry_min=17.0,
            preferred_entry_max=17.99,
            enabled_entry_min=16.0,
            enabled_entry_max=19.99,
            accepted_products=(VixProduct.VIX, VixProduct.VIXW),
            second_leg_management_rule="hold_remaining_leg_until_reversal_or_deadline",
            addition_sizing=1,
        ),
    )


def apply_operator_ceilings(config: Any, ceilings: Any | None) -> Any:
    """Operator values may only tighten risk; they cannot invent an unproven strategy."""

    from .vix_trader import VixStrategyConfig

    if ceilings is None:
        return config
    updates: dict[str, Any] = {}
    if ceilings.maximum_combined_debit is not None and config.maximum_combined_debit is not None:
        updates["maximum_combined_debit"] = min(config.maximum_combined_debit, ceilings.maximum_combined_debit)
    if ceilings.maximum_cycle_allocation is not None and config.maximum_cycle_allocation is not None:
        updates["maximum_cycle_allocation"] = min(config.maximum_cycle_allocation, ceilings.maximum_cycle_allocation)
    if ceilings.maximum_additional_capital is not None and config.maximum_additional_capital is not None:
        updates["maximum_additional_capital"] = min(config.maximum_additional_capital, ceilings.maximum_additional_capital)
    if ceilings.maximum_additions is not None and config.maximum_additions is not None:
        updates["maximum_additions"] = min(config.maximum_additions, ceilings.maximum_additions)
    if ceilings.maximum_days_to_expiration is not None and config.maximum_days_to_expiration is not None:
        updates["maximum_days_to_expiration"] = min(config.maximum_days_to_expiration, ceilings.maximum_days_to_expiration)
    if not updates:
        return config
    return VixStrategyConfig(**{**asdict(config), "accepted_products": config.accepted_products, **updates})


def _cycle_fingerprint(row: dict[str, Any]) -> str | None:
    from .vix_trader import vix_strategy_config_from_dict

    payload = row.get("strategy_payload") or {}
    fingerprint = payload.get("configuration_fingerprint")
    if fingerprint:
        return str(fingerprint)
    embedded = payload.get("strategy_configuration")
    if isinstance(embedded, dict):
        try:
            return vix_strategy_fingerprint(vix_strategy_config_from_dict(embedded))
        except (TypeError, ValueError, KeyError):
            return None
    return None


def _closed_cycle_pnls(rows: list[dict[str, Any]], fingerprint: str) -> list[dict[str, float]]:
    outcomes: list[dict[str, float]] = []
    for row in rows:
        if str(row.get("lifecycle_state") or "") not in CLOSED_LIFECYCLE_STATES:
            continue
        if _cycle_fingerprint(row) != fingerprint:
            continue
        pnl = float(row.get("combined_cycle_pnl") or 0.0)
        capital = float((row.get("strategy_payload") or {}).get("maximum_cycle_capital") or 0.0)
        if capital <= 0:
            capital = float(row.get("capital_committed") or 0.0)
        drawdown = float(row.get("maximum_drawdown") or 0.0)
        outcomes.append({"pnl": pnl, "capital": capital, "drawdown": drawdown})
    return outcomes


def summarize_candidate_outcomes(outcomes: list[dict[str, float]]) -> dict[str, Any]:
    closed = len(outcomes)
    if closed == 0:
        return {
            "closed_cycles": 0,
            "wins": 0,
            "losses": 0,
            "win_rate": 0.0,
            "profit_factor": 0.0,
            "expectancy": 0.0,
            "net_profit": 0.0,
            "max_drawdown_pct_of_capital": 0.0,
        }
    pnls = [row["pnl"] for row in outcomes]
    wins = [value for value in pnls if value > 0]
    losses = [value for value in pnls if value < 0]
    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))
    net_profit = sum(pnls)
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else (float("inf") if gross_profit > 0 else 0.0)
    if profit_factor == float("inf"):
        profit_factor = 999.0
    capitals = [row["capital"] for row in outcomes if row["capital"] > 0]
    avg_capital = (sum(capitals) / len(capitals)) if capitals else 0.0
    max_dd = max((row["drawdown"] for row in outcomes), default=0.0)
    dd_pct = (max_dd / avg_capital * 100.0) if avg_capital > 0 else (100.0 if max_dd > 0 else 0.0)
    return {
        "closed_cycles": closed,
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": round(len(wins) / closed, 4),
        "profit_factor": round(profit_factor, 4),
        "expectancy": round(net_profit / closed, 4),
        "net_profit": round(net_profit, 2),
        "max_drawdown_pct_of_capital": round(dd_pct, 4),
    }


def candidate_blocking_reasons(metrics: dict[str, Any], rules: VixEvidenceRules) -> list[str]:
    reasons: list[str] = []
    if metrics["closed_cycles"] < rules.min_closed_cycles:
        reasons.append("closed_cycles_below_evidence_threshold")
    if metrics["profit_factor"] < rules.min_profit_factor:
        reasons.append("profit_factor_below_evidence_threshold")
    if metrics["expectancy"] <= rules.min_expectancy:
        reasons.append("expectancy_not_positive")
    if metrics["max_drawdown_pct_of_capital"] > rules.max_drawdown_pct_of_capital:
        reasons.append("max_drawdown_above_evidence_threshold")
    if metrics["win_rate"] < rules.min_win_rate:
        reasons.append("win_rate_below_evidence_threshold")
    return reasons


def evaluate_vix_candidates(
    *,
    cycles: list[dict[str, Any]] | None = None,
    candidates: tuple[Any, ...] | None = None,
    rules: VixEvidenceRules | None = None,
) -> list[dict[str, Any]]:
    from .vix_trader import load_vix_cycles

    rules = rules or VixEvidenceRules()
    rows = cycles if cycles is not None else load_vix_cycles(limit=100_000)
    summaries: list[dict[str, Any]] = []
    for candidate in candidates or vix_parameter_candidates():
        fingerprint = vix_strategy_fingerprint(candidate)
        metrics = summarize_candidate_outcomes(_closed_cycle_pnls(rows, fingerprint))
        blockers = candidate_blocking_reasons(metrics, rules)
        summaries.append(
            {
                "fingerprint": fingerprint,
                "eligible": not blockers,
                "blocking_reasons": blockers,
                "metrics": metrics,
                "config": candidate.to_json_dict(),
            }
        )
    summaries.sort(
        key=lambda row: (
            row["eligible"],
            row["metrics"]["expectancy"],
            row["metrics"]["profit_factor"],
            row["metrics"]["closed_cycles"],
        ),
        reverse=True,
    )
    return summaries


def resolve_vix_strategy_config(
    *,
    cycles: list[dict[str, Any]] | None = None,
    candidates: tuple[Any, ...] | None = None,
    rules: VixEvidenceRules | None = None,
    ceilings: Any | None = None,
    evidence_path: str | Path | None = None,
) -> VixEvidenceResolution:
    """Select executable VIX params only from proven candidate outcomes.

    Operator ceilings may tighten risk. Manual filled-in strategy forms are not authority.
    """

    from .vix_trader import load_vix_strategy_config, vix_strategy_config_from_dict

    rules = rules or VixEvidenceRules()
    operator_ceilings = ceilings if ceilings is not None else load_vix_strategy_config()
    artifact = _load_evidence_artifact(evidence_path)
    if artifact is not None:
        promoted = _resolution_from_artifact(artifact, operator_ceilings)
        if promoted is not None:
            return promoted

    summaries = evaluate_vix_candidates(cycles=cycles, candidates=candidates, rules=rules)
    total_closed = sum(int(row["metrics"]["closed_cycles"]) for row in summaries)
    winners = [row for row in summaries if row["eligible"]]
    if not winners:
        reasons = ["no_candidate_meets_evidence_thresholds"]
        if total_closed == 0:
            reasons = ["no_closed_vix_cycles_with_configuration_fingerprint"]
        elif total_closed < rules.min_closed_cycles:
            reasons = ["closed_cycles_below_evidence_threshold"]
        return VixEvidenceResolution(
            config=None,
            source="none",
            fingerprint=None,
            closed_cycles=total_closed,
            blocking_reasons=tuple(reasons),
            candidate_summaries=tuple(summaries),
            evidence_artifact_id=None,
            profitability_status="insufficient_evidence",
        )

    selected = winners[0]
    config = apply_operator_ceilings(vix_strategy_config_from_dict(selected["config"]), operator_ceilings)
    return VixEvidenceResolution(
        config=config,
        source="evidence",
        fingerprint=str(selected["fingerprint"]),
        closed_cycles=int(selected["metrics"]["closed_cycles"]),
        blocking_reasons=(),
        candidate_summaries=tuple(summaries),
        evidence_artifact_id=None,
        profitability_status="evidence_selected",
        selected_metrics=dict(selected["metrics"]),
    )


def write_vix_evidence_report(
    resolution: VixEvidenceResolution | None = None,
    *,
    path: str | Path | None = None,
) -> Path:
    resolved = resolution or resolve_vix_strategy_config()
    target = Path(path) if path is not None else vix_evidence_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    artifact_id = f"vix-evidence-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}"
    payload = {
        "schema_version": "vix_evidence_report.v1",
        "artifact_id": artifact_id,
        "generated_at": datetime.now(UTC).isoformat(),
        "resolution": resolved.to_json_dict(),
    }
    temporary = target.with_suffix(".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temporary.replace(target)
    return target


def _load_evidence_artifact(path: str | Path | None) -> dict[str, Any] | None:
    target = Path(path) if path is not None else vix_evidence_path()
    if not target.exists():
        return None
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def _resolution_from_artifact(artifact: dict[str, Any], ceilings: Any) -> VixEvidenceResolution | None:
    from .vix_trader import vix_strategy_config_from_dict

    resolution = artifact.get("resolution")
    if not isinstance(resolution, dict):
        return None
    if resolution.get("profitability_status") != "evidence_selected":
        return None
    config_payload = resolution.get("config")
    if not isinstance(config_payload, dict):
        return None
    config = apply_operator_ceilings(vix_strategy_config_from_dict(config_payload), ceilings)
    if config.missing_required_fields() or config.validation_errors():
        return None
    return VixEvidenceResolution(
        config=config,
        source="evidence_artifact",
        fingerprint=str(resolution.get("fingerprint") or vix_strategy_fingerprint(config)),
        closed_cycles=int(resolution.get("closed_cycles") or 0),
        blocking_reasons=(),
        candidate_summaries=tuple(resolution.get("candidate_summaries") or ()),
        evidence_artifact_id=str(artifact.get("artifact_id") or ""),
        profitability_status="evidence_selected",
        selected_metrics=dict(resolution.get("selected_metrics") or {}),
    )
