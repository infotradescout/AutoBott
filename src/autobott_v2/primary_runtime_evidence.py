"""Read-only continuation of primary-entry evidence outside the entry window.

The hosted scanner stops opening entries at its configured end time. Existing
watches can still need broker-fill and quote observations after that time.
This helper performs only those evidence reads; it never scans, submits,
cancels, exits, or changes risk/accounting state.
"""
from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

from .execution_broker import AlpacaExecutionBroker
from .phase1_alpaca_client import AlpacaPaperClient
from .primary_fill_capture import poll_primary_fills
from .primary_development_metrics import materialize_primary_development_metrics
from .primary_followthrough import poll_primary_observations
from .primary_quality_runtime import evaluate_completed_primary_watches
from .runtime_paths import artifacts_root


def _empty_entry_quality_summary() -> dict[str, Any]:
    return {
        "checked": 0, "evaluated": 0, "already_evaluated": 0,
        "not_configured": 0, "not_ready": 0, "errors": [],
        "quality_statuses": {}, "underlying_diagnostics": {},
        "broker_reads": 0, "broker_writes": 0, "journal_writes": 0,
        "edge_established": False,
    }


def poll_primary_runtime_evidence_once(
    *,
    broker: Any | None = None,
    data_client: Any | None = None,
    now_fn: Callable[[], datetime] | None = None,
    root: str | Path | None = None,
) -> dict[str, Any]:
    """Advance existing primary watches without creating any new trading action."""
    evidence_root = Path(root) if root is not None else artifacts_root() / "primary_followthrough"
    if not evidence_root.exists() or not any(evidence_root.glob("*.json")):
        return {
            "enabled": True,
            "root": str(evidence_root),
            "fills": {"checked": 0, "filled": 0, "pending": 0, "errors": [],
                      "broker_writes": 0, "journal_writes": 0},
            "observations": {"checked": 0, "observed": 0, "window_closed": 0,
                             "errors": [], "underlying_observed": 0,
                             "underlying_errors": [], "broker_writes": 0},
            "entry_quality": _empty_entry_quality_summary(),
            "development_metrics": {
                "checked": 0, "materialized": 0, "already_materialized": 0,
                "not_development": 0, "not_ready": 0, "errors": [],
                "passes": None, "fails": None, "edge_established": False,
                "eligible_for_holdout": False, "broker_reads": 0,
                "broker_writes": 0, "journal_writes": 0,
            },
            "trading_actions": 0,
        }

    clock = now_fn or (lambda: datetime.now(tz=UTC))
    resolved_broker = broker or AlpacaExecutionBroker()
    resolved_data = data_client or AlpacaPaperClient()

    # Read the broker first so a just-completed fill can anchor the evidence
    # window before the quote recorder decides whether the watch is due/expired.
    try:
        fills = poll_primary_fills(evidence_root, resolved_broker, now_fn=clock)
    except Exception as exc:
        fills = {"checked": 0, "filled": 0, "pending": 0,
                 "errors": [{"reason": f"{type(exc).__name__}:{exc}"}],
                 "broker_writes": 0, "journal_writes": 0}
    try:
        observations = poll_primary_observations(evidence_root, resolved_data, now_fn=clock)
    except Exception as exc:
        observations = {"checked": 0, "observed": 0, "window_closed": 0,
                        "errors": [{"reason": f"{type(exc).__name__}:{exc}"}],
                        "underlying_observed": 0, "underlying_errors": [],
                        "broker_writes": 0}

    # A watch can close after new entries stop. Score it in this same
    # continuation using its already-bound protocol; never load current
    # environment rules or turn development capture into a scored study.
    try:
        entry_quality = evaluate_completed_primary_watches(evidence_root)
    except Exception as exc:
        entry_quality = {
            **_empty_entry_quality_summary(),
            "errors": [{"reason": f"{type(exc).__name__}:{exc}"}],
        }

    try:
        development_metrics = materialize_primary_development_metrics(evidence_root)
    except Exception as exc:
        development_metrics = {
            "checked": 0, "materialized": 0, "already_materialized": 0,
            "not_development": 0, "not_ready": 0,
            "errors": [{"reason": f"{type(exc).__name__}:{exc}"}],
            "passes": None, "fails": None, "edge_established": False,
            "eligible_for_holdout": False, "broker_reads": 0,
            "broker_writes": 0, "journal_writes": 0,
        }

    return {
        "enabled": True,
        "root": str(evidence_root),
        "fills": fills,
        "observations": observations,
        "entry_quality": entry_quality,
        "development_metrics": development_metrics,
        "trading_actions": 0,
    }
