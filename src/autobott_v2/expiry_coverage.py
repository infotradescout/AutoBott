"""Bounded expiration coverage from the same chain the native capture receives.

Counts show observed listed contracts, not liquidity, tradability guarantees,
profitable opportunities, or coverage of contracts not returned by the provider.
"""
from __future__ import annotations

from datetime import UTC, date, datetime
import re
from typing import Any
from zoneinfo import ZoneInfo


_OCC = re.compile(r"^([A-Z][A-Z0-9.]*?)(\d{6})([CP])(\d{8})$")
_MAX_CONTRACTS = 30000


def summarize_expirations(symbol: str, snapshots: Any, *, as_of: date) -> dict[str, Any]:
    report: dict[str, Any] = {
        "symbol": symbol.upper(), "reference_date": as_of.isoformat(),
        "source": "native_chain_before_subset", "status": "unavailable",
        "contracts_returned": len(snapshots) if isinstance(snapshots, dict) else None,
        "contracts_counted": 0, "invalid_contracts": 0, "expirations": [],
        "is_profitability_evidence": False,
    }
    if not isinstance(snapshots, dict) or not snapshots:
        report["reason"] = "chain_missing_or_empty"
        return report
    if len(snapshots) > _MAX_CONTRACTS:
        report["reason"] = "coverage_size_limit"
        return report
    by_date: dict[date, dict[str, int]] = {}
    roots = {symbol.upper()}
    if symbol.upper() in {"VIX", "VIXW"}:
        roots = {"VIX", "VIXW"}
    for name, row in snapshots.items():
        match = _OCC.fullmatch(name) if isinstance(name, str) else None
        if not match or match[1] not in roots or not isinstance(row, dict):
            report["invalid_contracts"] += 1
            continue
        stamp = match[2]
        try:
            expiration = date(2000 + int(stamp[:2]), int(stamp[2:4]), int(stamp[4:6]))
        except ValueError:
            report["invalid_contracts"] += 1
            continue
        option_type = "call" if match[3] == "C" else "put"
        details = row.get("details") or row.get("option_details") or {}
        if not isinstance(details, dict):
            report["invalid_contracts"] += 1
            continue
        declared_expiration = details.get("expiration_date") or details.get("expiration")
        declared_type = details.get("type") or details.get("option_type")
        if (declared_expiration is not None and str(declared_expiration) != expiration.isoformat()
                or declared_type is not None and str(declared_type).lower() != option_type):
            report["invalid_contracts"] += 1
            continue
        counts = by_date.setdefault(expiration, {"call": 0, "put": 0})
        counts[option_type] += 1
        report["contracts_counted"] += 1
    report["expirations"] = [
        {"expiration": expiration.isoformat(), "calendar_dte": (expiration - as_of).days,
         "calls": counts["call"], "puts": counts["put"]}
        for expiration, counts in sorted(by_date.items())
    ]
    if report["contracts_counted"]:
        report["status"] = "partial" if report["invalid_contracts"] else "observed"
    else:
        report["reason"] = "no_valid_contract_identity"
    return report


class ExpiryCoverageClient:
    """Delegate unchanged provider calls; only summarize a returned chain."""
    def __init__(self, client: Any, records: list[dict[str, Any]]) -> None:
        self._client = client
        self._records = records

    def __getattr__(self, name: str) -> Any:
        return getattr(self._client, name)

    def get_option_chain_snapshots(self, symbol: str, **kwargs: Any) -> Any:
        # Do not swallow a provider error or retry: the existing capture owns it.
        snapshots = self._client.get_option_chain_snapshots(symbol, **kwargs)
        try:
            observed = datetime.now(UTC)
            report = summarize_expirations(
                symbol, snapshots, as_of=observed.astimezone(ZoneInfo("America/New_York")).date(),
            )
            report["observed_at"] = observed.isoformat()
            if len(self._records) < 103:
                self._records.append(report)
        except Exception:
            # Telemetry cannot change a valid capture, and failure is explicit.
            if len(self._records) < 103:
                self._records.append({"symbol": str(symbol), "status": "unavailable",
                                      "reason": "coverage_summary_failed"})
        return snapshots
