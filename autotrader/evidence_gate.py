"""Executor-side evidence gate based on conservative closed-trade P/L."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

import config


@dataclass(frozen=True)
class EvidenceDecision:
    allowed: bool
    reason: str = ""


def _safe_float(value, default: float | None = None) -> float | None:
    try:
        if value is None or str(value).strip() == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _score_bucket(value) -> str:
    score = _safe_float(value)
    if score is None:
        return ""
    if score < 3:
        return "[0-3)"
    if score < 5:
        return "[3-5)"
    if score < 7:
        return "[5-7)"
    return "[7+)"


def _spread_bucket(value) -> str:
    spread = _safe_float(value)
    if spread is None:
        return ""
    if spread <= 1.0:
        return "<=1"
    if spread <= 2.0:
        return "1-2"
    if spread <= 3.0:
        return "2-3"
    return ">3"


def _entry_hour(row: dict) -> str:
    for key in ("entry_time", "timestamp"):
        raw = str(row.get(key, "") or "").strip()
        if not raw:
            continue
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            return str(int(parsed.hour))
        except ValueError:
            pass
        if len(raw) >= 13 and raw[11:13].isdigit():
            return str(int(raw[11:13]))
    return ""


def _normalize_row(row: dict) -> dict:
    normalized = {str(k): v for k, v in row.items()}
    normalized["ticker"] = str(normalized.get("ticker", "") or "").upper()
    normalized["direction"] = str(normalized.get("direction", "") or "").lower()
    normalized["strategy_profile"] = str(normalized.get("strategy_profile", "") or "").lower()
    normalized["entry_hour"] = _entry_hour(normalized)
    normalized["score_bucket"] = _score_bucket(normalized.get("signal_score"))
    normalized["spread_bucket"] = _spread_bucket(
        normalized.get("entry_spread_pct") or normalized.get("contract_spread_pct")
    )
    normalized["exposure_bucket"] = str(normalized.get("exposure_bucket", "") or "").lower()
    normalized["conservative_pnl"] = _safe_float(normalized.get("conservative_executable_pnl_usd"))
    return normalized


def load_recent_trade_rows(path: Path | None = None) -> list[dict]:
    trades_path = path or config.TRADES_CSV_PATH
    if not trades_path.exists():
        return []
    try:
        with trades_path.open("r", newline="", encoding="utf-8") as f:
            rows = [_normalize_row(row) for row in csv.DictReader(f)]
    except Exception as exc:  # noqa: BLE001
        print(f"[evidence_gate] trade history unavailable: {exc}")
        return []
    rows = [row for row in rows if _safe_float(row.get("conservative_pnl")) is not None]
    recent_rows = max(0, int(getattr(config, "EVIDENCE_GATE_RECENT_ROWS", 1000) or 1000))
    if recent_rows > 0:
        rows = rows[-recent_rows:]
    return rows


def _candidate_from_signal(
    *,
    signal: dict,
    ticker: str,
    direction: str,
    now_et: datetime,
    exposure_bucket: str = "",
    spread_pct: float | None = None,
) -> dict:
    return {
        "ticker": str(ticker or signal.get("symbol", "") or "").upper(),
        "direction": str(direction or signal.get("direction", "") or "").lower(),
        "strategy_profile": str(signal.get("strategy_profile", "generic") or "generic").lower(),
        "entry_hour": str(int(now_et.hour)),
        "score_bucket": _score_bucket(signal.get("signal_score")),
        "spread_bucket": _spread_bucket(spread_pct),
        "exposure_bucket": str(exposure_bucket or "").lower(),
    }


def _matching_rows(rows: Iterable[dict], candidate: dict, keys: tuple[str, ...]) -> list[dict]:
    matches = []
    for row in rows:
        ok = True
        for key in keys:
            value = str(candidate.get(key, "") or "")
            if not value or str(row.get(key, "") or "") != value:
                ok = False
                break
        if ok:
            matches.append(row)
    return matches


def _block_reason(rows: list[dict], keys: tuple[str, ...], label: str) -> str:
    pnls = [float(row["conservative_pnl"]) for row in rows if _safe_float(row.get("conservative_pnl")) is not None]
    if not pnls:
        return ""
    min_samples = max(1, int(getattr(config, "EVIDENCE_GATE_MIN_SAMPLES", 3) or 3))
    min_losses = max(1, int(getattr(config, "EVIDENCE_GATE_MIN_LOSSES", 2) or 2))
    max_expectancy = float(getattr(config, "EVIDENCE_GATE_MAX_CONSERVATIVE_EXPECTANCY_USD", -0.01) or -0.01)
    losses = sum(1 for value in pnls if value < 0)
    expectancy = sum(pnls) / len(pnls)
    if len(pnls) < min_samples or losses < min_losses or expectancy > max_expectancy:
        return ""
    key_text = ",".join(keys)
    return (
        f"evidence gate blocked {label} bucket ({key_text}): "
        f"n={len(pnls)} losses={losses} conservative_exp=${expectancy:.2f}"
    )


def evaluate_signal(
    *,
    signal: dict,
    ticker: str,
    direction: str,
    now_et: datetime,
    rows: list[dict] | None = None,
) -> EvidenceDecision:
    if not bool(getattr(config, "ENABLE_EXECUTION_EVIDENCE_GATE", False)):
        return EvidenceDecision(True, "")
    history = rows if rows is not None else load_recent_trade_rows()
    if not history:
        return EvidenceDecision(True, "")
    candidate = _candidate_from_signal(signal=signal, ticker=ticker, direction=direction, now_et=now_et)
    groups = (
        ("ticker", "entry_hour", "direction"),
        ("ticker", "direction"),
        ("strategy_profile", "entry_hour", "direction"),
        ("score_bucket", "direction"),
    )
    for keys in groups:
        reason = _block_reason(_matching_rows(history, candidate, keys), keys, "pre-contract")
        if reason:
            return EvidenceDecision(False, reason)
    return EvidenceDecision(True, "")


def evaluate_contract(
    *,
    signal: dict,
    ticker: str,
    direction: str,
    now_et: datetime,
    exposure_bucket: str,
    spread_pct: float | None,
    rows: list[dict] | None = None,
) -> EvidenceDecision:
    if not bool(getattr(config, "ENABLE_EXECUTION_EVIDENCE_GATE", False)):
        return EvidenceDecision(True, "")
    history = rows if rows is not None else load_recent_trade_rows()
    if not history:
        return EvidenceDecision(True, "")
    candidate = _candidate_from_signal(
        signal=signal,
        ticker=ticker,
        direction=direction,
        now_et=now_et,
        exposure_bucket=exposure_bucket,
        spread_pct=spread_pct,
    )
    groups = (
        ("exposure_bucket", "direction"),
        ("ticker", "spread_bucket", "direction"),
        ("spread_bucket", "score_bucket", "direction"),
    )
    for keys in groups:
        reason = _block_reason(_matching_rows(history, candidate, keys), keys, "contract")
        if reason:
            return EvidenceDecision(False, reason)
    return EvidenceDecision(True, "")
