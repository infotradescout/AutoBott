"""Decision outcome evaluator for AutoBott.

Read-only learning layer.

Goal:
- Determine whether recent decisions were good or bad after the market had time
  to prove them right or wrong.
- Do not place orders.
- Do not auto-tune config.
- Produce evidence that can be used by a human/operator or future optimizer.

Outcome rules:
- CALL is directionally correct when the underlying is higher after the horizon.
- PUT is directionally correct when the underlying is lower after the horizon.
- An approved/pass decision is good when direction was correct.
- A rejected/watch-only decision is good when the avoided direction would not
  have worked, and bad when it would have worked.
"""

from __future__ import annotations

import csv
import math
import os
import re
from collections import Counter, defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import pytz

try:
    from autotrader import config
except ImportError:
    import config  # type: ignore

try:
    from data import AlpacaDataClient
    from decision_journal import build_decision_journal
except ImportError:
    from autotrader.data import AlpacaDataClient  # type: ignore
    from autotrader.decision_journal import build_decision_journal  # type: ignore

EASTERN = pytz.timezone(str(getattr(config, "EASTERN_TZ", "US/Eastern") or "US/Eastern"))
API_KEY = str(os.getenv("ALPACA_API_KEY") or "").strip()
SECRET_KEY = str(os.getenv("ALPACA_SECRET_KEY") or "").strip()
_EQUITY_SYMBOL_RE = re.compile(r"^[A-Z][A-Z.]{0,5}$")


@dataclass(frozen=True)
class PriceOutcome:
    evaluated: bool
    reason: str
    entry_price: float = 0.0
    end_price: float = 0.0
    move_pct: float = 0.0
    max_favorable_pct: float = 0.0
    max_adverse_pct: float = 0.0
    bars_used: int = 0


def _now_et() -> datetime:
    return datetime.now(EASTERN)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return float(default)
    if math.isnan(parsed) or math.isinf(parsed):
        return float(default)
    return parsed


def _parse_ts(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            return EASTERN.localize(dt)
        return dt.astimezone(EASTERN)
    except ValueError:
        pass
    for suffix in (" EDT", " EST", " CDT", " CST"):
        if raw.upper().endswith(suffix.strip()):
            base = raw[: -len(suffix)].strip()
            try:
                return EASTERN.localize(datetime.strptime(base, "%Y-%m-%d %H:%M:%S"))
            except ValueError:
                return None
    try:
        return EASTERN.localize(datetime.strptime(raw, "%Y-%m-%d %H:%M:%S"))
    except ValueError:
        return None


def _is_equity_symbol(symbol: str) -> bool:
    raw = str(symbol or "").upper().strip()
    return bool(_EQUITY_SYMBOL_RE.match(raw))


def _decision_direction(item: dict[str, Any]) -> str:
    direction = str(item.get("direction", "") or "").upper().strip()
    if direction in {"CALL", "PUT"}:
        return direction
    reason = str(item.get("reason", "") or "")
    if " CALL " in f" {reason.upper()} ":
        return "CALL"
    if " PUT " in f" {reason.upper()} ":
        return "PUT"
    return ""


def _read_csv_tail(path: Path, limit: int = 5000) -> list[dict[str, str]]:
    if not path.exists():
        return []
    try:
        with path.open("r", newline="", encoding="utf-8") as handle:
            return list(deque(csv.DictReader(handle), maxlen=max(1, int(limit))))
    except Exception:
        return []


def _directional_outcome(
    *,
    symbol: str,
    direction: str,
    timestamp: datetime,
    horizon_minutes: int,
    data_client: AlpacaDataClient,
) -> PriceOutcome:
    end_et = min(_now_et(), timestamp + timedelta(minutes=max(1, int(horizon_minutes))))
    if end_et <= timestamp + timedelta(minutes=1):
        return PriceOutcome(False, "not enough elapsed time")

    try:
        bars = data_client.get_intraday_bars_window(
            symbol=symbol,
            start_et=timestamp - timedelta(minutes=5),
            end_et=end_et,
            limit=max(20, int(horizon_minutes // 5) + 10),
        )
    except Exception as exc:  # noqa: BLE001
        return PriceOutcome(False, f"bar fetch failed: {exc}")

    if bars is None or bars.empty or "close" not in bars.columns:
        return PriceOutcome(False, "no bars available")

    df = bars.copy()
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce").dt.tz_convert(EASTERN)
        df = df[df["timestamp"] >= timestamp - timedelta(minutes=1)]
    if df.empty or len(df) < 2:
        return PriceOutcome(False, "not enough bars after decision")

    closes = df["close"].astype(float)
    highs = df["high"].astype(float) if "high" in df.columns else closes
    lows = df["low"].astype(float) if "low" in df.columns else closes
    entry_price = float(closes.iloc[0])
    end_price = float(closes.iloc[-1])
    if entry_price <= 0:
        return PriceOutcome(False, "invalid entry price")

    raw_move_pct = ((end_price - entry_price) / entry_price) * 100.0
    if direction == "CALL":
        favorable = ((float(highs.max()) - entry_price) / entry_price) * 100.0
        adverse = ((float(lows.min()) - entry_price) / entry_price) * 100.0
        directional_move = raw_move_pct
    elif direction == "PUT":
        favorable = ((entry_price - float(lows.min())) / entry_price) * 100.0
        adverse = ((entry_price - float(highs.max())) / entry_price) * 100.0
        directional_move = -raw_move_pct
    else:
        return PriceOutcome(False, "missing CALL/PUT direction")

    return PriceOutcome(
        True,
        "ok",
        entry_price=round(entry_price, 4),
        end_price=round(end_price, 4),
        move_pct=round(directional_move, 4),
        max_favorable_pct=round(favorable, 4),
        max_adverse_pct=round(adverse, 4),
        bars_used=len(df),
    )


def _verdict_for_decision(item: dict[str, Any], outcome: PriceOutcome) -> tuple[str, str, int]:
    """Return (verdict, lesson, score_delta)."""
    if not outcome.evaluated:
        return "unevaluated", outcome.reason, 0

    decision = str(item.get("decision", "") or "").lower()
    source = str(item.get("source", "") or "").lower()
    move_threshold = float(getattr(config, "DECISION_OUTCOME_MIN_MOVE_PCT", 0.05) or 0.05)
    strong_threshold = float(getattr(config, "DECISION_OUTCOME_STRONG_MOVE_PCT", 0.20) or 0.20)

    worked = outcome.move_pct >= move_threshold
    strongly_worked = outcome.max_favorable_pct >= strong_threshold and outcome.move_pct >= 0
    failed = outcome.move_pct <= -move_threshold

    is_go = decision in {"pass", "approved", "submitted_buy", "filled"} or source == "alpaca_order"
    is_no_go = decision in {"reject", "scanner_rejected", "watch_only", "skip", "rejected", "canceled", "expired"}

    if is_go:
        if strongly_worked:
            return "good_go", "approved direction produced favorable follow-through", 2
        if worked:
            return "acceptable_go", "approved direction moved correctly but not strongly", 1
        if failed:
            return "bad_go", "approved direction moved against the decision", -2
        return "neutral_go", "approved direction did not move enough to prove edge", 0

    if is_no_go:
        if strongly_worked:
            return "bad_block", "blocked setup later moved well in the blocked direction", -2
        if worked:
            return "questionable_block", "blocked setup moved modestly in the blocked direction", -1
        if failed:
            return "good_block", "blocked setup avoided a wrong-direction move", 2
        return "neutral_block", "blocked setup did not move enough to matter", 0

    return "neutral", "decision type is informational, not a trade go/no-go", 0


def _evaluate_decision(
    item: dict[str, Any],
    *,
    data_client: AlpacaDataClient,
    horizon_minutes: int,
) -> dict[str, Any] | None:
    source = str(item.get("source", "") or "")
    if source not in {"scanner", "entry_accuracy", "vix_proxy"}:
        return None

    symbol = str(item.get("symbol", "") or "").upper().strip()
    if source == "vix_proxy":
        symbol = str(item.get("symbol", "") or "VIXY").upper().strip() or "VIXY"
    if not _is_equity_symbol(symbol):
        return None

    direction = _decision_direction(item)
    if direction not in {"CALL", "PUT"}:
        return None

    timestamp = _parse_ts(item.get("timestamp"))
    if timestamp is None:
        return None

    outcome = _directional_outcome(
        symbol=symbol,
        direction=direction,
        timestamp=timestamp,
        horizon_minutes=horizon_minutes,
        data_client=data_client,
    )
    verdict, lesson, score_delta = _verdict_for_decision(item, outcome)
    metrics = item.get("metrics", {}) if isinstance(item.get("metrics"), dict) else {}

    return {
        "timestamp": item.get("timestamp", ""),
        "source": source,
        "stage": item.get("stage", ""),
        "symbol": symbol,
        "direction": direction,
        "decision": item.get("decision", ""),
        "reason": item.get("reason", ""),
        "summary": item.get("summary", ""),
        "horizon_minutes": horizon_minutes,
        "evaluated": outcome.evaluated,
        "verdict": verdict,
        "lesson": lesson,
        "score_delta": score_delta,
        "entry_price": outcome.entry_price,
        "end_price": outcome.end_price,
        "directional_move_pct": outcome.move_pct,
        "max_favorable_pct": outcome.max_favorable_pct,
        "max_adverse_pct": outcome.max_adverse_pct,
        "bars_used": outcome.bars_used,
        "metrics": metrics,
    }


def _summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    evaluated = [row for row in rows if row.get("evaluated")]
    by_verdict = Counter(str(row.get("verdict", "unknown")) for row in rows)
    by_source = Counter(str(row.get("source", "unknown")) for row in rows)
    by_stage = Counter(str(row.get("stage", "unknown")) for row in rows)
    score_total = sum(int(row.get("score_delta", 0) or 0) for row in evaluated)

    grouped: dict[str, dict[str, Any]] = defaultdict(lambda: {"count": 0, "score": 0, "avg_move_pct": 0.0, "moves": []})
    for row in evaluated:
        key = f"{row.get('source')}::{row.get('stage')}::{row.get('decision')}"
        grouped[key]["count"] += 1
        grouped[key]["score"] += int(row.get("score_delta", 0) or 0)
        grouped[key]["moves"].append(float(row.get("directional_move_pct", 0.0) or 0.0))

    groups = []
    for key, payload in grouped.items():
        moves = payload.pop("moves", [])
        avg_move = sum(moves) / len(moves) if moves else 0.0
        parts = key.split("::")
        groups.append(
            {
                "source": parts[0],
                "stage": parts[1],
                "decision": parts[2],
                "count": payload["count"],
                "score": payload["score"],
                "avg_directional_move_pct": round(avg_move, 4),
            }
        )
    groups.sort(key=lambda item: (int(item["score"]), -int(item["count"])))

    lessons = []
    bad_blocks = [row for row in evaluated if row.get("verdict") in {"bad_block", "questionable_block"}]
    bad_go = [row for row in evaluated if row.get("verdict") == "bad_go"]
    good_go = [row for row in evaluated if row.get("verdict") in {"good_go", "acceptable_go"}]
    good_blocks = [row for row in evaluated if row.get("verdict") == "good_block"]

    if bad_blocks:
        common = Counter(str(row.get("reason", "")) for row in bad_blocks).most_common(5)
        lessons.append({"type": "missed_opportunity", "message": "Some blocked/watch-only decisions moved in the predicted direction.", "examples": [{"reason": key, "count": value} for key, value in common]})
    if bad_go:
        common = Counter(str(row.get("reason", "")) for row in bad_go).most_common(5)
        lessons.append({"type": "bad_approval", "message": "Some approved/pass decisions moved against the chosen direction.", "examples": [{"reason": key, "count": value} for key, value in common]})
    if good_go:
        lessons.append({"type": "working_signal", "message": f"{len(good_go)} approved/pass decisions had positive follow-through."})
    if good_blocks:
        lessons.append({"type": "useful_filter", "message": f"{len(good_blocks)} blocks avoided wrong-direction movement."})

    return {
        "total_rows": len(rows),
        "evaluated_rows": len(evaluated),
        "unevaluated_rows": len(rows) - len(evaluated),
        "score_total": score_total,
        "verdict_counts": dict(by_verdict),
        "source_counts": dict(by_source),
        "stage_counts": dict(by_stage),
        "groups_worst_first": groups[:20],
        "lessons": lessons,
    }


def build_decision_outcomes(*, journal_limit: int = 200, horizon_minutes: int = 15) -> dict[str, Any]:
    if not API_KEY or not SECRET_KEY:
        return {
            "generated_at_et": _now_et().isoformat(),
            "ok": False,
            "error": "Alpaca API keys missing; cannot evaluate outcomes from market bars.",
            "summary": {},
            "outcomes": [],
        }

    data_client = AlpacaDataClient(API_KEY, SECRET_KEY, paper=bool(getattr(config, "PAPER", True)))
    journal = build_decision_journal(limit=max(50, min(500, int(journal_limit))))
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str, str]] = set()

    for item in journal.get("decisions", []):
        if not isinstance(item, dict):
            continue
        evaluated = _evaluate_decision(item, data_client=data_client, horizon_minutes=horizon_minutes)
        if not evaluated:
            continue
        key = (
            str(evaluated.get("timestamp", "")),
            str(evaluated.get("source", "")),
            str(evaluated.get("symbol", "")),
            str(evaluated.get("direction", "")),
            str(evaluated.get("decision", "")),
        )
        if key in seen:
            continue
        seen.add(key)
        rows.append(evaluated)

    return {
        "generated_at_et": _now_et().isoformat(),
        "ok": True,
        "horizon_minutes": int(horizon_minutes),
        "summary": _summarize(rows),
        "outcomes": rows,
    }


if __name__ == "__main__":
    import json

    print(json.dumps(build_decision_outcomes(journal_limit=200, horizon_minutes=15), indent=2))
