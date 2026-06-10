"""Semantic outcome ledger for closed trades and skipped decisions.

This module does not trade or tune strategy. It converts plumbing outcomes
(scanner pass, order filled, trade closed) into trading outcome labels:
direction, entry, contract, exit, risk, setup, and near-miss quality.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
from collections import Counter, defaultdict, deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    from autotrader import config
except ImportError:
    import config  # type: ignore


OUTCOME_COLUMNS = [
    "record_id",
    "record_type",
    "timestamp",
    "date",
    "ticker",
    "direction",
    "strategy_profile",
    "setup_key",
    "option_symbol",
    "source_decision",
    "reject_reason",
    "realized_pnl_usd",
    "entry_price",
    "exit_price",
    "qty",
    "underlying_move_1m_pct",
    "underlying_move_3m_pct",
    "underlying_move_5m_pct",
    "underlying_move_exit_pct",
    "direction_correct_1m",
    "direction_correct_3m",
    "direction_correct_5m",
    "direction_correct_exit",
    "direction_quality",
    "entry_quality",
    "contract_quality",
    "exit_quality",
    "risk_quality",
    "setup_quality",
    "near_miss_quality",
    "planned_risk_usd",
    "actual_loss_usd",
    "stop_miss_usd",
    "stop_miss_pct",
    "notes",
]

GOOD_DIRECTION = "correct_direction"
BAD_DIRECTION = "wrong_direction"
UNKNOWN_DIRECTION = "direction_unknown"


@dataclass(frozen=True)
class SemanticsConfig:
    direction_move_threshold_pct: float = 0.05
    timely_move_threshold_pct: float = 0.05
    chased_move_threshold_pct: float = 0.30
    high_spread_pct: float = 3.5
    poor_fill_slippage_pct: float = 1.5
    stop_miss_tolerance_usd: float = 5.0


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(str(value).replace("$", "").replace("%", "").replace(",", "").strip())
    except (TypeError, ValueError):
        return float(default)
    if math.isnan(parsed) or math.isinf(parsed):
        return float(default)
    return parsed


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return int(default)


def _bool_text(value: bool | None) -> str:
    if value is None:
        return ""
    return "1" if value else "0"


def _directional_move(raw_move_pct: float, direction: str) -> float:
    direction_lc = str(direction or "").lower()
    if direction_lc == "call":
        return raw_move_pct
    if direction_lc == "put":
        return -raw_move_pct
    return 0.0


def _direction_correct(raw_move_pct: float | None, direction: str, cfg: SemanticsConfig) -> bool | None:
    if raw_move_pct is None:
        return None
    return _directional_move(float(raw_move_pct), direction) >= cfg.direction_move_threshold_pct


def _row_move(row: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        raw = row.get(key)
        if raw is None or str(raw).strip() == "":
            continue
        return _safe_float(raw, 0.0)
    return None


def _entry_exit_underlying_move(row: dict[str, Any]) -> float | None:
    explicit = _row_move(row, "underlying_move_exit_pct", "loss_underlying_move_pct")
    if explicit is not None:
        return explicit
    entry = _safe_float(row.get("entry_underlying_price"), 0.0)
    exit_ = _safe_float(row.get("exit_underlying_price"), 0.0)
    if entry > 0 and exit_ > 0:
        return ((exit_ - entry) / entry) * 100.0
    return None


def _premium_usd(row: dict[str, Any]) -> float:
    qty = max(0, _safe_int(row.get("qty"), 0))
    entry = _safe_float(row.get("entry_price"), 0.0)
    return entry * qty * 100.0


def _planned_risk_usd(row: dict[str, Any]) -> float:
    explicit = _safe_float(row.get("planned_risk_usd") or row.get("stop_loss_usd"), 0.0)
    if explicit > 0:
        return explicit
    return abs(float(getattr(config, "STOP_LOSS_USD", 0.0) or 0.0))


def _record_id(row: dict[str, Any], record_type: str) -> str:
    stable = "|".join(
        str(row.get(key, ""))
        for key in ("timestamp", "date", "ticker", "symbol", "direction", "option_symbol", "exit_reason", "reason")
    )
    return hashlib.sha256(f"{record_type}|{stable}".encode("utf-8", errors="ignore")).hexdigest()[:24]


def classify_direction(row: dict[str, Any], cfg: SemanticsConfig | None = None) -> dict[str, Any]:
    cfg = cfg or SemanticsConfig()
    direction = str(row.get("direction", "") or "").lower()
    moves = {
        "1m": _row_move(row, "underlying_move_1m_pct", "directional_move_1m_pct"),
        "3m": _row_move(row, "underlying_move_3m_pct", "directional_move_3m_pct"),
        "5m": _row_move(row, "underlying_move_5m_pct", "directional_move_5m_pct"),
        "exit": _entry_exit_underlying_move(row),
    }
    correct = {key: _direction_correct(value, direction, cfg) for key, value in moves.items()}
    evaluated = [value for value in correct.values() if value is not None]
    if not evaluated:
        quality = UNKNOWN_DIRECTION
    elif correct.get("exit") is True or sum(1 for value in evaluated if value) >= max(1, math.ceil(len(evaluated) / 2)):
        quality = GOOD_DIRECTION
    else:
        quality = BAD_DIRECTION
    return {"moves": moves, "correct": correct, "quality": quality}


def classify_entry(row: dict[str, Any], direction_info: dict[str, Any], cfg: SemanticsConfig | None = None) -> str:
    cfg = cfg or SemanticsConfig()
    direction = str(row.get("direction", "") or "").lower()
    engine_direction = str(row.get("direction_engine_direction", "") or "").lower()
    if engine_direction in {"call", "put"} and direction in {"call", "put"} and engine_direction != direction:
        return "conflicted"
    reason_text = " ".join(str(row.get(key, "") or "").lower() for key in ("direction_engine_reason", "pattern_direction_override_reason"))
    if "countertrend" in reason_text:
        return "countertrend"
    moves = direction_info.get("moves", {}) if isinstance(direction_info, dict) else {}
    move_1m = moves.get("1m")
    move_3m = moves.get("3m")
    dir_1m = _directional_move(float(move_1m), direction) if move_1m is not None else None
    dir_3m = _directional_move(float(move_3m), direction) if move_3m is not None else None
    if dir_1m is not None and dir_1m >= cfg.chased_move_threshold_pct:
        return "chased"
    if dir_1m is not None and dir_3m is not None:
        if dir_1m >= cfg.timely_move_threshold_pct and dir_3m >= cfg.timely_move_threshold_pct:
            return "timely"
        if dir_1m < -cfg.timely_move_threshold_pct and dir_3m >= cfg.timely_move_threshold_pct:
            return "early"
        if dir_1m < -cfg.timely_move_threshold_pct and dir_3m < -cfg.timely_move_threshold_pct:
            return "late"
    return "entry_unknown"


def classify_contract(row: dict[str, Any], cfg: SemanticsConfig | None = None) -> str:
    cfg = cfg or SemanticsConfig()
    entry_spread = _safe_float(row.get("entry_spread_pct") or row.get("contract_spread_pct"), 0.0)
    exit_spread = _safe_float(row.get("exit_spread_pct"), 0.0)
    entry_slip = _safe_float(row.get("entry_fill_slippage_vs_ask_pct"), 0.0)
    exit_slip = abs(_safe_float(row.get("exit_fill_slippage_vs_bid_pct"), 0.0))
    oi = _safe_float(row.get("contract_open_interest"), 0.0)
    vol = _safe_float(row.get("contract_daily_volume"), 0.0)
    premium = _premium_usd(row)
    max_premium = float(getattr(config, "MAX_PREMIUM_PER_TRADE_USD", 0.0) or 0.0)
    exposure = str(row.get("exposure_bucket", "") or "").lower()
    pnl = _safe_float(row.get("realized_pnl_usd") or row.get("result_usd"), 0.0)
    direction_quality = str(row.get("direction_quality", "") or "")
    if max(entry_spread, exit_spread) > cfg.high_spread_pct:
        return "spread_damage"
    if entry_slip > cfg.poor_fill_slippage_pct or exit_slip > cfg.poor_fill_slippage_pct:
        return "poor_fill"
    if oi <= 0 and vol <= 0:
        return "liquidity_damage"
    if max_premium > 0 and premium > max_premium:
        return "overpriced_premium"
    if "0dte" in exposure and pnl < 0:
        return "gamma_damage"
    if direction_quality == GOOD_DIRECTION and pnl < 0:
        return "tracked_underlying_poorly"
    return "tracked_underlying_well"


def classify_exit(row: dict[str, Any], cfg: SemanticsConfig | None = None) -> str:
    cfg = cfg or SemanticsConfig()
    exit_reason = str(row.get("exit_reason", "") or "").lower()
    pnl = _safe_float(row.get("realized_pnl_usd") or row.get("result_usd"), 0.0)
    max_fav = _safe_float(row.get("max_favorable_excursion_pct"), 0.0)
    planned = _planned_risk_usd(row)
    actual_loss = abs(min(0.0, pnl))
    if "manual" in exit_reason or "dashboard" in exit_reason:
        return "manual_system_stop"
    if "stale" in exit_reason or "orphan" in exit_reason:
        return "stale_position_close"
    if exit_reason in {"profit_target", "base_win_bank"} or "profit" in exit_reason:
        return "profit_target_hit"
    if planned > 0 and actual_loss > planned + cfg.stop_miss_tolerance_usd:
        return "stop_miss"
    if pnl < 0 and planned > 0 and actual_loss <= planned + cfg.stop_miss_tolerance_usd:
        return "protected_capital"
    if pnl > 0 and max_fav > 0 and (pnl / max(0.01, _premium_usd(row)) * 100.0) < (max_fav * 0.35):
        return "cut_winner_early"
    if pnl < 0:
        return "let_loser_expand"
    return "exit_unknown"


def classify_risk(row: dict[str, Any], cfg: SemanticsConfig | None = None) -> dict[str, Any]:
    cfg = cfg or SemanticsConfig()
    pnl = _safe_float(row.get("realized_pnl_usd") or row.get("result_usd"), 0.0)
    planned = _planned_risk_usd(row)
    actual_loss = abs(min(0.0, pnl))
    miss = max(0.0, actual_loss - planned) if planned > 0 else 0.0
    miss_pct = (miss / planned * 100.0) if planned > 0 else 0.0
    daily_cap = abs(float(getattr(config, "DAILY_LOSS_LIMIT_USD", 0.0) or 0.0))
    if planned > 0 and miss > cfg.stop_miss_tolerance_usd:
        quality = "uncontrolled_loss" if actual_loss >= planned * 2 else "exceeded_planned_risk"
    elif daily_cap > 0 and actual_loss > daily_cap:
        quality = "exceeded_daily_tolerance"
    elif actual_loss > 0 and planned > 0:
        quality = "within_planned_risk"
    elif "broker" in str(row.get("exit_reason", "") or "").lower():
        quality = "broker_market_constraint"
    else:
        quality = "risk_not_tested"
    return {
        "quality": quality,
        "planned_risk_usd": round(planned, 2),
        "actual_loss_usd": round(actual_loss, 2),
        "stop_miss_usd": round(miss, 2),
        "stop_miss_pct": round(miss_pct, 4),
    }


def _setup_key(row: dict[str, Any]) -> str:
    return "|".join(
        str(row.get(key, "") or "")
        for key in ("strategy_profile", "ticker", "direction", "exposure_bucket")
    ).strip("|")


def classify_closed_trade(row: dict[str, Any], cfg: SemanticsConfig | None = None) -> dict[str, Any]:
    cfg = cfg or SemanticsConfig()
    direction = classify_direction(row, cfg)
    enriched = dict(row)
    enriched["direction_quality"] = direction["quality"]
    entry_quality = classify_entry(enriched, direction, cfg)
    contract_quality = classify_contract(enriched, cfg)
    exit_quality = classify_exit(enriched, cfg)
    risk = classify_risk(enriched, cfg)
    pnl = _safe_float(row.get("realized_pnl_usd") or row.get("result_usd"), 0.0)
    setup_quality = "setup_profitable_trade" if pnl > 0 else ("setup_losing_trade" if pnl < 0 else "setup_flat_trade")
    record = {
        "record_id": _record_id(row, "closed_trade"),
        "record_type": "closed_trade",
        "timestamp": row.get("exit_time") or row.get("timestamp", ""),
        "date": row.get("date", ""),
        "ticker": str(row.get("ticker", "") or "").upper(),
        "direction": str(row.get("direction", "") or "").lower(),
        "strategy_profile": row.get("strategy_profile", ""),
        "setup_key": _setup_key(row),
        "option_symbol": row.get("option_symbol", ""),
        "source_decision": "filled_trade",
        "reject_reason": "",
        "realized_pnl_usd": round(pnl, 2),
        "entry_price": row.get("entry_price", ""),
        "exit_price": row.get("exit_price", ""),
        "qty": row.get("qty", ""),
        "underlying_move_1m_pct": "" if direction["moves"]["1m"] is None else round(float(direction["moves"]["1m"]), 4),
        "underlying_move_3m_pct": "" if direction["moves"]["3m"] is None else round(float(direction["moves"]["3m"]), 4),
        "underlying_move_5m_pct": "" if direction["moves"]["5m"] is None else round(float(direction["moves"]["5m"]), 4),
        "underlying_move_exit_pct": "" if direction["moves"]["exit"] is None else round(float(direction["moves"]["exit"]), 4),
        "direction_correct_1m": _bool_text(direction["correct"]["1m"]),
        "direction_correct_3m": _bool_text(direction["correct"]["3m"]),
        "direction_correct_5m": _bool_text(direction["correct"]["5m"]),
        "direction_correct_exit": _bool_text(direction["correct"]["exit"]),
        "direction_quality": direction["quality"],
        "entry_quality": entry_quality,
        "contract_quality": contract_quality,
        "exit_quality": exit_quality,
        "risk_quality": risk["quality"],
        "setup_quality": setup_quality,
        "near_miss_quality": "",
        "planned_risk_usd": risk["planned_risk_usd"],
        "actual_loss_usd": risk["actual_loss_usd"],
        "stop_miss_usd": risk["stop_miss_usd"],
        "stop_miss_pct": risk["stop_miss_pct"],
        "notes": _trade_notes(direction["quality"], entry_quality, contract_quality, exit_quality, risk["quality"]),
    }
    return {key: record.get(key, "") for key in OUTCOME_COLUMNS}


def _trade_notes(*labels: str) -> str:
    return "; ".join(label for label in labels if label)


def classify_skipped_signal(row: dict[str, Any], cfg: SemanticsConfig | None = None) -> dict[str, Any]:
    cfg = cfg or SemanticsConfig()
    direction = classify_direction(row, cfg)
    decision = str(row.get("decision", "") or row.get("source_decision", "") or "skipped_signal")
    reason = str(row.get("reason", "") or row.get("reject_reason", "") or "")
    move_exit = direction["moves"]["exit"]
    directional = _directional_move(float(move_exit), str(row.get("direction", "") or "")) if move_exit is not None else 0.0
    if direction["quality"] == GOOD_DIRECTION:
        near_miss = "missed_profitable_skip"
    elif direction["quality"] == BAD_DIRECTION:
        near_miss = "useful_reject_avoided_loss"
    else:
        near_miss = "skip_outcome_unknown"
    record = {
        "record_id": _record_id(row, "skipped_signal"),
        "record_type": "skipped_signal",
        "timestamp": row.get("timestamp", ""),
        "date": row.get("date", ""),
        "ticker": str(row.get("ticker", "") or row.get("symbol", "") or "").upper(),
        "direction": str(row.get("direction", "") or "").lower(),
        "strategy_profile": row.get("strategy_profile", ""),
        "setup_key": _setup_key(row),
        "option_symbol": "",
        "source_decision": decision,
        "reject_reason": reason,
        "realized_pnl_usd": "",
        "entry_price": row.get("entry_price", ""),
        "exit_price": row.get("exit_price", ""),
        "qty": "",
        "underlying_move_1m_pct": "" if direction["moves"]["1m"] is None else round(float(direction["moves"]["1m"]), 4),
        "underlying_move_3m_pct": "" if direction["moves"]["3m"] is None else round(float(direction["moves"]["3m"]), 4),
        "underlying_move_5m_pct": "" if direction["moves"]["5m"] is None else round(float(direction["moves"]["5m"]), 4),
        "underlying_move_exit_pct": "" if move_exit is None else round(float(move_exit), 4),
        "direction_correct_1m": _bool_text(direction["correct"]["1m"]),
        "direction_correct_3m": _bool_text(direction["correct"]["3m"]),
        "direction_correct_5m": _bool_text(direction["correct"]["5m"]),
        "direction_correct_exit": _bool_text(direction["correct"]["exit"]),
        "direction_quality": direction["quality"],
        "entry_quality": "",
        "contract_quality": "",
        "exit_quality": "",
        "risk_quality": "",
        "setup_quality": "skipped_signal_would_work" if directional > 0 else "skipped_signal_would_fail",
        "near_miss_quality": near_miss,
        "planned_risk_usd": "",
        "actual_loss_usd": "",
        "stop_miss_usd": "",
        "stop_miss_pct": "",
        "notes": f"{near_miss}; reason={reason}",
    }
    return {key: record.get(key, "") for key in OUTCOME_COLUMNS}


def _read_csv(path: Path, limit: int = 5000) -> list[dict[str, str]]:
    if not path.exists():
        return []
    try:
        with path.open("r", newline="", encoding="utf-8") as handle:
            return list(deque(csv.DictReader(handle), maxlen=max(1, int(limit))))
    except Exception:
        return []


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    with temp.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTCOME_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in OUTCOME_COLUMNS})
    temp.replace(path)


def _counter(rows: list[dict[str, Any]], field: str) -> dict[str, int]:
    return dict(Counter(str(row.get(field, "") or "unknown") for row in rows))


def build_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    closed = [row for row in rows if row.get("record_type") == "closed_trade"]
    skipped = [row for row in rows if row.get("record_type") == "skipped_signal"]
    wins = [_safe_float(row.get("realized_pnl_usd"), 0.0) for row in closed if _safe_float(row.get("realized_pnl_usd"), 0.0) > 0]
    losses = [_safe_float(row.get("realized_pnl_usd"), 0.0) for row in closed if _safe_float(row.get("realized_pnl_usd"), 0.0) < 0]
    pnl_values = [_safe_float(row.get("realized_pnl_usd"), 0.0) for row in closed]
    call_rows = [row for row in closed if str(row.get("direction", "")).lower() == "call"]
    put_rows = [row for row in closed if str(row.get("direction", "")).lower() == "put"]

    def _expectancy(subset: list[dict[str, Any]]) -> float:
        values = [_safe_float(row.get("realized_pnl_usd"), 0.0) for row in subset]
        return round(sum(values) / len(values), 2) if values else 0.0

    setup: dict[str, dict[str, Any]] = defaultdict(lambda: {"signals": 0, "taken_trades": 0, "skipped_trades": 0, "pnl": 0.0, "wins": 0, "losses": 0, "call_pnl": [], "put_pnl": []})
    for row in rows:
        key = str(row.get("setup_key", "") or "unknown")
        setup[key]["signals"] += 1
        if row.get("record_type") == "closed_trade":
            pnl = _safe_float(row.get("realized_pnl_usd"), 0.0)
            setup[key]["taken_trades"] += 1
            setup[key]["pnl"] += pnl
            if pnl > 0:
                setup[key]["wins"] += 1
            elif pnl < 0:
                setup[key]["losses"] += 1
            if str(row.get("direction", "")).lower() == "call":
                setup[key]["call_pnl"].append(pnl)
            elif str(row.get("direction", "")).lower() == "put":
                setup[key]["put_pnl"].append(pnl)
        else:
            setup[key]["skipped_trades"] += 1

    setup_rows = []
    for key, payload in setup.items():
        taken = int(payload["taken_trades"])
        closed_count = max(1, taken)
        call_pnl = payload.pop("call_pnl")
        put_pnl = payload.pop("put_pnl")
        setup_rows.append(
            {
                "setup_key": key,
                "total_signals": int(payload["signals"]),
                "taken_trades": taken,
                "skipped_trades": int(payload["skipped_trades"]),
                "direction_correct_count": sum(1 for row in rows if str(row.get("setup_key", "")) == key and row.get("direction_quality") == GOOD_DIRECTION),
                "average_win": round(sum(v for v in call_pnl + put_pnl if v > 0) / max(1, len([v for v in call_pnl + put_pnl if v > 0])), 2),
                "average_loss": round(sum(v for v in call_pnl + put_pnl if v < 0) / max(1, len([v for v in call_pnl + put_pnl if v < 0])), 2),
                "expectancy": round(float(payload["pnl"]) / closed_count, 2) if taken else 0.0,
                "call_expectancy": round(sum(call_pnl) / len(call_pnl), 2) if call_pnl else 0.0,
                "put_expectancy": round(sum(put_pnl) / len(put_pnl), 2) if put_pnl else 0.0,
            }
        )
    setup_rows.sort(key=lambda item: (float(item["expectancy"]), int(item["taken_trades"])))

    return {
        "record_count": len(rows),
        "closed_trade_count": len(closed),
        "skipped_signal_count": len(skipped),
        "gross_pnl_usd": round(sum(pnl_values), 2),
        "average_win": round(sum(wins) / len(wins), 2) if wins else 0.0,
        "average_loss": round(sum(losses) / len(losses), 2) if losses else 0.0,
        "win_rate": round(len(wins) / len(closed), 4) if closed else 0.0,
        "expectancy": round(sum(pnl_values) / len(closed), 2) if closed else 0.0,
        "call_expectancy": _expectancy(call_rows),
        "put_expectancy": _expectancy(put_rows),
        "call_correctness": _counter(call_rows, "direction_quality"),
        "put_correctness": _counter(put_rows, "direction_quality"),
        "entry_quality": _counter(closed, "entry_quality"),
        "contract_quality": _counter(closed, "contract_quality"),
        "exit_quality": _counter(closed, "exit_quality"),
        "risk_quality": _counter(closed, "risk_quality"),
        "near_miss_quality": _counter(skipped, "near_miss_quality"),
        "missed_profitable_skips": sum(1 for row in skipped if row.get("near_miss_quality") == "missed_profitable_skip"),
        "harmful_entries": sum(1 for row in closed if row.get("direction_quality") == BAD_DIRECTION or row.get("risk_quality") in {"exceeded_planned_risk", "uncontrolled_loss"}),
        "largest_stop_miss_usd": round(max((_safe_float(row.get("stop_miss_usd"), 0.0) for row in closed), default=0.0), 2),
        "setup_quality": setup_rows[:50],
    }


def update_trade_outcome_semantics(
    *,
    trades_path: Path | None = None,
    skipped_rows: list[dict[str, Any]] | None = None,
    outcome_path: Path | None = None,
    summary_path: Path | None = None,
    limit: int = 5000,
) -> dict[str, Any]:
    trades_path = trades_path or Path(getattr(config, "TRADES_CSV_PATH"))
    outcome_path = outcome_path or Path(getattr(config, "TRADE_OUTCOME_SEMANTICS_CSV_PATH", Path(config.DATA_DIR) / "trade_outcome_semantics.csv"))
    summary_path = summary_path or Path(getattr(config, "TRADE_OUTCOME_SEMANTICS_SUMMARY_PATH", Path(config.DATA_DIR) / "trade_outcome_semantics_summary.json"))
    trade_rows = _read_csv(trades_path, limit=limit)
    records = [classify_closed_trade(row) for row in trade_rows]
    for row in skipped_rows or []:
        records.append(classify_skipped_signal(row))
    deduped = {str(row["record_id"]): row for row in records}
    ordered = sorted(deduped.values(), key=lambda row: str(row.get("timestamp", "")))
    _write_csv(outcome_path, ordered)
    summary = build_summary(ordered)
    summary["outcome_csv"] = str(outcome_path)
    summary["summary_json"] = str(summary_path)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    return summary


def load_trade_outcome_semantics_summary() -> dict[str, Any]:
    path = Path(getattr(config, "TRADE_OUTCOME_SEMANTICS_SUMMARY_PATH", Path(config.DATA_DIR) / "trade_outcome_semantics_summary.json"))
    if path.exists():
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            return payload if isinstance(payload, dict) else {}
        except Exception:
            return {}
    return {}
