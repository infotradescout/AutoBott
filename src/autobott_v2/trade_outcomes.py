from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

from .runtime_paths import data_root


def trade_outcome_journal_path() -> Path:
    return data_root() / "execution" / "trade_outcomes.jsonl"


def sync_trade_outcomes_from_broker(broker: Any, *, journal_path: str | Path | None = None, limit: int = 200) -> dict[str, Any]:
    if not hasattr(broker, "list_orders"):
        return {"ok": True, "recorded": 0, "outcomes": [], "blocked_underlyings": []}
    try:
        orders = broker.list_orders(status="all", limit=limit, direction="desc")
    except Exception as exc:
        return {"ok": False, "recorded": 0, "error": str(exc), "outcomes": [], "blocked_underlyings": []}
    return record_trade_outcomes_from_orders(orders, journal_path=journal_path)


def record_trade_outcomes_from_orders(orders: list[dict[str, Any]], *, journal_path: str | Path | None = None) -> dict[str, Any]:
    existing = {str(row.get("outcome_id")) for row in load_trade_outcomes(journal_path=journal_path)}
    outcomes = build_trade_outcomes_from_orders(orders)
    new_rows = [row for row in outcomes if row["outcome_id"] not in existing]
    path = Path(journal_path) if journal_path is not None else trade_outcome_journal_path()
    if new_rows:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            for row in new_rows:
                handle.write(json.dumps(row, sort_keys=True))
                handle.write("\n")
    all_rows = load_trade_outcomes(journal_path=journal_path)
    return {
        "ok": True,
        "recorded": len(new_rows),
        "outcomes": new_rows,
        "summary": summarize_trade_outcomes(all_rows),
        "blocked_underlyings": recent_loss_guard(all_rows)["blocked_underlyings"],
    }


def build_trade_outcomes_from_orders(orders: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized = sorted((_normalize_order(order) for order in orders), key=lambda row: row["event_time"] or datetime.min.replace(tzinfo=UTC))
    open_buys: dict[str, list[dict[str, Any]]] = {}
    outcomes: list[dict[str, Any]] = []
    for order in normalized:
        if order["status"] not in {"filled", "partially_filled"}:
            continue
        if order["filled_qty"] <= 0 or order["filled_avg_price"] is None:
            continue
        if order["side"] == "buy":
            open_buys.setdefault(order["symbol"], []).append(order)
            continue
        if order["side"] != "sell":
            continue
        buy = open_buys.get(order["symbol"], []).pop(0) if open_buys.get(order["symbol"]) else None
        if buy is None:
            continue
        outcomes.append(_outcome_from_pair(buy, order))
    return outcomes


def load_trade_outcomes(*, journal_path: str | Path | None = None, limit: int | None = None) -> list[dict[str, Any]]:
    path = Path(journal_path) if journal_path is not None else trade_outcome_journal_path()
    if not path.exists():
        return []
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    return rows[-limit:] if limit is not None else rows


def summarize_trade_outcomes(rows: list[dict[str, Any]]) -> dict[str, Any]:
    pnls = [float(row.get("pnl") or 0.0) for row in rows]
    wins = [pnl for pnl in pnls if pnl > 0]
    losses = [pnl for pnl in pnls if pnl < 0]
    net = round(sum(pnls), 2)
    return {
        "closed_trades": len(rows),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": round(len(wins) / len(rows), 4) if rows else 0.0,
        "net_pnl": net,
        "expectancy": round(net / len(rows), 2) if rows else 0.0,
        "profit_factor": round(sum(wins) / abs(sum(losses)), 4) if losses else (float("inf") if wins else 0.0),
    }


def recent_loss_guard(rows: list[dict[str, Any]] | None = None, *, journal_path: str | Path | None = None) -> dict[str, Any]:
    if not _loss_guard_enabled():
        return {"enabled": False, "blocked_underlyings": [], "reasons": {}}
    source = rows if rows is not None else load_trade_outcomes(journal_path=journal_path, limit=_loss_guard_lookback())
    recent = source[-_loss_guard_lookback() :]
    by_underlying: dict[str, list[dict[str, Any]]] = {}
    for row in recent:
        underlying = str(row.get("underlying") or "").upper()
        if underlying:
            by_underlying.setdefault(underlying, []).append(row)
    blocked: list[str] = []
    reasons: dict[str, dict[str, Any]] = {}
    for underlying, outcomes in by_underlying.items():
        tail = outcomes[-_loss_guard_consecutive_losses() :]
        consecutive_losses = len(tail) >= _loss_guard_consecutive_losses() and all(float(row.get("pnl") or 0.0) < 0 for row in tail)
        loss_rate = sum(1 for row in outcomes if float(row.get("pnl") or 0.0) < 0) / len(outcomes)
        loss_rate_block = len(outcomes) >= _loss_guard_min_sample() and loss_rate >= _loss_guard_loss_rate()
        if consecutive_losses or loss_rate_block:
            blocked.append(underlying)
            reasons[underlying] = {
                "recent_trades": len(outcomes),
                "loss_rate": round(loss_rate, 4),
                "consecutive_losses": consecutive_losses,
                "loss_rate_block": loss_rate_block,
                "last_outcome_id": outcomes[-1].get("outcome_id"),
            }
    return {"enabled": True, "blocked_underlyings": sorted(blocked), "reasons": reasons}


def recent_winner_bias(rows: list[dict[str, Any]] | None = None, *, journal_path: str | Path | None = None) -> dict[str, Any]:
    if not _winner_bias_enabled():
        return {"enabled": False, "preferred_underlyings": [], "reasons": {}}
    source = rows if rows is not None else load_trade_outcomes(journal_path=journal_path, limit=_winner_bias_lookback())
    recent = source[-_winner_bias_lookback() :]
    by_underlying: dict[str, list[dict[str, Any]]] = {}
    for row in recent:
        underlying = str(row.get("underlying") or "").upper()
        if underlying:
            by_underlying.setdefault(underlying, []).append(row)
    preferred: list[str] = []
    reasons: dict[str, dict[str, Any]] = {}
    for underlying, outcomes in by_underlying.items():
        wins = [row for row in outcomes if float(row.get("pnl") or 0.0) > 0]
        pnls = [float(row.get("pnl") or 0.0) for row in outcomes]
        win_rate = len(wins) / len(outcomes)
        net_pnl = round(sum(pnls), 2)
        tail = outcomes[-_winner_bias_consecutive_wins() :]
        consecutive_wins = len(tail) >= _winner_bias_consecutive_wins() and all(float(row.get("pnl") or 0.0) > 0 for row in tail)
        if len(outcomes) >= _winner_bias_min_sample() and net_pnl > 0 and (win_rate >= _winner_bias_win_rate() or consecutive_wins):
            preferred.append(underlying)
            reasons[underlying] = {
                "recent_trades": len(outcomes),
                "wins": len(wins),
                "win_rate": round(win_rate, 4),
                "net_pnl": net_pnl,
                "consecutive_wins": consecutive_wins,
                "last_outcome_id": outcomes[-1].get("outcome_id"),
            }
    ranked = sorted(preferred, key=lambda symbol: (-float(reasons[symbol]["net_pnl"]), symbol))
    return {"enabled": True, "preferred_underlyings": ranked, "reasons": reasons}


def _outcome_from_pair(entry: dict[str, Any], exit_order: dict[str, Any]) -> dict[str, Any]:
    entry_price = float(entry["filled_avg_price"])
    exit_price = float(exit_order["filled_avg_price"])
    qty = min(float(entry["filled_qty"]), float(exit_order["filled_qty"]))
    pnl = round((exit_price - entry_price) * qty * 100.0, 2)
    return_pct = ((exit_price - entry_price) / entry_price) if entry_price else 0.0
    symbol = str(entry["symbol"])
    parts = _option_symbol_parts(symbol)
    classification, reason = _classify_outcome(return_pct, pnl)
    payload = {
        "schema_version": "trade_outcome.v1",
        "recorded_at": datetime.now(tz=UTC).isoformat(),
        "symbol": symbol,
        **parts,
        "entry_time": entry.get("filled_at") or entry.get("submitted_at"),
        "exit_time": exit_order.get("filled_at") or exit_order.get("submitted_at"),
        "entry_price": entry_price,
        "exit_price": exit_price,
        "qty": qty,
        "pnl": pnl,
        "return_pct": round(return_pct, 4),
        "result": "winner" if pnl > 0 else "loser" if pnl < 0 else "flat",
        "classification": classification,
        "reason_inferred": reason,
        "why": _why_tags(parts, return_pct, pnl),
    }
    payload["outcome_id"] = _outcome_id(payload)
    return payload


def _classify_outcome(return_pct: float, pnl: float) -> tuple[str, str]:
    if return_pct >= 1.20:
        return "huge_winner", "large_option_return"
    if return_pct >= 0.30:
        return "winner", "profit_target_region"
    if pnl > 0:
        return "small_winner", "profitable_exit"
    if return_pct <= -0.50:
        return "large_loss", "deep_loss_beyond_stop_threshold"
    if return_pct <= -0.22:
        return "loss_cut", "stop_loss_threshold_breached"
    if pnl < 0:
        return "small_loss", "loss_before_stop_threshold"
    return "flat", "flat_exit"


def _why_tags(parts: dict[str, Any], return_pct: float, pnl: float) -> list[str]:
    tags = [f"option_return_pct={round(return_pct, 4)}"]
    if pnl < 0 and return_pct <= -0.22:
        tags.append("loss_exceeded_configured_stop_zone")
    if pnl < 0 and abs(return_pct) >= 0.50:
        tags.append("severe_option_decay_or_wrong_direction")
    if pnl > 0 and return_pct >= 0.30:
        tags.append("profit_move_captured")
    if parts.get("option_type"):
        tags.append(f"option_type={str(parts['option_type']).lower()}")
    return tags


def _normalize_order(order: dict[str, Any]) -> dict[str, Any]:
    submitted_at = _parse_datetime(order.get("submitted_at"))
    filled_at = _parse_datetime(order.get("filled_at"))
    event_time = filled_at or submitted_at
    return {
        "symbol": str(order.get("symbol") or "").upper(),
        "side": str(order.get("side") or "").lower(),
        "status": str(order.get("status") or "").lower(),
        "filled_qty": _float_or_zero(order.get("filled_qty")),
        "filled_avg_price": _float_or_none(order.get("filled_avg_price")),
        "submitted_at": submitted_at.isoformat() if submitted_at else None,
        "filled_at": filled_at.isoformat() if filled_at else None,
        "event_time": event_time,
    }


def _outcome_id(payload: dict[str, Any]) -> str:
    raw = "|".join(
        str(payload.get(key) or "")
        for key in ("symbol", "entry_time", "exit_time", "entry_price", "exit_price", "qty")
    )
    return sha256(raw.encode("utf-8")).hexdigest()[:24]


def _option_symbol_parts(symbol: str) -> dict[str, Any]:
    stripped = symbol.strip().upper()
    for index, char in enumerate(stripped):
        if char in {"C", "P"} and index >= 6:
            expiry = stripped[index - 6 : index]
            suffix = stripped[index + 1 :]
            if expiry.isdigit() and suffix.isdigit():
                return {
                    "underlying": stripped[: index - 6],
                    "option_type": "CALL" if char == "C" else "PUT",
                    "expiration": f"20{expiry[:2]}-{expiry[2:4]}-{expiry[4:6]}",
                    "strike": int(suffix) / 1000.0,
                }
    return {"underlying": stripped, "option_type": None, "expiration": None, "strike": None}


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


def _float_or_zero(value: Any) -> float:
    parsed = _float_or_none(value)
    return parsed if parsed is not None else 0.0


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _loss_guard_enabled() -> bool:
    return (os.getenv("AUTOBOTT_RECENT_LOSS_GUARD_ENABLED") or "true").strip().lower() in {"1", "true", "yes", "on"}


def _loss_guard_lookback() -> int:
    return max(1, int(os.getenv("AUTOBOTT_RECENT_LOSS_GUARD_LOOKBACK", "20")))


def _loss_guard_consecutive_losses() -> int:
    return max(1, int(os.getenv("AUTOBOTT_RECENT_LOSS_GUARD_CONSECUTIVE_LOSSES", "2")))


def _loss_guard_min_sample() -> int:
    return max(1, int(os.getenv("AUTOBOTT_RECENT_LOSS_GUARD_MIN_SAMPLE", "3")))


def _loss_guard_loss_rate() -> float:
    return min(1.0, max(0.0, float(os.getenv("AUTOBOTT_RECENT_LOSS_GUARD_LOSS_RATE", "0.67"))))


def _winner_bias_enabled() -> bool:
    return (os.getenv("AUTOBOTT_RECENT_WINNER_BIAS_ENABLED") or "true").strip().lower() in {"1", "true", "yes", "on"}


def _winner_bias_lookback() -> int:
    return max(1, int(os.getenv("AUTOBOTT_RECENT_WINNER_BIAS_LOOKBACK", "20")))


def _winner_bias_min_sample() -> int:
    return max(1, int(os.getenv("AUTOBOTT_RECENT_WINNER_BIAS_MIN_SAMPLE", "2")))


def _winner_bias_win_rate() -> float:
    return min(1.0, max(0.0, float(os.getenv("AUTOBOTT_RECENT_WINNER_BIAS_WIN_RATE", "0.67"))))


def _winner_bias_consecutive_wins() -> int:
    return max(1, int(os.getenv("AUTOBOTT_RECENT_WINNER_BIAS_CONSECUTIVE_WINS", "2")))
