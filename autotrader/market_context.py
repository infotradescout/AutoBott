"""Market-regime snapshot builder for the shared trading desk state."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import config
from scanner import calculate_vwap


def _safe_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _symbol_context(data_client, symbol: str) -> dict[str, Any]:
    try:
        bars = data_client.get_stock_bars(
            symbol=symbol,
            timeframe=str(getattr(config, "MARKET_CONTEXT_TIMEFRAME", "5m") or "5m"),
            limit=max(25, int(getattr(config, "MARKET_CONTEXT_LOOKBACK", 40) or 40)),
        )
    except Exception as exc:  # noqa: BLE001
        return {"symbol": symbol, "status": "error", "error": str(exc)[:160]}
    if bars is None or bars.empty or len(bars) < 21:
        return {"symbol": symbol, "status": "insufficient_bars"}

    closes = bars["close"].astype(float)
    ema9 = closes.ewm(span=9, adjust=False).mean()
    ema21 = closes.ewm(span=21, adjust=False).mean()
    last = float(closes.iloc[-1])
    prev = float(closes.iloc[-6]) if len(closes) >= 6 else float(closes.iloc[0])
    roc_5 = ((last - prev) / prev) * 100.0 if prev > 0 else 0.0
    vwap = calculate_vwap(bars)
    if ema9.iloc[-1] > ema21.iloc[-1] and ema21.iloc[-1] >= ema21.iloc[-2]:
        trend = "up"
    elif ema9.iloc[-1] < ema21.iloc[-1] and ema21.iloc[-1] <= ema21.iloc[-2]:
        trend = "down"
    else:
        trend = "mixed"
    return {
        "symbol": symbol,
        "status": "ok",
        "trend": trend,
        "roc_5_pct": round(roc_5, 4),
        "last": round(last, 4),
        "vwap": round(float(vwap), 4) if vwap == vwap else None,
        "above_vwap": bool(vwap == vwap and last >= float(vwap)),
    }


def build_market_context(data_client, now_et: datetime, *, vix_value: float | None = None) -> dict[str, Any]:
    symbols = tuple(getattr(config, "MARKET_CONTEXT_SYMBOLS", ("SPY", "QQQ", "IWM")) or ("SPY", "QQQ", "IWM"))
    symbol_rows = [_symbol_context(data_client, str(symbol).upper()) for symbol in symbols]
    ok_rows = [row for row in symbol_rows if row.get("status") == "ok"]
    up_votes = sum(1 for row in ok_rows if row.get("trend") == "up")
    down_votes = sum(1 for row in ok_rows if row.get("trend") == "down")
    mixed_votes = max(0, len(ok_rows) - up_votes - down_votes)

    if ok_rows and up_votes >= max(2, len(ok_rows) - 1):
        regime = "trend_up"
        preferred_direction = "call"
    elif ok_rows and down_votes >= max(2, len(ok_rows) - 1):
        regime = "trend_down"
        preferred_direction = "put"
    elif ok_rows and mixed_votes >= max(1, len(ok_rows) // 2):
        regime = "mixed_chop"
        preferred_direction = "both"
    else:
        regime = "mixed"
        preferred_direction = "both"

    volatility = "unknown"
    if vix_value is not None:
        vix = _safe_float(vix_value)
        if vix >= float(getattr(config, "MARKET_CONTEXT_VIX_HIGH", 25.0) or 25.0):
            volatility = "high"
        elif vix <= float(getattr(config, "MARKET_CONTEXT_VIX_LOW", 14.0) or 14.0):
            volatility = "low"
        else:
            volatility = "normal"

    blocked_profiles: list[str] = []
    allowed_profiles: list[str] = []
    if regime == "mixed_chop":
        blocked_profiles = ["open_drive_momentum"]
        allowed_profiles = ["vwap_reclaim", "reversal_snapback"]
    elif regime in {"trend_up", "trend_down"}:
        blocked_profiles = ["reversal_snapback"]
        allowed_profiles = ["open_drive_momentum", "vwap_continuation", "catalyst_impulse"]

    return {
        "timestamp_et": now_et.isoformat(),
        "source": "market_context_worker",
        "regime": regime,
        "preferred_direction": preferred_direction,
        "volatility": volatility,
        "vix": round(float(vix_value), 4) if vix_value is not None else None,
        "breadth": {
            "up_votes": up_votes,
            "down_votes": down_votes,
            "mixed_votes": mixed_votes,
            "symbols_ok": len(ok_rows),
        },
        "symbols": symbol_rows,
        "allowed_profiles": allowed_profiles,
        "blocked_profiles": blocked_profiles,
    }
