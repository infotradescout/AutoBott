"""Risk and time-window helper utilities."""

from __future__ import annotations

import re
from datetime import datetime, time


def parse_hhmm(value: str) -> time:
    return datetime.strptime(value, "%H:%M").time()


def is_at_or_after(now_et: datetime, hhmm: str) -> bool:
    return now_et.time() >= parse_hhmm(hhmm)


def can_open_new_positions(open_count: int, max_positions: int) -> bool:
    return open_count < max_positions


def calculate_entry_qty(position_size_usd: float, ask_price: float) -> int:
    if ask_price <= 0:
        return 0
    qty = int(position_size_usd // (ask_price * 100))
    return max(1, qty)


def calculate_position_budget_usd(
    *,
    equity: float | None,
    base_position_size_usd: float,
    risk_per_trade_pct: float,
    max_position_size_usd: float,
    consecutive_losses: int,
    reduce_after_consecutive_losses: int,
    drawdown_size_multiplier: float,
) -> float:
    budget = float(base_position_size_usd)
    if equity is not None and equity > 0 and risk_per_trade_pct > 0:
        budget = min(max_position_size_usd, equity * risk_per_trade_pct)
    budget = max(100.0, budget)

    if consecutive_losses >= reduce_after_consecutive_losses:
        budget *= max(0.1, drawdown_size_multiplier)
    return budget


def infer_underlying_from_option_symbol(option_symbol: str) -> str:
    # OCC-style option symbols start with the underlying before YYMMDD.
    match = re.match(r"^([A-Z.]+)\d{6}[CP]\d{8}$", option_symbol)
    if match:
        return match.group(1)
    return ""


def position_matches_ticker(position_symbol: str, ticker: str, underlying_symbol: str | None = None) -> bool:
    if underlying_symbol:
        return underlying_symbol.upper() == ticker.upper()
    inferred = infer_underlying_from_option_symbol(position_symbol.upper())
    return inferred == ticker.upper()
