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
