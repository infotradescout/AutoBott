"""CSV trade logger."""

from __future__ import annotations

import csv
from pathlib import Path

import config


class TradeLogger:
    columns = [
        "timestamp",
        "ticker",
        "direction",
        "option_symbol",
        "strike",
        "expiry",
        "qty",
        "entry_price",
        "exit_price",
        "pnl_pct",
        "exit_reason",
    ]

    def __init__(self, path: Path | None = None):
        self.path = path or config.TRADES_CSV_PATH
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_header()

    def _ensure_header(self):
        if not self.path.exists():
            with self.path.open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=self.columns)
                writer.writeheader()

    def log_trade(self, row: dict):
        payload = {key: row.get(key, "") for key in self.columns}
        with self.path.open("a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.columns)
            writer.writerow(payload)
