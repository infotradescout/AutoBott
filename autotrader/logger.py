"""CSV trade logger."""

from __future__ import annotations

import csv
from pathlib import Path

try:
    from autotrader import config
except ImportError:
    import config


class TradeLogger:
    columns = [
        "timestamp",
        "date",
        "ticker",
        "direction",
        "strategy_profile",
        "option_symbol",
        "strike",
        "expiry",
        "qty",
        "signal_score",
        "direction_score",
        "rvol",
        "rsi",
        "roc",
        "iv_rank",
        "contract_spread_pct",
        "entry_time",
        "exit_time",
        "hold_seconds",
        "time_to_first_green_seconds",
        "entry_price",
        "exit_price",
        "realized_pnl_usd",
        "pnl_pct",
        "paper_reported_pnl_usd",
        "paper_reported_pnl_pct",
        "conservative_executable_pnl_usd",
        "conservative_executable_pnl_pct",
        "max_favorable_excursion_pct",
        "max_adverse_excursion_pct",
        "entry_underlying_symbol",
        "entry_bid_submit",
        "entry_ask_submit",
        "entry_midpoint_submit",
        "entry_intended_limit",
        "entry_filled_price",
        "entry_spread_pct",
        "entry_fill_slippage_vs_ask_pct",
        "entry_fill_seconds",
        "entry_attempts",
        "index_bias_at_entry",
        "weak_index_bias_trade",
        "exit_underlying_symbol",
        "exit_bid_submit",
        "exit_ask_submit",
        "exit_midpoint_submit",
        "exit_intended_limit",
        "exit_filled_price",
        "exit_spread_pct",
        "exit_fill_slippage_vs_bid_pct",
        "exit_fill_seconds",
        "exit_attempts",
        "exit_reason",
    ]

    def __init__(self, path: Path | None = None):
        self.path = path or config.TRADES_CSV_PATH
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.max_rows = max(0, int(getattr(config, "TRADES_MAX_ROWS", 0)))
        self._ensure_header()

    def _ensure_header(self):
        if not self.path.exists():
            with self.path.open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=self.columns)
                writer.writeheader()
            return

        try:
            with self.path.open("r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                existing_columns = list(reader.fieldnames or [])
                rows = list(reader)
        except Exception as exc:  # noqa: BLE001
            print(f"[logger] header read failed: {exc}")
            return

        if existing_columns == self.columns:
            return

        try:
            with self.path.open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=self.columns)
                writer.writeheader()
                for row in rows:
                    payload = {key: row.get(key, "") for key in self.columns}
                    writer.writerow(payload)
        except Exception as exc:  # noqa: BLE001
            print(f"[logger] header migration failed: {exc}")

    def log_trade(self, row: dict):
        payload = {key: row.get(key, "") for key in self.columns}
        with self.path.open("a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.columns)
            writer.writerow(payload)
        self._trim_if_needed()

    def _trim_if_needed(self):
        if self.max_rows <= 0 or not self.path.exists():
            return
        try:
            with self.path.open("r", newline="", encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
            if len(rows) <= self.max_rows:
                return
            kept = rows[-self.max_rows :]
            with self.path.open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=self.columns)
                writer.writeheader()
                writer.writerows(kept)
        except Exception as exc:  # noqa: BLE001
            print(f"[logger] trim failed: {exc}")
