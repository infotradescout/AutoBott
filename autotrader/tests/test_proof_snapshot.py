"""Regression tests for the read-only proof snapshot report."""

from __future__ import annotations

import csv
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path

_PKG_DIR = Path(__file__).resolve().parent.parent
if str(_PKG_DIR) not in sys.path:
    sys.path.insert(0, str(_PKG_DIR))

from proof_snapshot import ProofPaths, build_snapshot  # noqa: E402


class ProofSnapshotTests(unittest.TestCase):
    def test_build_snapshot_summarizes_trades_and_scans_for_target_date(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            trades_csv = tmp_path / "trades.csv"
            scan_csv = tmp_path / "scan_log.csv"

            with trades_csv.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "timestamp",
                        "date",
                        "ticker",
                        "direction",
                        "option_symbol",
                        "qty",
                        "entry_price",
                        "exit_price",
                        "realized_pnl_usd",
                        "pnl_pct",
                        "exit_reason",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "timestamp": "2026-05-01 09:45:00 EDT",
                        "date": "2026-05-01",
                        "ticker": "AAPL",
                        "direction": "call",
                        "option_symbol": "AAPL260501C00200000",
                        "qty": "1",
                        "entry_price": "1.00",
                        "exit_price": "1.50",
                        "realized_pnl_usd": "50.00",
                        "pnl_pct": "0.50",
                        "exit_reason": "take_profit",
                    }
                )
                writer.writerow(
                    {
                        "timestamp": "2026-05-01 10:15:00 EDT",
                        "date": "2026-05-01",
                        "ticker": "TSLA",
                        "direction": "put",
                        "option_symbol": "TSLA260501P00400000",
                        "qty": "1",
                        "entry_price": "2.00",
                        "exit_price": "1.70",
                        "realized_pnl_usd": "-30.00",
                        "pnl_pct": "-0.15",
                        "exit_reason": "stop_loss",
                    }
                )
                writer.writerow(
                    {
                        "timestamp": "2026-04-30 10:15:00 EDT",
                        "date": "2026-04-30",
                        "ticker": "MSFT",
                        "direction": "call",
                        "option_symbol": "MSFT260430C00400000",
                        "qty": "1",
                        "entry_price": "1.00",
                        "exit_price": "2.00",
                        "realized_pnl_usd": "100.00",
                        "pnl_pct": "1.00",
                        "exit_reason": "take_profit",
                    }
                )

            with scan_csv.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=["timestamp", "symbol", "result", "direction", "signal_score", "rvol", "roc", "reason"],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "timestamp": "2026-05-01 09:35:00 EDT",
                        "symbol": "AAPL",
                        "result": "pass",
                        "direction": "call",
                        "signal_score": "7.5",
                        "rvol": "2.2",
                        "roc": "0.4",
                        "reason": "momentum pass",
                    }
                )
                writer.writerow(
                    {
                        "timestamp": "2026-05-01 09:36:00 EDT",
                        "symbol": "TSLA",
                        "result": "fail",
                        "direction": "put",
                        "signal_score": "3.1",
                        "rvol": "0.8",
                        "roc": "-0.1",
                        "reason": "setup_reject: weak rvol",
                    }
                )

            snapshot = build_snapshot(
                date(2026, 5, 1),
                ProofPaths(trades_csv=trades_csv, scan_log_csv=scan_csv),
            )

        self.assertEqual(snapshot["metadata"]["target_date"], "2026-05-01")
        self.assertEqual(snapshot["trade_summary"]["closed_trades"], 2)
        self.assertEqual(snapshot["trade_summary"]["wins"], 1)
        self.assertEqual(snapshot["trade_summary"]["losses"], 1)
        self.assertEqual(snapshot["trade_summary"]["total_pnl_usd"], 20.0)
        self.assertEqual(snapshot["trade_summary"]["profit_factor"], 1.6667)
        self.assertEqual(snapshot["scan_summary"]["scan_rows"], 2)
        self.assertEqual(snapshot["scan_summary"]["pass_rows"], 1)
        self.assertEqual(snapshot["scan_summary"]["fail_rows"], 1)
        self.assertEqual(snapshot["scan_summary"]["top_pass_signals"][0]["symbol"], "AAPL")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
