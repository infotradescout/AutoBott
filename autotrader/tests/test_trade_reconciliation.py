"""Regression tests for deterministic trade reconciliation report generation."""

from __future__ import annotations

import unittest
from datetime import date, datetime

import pytz

import trade_reconciliation  # noqa: E402


EASTERN = pytz.timezone("US/Eastern")


class TradeReconciliationTests(unittest.TestCase):
    def test_reconciliation_detects_missing_trade_symbol(self):
        orders = [
            {
                "id": "o-1",
                "symbol": "AMD260612C00465000",
                "status": "filled",
                "side": "sell",
                "filled_qty": "1",
                "submitted_at": "2026-06-10T10:39:07-04:00",
            },
            {
                "id": "o-2",
                "symbol": "INTC260612C00110000",
                "status": "filled",
                "side": "sell",
                "filled_qty": "1",
                "submitted_at": "2026-06-10T08:32:49-04:00",
            },
        ]
        trade_rows = [
            {
                "timestamp": "2026-06-10T08:40:00-04:00",
                "option_symbol": "INTC260612C00110000",
                "realized_pnl_usd": "10.50",
            }
        ]

        report = trade_reconciliation.build_reconciliation_report(
            orders=orders,
            trades_csv_rows=trade_rows,
            target_day=date(2026, 6, 10),
            generated_at=EASTERN.localize(datetime(2026, 6, 10, 12, 0, 0)),
        )

        self.assertEqual(report["report_type"], "trade_reconciliation_v1")
        self.assertFalse(report["execution_allowed"])
        self.assertFalse(report["mutation_allowed"])
        self.assertFalse(report["live_export_allowed"])
        self.assertEqual(report["broker"]["option_sell_fills_today"], 2)
        self.assertEqual(report["trades_csv"]["option_trade_rows_today"], 1)
        self.assertEqual(report["comparison"]["sell_fills_minus_trade_rows"], 1)
        self.assertEqual(report["comparison"]["missing_symbols_in_trades_csv"], ["AMD260612C00465000"])
        self.assertFalse(report["comparison"]["is_balanced"])

    def test_reconciliation_balanced_when_symbols_and_counts_match(self):
        orders = [
            {
                "id": "o-1",
                "symbol": "IWM260608C00286000",
                "status": "filled",
                "side": "sell",
                "filled_qty": 1,
                "submitted_at": "2026-06-08T11:37:33-04:00",
            }
        ]
        trade_rows = [
            {
                "timestamp": "2026-06-08T11:38:00-04:00",
                "option_symbol": "IWM260608C00286000",
                "realized_pnl_usd": "1.00",
            }
        ]

        report = trade_reconciliation.build_reconciliation_report(
            orders=orders,
            trades_csv_rows=trade_rows,
            target_day=date(2026, 6, 8),
            generated_at=EASTERN.localize(datetime(2026, 6, 8, 13, 0, 0)),
        )

        self.assertEqual(report["broker"]["option_sell_fills_today"], 1)
        self.assertEqual(report["trades_csv"]["option_trade_rows_today"], 1)
        self.assertEqual(report["comparison"]["missing_symbols_in_trades_csv"], [])
        self.assertTrue(report["comparison"]["is_balanced"])


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
