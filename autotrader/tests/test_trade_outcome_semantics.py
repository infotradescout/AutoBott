from __future__ import annotations

import csv
import sys
import tempfile
import unittest
from pathlib import Path

_PKG_DIR = Path(__file__).resolve().parent.parent
if str(_PKG_DIR) not in sys.path:
    sys.path.insert(0, str(_PKG_DIR))

import trade_outcome_semantics as semantics  # noqa: E402


def base_trade(**overrides):
    row = {
        "timestamp": "2026-06-10 10:00:00 EDT",
        "date": "2026-06-10",
        "ticker": "AMD",
        "direction": "call",
        "strategy_profile": "aggressive",
        "exposure_bucket": "weekly_single_name",
        "option_symbol": "AMD260612C00120000",
        "qty": "1",
        "entry_price": "2.00",
        "exit_price": "2.40",
        "entry_underlying_price": "120.00",
        "exit_underlying_price": "121.20",
        "underlying_move_1m_pct": "0.10",
        "underlying_move_3m_pct": "0.22",
        "underlying_move_5m_pct": "0.35",
        "realized_pnl_usd": "40.00",
        "entry_spread_pct": "1.00",
        "exit_spread_pct": "1.00",
        "entry_fill_slippage_vs_ask_pct": "0.20",
        "exit_fill_slippage_vs_bid_pct": "0.20",
        "contract_open_interest": "100",
        "contract_daily_volume": "25",
        "exit_reason": "profit_target",
        "max_favorable_excursion_pct": "30.00",
        "stop_loss_usd": "60",
    }
    row.update(overrides)
    return row


class TradeOutcomeSemanticsTests(unittest.TestCase):
    def test_winning_call_labels_correct_direction(self):
        record = semantics.classify_closed_trade(base_trade())

        self.assertEqual(record["direction_quality"], "correct_direction")
        self.assertEqual(record["entry_quality"], "timely")
        self.assertEqual(record["exit_quality"], "profit_target_hit")

    def test_losing_call_labels_wrong_direction(self):
        record = semantics.classify_closed_trade(
            base_trade(
                exit_underlying_price="118.80",
                underlying_move_1m_pct="-0.10",
                underlying_move_3m_pct="-0.22",
                underlying_move_5m_pct="-0.35",
                realized_pnl_usd="-50.00",
                exit_price="1.50",
                exit_reason="stop_loss",
            )
        )

        self.assertEqual(record["direction_quality"], "wrong_direction")
        self.assertEqual(record["risk_quality"], "within_planned_risk")

    def test_winning_put_labels_correct_direction(self):
        record = semantics.classify_closed_trade(
            base_trade(
                direction="put",
                option_symbol="AMD260612P00120000",
                exit_underlying_price="118.80",
                underlying_move_1m_pct="-0.10",
                underlying_move_3m_pct="-0.22",
                underlying_move_5m_pct="-0.35",
            )
        )

        self.assertEqual(record["direction_quality"], "correct_direction")

    def test_losing_put_labels_wrong_direction(self):
        record = semantics.classify_closed_trade(
            base_trade(
                direction="put",
                option_symbol="AMD260612P00120000",
                exit_underlying_price="121.20",
                underlying_move_1m_pct="0.10",
                underlying_move_3m_pct="0.22",
                underlying_move_5m_pct="0.35",
                realized_pnl_usd="-40.00",
                exit_price="1.60",
                exit_reason="stop_loss",
            )
        )

        self.assertEqual(record["direction_quality"], "wrong_direction")

    def test_correct_direction_bad_contract(self):
        record = semantics.classify_closed_trade(
            base_trade(
                realized_pnl_usd="-30.00",
                exit_price="1.70",
            )
        )

        self.assertEqual(record["direction_quality"], "correct_direction")
        self.assertEqual(record["contract_quality"], "tracked_underlying_poorly")

    def test_wrong_direction_lucky_profit(self):
        record = semantics.classify_closed_trade(
            base_trade(
                exit_underlying_price="118.80",
                underlying_move_1m_pct="-0.10",
                underlying_move_3m_pct="-0.22",
                underlying_move_5m_pct="-0.35",
                realized_pnl_usd="15.00",
                exit_price="2.15",
            )
        )

        self.assertEqual(record["direction_quality"], "wrong_direction")
        self.assertEqual(record["setup_quality"], "setup_profitable_trade")

    def test_good_entry_bad_exit_stop_miss(self):
        record = semantics.classify_closed_trade(
            base_trade(
                realized_pnl_usd="-510.00",
                exit_price="-3.10",
                exit_reason="stop_loss",
                stop_loss_usd="60",
            )
        )

        self.assertEqual(record["entry_quality"], "timely")
        self.assertEqual(record["exit_quality"], "stop_miss")
        self.assertEqual(record["risk_quality"], "uncontrolled_loss")
        self.assertEqual(record["stop_miss_usd"], 450.0)

    def test_skipped_signal_that_would_have_won(self):
        record = semantics.classify_skipped_signal(
            {
                "timestamp": "2026-06-10 10:00:00 EDT",
                "date": "2026-06-10",
                "symbol": "QQQ",
                "direction": "call",
                "reason": "entry_confirmation_mismatch",
                "underlying_move_exit_pct": "0.40",
            }
        )

        self.assertEqual(record["near_miss_quality"], "missed_profitable_skip")

    def test_skipped_signal_that_would_have_lost(self):
        record = semantics.classify_skipped_signal(
            {
                "timestamp": "2026-06-10 10:00:00 EDT",
                "date": "2026-06-10",
                "symbol": "QQQ",
                "direction": "call",
                "reason": "fresh_tape_direction_mismatch",
                "underlying_move_exit_pct": "-0.40",
            }
        )

        self.assertEqual(record["near_miss_quality"], "useful_reject_avoided_loss")

    def test_update_writes_durable_ledger_and_summary(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            trades_path = tmp_path / "trades.csv"
            outcome_path = tmp_path / "outcomes.csv"
            summary_path = tmp_path / "summary.json"
            rows = [base_trade(), base_trade(direction="put", realized_pnl_usd="-40.00", exit_underlying_price="121.00")]
            with trades_path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=sorted(rows[0].keys()))
                writer.writeheader()
                writer.writerows(rows)

            summary = semantics.update_trade_outcome_semantics(
                trades_path=trades_path,
                skipped_rows=[
                    {
                        "timestamp": "2026-06-10 10:00:00 EDT",
                        "date": "2026-06-10",
                        "symbol": "SPY",
                        "direction": "put",
                        "underlying_move_exit_pct": "-0.20",
                        "reason": "blocked_entry_hour",
                    }
                ],
                outcome_path=outcome_path,
                summary_path=summary_path,
            )

            self.assertTrue(outcome_path.exists())
            self.assertTrue(summary_path.exists())
            self.assertEqual(summary["closed_trade_count"], 2)
            self.assertEqual(summary["skipped_signal_count"], 1)
            self.assertIn("setup_quality", summary)


if __name__ == "__main__":
    unittest.main()
