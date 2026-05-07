"""Regression tests for replay optimizer rolling window helpers."""

from __future__ import annotations

import tempfile
import unittest
from datetime import date
from pathlib import Path
from types import SimpleNamespace
import csv

import replay_optimizer  # noqa: E402


class ReplayOptimizerTests(unittest.TestCase):
    def _namespace(self, **kwargs):
        defaults = {
            "rolling_step_days": 1,
            "rolling_end_policy": "fixed",
            "interval": "5m",
        }
        defaults.update(kwargs)
        return SimpleNamespace(**defaults)

    def test_next_window_dates_fixed_step_advances_windows(self):
        args = self._namespace(rolling_step_days=7, rolling_end_policy="fixed")
        nxt = replay_optimizer._next_window_dates(
            current_start=date(2026, 2, 1),
            current_end=date(2026, 2, 8),
            args=args,
            symbols=["AAPL"],
            cache_dir=Path("n/a"),
        )
        self.assertEqual(nxt, (date(2026, 2, 8), date(2026, 2, 15)))

    def test_next_window_dates_cache_requires_window_coverage(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "AAPL_5m_2026-01-01_2026-01-20.csv").touch()
            (root / "MSFT_5m_2026-01-01_2026-01-20.csv").touch()

            args = self._namespace(
                rolling_step_days=1,
                rolling_end_policy="cache",
                interval="5m",
            )
            nxt = replay_optimizer._next_window_dates(
                current_start=date(2026, 1, 1),
                current_end=date(2026, 1, 11),
                args=args,
                symbols=["AAPL", "MSFT"],
                cache_dir=root,
            )
            self.assertEqual(nxt, (date(2026, 1, 10), date(2026, 1, 20)))

            (root / "MSFT_5m_2026-01-01_2026-01-20.csv").unlink()
            (root / "MSFT_5m_2026-01-15_2026-01-20.csv").write_text(
                "timestamp,open,high,low,close,volume\n",
                encoding="utf-8",
            )
            nxt = replay_optimizer._next_window_dates(
                current_start=date(2026, 1, 1),
                current_end=date(2026, 1, 11),
                args=args,
                symbols=["AAPL", "MSFT"],
                cache_dir=root,
            )
            self.assertIsNone(nxt)


    def test_append_ratio_history(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "ratio_history.csv"
            board = [
                {
                    "candidate": "baseline",
                    "windows": 2,
                    "evaluated": 40,
                    "wins": 22,
                    "losses": 18,
                    "win_rate_pct": 55.0,
                },
                {
                    "candidate": "strict",
                    "windows": 2,
                    "evaluated": 10,
                    "wins": 3,
                    "losses": 0,
                    "win_rate_pct": 100.0,
                },
            ]
            replay_optimizer._append_ratio_history(path=path, board=board, iteration=4, iteration_timestamp="2026-05-07T12:00:00")
            with path.open("r", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 2)
            baseline = next(row for row in rows if row["candidate"] == "baseline")
            strict = next(row for row in rows if row["candidate"] == "strict")
            self.assertEqual(float(baseline["win_loss_ratio"]), 1.2222)
            self.assertEqual(strict["win_loss_ratio"], "inf")

    def test_row_passes_respects_min_win_loss_ratio(self):
        row = {
            "evaluated": 20,
            "wins": 11,
            "losses": 9,
            "win_rate_pct": 55.0,
            "expectancy_pct": 0.06,
        }
        self.assertFalse(
            replay_optimizer._row_passes(
                row,
                min_trades=5,
                target_win_rate_pct=55.0,
                target_expectancy_pct=0.05,
                min_win_loss_ratio=1.25,
            )
        )
        self.assertTrue(
            replay_optimizer._row_passes(
                row,
                min_trades=5,
                target_win_rate_pct=55.0,
                target_expectancy_pct=0.05,
                min_win_loss_ratio=1.20,
            )
        )


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
