"""Unit tests for the scanner's pure direction-voting helper."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

# Allow `python -m unittest` from the repo root to import `scanner`.
_PKG_DIR = Path(__file__).resolve().parent.parent
if str(_PKG_DIR) not in sys.path:
    sys.path.insert(0, str(_PKG_DIR))

from scanner import compute_direction_from_votes  # noqa: E402


class ComputeDirectionFromVotesTests(unittest.TestCase):
    def test_all_positive_votes_pick_call_with_full_score(self):
        votes = [
            ("price", 1.0, 1.0),
            ("momentum_fast", 0.9, 1.0),
            ("momentum_slow", 0.6, 1.0),
            ("ema_trend", 0.6, 1.0),
            ("vwap_side", 0.3, 1.0),
        ]
        direction, score, abs_score, aligned = compute_direction_from_votes(votes)
        self.assertEqual(direction, "call")
        self.assertAlmostEqual(score, 1.0, places=6)
        self.assertAlmostEqual(abs_score, 1.0, places=6)
        self.assertEqual(aligned, len(votes))

    def test_all_negative_votes_pick_put_with_full_score(self):
        votes = [
            ("price", 1.0, -1.0),
            ("momentum_fast", 0.9, -1.0),
            ("momentum_slow", 0.6, -1.0),
            ("ema_trend", 0.6, -1.0),
            ("vwap_side", 0.3, -1.0),
        ]
        direction, score, abs_score, aligned = compute_direction_from_votes(votes)
        self.assertEqual(direction, "put")
        self.assertAlmostEqual(score, -1.0, places=6)
        self.assertAlmostEqual(abs_score, 1.0, places=6)
        self.assertEqual(aligned, len(votes))

    def test_split_votes_choose_higher_weight_side(self):
        # Heavy bullish weight (1.0 + 0.9 = 1.9) vs lighter bearish (0.6 + 0.6 + 0.3 = 1.5).
        votes = [
            ("price", 1.0, 1.0),
            ("momentum_fast", 0.9, 1.0),
            ("momentum_slow", 0.6, -1.0),
            ("ema_trend", 0.6, -1.0),
            ("vwap_side", 0.3, -1.0),
        ]
        direction, score, abs_score, aligned = compute_direction_from_votes(votes)
        self.assertEqual(direction, "call")
        self.assertGreater(score, 0.0)
        self.assertLess(abs_score, 1.0)
        # Aligned count equals number of bullish votes.
        self.assertEqual(aligned, 2)

    def test_tie_breaks_to_call(self):
        # Symmetric weights summing to zero must pick "call" by convention.
        votes = [
            ("a", 1.0, 1.0),
            ("b", 1.0, -1.0),
        ]
        direction, score, _abs, aligned = compute_direction_from_votes(votes)
        self.assertEqual(direction, "call")
        self.assertEqual(score, 0.0)
        self.assertEqual(aligned, 1)

    def test_empty_votes_return_neutral_call(self):
        direction, score, abs_score, aligned = compute_direction_from_votes([])
        self.assertEqual(direction, "call")
        self.assertEqual(score, 0.0)
        self.assertEqual(abs_score, 0.0)
        self.assertEqual(aligned, 0)

    def test_zero_weight_votes_yield_zero_score(self):
        votes = [("a", 0.0, 1.0), ("b", 0.0, -1.0)]
        direction, score, abs_score, aligned = compute_direction_from_votes(votes)
        self.assertEqual(direction, "call")
        self.assertEqual(abs_score, 0.0)
        self.assertEqual(score, 0.0)
        # aligned still counts sign matches against chosen direction.
        self.assertEqual(aligned, 1)

    def test_aligned_count_excludes_zero_votes(self):
        votes = [
            ("a", 1.0, 1.0),
            ("b", 1.0, 0.0),
            ("c", 1.0, 1.0),
        ]
        direction, _score, _abs, aligned = compute_direction_from_votes(votes)
        self.assertEqual(direction, "call")
        # Zero-vote contributors are not counted as aligned.
        self.assertEqual(aligned, 2)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
